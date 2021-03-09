/**
 * Implementation of Cross Referencing Logs as described in
 *   Closing the Performance Gap Between Volatile and Persistent Key-Value Stores Using Cross-Referencing Logs
 *   by Huang et al., ATC '18
 */

#pragma once

#include "common_fixture.hpp"
#include "tbb/concurrent_hash_map.h"
#include "libpmem.h"
#include "libpmemobj++/container/concurrent_hash_map.hpp"
#include "libpmemobj++/container/string.hpp"
#include "libpmemobj++/container/vector.hpp"


namespace viper::kv_bm {

static constexpr size_t MAX_NUM_GLEANERS = 18;
static constexpr size_t NUM_LOGS = 18;

template <typename KeyT>
struct CrlFrontendHash {
    // Use same impl as tbb_hasher
    static const size_t hash_multiplier = tbb::detail::select_size_t_constant<2654435769U, 11400714819323198485ULL>::value;
    inline static size_t hash(const KeyT& a) {
        if constexpr (std::is_same_v<KeyT, std::string> || std::is_same_v<KeyT, pmem::obj::string>) {
            return std::_Hash_impl::hash(a.data(), a.length());
        } else {
            return static_cast<size_t>(a.data[0]) * hash_multiplier;
        }
    }
    inline static bool equal(const KeyT& a, const KeyT& b) { return a == b; }
};

template <typename KeyT>
struct CrlBackendHash {
    inline size_t operator()(const KeyT& a) {
        return CrlFrontendHash<KeyT>::hash(a);
    }
};

enum OpCode : uint8_t {
    INSERT = 0,
    DELETE = 1
};

struct CrlVarValue {
    size_t length;
    std::array<char, 400> data;

    inline void from_string(const std::string& value) {
        length = value.length();
        memcpy(data.data(), value.data(), value.length());
    }
};

template <typename KeyT, typename ValueT>
struct CrlRecord {
    static constexpr size_t SIZE = sizeof(CrlRecord<KeyT, ValueT>);

    size_t len;
    size_t klen;
    OpCode opcode;
    bool applied;
    CrlRecord<KeyT, ValueT>* prev;
    size_t epoch;
    KeyT key;
    ValueT value;
};

template <typename EntryT>
struct CrossReferencingLog {
    static constexpr size_t _LOG_FILE_SIZE = 60 * (1024ul * 1024);
    static constexpr size_t NUM_ENTRIES = _LOG_FILE_SIZE / sizeof(EntryT);
    static constexpr size_t LOG_FULL = NUM_ENTRIES + 1;

    std::atomic<size_t> head;
    std::atomic<size_t> tail;
    std::atomic<bool> locked;
    std::array<EntryT, NUM_ENTRIES> log;

    void init() {
        locked = false;
        head = 0;
        tail = 0;
    }
};

template <typename ValueT, typename EntryT>
struct FrontendValue {
    ValueT value;
    EntryT* lentry;
    bool tombstone;
};

template <typename KeyT, typename ValueT>
class CrlStore {
    using FrontendKeyT = KeyT;
    using FrontendValueT = ValueT;
    using BackendKeyT = typename std::conditional<std::is_same_v<KeyT, std::string>, pmem::obj::string, KeyT>::type;
    using BackendValueT = typename std::conditional<std::is_same_v<ValueT, std::string>, pmem::obj::string, ValueT>::type;

    using CrlStoreT = CrlStore<KeyT, ValueT>;
    using LogRecordT = CrlRecord<BackendKeyT, BackendValueT>;
    using CrossReferencingLogT = CrossReferencingLog<LogRecordT>;
    using WrappedFrontendValueT = FrontendValue<FrontendValueT, LogRecordT>;
    using FrontendT = tbb::concurrent_hash_map<FrontendKeyT, WrappedFrontendValueT, CrlFrontendHash<FrontendKeyT>>;
    using BackendT = pmem::obj::concurrent_hash_map<BackendKeyT, BackendValueT, CrlBackendHash<BackendKeyT>>;

    static constexpr size_t LOG_FILE_SIZE = NUM_LOGS * sizeof(CrossReferencingLogT);

public:
    CrlStore(const std::string& pool_file, const std::string& backend_file, size_t backend_file_size);
    ~CrlStore();

    class Client {
        friend class CrlStore<KeyT, ValueT>;
    public:
        bool put(const KeyT& key, const ValueT& value);
        bool get(const KeyT& key, ValueT* value);
        bool remove(const KeyT& key);
        ~Client();
    protected:
        Client(CrlStoreT& crl_store, size_t client_id, bool add_gleaner);
        CrlStoreT& crl_store_;
        CrossReferencingLogT& log_;
        const bool has_gleaner_;
    };

    Client get_client();
    Client get_read_only_client();

    void collect_gleaners();

private:
    void glean(const size_t id, std::atomic<bool>& killed);

    using LogsT = std::array<CrossReferencingLogT, NUM_LOGS>;
    struct BackendRootT { pmem::obj::persistent_ptr<BackendT> pmem_map; };
    struct LogRootT { pmem::obj::persistent_ptr<LogsT> logs; };

    std::unique_ptr<FrontendT> frontend_;
    pmem::obj::persistent_ptr<BackendT> backend_;
    pmem::obj::persistent_ptr<LogsT> logs_;
    std::atomic<size_t> num_clients_;
    std::vector<std::thread> gleaners_;
    std::atomic<size_t> num_gleaners_;
    std::atomic<bool> adding_gleaner_;
    std::array<std::atomic<bool>, MAX_NUM_GLEANERS> gleaner_locks_;

    const std::string& log_file_;
    const std::string& backend_file_;
    pmem::obj::pool<LogRootT> log_pool_;
    pmem::obj::pool<BackendRootT> backend_pool_;
};

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::CrlStore(const std::string& log_file, const std::string& backend_file,
                                 const size_t backend_file_size)
    : log_file_{log_file}, backend_file_{backend_file} {
    frontend_ = std::make_unique<FrontendT>();

    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    log_pool_ = pmem::obj::pool<LogRootT>::create(log_file, "", LOG_FILE_SIZE * 100, S_IRWXU);
    pmem::obj::transaction::run(log_pool_, [&] {
        log_pool_.root()->logs = pmem::obj::make_persistent<LogsT>();
    });
    logs_ = log_pool_.root()->logs;
    for (CrossReferencingLogT& log : *logs_) {
        log.init();
    }

    backend_pool_ = pmem::obj::pool<BackendRootT>::create(backend_file, "", backend_file_size, S_IRWXU);
    pmem::obj::transaction::run(backend_pool_, [&] {
        backend_pool_.root()->pmem_map = pmem::obj::make_persistent<BackendT>();
    });
    backend_ = backend_pool_.root()->pmem_map;

    num_gleaners_.store(0);
    adding_gleaner_.store(false);
    gleaners_.resize(MAX_NUM_GLEANERS);
    num_clients_ = 0;
}

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::~CrlStore() {
    collect_gleaners();

    frontend_ = nullptr;
    backend_ = nullptr;
    logs_ = nullptr;
    log_pool_.close();
    backend_pool_.close();
    pmempool_rm(log_file_.c_str(), 0);
    pmempool_rm(backend_file_.c_str(), 0);
}

template<typename KeyT, typename ValueT>
typename CrlStore<KeyT, ValueT>::Client CrlStore<KeyT, ValueT>::get_client() {
    return CrlStore::Client(*this, num_clients_.fetch_add(1), true);
}

template<typename KeyT, typename ValueT>
typename CrlStore<KeyT, ValueT>::Client CrlStore<KeyT, ValueT>::get_read_only_client() {
    return CrlStore::Client(*this, 0, false);
}

template<typename KeyT, typename ValueT>
void CrlStore<KeyT, ValueT>::collect_gleaners() {
    for (std::atomic<bool>& lock : gleaner_locks_) { lock.store(true); }
    for (std::thread& t : gleaners_) {
        if (t.joinable()) { t.join(); }
    }
    num_gleaners_.store(0);
    gleaners_.clear();
    gleaners_.resize(MAX_NUM_GLEANERS);
}

template<typename KeyT, typename ValueT>
void CrlStore<KeyT, ValueT>::glean(const size_t id, std::atomic<bool>& killed) {
    set_cpu_affinity(id + (NUM_MAX_THREADS / 2));
    const size_t num_entries = CrossReferencingLogT::NUM_ENTRIES;
    CrossReferencingLogT &log = logs_->at(id);
    while (!killed.load()) {
        const size_t head = log.head.load();
        const size_t tail = log.tail.load();
        const size_t start_entry = head;
        size_t end_entry = tail;

        // Nothing to do for this log
        if (head == tail) { continue; }

        // Might have circled in log
        if (head > tail) { end_entry = num_entries + tail; }

        bool nop_round = true;
        for (size_t entry_num = start_entry; entry_num < end_entry; ++entry_num) {
            // Get from frontend store
            typename FrontendT::accessor frontend_entry;
            LogRecordT &record = log.log[entry_num % num_entries];
            const BackendKeyT& be_key = record.key;
            const FrontendKeyT fe_key{be_key};
            frontend_->insert(frontend_entry, fe_key);
            WrappedFrontendValueT& frontend_value = frontend_entry->second;

            // No need to insert if already inserted into backend
            LogRecordT *lentry = frontend_value.lentry;
            if (lentry == nullptr || lentry->applied) { continue; }

            // Add to backend store
            nop_round = false;
            if (lentry->opcode == INSERT) {
                backend_->insert_or_assign(be_key, frontend_value.value);
            } else if (lentry->opcode == DELETE) {
                backend_->erase(be_key);
            } else {
                throw std::runtime_error("Unknown opcode: " + std::to_string(lentry->opcode));
            }

            // Update log record
            lentry->applied = true;
            pmem_persist(&lentry->applied, sizeof(lentry->applied));
            __atomic_compare_exchange_n(&frontend_value.lentry, lentry, nullptr, false, __ATOMIC_ACQUIRE,
                                        __ATOMIC_ACQUIRE);
        }

        const size_t new_head = (tail + 1) % log.NUM_ENTRIES;
        log.head.store(new_head);

        const size_t full_tail = log.tail.load();
        if (full_tail == log.LOG_FULL) {
            // Need to set log free again
            log.tail.store(head);
        }

        if (nop_round) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::Client::Client(CrlStoreT& crl_store, size_t client_id, bool add_gleaner)
    : crl_store_{crl_store}, log_{crl_store.logs_->at(client_id)}, has_gleaner_{add_gleaner} {
    if (add_gleaner) {
        auto& gleaner_lock = crl_store_.gleaner_locks_[client_id];
        gleaner_lock.store(false);
        crl_store_.gleaners_[client_id] = std::thread{&CrlStoreT::glean, &crl_store_, client_id, std::ref(gleaner_lock)};
    }
}

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::Client::~Client() {
    if (has_gleaner_) {
        crl_store_.num_clients_--;
    }
}

template<typename KeyT, typename ValueT>
bool CrlStore<KeyT, ValueT>::Client::put(const KeyT& key, const ValueT& value) {
    // Wait if log is full
    size_t current_pos = log_.tail.load();
    while (current_pos == log_.LOG_FULL) {
        _mm_pause();
        current_pos = log_.tail.load();
    }

    typename FrontendT::accessor result;
    const bool is_new = crl_store_.frontend_->insert(result, key);

    LogRecordT* lentry = is_new ? nullptr : result->second.lentry;
    LogRecordT& newest_entry = log_.log[current_pos];
    newest_entry.len = LogRecordT::SIZE;
    newest_entry.opcode = INSERT;
    newest_entry.applied = false;
    newest_entry.prev = lentry;
    newest_entry.key = key;
    newest_entry.value = value;
    pmem_persist(&newest_entry, newest_entry.len);

    // Check if this record made log full
    size_t new_tail = (current_pos + 1) % log_.NUM_ENTRIES;
    if (new_tail == log_.head.load()) { new_tail = log_.LOG_FULL; }

    log_.tail.store(new_tail);
    pmem_persist(&log_.tail, sizeof(log_.tail));

    WrappedFrontendValueT val_entry{.value = value, .lentry = &newest_entry, .tombstone = false};
    result->second = val_entry;

    return is_new;
}

template<typename KeyT, typename ValueT>
bool CrlStore<KeyT, ValueT>::Client::get(const KeyT& key, ValueT* value) {
    typename FrontendT::const_accessor frontend_result;
    const bool fe_found = crl_store_.frontend_->find(frontend_result, key);
    if (fe_found) {
        *value = frontend_result->second.value;
        return true;
    }

    typename BackendT::const_accessor backend_result;
    const bool be_found = crl_store_.backend_->find(backend_result, key);
    if (be_found) {
        *value = backend_result->second;

        typename FrontendT::accessor frontend_put;
        crl_store_.frontend_->insert(frontend_put, key);
        frontend_put->second = WrappedFrontendValueT{ .value = *value, .lentry = nullptr, .tombstone = false };
        return true;
    }

    return false;
}

template<typename KeyT, typename ValueT>
bool CrlStore<KeyT, ValueT>::Client::remove(const KeyT &key) {
    typename FrontendT::accessor frontend_remove;
    const bool found = crl_store_.frontend_->find(frontend_remove, key);

    // Wait if log is full
    size_t current_pos = log_.tail.load();
    while (current_pos == log_.LOG_FULL) {
        _mm_pause();
        current_pos = log_.tail.load();
    }

    LogRecordT* lentry = found ? frontend_remove->second.lentry : nullptr;
    LogRecordT& newest_entry = log_.log[current_pos];
    newest_entry.len = LogRecordT::SIZE;
    newest_entry.opcode = DELETE;
    newest_entry.applied = false;
    newest_entry.prev = lentry;
    newest_entry.key = key;
    pmem_persist(&newest_entry, newest_entry.len);

    // Check if this record made log full
    size_t new_tail = (current_pos + 1) % log_.NUM_ENTRIES;
    if (new_tail == log_.head.load()) { new_tail = log_.LOG_FULL; }

    log_.tail.store(new_tail);
    pmem_persist(&log_.tail, sizeof(log_.tail));

    if (found) {
        frontend_remove->second.tombstone = true;
    }
    return true;
}


}  // namespace viper::kv_bm
