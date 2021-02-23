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


namespace viper::kv_bm {

static constexpr size_t NUM_GLEANERS = 1;
static constexpr size_t NUM_LOGS = 4;

template <typename KeyT>
struct CrlFrontendHash {
    // Use same impl as tbb_hasher
    static const size_t hash_multiplier = tbb::detail::select_size_t_constant<2654435769U, 11400714819323198485ULL>::value;
    inline static size_t hash(const KeyT& a) {
        return static_cast<size_t>(a.data[0]) * hash_multiplier;
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

template <typename KeyT, typename ValueT>
struct CrlRecord {
    static constexpr size_t SIZE = sizeof(CrlRecord<KeyT, ValueT>);

    size_t len;
    size_t klen;
    OpCode opcode;
    bool applied;
    CrlRecord* prev;
    size_t epoch;
    KeyT key;
    ValueT value;
};

template <typename KeyT, typename ValueT>
struct CrossReferencingLog {
    using Entry = CrlRecord<KeyT, ValueT>;
    static constexpr size_t _LOG_FILE_SIZE = 60 * (1024ul * 1024);
    static constexpr size_t NUM_ENTRIES = _LOG_FILE_SIZE / sizeof(Entry);
    static constexpr size_t LOG_FULL = NUM_ENTRIES + 1;

    std::atomic<size_t> head;
    std::atomic<size_t> tail;
    std::atomic<bool> locked;
    std::array<Entry, NUM_ENTRIES> log;

    void init() {
        locked = false;
        head = 0;
        tail = 0;
    }
};

template <typename KeyT, typename ValueT>
struct FrontendValue {
    ValueT value;
    CrlRecord<KeyT, ValueT>* lentry;
    bool tombstone;
};

template <typename KeyT, typename ValueT>
class CrlStore {
    using CrlStoreT = CrlStore<KeyT, ValueT>;
    using LogRecordT = CrlRecord<KeyT, ValueT>;
    using CrossReferencingLogT = CrossReferencingLog<KeyT, ValueT>;
    using FrontendValueT = FrontendValue<KeyT, ValueT>;
    using FrontendT = tbb::concurrent_hash_map<KeyT, FrontendValueT, CrlFrontendHash<KeyT>>;
    using BackendT = pmem::obj::concurrent_hash_map<KeyT, ValueT, CrlBackendHash<KeyT>>;

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
    protected:
        Client(CrlStoreT& crl_store, size_t client_id);
        CrlStoreT& crl_store_;
        CrossReferencingLogT& log_;
    };

    Client get_client();

private:
    void glean(bool is_epoch_manager, std::atomic<bool>& killed);

    struct BackendRootT {
        pmem::obj::persistent_ptr<BackendT> pmem_map;
    };

    std::unique_ptr<FrontendT> frontend_;
    pmem::obj::persistent_ptr<BackendT> backend_;
    std::atomic<size_t> num_clients_;
    std::array<CrossReferencingLogT, NUM_LOGS>* logs_;
    std::vector<std::thread> gleaners_;
    std::array<std::atomic<bool>, NUM_GLEANERS> gleaner_locks_;

    const std::string& log_file_;
    const std::string& backend_file_;
    pmem::obj::pool<BackendRootT> backend_pool_;
};

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::CrlStore(const std::string& log_file, const std::string& backend_file,
                                 const size_t backend_file_size)
    : log_file_{log_file}, backend_file_{backend_file} {
    int is_pmem;
    size_t mapped_length;
    void* log_addr = pmem_map_file(log_file.c_str(), LOG_FILE_SIZE, PMEM_FILE_CREATE, 0644, &mapped_length, &is_pmem);
    if (log_addr == nullptr || log_addr == MAP_FAILED) {
        throw std::runtime_error(std::string("bad mmap log file! ") + std::strerror(errno));
    }
    logs_ = static_cast<std::array<CrossReferencingLogT, NUM_LOGS>*>(log_addr);
    for (CrossReferencingLogT& log : *logs_) {
        log.init();
    }

    frontend_ = std::make_unique<FrontendT>();

    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
    backend_pool_ = pmem::obj::pool<BackendRootT>::create(backend_file, "", backend_file_size, S_IRWXU);
    pmem::obj::transaction::run(backend_pool_, [&] {
        backend_pool_.root()->pmem_map = pmem::obj::make_persistent<BackendT>();
    });
    backend_ = backend_pool_.root()->pmem_map;

    gleaners_.reserve(NUM_GLEANERS);
    for (size_t i = 0; i < NUM_GLEANERS; ++i) {
        gleaner_locks_[i] = false;
        const bool is_epoch_manager = (i == 0);
        gleaners_.emplace_back(&CrlStoreT::glean, this, is_epoch_manager, std::ref(gleaner_locks_[i]));
    }

    num_clients_ = 0;
}

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::~CrlStore() {
    for (std::atomic<bool>& lock : gleaner_locks_) { lock.store(true); }
    for (std::thread& t : gleaners_) { t.join(); }

    frontend_ = nullptr;
    backend_ = nullptr;
    munmap(logs_, LOG_FILE_SIZE);
    backend_pool_.close();
    pmempool_rm(log_file_.c_str(), 0);
    pmempool_rm(backend_file_.c_str(), 0);
}

template<typename KeyT, typename ValueT>
typename CrlStore<KeyT, ValueT>::Client CrlStore<KeyT, ValueT>::get_client() {
    return CrlStore::Client(*this, num_clients_.fetch_add(1));
}

template<typename KeyT, typename ValueT>
void CrlStore<KeyT, ValueT>::glean(const bool is_epoch_manager, std::atomic<bool>& killed) {
    const size_t num_entries = CrossReferencingLogT::NUM_ENTRIES;
    while (!killed.load(std::memory_order_acquire)) {
        bool nop_round = true;
        for (CrossReferencingLogT& log : *logs_) {
            // if (killed.load(std::memory_order_acquire)) { return; }
            const size_t head = log.head.load();
            const size_t tail = log.tail.load();
            const size_t start_entry = head;
            size_t end_entry = tail;

            // Nothing to do for this log
            if (head == tail) { continue; }

            // Log is locked, continue
            bool expected = false;
            if (!log.locked.compare_exchange_strong(expected, true)) { continue; }

            // Might have circled in log
            if (head > tail) { end_entry = num_entries + tail; }

            for (size_t entry_num = start_entry; entry_num < end_entry; ++entry_num) {
                typename FrontendT::accessor frontend_entry;

                // Get from frontend store
                LogRecordT& record = log.log[entry_num % num_entries];
                const KeyT& key = record.key;
                frontend_->insert(frontend_entry, key);
                FrontendValueT& frontend_value = frontend_entry->second;

                // No need to insert if already inserted into backend
                LogRecordT* lentry = frontend_value.lentry;
                if (lentry == nullptr || lentry->applied) { continue; }

                // Add to backend store
                nop_round = false;
                if (lentry->opcode == INSERT) {
                    typename BackendT::accessor backend_entry;
                    backend_->insert(backend_entry, key);
                    backend_entry->second = frontend_value.value;
                } else if (lentry->opcode == DELETE) {
                    backend_->erase(key);
                } else {
                    throw std::runtime_error("Unknown opcode: " + std::to_string(lentry->opcode));
                }

                // Update log record
                lentry->applied = true;
                pmem_persist(&lentry->applied, sizeof(lentry->applied));
                __atomic_compare_exchange_n(&frontend_value.lentry, lentry, nullptr, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
            }

            const size_t new_head = (tail + 1) % log.NUM_ENTRIES;
            log.head.store(new_head);

            const size_t full_tail = log.tail.load();
            if (full_tail == log.LOG_FULL) {
                // Need to set log free again
                log.tail.store(head);
            }

            log.locked.store(false);

            if (nop_round) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
}

template<typename KeyT, typename ValueT>
CrlStore<KeyT, ValueT>::Client::Client(CrlStoreT& crl_store, size_t client_id)
    : crl_store_{crl_store}, log_{crl_store.logs_->at(client_id)} {}

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
    LogRecordT log_entry{.len = LogRecordT::SIZE, .klen = sizeof(KeyT), .opcode = INSERT,
                         .applied = false, .prev = lentry, .epoch = 0, .key = key, .value = value};
    log_.log[current_pos] = log_entry;
    LogRecordT* newest_entry = &log_.log[current_pos];
    pmem_persist(newest_entry, newest_entry->len);

    // Check if this record made log full
    size_t new_tail = (current_pos + 1) % log_.NUM_ENTRIES;
    if (new_tail == log_.head.load()) { new_tail = log_.LOG_FULL; }

    log_.tail.store(new_tail);
    pmem_persist(&log_.tail, sizeof(log_.tail));

    FrontendValueT val_entry{.value = value, .lentry = newest_entry, .tombstone = false};
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
        frontend_put->second = FrontendValueT{ .value = *value, .lentry = nullptr, .tombstone = false };
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
    LogRecordT log_entry{.len = LogRecordT::SIZE, .klen = sizeof(KeyT), .opcode = DELETE,
                         .applied = false, .prev = lentry, .epoch = 0, .key = key};
    log_.log[current_pos] = log_entry;
    LogRecordT* newest_entry = &log_.log[current_pos];
    pmem_persist(newest_entry, newest_entry->len);

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
