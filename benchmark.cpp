#include <iostream>
#include <string>
#include <filesystem>
#include <random>
#include <mutex>

#include <benchmark/benchmark.h>
#include <tbb/concurrent_hash_map.h>
#include <libpmempool.h>
#include <libpmem.h>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/vector.hpp>

#include "src/viper.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "third_party/faster/cc/src/core/faster.h"
#include "third_party/faster/cc/src/environment/file.h"
#include "third_party/faster/cc/src/device/file_system_disk.h"

static constexpr char POOL_FILE_DIR[] = "/mnt/nvrams1/kv-bm";
//static constexpr char DB_FILE_DIR[] = "/mnt/nvrams1/dbfiles";
static constexpr char DB_FILE_DIR[] = "/home/lawrence.benson/dbfiles";
static const uint64_t BM_POOL_SIZE = (1024l*1024*1024) * 5;  // 5GB

static constexpr uint64_t NUM_INSERTS = 10000000;
static constexpr uint64_t NUM_PREFILLS = NUM_INSERTS;
static constexpr uint64_t MAX_DATA_SIZE = NUM_PREFILLS + NUM_INSERTS;
static constexpr uint64_t NUM_FINDS = 1000000;
static constexpr uint64_t NUM_REPETITIONS = 1;
static constexpr uint64_t NUM_MAX_THREADS = 64;
static constexpr benchmark::TimeUnit BM_TIME_UNIT = benchmark::TimeUnit::kMicrosecond;

using KeyType = uint64_t;
using ValueType = uint64_t;
using Offset = uint64_t;

using DramMapType = tbb::concurrent_hash_map<KeyType, ValueType>;
using PmemMapType = pmem::obj::concurrent_hash_map<KeyType, ValueType>;
using HybridMapType = tbb::concurrent_hash_map<KeyType, Offset>;
using HybridVectorType = pmem::obj::array<ValueType, MAX_DATA_SIZE>;

using pmem::obj::p;
using pmem::obj::persistent_ptr;

std::string random_file(const std::filesystem::path& base_dir) {
    if (!std::filesystem::exists(base_dir)) {
        if (!std::filesystem::create_directories(base_dir)) {
            throw std::runtime_error{"Could not create dir: " + base_dir.string() + "\n"};
        }
    }
    std::string str("abcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(str.begin(), str.end(), generator);
    std::string file_name = str.substr(0, 15) + ".file";
    std::filesystem::path file{file_name};
    return base_dir / file;
}

class BaseFixture : public benchmark::Fixture {
  public:
    void SetUp(benchmark::State& state) override {
        std::random_device rnd{};
        rnd_engine_ = std::default_random_engine(rnd());
    }

    static void log_find_count(benchmark::State& state, const uint64_t num_found, const uint64_t num_expected) {
        state.counters["found"] = num_found;
        if (num_found != num_expected) {
            std::cout << "DID NOT FIND ALL ENTRIES (" + std::to_string(num_found)
                + "/" + std::to_string(num_expected) + ")\n";
        }
    }

  protected:
    std::default_random_engine rnd_engine_;
};

template <typename RootType>
class BasePmemFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) override {
        BaseFixture::SetUp(state);
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

        {
            std::scoped_lock lock(pool_mutex_);
            if (pool_file_.empty()) {
                pool_file_ = random_file(POOL_FILE_DIR);
                pmem_pool_ = pmem::obj::pool<RootType>::create(pool_file_, "", BM_POOL_SIZE, S_IRWXU);
            }
        }

    }

    void TearDown(benchmark::State& state) override {
        {
            std::scoped_lock lock(pool_mutex_);
            if (!pool_file_.empty() && std::filesystem::exists(pool_file_)) {
                if (pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL) == -1) {
                    std::cout << pmempool_errormsg() << std::endl;
                }
                pool_file_.clear();
            }
        }
    }

    void DeInitMap() {
        pmem_pool_.close();
    }

  protected:
    pmem::obj::pool<RootType> pmem_pool_;
    std::filesystem::path pool_file_;
    std::mutex pool_mutex_;
};

class FileBasedFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) override {
        BaseFixture::SetUp(state);
        {
            std::scoped_lock lock(db_mutex_);
            if (db_file_.empty()) {
                db_file_ = random_file(DB_FILE_DIR);
            }
        }
    }

    void TearDown(benchmark::State& state) override {
        {
            std::scoped_lock lock(db_mutex_);
            if (!db_file_.empty() && std::filesystem::exists(db_file_)) {
                std::filesystem::remove_all(db_file_);
                db_file_.clear();
            }
        }
    }

  protected:
    std::filesystem::path db_file_;
    std::mutex db_mutex_;
};

class DramMapFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (map_initialized_ && !re_init) {
            return;
        }
        dram_map_ = std::make_unique<DramMapType>();
        for (uint64_t i = 0; i < num_prefill_inserts; ++i) {
            dram_map_->insert({i, i});
        }
        map_initialized_ = true;
    }

  protected:
    std::unique_ptr<DramMapType> dram_map_;
    bool map_initialized_ = false;

};

struct PmemMapRoot {
    persistent_ptr<PmemMapType> pmem_map;
};

class PmemMapFixture : public BasePmemFixture<PmemMapRoot> {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (map_initialized_ && !re_init) {
            return;
        }

        pmem::obj::transaction::run(pmem_pool_, [&] {
            pmem_pool_.root()->pmem_map = pmem::obj::make_persistent<PmemMapType>();
        });
        pmem_map_ = pmem_pool_.root()->pmem_map;
        for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
            PmemMapType::accessor result;
            pmem_map_->insert(result, key);
            result->second = key*100;
        }
        map_initialized_ = true;
    }

  protected:
    persistent_ptr<PmemMapType> pmem_map_;
    bool map_initialized_ = false;
};

struct HybridMapRoot {
    persistent_ptr<HybridVectorType> data;
};

class HybridMapFixture : public BasePmemFixture<HybridMapRoot> {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (map_initialized_ && !re_init) {
            return;
        }

        pmem::obj::transaction::run(pmem_pool_, [&] {
            pmem_pool_.root()->data = pmem::obj::make_persistent<HybridVectorType>();
        });
        map_ = std::make_unique<HybridMapType>();
        data_ = pmem_pool_.root()->data;

        for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
            (*data_)[key] = key;
            HybridMapType::accessor slot;
            map_->insert(slot, key);
            slot->second = key;
        }
        data_.persist();
        map_initialized_ = true;
    }


  protected:
    std::unique_ptr<HybridMapType> map_;
    persistent_ptr<HybridVectorType> data_;
    bool map_initialized_ = false;
};

class ViperFixture : public BasePmemFixture<viper::ViperRoot<KeyType, ValueType>> {
    using ViperRoot = viper::ViperRoot<KeyType, ValueType>;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (viper_initialized_ && !re_init) {
            return;
        }

        pmem::obj::transaction::run(pmem_pool_, [&] {
            pmem_pool_.root()->create_new_block();
        });
        viper_ = std::make_unique<viper::Viper<KeyType, ValueType>>(pmem_pool_);

        for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
            viper_->put(key, key);
        }
        viper_initialized_ = true;
    }

  protected:
    std::unique_ptr<viper::Viper<KeyType, ValueType>> viper_;
    bool viper_initialized_ = false;
};

class RocksDbFixture : public FileBasedFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (rocksdb_initialized_ && !re_init) {
            return;
        }

        rocksdb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = true;
        rocksdb::Status status = rocksdb::DB::Open(options, db_file_, &db_);
        if (!status.ok()) {
            std::cerr << status.ToString() << std::endl;
        }

        const rocksdb::WriteOptions& write_options = rocksdb::WriteOptions();
        for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
            const rocksdb::Slice db_key = std::to_string(key);
            const rocksdb::Slice value = std::to_string(key);
            db_->Put(write_options, db_key, value);
        }
        rocksdb_initialized_ = true;
    }

    void DeInitMap() {
        delete db_;
    }

  protected:
    rocksdb::DB* db_;
    bool rocksdb_initialized_;
};

class FasterFixture : public FileBasedFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {
        if (faster_initialized_ && !re_init) {
            return;
        }

        db_ = std::make_unique<faster_t>((1L << 27), 17179869184, db_file_);

        auto callback = [](IAsyncContext* ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt };
            assert(result == Status::Ok);
        };

        db_->StartSession();

        for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
            if (key % kRefreshInterval == 0) {
                db_->Refresh();
                if (key % kCompletePendingInterval == 0) {
                    db_->CompletePending(false);
                }
            }

            UpsertContext context{ key, key };
            db_->Upsert(context, callback, 1);
        }

        db_->Refresh();
        db_->CompletePending(true);
        db_->StopSession();

        faster_initialized_ = true;
    }

    void DeInitMap() {
        db_ = nullptr;
        faster_initialized_ = false;
    }

  protected:
    class FasterKey {
      public:
        FasterKey(uint64_t key)
            : key_{ key } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterKey));
        }
        inline KeyHash GetHash() const {
            return KeyHash{ Utility::GetHashCode(key_) };
        }

        inline bool operator==(const FasterKey& other) const {
            return key_ == other.key_;
        }
        inline bool operator!=(const FasterKey& other) const {
            return key_ != other.key_;
        }

      private:
        uint64_t key_;
    };

    class FasterValue {
      public:
        FasterValue()
            : value_{0 } {
        }
        FasterValue(const FasterValue& other)
            : value_{other.value_ } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterValue));
        }

        union {
            uint64_t value_;
            std::atomic<uint64_t> atomic_value_;
        };
    };

    class UpsertContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        UpsertContext(const FasterKey& key, uint64_t input)
            : key_{ key }
            , input_{ input } {
        }

        /// Copy (and deep-copy) constructor.
        UpsertContext(const UpsertContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ } {
        }

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FasterKey& key() const {
            return key_;
        }

        inline static constexpr uint32_t value_size() {
            return sizeof(value_t);
        }
        inline static constexpr uint32_t value_size(const FasterValue& old_value) {
            return sizeof(value_t);
        }

        inline void Put(value_t& value) {
            value.value_ = input_;
        }
        inline bool PutAtomic(value_t& value) {
            value.atomic_value_.store(input_);
            return true;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        uint64_t input_;
    };

/// Context to read the store (after recovery).
    class ReadContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        ReadContext(const FasterKey& key, uint64_t* result)
            : key_{ key }
            , result_{ result } {
        }

        /// Copy (and deep-copy) constructor.
        ReadContext(const ReadContext& other)
            : key_{ other.key_ }
            , result_{ other.result_ } {
        }

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FasterKey& key() const {
            return key_;
        }

        inline void Get(const value_t& value) {
            *result_ = value.value_;
        }
        inline void GetAtomic(const value_t& value) {
            *result_ = value.atomic_value_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        uint64_t* result_;
    };

    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
    typedef FASTER::core::FasterKv<FasterKey, FasterValue, disk_t> faster_t;

    std::unique_ptr<faster_t> db_;
    bool faster_initialized_;
    const uint64_t kRefreshInterval = 64;
    const uint64_t kCompletePendingInterval = 1600;
};

BENCHMARK_DEFINE_F(DramMapFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 0) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            uint64_t key = i;
            DramMapType::accessor result;
            dram_map_->insert(result, key);
            result->second = key*100;
        }
    }
}

BENCHMARK_DEFINE_F(DramMapFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            uint64_t key = i;
            DramMapType::accessor result;
            dram_map_->insert(result, key);
            result->second = key*100;
        }
    }
}

BENCHMARK_DEFINE_F(DramMapFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    int found_counter = 0;
    for (auto _ : state) {
        found_counter = 0;
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            DramMapType::const_accessor result;
            const bool found = dram_map_->find(result, i);
            found_counter += found;
        }
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_DEFINE_F(PmemMapFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 0) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            uint64_t key = i;
            PmemMapType::accessor result;
            pmem_map_->insert(result, key);
            result->second = key*100;
        }
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(PmemMapFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            uint64_t key = i;
            PmemMapType::accessor result;
            pmem_map_->insert(result, key);
            result->second = key*100;
        }
    }
}

BENCHMARK_DEFINE_F(PmemMapFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    int found_counter = 0;
    for (auto _ : state) {
        found_counter = 0;
        for (uint64_t i = start_idx; i < end_idx; ++i) {
            PmemMapType::const_accessor result;
            const bool found = pmem_map_->find(result, i);
            found_counter += found;
        }
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_DEFINE_F(HybridMapFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 0) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        const ValueType* data_start = data_->data();
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            // This is not correct because of race conditions.
            // But it's enough to get a feeling for the performance.
            const uint64_t pos = key;
            const ValueType value = key*100;
            pmem_memmove_persist((void*) (data_start + key), &value, sizeof(ValueType));
//            (*data_)[key] = key*100;
            map_->insert({key, pos});
        }
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(HybridMapFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        const ValueType* data_start = data_->data();
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            // This is not correct because of race conditions.
            // But it's enough to get a feeling for the performance.
            const uint64_t pos = key;
            const ValueType value = key*100;
            pmem_memmove_persist((void*) (data_start + key), &value, sizeof(ValueType));
//            (*data_)[key] = key*100;
            map_->insert({key, pos});
        }
    }
}

BENCHMARK_DEFINE_F(HybridMapFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    int found_counter = 0;
    for (auto _ : state) {
        found_counter = 0;
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            HybridMapType::const_accessor result;
            found_counter += map_->find(result, key);
            benchmark::DoNotOptimize((*data_)[result->second] == 0);
        }
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_DEFINE_F(ViperFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 0) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            const ValueType value = key*100;
            viper_->put(key, value);
        }
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(ViperFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            const ValueType value = key*100;
            viper_->put(key, value);
        }
    }

    if (viper_->count() != num_total_inserts + num_total_prefill) {
        std::cout << "Did not insert all values! ("
                  << viper_->count() << "/" << (num_total_inserts + num_total_prefill) << ")" << std::endl;
    }
}

BENCHMARK_DEFINE_F(ViperFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    int found_counter = 0;
    for (auto _ : state) {
        found_counter = 0;
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            found_counter += (viper_->get(key) == key);
        }
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_DEFINE_F(RocksDbFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 1 || state.threads == 1) {
        InitMap();
    } else {
        std::this_thread::sleep_for(std::chrono::seconds{1});
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    auto start = std::chrono::high_resolution_clock::now();
    const auto& write_options = rocksdb::WriteOptions();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        const rocksdb::Slice db_key = std::to_string(key);
        const rocksdb::Slice value = std::to_string(key*100);
        db_->Put(write_options, db_key, value);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::string msg = "Thread (" + std::to_string(state.thread_index) + ") took: "
                        + std::to_string(duration) + " us (" + std::to_string(num_total_inserts) + " ops)\n";
    std::cout << msg;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

//    const auto& write_options = rocksdb::WriteOptions();
    for (auto _ : state) {
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            const rocksdb::Slice db_key = std::to_string(key);
            const rocksdb::Slice value = std::to_string(key*100);
            db_->Put(write_options, db_key, value);
        }
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(RocksDbFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    const auto& write_options = rocksdb::WriteOptions();
    for (auto _ : state) {
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            const rocksdb::Slice db_key = std::to_string(key);
            const rocksdb::Slice value = std::to_string(key*100);
            db_->Put(write_options, db_key, value);
        }
    }
}

BENCHMARK_DEFINE_F(RocksDbFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 0) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    int found_counter = 0;
    const auto& read_options = rocksdb::ReadOptions();
    for (auto _ : state) {
        found_counter = 0;
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            std::string value;
            const rocksdb::Slice db_key = std::to_string(key);
            found_counter += db_->Get(read_options, db_key, &value).ok();
        }
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_DEFINE_F(FasterFixture, insert_empty)(benchmark::State& state) {
    const uint64_t num_total_inserts = state.range(0);

    if (state.thread_index == 1 || state.threads == 1) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt };
        assert(result == Status::Ok);
    };

    while (!faster_initialized_) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    for (auto _ : state) {
        db_->StartSession();

        for (uint64_t key = start_idx; key < end_idx; ++key) {
            if (key % kRefreshInterval == 0) {
                db_->Refresh();
                if (key % kCompletePendingInterval == 0) {
                    db_->CompletePending(false);
                }
            }

            UpsertContext context{ key, key*100 };
            db_->Upsert(context, callback, 1);
        }

        db_->Refresh();
        db_->CompletePending(true);
        db_->StopSession();
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(FasterFixture, setup_and_insert)(benchmark::State& state) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    if (state.thread_index == 1 || state.threads == 1) {
        InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt };
        assert(result == Status::Ok);
    };

    while (!faster_initialized_) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    for (auto _ : state) {
        db_->StartSession();

        for (uint64_t key = start_idx; key < end_idx; ++key) {
            if (key % kRefreshInterval == 0) {
                db_->Refresh();
                if (key % kCompletePendingInterval == 0) {
                    db_->CompletePending(false);
                }
            }

            UpsertContext context{ key, key*100 };
            db_->Upsert(context, callback, 1);
        }

        db_->Refresh();
        db_->CompletePending(true);
        db_->StopSession();
    }

    if (state.thread_index == 0) {
        DeInitMap();
    }
}

BENCHMARK_DEFINE_F(FasterFixture, setup_and_find)(benchmark::State& state) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    if (state.thread_index == 1 || state.threads == 1) {
        InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt };
        assert(result == Status::Ok);
    };

    int found_counter = 0;
    for (auto _ : state) {
        db_->StartSession();

        found_counter = 0;
        for (uint64_t key = start_idx; key < end_idx; ++key) {
            if (key % kRefreshInterval == 0) {
                db_->Refresh();
                if (key % kCompletePendingInterval == 0) {
                    db_->CompletePending(false);
                }
            }

            ValueType result;
            ReadContext context{key, &result};
            found_counter += db_->Read(context, callback, key) == FASTER::core::Status::Ok;
        }

        db_->Refresh();
        db_->CompletePending(true);
        db_->StopSession();
    }

    log_find_count(state, found_counter, num_finds_per_thread);
}

BENCHMARK_REGISTER_F(DramMapFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(DramMapFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(DramMapFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(HybridMapFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(HybridMapFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(HybridMapFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(RocksDbFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(RocksDbFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(RocksDbFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(FasterFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(FasterFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(FasterFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(ViperFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, 1);
//    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(ViperFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_INSERTS})
    ->ThreadRange(1, 1);
//    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(ViperFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_FINDS})
    ->ThreadRange(1, 1);
//    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_MAIN();
