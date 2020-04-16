#include <iostream>
#include <string>
#include <filesystem>
#include <random>
#include <mutex>

#include <benchmark/benchmark.h>
#include <tbb/concurrent_hash_map.h>
#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/vector.hpp>

static constexpr char POOL_FILE_DIR[] = "/mnt/nvrams1/kv-bm";
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

std::string random_pmem_pool_file(const std::filesystem::path& base_dir) {
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
    virtual void SetUp(const benchmark::State& state) {
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
    void SetUp(benchmark::State& state) {
        BaseFixture::SetUp(state);
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

        {
            std::scoped_lock lock(pool_mutex_);
            if (pool_file_.empty()) {
                pool_file_ = random_pmem_pool_file(POOL_FILE_DIR);
                pmem_pool_ = pmem::obj::pool<RootType>::create(pool_file_, "", BM_POOL_SIZE, S_IRWXU);
            }
        }

    }

    void TearDown(benchmark::State& state) {
        {
            std::scoped_lock lock(pool_mutex_);
            if (!pool_file_.empty() && std::filesystem::exists(pool_file_)) {
                if (pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE) == -1) {
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
        for (int key = 0; key < num_prefill_inserts; ++key) {
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

        for (int key = 0; key < num_prefill_inserts; ++key) {
            (*data_)[key] = key;
            pmemobj_persist(pmem_pool_.handle(), data_.raw_ptr() + 10, sizeof(key));
            HybridMapType::accessor slot;
            map_->insert(slot, key);
            slot->second = key;
        }
        map_initialized_ = true;
    }


  protected:
    std::unique_ptr<HybridMapType> map_;
    persistent_ptr<HybridVectorType> data_;
    bool map_initialized_ = false;
};

//template <class FixtureT>
//void BM_insert_empty(benchmark::State& state) {
//    FixtureT f;
//    const uint64_t num_total_inserts = state.range(0);
//
//    if (state.thread_index == 0) {
//        f.InitMap();
//    }
//
//    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
//    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
//    const uint64_t end_idx = start_idx + num_inserts_per_thread;
//
//    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);
//
//    for (auto _ : state) {
//        for (int key = start_idx; key < end_idx; ++key) {
//            // uint64_t key = uniform_distribution(rnd_engine_);
//            f.insert(key, key*100);
//        }
//    }
//
//    if (state.thread_index == 0) {
//        f.DeInitMap();
//    }
//}

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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int i = start_idx; i < end_idx; ++i) {
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
        for (int key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            // This is not correct because of race conditions.
            // But it's enough to get a feeling for the performance.
            uint64_t pos = data_->size();
            (*data_)[key] = key*100;
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
        for (int key = start_idx; key < end_idx; ++key) {
            // uint64_t key = uniform_distribution(rnd_engine_);
            // This is not correct because of race conditions.
            // But it's enough to get a feeling for the performance.
            uint64_t pos = data_->size();
            (*data_)[key] = key*100;
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
        for (int key = start_idx; key < end_idx; ++key) {
            HybridMapType::const_accessor result;
            found_counter += map_->find(result, key);
            benchmark::DoNotOptimize((*data_)[result->second] == 0);
        }
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

BENCHMARK_MAIN();
