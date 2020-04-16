#include <iostream>
#include <string>
#include <filesystem>
#include <mutex>
#include "benchmark.hpp"

#include <benchmark/benchmark.h>
#include <tbb/concurrent_hash_map.h>
#include <libpmempool.h>

static constexpr char POOL_FILE_DIR[] = "/mnt/nvrams1/kv-bm";
//static const uint64_t BM_POOL_SIZE = static_cast<uint64_t>(1024u*1024u*1024u) * 5u;  // 5GB
static const uint64_t BM_POOL_SIZE = (1024l*1024*1024) * 5;  // 5GB

static constexpr uint64_t NUM_INSERTS = 10000000;
static constexpr uint64_t NUM_FINDS = 1000000;
static constexpr uint64_t NUM_REPETITIONS = 1;
static constexpr uint64_t NUM_MAX_THREADS = 64;
static constexpr benchmark::TimeUnit BM_TIME_UNIT = benchmark::TimeUnit::kMicrosecond;


using DramMapType = tbb::concurrent_hash_map<KeyType, ValueType>;
using PmemMapType = pmem::obj::concurrent_hash_map<KeyType, ValueType>;

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

  protected:
    std::default_random_engine rnd_engine_;
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

class PmemMapFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) {
        BaseFixture::SetUp(state);
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

        {
            std::scoped_lock lock(pool_mutex_);
            if (pool_file_.empty()) {
                pool_file_ = random_pmem_pool_file(POOL_FILE_DIR);
                pmem_pool_ = pmem::obj::pool<PmemMapRoot>::create(pool_file_, "", BM_POOL_SIZE, S_IRWXU);
            }
        }

    }

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

    void DeInitMap() {
        pmem_pool_.close();
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

  protected:
    struct PmemMapRoot {
        persistent_ptr<PmemMapType> pmem_map;
    };

    persistent_ptr<PmemMapType> pmem_map_;
    pmem::obj::pool<PmemMapRoot> pmem_pool_;
    std::filesystem::path pool_file_;
    std::mutex pool_mutex_;
    bool map_initialized_ = false;
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

    state.counters["found"] = found_counter;
    if (found_counter != num_finds_per_thread) {
        std::cout << "DID NOT FIND ALL ENTRIES (" + std::to_string(found_counter)
                     + "/" + std::to_string(num_finds_per_thread) + ")\n";
    }
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

    state.counters["found"] = found_counter;
    if (found_counter != num_finds_per_thread) {
        std::cout << "DID NOT FIND ALL ENTRIES (" + std::to_string(found_counter)
            + "/" + std::to_string(num_finds_per_thread) + ")\n";
    }
}


BENCHMARK_REGISTER_F(DramMapFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(DramMapFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({/*prefill=*/NUM_INSERTS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(DramMapFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({/*prefill=*/NUM_INSERTS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, insert_empty)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Arg(NUM_INSERTS)
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_insert)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({/*prefill=*/NUM_INSERTS, NUM_INSERTS})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_find)
    ->Repetitions(NUM_REPETITIONS)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({/*prefill=*/NUM_INSERTS, NUM_FINDS})
    ->ThreadRange(1, NUM_MAX_THREADS);

// 1mio: 203.4M
// 2mio: 405.7M
// 3mio: 688.0M
// 4mio: 810.1M
// 5mio: 1.2G
//10mio: 2.4G

static void BM_dram_map_pmem_data_map(benchmark::State& state) {
    Benchmark bm{std::string(POOL_FILE_DIR) + "/dram_map_pmem_data.file"};
    auto pmem_pool = bm.get_pmem_pool();
    auto root = pmem_pool.root();

    for (auto _ : state) {
        state.PauseTiming();
        pmem::obj::transaction::run(pmem_pool, [&] {
            root->kv_data = pmem::obj::make_persistent<pmem::obj::vector<ValueType>>();
        });
        state.ResumeTiming();

        bm.run_dram_map_pmemdata_insert_only(NUM_INSERTS);
    }
}
//BENCHMARK(BM_dram_map_pmem_data_map)
//    ->Iterations(NUM_REPETITIONS)
//    ->Unit(benchmark::TimeUnit::kMillisecond);

static void BM_random_pmem_vector(benchmark::State& state) {
    Benchmark bm{std::string(POOL_FILE_DIR) + "/pmem_vector_seq.file"};
    auto pmem_pool = bm.get_pmem_pool();
    auto root = pmem_pool.root();

    pmem::obj::transaction::run(pmem_pool, [&] {
        root->kv_data = pmem::obj::make_persistent<pmem::obj::vector<ValueType>>(NUM_INSERTS, 0);
    });

    auto vector = root->kv_data;

    for (auto _ : state) {
        // This rand takes about 6ns per iteration.
        uint64_t pos = std::rand() % NUM_INSERTS;
        vector->at(pos) = 10;
    }
}
//BENCHMARK(BM_random_pmem_vector)->Iterations(NUM_INSERTS);

static void BM_sequential_pmem_vector(benchmark::State& state) {
    Benchmark bm{std::string(POOL_FILE_DIR) + "/pmem_vector_seq.file"};
    auto pmem_pool = bm.get_pmem_pool();
    auto root = pmem_pool.root();

    pmem::obj::transaction::run(pmem_pool, [&] {
        root->kv_data = pmem::obj::make_persistent<pmem::obj::vector<ValueType>>(NUM_INSERTS, 0);
    });

    auto vector = root->kv_data;

    int counter = 0;
    for (auto _ : state) {
        vector->at(counter++) = 10;
    }
}
//BENCHMARK(BM_sequential_pmem_vector)->Iterations(NUM_INSERTS);


BENCHMARK_MAIN();
