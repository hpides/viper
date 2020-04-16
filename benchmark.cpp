#include "benchmark.hpp"

#include <unistd.h>

#include <libpmempool.h>
#include <libpmemobj.h>
#include <sys/stat.h>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemkv.hpp>
#include <tbb/concurrent_hash_map.h>
#include <random>

Benchmark::Benchmark() {
    std::random_device rnd{};
    rnd_engine_ = std::default_random_engine(rnd());
}

Benchmark::Benchmark(const std::string& pool_file) : Benchmark() {
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    if (access(pool_file.c_str(), F_OK) == 0) {
        if (pmempool_rm(pool_file.c_str(), PMEMPOOL_RM_FORCE) == -1) {
            std::cout << pmempool_errormsg() << std::endl;
        }
    }

//    std::cout << "Creating filesystem pool, path=" << pool_file << ", size=" << POOL_SIZE << std::endl;
//    pmem_pool_ = pmem::obj::pool<BenchmarkRoot>::create(pool_file, "", POOL_SIZE, S_IRWXU);
}

Benchmark::~Benchmark() {
//    if (pmem_kv_ != nullptr) {
//        pmem_kv_->close();
//    }
    if (pmem_pool_.handle() != nullptr) {
        pmem_pool_.close();
    }
}



void Benchmark::run_dram_insert_only(const uint64_t num_inserts) {
    tbb::concurrent_hash_map<KeyType, ValueType> dram_map{};

    for (uint64_t i = 0; i < num_inserts; ++i) {
        dram_map.insert(std::pair{i, 100 * i});
    }
}

void Benchmark::run_dram_map_pmemdata_insert_only(const uint64_t num_inserts) {
//    std::unordered_map<uint64_t, uint64_t> dram_map;
    tbb::concurrent_hash_map<KeyType, Offset> dram_map{};
    persistent_ptr<pmem::obj::vector<ValueType>> pmem_data = pmem_pool_.root()->kv_data;
//    pmem_data->reserve(num_inserts);

    for (uint64_t i = 0; i < num_inserts; ++i) {
        const uint64_t pos = pmem_data->size();
        pmem_data->push_back(100 * i);
        dram_map.insert(std::pair{i, pos});
    };
}

void Benchmark::run_pmem_insert_only(const uint64_t num_inserts) {
    persistent_ptr<pmem_map_t> pmem_map = pmem_pool_.root()->pmem_map;

    for (uint64_t i = 0; i < num_inserts; ++i) {
        pmem_map->insert(pmem_map_t::value_type(i, 100 * i));
    }
}

void Benchmark::run_pmemkv_insert_only(const uint64_t num_inserts) {
    pmem::kv::config kv_config;
    kv_config.put_object("oid", &(pmem_pool_.root()->pmem_kv_oid), nullptr);

    pmem_kv_ = std::make_unique<pmem::kv::db>();
//    std::cout << "pmemkv" << std::endl;

//    pmem::kv::status ok = pmem_kv_->open("cmap", std::move(kv_config));
//    if (ok != pmem::kv::status::OK) {
//        throw std::runtime_error("OPEN NOT OKAY " + std::to_string(static_cast<int>(ok)));
//    }
//    std::cout << "pmemkv open" << std::endl;
//
//
//    for (uint64_t i = 0; i < num_inserts; ++i) {
//        ok = pmem_kv_->put(std::to_string(i), std::to_string(100 * i));
//        if (ok != pmem::kv::status::OK) {
//            std::cout << "INSERT NOT OKAY" << std::endl;
//        }
//    }
//    std::cout << "pmemkv inserted" << std::endl;

}

long Benchmark::run_dram_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
    tbb::concurrent_hash_map<KeyType, ValueType> dram_map{};

    for (uint64_t i = 0; i < num_inserts; ++i) {
        dram_map.insert(std::pair{i, 100 * i});
    }

    std::srand(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_inserts - 1);

    const auto dram_map_bm_start = std::chrono::high_resolution_clock::now();
    uint64_t counter = 0;

    for (uint64_t j = 0; j < num_finds; ++j) {
        tbb::concurrent_hash_map<KeyType, ValueType>::accessor result;
        dram_map.find(result, uniform_distribution(rnd_engine_));
        counter += result->second;
    }

    const auto dram_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto dram_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(dram_map_bm_end - dram_map_bm_start);
    std::cout << "Counter: " << counter << std::endl;
    return dram_map_bm_duration.count();
}
long Benchmark::run_pmem_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
    persistent_ptr<pmem_map_t> pmem_map = pmem_pool_.root()->pmem_map;

    pmem_map->clear();
    pmem_map->defragment();

    for (uint64_t i = 0; i < num_inserts; ++i) {
        pmem_map->insert(pmem_map_t::value_type(i, 100 * i));
    }

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_inserts - 1);

    const auto pmem_map_bm_start = std::chrono::high_resolution_clock::now();
    uint64_t counter = 0;

    for (uint64_t j = 0; j < num_finds; ++j) {
        pmem_map_t::accessor result;
        pmem_map->find(result, uniform_distribution(rnd_engine_));
        counter += result->second;
    }

    const auto pmem_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto pmem_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(pmem_map_bm_end - pmem_map_bm_start);
    std::cout << "Counter: " << counter << std::endl;
    return pmem_map_bm_duration.count();
}

long Benchmark::run_dram_map_pmemdata_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
//    std::unordered_map<uint64_t, uint64_t> dram_map;
    tbb::concurrent_hash_map<KeyType, Offset> dram_map{};
    persistent_ptr<pmem::obj::vector<ValueType>> pmem_data = pmem_pool_.root()->kv_data;
    pmem_data->clear();
    pmem_data->resize(0);

    for (uint64_t i = 0; i < num_inserts; ++i) {
        const uint64_t pos = pmem_data->size();
        pmem_data->push_back(100 * i);
        dram_map.insert(std::pair{i, pos});
    }

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_inserts - 1);

    const auto dram_map_bm_start = std::chrono::high_resolution_clock::now();
    uint64_t counter = 0;

    for (uint64_t j = 0; j < num_finds; ++j) {
        tbb::concurrent_hash_map<KeyType, ValueType>::accessor result;
        dram_map.find(result, uniform_distribution(rnd_engine_));
        counter += (*pmem_data)[result->second];
    }

    const auto dram_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto dram_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(dram_map_bm_end - dram_map_bm_start);
    std::cout << "Counter: " << counter << std::endl;
    return dram_map_bm_duration.count();
}

long Benchmark::run_pmemkv_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
    return 0;
}

const pmem::obj::pool<BenchmarkRoot>& Benchmark::get_pmem_pool() const {
    return pmem_pool_;
}
