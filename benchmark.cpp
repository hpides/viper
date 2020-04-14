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

Benchmark::Benchmark(const std::string& pool_file) {
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    /* Open and create in persistent memory device */
    if (access(pool_file.c_str(), F_OK) != 0) {
        std::cout << "Creating filesystem pool, path=" << pool_file << ", size=" << POOL_SIZE << std::endl;
        pmem_pool_ = pmem::obj::pool<BenchmarkRoot>::create(pool_file, "", POOL_SIZE, S_IRWXU);
    } else {
        std::cout << "Opening pool, path=" << pool_file << std::endl;
        pmem_pool_ = pmem::obj::pool<BenchmarkRoot>::open(pool_file, "");
    }

    auto root = pmem_pool_.root();
    if (root->pmem_map == nullptr) {
        pmem::obj::transaction::run(pmem_pool_, [&] {
            root->pmem_map = pmem::obj::make_persistent<pmem_map_t>();
            root->pmem_kv_oid = pmem::obj::make_persistent<PMEMoid>(OID_NULL);
            root->kv_data_ = pmem::obj::make_persistent<pmem::obj::vector<ValueType>>();
        });
    } else {
        root->pmem_map->runtime_initialize();
        root->pmem_map->clear();
    }

    std::random_device rnd;
    rnd_engine_ = std::default_random_engine(rnd());
}

Benchmark::~Benchmark() {
    if (pmem_kv_ != nullptr) {
        pmem_kv_->close();
    }
    if (pmem_pool_.root()->kv_data_ != nullptr) {
        pmem_pool_.root()->kv_data_->clear();
        pmem_pool_.root()->kv_data_->resize(0);
    }
    pmem_pool_.close();
}

std::tuple<long, long, long, long> Benchmark::run_insert_only(const uint64_t num_inserts) {
    const long pmem_map_bm_duration = run_pmem_insert_only(num_inserts);
    const long dram_map_bm_duration = run_dram_insert_only(num_inserts);
    const long pmemkv_map_bm_duration = run_pmemkv_insert_only(num_inserts);
    const long dram_map_pmemdata_bm_duration = run_dram_map_pmemdata_insert_only(num_inserts);

    return {pmem_map_bm_duration, dram_map_bm_duration, pmemkv_map_bm_duration, dram_map_pmemdata_bm_duration};
}

std::tuple<long, long, long, long> Benchmark::run_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
    const long pmem_map_bm_duration = run_pmem_setup_and_find(num_inserts, num_finds);
    const long dram_map_bm_duration = run_dram_setup_and_find(num_inserts, num_finds);
    const long pmemkv_map_bm_duration = run_pmemkv_setup_and_find(num_inserts, num_finds);
    const long dram_map_pmemdata_bm_duration = run_dram_map_pmemdata_setup_and_find(num_inserts, num_finds);

    return {pmem_map_bm_duration, dram_map_bm_duration, pmemkv_map_bm_duration, dram_map_pmemdata_bm_duration};
}


long Benchmark::run_dram_insert_only(const uint64_t num_inserts) {
//    std::unordered_map<uint64_t, uint64_t> dram_map;
    tbb::concurrent_hash_map<KeyType, ValueType> dram_map{};
    const auto dram_map_bm_start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < num_inserts; ++i) {
        dram_map.insert(std::pair{i, 100 * i});
    }

    const auto dram_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto dram_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(dram_map_bm_end - dram_map_bm_start);
    return dram_map_bm_duration.count();
}

long Benchmark::run_dram_map_pmemdata_insert_only(const uint64_t num_inserts) {
//    std::unordered_map<uint64_t, uint64_t> dram_map;
    tbb::concurrent_hash_map<KeyType, Offset> dram_map{};
    persistent_ptr<pmem::obj::vector<ValueType>> pmem_data = pmem_pool_.root()->kv_data_;
//    pmem_data->reserve(num_inserts);
    const auto dram_map_bm_start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < num_inserts; ++i) {
        const uint64_t pos = pmem_data->size();
        pmem_data->push_back(100 * i);
        dram_map.insert(std::pair{i, pos});
    }

    const auto dram_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto dram_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(dram_map_bm_end - dram_map_bm_start);
    return dram_map_bm_duration.count();
}

long Benchmark::run_pmem_insert_only(const uint64_t num_inserts) {

    persistent_ptr<pmem_map_t> pmem_map = pmem_pool_.root()->pmem_map;

    pmem_map->clear();
    pmem_map->defragment();

    const auto pmem_map_bm_start = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < num_inserts; ++i) {
        pmem_map->insert(pmem_map_t::value_type(i, 100 * i));
    }

    const auto pmem_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto pmem_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(pmem_map_bm_end - pmem_map_bm_start);
    return pmem_map_bm_duration.count();
}

long Benchmark::run_pmemkv_insert_only(const uint64_t num_inserts) {
    pmem::kv::config kv_config;
    kv_config.put_object("oid", &(pmem_pool_.root()->pmem_kv_oid), nullptr);

    pmem_kv_ = std::make_unique<pmem::kv::db>();
    std::cout << "pmemkv" << std::endl;

//    pmem::kv::status ok = pmem_kv_->open("cmap", std::move(kv_config));
//    if (ok != pmem::kv::status::OK) {
//        throw std::runtime_error("OPEN NOT OKAY " + std::to_string(static_cast<int>(ok)));
//    }
//    std::cout << "pmemkv open" << std::endl;
//
    const auto pmem_map_bm_start = std::chrono::high_resolution_clock::now();
//
//    for (uint64_t i = 0; i < num_inserts; ++i) {
//        ok = pmem_kv_->put(std::to_string(i), std::to_string(100 * i));
//        if (ok != pmem::kv::status::OK) {
//            std::cout << "INSERT NOT OKAY" << std::endl;
//        }
//    }
//    std::cout << "pmemkv inserted" << std::endl;

    const auto pmem_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto pmem_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(pmem_map_bm_end - pmem_map_bm_start);
    return pmem_map_bm_duration.count();
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
    persistent_ptr<pmem::obj::vector<ValueType>> pmem_data = pmem_pool_.root()->kv_data_;
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
    return dram_map_bm_duration.count();
}

long Benchmark::run_pmemkv_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds) {
    return 0;
}

