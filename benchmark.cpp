#include "benchmark.hpp"

#include <unistd.h>

#include <libpmempool.h>
#include <libpmemobj.h>
#include <sys/stat.h>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>

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
        });
    } else {
        root->pmem_map->runtime_initialize();
        root->pmem_map->clear();
        root->pmem_map->defragment();
    }
}

Benchmark::~Benchmark() {
    pmem_pool_.close();
}

std::pair<long, long> Benchmark::run(const uint64_t num_inserts) {
//    std::cout << "Running PMEM BM..." << std::endl;
    persistent_ptr<pmem_map_t> pmem_map = pmem_pool_.root()->pmem_map;

    pmem_map->clear();
    pmem_map->defragment();

    const auto pmem_map_bm_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_inserts; ++i) {
        pmem_map->insert(pmem_map_t::value_type(i, 100 * i));
    }

    const auto pmem_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto pmem_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(pmem_map_bm_end - pmem_map_bm_start);

//    std::cout << "PMEM MAP DURATION: " << pmem_map_bm_duration.count() << " µs" << std::endl;


//    std::cout << "Running DRAM BM..." << std::endl;
    std::unordered_map<uint64_t, uint64_t> dram_map;
    const auto dram_map_bm_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_inserts; ++i) {
        dram_map.insert(std::pair{i, 100 * i});
    }

    const auto dram_map_bm_end = std::chrono::high_resolution_clock::now();
    const auto dram_map_bm_duration = std::chrono::duration_cast<std::chrono::microseconds>(dram_map_bm_end - dram_map_bm_start);

//    std::cout << "DRAM MAP DURATION: " << dram_map_bm_duration.count() << " µs" << std::endl;

    return {pmem_map_bm_duration.count(), dram_map_bm_duration.count()};
}