#include <iostream>
#include <string>
#include "benchmark.hpp"


int main() {
    const std::string pool_file = "/mnt/nvrams1/bm_pool.file";
    Benchmark benchmark{pool_file};

    const uint64_t num_runs = 1;
    const uint64_t num_inserts = 1000000;
    const uint64_t num_finds = 500000;

    // INSERT ONLY
    long pmem_insert_duration = 0;
    long dram_insert_duration = 0;
    long pmemkv_insert_duration = 0;
    long dram_map_pmemdata_insert_duration = 0;

    for (uint64_t i = 0; i < num_runs; ++i) {
        auto [pmem_duration, dram_duration, pmemkv_duration, dram_map_pmemdata_duration] = benchmark.run_insert_only(num_inserts);
        pmem_insert_duration += pmem_duration;
        dram_insert_duration += dram_duration;
        pmemkv_insert_duration += pmemkv_duration;
        dram_map_pmemdata_insert_duration += dram_map_pmemdata_duration;
    }

    std::cout << "PMEM INSERT AVG: " << (pmem_insert_duration / num_runs) << std::endl;
    std::cout << "DRAM INSERT AVG: " << (dram_insert_duration / num_runs) << std::endl;
    std::cout << "PMEMKV INSERT AVG: " << (pmemkv_insert_duration / num_runs) << std::endl;
    std::cout << "DRAM MAP PMEMDATA INSERT AVG: " << (dram_map_pmemdata_insert_duration / num_runs) << std::endl;

    // SETUP AND FIND
    long pmem_find_duration = 0;
    long dram_find_duration = 0;
    long pmemkv_find_duration = 0;
    long dram_map_pmemdata_find_duration = 0;

    for (uint64_t i = 0; i < num_runs; ++i) {
        auto [pmem_duration, dram_duration, pmemkv_duration, dram_map_pmemdata_duration] = benchmark.run_setup_and_find(num_inserts, num_finds);
        pmem_find_duration += pmem_duration;
        dram_find_duration += dram_duration;
        pmemkv_find_duration += pmemkv_duration;
        dram_map_pmemdata_find_duration += dram_map_pmemdata_duration;
    }

    std::cout << "PMEM FIND AVG: " << (pmem_find_duration / num_runs) << std::endl;
    std::cout << "DRAM FIND AVG: " << (dram_find_duration / num_runs) << std::endl;
    std::cout << "PMEMKV FIND AVG: " << (pmemkv_find_duration / num_runs) << std::endl;
    std::cout << "DRAM MAP PMEMDATA FIND AVG: " << (dram_map_pmemdata_find_duration / num_runs) << std::endl;
}
