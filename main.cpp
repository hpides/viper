#include <iostream>
#include <string>
#include "benchmark.hpp"


int main() {
    const std::string pool_file = "/mnt/nvrams1/bm_pool.file";

    const uint64_t num_runs = 10;
    const uint64_t num_inserts = 100000;

    long total_pmem_duration = 0;
    long total_dram_duration = 0;

    Benchmark benchmark{pool_file};
    for (uint64_t i = 0; i < num_runs; ++i) {
        auto [pmem_duration, dram_duration] = benchmark.run(num_inserts);
        total_pmem_duration += pmem_duration;
        total_dram_duration += dram_duration;
    }

    std::cout << "PMEM TOTAL: " << total_pmem_duration << " µs; AVG: " << (total_pmem_duration / num_runs) << std::endl;
    std::cout << "DRAM TOTAL: " << total_dram_duration << " µs; AVG: " << (total_dram_duration / num_runs) << std::endl;
}
