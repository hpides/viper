#include <iostream>
#include <string>
#include <unistd.h>
#include <stdio.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmempool.h>
#include "benchmark.hpp"

using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::make_persistent;
using pmem::obj::transaction;
using pmem::obj::pool;
using namespace std;

#define PMEMPOOLSIZE 1024*1024*1024
#define DATASIZE 32
#define LOG(msg) std::cout << msg << std::endl

struct MEMBLOCK{
    persistent_ptr<MEMBLOCK> next;
    persistent_ptr<char[]> value_ptr;
    p<size_t> size;

    MEMBLOCK(persistent_ptr<MEMBLOCK> next_block, persistent_ptr<char[]> value_ptr_in, size_t size_in) {
        next = std::move(next_block);
        value_ptr = std::move(value_ptr_in);
        size = size_in;
    }
};


int main() {
    const std::string pool_file = "/mnt/nvrams1/bm_pool.file";

    const uint64_t num_runs = 100;
    const uint64_t num_inserts = 100000;

    long total_pmem_duration = 0;
    long total_dram_duration = 0;

    Benchmark benchmark{pool_file};
    for (int i = 0; i < num_runs; ++i) {
        auto [pmem_duration, dram_duration] = benchmark.run(num_inserts);
        total_pmem_duration += pmem_duration;
        total_dram_duration += dram_duration;
    }

    std::cout << "PMEM TOTAL: " << total_pmem_duration << " µs; AVG: " << (total_pmem_duration / num_runs) << std::endl;
    std::cout << "DRAM TOTAL: " << total_dram_duration << " µs; AVG: " << (total_dram_duration / num_runs) << std::endl;
}

//int main() {
//    const size_t size = PMEMPOOLSIZE;
//    string path = "/mnt/nvrams1/poolset.file";
//    pool<MEMBLOCK> pmpool;
//
//    int sds_write_value = 0;
//    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
//
//    /* Open and create in persistent memory device */
//    if (access(path.c_str(), F_OK) != 0) {
//        LOG("Creating filesystem pool, path=" << path << ", size=" << to_string(size));
//        pmpool = pool<MEMBLOCK>::create(path, "", size, S_IRWXU);
//    } else {
//        LOG("Opening pool, path=" << path);
//        pmpool = pool<MEMBLOCK>::open(path, "");
//    }
//
//    /* Get root persistent_ptr,
//    the first struct pointer
//    and link to the next one */
//    const auto root = pmpool.root();
//    auto block_ptr = root;
//
//    LOG("root: " << root->size);
//
//    const int num_blocks = 5;
//    block_ptr->size = 111;
//    pmpool.persist(block_ptr->size);
//
//    for (int i = 1; i <= num_blocks; ++i) {
//        persistent_ptr<MEMBLOCK> next_block;
//        pmem::obj::make_persistent_atomic<MEMBLOCK>(pmpool, next_block, nullptr, nullptr, 10*i);
//        LOG("Adding Block #" << i*10);
//        block_ptr->next = next_block;
//        pmpool.persist(block_ptr->next);
//        block_ptr = next_block;
//    }
//
//    LOG("root: " << root->size);
//
//    block_ptr = root;
//    while (block_ptr->next != nullptr) {
//        LOG("Block #" << block_ptr->size);
//        block_ptr = block_ptr->next;
//    }
//
//    pmpool.close();
//}