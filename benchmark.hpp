#pragma once

#include <iostream>
#include <random>
#include <libpmemkv.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/vector.hpp>

using pmem::obj::p;
using pmem::obj::persistent_ptr;

using pmem_map_t = pmem::obj::concurrent_hash_map<uint64_t, uint64_t>;

using KeyType = uint64_t;
using ValueType = uint64_t;
using Offset = uint64_t;

struct BenchmarkRoot {
    persistent_ptr<pmem_map_t> pmem_map;
    persistent_ptr<PMEMoid> pmem_kv_oid;
    persistent_ptr<pmem::obj::vector<ValueType>> kv_data;
};

class Benchmark {
    static constexpr uint64_t POOL_SIZE = 1024*1024*1024;  // 1GB

  public:
    explicit Benchmark();
    explicit Benchmark(const std::string& pool_file);

    ~Benchmark();

    void run_pmem_insert_only(const uint64_t num_inserts);
    void run_dram_insert_only(const uint64_t num_inserts);
    void run_pmemkv_insert_only(const uint64_t num_inserts);
    void run_dram_map_pmemdata_insert_only(const uint64_t num_inserts);

    long run_pmem_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds);
    long run_dram_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds);
    long run_pmemkv_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds);
    long run_dram_map_pmemdata_setup_and_find(const uint64_t num_inserts, const uint64_t num_finds);

    const pmem::obj::pool<BenchmarkRoot>& get_pmem_pool() const;

  private:
    pmem::obj::pool<BenchmarkRoot> pmem_pool_;
    std::unique_ptr<pmem::kv::db> pmem_kv_;
    std::default_random_engine rnd_engine_;
};


