#pragma once

#include <iostream>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>

using pmem::obj::p;
using pmem::obj::persistent_ptr;

using pmem_map_t = pmem::obj::concurrent_hash_map<uint64_t, uint64_t>;

struct BenchmarkRoot {
    persistent_ptr<pmem_map_t> pmem_map;
};

class Benchmark {
  public:
    explicit Benchmark(const std::string& pool_file);
    ~Benchmark();

    std::pair<long, long> run(const uint64_t num_inserts);

    static constexpr uint64_t POOL_SIZE = 1024*1024*1024;  // 1GB

  protected:
    long run_pmem(const uint64_t num_inserts);
    long run_dram(const uint64_t num_inserts);


  private:
    pmem::obj::pool<BenchmarkRoot> pmem_pool_;
};


