#pragma once

#include <iostream>
#include <string>
#include <filesystem>
#include <random>
#include <mutex>

#include <benchmark/benchmark.h>
#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>
#include <tbb/tbb_stddef.h>
#include <hdr_histogram.h>

#include "../benchmark.hpp"
#include "ycsb_common.hpp"

namespace viper::kv_bm {

static constexpr std::array CPUS {
    // CPU 1
     0,  1,  2,  5,  6,  9, 10, 14, 15, // NUMA NODE 0
     3,  4,  7,  8, 11, 12, 13, 16, 17, // NUMA NODE 1
    36, 37, 38, 41, 42, 45, 46, 50, 51, // NUMA NODE 0
    39, 40, 43, 44, 47, 48, 49, 52, 53, // NUMA NODE 1
    // CPU 2
    18, 19, 20, 23, 24, 27, 28, 32, 33, // NUMA NODE 2
    21, 22, 25, 26, 29, 30, 31, 34, 35, // NUMA NODE 3
    54, 55, 56, 59, 60, 63, 64, 68, 69, // NUMA NODE 2
    57, 58, 61, 62, 65, 66, 67, 70, 71  // NUMA NODE 3
};

bool is_init_thread(const benchmark::State& state);

void set_cpu_affinity();
void set_cpu_affinity(uint16_t thread_idx);
void set_cpu_affinity(uint16_t from, uint16_t to);

std::string random_file(const std::filesystem::path& base_dir);

using VarSizeKVs = std::pair<std::vector<std::string>, std::vector<std::string>>;

void zero_block_device(const std::string& block_dev, size_t length);

template <typename KeyT>
struct TbbFixedKeyCompare {
    // Use same impl as tbb_hasher
    static const size_t hash_multiplier = tbb::internal::select_size_t_constant<2654435769U, 11400714819323198485ULL>::value;
    static size_t hash(const KeyT& a) {
        return static_cast<size_t>(a.data[0]) * hash_multiplier;
    }
    static bool equal(const KeyT& a, const KeyT& b) { return a == b; }
};

template <>
struct TbbFixedKeyCompare<std::string> {
    static size_t hash(const std::string& a) {
        return std::hash<std::string>{}(a);
    }
    static bool equal(const std::string& a, const std::string& b) { return a == b; }
};

class BaseFixture : public benchmark::Fixture {
  public:
    void SetUp(benchmark::State& state) override {
        hdr_init(1, 1000000000, 4, &hdr_);
    }
    void TearDown(benchmark::State& state) override {};

    virtual void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {};
    virtual void DeInitMap() {};

    template <typename PrefillFn>
    void prefill_internal(size_t num_prefills, PrefillFn prefill_fn);

    void prefill(size_t num_prefills);
    void prefill_ycsb(const std::vector<ycsb::Record>& data);

    void generate_strings(size_t num_strings, size_t key_size, size_t value_size);

    // Benchmark methods. All pure virtual.
    virtual uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) = 0;
    virtual uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) = 0;
    virtual uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) = 0;
    virtual uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) = 0;

    virtual uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
                              const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
        throw std::runtime_error("YCSB not implemented");
    }

    void merge_hdr(hdr_histogram* other) {
        std::lock_guard lock{hdr_lock_};
        hdr_add(hdr_, other);
    }

    hdr_histogram* get_hdr() { return hdr_; }

    static void log_find_count(benchmark::State& state, const uint64_t num_found, const uint64_t num_expected);

  protected:
    virtual uint64_t insert(uint64_t start_idx, uint64_t end_idx) = 0;

    hdr_histogram* hdr_;
    std::mutex hdr_lock_;

    VarSizeKVs var_size_kvs_{};
};

template <typename RootType>
class BasePmemFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) override {
        BaseFixture::SetUp(state);
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

        {
            std::scoped_lock lock(pool_mutex_);
            if (pool_file_.empty()) {
                pool_file_ = random_file(DB_NVM_DIR);
                // std::cout << "Working on NVM file " << pool_file_ << std::endl;
                pmem_pool_ = pmem::obj::pool<RootType>::create(pool_file_, "", BM_POOL_SIZE, S_IRWXU);
            }
        }
    }

//    this is called and pool is closed but viper still points to something
    void TearDown(benchmark::State& state) override {
        {
            std::scoped_lock lock(pool_mutex_);
            if (!pool_file_.empty() && std::filesystem::exists(pool_file_)) {
                pmem_pool_.close();
                if (pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL) == -1) {
                    std::cout << pmempool_errormsg() << std::endl;
                }
                pool_file_.clear();
            }
        }
    }

  protected:
    pmem::obj::pool<RootType> pmem_pool_;
    std::filesystem::path pool_file_;
    std::mutex pool_mutex_;
};

}  // namespace viper::kv_bm
