#pragma once

#include <iostream>
#include <string>
#include <filesystem>
#include <random>
#include <mutex>

#include <benchmark/benchmark.h>
#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>

#include "../benchmark.hpp"

namespace viper {
namespace kv_bm {

static constexpr std::array CPUS {
    0, 1, 2, 5, 6, 9, 10, 14, 15,  // NUMA NODE 1
    3, 4, 7, 8, 11, 12, 13, 16, 17,  // NUMA NODE 2
    36, 37, 38, 41, 42, 45, 46, 50, 51,  // NUMA NODE 1
    39, 40, 43, 44, 47, 48, 49, 52, 53  // NUMA NODE 2
};

bool is_init_thread(const benchmark::State& state);

void set_cpu_affinity();
void set_cpu_affinity(uint16_t thread_idx);

std::string random_file(const std::filesystem::path& base_dir);

class BaseFixture : public benchmark::Fixture {
  public:
    void SetUp(benchmark::State& state) override {
        std::random_device rnd{};
        rnd_engine_ = std::default_random_engine(rnd());
    }

    void TearDown(benchmark::State& state) override {};

    virtual void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) {};
    virtual void DeInitMap() {};

    // Benchmark methods. All pure virtual.
    virtual void insert_empty(uint64_t start_idx, uint64_t end_idx) = 0;
    virtual void setup_and_insert(uint64_t start_idx, uint64_t end_idx) = 0;
    virtual uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) = 0;

    static void log_find_count(benchmark::State& state, const uint64_t num_found, const uint64_t num_expected);

  protected:
    std::default_random_engine rnd_engine_;
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
                pool_file_ = random_file(POOL_FILE_DIR);
                std::cout << "Working on NVM file " << pool_file_ << std::endl;
                pmem_pool_ = pmem::obj::pool<RootType>::create(pool_file_, "", BM_POOL_SIZE, S_IRWXU);
            }
        }
    }

//    this is called and pool is closed but viper still points to something
//    void TearDown(benchmark::State& state) override {
//        {
//            std::scoped_lock lock(pool_mutex_);
//            if (!pool_file_.empty() && std::filesystem::exists(pool_file_)) {
//                pmem_pool_.close();
//                if (pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL) == -1) {
//                    std::cout << pmempool_errormsg() << std::endl;
//                }
//                pool_file_.clear();
//            }
//        }
//    }

  protected:
    pmem::obj::pool<RootType> pmem_pool_;
    std::filesystem::path pool_file_;
    std::mutex pool_mutex_;
};

class FileBasedFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) override {
        BaseFixture::SetUp(state);
        {
            std::scoped_lock lock(db_mutex_);
            if (db_file_.empty()) {
                db_file_ = random_file(DB_FILE_DIR);
            }
        }
    }

    void TearDown(benchmark::State& state) override {
        {
            std::scoped_lock lock(db_mutex_);
            if (!db_file_.empty() && std::filesystem::exists(db_file_)) {
                std::filesystem::remove_all(db_file_);
                db_file_.clear();
            }
        }
    }

  protected:
    std::filesystem::path db_file_;
    std::mutex db_mutex_;
};

}  // namespace kv_bm
}  // namespace viper