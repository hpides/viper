#pragma once

#include "common_fixture.hpp"
#include "hybrid_map_fixture.hpp"
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemlog.h>
#include <tbb/concurrent_hash_map.h>

namespace viper::kv_bm {


class PmemLogFixture : public BaseFixture {
  public:
    void SetUp(benchmark::State& state) override;

    void InitMap(uint64_t num_prefill_inserts = 0, bool re_init = true) override;

    void DeInitMap() override;

    void insert_empty(uint64_t start_idx, uint64_t end_idx) final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    PMEMlogpool* pmem_log_pool_;
    std::filesystem::path pool_file_;
    std::mutex pool_mutex_;

    std::unique_ptr<HybridMapType> map_;
    bool map_initialized_ = false;
};

}  // namespace viper::kv_bm


