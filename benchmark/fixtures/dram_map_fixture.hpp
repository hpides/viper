#pragma once

#include "common_fixture.hpp"
#include "../benchmark.hpp"
#include <tbb/concurrent_hash_map.h>

namespace viper {
namespace kv_bm {

using DramMapType = tbb::concurrent_hash_map<KeyType, ValueType, TbbFixedKeyCompare>;

class DramMapFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true);

    void insert_empty(const uint64_t start_idx, const uint64_t end_idx) override final;

    void setup_and_insert(const uint64_t start_idx, const uint64_t end_idx) override final;

    uint64_t setup_and_find(const uint64_t start_idx, const uint64_t end_idx) override final;

  protected:
    std::unique_ptr<DramMapType> dram_map_;
    bool map_initialized_ = false;

};

}  // namespace kv_bm
}  // namespace viper