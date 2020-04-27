#pragma once

#include "common_fixture.hpp"
#include "../benchmark.hpp"
#include <tbb/concurrent_hash_map.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>
#include <libpmem.h>

namespace viper {
namespace kv_bm {

using HybridMapType = tbb::concurrent_hash_map<KeyType, Offset>;
using HybridVectorType = pmem::obj::array<ValueType, MAX_DATA_SIZE>;

struct HybridMapRoot {
    persistent_ptr<HybridVectorType> data;
};

class HybridMapFixture : public BasePmemFixture<HybridMapRoot> {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    void insert_empty(const uint64_t start_idx, const uint64_t end_idx) override final;

    void setup_and_insert(const uint64_t start_idx, const uint64_t end_idx) override final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) override final;

  protected:
    std::unique_ptr<HybridMapType> map_;
    persistent_ptr<HybridVectorType> data_;
    bool map_initialized_ = false;
};

}  // namespace kv_bm
}  // namespace viper