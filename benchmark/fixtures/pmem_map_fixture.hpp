#pragma once

#include "common_fixture.hpp"
#include <libpmemobj++/container/concurrent_hash_map.hpp>

namespace std {
    template <>
    struct hash<viper::kv_bm::BMKeyFixed> {
        size_t operator()(const viper::kv_bm::BMKeyFixed& key) {
            return std::hash<uint64_t>()(key.uuid[0]);
        }
    };
}

namespace viper {
namespace kv_bm {

using PmemMapType = pmem::obj::concurrent_hash_map<KeyType, ValueType>;

struct PmemMapRoot {
    persistent_ptr<PmemMapType> pmem_map;
};

class PmemMapFixture : public BasePmemFixture<PmemMapRoot> {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    void insert_empty(uint64_t start_idx, uint64_t end_idx) final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    persistent_ptr<PmemMapType> pmem_map_;
    bool map_initialized_ = false;
};

}  // namespace kv_bm
}  // namespace viper