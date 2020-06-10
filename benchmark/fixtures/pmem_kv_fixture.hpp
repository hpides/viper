#pragma once

#include "common_fixture.hpp"
#include <libpmemkv.hpp>

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

class PmemKVFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    void insert_empty(uint64_t start_idx, uint64_t end_idx) final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    std::unique_ptr<pmem::kv::db> pmem_db_;
    std::string pool_file_;
    bool db_initialized_ = false;
};

}  // namespace kv_bm
}  // namespace viper