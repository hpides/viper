#pragma once

#include "../benchmark.hpp"
#include "common_fixture.hpp"
#include "viper.hpp"

namespace viper {
namespace kv_bm {

class ViperFixture : public BasePmemFixture<viper::ViperRoot<KeyType, ValueType>> {
    using ViperRoot = viper::ViperRoot<KeyType, ValueType>;
    using ViperT = viper::Viper<KeyType, ValueType>;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void insert_empty(uint64_t start_idx, uint64_t end_idx) final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    std::unique_ptr<ViperT> viper_;
    bool viper_initialized_ = false;
};

}  // namespace kv_bm
}  // namespace viper