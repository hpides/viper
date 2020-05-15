#include "viper_fixture.hpp"

namespace viper::kv_bm {

void ViperFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (viper_initialized_ && !re_init) {
        return;
    }

    viper_ = std::make_unique<ViperT>(pmem_pool_);

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        viper_->put(key, key);
    }
    viper_initialized_ = true;
}

void ViperFixture::DeInitMap() {
    BaseFixture::DeInitMap();
    viper_ = nullptr;
    viper_initialized_ = false;
}

void ViperFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        const ValueType value = key * 100;
        viper_->put(key, value);
    }
}

void ViperFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t ViperFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        ViperT::ConstAccessor result;
        const bool found = viper_->get(key, result);
        found_counter += found && (result->data[0] == key);
    }
    return found_counter;
}

}  // namespace viper::kv_bm