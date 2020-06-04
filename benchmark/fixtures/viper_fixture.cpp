#include "viper_fixture.hpp"

namespace viper::kv_bm {

void ViperFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (viper_initialized_ && !re_init) {
        return;
    }

    pool_file_ = VIPER_POOL_FILE;
    viper_ = std::make_unique<ViperT>(pool_file_, BM_POOL_SIZE);
    auto v_client = viper_->get_client();

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        v_client.put(key, key);
    }
    viper_initialized_ = true;
}

void ViperFixture::DeInitMap() {
    BaseFixture::DeInitMap();
    viper_ = nullptr;
    viper_initialized_ = false;
//    pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL);
}

void ViperFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    auto v_client = viper_->get_client();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        const ValueType value = key * 100;
        v_client.put(key, value);
    }
}

void ViperFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t ViperFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    const auto v_client = viper_->get_const_client();
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        ViperT::ConstAccessor result;
        const bool found = v_client.get(key, result);
        found_counter += found && (result->data[0] == key);
    }
    return found_counter;
}

uint64_t ViperFixture::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    auto v_client = viper_->get_client();
    uint64_t delete_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        delete_counter += v_client.remove(key);
    }
    return delete_counter;
}

}  // namespace viper::kv_bm