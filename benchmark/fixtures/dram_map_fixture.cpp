#include "dram_map_fixture.hpp"

void viper::kv_bm::DramMapFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }
    dram_map_ = std::make_unique<DramMapType>();
    for (uint64_t i = 0; i < num_prefill_inserts; ++i) {
        KeyType key{i};
        ValueType value{i};
        dram_map_->insert({key, value});
    }
    map_initialized_ = true;
}

void viper::kv_bm::DramMapFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        DramMapType::accessor result;
        const ValueType value = key * 100;
        const bool new_insert = dram_map_->insert(result, {key, value});
        if (!new_insert) {
            result->second = value;
        }
    }
}

void viper::kv_bm::DramMapFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t viper::kv_bm::DramMapFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        DramMapType::const_accessor result;
        const bool found = dram_map_->find(result, key);
        found_counter += found && result->second.data[0] == key;
    }
    return found_counter;
}
uint64_t viper::kv_bm::DramMapFixture::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    uint64_t delete_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        DramMapType::const_accessor result;
        delete_counter += dram_map_->erase(key);
    }
    return delete_counter;
}
