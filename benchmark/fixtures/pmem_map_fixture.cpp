#include "pmem_map_fixture.hpp"

void viper::kv_bm::PmemMapFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    pmem::obj::transaction::run(pmem_pool_, [&] {
        pmem_pool_.root()->pmem_map = pmem::obj::make_persistent<PmemMapType>();
    });
    pmem_map_ = pmem_pool_.root()->pmem_map;
    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        PmemMapType::accessor result;
        pmem_map_->insert(result, key);
        result->second = key;
    }
    map_initialized_ = true;
}

void viper::kv_bm::PmemMapFixture::DeInitMap() {
    map_initialized_ = false;
}

void viper::kv_bm::PmemMapFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    for (uint64_t i = start_idx; i < end_idx; ++i) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        uint64_t key = i;
        PmemMapType::accessor result;
        pmem_map_->insert(result, key);
        result->second = key*100;
    }
}

void viper::kv_bm::PmemMapFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}
uint64_t viper::kv_bm::PmemMapFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        PmemMapType::const_accessor result;
        const bool found = pmem_map_->find(result, key);
        found_counter += found && (result->second == key);
    }
    return found_counter;
}
