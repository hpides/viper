#include "viper_fixture.hpp"

void viper::kv_bm::ViperFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (viper_initialized_ && !re_init) {
        return;
    }

    pmem::obj::transaction::run(pmem_pool_, [&] {
        pmem_pool_.root()->create_new_block();
    });
    viper_ = std::make_unique<viper::Viper<KeyType, ValueType>>(pmem_pool_);

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        viper_->put(key, key);
    }
    viper_initialized_ = true;
}
void viper::kv_bm::ViperFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        const ValueType value = key*100;
        viper_->put(key, value);
    }
}
void viper::kv_bm::ViperFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}
uint64_t viper::kv_bm::ViperFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        found_counter += (viper_->get(key) == key);
    }
    return found_counter;
}
