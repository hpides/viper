#include "hybrid_map_fixture.hpp"
#include <libpmemobj++/make_persistent.hpp>

void viper::kv_bm::HybridMapFixture::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    pmem::obj::transaction::run(pmem_pool_, [&] {
        pmem_pool_.root()->data = pmem::obj::make_persistent<HybridVectorType>();
    });
    map_ = std::make_unique<HybridMapType>();
    data_ = pmem_pool_.root()->data;

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        (*data_)[key] = {key, key};
        HybridMapType::accessor slot;
        map_->insert(slot, key);
        slot->second = key;
    }
    data_.persist();
    map_initialized_ = true;
}

void viper::kv_bm::HybridMapFixture::DeInitMap() {
    map_ = nullptr;
    data_ = nullptr;
    map_initialized_ = false;
}

void viper::kv_bm::HybridMapFixture::insert_empty(const uint64_t start_idx, const uint64_t end_idx) {
    const auto* data_start = data_->data();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        // This is not correct because of race conditions.
        // But it's enough to get a feeling for the performance.
        std::pair<KeyType, ValueType> entry{key, key*100};
        pmem_memmove_persist((void*) (data_start + key), &entry, sizeof(entry));
        const uint64_t pos = key;
        typename HybridMapType::accessor accessor;
        map_->insert(accessor, {key, pos});
    }
}

void viper::kv_bm::HybridMapFixture::setup_and_insert(const uint64_t start_idx, const uint64_t end_idx) {
    return insert_empty(start_idx, end_idx);
}

uint64_t viper::kv_bm::HybridMapFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    const auto* data_start = data_->data();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        HybridMapType::const_accessor result;
        const bool found = map_->find(result, key);
        found_counter += found && ((data_start + result->second)->second == key*100);
    }
    return found_counter;
}
