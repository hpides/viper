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
        (*data_)[key] = key;
        HybridMapType::accessor slot;
        map_->insert(slot, key);
        slot->second = key;
    }
    data_.persist();
    map_initialized_ = true;
}

void viper::kv_bm::HybridMapFixture::insert_empty(const uint64_t start_idx, const uint64_t end_idx) {
    const ValueType* data_start = data_->data();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        // This is not correct because of race conditions.
        // But it's enough to get a feeling for the performance.
        const uint64_t pos = key;
        const ValueType value = key*100;
        pmem_memmove_persist((void*) (data_start + key), &value, sizeof(ValueType));
//            (*data_)[key] = key*100;
        map_->insert({key, pos});
    }
}

void viper::kv_bm::HybridMapFixture::setup_and_insert(const uint64_t start_idx, const uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t viper::kv_bm::HybridMapFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        HybridMapType::const_accessor result;
        found_counter += map_->find(result, key);
        benchmark::DoNotOptimize((*data_)[result->second] == 0);
    }
    return found_counter;
}
