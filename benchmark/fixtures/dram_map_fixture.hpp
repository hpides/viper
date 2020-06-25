#pragma once

#include "common_fixture.hpp"
#include "../benchmark.hpp"
#include <tbb/concurrent_hash_map.h>

namespace viper {
namespace kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class DramMapFixture : public BaseFixture {
    using DramMapType = tbb::concurrent_hash_map<KeyT, ValueT, TbbFixedKeyCompare<KeyT>>;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;
    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(const uint64_t start_idx, const uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

  protected:
    std::unique_ptr<DramMapType> dram_map_;
    bool map_initialized_ = false;

};

template <typename KeyT, typename ValueT>
void DramMapFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }
    dram_map_ = std::make_unique<DramMapType>();
    prefill(num_prefill_inserts);
    map_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void DramMapFixture<KeyT, ValueT>::DeInitMap() {
    dram_map_->clear();
    dram_map_ = nullptr;
    map_initialized_ = false;
}

template <typename KeyT, typename ValueT>
uint64_t DramMapFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        typename DramMapType::accessor result;
        const KeyT db_key{key};
        const ValueT value{key};
        const bool new_insert = dram_map_->insert(result, {db_key, value});
        if (!new_insert) {
            result->second = value;
        }
        insert_counter += new_insert;
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t DramMapFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t DramMapFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t update_counter = 0;
    for (uint64_t i = 0; i < num_updates; ++i) {
        typename DramMapType::accessor result;
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        const bool found = dram_map_->find(result, db_key);
        if (found) {
            result->second.update_value();
            ++update_counter;
        }
    }

    return update_counter;
}

template <typename KeyT, typename ValueT>
uint64_t DramMapFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        typename DramMapType::const_accessor result;
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        const bool found = dram_map_->find(result, db_key);
        found_counter += found && result->second.data[0] == key;
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t DramMapFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        delete_counter += dram_map_->erase(db_key);
    }
    return delete_counter;
}

}  // namespace kv_bm
}  // namespace viper