#pragma once

#include <cceh/CCEH.h>
#include "common_fixture.hpp"
#include "../benchmark.hpp"

namespace viper::kv_bm {

template <typename KeyT = KeyType8, typename ValueT = ValueType8>
class CcehFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts, const bool re_init) final;
    void DeInitMap() final;
    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds);
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes);
    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data,
                      hdr_histogram* hdr) final;
    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    std::unique_ptr<CCEH> dram_map_;
    bool map_initialized_ = false;
};

template <typename KeyT, typename ValueT>
void CcehFixture<KeyT, ValueT>::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }
    dram_map_ = std::make_unique<CCEH>(1000000);
    prefill(num_prefill_inserts);
    map_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void CcehFixture<KeyT, ValueT>::DeInitMap() {
    dram_map_ = nullptr;
    map_initialized_ = false;
}

template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        dram_map_->Insert(key, key);
        insert_counter++;
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    return 0;
}
template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const ValueT value = dram_map_->Get(const_cast<size_t&>(key));
        found_counter += value == key;
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    return 0;
}

template <typename KeyT, typename ValueT>
uint64_t CcehFixture<KeyT, ValueT>::run_ycsb(uint64_t start_idx,
                                             uint64_t end_idx,
                                             const std::vector<ycsb::Record>& data,
                                             hdr_histogram* hdr) {
    return BaseFixture::run_ycsb(start_idx, end_idx, data, hdr);
}

}  // namespace
