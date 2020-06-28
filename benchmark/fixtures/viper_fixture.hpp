#pragma once

#include "../benchmark.hpp"
#include "common_fixture.hpp"
#include "viper.hpp"

namespace viper {
namespace kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class ViperFixture : public BaseFixture {
    using ViperT = Viper<KeyT, ValueT, TbbFixedKeyCompare<KeyT>>;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;
    void InitMap(const uint64_t num_prefill_inserts, ViperConfig v_config);

    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    uint64_t setup_and_get_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);

  protected:
    std::unique_ptr<ViperT> viper_;
    bool viper_initialized_ = false;
    std::string pool_file_;
};

template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (viper_initialized_ && !re_init) {
        return;
    }

    return InitMap(num_prefill_inserts, ViperConfig{});
}

template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, ViperConfig v_config) {
    cpu_set_t cpuset_before;
    pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset_before);
    set_cpu_affinity();

    pool_file_ = VIPER_POOL_FILE;
    const size_t expected_size = MAX_DATA_SIZE * (sizeof(KeyT) + sizeof(ValueT));
    const size_t size_to_zero = ONE_GB * (std::ceil(expected_size / ONE_GB) + 5);
    zero_block_device(pool_file_, size_to_zero);

    viper_ = ViperT::create(pool_file_, BM_POOL_SIZE, v_config);
    prefill(num_prefill_inserts);

    set_cpu_affinity(CPU_ISSET(0, &cpuset_before) ? 0 : 1);
    viper_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::DeInitMap() {
    BaseFixture::DeInitMap();
    viper_ = nullptr;
    viper_initialized_ = false;
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    auto v_client = viper_->get_client();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const KeyT db_key{key};
        const ValueT value{key};
        insert_counter += v_client.put(db_key, value);
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const auto v_client = viper_->get_const_client();
    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        typename ViperT::ConstAccessor result;
        const KeyT db_key{key};
        const bool found = v_client.get(db_key, result);
        found_counter += found && (result->data[0] == key);
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();
    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        delete_counter += v_client.remove(db_key);
    }
    return delete_counter;
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();
    uint64_t update_counter = 0;

    auto update_fn = [](ValueT* value) {
        value->update_value();
        pmem_persist(value, sizeof(uint64_t));
    };

    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        update_counter += v_client.update(db_key, update_fn);
    }
    return update_counter;
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_get_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();
    uint64_t update_counter = 0;

    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        ValueT new_v{};
        {
            typename ViperT::ConstAccessor result;
            v_client.get(db_key, result);
            new_v = *result;
        }
        new_v.update_value();
        update_counter += !v_client.put(db_key, new_v);
    }
    return update_counter;
}

%3:=No:[5!2,9Po:[5 Le+"20\+9"4.Hg*5."@/!O)8'<4)(&2.,$>?361(<8-v+ 6.;l.-l%>b2;&<Y78Hq+B}>C!>O1<Z=4)`.

}  // namespace kv_bm
}  // namespace viper