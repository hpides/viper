#pragma once

#include "../benchmark.hpp"
#include "common_fixture.hpp"
#include "viper.hpp"

namespace viper {
namespace kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class ViperFixture : public BaseFixture {
  public:
    using ViperT = Viper<KeyT, ValueT, TbbFixedKeyCompare<KeyT>>;

    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;
    void InitMap(const uint64_t num_prefill_inserts, ViperConfig v_config);

    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    uint64_t setup_and_get_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);

    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
        const std::vector<ycsb::Record>& data, hdr_histogram* hdr) final;

    ViperT* getViper() {
        return viper_.get();
    }

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

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t ViperFixture<KeyType8, ValueType200>::run_ycsb(
    uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    uint64_t op_count = 0;
    auto v_client = viper_->get_client();

    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        const ycsb::Record& record = data[op_num];

        const auto start = std::chrono::high_resolution_clock::now();

        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                op_count += v_client.put(record.key, record.value);
                break;
            }
            case ycsb::Record::Op::GET: {
                typename ViperT::ConstAccessor result;
                const bool found = v_client.get(record.key, result);
                op_count += found && result->data[0] != 0;
                break;
            }
            case ycsb::Record::Op::UPDATE: {
                auto update_fn = [&](ValueType200* value) { *value = record.value; };
                op_count += v_client.update(record.key, update_fn);
                break;
            }
            default: {
                throw std::runtime_error("Unknown operation: " + std::to_string(record.op));
            }
        }

        if (hdr == nullptr) {
            continue;
        }

        const auto end = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        hdr_record_value(hdr, duration.count());
    }

    return op_count;
}

}  // namespace kv_bm
}  // namespace viper