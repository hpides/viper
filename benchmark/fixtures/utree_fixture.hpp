#pragma once

#include "common_fixture.hpp"
#include "../benchmark.hpp"

#ifndef UTREE_KEY_T
#define UTREE_KEY_T viper::kv_bm::KeyType16
#endif

#ifndef UTREE_VALUE_T
#define UTREE_VALUE_T viper::kv_bm::ValueType200
#endif

#include "utree.hpp"
#include "viper/cceh.hpp"

namespace viper::kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class UTreeFixture : public BaseFixture {
  public:
    static constexpr bool IS_YCSBABLE = std::is_same_v<UTREE_KEY_T, KeyType8>;

    void InitMap(const uint64_t num_prefill_inserts, const bool re_init) final;
    void DeInitMap() final;
    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds);
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes);
    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data,
                      hdr_histogram* hdr) final;
    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;
    void prefill_ycsb(const std::vector<ycsb::Record>& data) override;

  protected:
    std::unique_ptr<btree> utree_;
    std::string pmem_pool_name_;
    bool map_initialized_ = false;
};

template <typename KeyT, typename ValueT>
void UTreeFixture<KeyT, ValueT>::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    pmem_pool_name_ = random_file(DB_PMEM_DIR);
    utree_ = std::make_unique<btree>(pmem_pool_name_);
    prefill(num_prefill_inserts);
    map_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void UTreeFixture<KeyT, ValueT>::DeInitMap() {
    utree_ = nullptr;
    pmempool_rm(pmem_pool_name_.c_str(), 0);
    map_initialized_ = false;
}

template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t pos = start_idx; pos < end_idx; ++pos) {
        const KeyT db_key{pos};
        const ValueT value{pos};
        insert_counter += utree_->insert(db_key, value);
    }
    return insert_counter;
}

template <>
uint64_t UTreeFixture<std::string, std::string>::insert(uint64_t start_idx, uint64_t end_idx) {
    throw std::runtime_error("not supported");
}

template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        ValueT value;
        const bool found = utree_->search(key, &value);
        found_counter += found && (value.data[0] == key);
    }
    return found_counter;
}

template <>
uint64_t UTreeFixture<std::string, std::string>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    throw std::runtime_error("not supported");
}

template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t update_counter = 0;
    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        ValueT value;
        bool found = utree_->search(db_key, &value);
        if (found) {
            value.update_value();
            update_counter += utree_->insert(db_key, value);
        }
    }
    return update_counter;
}

template <>
uint64_t UTreeFixture<std::string, std::string>::setup_and_update(uint64_t, uint64_t, uint64_t) {
    throw std::runtime_error("not supported");
}

template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const size_t prefills_per_thread = (100'000'000 / num_util_threads_) + 1;

    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        if (key % prefills_per_thread == 0) {
            continue;
        }
        const KeyT db_key{key};
        delete_counter += utree_->remove(db_key);
    }
    return delete_counter;
}

template <>
uint64_t UTreeFixture<std::string, std::string>::setup_and_delete(uint64_t, uint64_t, uint64_t) {
    throw std::runtime_error("not supported");
}


template <typename KeyT, typename ValueT>
uint64_t UTreeFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t UTreeFixture<KeyType8, ValueType200>::run_ycsb(uint64_t start_idx,
    uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
#ifdef YCSB_BM
    ValueType200 value;
    const ValueType200 null_value{0ul};
    std::chrono::high_resolution_clock::time_point start;
    uint64_t op_count = 0;
    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        const ycsb::Record &record = data[op_num];

        if (hdr != nullptr) {
            start = std::chrono::high_resolution_clock::now();
        }

        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                utree_->insert(record.key, record.value);
                op_count++;
                break;
            }
            case ycsb::Record::Op::GET: {
                const bool found = utree_->search(record.key, &value);
                op_count += found && (value != null_value);
                break;
            }
            case ycsb::Record::Op::UPDATE: {
                const bool found = utree_->search(record.key, &value);
                if (found) {
                    value.update_value();
                    utree_->insert(record.key, value);
                    op_count++;
                }
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
#else
    throw std::runtime_error("Change UTREE_KEY_T to 8");
#endif
}

template <typename KeyT, typename ValueT>
void UTreeFixture<KeyT, ValueT>::prefill_ycsb(const std::vector<ycsb::Record>& data) {
    BaseFixture::prefill_ycsb(data);
}

}  // namespace
