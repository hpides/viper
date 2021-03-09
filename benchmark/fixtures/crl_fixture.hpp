#pragma once

#include "common_fixture.hpp"
#include "../benchmark.hpp"

#include "crl.hpp"

namespace viper::kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class CrlFixture : public BaseFixture {
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
    void prefill_ycsb(const std::vector<ycsb::Record>& data) override;

protected:
    std::unique_ptr<CrlStore<KeyT, ValueT>> crl_store_;
    std::string log_pool_name_;
    std::string backend_pool_name_;
    bool map_initialized_ = false;
};

template <typename KeyT, typename ValueT>
void CrlFixture<KeyT, ValueT>::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    num_util_threads_ = 18;
    log_pool_name_ = random_file(DB_PMEM_DIR);
    backend_pool_name_ = random_file(DB_PMEM_DIR);
    const size_t backend_file_size = 200 * ONE_GB;
    crl_store_ = std::make_unique<CrlStore<KeyT, ValueT>>(log_pool_name_, backend_pool_name_, backend_file_size);
    prefill(num_prefill_inserts);
    crl_store_->collect_gleaners();
    map_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void CrlFixture<KeyT, ValueT>::DeInitMap() {
    crl_store_ = nullptr;
    map_initialized_ = false;
}

template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    auto client = crl_store_->get_client();
    uint64_t insert_counter = 0;
    for (uint64_t pos = start_idx; pos < end_idx; ++pos) {
        const KeyT db_key{pos};
        const ValueT value{pos};
        insert_counter += client.put(db_key, value);
    }
    return insert_counter;
}

template <>
uint64_t CrlFixture<std::string, std::string>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    auto client = crl_store_->get_client();
    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        insert_counter += client.put(db_key, value);
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto client = crl_store_->get_read_only_client();
    uint64_t found_counter = 0;
    ValueT value;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        const bool found = client.get(db_key, &value);
        found_counter += found && (value == ValueT{key});
    }
    return found_counter;
}

template <>
uint64_t CrlFixture<std::string, std::string>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);

    auto client = crl_store_->get_read_only_client();
    uint64_t found_counter = 0;
    std::string result;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        const bool found = client.get(db_key, &result);
        found_counter += found && (result == value);
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto client = crl_store_->get_client();
    uint64_t update_counter = 0;
    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        ValueT value;
        bool found = client.get(db_key, &value);
        if (found) {
            value.update_value();
            client.put(db_key, value);
            update_counter++;
        }
    }
    return update_counter;
}

template <>
uint64_t CrlFixture<std::string, std::string>::setup_and_update(uint64_t, uint64_t, uint64_t) {
    throw std::runtime_error("not supported");
}

template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto client = crl_store_->get_client();
    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        delete_counter += client.remove(db_key);
    }
    return delete_counter;
}

template <>
uint64_t CrlFixture<std::string, std::string>::setup_and_delete(uint64_t, uint64_t, uint64_t) {
    throw std::runtime_error("not supported");
}


template <typename KeyT, typename ValueT>
uint64_t CrlFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t CrlFixture<KeyType8, ValueType200>::run_ycsb(uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    ValueType200 value;
    const ValueType200 null_value{0ul};
    std::chrono::high_resolution_clock::time_point start;

    auto client = crl_store_->get_client();
    uint64_t op_count = 0;
    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        const ycsb::Record& record = data[op_num];

        if (hdr != nullptr) {
            start = std::chrono::high_resolution_clock::now();
        }

        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                client.put(record.key, record.value);
                op_count++;
                break;
            }
            case ycsb::Record::Op::GET: {
                const bool found = client.get(record.key, &value);
                op_count += found && (value != null_value);
                break;
            }
            case ycsb::Record::Op::UPDATE: {
                const bool found = client.get(record.key, &value);
                if (found) {
                    value.update_value();
                    client.put(record.key, value);
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
}

template <typename KeyT, typename ValueT>
void CrlFixture<KeyT, ValueT>::prefill_ycsb(const std::vector<ycsb::Record>& data) {
    BaseFixture::prefill_ycsb(data);
    crl_store_->collect_gleaners();
}

}  // namespace
