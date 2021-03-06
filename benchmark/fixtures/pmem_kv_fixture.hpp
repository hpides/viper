#pragma once

#include "common_fixture.hpp"
#include <libpmemkv.hpp>

namespace std {

//template <>
//struct hash<viper::kv_bm::BMKeyFixed> {
//    size_t operator()(const viper::kv_bm::BMKeyFixed& key) {
//        return std::hash<uint64_t>()(key.uuid[0]);
//    }
//};
}

namespace viper {
namespace kv_bm {

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class PmemKVFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;
    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
        const std::vector<ycsb::Record>& data, hdr_histogram* hdr) final;

  protected:
    std::unique_ptr<pmem::kv::db> pmem_db_;
    std::string pool_file_;
    bool db_initialized_ = false;
};

template <typename KeyT, typename ValueT>
void PmemKVFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (db_initialized_ && !re_init) {
        return;
    }

    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    pool_file_ = random_file(DB_PMEM_DIR);
    pmem_db_ = std::make_unique<pmem::kv::db>();

    const size_t expected_pool_file_size = 90 * ONE_GB;
    pmem::kv::config config{};
    config.put_string("path", pool_file_);
    config.put_uint64("size", expected_pool_file_size);
    config.put_uint64("force_create", 1);
    pmem::kv::status s = pmem_db_->open("cmap", std::move(config));
    if (s != pmem::kv::status::OK) {
        throw std::runtime_error("Could not open PmemKV!");
    }

    prefill(num_prefill_inserts);
    db_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void PmemKVFixture<KeyT, ValueT>::DeInitMap() {
    BaseFixture::DeInitMap();
    pmem_db_->close();
    pmem_db_ = nullptr;
    db_initialized_ = false;
    pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL);
    std::filesystem::remove(pool_file_);
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const KeyT k{key};
        const ValueT v{key};
        const pmem::kv::string_view db_key{(char*) &k.data, sizeof(KeyT)};
        const pmem::kv::string_view value_str{(char*) &v.data, sizeof(ValueT)};
        insert_counter += pmem_db_->put(db_key, value_str) == pmem::kv::status::OK;
    }
    return insert_counter;
}

template <>
uint64_t PmemKVFixture<std::string, std::string>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        insert_counter += pmem_db_->put(db_key, value) == pmem::kv::status::OK;
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT k{key};
        std::string value;
        const pmem::kv::string_view db_key{(char*) &k.data, sizeof(KeyT)};

        const bool found = pmem_db_->get(db_key, &value) == pmem::kv::status::OK;
        if (found) {
            found_counter += ValueT{}.from_str(value).data[0] == key;
        }
    }
    return found_counter;
}

template <>
uint64_t PmemKVFixture<std::string, std::string>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        std::string result{};

        const bool found = pmem_db_->get(db_key, &result) == pmem::kv::status::OK;
        found_counter += found && result == value;
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT k{key};
        const pmem::kv::string_view db_key{(char*) &k.data, sizeof(KeyT)};
        delete_counter += pmem_db_->remove(db_key) == pmem::kv::status::OK;
    }
    return delete_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t update_counter = 0;
    for (uint64_t i = 0; i < num_updates; ++i) {
        std::string value;
        const uint64_t key = distrib(rnd_engine);
        const KeyT k{key};
        const pmem::kv::string_view db_key{(char*) &k.data, sizeof(KeyT)};

        const bool found = pmem_db_->get(db_key, &value) == pmem::kv::status::OK;
        if (found) {
            ValueT new_val{};
            new_val.from_str(value);
            new_val.update_value();
            const pmem::kv::string_view new_val_str{(char*) &new_val.data, sizeof(ValueT)};
            update_counter += pmem_db_->put(db_key, new_val_str) == pmem::kv::status::OK;
        }
    }
    return update_counter;
}

template <>
uint64_t PmemKVFixture<std::string, std::string>::setup_and_update(uint64_t, uint64_t, uint64_t) {
    return 0;
}

template <>
uint64_t PmemKVFixture<std::string, std::string>::setup_and_delete(uint64_t, uint64_t, uint64_t) {
    return 0;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t PmemKVFixture<KeyType8, ValueType200>::run_ycsb(
    uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    uint64_t op_count = 0;

    std::chrono::high_resolution_clock::time_point start;
    std::string value;
    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        const ycsb::Record& record = data[op_num];

        if (hdr != nullptr) {
            start = std::chrono::high_resolution_clock::now();
        }

        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                const pmem::kv::string_view db_key{(char*) &record.key.data, sizeof(KeyType8)};
                const pmem::kv::string_view value_str{(char*) &record.value.data, sizeof(ValueType200)};
                op_count += pmem_db_->put(db_key, value_str) == pmem::kv::status::OK;
                break;
            }
            case ycsb::Record::Op::GET: {
                const pmem::kv::string_view db_key{(char*) &record.key.data, sizeof(KeyType8)};

                const bool found = pmem_db_->get(db_key, &value) == pmem::kv::status::OK;
                if (found) {
                    op_count += ValueType200 {}.from_str(value).data[0] != 0;
                }
                break;
            }
            case ycsb::Record::Op::UPDATE: {
                std::string old_value;
                const pmem::kv::string_view db_key{(char*) &record.key.data, sizeof(KeyType8)};

                const bool found = pmem_db_->get(db_key, &old_value) == pmem::kv::status::OK;
                if (found) {
                    ValueType200 new_val{};
                    new_val.from_str(old_value);
                    new_val.update_value();
                    const pmem::kv::string_view new_val_str{(char*) &new_val.data, sizeof(ValueType200)};
                    op_count += pmem_db_->put(db_key, new_val_str) == pmem::kv::status::OK;
                }
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
