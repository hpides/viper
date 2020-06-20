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
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx) override;

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

    pool_file_ = random_file(DB_NVM_DIR);
    pmem_db_ = std::make_unique<pmem::kv::db>();

    pmem::kv::config config{};
    config.put_string("path", pool_file_);
    config.put_uint64("size", BM_POOL_SIZE);
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
    pmem_db_ = nullptr;
    db_initialized_ = false;
    pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL);
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string key_str = KeyT{key}.to_str();
        const std::string value_str = ValueT{key}.to_str();
        insert_counter += pmem_db_->put(key_str, value_str) == pmem::kv::status::OK;
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string str_key = KeyT{key}.to_str();
        std::string value;

        const bool found = pmem_db_->get(str_key, &value) == pmem::kv::status::OK;
        if (found) {
            found_counter += ValueT{}.from_str(value).data[0] == key;
        }
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    uint64_t delete_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string str_key = KeyT{key}.to_str();
        delete_counter += pmem_db_->remove(str_key) == pmem::kv::status::OK;
    }
    return delete_counter;
}

template <typename KeyT, typename ValueT>
uint64_t PmemKVFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx) {
    uint64_t update_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string str_key = KeyT{key}.to_str();
        std::string value;

        const bool found = pmem_db_->get(str_key, &value) == pmem::kv::status::OK;
        if (found) {
            ValueT new_val{};
            new_val.from_str(value);
            new_val.update_value();
            update_counter += pmem_db_->put(str_key, new_val.to_str()) == pmem::kv::status::OK;
        }
    }
    return update_counter;
}

}  // namespace kv_bm
}  // namespace viper