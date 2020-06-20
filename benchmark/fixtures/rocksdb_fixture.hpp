#pragma once

#include <rocksdb/utilities/options_util.h>
#include "common_fixture.hpp"
#include "rocksdb/db.h"

namespace viper {
namespace kv_bm {

template <typename KeyT, typename ValueT>
class RocksDbFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx) final;

    virtual std::string get_base_dir() = 0;

  protected:
    rocksdb::DB* db_;
    std::filesystem::path base_dir_;
    std::filesystem::path db_dir_;
    bool rocksdb_initialized_;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class DiskRocksDbFixture : public RocksDbFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class PmemRocksDbFixture : public RocksDbFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT, typename ValueT>
void RocksDbFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (rocksdb_initialized_ && !re_init) {
        return;
    }

    const std::string config_file = CONFIG_DIR + std::string("rocksdb.conf");
    rocksdb::Options options;
    rocksdb::Env* env = rocksdb::Env::Default();
    std::vector<rocksdb::ColumnFamilyDescriptor> cfd;
    rocksdb::Status s = rocksdb::LoadOptionsFromFile(config_file, env, &options, &cfd);
    if (!s.ok()) {
        throw std::runtime_error("Could not load RocksDb config file.");
    }

    options.create_if_missing = true;
    options.error_if_exists = true;
    base_dir_ = get_base_dir();
    db_dir_ = random_file(base_dir_);

    if (base_dir_.string().rfind("/mnt/nvram", 0) == 0) {
        // Is NVM version
        options.wal_dir = "/mnt/nvram-gp/rocksdb-wal";
    } else {
        // Disk version
        options.wal_dir = "/home/lawrence.benson/rocksdb-wal";
    }
    std::filesystem::remove_all(options.wal_dir);

    rocksdb::Status status = rocksdb::DB::Open(options, db_dir_, &db_);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
    }

    prefill(num_prefill_inserts);
    rocksdb_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void RocksDbFixture<KeyT, ValueT>::DeInitMap() {
    delete db_;
    rocksdb_initialized_ = false;
    std::filesystem::remove_all(db_dir_);
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    const rocksdb::WriteOptions write_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const rocksdb::Slice key_str = KeyT{key}.to_str();
        const rocksdb::Slice value_str = ValueT{key}.to_str();
        insert_counter += db_->Put(write_options, key_str, value_str).ok();
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    const rocksdb::ReadOptions read_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        std::string value;
        const rocksdb::Slice db_key = KeyT(key).to_str();
        const bool found = db_->Get(read_options, db_key, &value).ok();
        if (found) {
            found_counter += ValueT{}.from_str(value).data[0] == key;
        }
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    uint64_t delete_counter = 0;
    const rocksdb::WriteOptions delete_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const rocksdb::Slice db_key = KeyT(key).to_str();
        delete_counter += db_->Delete(delete_options, db_key).ok();
    }
    return delete_counter;
}
template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx) {
    uint64_t update_counter = 0;
    const rocksdb::ReadOptions read_options{};
    const rocksdb::WriteOptions write_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        std::string value;
        const rocksdb::Slice db_key = KeyT(key).to_str();
        const bool found = db_->Get(read_options, db_key, &value).ok();
        if (found) {
            ValueT new_value{};
            new_value.from_str(value);
            new_value.update_value();
            update_counter += db_->Put(write_options, db_key, new_value.to_str()).ok();
        }
    }
    return update_counter;
}

template <typename KeyT, typename ValueT>
std::string DiskRocksDbFixture<KeyT, ValueT>::get_base_dir() {
    return DB_FILE_DIR;
}

template <typename KeyT, typename ValueT>
std::string PmemRocksDbFixture<KeyT, ValueT>::get_base_dir() {
    return DB_NVM_DIR;
}

}  // namespace kv_bm
}  // namespace viper