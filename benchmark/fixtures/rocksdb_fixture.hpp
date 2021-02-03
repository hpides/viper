#pragma once

#include <rocksdb/utilities/options_util.h>
#include <rocksdb/table.h>
#include "common_fixture.hpp"
#include "rocksdb/db.h"
#include "env_dcpmm.h"
#include "cache_dcpmm.h"

namespace viper {
namespace kv_bm {

template <typename KeyT, typename ValueT>
class RocksDbFixture : public BaseFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    virtual std::string get_base_dir() = 0;

  protected:
    rocksdb::DB* db_;
    std::filesystem::path base_dir_;
    std::filesystem::path db_dir_;
    std::filesystem::path wal_dir_;
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
//    rocksdb::Env* env = rocksdb::Env::Default();
    rocksdb::Env* env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());

//    std::vector<rocksdb::ColumnFamilyDescriptor> cfd;
//    rocksdb::Status s = rocksdb::LoadOptionsFromFile(config_file, env, &options, &cfd);
//    auto cache = rocksdb::NewLRUCache(671088640);
//    rocksdb::Status s = rocksdb::LoadOptionsFromFile(config_file, env, &options, &cfd, false, &cache);
//    if (!s.ok()) {
//        throw std::runtime_error("Could not load RocksDb config file.");
//    }

    base_dir_ = get_base_dir();
    db_dir_ = random_file(base_dir_);
    wal_dir_ = db_dir_ / "rocksdb-wal";

    auto cache = rocksdb::NewLRUCache(4294967296);
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    options.dcpmm_kvs_mmapped_file_fullpath = db_dir_ / "rocksdb.value";
    options.dcpmm_kvs_mmapped_file_size = 50 * ONE_GB;
    options.dcpmm_kvs_value_thres = 64;
    options.dcpmm_compress_value = true;
    options.wal_dir = wal_dir_;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.enable_write_thread_adaptive_yield = false;
    options.disable_auto_compactions = false;
    options.max_background_compactions = 32;
    options.max_background_flushes = 4;
    options.enable_pipelined_write = true;
    options.allow_concurrent_memtable_write = true;
    options.use_direct_io_for_flush_and_compaction = true;
    options.target_file_size_base = 67108864;

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
    std::filesystem::remove_all(wal_dir_);
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    const rocksdb::WriteOptions write_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const KeyT kt{key};
        const ValueT vt{key};
        const rocksdb::Slice db_key{(char*) &kt.data, sizeof(KeyT)};
        const rocksdb::Slice value_str{(char*) &vt.data, sizeof(ValueT)};
        insert_counter += db_->Put(write_options, db_key, value_str).ok();
    }
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    const rocksdb::ReadOptions read_options{};
    for (uint64_t i = 0; i < num_finds; ++i) {
        std::string value;

        const uint64_t key = distrib(rnd_engine);
        const KeyT kt{key};
        const rocksdb::Slice db_key{(char*) &kt.data, sizeof(KeyT)};
        const bool found = db_->Get(read_options, db_key, &value).ok();
        if (found) {
            found_counter += ValueT{}.from_str(value).data[0] == key;
        }
    }
    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t delete_counter = 0;
    const rocksdb::WriteOptions delete_options{};
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT kt{key};
        const rocksdb::Slice db_key{(char*) &kt.data, sizeof(KeyT)};
        delete_counter += db_->Delete(delete_options, db_key).ok();
    }
    return delete_counter;
}
template <typename KeyT, typename ValueT>
uint64_t RocksDbFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t update_counter = 0;
    const rocksdb::ReadOptions read_options{};
    const rocksdb::WriteOptions write_options{};
    for (uint64_t i = 0; i < num_updates; ++i) {
        std::string value;
        const uint64_t key = distrib(rnd_engine);
        const KeyT kt{key};
        const rocksdb::Slice db_key{(char*) &kt.data, sizeof(KeyT)};
        const bool found = db_->Get(read_options, db_key, &value).ok();
        if (found) {
            ValueT new_value{};
            new_value.from_str(value);
            new_value.update_value();
            const rocksdb::Slice new_value_str{(char*) &new_value.data, sizeof(ValueT)};
            update_counter += db_->Put(write_options, db_key, new_value_str).ok();
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
    return DB_PMEM_DIR + std::string("/rocks");
}

}  // namespace kv_bm
}  // namespace viper
