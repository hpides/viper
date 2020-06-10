#include <rocksdb/utilities/options_util.h>
#include "rocksdb_fixture.hpp"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace viper::kv_bm {

void RocksDbFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
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

void RocksDbFixture::DeInitMap() {
    delete db_;
    rocksdb_initialized_ = false;
    std::filesystem::remove_all(db_dir_);
}

void RocksDbFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    const rocksdb::WriteOptions write_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const rocksdb::Slice kv = std::to_string(key);
        db_->Put(write_options, kv, kv);
    }
}

void RocksDbFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t RocksDbFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    const rocksdb::ReadOptions read_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        std::string value;
        const rocksdb::Slice db_key = std::to_string(key);
        db_->Get(read_options, db_key, &value);
        found_counter += value == db_key;
    }
    return found_counter;
}

uint64_t RocksDbFixture::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    uint64_t delete_counter = 0;
    const rocksdb::WriteOptions delete_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const rocksdb::Slice db_key = std::to_string(key);
        delete_counter += db_->Delete(delete_options, db_key).ok();
    }
    return delete_counter;
}

std::string DiskRocksDbFixture::get_base_dir() {
    return DB_FILE_DIR;
}

std::string PmemRocksDbFixture::get_base_dir() {
    return DB_NVM_DIR;
}

}  // namespace viper::kv_bm