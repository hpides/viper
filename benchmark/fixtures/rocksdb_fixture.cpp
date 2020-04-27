#include "rocksdb_fixture.hpp"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

void viper::kv_bm::RocksDbFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (rocksdb_initialized_ && !re_init) {
        return;
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_file_, &db_);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
    }

    const rocksdb::WriteOptions& write_options = rocksdb::WriteOptions();
    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        const rocksdb::Slice db_key = std::to_string(key);
        const rocksdb::Slice value = std::to_string(key);
        db_->Put(write_options, db_key, value);
    }
    rocksdb_initialized_ = true;
}

void viper::kv_bm::RocksDbFixture::DeInitMap() {
    delete db_;
    rocksdb_initialized_ = false;
}

void viper::kv_bm::RocksDbFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    const rocksdb::WriteOptions write_options{};
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        // uint64_t key = uniform_distribution(rnd_engine_);
        const rocksdb::Slice db_key = std::to_string(key);
        const rocksdb::Slice value = std::to_string(key*100);
        db_->Put(write_options, db_key, value);
    }
}

void viper::kv_bm::RocksDbFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t viper::kv_bm::RocksDbFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
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
