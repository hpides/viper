#include "pmem_kv_fixture.hpp"

namespace viper::kv_bm {

void PmemKVFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (db_initialized_ && !re_init) {
        return;
    }

    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    pool_file_ = random_file(POOL_FILE_DIR);
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

void PmemKVFixture::DeInitMap() {
    BaseFixture::DeInitMap();
    pmem_db_ = nullptr;
    db_initialized_ = false;
    pmempool_rm(pool_file_.c_str(), PMEMPOOL_RM_FORCE | PMEMPOOL_RM_POOLSET_LOCAL);
}

void PmemKVFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string kv = std::to_string(key);
        pmem_db_->put(kv, kv);
    }
}

void PmemKVFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t PmemKVFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    uint64_t found_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string str_key = std::to_string(key);
        std::string value;

        const bool found = pmem_db_->get(str_key, &value) == pmem::kv::status::OK;
        found_counter += found; //&& (value->second.data[0] == key);
    }
    return found_counter;
}

uint64_t PmemKVFixture::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    uint64_t delete_counter = 0;
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        std::string str_key = std::to_string(key);
        delete_counter += pmem_db_->remove(str_key) == pmem::kv::status::OK;
    }
    return delete_counter;
}


}  // namespace viper::kv_bm