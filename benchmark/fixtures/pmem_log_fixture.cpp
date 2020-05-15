#include "pmem_log_fixture.hpp"

namespace viper{
namespace kv_bm {

void PmemLogFixture::SetUp(benchmark::State& state) {
    BaseFixture::SetUp(state);
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    {
        std::scoped_lock lock(pool_mutex_);
        if (pool_file_.empty()) {
//            pool_file_ = random_file(POOL_FILE_DIR);
            pool_file_ = "/mnt/nvrams1/log-test.file";
            std::cout << "Working on NVM file " << pool_file_ << std::endl;
//            pmem_log_pool_ = pmemlog_create(pool_file_.c_str(), BM_POOL_SIZE, S_IRWXU);
            pmem_log_pool_ = pmemlog_open(pool_file_.c_str());
            if (pmem_log_pool_ == nullptr) {
                throw std::runtime_error("Need to create log file " + pool_file_.string() + " manually due to some weird bug...");
            }
        }
    }
}

void PmemLogFixture::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    map_ = std::make_unique<HybridMapType>();

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        std::pair<KeyType, ValueType> data{key, key};
        pmemlog_append(pmem_log_pool_, &data, sizeof(data));
        HybridMapType::accessor slot;
        map_->insert(slot, key);
        slot->second = key;
    }
    map_initialized_ = true;
}

void PmemLogFixture::DeInitMap() {
    map_ = nullptr;
    map_initialized_ = false;
}

void PmemLogFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    // This is not thread safe. We just want a rough estimate of performance.
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const size_t insert_pos = pmemlog_tell(pmem_log_pool_);
        std::pair<KeyType, ValueType> data{key, key*100};
        pmemlog_append(pmem_log_pool_, &data, sizeof(data));
        HybridMapType::accessor slot;
        map_->insert(slot, key);
        slot->second = insert_pos;
    }
}

void PmemLogFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert_empty(start_idx, end_idx);
}

uint64_t PmemLogFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    throw std::runtime_error("Only insert supported for speed check. Not thread-safe so find is wrong anyway.");
}

}  // namespace kv_bm
}  // namespace viper