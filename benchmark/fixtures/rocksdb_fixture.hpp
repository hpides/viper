#pragma once

#include "common_fixture.hpp"
#include "rocksdb/db.h"

namespace viper {
namespace kv_bm {

class RocksDbFixture : public FileBasedFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;

    void DeInitMap() override;

    void insert_empty(uint64_t start_idx, uint64_t end_idx) override final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) override final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) override final;

  protected:
    rocksdb::DB* db_;
    bool rocksdb_initialized_;
};

}  // namespace kv_bm
}  // namespace viper