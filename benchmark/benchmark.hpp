#pragma once

#include <stdint.h>
#include <benchmark/benchmark.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>

namespace viper {
namespace kv_bm {

static constexpr uint64_t NUM_INSERTS = 1000;
static constexpr uint64_t NUM_PREFILLS = NUM_INSERTS;
static constexpr uint64_t MAX_DATA_SIZE = NUM_PREFILLS + NUM_INSERTS;
static constexpr uint64_t NUM_FINDS = 1000;
static constexpr uint64_t NUM_REPETITIONS = 1;
static constexpr uint64_t NUM_MAX_THREADS = 2; //64;
static constexpr benchmark::TimeUnit BM_TIME_UNIT = benchmark::TimeUnit::kMicrosecond;

using KeyType = uint64_t;
using ValueType = uint64_t;
using Offset = uint64_t;

static constexpr char POOL_FILE_DIR[] = "/mnt/nvrams1/kv-bm";
//static constexpr char DB_FILE_DIR[] = "/mnt/nvrams1/dbfiles";
static constexpr char DB_FILE_DIR[] = "/home/lawrence.benson/dbfiles";
static const uint64_t BM_POOL_SIZE = (1024l*1024*1024) * 5;  // 5GB

using pmem::obj::p;
using pmem::obj::persistent_ptr;

}  // namespace kv_bm
}  // namespace viper