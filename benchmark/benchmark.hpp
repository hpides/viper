#pragma once

#include <stdint.h>
#include <chrono>
#include <iomanip>
#include <benchmark/benchmark.h>

#include "benchmark_data_types.hpp"

namespace viper::kv_bm {

static constexpr uint64_t NUM_PREFILLS = 100000; //100'000'000;
static constexpr uint64_t NUM_INSERTS = 100000; //100'000'000;
static constexpr uint64_t NUM_UPDATES = NUM_INSERTS;
static constexpr uint64_t NUM_FINDS = NUM_INSERTS / 2;
static constexpr uint64_t NUM_DELETES = NUM_INSERTS / 2;
static constexpr uint64_t MAX_DATA_SIZE = NUM_PREFILLS + NUM_INSERTS;
static constexpr uint64_t NUM_REPETITIONS = 1;
static constexpr uint64_t NUM_MAX_THREADS = 36;
static constexpr uint64_t NUM_UTIL_THREADS = 64;
static constexpr benchmark::TimeUnit BM_TIME_UNIT = benchmark::TimeUnit::kMicrosecond;

struct ValuePlaceholder {
    ValuePlaceholder() {}
    ValuePlaceholder(uint64_t x) { data[0] = x; }
    std::array<uint64_t, 1> data;
};

using KeyType = BMKeyFixed;
using ValueType = BMValueFixed;
//using KeyType = uint64_t;
//using ValueType = ValuePlaceholder;
using Offset = uint64_t;

static constexpr char VIPER_POOL_FILE[] = "/dev/dax0.0";
static constexpr char POOL_FILE_DIR[] = "/mnt/nvram-gp/kv-bm";
static constexpr char DB_NVM_DIR[] = "/mnt/nvram-gp/dbfiles";
static constexpr char DB_FILE_DIR[] = "/home/lawrence.benson/dbfiles";
static constexpr char RESULT_FILE_DIR[] = "/home/lawrence.benson/clion/viper/results/";
static constexpr char CONFIG_DIR[] = "/home/lawrence.benson/clion/viper/benchmark/config/";
static const uint64_t BM_POOL_SIZE = (1024l*1024*1024) * 1;  // 1GB

std::string get_time_string();
std::string get_output_file(const std::string& bm_name);
int bm_main(std::vector<std::string> args);

}  // namespace viper::kv_bm