#pragma once

#include <stdint.h>
#include <chrono>
#include <iomanip>
#include <benchmark/benchmark.h>

namespace viper::kv_bm {

static constexpr double SCALE_FACTOR = 1;
static constexpr uint64_t NUM_BASE_PREFILLS = 100'000'000;
static constexpr uint64_t NUM_BASE_OPS = 50'000'000;
static constexpr uint64_t NUM_OPS = NUM_BASE_OPS * SCALE_FACTOR;

static constexpr uint64_t NUM_PREFILLS = NUM_BASE_PREFILLS * SCALE_FACTOR;
static constexpr uint64_t NUM_INSERTS = NUM_OPS;
static constexpr uint64_t NUM_UPDATES = NUM_OPS;
static constexpr uint64_t NUM_FINDS = NUM_OPS;
static constexpr uint64_t NUM_DELETES = NUM_OPS;
static constexpr uint64_t MAX_DATA_SIZE = NUM_PREFILLS + NUM_INSERTS;
static constexpr uint64_t NUM_REPETITIONS = 1;
static constexpr uint64_t NUM_MAX_THREADS = 36;
static constexpr uint64_t NUM_UTIL_THREADS = 36;
static constexpr benchmark::TimeUnit BM_TIME_UNIT = benchmark::TimeUnit::kMicrosecond;

template <typename T, size_t N>
struct BMRecord {
    static_assert(N > 0, "N needs to be greater than 0");
    static constexpr size_t total_size = N * sizeof(T);

    BMRecord() {}

    static BMRecord max_value() {
        return BMRecord(std::numeric_limits<uint64_t>::max());
    }

    BMRecord(uint64_t x) { data.fill(static_cast<T>(x)); }
    BMRecord(uint32_t x) { data.fill(static_cast<T>(x)); }

    inline bool operator==(const BMRecord& other) const {
        return data == other.data;
    }

    inline bool operator!=(const BMRecord& other) const {
        return data != other.data;
    }

    bool operator<(const BMRecord &rhs) const {
        return get_key() < rhs.get_key();
    }

    bool operator>(const BMRecord &rhs) const {
        return rhs < *this;
    }

    bool operator<=(const BMRecord &rhs) const {
        return !(rhs < *this);
    }

    bool operator>=(const BMRecord &rhs) const {
        return !(*this < rhs);
    }

    BMRecord<T, N>& from_str(const std::string& bytes) {
        assert(bytes.size() == total_size);
        const char* raw = static_cast<const char*>(bytes.data());
        char* raw_data = static_cast<char*>(static_cast<void*>(data.data()));
        std::copy(raw, raw + total_size, raw_data);
        return *this;
    }

    uint64_t get_key() const {
        return *(uint64_t*) data.data();
    }

    void update_value() {
        data[0]++;
    }

    std::array<T, N> data;
};


using Offset = uint64_t;

using KeyType8 = BMRecord<uint32_t, 2>;
using KeyType16 = BMRecord<uint32_t, 4>;
using KeyType32 = BMRecord<uint32_t, 8>;
using KeyType100 = BMRecord<uint32_t, 25>;
using ValueType8 = KeyType8;
using ValueType100 = KeyType100;
using ValueType200 = BMRecord<uint32_t, 50>;
using ValueType500 = BMRecord<uint32_t, 125>;
using ValueType900 = BMRecord<uint32_t, 225>;

#if defined(NVRAM01)
static constexpr size_t CPU_AFFINITY_OFFSET = 36;
static constexpr char VIPER_POOL_FILE[] = "/dev/dax1.1";
static constexpr char DB_PMEM_DIR[] = "/mnt/pmem2/viper";
static constexpr char DB_FILE_DIR[] = "/scratch/lawrence.benson/viper-dev/data/";
static constexpr char RESULT_FILE_DIR[] = "/scratch/lawrence.benson/viper-dev/results/";
static constexpr char CONFIG_DIR[] = "/scratch/lawrence.benson/viper-dev/benchmark/config/";
#elif defined(NVRAM02)
static constexpr char VIPER_POOL_FILE[] = "/dev/dax0.0";
static constexpr char DB_PMEM_DIR[] = "/mnt/nvram-viper";
static constexpr char DB_FILE_DIR[] = "/scratch/viper";
static constexpr char RESULT_FILE_DIR[] = "/hpi/fs00/home/lawrence.benson/clion/viper/results/";
static constexpr char CONFIG_DIR[] = "/hpi/fs00/home/lawrence.benson/clion/viper/benchmark/config/";
static constexpr size_t CPU_AFFINITY_OFFSET = 0;
#else
static constexpr char VIPER_POOL_FILE[] = "/dev/dax0.0";
static constexpr char DB_PMEM_DIR[] = "/mnt/pmem/";
static constexpr char DB_FILE_DIR[] = "/home/user/data/";
static constexpr char RESULT_FILE_DIR[] = "/home/user/viper/results/";
static constexpr char CONFIG_DIR[] = "/home/user/viper/benchmark/config/";
static constexpr size_t CPU_AFFINITY_OFFSET = 0;  // 0 or #logical-cpu-per-socket
//static_assert(false, "Need to set these variables for unknown host.");
#endif

static constexpr uint64_t ONE_GB = (1024ul*1024*1024) * 1;  // 1GB
static constexpr uint64_t BM_POOL_SIZE = ONE_GB;

std::string get_time_string();
std::string get_output_file(const std::string& bm_name);
int bm_main(std::vector<std::string> args);

}  // namespace viper::kv_bm

template <typename T, size_t N>
std::ostream& operator<<(std::ostream& s, const viper::kv_bm::BMRecord<T, N>& rec) {
    s << "BMRecord[s" << N << ", data=" << rec.data[0] << "]";
    return s;
}
