#include <linux/mman.h>
#include <sys/mman.h>
#include <immintrin.h>
#include <xmmintrin.h>
#include <cstdint>
#include <cstring>
#include <vector>
#include <fstream>

#include <benchmark/benchmark.h>
#include "benchmark.hpp"
#include "fixtures/common_fixture.hpp"
#include "viper/viper.hpp"


using namespace viper::kv_bm;

static constexpr size_t DRAM_BM = 0;
static constexpr size_t DEVDAX_BM = 1;
static constexpr size_t FSDAX_BM = 2;

static constexpr size_t SEQ_READ          = 0;
static constexpr size_t RND_READ          = 1;
static constexpr size_t SEQ_WRITE         = 2;
static constexpr size_t RND_WRITE         = 3;
static constexpr size_t SEQ_WRITE_GROUPED = 4;
static constexpr size_t SEQ_WRITE_OFFSET  = 5;

static constexpr size_t NO_PREFAULT = 0;
static constexpr size_t DO_PREFAULT = 1;

static const char WRITE_DATA[] __attribute__((aligned(256))) =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-";

static constexpr size_t THREAD_CHUNK_SIZE = ONE_GB;
static constexpr size_t JUMP_4K = 64 * CACHE_LINE_SIZE;
static constexpr size_t NUM_OPS_PER_THREAD = THREAD_CHUNK_SIZE / JUMP_4K;
static constexpr size_t NUM_ACCESSES_PER_THREAD = THREAD_CHUNK_SIZE / CACHE_LINE_SIZE;
static constexpr size_t NUM_ACCESSES_PER_OP = JUMP_4K / CACHE_LINE_SIZE;
static constexpr size_t DRAM_PAGE_SIZE = 4 * 1024ul; // 4 KiB
static constexpr size_t PMEM_PAGE_SIZE = 2 * (1024ul * 1024); // 2 MiB

#define WRITE(addr) \
    _mm512_store_si512((__m512i*) (addr), *data);\
    _mm_clwb((addr));\
    _mm_sfence()

#define _RD(off) "vmovntdqa " #off "*64(%[addr]), %%zmm0 \n"

#define READ_4K(cur_addr) \
  asm volatile(\
     _RD( 0) _RD( 1) _RD( 2) _RD( 3) _RD( 4) _RD( 5) _RD( 6) _RD( 7) _RD( 8) _RD( 9) _RD(10) _RD(11) _RD(12)\
     _RD(13) _RD(14) _RD(15) _RD(16) _RD(17) _RD(18) _RD(19) _RD(20) _RD(21) _RD(22) _RD(23) _RD(24) _RD(25)\
     _RD(26) _RD(27) _RD(28) _RD(29) _RD(30) _RD(31) _RD(32) _RD(33) _RD(34) _RD(35) _RD(36) _RD(37) _RD(38)\
     _RD(39) _RD(40) _RD(41) _RD(42) _RD(43) _RD(44) _RD(45) _RD(46) _RD(47) _RD(48) _RD(49) _RD(50) _RD(51)\
     _RD(52) _RD(53) _RD(54) _RD(55) _RD(56) _RD(57) _RD(58) _RD(59) _RD(60) _RD(61) _RD(62) _RD(63)\
  : : [addr] "r" (cur_addr) : "%zmm0");


struct BMData {
    size_t storage_type;
    size_t bm_type;
    std::filesystem::path pmem_file;
    int pmem_fd;
    char* dram_data;
    char* pmem_data;
    std::vector<size_t> thread_starts;
    size_t max_num_write_groups;
    size_t write_offset;
};

static BMData bm_data{};

void check_mmap(void* data) {
    if (data == nullptr || data == MAP_FAILED) {
        throw std::runtime_error("Could not mmap data");
    }
}

void check_fd(int fd, const std::string& file) {
    if (fd < 0) {
        throw std::runtime_error("Cannot open file: " + file + " | " + std::strerror(errno));
    }
}

inline uint64_t write_data(char* start_addr, const size_t thread_jump) {
    const __m512i* data = (__m512i*)(WRITE_DATA);

    const auto start = std::chrono::high_resolution_clock::now();
    char* cur_addr = start_addr;
    for (size_t num_op = 0; num_op < NUM_OPS_PER_THREAD; ++num_op) {
        // Write 4096 Byte per iteration
        for (size_t i = 0; i < 64; i += 8) {
            WRITE(cur_addr + ((i + 0) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 1) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 2) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 3) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 4) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 5) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 6) * CACHE_LINE_SIZE));
            WRITE(cur_addr + ((i + 7) * CACHE_LINE_SIZE));
        }
        cur_addr += thread_jump;
    }

    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

inline uint64_t write_offset(char* start_addr, const size_t thread_jump, const size_t offset) {
    const __m512i* data = (__m512i*)(WRITE_DATA);

    const auto start = std::chrono::high_resolution_clock::now();
    char* cur_addr = start_addr;
    for (size_t num_op = 0; num_op < NUM_OPS_PER_THREAD - 1; ++num_op) {
        // Write 4096 Byte per iteration
        for (size_t i = 0; i < 64; i += 8) {
            WRITE(cur_addr + ((i + 0) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 1) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 2) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 3) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 4) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 5) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 6) * CACHE_LINE_SIZE) + offset);
            WRITE(cur_addr + ((i + 7) * CACHE_LINE_SIZE) + offset);
        }
        cur_addr += thread_jump;
    }

    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

inline uint64_t write_grouped(char *start_addr, const size_t thread_jump, const size_t loop_jump, const size_t num_threads_per_group) {
    const __m512i* data = (__m512i*)(WRITE_DATA);
    const size_t num_ops_per_tight_loop = NUM_ACCESSES_PER_OP / num_threads_per_group;
    const auto start = std::chrono::high_resolution_clock::now();

    char* cur_addr = start_addr;
    for (size_t num_op = 0; num_op < NUM_OPS_PER_THREAD * num_threads_per_group; ++num_op) {
        // Write 4096 Byte per iteration
        for (size_t i = 0; i < num_ops_per_tight_loop; i += 2) {
            WRITE(cur_addr);
            cur_addr += thread_jump;
            WRITE(cur_addr);
            cur_addr += thread_jump;
        }
        cur_addr += loop_jump;
    }

    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

inline uint64_t read_data(char* start_addr, const size_t thread_jump) {
    const auto start = std::chrono::high_resolution_clock::now();

    char buffer[JUMP_4K];
    char *cur_addr = start_addr;
    for (size_t num_op = 0; num_op < NUM_OPS_PER_THREAD; ++num_op) {
        // Read 4096 Byte per iteration
        READ_4K(cur_addr);
        cur_addr += thread_jump;
    }

    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

inline uint64_t rnd_read(char* start_addr, char* base_addr) {
    const auto start = std::chrono::high_resolution_clock::now();

    char* cur_addr = start_addr;
    for (size_t num_op = 0; num_op < NUM_ACCESSES_PER_THREAD; ++num_op) {
        __m512i data = _mm512_load_si512(cur_addr);
        const size_t offset = *(size_t *) &data;
        cur_addr = base_addr + offset;
    }

    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    std::ofstream out_file("/dev/null");
    out_file << (start_addr - base_addr) << " --> " << (cur_addr - base_addr);
    return duration;
}

inline size_t rnd_write(char* base_addr, const size_t total_mem_range, const size_t thread_idx) {
    // Simplified RNG from https://github.com/lemire/testingRNG/blob/master/source/lehmer64.h
    __uint128_t g_lehmer64_state = 0xdeadbeef << thread_idx;
    auto next_addr = [&]() {
        g_lehmer64_state *= UINT64_C(0xda942042e4dd58b5);
        return base_addr + (((g_lehmer64_state >> 64) * 64) % total_mem_range);
    };

    __m512i* data = (__m512i*) WRITE_DATA;
    const auto start = std::chrono::high_resolution_clock::now();

    for (size_t num_access = 0; num_access < NUM_ACCESSES_PER_THREAD; ++num_access) {
        char* addr = next_addr();
        WRITE(addr);
    }

    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

inline size_t do_mem_op(benchmark::State& state, const size_t bm_type) {
    const size_t thread_jump = state.threads * JUMP_4K;
    char* base_addr = bm_data.storage_type == DRAM_BM ? bm_data.dram_data : bm_data.pmem_data;
    char* start_addr = base_addr + (state.thread_index * JUMP_4K);

    switch (bm_type) {
        case SEQ_WRITE: {
            return write_data(start_addr, thread_jump);
        }
        case SEQ_WRITE_OFFSET: {
            return write_offset(start_addr, thread_jump, bm_data.write_offset);
        }
        case SEQ_WRITE_GROUPED: {
            const size_t max_num_write_groups = bm_data.max_num_write_groups;
            if (state.threads > max_num_write_groups && state.threads % max_num_write_groups != 0) {
                throw std::runtime_error("#threads must be < or multiple of " + std::to_string(max_num_write_groups));
            }
            const size_t max_write_groups = std::min((size_t) state.threads, max_num_write_groups);
            const size_t num_write_groups = std::max(1ul, max_write_groups);
            const size_t num_threads_per_write_group = state.threads / num_write_groups;
            const size_t grouped_thread_jump = num_threads_per_write_group * CACHE_LINE_SIZE;
            const size_t loop_jump = (num_write_groups - 1) * JUMP_4K;
            const size_t thread_group = state.thread_index / num_threads_per_write_group;
            const size_t thread_offset = state.thread_index % num_threads_per_write_group;
            const size_t thread_addr_offset = (thread_group * JUMP_4K) + (thread_offset * CACHE_LINE_SIZE);
            start_addr = base_addr + thread_addr_offset;
            return write_grouped(start_addr, grouped_thread_jump, loop_jump, num_threads_per_write_group);
        }
        case SEQ_READ: {
            return read_data(start_addr, thread_jump);
        }
        case RND_WRITE: {
            const size_t total_mem_range = state.threads * THREAD_CHUNK_SIZE;
            return rnd_write(base_addr, total_mem_range, state.thread_index);
        }
        case RND_READ: {
            start_addr = base_addr + bm_data.thread_starts[state.thread_index];
            return rnd_read(start_addr, base_addr);
        }
        default: {
            throw std::runtime_error("not supported");
        }
    }
}

void fill_random(benchmark::State& state, std::vector<size_t>& offsets) {
    const size_t total_accesses = state.threads * NUM_ACCESSES_PER_THREAD;
    offsets.resize(total_accesses);

    for (size_t offset = 1; offset < total_accesses; ++offset) {
        offsets[offset - 1] = offset * CACHE_LINE_SIZE;
    }
    offsets[total_accesses - 1] = 0;
    auto rng = std::default_random_engine{};
    std::shuffle(offsets.begin(), offsets.end() - 1, rng);
    if (offsets[total_accesses - 1] != 0) throw std::runtime_error("bad shuffle");
}

void init_bm_data(benchmark::State& state, const size_t storage_type, const size_t bm_type, const size_t data_len) {
    bm_data.storage_type = storage_type;
    bm_data.bm_type = bm_type;

    std::stringstream label;
    auto start_mmap = std::chrono::high_resolution_clock::now();
    switch (storage_type) {
        case DRAM_BM: {
            const auto map_flags = viper::VIPER_DRAM_MAP_FLAGS;
            void* data = mmap(nullptr, data_len, PROT_READ | PROT_WRITE, map_flags, -1, 0);
            check_mmap(data);
            bm_data.dram_data = (char*) data;
            label << "dram";
            break;
        }
        case FSDAX_BM: {
            const auto pmem_file = random_file(DB_PMEM_DIR + std::string("/latency"));
            const int fd = ::open(pmem_file.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0600);
            check_fd(fd, pmem_file);
            if (ftruncate(fd, data_len) != 0) throw std::runtime_error("Cannot truncate");
            void* data = mmap(nullptr, data_len, viper::VIPER_MAP_PROT, viper::VIPER_MAP_FLAGS, fd, 0);
            check_mmap(data);
            bm_data.pmem_file = pmem_file;
            bm_data.pmem_fd = fd;
            bm_data.pmem_data = (char*) data;
            label << "fsdax";
            break;
        }
        case DEVDAX_BM: {
            const auto pmem_file = VIPER_POOL_FILE;
            const int fd = ::open(pmem_file, O_RDWR);
            check_fd(fd, pmem_file);
            void* data = mmap(nullptr, data_len, viper::VIPER_MAP_PROT, viper::VIPER_MAP_FLAGS, fd, 0);
            check_mmap(data);
            bm_data.pmem_file = pmem_file;
            bm_data.pmem_fd = fd;
            bm_data.pmem_data = (char*) data;
            label << "devdax";
            break;
        }
        default:
            throw std::runtime_error("Unknown BM TYPE " + std::to_string(storage_type));
    }
    auto end_mmap = std::chrono::high_resolution_clock::now();
    state.counters["mmap_ns"] = (end_mmap - start_mmap).count();

    char* data = (storage_type == DRAM_BM) ? bm_data.dram_data : bm_data.pmem_data;

    // prefault pages
    bool should_prefault = true;
    if (bm_type == SEQ_WRITE || bm_type == RND_WRITE) {
        should_prefault = state.range(2);
    } else if (bm_type == SEQ_WRITE_GROUPED || bm_type == SEQ_WRITE_OFFSET) {
        should_prefault = state.range(3);
    }

    if (should_prefault) {
        const size_t page_size = storage_type == DRAM_BM ? DRAM_PAGE_SIZE : PMEM_PAGE_SIZE;
        const size_t num_prefault_pages = data_len / page_size;
        auto start_prefault = std::chrono::high_resolution_clock::now();
        for (size_t prefault_offset = 0; prefault_offset < num_prefault_pages; ++prefault_offset) {
            data[prefault_offset * page_size] = '\0';
        }
        auto end_prefault = std::chrono::high_resolution_clock::now();
        const long prefault_dur = (end_prefault - start_prefault).count();
        state.counters["prefault_ns"] = prefault_dur;
        state.counters["prefault_ns_page"] = prefault_dur / num_prefault_pages;
    } else {
        state.counters["prefault_ns"] = 0;
        state.counters["prefault_ns_page"] = 0;
    }

    // prepare file to read from
    std::vector<std::thread> threads{};
    if (bm_type == SEQ_READ) {
        auto write_fn = [](char* addr, size_t idx, size_t num_threads) {
            write_data(addr + (idx * JUMP_4K), JUMP_4K * num_threads);
        };
        for (size_t i = 0; i < state.threads; ++i) {
            threads.emplace_back(write_fn, data, i, state.threads);
        }
        for (auto& t : threads) t.join();
    } else if (bm_type == RND_READ) {
        std::vector<size_t> thread_starts{};
        thread_starts.resize(state.threads);
        std::vector<size_t> offset_queue;
        fill_random(state, offset_queue);

        auto rnd_fill_fn = [&](const size_t thread_idx, const size_t start_offset) {
            size_t offset = 0;
            char *addr = data + start_offset;
            const size_t thread_offset = thread_idx * NUM_ACCESSES_PER_THREAD;
            for (auto i = 0; i < NUM_ACCESSES_PER_THREAD; ++i) {
                offset = offset_queue[thread_offset + i];
                memcpy(addr, &offset, sizeof(size_t));
                memcpy(addr + sizeof(size_t), WRITE_DATA, CACHE_LINE_SIZE - sizeof(size_t));
                addr = data + offset;
            }

            thread_starts[(thread_idx + 1) % state.threads] = offset;
        };

        for (size_t i = 0; i < state.threads; ++i) {
            const size_t prev_end = (i*NUM_ACCESSES_PER_THREAD - 1) % offset_queue.size();
            const size_t start_offset = offset_queue[prev_end];
            threads.emplace_back(rnd_fill_fn, i, start_offset);
        }

        for (auto& t : threads) t.join();
        bm_data.thread_starts = thread_starts;
    }

    if (bm_type == SEQ_WRITE_GROUPED) {
        bm_data.max_num_write_groups = state.range(2);
    }

    if (bm_type == SEQ_WRITE_OFFSET) {
        bm_data.write_offset = state.range(2);
    }

    std::string bm_type_label;
    switch (bm_type) {
        case (SEQ_READ)          : bm_type_label = "seq_read";  break;
        case (RND_READ)          : bm_type_label = "rnd_read";  break;
        case (SEQ_WRITE)         : bm_type_label = "seq_write"; break;
        case (RND_WRITE)         : bm_type_label = "rnd_write"; break;
        case (SEQ_WRITE_GROUPED) : bm_type_label = "seq_grp_write_" + std::to_string(bm_data.max_num_write_groups); break;
        case (SEQ_WRITE_OFFSET)  : bm_type_label = "seq_off_write_" + std::to_string(bm_data.write_offset); break;
    }
    label << "-t" << state.threads << "-" << (should_prefault ? "prefault" : "no_prefault") << "-" << bm_type_label;
    state.SetLabel(label.str());
}

void latency_bw_bm(benchmark::State& state) {
    const size_t storage_type = state.range(0);
    const size_t bm_type = state.range(1);
    const size_t data_len = state.threads * THREAD_CHUNK_SIZE;

    if (is_init_thread(state)) {
        init_bm_data(state, storage_type, bm_type, data_len);
    }

    set_cpu_affinity(state.thread_index);

    size_t duration;
    for (auto _ : state) {
        duration = do_mem_op(state, bm_type);
    }

    // Offset writing does one op fewer to avoid segfaults at the end of the file
    const size_t num_total_accesses = bm_type == SEQ_WRITE_OFFSET
            ? NUM_ACCESSES_PER_THREAD - NUM_ACCESSES_PER_OP
            : NUM_ACCESSES_PER_THREAD;

    const size_t avg_lat = duration / num_total_accesses;
    state.counters["avg_latency_ns"] = benchmark::Counter(avg_lat, benchmark::Counter::kAvgThreads);

    const double thread_dur_s = duration / 1e9;
    state.counters["total_dur_s"] = benchmark::Counter(thread_dur_s, benchmark::Counter::kAvgThreads);

    state.SetBytesProcessed(THREAD_CHUNK_SIZE);

    if (is_init_thread(state)) {
        munmap(bm_data.dram_data, data_len);
        munmap(bm_data.pmem_data, data_len);
        ::close(bm_data.pmem_fd);
        if (bm_data.pmem_file.string().find("/dev/dax") == std::string::npos) {
            std::filesystem::remove(bm_data.pmem_file);
        }
        bm_data.dram_data = nullptr;
        bm_data.pmem_data = nullptr;
        bm_data.pmem_fd = -1;
        bm_data.pmem_file = "";
    }

}

#define BM_ARGS Repetitions(1)->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime()

#define ADD_GROUPED_STORAGE_TYPE(storage_type)\
      Args({storage_type, SEQ_WRITE_GROUPED,  1, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED,  1, NO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED,  4, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED,  4, NO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED,  8, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED,  8, NO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED, 16, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED, 16, NO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED, 32, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE_GROUPED, 32, NO_PREFAULT})

#define ADD_STORAGE_TYPE(storage_type)\
      Args({storage_type, SEQ_READ})\
    ->Args({storage_type, SEQ_WRITE, DO_PREFAULT})\
    ->Args({storage_type, SEQ_WRITE, NO_PREFAULT})\
    ->Args({storage_type, RND_READ})\
    ->Args({storage_type, RND_WRITE, DO_PREFAULT})\
    ->Args({storage_type, RND_WRITE, NO_PREFAULT})\
    ->ADD_GROUPED_STORAGE_TYPE(storage_type)

//#define ADD_STORAGE_TYPE(storage_type)\
//      Args({storage_type, SEQ_WRITE_OFFSET,    0, DO_PREFAULT})\
//    ->Args({storage_type, SEQ_WRITE_OFFSET, 1024, DO_PREFAULT})\
//    ->Args({storage_type, SEQ_WRITE_OFFSET, 2048, DO_PREFAULT})\
//    ->Args({storage_type, SEQ_WRITE_OFFSET, 3072, DO_PREFAULT})

BENCHMARK(latency_bw_bm)->BM_ARGS
    ->Threads(1) // Warm up run
    ->Threads(1)
    ->Threads(4)
    ->Threads(8)
    ->Threads(16)
    ->Threads(32)
    ->ADD_STORAGE_TYPE(DRAM_BM)
    ->ADD_STORAGE_TYPE(DEVDAX_BM);
//    ->ADD_STORAGE_TYPE(FSDAX_BM);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("latency_bw/latency_bw");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
