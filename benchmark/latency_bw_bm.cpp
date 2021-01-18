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
#include "viper.hpp"


using namespace viper::kv_bm;

static constexpr size_t DRAM_BM = 0;
static constexpr size_t PMEM_BM = 1;
static constexpr size_t SSD_BM  = 2;

static constexpr size_t SEQ_READ   = 0;
static constexpr size_t RND_READ   = 1;
static constexpr size_t SEQ_WRITE  = 2;
static constexpr size_t RND_WRITE  = 3;

static const char WRITE_DATA[] __attribute__((aligned(256))) =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-";

static constexpr size_t THREAD_CHUNK_SIZE = ONE_GB;
static constexpr size_t JUMP_4K = 64 * CACHE_LINE_SIZE;
static constexpr size_t NUM_OPS_PER_THREAD = THREAD_CHUNK_SIZE / JUMP_4K;
static constexpr size_t NUM_ACCESSES_PER_THREAD = THREAD_CHUNK_SIZE / CACHE_LINE_SIZE;
static constexpr size_t NUM_OPS_PER_INNER_LOOP = 8;
static constexpr size_t NUM_OPS_PER_OUTER_LOOP = NUM_OPS_PER_THREAD / NUM_OPS_PER_INNER_LOOP;

#define WRITE(addr) \
    _mm512_store_si512((__m512i*) addr, *data);\
    _mm_clwb(addr);\
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
    std::filesystem::path ssd_file;

    int pmem_fd;
    int ssd_fd;

    char* dram_data;
    char* pmem_data;
    char* ssd_data;

    std::vector<size_t> thread_starts;
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

inline uint64_t write_nt(char* start_addr, const size_t thread_jump) {
    const __m512i* data = (__m512i*)(WRITE_DATA);
    uint64_t duration = 0;

    char* cur_addr = start_addr;
    for (uint32_t num_op = 0; num_op < NUM_OPS_PER_OUTER_LOOP; ++num_op) {
        const auto start = std::chrono::high_resolution_clock::now();

        for (uint32_t loop = 0; loop < NUM_OPS_PER_INNER_LOOP; ++loop) {
            // Write 4096 Byte per iteration
            #pragma unroll 64
            for (auto i = 0; i < 64; ++i) {
                char* addr = cur_addr + (i * CACHE_LINE_SIZE);
                WRITE(addr);
            }
            cur_addr += thread_jump;
        }

        const auto end = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    return duration;
}

inline uint64_t read_nt(char* start_addr, const size_t thread_jump) {
    uint64_t duration = 0;

    char buffer[JUMP_4K];
    char* cur_addr = start_addr;
    for (uint32_t num_op = 0; num_op < NUM_OPS_PER_OUTER_LOOP; ++num_op) {
        const auto start = std::chrono::high_resolution_clock::now();

        for (uint32_t loop = 0; loop < NUM_OPS_PER_INNER_LOOP; ++loop) {
            // Read 4096 Byte per iteration
            READ_4K(cur_addr);
            cur_addr += thread_jump;
        }

        const auto end = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    return duration;
}

inline uint64_t rnd_read(char* start_addr, char* base_addr) {
    uint64_t duration = 0;

    char* cur_addr = start_addr;
    for (uint32_t num_op = 0; num_op < NUM_OPS_PER_OUTER_LOOP; ++num_op) {
        const auto start = std::chrono::high_resolution_clock::now();

        for (uint32_t loop = 0; loop < NUM_OPS_PER_INNER_LOOP; ++loop) {
            // Read 4096 Byte per iteration
            // 64 loop == 1 op
            for (int i = 0; i < 64; ++i) {
//                __m512i data = _mm512_stream_load_si512(cur_addr);
                __m512i data = _mm512_load_si512(cur_addr);
                const size_t offset = *(size_t *) &data;
                cur_addr = base_addr + offset;
            }
        }

        const auto end = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

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
    uint64_t duration = 0;
    char* cur_addr = next_addr();

    for (uint32_t num_op = 0; num_op < NUM_OPS_PER_OUTER_LOOP; ++num_op) {
        const auto start = std::chrono::high_resolution_clock::now();

        for (uint32_t loop = 0; loop < NUM_OPS_PER_INNER_LOOP; ++loop) {
            // Write 4096 Byte per iteration
            // 64 loop == 1 op == 4KiB
            for (int i = 0; i < 64; ++i) {
                WRITE(cur_addr);
                cur_addr = next_addr();
            }
        }

        const auto end = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    return duration;
}

inline size_t do_mem_op(benchmark::State& state, const size_t bm_type) {
    const size_t total_mem_range = state.threads * THREAD_CHUNK_SIZE;
    const size_t thread_jump = state.threads * JUMP_4K;
    char* base_addr = bm_data.storage_type == DRAM_BM ? bm_data.dram_data : bm_data.pmem_data;
    char* start_addr = base_addr + (state.thread_index * JUMP_4K);

    if (bm_type == RND_READ) {
        start_addr = base_addr + bm_data.thread_starts[state.thread_index];
    }

    switch (bm_type) {
        case SEQ_WRITE: return write_nt(start_addr, thread_jump);
        case SEQ_READ:  return read_nt(start_addr, thread_jump);
        case RND_WRITE: return rnd_write(base_addr, total_mem_range, state.thread_index);
        case RND_READ:  return rnd_read(start_addr, base_addr);
        default:        throw std::runtime_error("not supported");
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
    switch (storage_type) {
        case DRAM_BM: {
            void* data = mmap(nullptr, data_len, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_POPULATE, -1, 0);
            check_mmap(data);
            bm_data.dram_data = (char*) data;
            label << "dram";
            break;
        }
        case PMEM_BM: {
            const auto pmem_file = random_file(DB_NVM_DIR + std::string("/latency"));
            const int fd = ::open(pmem_file.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0600);
            check_fd(fd, pmem_file);
            if (ftruncate(fd, data_len) != 0) throw std::runtime_error("Cannot truncate");
            void* data = mmap(nullptr, data_len, viper::VIPER_MAP_PROT, viper::VIPER_MAP_FLAGS, fd, 0);
            check_mmap(data);
            bm_data.pmem_file = pmem_file;
            bm_data.pmem_fd = fd;
            bm_data.pmem_data = (char*) data;
            label << "pmem";
            break;
        }
        case SSD_BM: {
            bm_data.ssd_file = random_file(DB_FILE_DIR + std::string("/latency"));
            label << "ssd";
            break;
        }
        default:
            throw std::runtime_error("Unknown BM TYPE " + std::to_string(storage_type));
    }

    // prepare file to read from
    std::vector<std::thread> threads{};
    char* data = (storage_type == DRAM_BM) ? bm_data.dram_data : bm_data.pmem_data;
    if (bm_type == SEQ_READ) {
        auto write_fn = [](char* addr, size_t idx, size_t num_threads) {
            write_nt(addr + (idx * JUMP_4K), JUMP_4K * num_threads);
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

    std::string bm_type_label;
    switch (bm_type) {
        case (SEQ_READ)  : bm_type_label = "seq_read";  break;
        case (RND_READ)  : bm_type_label = "rnd_read";  break;
        case (SEQ_WRITE) : bm_type_label = "seq_write"; break;
        case (RND_WRITE) : bm_type_label = "rnd_write"; break;
    }
    label << "-threads_" << state.threads << "-" << bm_type_label;
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
        switch (storage_type) {
            case DRAM_BM:
            case PMEM_BM: {
                duration = do_mem_op(state, bm_type);
                break;
            }
            case SSD_BM: {
                break;
            }
        }
    }

    const size_t avg_lat = duration / NUM_ACCESSES_PER_THREAD;
    state.counters["avg_latency_ns"] = benchmark::Counter(avg_lat, benchmark::Counter::kAvgThreads);

    const double thread_dur_s = duration / 1e9;
    state.counters["total_dur_s"] = benchmark::Counter(thread_dur_s, benchmark::Counter::kAvgThreads);

    state.SetBytesProcessed(THREAD_CHUNK_SIZE);

    if (is_init_thread(state)) {
        munmap(bm_data.dram_data, data_len);
        munmap(bm_data.pmem_data, data_len);
        munmap(bm_data.ssd_data, data_len);

        ::close(bm_data.pmem_fd);
        ::close(bm_data.ssd_fd);

        std::filesystem::remove(bm_data.pmem_file);
        std::filesystem::remove(bm_data.ssd_file);
    }

}

BENCHMARK(latency_bw_bm)
    ->Repetitions(1)->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime()
    ->Threads(1) // Warmup run
    ->Threads(1)
    ->Threads(4)
    ->Threads(18)
    ->Threads(36)
//    ->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(24)->Threads(36)
    ->Args({DRAM_BM, RND_WRITE})->Args({PMEM_BM, RND_WRITE})
    ->Args({DRAM_BM, RND_READ})->Args({PMEM_BM, RND_READ})
    ->Args({DRAM_BM, SEQ_WRITE})->Args({PMEM_BM, SEQ_WRITE})
    ->Args({DRAM_BM, SEQ_READ}) ->Args({PMEM_BM, SEQ_READ});

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("latency_bw/latency_bw");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
