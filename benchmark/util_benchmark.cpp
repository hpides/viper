#include <array>
#include <atomic>
#include <chrono>
#include <vector>
#include <iostream>

#include <benchmark/benchmark.h>
#include "fixtures/common_fixture.hpp"

static constexpr uint64_t NUM_THREADS = 16;
static constexpr uint64_t NUM_ATOMIC_OPS = 10000000;

using namespace viper::kv_bm;

struct AtomicRoot {
    std::array<std::atomic<uint64_t>, NUM_THREADS> counters;
    std::atomic<uint64_t> single_counter;
};

class AtomicFixturePmem : public BasePmemFixture<AtomicRoot> {
  public:
    void insert_empty(uint64_t start_idx, uint64_t end_idx) override {}
    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) override {}
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) override { return 0; }
};

class AtomicFixture : public benchmark::Fixture {
  public:
    AtomicFixture() : counters{}, single_counter{} {};
    std::array<std::atomic<uint64_t>, NUM_THREADS> counters;
    std::atomic<uint64_t> single_counter;
};

void load(benchmark::State& state, std::atomic<uint64_t>& counter) {
//    set_cpu_affinity(state.thread_index);
    set_cpu_affinity();

    const auto num_loads = state.range(0) / state.threads;

    for (auto _ : state) {
        uint64_t load_count = 0;
        auto start = std::chrono::high_resolution_clock::now();
        for (auto i = 0; i < num_loads; ++i) {
            load_count += counter.load(std::memory_order_acquire);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "DURATION THREAD #" << state.thread_index << ": " << duration << " us" << std::endl;
        state.SetItemsProcessed(num_loads);
    }
}

void increment(benchmark::State& state, std::atomic<uint64_t>& counter) {
//    set_cpu_affinity(state.thread_index);
    set_cpu_affinity();

    const auto num_increments = state.range(0) / state.threads;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        for (auto i = 0; i < num_increments; ++i) {
            ++counter;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "DURATION THREAD #" << state.thread_index << ": " << duration << " us" << std::endl;
        state.SetItemsProcessed(num_increments);
    }
}

void cas_increment(benchmark::State& state, std::atomic<uint64_t>& counter) {
//    set_cpu_affinity(state.thread_index);
    set_cpu_affinity();

    const auto num_increments = state.range(0) / state.threads;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        uint64_t expected = counter.load(std::memory_order_acquire);
        for (auto i = 0; i < num_increments; ++i) {
            while (!counter.compare_exchange_weak(expected, expected + 1)) {}
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        std::cout << "DURATION THREAD #" << state.thread_index << ": " << duration << " us" << std::endl;
        state.SetItemsProcessed(num_increments);
    }
}

BENCHMARK_DEFINE_F(AtomicFixture, load_single)(benchmark::State& state) {
    single_counter = 1;
    load(state, single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, load_single)(benchmark::State& state) {
    pmem_pool_.root()->single_counter = 1;
    load(state, pmem_pool_.root()->single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixture, load_individual)(benchmark::State& state) {
    counters[state.thread_index] = 1;
    load(state, counters[state.thread_index]);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, load_individual)(benchmark::State& state) {
    pmem_pool_.root()->counters[state.thread_index] = 1;
    load(state, pmem_pool_.root()->counters[state.thread_index]);
}

BENCHMARK_REGISTER_F(AtomicFixture, load_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS)->Iterations(1);
BENCHMARK_REGISTER_F(AtomicFixture, load_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS)->Iterations(1);
BENCHMARK_REGISTER_F(AtomicFixturePmem, load_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS)->Iterations(1);
BENCHMARK_REGISTER_F(AtomicFixturePmem, load_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS)->Iterations(1);


BENCHMARK_DEFINE_F(AtomicFixture, increment_single)(benchmark::State& state) {
    increment(state, single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, increment_single)(benchmark::State& state) {
    increment(state, pmem_pool_.root()->single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixture, increment_individual)(benchmark::State& state) {
    increment(state, counters[state.thread_index]);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, increment_individual)(benchmark::State& state) {
    increment(state, pmem_pool_.root()->counters[state.thread_index]);
}

BENCHMARK_REGISTER_F(AtomicFixture, increment_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixture, increment_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixturePmem, increment_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixturePmem, increment_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);

BENCHMARK_DEFINE_F(AtomicFixture, cas_increment_single)(benchmark::State& state) {
    cas_increment(state, single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, cas_increment_single)(benchmark::State& state) {
    cas_increment(state, pmem_pool_.root()->single_counter);
}

BENCHMARK_DEFINE_F(AtomicFixture, cas_increment_individual)(benchmark::State& state) {
    cas_increment(state, counters[state.thread_index]);
}

BENCHMARK_DEFINE_F(AtomicFixturePmem, cas_increment_individual)(benchmark::State& state) {
    cas_increment(state, pmem_pool_.root()->counters[state.thread_index]);
}

BENCHMARK_REGISTER_F(AtomicFixture, cas_increment_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixture, cas_increment_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixturePmem, cas_increment_single)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);
BENCHMARK_REGISTER_F(AtomicFixturePmem, cas_increment_individual)->Arg(NUM_ATOMIC_OPS)->Threads(NUM_THREADS);

BENCHMARK_MAIN();