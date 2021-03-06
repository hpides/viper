#include <string>
#include <random>

#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/common_fixture.hpp"
#include "fixtures/viper_fixture.hpp"
#include "fixtures/utree_fixture.hpp"
#include "fixtures/faster_fixture.hpp"
#include "fixtures/dash_fixture.hpp"
#include "fixtures/tbb_fixture.hpp"
#include "fixtures/crl_fixture.hpp"
#include "fixtures/pmem_kv_fixture.hpp"
#include "fixtures/rocksdb_fixture.hpp"

using namespace viper::kv_bm;

constexpr size_t ALL_OPS_NUM_REPETITIONS = 1;
constexpr size_t ALL_OPS_NUM_PREFILLS = 100'000'000;
constexpr size_t ALL_OPS_NUM_INSERTS = 50'000'000;
constexpr size_t ALL_OPS_NUM_FINDS = 50'000'000;
constexpr size_t ALL_OPS_NUM_UPDATES = 50'000'000;
constexpr size_t ALL_OPS_NUM_DELETES = 50'000'000;

#define GENERAL_ARGS \
              Repetitions(ALL_OPS_NUM_REPETITIONS) \
            ->Iterations(1) \
            ->Unit(BM_TIME_UNIT) \
            ->UseRealTime() \
            ->ThreadRange(1, NUM_MAX_THREADS) \
            ->Threads(24)


#define DEFINE_BM(fixture, method) \
            BENCHMARK_TEMPLATE2_DEFINE_F(fixture, method, KeyType16, ValueType200)(benchmark::State& state) { \
                bm_##method(state, *this); \
            } \
            BENCHMARK_REGISTER_F(fixture, method)->GENERAL_ARGS

#define BM_INSERT(fixture) DEFINE_BM(fixture, insert)->Args({ALL_OPS_NUM_PREFILLS, ALL_OPS_NUM_INSERTS})
#define BM_FIND(fixture)   DEFINE_BM(fixture, get)   ->Args({ALL_OPS_NUM_PREFILLS, ALL_OPS_NUM_FINDS})
#define BM_UPDATE(fixture) DEFINE_BM(fixture, update)->Args({ALL_OPS_NUM_PREFILLS, ALL_OPS_NUM_UPDATES})
#define BM_DELETE(fixture) DEFINE_BM(fixture, delete)->Args({ALL_OPS_NUM_PREFILLS, ALL_OPS_NUM_DELETES})

#define ALL_BMS(fixture) \
            BM_INSERT(fixture); \
            BM_FIND(fixture); \
            BM_UPDATE(fixture); \
            BM_DELETE(fixture)


void bm_insert(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = (num_total_inserts / state.threads) + 1;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = std::min(start_idx + num_inserts_per_thread, num_total_prefill + num_total_inserts);

    uint64_t insert_counter = 0;
    for (auto _ : state) {
        auto start_op = std::chrono::high_resolution_clock::now();
        insert_counter = fixture.setup_and_insert(start_idx, end_idx);
        auto end_op = std::chrono::high_resolution_clock::now();
        state.counters["insert-ns"] = (end_op - start_op).count();
    }

    state.SetItemsProcessed(num_inserts_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, insert_counter, end_idx - start_idx);
}

void bm_update(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_updates = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill);
    }

    const uint64_t num_updates_per_thread = num_total_updates / state.threads;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefill - state.threads;

    uint64_t update_counter = 0;
    for (auto _ : state) {
        auto start_op = std::chrono::high_resolution_clock::now();
        update_counter = fixture.setup_and_update(start_idx, end_idx, num_updates_per_thread);
        auto end_op = std::chrono::high_resolution_clock::now();
        state.counters["update-ns"] = (end_op - start_op).count();
    }

    state.SetItemsProcessed(num_updates_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, update_counter, num_updates_per_thread);
}

void bm_get(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefills);
    }

    const uint64_t num_finds_per_thread = (num_total_finds / state.threads) + 1;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefills - state.threads;

    uint64_t found_counter = 0;
    for (auto _ : state) {
        auto start_op = std::chrono::high_resolution_clock::now();
        found_counter = fixture.setup_and_find(start_idx, end_idx, num_finds_per_thread);
        auto end_op = std::chrono::high_resolution_clock::now();
        state.counters["get-ns"] = (end_op - start_op).count();
    }

    state.SetItemsProcessed(num_finds_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, found_counter, num_finds_per_thread);
}

void bm_delete(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_deletes = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefills);
    }

    const uint64_t num_deletes_per_thread = (num_total_deletes / state.threads) + 1;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefills - state.threads;

    uint64_t found_counter = 0;
    for (auto _ : state) {
        auto start_op = std::chrono::high_resolution_clock::now();
        found_counter = fixture.setup_and_delete(start_idx, end_idx, num_deletes_per_thread);
        auto end_op = std::chrono::high_resolution_clock::now();
        state.counters["delete-ns"] = (end_op - start_op).count();
    }

    state.SetItemsProcessed(found_counter);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, found_counter, found_counter);
}

//ALL_BMS(DashFixture);
ALL_BMS(ViperFixture);
//ALL_BMS(PmemHybridFasterFixture);
//ALL_BMS(PmemKVFixture);
//ALL_BMS(CrlFixture);
//ALL_BMS(UTreeFixture);
//ALL_BMS(PmemRocksDbFixture);


int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("all_ops/all_ops");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
