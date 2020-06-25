#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

constexpr size_t ACCESS_NUM_REPITITIONS = 1;
constexpr size_t ACCESS_NUM_MAX_THREADS = 24;
constexpr size_t ACCESS_NUM_PREFILLS = NUM_PREFILLS;
constexpr size_t ACCESS_NUM_INSERTS = NUM_INSERTS;

template <typename KT, typename VT>
inline void bm_insert(benchmark::State& state, ViperFixture<KT, VT>& fixture, ViperConfig v_config) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill, v_config);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    for (auto _ : state) {
        fixture.setup_and_insert(start_idx, end_idx);
    }

    state.SetItemsProcessed(num_inserts_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }
}

BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, insert_dimm_based, KeyType16, ValueType200)(benchmark::State& state) {
    ViperConfig v_config{};
    v_config.force_dimm_based = true;
    bm_insert(state, *this, v_config);
}

BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, insert_block_based, KeyType16, ValueType200)(benchmark::State& state) {
    ViperConfig v_config{};
    v_config.force_block_based = true;
    bm_insert(state, *this, v_config);
}

BENCHMARK_REGISTER_F(ViperFixture, insert_dimm_based)
    ->Repetitions(ACCESS_NUM_REPITITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({ACCESS_NUM_PREFILLS, ACCESS_NUM_INSERTS})
    ->DenseThreadRange(17, ACCESS_NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(ViperFixture, insert_block_based)
    ->Repetitions(ACCESS_NUM_REPITITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({ACCESS_NUM_PREFILLS, ACCESS_NUM_INSERTS})
    ->DenseThreadRange(17, ACCESS_NUM_MAX_THREADS);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("access_pattern/bm");
    return bm_main({exec_name, arg});
}