#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

constexpr size_t UPDATE_NUM_REPETITIONS = 1;
constexpr size_t UPDATE_NUM_PREFILLS = 100'000'000;
constexpr size_t UPDATE_NUM_INSERTS = 50'000'000;

#define GENERAL_ARGS \
            ->Repetitions(UPDATE_NUM_REPETITIONS) \
            ->Iterations(1) \
            ->Unit(BM_TIME_UNIT) \
            ->UseRealTime() \
            ->ThreadRange(1, NUM_MAX_THREADS) \
            ->Threads(24)

#define DEFINE_BM_INTERNAL(fixture, method) \
        BENCHMARK_TEMPLATE2_DEFINE_F(fixture, method,  \
                                     KeyType16, ValueType200)(benchmark::State& state) { \
            bm_##method(state, *this); \
        } \
        BENCHMARK_REGISTER_F(fixture, method) GENERAL_ARGS

#define DEFINE_BM(fixture) \
        DEFINE_BM_INTERNAL(fixture, in_place_update) \
            ->Args({UPDATE_NUM_PREFILLS, UPDATE_NUM_INSERTS}); \
        DEFINE_BM_INTERNAL(fixture, get_update) \
            ->Args({UPDATE_NUM_PREFILLS, UPDATE_NUM_INSERTS})

inline void bm_in_place_update(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill);
    }

    const uint64_t num_updates_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefill - state.threads;

    for (auto _ : state) {
        fixture.setup_and_update(start_idx, end_idx, num_updates_per_thread);

    }

    state.SetItemsProcessed(num_updates_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }
}

template <typename KT, typename VT>
inline void bm_get_update(benchmark::State& state, ViperFixture<KT, VT>& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill);
    }

    const uint64_t num_updates_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefill - state.threads;

    size_t update_counter = 0;
    for (auto _ : state) {
        update_counter = fixture.setup_and_get_update(start_idx, end_idx, num_updates_per_thread);
    }

    state.SetItemsProcessed(num_updates_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, update_counter, num_updates_per_thread);
}

DEFINE_BM(ViperFixture);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("update/update");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
