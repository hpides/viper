#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

using VFixture = ViperFixture<KeyType16, ValueType200>;

constexpr size_t RECLAIM_NUM_PREFILLS = 100'000'000;

inline void bm_recovery(benchmark::State& state, VFixture& fixture) {
    const uint64_t num_total_prefill = RECLAIM_NUM_PREFILLS;
    const uint64_t recovery_threads = state.range(0);

    fixture.InitMap(num_total_prefill);
    fixture.DeInitMap();

    std::unique_ptr<VFixture::ViperT> viper;
    viper::ViperConfig v_config{};
    v_config.num_recovery_threads = recovery_threads;

    set_cpu_affinity(0, 36);
    uint64_t rec_time_ms = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        viper = VFixture::ViperT::open(VIPER_POOL_FILE, v_config);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        rec_time_ms = duration.count();
    }
    state.counters["rec_time_ms"] = rec_time_ms;
}

BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, recovery, KeyType16, ValueType200)(benchmark::State& state) { \
    bm_recovery(state, *this);
}

BENCHMARK_REGISTER_F(ViperFixture, recovery)
    ->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime()
    ->Arg(1)->Arg(2)->Arg(4)->Arg(8)
    ->Arg(16)->Arg(24)->Arg(32)->Arg(36);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("recovery/recovery");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
