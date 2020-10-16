#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

constexpr size_t RECLAIM_NUM_PREFILLS = 100'000'000;
constexpr size_t RECLAIM_NUM_DELETES = 33'000'000;

template <typename VFixture>
inline void bm_reclaim(benchmark::State& state, VFixture& fixture) {
    viper::ViperConfig v_config{};
    v_config.enable_reclamation = true;
    v_config.reclaim_free_percentage = 0.1;

    fixture.InitMap(RECLAIM_NUM_PREFILLS, v_config);
    fixture.setup_and_delete(0, RECLAIM_NUM_PREFILLS - 1, RECLAIM_NUM_DELETES);

    uint64_t reclaim_time_ms = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        // For this benchmark, the `reclaim` method needs to be made public in Viper.
        fixture.getViper()->reclaim();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        reclaim_time_ms = duration.count();
    }

    std::cout << "RECLAIM TIME: " << reclaim_time_ms << std::endl;
    state.counters["reclaim_time_ms"] = reclaim_time_ms;

    fixture.DeInitMap();
}

BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, reclaim_fixed, KeyType16, ValueType200)(benchmark::State& state) { \
    bm_reclaim(state, *this);
}
BENCHMARK_REGISTER_F(ViperFixture, reclaim_fixed)
    ->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime();

//BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, reclaim_var, std::string, std::string)(benchmark::State& state) { \
//    bm_reclaim(state, *this);
//}
//BENCHMARK_REGISTER_F(ViperFixture, reclaim_var)
//    ->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime();

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
//    const std::string arg = get_output_file("reclaim/reclaim");
//    return bm_main({exec_name, arg});
    return bm_main({exec_name});
}
