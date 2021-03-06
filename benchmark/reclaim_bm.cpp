#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

constexpr size_t RECLAIM_NUM_OPS_PER_ITERATION = 50'000;
constexpr size_t RECLAIM_NUM_OP_THREADS = 32;
constexpr size_t RECLAIM_NUM_WRITE_IT =  200;
constexpr size_t RECLAIM_NUM_READ_IT =  4000;
constexpr size_t RECLAIM_NUM_MIXED_IT =  400;

std::atomic<bool> reclaim_done = false;

#define GENERAL_ARGS \
            ->Iterations(1) \
            ->Unit(BM_TIME_UNIT) \
            ->UseRealTime()

#define DEFINE_VAR_BM(method) \
    BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, reclaim_var ##_ ##method, std::string, std::string)(benchmark::State& state) { \
        bm_reclaim(state, *this, #method);  } \
    BENCHMARK_REGISTER_F(ViperFixture, reclaim_var ##_ ##method) GENERAL_ARGS \
     ->Threads(RECLAIM_NUM_OP_THREADS + 1)->Threads(RECLAIM_NUM_OP_THREADS);
    // Reclaim with extra thread            No reclaim

#define DEFINE_FIXED_BM(method) \
    BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, reclaim_fixed ##_ ##method, KeyType16, ValueType200)(benchmark::State& state) { \
        bm_reclaim(state, *this, #method);  } \
    BENCHMARK_REGISTER_F(ViperFixture, reclaim_fixed ##_ ##method) GENERAL_ARGS \
     ->Threads(RECLAIM_NUM_OP_THREADS + 1)->Threads(RECLAIM_NUM_OP_THREADS);
    // Reclaim with extra thread            No reclaim

template <typename VFixture>
inline void bm_reclaim(benchmark::State& state, VFixture& fixture, std::string method) {
    viper::ViperConfig v_config{};
    v_config.enable_reclamation = false;
    v_config.reclaim_free_percentage = 0;
    const bool should_reclaim = state.threads > RECLAIM_NUM_OP_THREADS;
    const bool is_reclaim_thread = state.thread_index == RECLAIM_NUM_OP_THREADS;

    set_cpu_affinity(state.thread_index);

    const size_t num_prefills = method == "READ" ? 100'000'000 : 10'000'000;
    const size_t num_deletes = num_prefills / 3;
    const size_t num_total_values = num_prefills * 2;

    if (is_init_thread(state)) {
        if constexpr (std::is_same_v<typename VFixture::KeyType, std::string>) {
            fixture.generate_strings(num_total_values, 16, 200);
        }

        fixture.InitMap(num_prefills, v_config);
        fixture.setup_and_delete(0, num_prefills - 1, num_deletes);
    }

    const size_t ops_per_thread = num_prefills / RECLAIM_NUM_OP_THREADS;
    size_t ops_performed = 0;

    uint64_t duration_ms = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        if (should_reclaim && is_reclaim_thread) {
            // For this benchmark, the `reclaim` method needs to be made public in Viper.
            fixture.getViper()->reclaim();
            reclaim_done.store(true);
        } else {
            size_t base_i = (state.thread_index * ops_per_thread) + num_prefills;
            const bool is_mixed = method == "MIXED";
            bool is_writing = method == "WRITE";
            bool is_reading = method == "READ";
            const size_t max_iterations = is_writing ?
                                            RECLAIM_NUM_WRITE_IT :
                                            (is_reading ? RECLAIM_NUM_READ_IT : RECLAIM_NUM_MIXED_IT);
            size_t iterations = 0;
            assert(is_writing || is_mixed || is_reading);

            size_t start_i = base_i;
            while (!reclaim_done.load()) {
                if (start_i + RECLAIM_NUM_OPS_PER_ITERATION > ops_per_thread) {
                    start_i = base_i;
                }
                size_t end_i = start_i + RECLAIM_NUM_OPS_PER_ITERATION;

                if (is_writing) {
                    fixture.insert(start_i, end_i);
                    start_i += RECLAIM_NUM_OPS_PER_ITERATION;
                } else {
                    fixture.setup_and_find(0, num_prefills - 1, RECLAIM_NUM_OPS_PER_ITERATION);
                }

                if (is_mixed) {
                    is_writing = !is_writing;
                }

                ops_performed += RECLAIM_NUM_OPS_PER_ITERATION;
                if (!should_reclaim && ++iterations == max_iterations) {
                    reclaim_done.store(true);
                }
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        duration_ms = duration.count();
    }

    if (is_reclaim_thread) {
        state.counters["reclaim_time_ms"] = duration_ms;
    } else {
        state.counters["op_time_ms"] = duration_ms;
        state.SetItemsProcessed(ops_performed);
    }

    if (is_init_thread(state)) {
        fixture.DeInitMap();
        reclaim_done.store(false);
    }
}

//DEFINE_FIXED_BM(WRITE);
DEFINE_FIXED_BM(READ);
//DEFINE_FIXED_BM(MIXED);

//DEFINE_VAR_BM(READ);
//DEFINE_VAR_BM(WRITE);
//DEFINE_VAR_BM(MIXED);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("reclaim/reclaim");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
