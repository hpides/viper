#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/viper_fixture.hpp"

using namespace viper::kv_bm;

using VFixture = ViperFixture<KeyType16, ValueType200>;

constexpr size_t BREAKDOWN_NUM_PREFILLS = 100'000'000;
constexpr size_t BREAKDOWN_OPS = 50'000'000;

void bm_breakdown(benchmark::State& state, VFixture& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_ops = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        viper::ViperConfig v_config{};
        v_config.track_op_breakdown = false;
        fixture.InitMap(num_total_prefill, v_config);
    }

    const uint64_t num_ops_per_thread = (num_total_ops / state.threads) + 1;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefill;

    for (const std::string& ot : {"insert", "get", "update", "delete"}) {
        state.counters[ot + "-lock"] = 0;
        state.counters[ot + "-map"] = 0;
        state.counters[ot + "-pmem"] = 0;
    }

    for (auto _ : state) {
        auto* viper = fixture.getViper();
        auto client = viper->get_client();

        std::random_device rnd{};
        auto rnd_engine = std::default_random_engine(rnd());
        std::uniform_int_distribution<> distrib(start_idx, end_idx);
        ValueType200 null_value{-1ul};
        ValueType200 result;
        size_t thread_insert_key = num_total_prefill + (num_ops_per_thread * state.thread_index);

        size_t op_counter = 0;
        std::string op_type;
        for (uint32_t op_num = 0; op_num < num_ops_per_thread; ++op_num) {
            auto start_op = std::chrono::high_resolution_clock::now();
            uint64_t key = static_cast<uint64_t>(distrib(rnd_engine));

            const uint64_t lock_wait_before = client.lock_wait_ns_;
            const uint64_t pmem_before = client.pmem_ns_;
            const uint64_t map_before = client.map_ns_;

            const uint8_t op = key % 4;
            if (op == 0) {
                op_type = "insert";
                KeyType16 db_key{thread_insert_key};
                ValueType200 value{thread_insert_key};
                op_counter += client.put(db_key, value);
                thread_insert_key++;
            } else if (op == 1) {
                op_type = "get";
                KeyType16 db_key{key};
                client.get(db_key, &result);
                bool eq = (result != null_value);
                benchmark::DoNotOptimize(eq);
            } else if (op == 2) {
                op_type = "update";
                auto update_fn = [&](ValueType200* value) {
                    value->update_value();
                    pmem_persist(value, sizeof(uint64_t));
                };

                KeyType16 db_key{key};
                op_counter += client.update(db_key, update_fn);
            } else {
                op_type = "delete";
                KeyType16 db_key{key};
                client.remove(db_key);
            }

            auto end_op = std::chrono::high_resolution_clock::now();

            const uint64_t lock_wait_new = client.lock_wait_ns_ - lock_wait_before;
            const uint64_t pmem_write_new = client.pmem_ns_ - pmem_before;
            const uint64_t map_update_new = client.map_ns_ - map_before;

            state.counters[op_type + "-lock"] += lock_wait_new;
            state.counters[op_type + "-pmem"] += pmem_write_new;
            state.counters[op_type + "-map"] += map_update_new;
            state.counters[op_type + "-ops"] += 1;
            state.counters[op_type + "-total"] += (end_op - start_op).count();
        }
    }

    state.SetItemsProcessed(num_ops_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }
}

BENCHMARK_TEMPLATE2_DEFINE_F(ViperFixture, breakdown, KeyType16, ValueType200)(benchmark::State& state) { \
    bm_breakdown(state, *this);
}

BENCHMARK_REGISTER_F(ViperFixture, breakdown)
        ->Iterations(1)->Unit(BM_TIME_UNIT)->UseRealTime()
        ->Args({BREAKDOWN_NUM_PREFILLS, BREAKDOWN_OPS})
        ->Threads(36);
//        ->Threads(1)

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("breakdown/breakdown");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}


