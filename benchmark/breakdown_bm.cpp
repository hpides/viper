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
        v_config.track_op_breakdown = true;
        fixture.InitMap(num_total_prefill, v_config);
    }

    const uint64_t num_ops_per_thread = (num_total_ops / state.threads) + 1;
    const uint64_t start_idx = 0;
    const uint64_t end_idx = num_total_prefill;

    state.counters["insert-lock"] = 0;
    state.counters["insert-write"] = 0;
    state.counters["insert-update"] = 0;
    state.counters["get-map"] = 0;
    state.counters["get-fetch"] = 0;
    state.counters["update-map"] = 0;
    state.counters["update-fetch"] = 0;
    state.counters["update-modify"] = 0;
    state.counters["update-write"] = 0;
    state.counters["delete-lock"] = 0;
    state.counters["delete-write"] = 0;
    state.counters["delete-update"] = 0;

    for (auto _ : state) {
        auto* viper = fixture.getViper();
        auto client = viper->get_client();

        std::random_device rnd{};
        auto rnd_engine = std::default_random_engine(rnd());
        std::uniform_int_distribution<> distrib(start_idx, end_idx);
        ValueType200 null_value{-1ul};

        for (uint32_t op_num = 0; op_num < num_ops_per_thread; ++op_num) {
            uint64_t key = static_cast<uint64_t>(distrib(rnd_engine));

            const uint8_t op = key % 4;
            if (op == 0) {
                // Insert
                const uint64_t lock_wait_before = client.lock_wait_ns_;
                const uint64_t pmem_write_before = client.pmem_write_ns_;
                const uint64_t map_update_before = client.map_update_ns_;

                {
                    KeyType16 db_key{num_total_prefill + key};
                    ValueType200 value{num_total_prefill + key};
                    client.put(db_key, value);
                }

                const uint64_t lock_wait_new = client.lock_wait_ns_ - lock_wait_before;
                const uint64_t pmem_write_new = client.pmem_write_ns_ - pmem_write_before;
                const uint64_t map_update_new = client.map_update_ns_ - map_update_before;

                state.counters["insert-lock"] += lock_wait_new;
                state.counters["insert-write"] += pmem_write_new;
                state.counters["insert-update"] += map_update_new;
            } else if (op == 1) {
                // Get
                const uint64_t map_fetch_before = client.map_fetch_ns_;
                const uint64_t value_fetch_before = client.value_fetch_ns_;
                uint64_t read_ns;

                {
                    KeyType16 db_key{key};
                    typename VFixture::ViperT::Accessor result;
                    if (client.get(db_key, result)) {
                        const auto start = std::chrono::high_resolution_clock::now();
                        bool eq = (*result != null_value);
                        benchmark::DoNotOptimize(eq);
                        const auto end = std::chrono::high_resolution_clock::now();
                        read_ns = (end - start).count();
                    }
                }

                const uint64_t map_fetch_new = client.map_fetch_ns_ - map_fetch_before;
                const uint64_t value_fetch_new = client.value_fetch_ns_ - value_fetch_before;

                state.counters["get-map"] += map_fetch_new;
                state.counters["get-fetch"] += value_fetch_new;
                state.counters["get-read"] += read_ns;

            } else if (op == 3) {
                // Update
                const uint64_t map_fetch_before = client.map_fetch_ns_;
                const uint64_t value_fetch_before = client.value_fetch_ns_;
                const uint64_t value_modify_before = client.value_modify_ns_;
                uint64_t write_ns;

                auto update_fn = [&](ValueType200* value) {
                    const auto start = std::chrono::high_resolution_clock::now();
                    value->update_value();
                    pmem_persist(value, sizeof(uint64_t));
                    const auto end = std::chrono::high_resolution_clock::now();
                    write_ns = (end - start).count();
                };

                {
                    KeyType16 db_key{key};
                    client.update(db_key, update_fn);
                }

                const uint64_t map_fetch_new = client.map_fetch_ns_ - map_fetch_before;
                const uint64_t value_fetch_new = client.value_fetch_ns_ - value_fetch_before;
                const uint64_t value_modify_new = client.value_modify_ns_ - value_modify_before;

                state.counters["update-map"] += map_fetch_new;
                state.counters["update-fetch"] += value_fetch_new;
                state.counters["update-modify"] += value_modify_new;
                state.counters["update-write"] += write_ns;
            } else {
                // Delete
                const uint64_t lock_wait_before = client.lock_wait_ns_;
                const uint64_t pmem_write_before = client.pmem_write_ns_;
                const uint64_t map_update_before = client.map_update_ns_;

                {
                    KeyType16 db_key{key};
                    client.remove(db_key);
                }

                const uint64_t lock_wait_new = client.lock_wait_ns_ - lock_wait_before;
                const uint64_t pmem_write_new = client.pmem_write_ns_ - pmem_write_before;
                const uint64_t map_update_new = client.map_update_ns_ - map_update_before;

                state.counters["delete-lock"] += lock_wait_new;
                state.counters["delete-write"] += pmem_write_new;
                state.counters["delete-update"] += map_update_new;
            }
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
    ->Threads(1)->Threads(36);

int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("breakdown/breakdown");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
