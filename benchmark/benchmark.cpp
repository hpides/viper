#include <string>
#include <random>

#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/common_fixture.hpp"
#include "fixtures/dram_map_fixture.hpp"
#include "fixtures/pmem_map_fixture.hpp"
#include "fixtures/hybrid_map_fixture.hpp"
#include "fixtures/viper_fixture.hpp"
#include "fixtures/rocksdb_fixture.hpp"
#include "fixtures/faster_fixture.hpp"
#include "fixtures/dash_fixture.hpp"


using namespace viper::kv_bm;

inline void bm_insert_empty(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_inserts = state.range(0);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap();
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = state.thread_index * num_inserts_per_thread;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        fixture.insert_empty(start_idx, end_idx);
    }

    state.SetItemsProcessed(num_inserts_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }
}

inline void bm_setup_and_insert(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefill = state.range(0);
    const uint64_t num_total_inserts = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefill);
    }

    const uint64_t num_inserts_per_thread = num_total_inserts / state.threads;
    const uint64_t start_idx = (state.thread_index * num_inserts_per_thread) + num_total_prefill;
    const uint64_t end_idx = start_idx + num_inserts_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_inserts * 1000);

    for (auto _ : state) {
        fixture.setup_and_insert(start_idx, end_idx);
    }

    state.SetItemsProcessed(num_inserts_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

}

inline void bm_setup_and_find(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_finds = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefills, /*re_init=*/false);
    }

    const uint64_t num_finds_per_thread = num_total_finds / state.threads;
    const uint64_t start_idx = state.thread_index * num_finds_per_thread;
    const uint64_t end_idx = start_idx + num_finds_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    uint64_t found_counter = 0;
    for (auto _ : state) {
        found_counter = fixture.setup_and_find(start_idx, end_idx);
    }

    state.SetItemsProcessed(num_finds_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, found_counter, num_finds_per_thread);
}

inline void bm_setup_and_delete(benchmark::State& state, BaseFixture& fixture) {
    const uint64_t num_total_prefills = state.range(0);
    const uint64_t num_total_deletes = state.range(1);

    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap(num_total_prefills);
    }

    const uint64_t num_deletes_per_thread = num_total_deletes / state.threads;
    const uint64_t start_idx = state.thread_index * num_deletes_per_thread;
    const uint64_t end_idx = start_idx + num_deletes_per_thread;

    std::uniform_int_distribution<uint64_t> uniform_distribution(0, num_total_prefills * 1000);

    uint64_t found_counter = 0;
    for (auto _ : state) {
        found_counter = fixture.setup_and_delete(start_idx, end_idx);
    }

    state.SetItemsProcessed(num_deletes_per_thread);

    if (is_init_thread(state)) {
        fixture.DeInitMap();
    }

    BaseFixture::log_find_count(state, found_counter, num_deletes_per_thread);
}


BENCHMARK_DEFINE_F(DramMapFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(DramMapFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(DramMapFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(DramMapFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

BENCHMARK_DEFINE_F(PmemMapFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(PmemMapFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(PmemMapFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(PmemMapFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

BENCHMARK_DEFINE_F(HybridMapFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(HybridMapFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(HybridMapFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(HybridMapFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

BENCHMARK_DEFINE_F(ViperFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(ViperFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(ViperFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(ViperFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

BENCHMARK_DEFINE_F(RocksDbFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(RocksDbFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(RocksDbFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(RocksDbFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

BENCHMARK_DEFINE_F(FasterFixture, insert_empty)(benchmark::State& state) {
    bm_insert_empty(state, *this);
}

BENCHMARK_DEFINE_F(FasterFixture, setup_and_insert)(benchmark::State& state) {
    bm_setup_and_insert(state, *this);
}

BENCHMARK_DEFINE_F(FasterFixture, setup_and_find)(benchmark::State& state) {
    bm_setup_and_find(state, *this);
}

BENCHMARK_DEFINE_F(FasterFixture, setup_and_delete)(benchmark::State& state) {
    bm_setup_and_delete(state, *this);
}

//BENCHMARK_DEFINE_F(DashLinearFixture, insert_empty)(benchmark::State& state) {
//    bm_insert_empty(state, *this);
//}
//
//BENCHMARK_DEFINE_F(DashLinearFixture, setup_and_insert)(benchmark::State& state) {
//    bm_setup_and_insert(state, *this);
//}
//
//BENCHMARK_DEFINE_F(DashLinearFixture, setup_and_find)(benchmark::State& state) {
//    bm_setup_and_find(state, *this);
//}
//

//BENCHMARK_REGISTER_F(DramMapFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(DramMapFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(DramMapFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
BENCHMARK_REGISTER_F(DramMapFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(PmemMapFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
BENCHMARK_REGISTER_F(PmemMapFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(HybridMapFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(HybridMapFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(HybridMapFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
BENCHMARK_REGISTER_F(HybridMapFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(RocksDbFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(RocksDbFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(RocksDbFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
BENCHMARK_REGISTER_F(RocksDbFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(FasterFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(FasterFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(FasterFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
BENCHMARK_REGISTER_F(FasterFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);

//BENCHMARK_REGISTER_F(DashLinearFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);

//BENCHMARK_REGISTER_F(DashLinearFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(DashLinearFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(ViperFixture, insert_empty)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Arg(NUM_INSERTS)
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(ViperFixture, setup_and_insert)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_INSERTS})
//    ->ThreadRange(1, NUM_MAX_THREADS);
//
//BENCHMARK_REGISTER_F(ViperFixture, setup_and_find)
//    ->Repetitions(NUM_REPETITIONS)
//    ->Iterations(1)
//    ->Unit(BM_TIME_UNIT)
//    ->UseRealTime()
//    ->Args({NUM_PREFILLS, NUM_FINDS})
//    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_REGISTER_F(ViperFixture, setup_and_delete)
    ->Repetitions(NUM_REPETITIONS)
    ->Iterations(1)
    ->Unit(BM_TIME_UNIT)
    ->UseRealTime()
    ->Args({NUM_PREFILLS, NUM_DELETES})
    ->ThreadRange(1, NUM_MAX_THREADS);

BENCHMARK_MAIN();
