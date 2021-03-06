#include <string>
#include <random>
#include <fstream>

#include <benchmark/benchmark.h>
#include <hdr_histogram.h>

#include "benchmark.hpp"
#include "fixtures/common_fixture.hpp"
#include "fixtures/viper_fixture.hpp"
#include "fixtures/faster_fixture.hpp"
#include "fixtures/crl_fixture.hpp"
#include "fixtures/dash_fixture.hpp"
//#include "fixtures/rocksdb_fixture.hpp"
#include "fixtures/pmem_kv_fixture.hpp"
#include "fixtures/ycsb_common.hpp"

#define YCSB_BM
#define UTREE_KEY_T viper::kv_bm::KeyType8
#include "fixtures/utree_fixture.hpp"

using namespace viper::kv_bm;

static constexpr char BASE_DIR[] = "/mnt/nvme2/viper";
static constexpr char PREFILL_FILE[] = "/ycsb_prefill.dat";

#define GENERAL_ARGS \
            ->Repetitions(1) \
            ->Iterations(1) \
            ->Unit(BM_TIME_UNIT) \
            ->UseRealTime() \
            ->ThreadRange(1, NUM_MAX_THREADS) \
            ->Threads(24)
//            ->ThreadRange(1, 18) \

#define DEFINE_BM(fixture, workload, data) \
            BENCHMARK_TEMPLATE2_DEFINE_F(fixture, workload ## _tp, KeyType8, ValueType200)(benchmark::State& state) { \
                ycsb_run(state, *this, &data, \
                    std::string{BASE_DIR} + "/ycsb_wl_" #workload ".dat", false); \
            } \
            BENCHMARK_REGISTER_F(fixture, workload ## _tp) GENERAL_ARGS;  \
            BENCHMARK_TEMPLATE2_DEFINE_F(fixture, workload ## _lat, KeyType8, ValueType200)(benchmark::State& state) { \
                ycsb_run(state, *this, &data, \
                    std::string{BASE_DIR} + "/ycsb_wl_" #workload ".dat", true); \
            } \
            BENCHMARK_REGISTER_F(fixture, workload ## _lat) GENERAL_ARGS

#define ALL_BMS(fixture) \
            DEFINE_BM(fixture, 5050_uniform, data_uniform_50_50); \
            DEFINE_BM(fixture, 1090_uniform, data_uniform_10_90); \
            DEFINE_BM(fixture, 5050_zipf,    data_zipf_50_50); \
            DEFINE_BM(fixture, 1090_zipf,    data_zipf_10_90)


static std::vector<ycsb::Record> prefill_data;
static std::vector<ycsb::Record> data_uniform_50_50;
static std::vector<ycsb::Record> data_uniform_10_90;
static std::vector<ycsb::Record> data_zipf_50_50;
static std::vector<ycsb::Record> data_zipf_10_90;

void ycsb_run(benchmark::State& state, BaseFixture& fixture, std::vector<ycsb::Record>* data,
              const std::filesystem::path& wl_file, bool log_latency) {
    set_cpu_affinity(state.thread_index);

    if (is_init_thread(state)) {
        fixture.InitMap();
        fixture.prefill_ycsb(prefill_data);
        if (data->empty()) {
            std::cout << "Reading workload file: " << wl_file << std::endl;
            ycsb::read_workload_file(wl_file, data);
            std::cout << "Done reading workload file." << std::endl;
        }
        hdr_init(1, 1000000000, 4, &fixture.hdr_);
    }

    struct hdr_histogram* hdr;
    if (log_latency) {
        hdr_init(1, 1000000000, 4, &hdr);
    } else {
        hdr = nullptr;
    }

    uint64_t start_idx = 0;
    uint64_t end_idx = 0;
    uint64_t op_counter = 0;
    for (auto _ : state) {
        // Need to do this in here as data might not be loaded yet.
        const uint64_t num_total_ops = data->size();
        const uint64_t num_ops_per_thread = num_total_ops / state.threads;
        start_idx = state.thread_index * num_ops_per_thread;
        end_idx = start_idx + num_ops_per_thread;

        // Actual benchmark
        op_counter = fixture.run_ycsb(start_idx, end_idx, *data, hdr);

        state.SetItemsProcessed(num_ops_per_thread);
        if (log_latency) {
            fixture.merge_hdr(hdr);
            hdr_close(hdr);
        }
    }

    if (is_init_thread(state)) {
        if (log_latency) {
            hdr_histogram* global_hdr = fixture.get_hdr();
            state.counters["hdr_max"] = hdr_max(global_hdr);
            state.counters["hdr_avg"] = hdr_mean(global_hdr);
            state.counters["hdr_min"] = hdr_min(global_hdr);
            state.counters["hdr_std"] = hdr_stddev(global_hdr);
            state.counters["hdr_median"] = hdr_value_at_percentile(global_hdr, 50.0);
            state.counters["hdr_90"] = hdr_value_at_percentile(global_hdr, 90.0);
            state.counters["hdr_95"] = hdr_value_at_percentile(global_hdr, 95.0);
            state.counters["hdr_99"] = hdr_value_at_percentile(global_hdr, 99.0);
            state.counters["hdr_999"] = hdr_value_at_percentile(global_hdr, 99.9);
            state.counters["hdr_9999"] = hdr_value_at_percentile(global_hdr, 99.99);
            // hdr_percentiles_print(global_hdr, stdout, 3, 1.0, CLASSIC);
            hdr_close(global_hdr);
        }

        fixture.DeInitMap();
    }

    if (op_counter == 0) {
        BaseFixture::log_find_count(state, op_counter, end_idx - start_idx);
    }
}

ALL_BMS(ViperFixture);
//ALL_BMS(PmemKVFixture);
//ALL_BMS(UTreeFixture);
//ALL_BMS(CrlFixture);
//ALL_BMS(DashFixture);

//ALL_BMS(RocksDbFixture);
//ALL_BMS(PmemHybridFasterFixture);


int main(int argc, char** argv) {
    std::cout << "Prefilling data..." << std::endl;
    std::filesystem::path prefill_file = BASE_DIR + std::string{PREFILL_FILE};
    ycsb::read_workload_file(prefill_file, &prefill_data);

    std::string exec_name = argv[0];
    const std::string arg = get_output_file("ycsb/ycsb");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}
