#include <string>
#include <random>

#include <benchmark/benchmark.h>

#include "benchmark.hpp"
#include "fixtures/common_fixture.hpp"
#include "fixtures/pmem_kv_fixture.hpp"
#include "fixtures/viper_fixture.hpp"
#include "fixtures/rocksdb_fixture.hpp"
#include "fixtures/faster_fixture.hpp"
#include "fixtures/dram_map_fixture.hpp"

using namespace viper::kv_bm;



int main(int argc, char** argv) {
    std::string exec_name = argv[0];
    const std::string arg = get_output_file("ycsb/ycsb");
    return bm_main({exec_name, arg});
//    return bm_main({exec_name});
}