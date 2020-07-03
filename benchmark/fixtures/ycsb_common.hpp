#pragma once

#include <filesystem>
#include <vector>
#include "../benchmark.hpp"

namespace viper::kv_bm::ycsb {

struct Record {
    enum Op : uint32_t { INSERT = 0, GET = 1, UPDATE = 2 };
    // This will be overwritten by the file read.
    const Op op = INSERT;
    const KeyType8 key;
    const ValueType200 value;
};

void read_workload_file(const std::filesystem::path wl_file, std::vector<Record>* data);

}


