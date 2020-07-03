#include "ycsb_common.hpp"

#include <fstream>
#include <cstring>

namespace viper::kv_bm::ycsb {

void read_workload_file(const std::filesystem::path wl_file, std::vector<Record>* data) {
    std::ifstream ifs{wl_file, std::ios::binary | std::ios::ate};

    if (!ifs) {
        throw std::runtime_error("Cannot open " + wl_file.string() + ": " + std::strerror(errno));
    }

    auto end = ifs.tellg();
    ifs.seekg(0, std::ios::beg);
    auto size = std::size_t(end - ifs.tellg());

    if (size == 0) {
        throw std::runtime_error(wl_file.string() + " is empty.");
    }

    assert(size % sizeof(Record) == 0);
    const uint64_t num_records = size / sizeof(Record);
    data->resize(num_records);

    if (!ifs.read((char*) data->data(), size)) {
        throw std::runtime_error("Error reading from " + wl_file.string() + ": " + std::strerror(errno));
    }
}

}