#include "src/viper.hpp"
#include <iostream>

using namespace std;
using namespace viper;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024 * 1;

struct VKey {
    VKey() {}
    VKey(uint64_t key) {
        uuid[0] = key;
        uuid[1] = key;
    }
    std::array<uint64_t, 2> uuid;
};

struct VValue {
    VValue() {}
    VValue(uint64_t value) {
        data.fill(value);
    }
    std::array<uint64_t, 25> data;
};

struct VKeyCompare {
    // Use same impl as tbb_hasher
    static const size_t hash_multiplier = tbb::internal::select_size_t_constant<2654435769U, 11400714819323198485ULL>::value;
    static size_t hash(const VKey a) {
        return static_cast<size_t>(a.uuid[0]) * hash_multiplier;
    }
    static bool equal(const VKey& a, const VKey& b) { return a.uuid[0] == b.uuid[0] && a.uuid[1] == b.uuid[1]; }
};

using ViperT = Viper<VKey, VValue, VKeyCompare>;

void zero_block_device(const std::string& block_dev, size_t length) {
    auto start = std::chrono::high_resolution_clock::now();
    int fd = open(block_dev.c_str(), O_RDWR);
    if (fd < 0) {
        throw std::runtime_error("Cannot open dax device: " + block_dev + " | " + std::strerror(errno));
    }

    void* addr = mmap(nullptr, length, PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == nullptr || addr == reinterpret_cast<void*>(0xffffffffffffffff)) {
        throw std::runtime_error("Cannot mmap pool file: " + block_dev + " | " + std::strerror(errno));
    }

    constexpr size_t buffer_size = 4096;
    std::array<char, buffer_size> buffer;
    buffer.fill(0);

    char* addr_start = static_cast<char*>(addr);
    for (size_t i = 0; i < length / buffer_size; ++i) {
        void* start_addr = addr_start + (i * buffer_size);
        memcpy(start_addr, &buffer, buffer_size);
    }

    munmap(addr, length);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    std::cout << "ZEROING DURATION: " << duration << std::endl;
}

int main() {
    const std::string pool_file = "/dev/dax0.0";
    const int num_values = 10000000;

    {
        zero_block_device(pool_file, POOL_SIZE * 3);
        ViperT viper = ViperT::create(pool_file, POOL_SIZE);
        auto v_client = viper.get_client();

        for (int i = 0; i < num_values; ++i) {
            v_client.put(i, i);
        }

        uint64_t found_counter = 0;
        for (int key = 0; key < num_values; ++key) {
            ViperT::ConstAccessor result;
            const bool found = v_client.get(key, result);
            found_counter += found;
        }
        cout << "FOUND IN TOTAL: " << found_counter << "/" << num_values << endl;
    }

    {
        ViperT viper = ViperT::open(pool_file);
        auto v_client = viper.get_const_client();

        uint64_t found_counter = 0;
        for (int key = 0; key < num_values; ++key) {
            ViperT::ConstAccessor result;
            const bool found = v_client.get(key, result);
            found_counter += found;
            if (!found) {
                std::cout << "NOT FOUND: " << key << std::endl;
            }
        }
        cout << "FOUND IN TOTAL: " << found_counter << "/" << num_values << endl;
    }


}
