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

int main() {
    const std::string pool_file = "/dev/dax0.0";

    ViperT viper{pool_file, POOL_SIZE};

    auto v_client = viper.get_client();
    const int num_values = 10000000;
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
