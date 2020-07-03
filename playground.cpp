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
    VValue(uint64_t val) {
        data.fill(val);
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

int main() {
    const int num_values = 50'000'000;

    tbb::concurrent_hash_map<VKey, uint64_t, VKeyCompare> map;
    typename tbb::concurrent_hash_map<VKey, uint64_t, VKeyCompare>::accessor accessor;
    for (uint64_t i = 0; i < num_values; ++i) {
        map.insert(accessor, {i, i});
    }
}
