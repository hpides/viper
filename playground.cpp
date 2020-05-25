#include <libpmempool.h>
#include "src/viper.hpp"
#include <iostream>
#include <libpmemobj++/make_persistent_array.hpp>

using namespace std;
using namespace viper;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024 * 10;

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
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    std::string pool_file = "/mnt/nvrams1/viper.file";

    if (std::filesystem::exists(pool_file)) {
        std::cout << "Deleting pool file " << pool_file << std::endl;
        pmempool_rm(pool_file.c_str(), PMEMPOOL_RM_POOLSET_LOCAL | PMEMPOOL_RM_POOLSET_REMOTE);
    }

    ViperT viper{pool_file, POOL_SIZE};
//    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    auto v_client = viper.get_client();
    const int num_values = 10;
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
