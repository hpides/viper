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
    std::cout << "Creating pool file " << pool_file << std::endl;
    pmem::obj::pool<ViperRoot<VKey, VValue>> v_pool = pmem::obj::pool<ViperRoot<VKey, VValue>>::create(pool_file, "", POOL_SIZE, S_IRWXU);

    pobj_alloc_class_desc alloc_description{ .unit_size = 24576, .alignment = 24576, .units_per_block = 1,
                                             .header_type = pobj_header_type::POBJ_HEADER_NONE, .class_id = 200 };
    pmemobj_ctl_set(v_pool.handle(), "heap.alloc_class.200.desc", &alloc_description);

    ViperT viper{std::move(v_pool)};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    const int num_values = 10000000;
    for (int i = 0; i < num_values; ++i) {
        viper.put(i, i);
    }

    cout << "Size: " << viper.count() << endl;

    uint64_t found_counter = 0;
    for (int key = 0; key < num_values; ++key) {
        ViperT::ConstAccessor result;
        const bool found = viper.get(key, result);
        found_counter += found;
    }
    cout << "FOUND IN TOTAL: " << found_counter << "/" << num_values << endl;

    pmempool_rm(pool_file, PMEMPOOL_RM_POOLSET_LOCAL | PMEMPOOL_RM_POOLSET_REMOTE);
}
