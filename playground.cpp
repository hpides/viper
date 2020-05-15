#include <libpmempool.h>
#include "src/viper.hpp"
#include <iostream>
#include <libpmemobj++/make_persistent_array.hpp>

using namespace std;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024;

using ViperT = viper::Viper<uint64_t, uint64_t>;

struct A {
    std::array<uint8_t, 24576> a;
} __attribute__((aligned(4096)));

struct Root {
    pmem::obj::vector<pmem::obj::persistent_ptr<A>> data;
    pmem::obj::p<int> x;
};

int main() {
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    std::cout << "X size: " << sizeof(A) << std::endl;

    std::string pool_file = "/mnt/nvrams1/alignment-test.file";
    std::cout << "Creating pool file " << pool_file << std::endl;
    pmem::obj::pool<Root> v_pool = pmem::obj::pool<Root>::create(pool_file, "", POOL_SIZE, S_IRWXU);

    pobj_alloc_class_desc alloc_description{ .unit_size = 24576, .alignment = 24576, .units_per_block = 1,
                                             .header_type = pobj_header_type::POBJ_HEADER_NONE, .class_id = 200 };
    pmemobj_ctl_set(v_pool.handle(), "heap.alloc_class.200.desc", &alloc_description);

    pmem::obj::transaction::run(v_pool, [&] {
        const pmem::obj::allocation_flag& alloc_flag = pmem::obj::allocation_flag::class_id(200);
        pmem::obj::persistent_ptr<A> new_block1 = pmem::obj::make_persistent<A>(alloc_flag);
        pmem::obj::persistent_ptr<A> new_block2 = pmem::obj::make_persistent<A>(alloc_flag);
        pmem::obj::persistent_ptr<A> new_block3 = pmem::obj::make_persistent<A>(alloc_flag);
        v_pool.root()->data.push_back(new_block1);
        v_pool.root()->data.push_back(new_block2);
        v_pool.root()->data.push_back(new_block3);

//        auto x = pmem::obj::make_persistent<A>();
//        v_pool.root()->data.push_back(x);

    });

    std::cout << "root addr: " << v_pool.root() << std::endl;
    std::cout << "root addr virt: " << v_pool.root().get() << std::endl;
    std::cout << "first A addr: " << v_pool.root()->data.data() << std::endl;

    return 0;


    ViperT viper{"/mnt/nvrams1/viper.file", POOL_SIZE};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    const int num_values = 10000000;
    for (int i = 0; i < num_values; ++i) {
        viper.put(i, i);
    }

    cout << "Size: " << viper.count() << endl;

    for (int key = 0; key < num_values; ++key) {
        ViperT::ConstAccessor result;
        const bool found = viper.get(key, result);
//        cout << "FOUND FOR KEY: " << key << " " << found << (found ? std::to_string(*result) : "") << endl;
    }
}
