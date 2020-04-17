#include <libpmemobj.h>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/mutex.hpp>
#include "src/viper.hpp"

using namespace std;


struct Root {
    pmem::obj::persistent_ptr<std::array<int, 100>> data;
};

struct Test {
    std::array<int, 10> a;
    int b;
    static constexpr int blub = 10;
};

int main() {
    const uint64_t pool_size = 1024l * 1024 * 1024;

    viper::Viper<uint64_t, uint64_t> viper{"/mnt/nvrams1/viper.file", pool_size};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;


    cout << "size: " << sizeof(Test) << std::endl;

    Test t{};
    cout << "addr: " << &t << std::endl;
    cout << "addr: " << &(t.a) << std::endl;
    cout << "addr: " << t.a.data() << std::endl;
    cout << "size str_view: " << sizeof(std::string_view) << std::endl;

//    int sds_write_value = 0;
//    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
//
//    string pool_file = "/mnt/nvrams1/test1.file";
//    auto pmem_pool = pmem::obj::pool<Root>::create(pool_file, "", PMEMOBJ_MIN_POOL, S_IRWXU);
//
//    pmem::obj::transaction::run(pmem_pool, [&] {
//        pmem_pool.root()->data = pmem::obj::make_persistent<std::array<int, 100>>();
//    });
//
//    auto data = pmem_pool.root()->data;
//    for (int key = 0; key < 100; ++key) {
//        (*data)[key] = key;
//        pmemobj_persist(pmem_pool.handle(), data->data() + key, sizeof(key));
//    }
//
//    cout << "Inserted all keys" << endl;
//
//    pmem_pool.close();
//
//    pmem_pool = pmem::obj::pool<Root>::open(pool_file, "");
//    data = pmem_pool.root()->data;
//    for (int key = 0; key < 100; ++key) {
//        cout << "key: " << key << " - value: " << (*data)[key] << endl;
//    }
//
//    pmem_pool.close();
}