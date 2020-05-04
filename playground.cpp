#include <libpmempool.h>
#include "src/viper.hpp"

using namespace std;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024;

using ViperT = viper::Viper<uint64_t, uint64_t>;

int main() {
    ViperT viper{"/mnt/nvrams1/viper.file", POOL_SIZE};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    const int num_values = 100000;
    for (int i = 0; i < num_values; ++i) {
        viper.put(i, i);
    }

    cout << "Size: " << viper.count() << endl;

    for (int key = 0; key < num_values; ++key) {
        ViperT::ConstAccessor result;
        const bool found = viper.get(key, result);
        cout << "FOUND FOR KEY: " << key << " " << found << (found ? std::to_string(*result) : "") << endl;
    }
}
