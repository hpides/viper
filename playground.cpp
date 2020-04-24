#include <libpmempool.h>
#include "src/viper.hpp"

using namespace std;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024;

int main() {
    viper::Viper<uint64_t, uint64_t> viper{"/mnt/nvrams1/viper.file", POOL_SIZE};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    const int num_values = 100000;
    for (int i = 0; i < num_values; ++i) {
        viper.put(i, i);
    }

    cout << "Size: " << viper.count() << endl;

    for (int j = 0; j < num_values; ++j) {
        cout << viper.get(j) << endl;
    }
}
