#include "src/viper.hpp"

using namespace std;
const uint64_t POOL_SIZE = 1024l * 1024 * 1024;

int main() {

    viper::Viper<uint64_t, uint64_t> viper{"/mnt/nvrams1/viper.file", POOL_SIZE};
    cout << "viper size: " << sizeof(viper) << std::endl;
//    cout << "viper map size: " << sizeof(viper.map_) << std::endl;
//    cout << "viper pool size: " << sizeof(viper.v_pool_) << std::endl;

    viper.put(0, 0);
    viper.put(1, 1);
    viper.put(2, 2);
}