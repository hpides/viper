#include <iostream>

#include "viper/viper.hpp"

int main(int argc, char** argv) {
    const size_t inital_size = 1073741824;  // 1 GiB
    auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem2/viper", inital_size);

    // To modify records in Viper, you need to use a Viper Client.
    auto v_client = viper_db->get_client();

    for (uint64_t key = 0; key < 10; ++key) {
        const uint64_t value = key + 10;
        v_client.put(key, value);
    }

    for (uint64_t key = 0; key < 10; ++key) {
        uint64_t value;
        v_client.get(key, &value);
        std::cout << "Record: " << key << " --> " << value << std::endl;
    }
}
