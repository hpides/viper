//
// Created by bysoulwarden on 2021/10/25.
//
#include <iostream>
#include "viper/viper.hpp"

int main(int argc, char** argv) {
    const size_t initial_size = 1024l * 1024 * 1024;  // 1 GiB
    auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem1/viper", initial_size,2);

    // To modify records in Viper, you need to use a Viper Client.
    auto v_client = viper_db->get_client();

    for (uint64_t key = 0; key < 10; ++key) {
        const uint64_t value = key + 10;
        v_client.put(key, value);
    }

    for (uint64_t key = 0; key < 11; ++key) {
        uint64_t value;
        const bool found = v_client.get(key, &value);
        if (found) {
            std::cout << "Record: " << key << " --> " << value << std::endl;
        } else {
            std::cout << "No record found for key: " << key << std::endl;
        }
    }
}
