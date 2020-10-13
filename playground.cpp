#include <iostream>
#include <memory>
#include "cceh.hpp"

#include "viper.hpp"

using namespace viper;

int main() {
    size_t capacity = 1000;
    auto cceh = std::make_unique<cceh::CCEH<size_t>>(capacity);

    for (size_t i = 0; i < 10; ++i) {
        IndexV old_offset = cceh->Insert(i, IndexV{i, 0, 0});
        std::cout << "OLD: " << old_offset.block_number << std::endl;
    }

    for (size_t i = 0; i < 10; ++i) {
        cceh::CcehAccessor value;
        cceh->Get(i, value);
        std::cout << value->block_number << std::endl;
    }

    IndexV old_offset = cceh->Insert(4, IndexV{44, 0, 0});
    std::cout << "OLD 4: " << old_offset.block_number << std::endl;
}
