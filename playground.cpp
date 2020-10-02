#include <iostream>
#include <memory>
#include <iostream>
#include "cceh/CCEH.h"

#include "viper.hpp"

using namespace viper;

int main() {
    size_t capacity = 10;
    auto cceh = std::make_unique<CCEH>(capacity);

    for (size_t i = 0; i < capacity + 10; ++i) {
        cceh->Insert(i, i);
    }

    for (size_t i = 0; i < capacity + 10; ++i) {
        size_t value = cceh->Get(i);
        std::cout << value << std::endl;
    }
}
