/**
 * This code was taken and modified from https://github.com/DICL/CCEH, the original authors of CCEH.
 *
 * Orignial License:
 * Copyright (c) 2018, Sungkyunkwan University. All rights reserved.
 * The license is a free non-exclusive, non-transferable license to reproduce,
 * use, modify and display the source code version of the Software, with or
 * without modifications solely for non-commercial research, educational or
 * evaluation purposes. The license does not entitle Licensee to technical
 * support, telephone assistance, enhancements or updates to the Software. All
 * rights, title to and ownership interest in the Software, including all
 * intellectual property rights therein shall remain in Sungkyunkwan University.
 */

#pragma once

#include <functional>
#include <stddef.h>

namespace viper::cceh {

inline size_t standard(const void* _ptr, size_t _len,
                       size_t _seed = static_cast<size_t>(0xc70f6907UL)) {
    return std::_Hash_bytes(_ptr, _len, _seed);
}

inline size_t murmur2(const void* key, size_t len, size_t seed = 0xc70f6907UL) {
    const unsigned int m = 0x5bd1e995;
    const int r = 24;
    unsigned int h = seed ^len;
    const unsigned char* data = (const unsigned char*) key;

    while (len >= 4) {
        unsigned int k = *(unsigned int*) data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }

    switch (len) {
        case 3: h ^= data[2] << 16;
        case 2: h ^= data[1] << 8;
        case 1: h ^= data[0];
            h *= m;
    };

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

static size_t
(* hash_funcs[2])(const void* key, size_t len, size_t seed) = {
    standard,
    murmur2
};

inline size_t h(const void* key, size_t len, size_t seed = 0xc70697UL) {
    return hash_funcs[0](key, len, seed);
}

}  // namespace viper::cceh
