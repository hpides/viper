//
// Created by tawnysky on 2021/1/3.
//
//
// Created by tawnysky on 2021/1/3.
//

#pragma once

#include <map>
#include <cmath>
#include <random>
#include <sys/time.h>

#define SKEW 0.99

class ZipfianRandom {

    std::map<double, size_t> map;
    std::uniform_real_distribution<double> distribution;
    std::mt19937 generator;

    std::map<size_t, size_t> map2;
    std::uniform_int_distribution<size_t> distribution2;
    std::mt19937 generator2;

public:
    explicit ZipfianRandom(size_t number)
            :   distribution(0, 1),
                generator(time(NULL)),
                distribution2(0, number - 1),
                generator2(time(NULL)) {
        double total_p = 0;
        for(size_t i = 1; i <= number; i++) {
            total_p += 1.0 / pow(i, SKEW);
        }
        double c = 1.0 / total_p;
        total_p = 0;
        for(size_t i = 1; i <= number; i++) {
            total_p += c / pow(i, SKEW);
            map.insert(std::pair<double, size_t>(total_p, i - 1));
            map2.insert(std::pair<size_t, size_t>(i, distribution2(generator2)));
        }
    }

    ~ZipfianRandom() = default;

    size_t next() {
        return map2.find(map.lower_bound(distribution(generator))->second)->second;
    }
};
