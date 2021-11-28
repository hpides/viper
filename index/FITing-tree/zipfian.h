//
// Created by tawnysky on 2021/1/3.
//

#pragma once

#include <map>
#include <cmath>
#include <random>
#include <sys/time.h>

#define SKEW 0.99

class Zipfian {

    std::map<double, size_t> map;
    std::uniform_real_distribution<double> distribution;
    std::mt19937 generator;

public:
    explicit Zipfian(size_t number)
        :   distribution(0, 1),
            generator(time(NULL)) {
        double total_p = 0;
        for(size_t i = 1; i <= number; i++) {
            total_p += 1.0 / pow(i, SKEW);
        }
        double c = 1.0 / total_p;
        total_p = 0;
        for(size_t i = 1; i <= number; i++) {
            total_p += c / pow(i, SKEW);
            map.insert(std::pair<double, size_t>(total_p, i - 1));
        }
    }

    ~Zipfian() = default;

    size_t next() {
        return map.lower_bound(distribution(generator))->second;
    }
};
