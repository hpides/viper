#!/usr/bin/env bash
rm /mnt/pmem1/viper/*;
cd /lab505/gjk/viper/cmake-build/benchmark/;
make;
./ycsb;