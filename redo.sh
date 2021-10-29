#!/usr/bin/env bash
rm /mnt/pmem1/viper/*;
rm /lab505/gjk/viper/cmake-build-debug/index-test/test;
cd /lab505/gjk/viper/cmake-build-debug/index-test/;
make;
./test;