#!/usr/bin/env bash
rm /mnt/pmem1/viper/*;
cd /lab505/gjk/viper/cmake-build/benchmark/;
make;
#200m
mv /mnt/tmpssd/aikv/ycsb-data-read200m/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb-data-read200m/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat
./ycsb;
mv /mnt/tmpssd/aikv/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb-data-read200m/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb-data-read200m/ycsb_wl_5050_uniform.dat
#400m
rm /mnt/pmem1/viper/*;
mv /mnt/tmpssd/aikv/ycsb-data-read400m/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb-data-read400m/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat
./ycsb;
mv /mnt/tmpssd/aikv/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb-data-read400m/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb-data-read400m/ycsb_wl_5050_uniform.dat
#600m
rm /mnt/pmem1/viper/*;
mv /mnt/tmpssd/aikv/ycsb-data-read600m/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb-data-read600m/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat
./ycsb;
mv /mnt/tmpssd/aikv/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb-data-read600m/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb-data-read600m/ycsb_wl_5050_uniform.dat
#800m
rm /mnt/pmem1/viper/*;
mv /mnt/tmpssd/aikv/ycsb-data-read800m/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb-data-read800m/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat
./ycsb;
mv /mnt/tmpssd/aikv/ycsb_prefill.dat /mnt/tmpssd/aikv/ycsb-data-read800m/ycsb_prefill.dat
mv /mnt/tmpssd/aikv/ycsb_wl_5050_uniform.dat /mnt/tmpssd/aikv/ycsb-data-read800m/ycsb_wl_5050_uniform.dat

