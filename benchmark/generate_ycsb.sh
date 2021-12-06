#!/usr/bin/env bash

set -e

#BASE_DIR="/hpi/fs00/home/lawrence.benson/clion/viper1/benchmark"
BASE_DIR="/lab505/gjk/viper/benchmark"
PREFILL_CONF="${BASE_DIR}/config/ycsb_prefill.conf"
DATA_DIR="/mnt/tmpssd/aikv/ycsb-data"

#CONFIGS=( "5050_uniform" "5050_zipf" "1090_uniform" "1090_zipf" )
CONFIGS=( "5050_uniform")
#cd "/hpi/fs00/home/lawrence.benson/ycsb"
cd "/lab505/gjk/ycsb-0.17.0"

echo "GENERATING PREFILL DATA"
./bin/ycsb load basic -P ${PREFILL_CONF} -s > "${DATA_DIR}/raw_prefill.dat"

echo "GENERATING YCSB DATA"
for config in "${CONFIGS[@]}"
do
  echo "GENERATING ${config}..."
  ./bin/ycsb run basic -P ${PREFILL_CONF} \
          -P "${BASE_DIR}/config/ycsb_${config}.conf" \
          -s > "${DATA_DIR}/raw_ycsb_wl_${config}.dat"
done


cd "${BASE_DIR}"
echo "CONVERTING DATA TO BINARY FORMAT"

python3 convert_ycsb.py "${DATA_DIR}/raw_prefill.dat" "${DATA_DIR}/ycsb_prefill.dat"

for config in "${CONFIGS[@]}"
do
  echo "CONVERTING: ${config}..."
  python3 convert_ycsb.py "${DATA_DIR}/raw_ycsb_wl_${config}.dat" "${DATA_DIR}/ycsb_wl_${config}.dat"
  rm "${DATA_DIR}/raw_ycsb_wl_${config}.dat"
done
