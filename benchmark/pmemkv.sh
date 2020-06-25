#!/usr/bin/env bash

export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

for nt in 1 2 4 8 16 24 32 36
do
  echo "RUNNING THREADS: ${nt}"

	PMEMOBJ_CONF="sds.at_create=0" \
	  /home/lawrence.benson/pmemkv-tools/pmemkv_bench \
	    --engine=cmap \
	    --db=/mnt/nvram-viper/pmemkv-bench.file \
	    --db_size_in_gb=40 --value_size=200 --threads=${nt} \
	    --num=10000000 --reads=5000000  \
	    --benchmarks=fillseq,readrandom,overwrite,deleterandom

	rm /mnt/nvram-viper/pmemkv-bench.file
done