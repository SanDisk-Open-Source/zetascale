#!/bin/bash

#./memcached-fthread-sdf_old -p11211 -L4 -N4 -T4 -u root  -Y -R --log sdf/client=trace --plat/shmem/prefault --plat/shmem/debug/replace_malloc --plat/shmem/file /hugepages/perf:8g  --property_file=/opt/schooner/config/sdf.cache.test.prop --no_sync
./memcached-fthread-sdf -p11211 -L4 -N4 -T4 -u root  -Y -R --log sdf/client=trace  --plat/shmem/debug/local_alloc --property_file=/opt/schooner/config/sdf.cache.test.prop --no_sync --no_aio_check --reformat --hot_key_stats=10000
