#!/bin/bash -x
export LD_LIBRARY_PATH=./output/lib
export ZS_PROPERTY_FILE=./briano/testz.prop
#export ZS_LICENSE_PATH=xxxzzz
echo $LD_LIBRARY_PATH
echo $ZS_PROPERTY_FILE
echo $ZS_LICENSE_PATH

#gcc -rdynamic -std=c99 -Iapi -I. -o stats2.o stats2.c -L output/lib/ -ldl -lpthread -lsnappy -laio -levent -lrt 
#gcc -rdynamic -std=c99 -Iapi -I. -o zstestz zstestz.c stats2.o ./output/lib/libzsdll.a -L output/lib/ -ldl -lpthread -lsnappy -laio -levent -lrt 

#gcc -rdynamic -Iapi -I. -o zstestz zstestz.c stats2.o ./output/lib/libzsdll.a -L output/lib/ -ldl -lm -lpthread -lsnappy -laio -levent -lrt 

#gcc -c -g  -o stats2.o stats2.c 
#gcc -std=c99 -Iapi -I. -o zstestz zstestz.c stats2.o -L output/lib/ -lzs -lpthread -lsnappy -laio -levent -lrt -lm

gcc -g -c -Iapi -I. -o stats2.o stats2.c -L output/lib/ -lpthread -lsnappy -laio -levent -lrt 
#gcc -g -Iapi -I. -o zstestz zstestz.c stats2.o -L output/lib/ -lzs -lpthread -lsnappy -laio -levent -lrt -lm
gcc -g -Iapi -I. -o zstestz zstestz.c stats2.o output/lib/libzsdll.a -L output/lib/ -lpthread -ldl -lsnappy -laio -levent -lrt -lm

#==================================================
#
#  This script configures zstest to measure
#  peak read IOPs for random read/write block accesses
#  with a fixed block size.  
#  
#  'ZS_TEST_NTHREADS' threads, in parallel, randomly select a block
#  between 0 and 'ZS_TEST_NBLOCKS'-1 and read it
#  from a randomly selected file.
#  Setting 'ZS_TEST_NBLOCKS' to zero uses the entire file.
#  This is repeated until a total (across all threads)
#  of 'ZS_TEST_ITERATIONS' accesses are done.
#  'ZS_TEST_ITERATIONS' should typically be set to 
#  3 times the number of blocks in all files.
#  If 'ZS_TEST_ITERATIONS' is set to zero, ssdtest
#  will be automatically set to 3*ZS_TEST_NBLOCKS*ZS_N_SSDS.
#
#  'ZS_TEST_WRITE_PERCENT' specifies the fraction of
#  accesses that are writes.
#
#  'ZS_DATA_FILE' specifies the location of the
#  'ZS_N_SSDS' data file(s).
#
#  For more detailed documentation on all parameters,
#  see the README.
#
#==================================================

ZS_CPU_MHZ=2800
ZS_SEED=1
ZS_RAND_DATA=0
#ZS_DATA_BLOCKS=1048576
#ZS_RAND_DATA_FILE=/mnt/ssd/schooner
ZS_TEST_NTHREADS=1
ZS_TEST_OBJSIZE=100
ZS_TEST_NOBJS=1000000
ZS_TEST_WRITE_PERCENT=50
ZS_TEST_ITERATIONS=0
#ZS_TEST_ITERATIONS=10000000

export ZS_CPU_MHZ
export ZS_SEED
export ZS_RAND_DATA
export ZS_DATA_BLOCKS
export ZS_RAND_DATA_FILE
export ZS_TEST_NTHREADS
export ZS_TEST_OBJSIZE
export ZS_TEST_NOBJS
export ZS_TEST_WRITE_PERCENT
export ZS_TEST_ITERATIONS

gdb --args zstestz -v
