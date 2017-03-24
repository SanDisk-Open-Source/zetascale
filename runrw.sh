#!/bin/bash 
#==================================================
#
#  runrw.sh
#
#  This script configures ssdtest to measure
#  peak read IOPs for random read/write block accesses
#  with a fixed block size.  
#  
#  'AIO_TEST_NTHREADS' threads, in parallel, randomly select a block
#  between 0 and 'AIO_TEST_NBLOCKS'-1 and read it
#  from a randomly selected file.
#  Setting 'AIO_TEST_NBLOCKS' to zero uses the entire file.
#  This is repeated until a total (across all threads)
#  of 'AIO_TEST_ITERATIONS' accesses are done.
#  'AIO_TEST_ITERATIONS' should typically be set to 
#  3 times the number of blocks in all files.
#  If 'AIO_TEST_ITERATIONS' is set to zero, ssdtest
#  will be automatically set to 3*AIO_TEST_NBLOCKS*AIO_N_SSDS.
#
#  'AIO_TEST_WRITE_PERCENT' specifies the fraction of
#  accesses that are writes.
#
#  'AIO_DATA_FILE' specifies the location of the
#  'AIO_N_SSDS' data file(s).
#
#  For more detailed documentation on all parameters,
#  see the README.
#
#==================================================

AIO_CPU_MHZ=2800
AIO_SEED=1
AIO_O_DIRECT=1
AIO_USE_P_CALLS=1
AIO_ALIGN_FLAG=1
AIO_N_SSDS=1
AIO_DATA_FILE=/mnt/ssd/schooner
AIO_RAND_DATA=1
AIO_DATA_BLOCKS=1048576
AIO_TEST_NTHREADS=64
AIO_TEST_BLOCKSIZE=16384
AIO_TEST_NBLOCKS=0
AIO_TEST_WRITE_PERCENT=50
AIO_TEST_SHUFFLEFLAG=0
AIO_TEST_RANDOMFLAG=1
#AIO_TEST_ITERATIONS=0
AIO_TEST_ITERATIONS=10000000

export AIO_CPU_MHZ
export AIO_SEED
export AIO_O_DIRECT
export AIO_USE_P_CALLS
export AIO_ALIGN_FLAG
export AIO_N_SSDS
export AIO_DATA_FILE
export AIO_RAND_DATA
export AIO_DATA_BLOCKS
export AIO_TEST_NTHREADS
export AIO_TEST_BLOCKSIZE
export AIO_TEST_NBLOCKS
export AIO_TEST_WRITE_PERCENT
export AIO_TEST_SHUFFLEFLAG
export AIO_TEST_RANDOMFLAG
export AIO_TEST_ITERATIONS

/opt/schooner/bin/iotest
