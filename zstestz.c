//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/******************************************************************
 *
 * File:   main.c for "zstest": a ZetaScale I/O performance test program
 * Author: Brian O'Krafka
 *
 * Created on February 3, 2010
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: main.c 802 2008-03-29 00:44:48Z briano $
 *
 ******************************************************************/

#define _MAIN_C
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <unistd.h>
#include <stdarg.h>
#include <assert.h>
#include <time.h>
#include <math.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>
#include <zs.h>
#include "stats2.h"

#define ERRBUF_LEN  1024

#define prtstuff(...) \
{\
    do_prtstuff(1, stderr, __VA_ARGS__);\
    if (f_stats_file) { \
        do_prtstuff(1, f_stats_file, __VA_ARGS__);\
    }\
}

#define prtstuff2(...) \
{\
    do_prtstuff(0, stderr, __VA_ARGS__);\
    if (f_stats_file) { \
        do_prtstuff(0, f_stats_file, __VA_ARGS__);\
    }\
}

// Add a common routine to get the hardware time-stamp counter
static __inline__ uint64_t rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u)); 
    return (((uint64_t) u << 32) | l); 
}

static char *Program = NULL;

static char FilePrefix[1024];

/* independent parameters */
static int           Verbose = 0;
static int           Serialize=0;
static uint64_t      dump_thresh=0;
static unsigned int  seed;
static double        cpu_mhz=3000;
static int           nthreads=1;
static int           objsize=4*1024;
static int           nobjs=100000; // default
static int           write_percent=50;
static int           random_data=1;
static uint32_t      n512_data_blocks=1024*1024;
static int           niters=0;
static char         *stats_filename;

/* dependent variables */
static int           n512=0;
static char         *randdatafile=NULL;
static char         **bufs;
static char         *rdata;
static char         *zeroes;
static uint64_t      aio_read_errs=0;
static uint64_t      aio_read_iops=0;
static uint64_t      aio_write_errs=0;
static uint64_t      aio_write_iops=0;
static uint64_t      last_read_iops=0;
static uint64_t      last_write_iops=0;
struct timeval       t;
struct timeval       tlast;
static int           fd[8];
static uint64_t      thread_count = 0;
static FILE         *f_stats_file = NULL;

#define MAX_THREADS   1024
static rt_statistics_t        rtstats_all_read;
static rt_statistics_t        rtstats_all_write;
static rt_statistics_t        rtstats_read[MAX_THREADS];
static rt_statistics_t        rtstats_write[MAX_THREADS];

static void usage()
{
    fprintf(stderr, "=============================================================================\n\n");
    fprintf(stderr, "usage: %s [-v]\n\n", Program);
    fprintf(stderr, "       -v: dump verbose output\n\n");
    fprintf(stderr, "       Set test parameters using these environment variables:\n\n");
    fprintf(stderr, "          ZS_SERIALIZE: if non-zero, do write serialization (default is 0)\n");
    fprintf(stderr, "         ZS_DUMP_ITERS: iterations between transient performance dump\n");
    fprintf(stderr, "            ZS_CPU_MHZ: used for response time\n");
    fprintf(stderr, "                         normalization\n");
    fprintf(stderr, "               ZS_SEED: seed for random number generator (default is 1)\n");
    fprintf(stderr, "          ZS_RAND_DATA: if non-zero, write random data,\n");
    fprintf(stderr, "                         otherwise use zeroes\n");
    fprintf(stderr, "     ZS_RAND_DATA_FILE: file from which to preload random data;\n");
    fprintf(stderr, "                         if unspecified, data is generated randomly\n");
    fprintf(stderr, "        ZS_DATA_BLOCKS: number of 512B blocks to hold random write data\n");
    fprintf(stderr, "      ZS_TEST_NTHREADS: number of threads\n");
    fprintf(stderr, "       ZS_TEST_OBJSIZE: object size in bytes\n");
    fprintf(stderr, "         ZS_TEST_NOBJS: number of objects to access\n");
    fprintf(stderr, " ZS_TEST_WRITE_PERCENT: percentage of accesses to be writes\n");
    fprintf(stderr, "    ZS_TEST_ITERATIONS: total number of block accesses in test;\n");
    fprintf(stderr, "                         if 0, use 3*ZS_TEST_NOBJS\n");
    fprintf(stderr, "\n=============================================================================\n");
    exit(1);
}

    /* predeclarations */
static void do_ssd_perftest();
static void *test_routine(void *arg);
static void do_prtstuff(int do_prefix, FILE *f, char *fmt, ...);
static struct ZS_state *init_zs();
static void process_stats(rt_statistics_t *rtstats);
static void dump_progress(uint64_t i, uint64_t iters_per_thread, uint32_t n_keys_written, uint64_t objs_per_thread);

int main(int argc, char **argv)
{
    int                i;

    /* initialization */

    Program = argv[0];
    Verbose = 0;

    for (i=1; i<argc; i++) {
        if (argv[i][0] == '-') {

	    /* switches without second arguments */

	    switch (argv[i][1]) {
	        case 'h': // help
		    usage();
		    break;
	        case 'v': // verbose output
		    Verbose = 1;
		    continue;
		    break;
	        default:
		    /* purposefully empty */
		    break;
	    }

	    /* switches with second arguments */

	    if (i == (argc - 1)) {
	        // second argument is missing!
	        usage();
	    }

	    switch (argv[i][1]) {
	        #ifdef notdef
	        case 'f': // fail threshold
		    pasd->fail_threshold = atoi(argv[i+1]);
		    if (pasd->fail_threshold <= 0) {
		        Error("fail threshold must be positive");
		    }
		    break;
		#endif
		default:
		    usage();
		    break;
	    }
	    i++; // skip the second argument
	} else {
	    usage();
	}
    }

    do_ssd_perftest();

    if (Verbose) {
	prtstuff("===============================\n");
	prtstuff("Read Response Time Statistics:\n");
	prtstuff("===============================\n");
	process_stats(&rtstats_all_read);
	prtstuff("===============================\n");
	prtstuff("Write Response Time Statistics:\n");
	prtstuff("===============================\n");
	process_stats(&rtstats_all_write);
	prtstuff("...done!\n");
    }

    return(0);
}

static struct ZS_state *init_zs()
{
    struct ZS_state		*zs_state;
    struct ZS_thread_state	*thd_state;
    ZS_status_t			 status;
    char			*version;
    const char		        *path;

    //Get the version ZS the program running with.
    if (ZSGetVersion(&version) == ZS_SUCCESS) {
	prtstuff("This is a sample program using ZS %s\n", version);
	ZSFreeBuffer(version);
    }

    path = ZSGetProperty("ZS_LICENSE_PATH", "Default path");
    if (path && (strcmp(path, "Default path") != 0)) {
	prtstuff("License will be searched at: %s\n", path);
	ZSFreeBuffer((char *)path);
    }

    //Initialize ZS state.
    if ((status = ZSInit(&zs_state)) != ZS_SUCCESS) {
	prtstuff("ZSInit failed with error %s\n", ZSStrError(status));
	exit(1);
    }

    //Initialize per-thread ZS state for main thread.
    if ((status = ZSInitPerThreadState(zs_state, &thd_state)) != ZS_SUCCESS) {
	prtstuff("ZSInitPerThreadState failed with error %s\n", ZSStrError(status));
	exit(1);
    }

    prtstuff("Initialized ZetaScale.\n");
    return(zs_state);
}

static void do_prtstuff(int do_prefix, FILE *f, char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);
   if (do_prefix) {
       (void) fprintf(f, "ZS_PERF> ");
   }
   vfprintf(f, fmt, args);
   va_end(args);
}

/**********************************************************************
 *
 *  simple bit vector data structure
 *
 **********************************************************************/

typedef struct bv {
    uint32_t    n_ints;
    uint32_t   *ints;
} bv_t;

struct bv *bv_init(uint32_t n)
{
    int     i;
    bv_t   *bv;

    bv = (bv_t *) malloc(sizeof(bv_t));
    assert(bv);
    bv->n_ints = (n+8*sizeof(uint32_t)-1)/(8*sizeof(uint32_t));
    bv->ints = (uint32_t *) malloc(bv->n_ints*sizeof(uint32_t));
    assert(bv->ints);
    for (i=0; i<bv->n_ints; i++) {
        bv->ints[i] = 0;
    }
    return(bv);
}

void bv_set(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    bv->ints[ni] |= (1<<no);
}

void bv_unset(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    bv->ints[ni] &= (~(1<<no));
}

int bv_test(struct bv *bv, uint32_t n)
{
    uint32_t  ni, no;
    
    ni = n/(8*sizeof(uint32_t));
    no = n % (8*sizeof(uint32_t));

    if (bv->ints[ni] & (1<<no)) {
        return(1);
    } else {
        return(0);
    }
}

/**********************************************************************
 *
 *  end of code for simple bit vector data structure
 *
 **********************************************************************/

static void do_ssd_perftest()
{
    int               d;
    uint64_t          i;
    char             *s;
    uint64_t          datasize;
    double            bw;
    double            bytes;
    double            iops, iops_read, iops_write;
    double            ios, ios_read, ios_write;
    time_t            t_start;
    time_t            t_end;
    void             *value_ptr;
    pthread_t        *pthreads;
    uint64_t          x;
    struct ZS_state  *zs_state;

    s = getenv("ZS_CPU_MHZ");
    if (s != NULL) {
	cpu_mhz = atof(s);
	if ((cpu_mhz <= 0) || (cpu_mhz > 10000)) {
	    prtstuff("cpu_mhz=%g is out of range\n", cpu_mhz);
	    exit(1);
	}
    }

    s = getenv("ZS_SEED");
    if (s == NULL) {
        seed = 1;
    } else {
	seed = atoi(s);
    }

    s = getenv("ZS_SERIALIZE");
    if (s == NULL) {
        Serialize = 0;
    } else {
	Serialize = atoi(s);
    }

    s = getenv("ZS_STATS_FILE");
    if (s == NULL) {
        stats_filename = "zs_stats.out";
    } else {
	stats_filename = s;
    }
    f_stats_file = fopen(stats_filename, "w");
    if (f_stats_file == NULL) {
        prtstuff("Could not open stats file '%s'\n", stats_filename);
	exit(1);
    }

    s = getenv("ZS_RAND_DATA");
    if (s != NULL) {
	random_data = atoi(s);
    }

    s = getenv("ZS_RAND_DATA_FILE");
    if (s == NULL) {
        randdatafile = NULL;
    } else {
	randdatafile = s;
    }

    s = getenv("ZS_DATA_BLOCKS");
    if (s != NULL) {
	n512_data_blocks = atoi(s);
	if ((n512_data_blocks <= 0) || (n512_data_blocks > 1024*1024)) {
	    prtstuff("n512_data_blocks=%d is out of range (%d max)\n", n512_data_blocks, 1024*1024);
	    exit(1);
	}
    }

    s = getenv("ZS_TEST_NTHREADS");
    if (s != NULL) {
	nthreads = atoi(s);
	if ((nthreads <= 0) || (nthreads > MAX_THREADS)) {
	    prtstuff("nthreads=%d is out of range (%d max)\n", nthreads, MAX_THREADS);
	    exit(1);
	}
    }

    s = getenv("ZS_TEST_OBJSIZE");
    if (s != NULL) {
	objsize = atoi(s);
	if ((objsize <= 0) || (objsize > 10*1024*1024)) {
	    prtstuff("objsize=%d is out of range\n", objsize);
	    exit(1);
	}
    }
    n512 = objsize/512;
    if (objsize % 512) {
        n512++;
    }

    s = getenv("ZS_TEST_NOBJS");
    if (s != NULL) {
	nobjs = atoi(s);
    }

    s = getenv("ZS_TEST_WRITE_PERCENT");
    if (s != NULL) {
	write_percent = atoi(s);
	if ((write_percent < 0) || (write_percent > 100)) {
	    prtstuff("writeflag=%d must between 0 and 100, inclusive\n", write_percent);
	    exit(1);
	}
    }

    s = getenv("ZS_TEST_ITERATIONS");
    if (s != NULL) {
	niters= atoi(s);
	if (niters < 0) {
	    prtstuff("niters=%d is out of range\n", niters);
	    exit(1);
	}
    }

    s = getenv("ZS_DUMP_ITERS");
    if (s == NULL) {
	dump_thresh  = niters/nthreads/100;
	if (dump_thresh == 0) {
	    dump_thresh = 1;
	}
    } else {
	dump_thresh = atoi(s);
	if (dump_thresh <= 0) {
	    dump_thresh  = niters/nthreads/100;
	    if (dump_thresh == 0) {
		dump_thresh = 1;
	    }
	}
    }

    if (Verbose) {
	prtstuff("===================================================================\n\n");
	prtstuff("aio test settings (set by environment variables named below):\n");
	prtstuff("      ZS_SERIALIZATION = %8d : in non-zero, do write serialization\n", Serialize);
	prtstuff("            ZS_CPU_MHZ = %8g : used for response time\n", cpu_mhz);
	prtstuff("                                    normalization\n");
	prtstuff("               ZS_SEED = %8d : seed for random number generator\n", seed);
	prtstuff("          ZS_RAND_DATA = %8d : if non-zero, write random data,\n", random_data);
	prtstuff("                                    otherwise use zeroes\n");
	prtstuff("     ZS_RAND_DATA_FILE = %8s : file for preloading random data;\n", ((randdatafile == NULL) ? "NULL": randdatafile));
	prtstuff("                                    if unspecified, data is generated\n");
	prtstuff("        ZS_DATA_BLOCKS = %8d : number of 512B blocks to\n", n512_data_blocks);
	prtstuff("                                    hold random write data\n");
	prtstuff("      ZS_TEST_NTHREADS = %8d : number of threads\n", nthreads);
	prtstuff("       ZS_TEST_OBJSIZE = %8d : object size in bytes\n", objsize);
	prtstuff("                                    (multiple of 512)\n");
	prtstuff("         ZS_TEST_NOBJS = %8d : number of objects to access;\n", nobjs);
	prtstuff("                                    if 0, use entire file\n");
	prtstuff(" ZS_TEST_WRITE_PERCENT = %8d : %% of accesses to be writes\n", write_percent);
	prtstuff("    ZS_TEST_ITERATIONS = %8d : total number of object accesses\n", niters);
	prtstuff("                                    in test; if 0, use\n");
	prtstuff("                                    3*ZS_TEST_NOBJS\n");
	prtstuff("         ZS_DUMP_ITERS = %8d : iterations between performance updates\n", dump_thresh);
	prtstuff("===================================================================\n\n");
    }

    // Set the seed for the random number generator
    srandom(seed);

    if (nobjs == 0) {
	prtstuff("nobjs must be non-zero!\n");
	exit(1);
    }

    if (niters == 0) {
        // set to 3*NOBJECTS
	prtstuff("ZS_TEST_ITERATIONS defaulting to 3*ZS_TEST_NOBJS\n");
	niters = 3*nobjs;
    }

    zs_state = init_zs();

    bufs = (char **) malloc(nthreads*sizeof(char *));
    assert(bufs);
    s = (char *) malloc(nthreads*objsize + 512);
    assert(s);
    bufs[0] = s;
    d = (((uint64_t) s) % 512);
    bufs[0] += (512 - d);
    for (i = 1; i < nthreads; i++) {
	bufs[i] = bufs[0] + i*objsize;
    }

    zeroes = (char *) malloc(n512*512 + 512);
    assert(zeroes);
    memset(zeroes, 0, n512*512 + 512);
    x = (uint64_t) zeroes;
    x += (512 - (x % 512));
    if (x % 512) {
         prtstuff("Could not align zeroes buffer\n");
         exit(1);
    }
    zeroes = (char *) x;

    rdata = (char *) malloc(n512_data_blocks*512 + 512);
    assert(rdata);
    if (random_data) {
	if (randdatafile) {
	    FILE          *f;
	    int            ic;

	    if (Verbose) {
		prtstuff("Preloading random data buffer from file '%s'...\n", randdatafile);
	    }
	    f = fopen(randdatafile, "r");
	    if (f == NULL) {
		prtstuff("Could not open data preload file '%s'\n", randdatafile);
		exit(1);
	    }
	    for (i=0; i<(n512_data_blocks*512 + 512); i++) {
		ic = fgetc(f);
		if (ic == EOF) {
		    prtstuff("Data preload file '%s' is shorter than random data buffer: wrapping around and reusing the file\n", randdatafile);
		    rewind(f);
		    ic = fgetc(f);
		    if (ic == EOF) {
			prtstuff("Could not rewind data preload file '%s'\n", randdatafile);
			exit(1);
		    }
		}
		rdata[i] = (unsigned char) ic;
	    }

	    if (fclose(f) != 0) {
		prtstuff("Could not close data preload file '%s'\n", randdatafile);
		exit(1);
	    }
	} else {
	    if (Verbose) {
		prtstuff("Preloading random data buffer with random data...\n");
	    }
	    for (i=0; i<(n512_data_blocks*512 + 512); i++) {
		rdata[i] = (random() % 256);
	    }
	}
	if (Verbose) {
	    prtstuff("... completed preloading random data buffer.\n");
	}
    }

    x = (uint64_t) rdata;
    x += (512 - (x % 512));
    if (x % 512) {
         prtstuff("Could not align random data buffer\n");
         exit(1);
    }
    rdata = (char *) x;

    time(&t_start);
    prtstuff("Starting time: %s", ctime(&t_start));

    pthreads = (pthread_t *) malloc(nthreads*sizeof(pthread_t));
    assert(pthreads);

    for (i = 0; i < nthreads; i++) {
        if (pthread_create(&(pthreads[i]), 
	    NULL, test_routine, (void *) zs_state) != 0) 
	{
	    prtstuff("Failed to create pthread %d\n", i);
	    exit(1);
	}
    }

    // Wait for all test threads to finish
    for (i=0; i<nthreads; i++) {
        if (pthread_join(pthreads[i], &value_ptr) != 0) {
	    prtstuff("pthread_join failed for thread %d\n", i);
	    // exit(1);
	}
    }
    // xxxzzz should I measure skew?

    // Coalesce all of the response time stats.
    rt_init_stats(&rtstats_all_read, "Read Response_Time");
    rt_init_stats(&rtstats_all_write, "Write Response_Time");
    for (i=0; i<nthreads; i++) {
        rt_sum_stats(&rtstats_all_read, &(rtstats_read[i]));
        rt_sum_stats(&rtstats_all_write, &(rtstats_write[i]));
    }

    ios_read  = rt_dump_stats(&rtstats_all_read, Verbose);
    ios_write = rt_dump_stats(&rtstats_all_write, Verbose);

    if (Verbose) {
	prtstuff("\n");
    }

    time(&t_end);
    prtstuff("  Ending time: %s\n", ctime(&t_end));

    bytes  = niters;
    bytes *= objsize;

    ios = ios_read + ios_write;
    if (t_end == t_start) {
	bw = 0;
	iops = 0;
	iops_read  = 0;
	iops_write = 0;
    } else {
	bw = bytes/(t_end - t_start)/(1024.*1024.*1024.);
	iops = ios/(t_end - t_start);
	iops_read  = ios_read/(t_end - t_start);
	iops_write = ios_write/(t_end - t_start);
    }
    prtstuff("===============================================================\n");
    prtstuff("Bandwidth: %g GB/sec (%g Gbytes in %lld sec)\n", bw, bytes/(1024.*1024.*1024.), t_end - t_start);
    prtstuff("===============================================================\n");
    prtstuff("Total IOPs: %8.4g kIO/sec (%10g IO's in %8lld sec)\n", iops/1000, ios, t_end - t_start);
    prtstuff("Read  IOPs: %8.4g kIO/sec (%10g IO's in %8lld sec)\n", iops_read/1000, ios_read, t_end - t_start);
    prtstuff("Write IOPs: %8.4g kIO/sec (%10g IO's in %8lld sec)\n", iops_write/1000, ios_write, t_end - t_start);
    prtstuff("===============================================================\n");
    if (aio_read_errs > 0) {
	prtstuff("%"PRIu64" read errors\n", aio_read_errs);
    }
    if (aio_write_errs > 0) {
	prtstuff("%"PRIu64" write errors\n", aio_write_errs);
    }
    // prtstuff("===================================================================\n");
}

static void *test_routine(void *arg)
{
    struct ZS_state	   *zs_state = (struct ZS_state *)arg;
    struct ZS_thread_state *thd_state;
    char		    cname[32] = {0};
    ZS_container_props_t    props;
    ZS_status_t		    status;
    ZS_cguid_t		    cguid;
    uint32_t		    myid;

    uint32_t		    keylen;
    char		    keybuf[1000];
    uint64_t		    datalen;

    uint64_t                i;
    uint64_t                iters_per_thread;
    uint64_t                objs_per_thread;
    uint64_t                tstart;
    int                     do_write;
    char                   *p;
    uint32_t                index;
    uint32_t                nobj;

    struct bv              *keys_bv;
    uint32_t               *keys_written;
    uint32_t                n_keys_written;

    /* structures used to keep track of objects available to read */
    objs_per_thread = nobjs/nthreads;
    keys_bv = bv_init(objs_per_thread);
    keys_written = (uint32_t *) malloc(objs_per_thread*sizeof(uint32_t));
    assert(keys_written);
    for (i=0; i<objs_per_thread; i++) {
        keys_written[i] = objs_per_thread;
    }
    n_keys_written = 0;

    myid = __sync_fetch_and_add(&thread_count, 1);

    prtstuff("%d starting...\n", myid);

    //Create the container name based on thread id.
    sprintf(cname, "%s%d", "container", myid);

    //Initialize per thread state of ZS for this thread.
    ZSInitPerThreadState(zs_state, &thd_state);

    //Create container in read/write mode with properties specified.
    status = ZSLoadCntrPropDefaults(&props);
    assert(status == ZS_SUCCESS);
    props.flash_only = 1;
    if (Serialize) {
	props.flags = ZS_SERIALIZED_CTNR|ZS_DATA_IN_LEAVES_CTNR;
    } else {
	props.flags = 0;
    }
    props.size_kb    = 0;
    status = ZSOpenContainer(thd_state, cname, &props, 
			      ZS_CTNR_RW_MODE|ZS_CTNR_CREATE, &cguid);

    if (status == ZS_SUCCESS) {
	//If created successfully, get the container properties. 
	ZSGetContainerProps(thd_state, cguid, &props);
	prtstuff("Container %s (cguid: %ld) created with size: %ldKB.\n", 
	         cname, cguid, props.size_kb);
    } else {
	prtstuff("ZSOpenContainer (of %s) failed with %s.\n", 
	         cname, ZSStrError(status));
	return(NULL);
    }

    rt_init_stats(&(rtstats_read[myid]), "Read Response_Time");
    rt_init_stats(&(rtstats_write[myid]), "Write Response_Time");

    iters_per_thread = niters/nthreads;
    gettimeofday(&tlast, NULL);
    for (i=0; i<iters_per_thread; i++) {

	tstart = rdtsc();

        if (i<100) {
	    // write a few keys so that reads can work
	    do_write = 1;
	} else {
	    if (write_percent == 0) {
		do_write = 0;
	    } else if (write_percent == 100) {
		do_write = 1;
	    } else {
		if ((random() % 100) < write_percent) {
		    do_write = 1;
		} else {
		    do_write = 0;
		}
	    }
	}

	if (do_write) {
	    if (random_data) {
		do {
		    index = (random() % n512_data_blocks);
		} while ((index + n512) >= n512_data_blocks);
		p = &rdata[index*512];
	    } else {
		p = zeroes;
	    }

	    nobj = random() % objs_per_thread;

	    sprintf(keybuf, "key%d_%d", myid, nobj);
	    keylen = strlen(keybuf);
	    status = ZSWriteObject(thd_state, cguid, keybuf, keylen, p, objsize, 0);
	    if (status != ZS_SUCCESS) {
		prtstuff("ZSWriteObject failed, status=%d (%s), key=%s\n", status, ZSStrError(status), keybuf);
		(void) __sync_fetch_and_add(&aio_write_errs, 1);
		break;
	    } else {
		rt_record_op(&tstart, &(rtstats_write[myid]));
		(void) __sync_fetch_and_add(&aio_write_iops, 1);
		if (!bv_test(keys_bv, nobj)) {
		    /* first time this object is written */
		    bv_set(keys_bv, nobj);
		    keys_written[n_keys_written] = nobj;
		    n_keys_written++;
		}
	    }
	} else {

	    /*   This ensures that we try to read something that was
	     *   already written.
	     */

	    nobj = keys_written[random() % n_keys_written];

	    sprintf(keybuf, "key%d_%d", myid, nobj);
	    keylen = strlen(keybuf);
	    status = ZSReadObject(thd_state, cguid, keybuf, keylen, &p, &datalen);

	    if (status != ZS_SUCCESS) {
		prtstuff("ZSReadObject failed, status=%d (%s), key=%s\n", status, ZSStrError(status), keybuf);
		(void) __sync_fetch_and_add(&aio_read_errs, 1);
	    } else {
		(void) __sync_fetch_and_add(&aio_read_iops, 1);
		rt_record_op(&tstart, &(rtstats_read[myid]));
	    }
	}
	if (myid == 0) {
	    if ((i % dump_thresh) == 0) {
		dump_progress(i, iters_per_thread, n_keys_written, objs_per_thread);
	    }
	}
    }
    if (myid == 0) {
	prtstuff("\n");
    }

    //  Close the Container.
    // ZSCloseContainer(thd_state, cguid);

    //  Release per-thread state.
    // ZSReleasePerThreadState(&thd_state);

    // fprintf(stderr, "... %"PRIu64" finished!n", myid);
    if (myid == 0) {
	dump_progress(i, iters_per_thread, n_keys_written, objs_per_thread);
    }
    return(NULL);
}

static void dump_progress(uint64_t i, uint64_t iters_per_thread, uint32_t n_keys_written, uint64_t objs_per_thread)
{
    double dt;
    struct timeval res;

    gettimeofday(&t, NULL);
    timersub(&t, &tlast, &res);
    tlast = t;
    dt = res.tv_sec + ((double) res.tv_usec)/1000000.;
    prtstuff(" %7d %5.3g%% (%8.2g kIOPs) %8d distinct objects (%7.3g %%)\n", i, floor(0.5+100.*((double) i)/iters_per_thread), ((double) aio_read_iops - last_read_iops + aio_write_iops - last_write_iops)/dt/1000., n_keys_written, 100.0*((double) n_keys_written)/((double) objs_per_thread));
    last_read_iops = aio_read_iops;
    last_write_iops = aio_write_iops;
    fflush(f_stats_file);
}

static void process_stats(rt_statistics_t *rtstats)
{
    uint64_t   i, j;
    uint64_t   nbits;
    double     n, x, z, nsamples;
    double     xmin, xmax, sum, sum2;
    double     counts[64];

    for (i=0; i<64; i++) {
        counts[i] = 0;
    }

    xmin = 1.0e64;
    xmax = 0;
    n    = 0;
    sum  = 0;
    sum2 = 0;

    for (nbits=0; nbits<64; nbits++) {
        nsamples = rtstats->dist[nbits];
	counts[nbits] = nsamples;
	x = (1ULL<<nbits);
        x *= 2;
	if (nsamples > 0) {
	    if (x < xmin) {
	        xmin = x;
	    }
	    if (x > xmax) {
	        xmax = x;
	    }
	}
	sum  += nsamples*x;
	sum2 += nsamples*x*x;
	n += nsamples;

        if (nbits > 60) {
	    break;
	}
    }
    if (n == 0) {
	prtstuff("%g samples\n", n);
        return;
    }

    prtstuff("------------------------------------------------------------------\n");
    prtstuff("Values are in milliseconds!\n\n");
    z = 0;
    for (i=0; i<64; i++) {
        x = 1ULL << i;
        x *= 2;
	if (x < xmin) {
	    continue;
	}
	z += counts[i];
	// printf("%10g (%10.3g):", x/1000, counts[i]);
	prtstuff("%10.4g (%7.5g):", x/(cpu_mhz*1000), z/n*100.0);
        for (j=0; j<(counts[i]/n*100); j++) {
	    prtstuff2("=");
	}
	prtstuff2("\n");
	if (x > xmax) {
	    break;
	}
    }

    prtstuff("------------------------------------------------------------------\n");
    prtstuff("%g samples\n", n);
    prtstuff("avg = %8.4g ms\n", sum/n/(cpu_mhz*1000));
    prtstuff("min = %8.4g ms\n", xmin/(cpu_mhz*1000));
    prtstuff("max = %8.4g ms\n", xmax/(cpu_mhz*1000));
    prtstuff("sd  = %8.4g ms\n", sqrt(sum2/n - (sum/n)*(sum/n))/(cpu_mhz*1000));
    prtstuff("------------------------------------------------------------------\n");
}



