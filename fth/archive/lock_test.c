/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#define _GNU_SOURCE
#include <sched.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#define CACHE_LINE_SIZE 64

#define MAX_NUM_THREADS 4

#ifndef NUM_ITERATIONS
#define NUM_ITERATIONS 5000000
#endif

#ifndef INITIAL_BACKOFF_VALUE
#define INITIAL_BACKOFF_VALUE 7500
#endif

#ifndef RANDOM_BACKOFF_MASK
#define RANDOM_BACKOFF_MASK 0x3FFF// min value 0, max value 16384, expected value 8192
#endif

//#define NO_ACTION
//#define ACTION_COUNT

//#define LOCK_TYPE_XCHG
//#define LOCK_TYPE_BOOL_CAS // broken count and/or infinite loop
//#define LOCK_TYPE_CAS

//#define NO_BACKOFF
//#define BACKOFF_LINEAR
//#define BACKOFF_CONSTANT
//#define BACKOFF_EXPONENTIAL
//#define BACKOFF_RANDOM

//#define DELAY_WITH_MOD
//#define DELAY_WITH_REP_NOP
//#define DELAY_WITH_EMPTY_LOOP

//#define FAST_CHECK_LOCK 
//#define NO_FAST_CHECK

#if defined(FAST_CHECK_LOCK)
#define PRE_TEST_LOCK (lock_.l)
#else
#define PRE_TEST_LOCK 0
#endif

typedef union
{
    __uint32_t l;
#ifdef PAD_LOCKS
    char padding[CACHE_LINE_SIZE];
#endif
} lock_t;

typedef union
{
    struct
    {
        int id;
#if defined(LOCK_TYPE_DYNAMIC_QUEUE)
        volatile lock_t *next_lock_node;
        volatile lock_t *prev_lock_node; 
#elif defined(LOCK_TYPE_FIXED_QUEUE)
        unsigned int lock_position;
#endif
#if defined(BACKOFF_RANDOM)
        unsigned int rand_seed;
#endif
    };
    char padding[CACHE_LINE_SIZE];
} worker_data_t;

#if defined(LOCK_TYPE_DYNAMIC_QUEUE)
static volatile lock_t lock_nodes_[MAX_NUM_THREADS + 1];
static volatile lock_t *lock_tail_;
#elif defined(LOCK_TYPE_FIXED_QUEUE)
static volatile lock_t lock_[MAX_NUM_THREADS];
static volatile unsigned int tail_position_;
#else
static volatile lock_t lock_;
#endif

static volatile int count_;
static volatile int wait_;

static int affinity_offset_;
static int affinity_stride_;

static const char* program_name_;

static inline unsigned int rdtsc_l(void)
{
    unsigned int u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u)); 
    return l; 
}

void init_lock (void)
{
#if defined(LOCK_TYPE_DYNAMIC_QUEUE) 
    memset((lock_t *)lock_nodes_, 0, sizeof(lock_nodes_));
    lock_tail_ = lock_nodes_ + MAX_NUM_THREADS;
#elif defined(LOCK_TYPE_FIXED_QUEUE)
    memset((lock_t *)lock_, 0, sizeof(lock_));
    tail_position_ = 0;
    lock_[tail_position_].l = 1; 
#else
    lock_.l = 0;
#endif
    __asm__ __volatile__("mfence");
}

static inline void aquire_lock(worker_data_t *wd)
{
#if defined(BACKOFF_RANDOM)
    int delay = wd->rand_seed & RANDOM_BACKOFF_MASK;
#elif !defined(NO_BACKOFF)
    int delay = INITIAL_BACKOFF_VALUE;
#endif

#if defined(LOCK_TYPE_DYNAMIC_QUEUE)
    wd->next_lock_node->l = 1;
    wd->prev_lock_node = __sync_lock_test_and_set(&lock_tail_, wd->next_lock_node);
    while (wd->prev_lock_node)

#elif defined(LOCK_TYPE_FIXED_QUEUE)
    wd->lock_position = __sync_fetch_and_add(&tail_position_, 1) % MAX_NUM_THREADS;
    while (((volatile lock_t *)lock_)[wd->lock_position].l != 0)

#elif defined(LOCK_TYPE_XCHG)
    while (PRE_TEST_LOCK == 1 || __sync_lock_test_and_set(&lock_.l, 1) != 0)

#elif defined(LOCK_TYPE_BOOL_CAS) // WARNING: count broken and/or infinite loop
    while (PRE_TEST_LOCK == 1 || __sync_bool_compare_and_swap(&lock_.l, 0, 1))

#elif defined(LOCK_TYPE_CAS)
    while (PRE_TEST_LOCK == 1 || __sync_val_compare_and_swap(&lock_.l, 0, 1) != 0)
#endif
    {

#ifndef NO_BACKOFF
        // Spin wait
        for (int i = 0; i < delay; ++i) 
        {
#if defined(DELAY_WITH_REP_NOP)
            __asm__ __volatile__("rep;nop" ::: "memory");
#elif defined(DELAY_WITH_MOD)
            i = i % (i + 1); 
#endif
        }

#if defined(BACKOFF_LINEAR)
        delay += INITIAL_BACKOFF_VALUE;
#elif defined(BACKOFF_EXPONENTIAL)
        delay += delay;
#elif defined(BACKOFF_RANDOM)
        rand_r(&wd->rand_seed);
        delay = wd->rand_seed & RANDOM_BACKOFF_MASK;
#endif

#endif//BACKOFF
    }
}

static inline void release_lock(worker_data_t *wd)
{
    // __sync_synchronize() is broken on gcc 4.2. It doesn't insert a fence 
    // like it should. So use an inline assembly sfence instead.
#if defined(LOCK_TYPE_DYNAMIC_QUEUE)
    __asm__ __volatile__("sfence"); 
    wd->next_lock_node->l = 0;
    wd->next_lock_node = wd->prev_lock_node;

#elif defined(LOCK_TYPE_FIXED_QUEUE)
    // explicit cast to volatile, becuase gcc seems to ignore the volatile 
    // qualifier on arrays
    ((volatile lock_t *)lock_)[wd->lock_position].l = 0;
    __asm__ __volatile__("sfence"); 
    ((volatile lock_t *)lock_)[(wd->lock_position + 1) % MAX_NUM_THREADS].l = 1;

#else
    __asm__ __volatile__("sfence"); 
    lock_.l = 0;
#endif
}

static void *worker (void *arg)
{
    worker_data_t *wd = (worker_data_t *)arg;

    if (affinity_offset_ >= 0)
    {
        cpu_set_t cpu_set;
        int rc = sched_getaffinity(0, sizeof(cpu_set_t), &cpu_set);
        if (rc != 0) 
        {
            perror(program_name_);
            _Exit(-errno);
        }

        int num_cpus = 0;
        for (int i = 0; i < (sizeof(cpu_set_t) * 8); i++) {
            if (CPU_ISSET(i, &cpu_set)) {
                num_cpus++;
            }
        }

        int i = (affinity_offset_ + affinity_stride_ * (wd->id - 1)) % num_cpus;
        CPU_ZERO(&cpu_set);
        CPU_SET(i, &cpu_set);
        rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
        if (rc != 0) 
        {
            perror(program_name_);
            _Exit(-errno);
        }
    }

#ifdef LOCK_TYPE_DYNAMIC_QUEUE
    wd->next_lock_node = lock_nodes_ + wd->id - 1;
#endif

#ifdef BACKOFF_RANDOM
    wd->rand_seed = rdtsc_l();
#endif

    // Wait for all the worker threads to be ready.
    do {} while (wait_); 

    int i;
    for (i = 0; i < NUM_ITERATIONS; ++i)
    {
        aquire_lock(wd);
#ifdef ACTION_COUNT
        count_++;
#endif
        release_lock(wd);
    }

    return 0;
}

int main (int argc, char **argv)
{
    long num_threads;
    pthread_t thread[MAX_NUM_THREADS];
    worker_data_t worker_data[MAX_NUM_THREADS];
    const char *config_string;

    if (argc != 5)
    {
        fprintf(stderr, "Usage: %s num_threads affinity_offset affinity_stride config_string\n", program_name_);
        return -1;
    }

    program_name_ = argv[0];

    errno = 0;
    num_threads = strtol(argv[1], NULL, 10);
    if (errno)
    {
        fprintf(stderr, "%s: Invalid argument for number of threads\n", program_name_);
        return -1;
    }
    if (num_threads <= 0)
    {
        fprintf(stderr, "%s: Number of threads must be at least 1\n", program_name_);
        return -1;
    }
    if (num_threads > MAX_NUM_THREADS)
    {
        fprintf(stderr, "%s: Number of threads cannot be more than %d\n", program_name_, MAX_NUM_THREADS);
        return -1;
    }

    errno = 0;
    affinity_offset_ = strtol(argv[2], NULL, 10);
    if (errno)
    {
        fprintf(stderr, "%s: Invalid argument for the affinity offset\n"
                        "If you don't want to set affinity, use an offset of -1\n", 
                        program_name_);
        return -1;
    }

    errno = 0;
    affinity_stride_ = strtol(argv[3], NULL, 10);
    if (errno)
    {
        fprintf(stderr, "%s: Invalid argument for the affinity stride\n", program_name_);
        return -1;
    }

    config_string = argv[4];

    init_lock();
    count_ = 0;

    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    wait_ = 1;
    size_t i;
    for (i = 0; i < num_threads; ++i)
    {
        worker_data[i].id = i+1;
        int rc = pthread_create(thread + i, NULL, worker, &worker_data[i]);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }
    wait_ = 0;

    for (i = 0; i < num_threads; ++i)
    {
        pthread_join(thread[i], NULL);
    }

    gettimeofday(&tv2, NULL);
    printf("Th:%ld Af:%+d,%d Time:%dms ", num_threads, affinity_offset_, affinity_stride_, (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000);
#ifdef ACTION_COUNT
    if (count_ != num_threads * NUM_ITERATIONS)
    {
        printf("Invalid Count:%d (should be %ld) ", count_, num_threads * NUM_ITERATIONS);
    }
#endif
    printf("Config:%s\n", config_string);

    return 0;
}
