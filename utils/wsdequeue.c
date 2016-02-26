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

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

typedef int work_t;
typedef __uint32_t uint32_t;
typedef __uint64_t uint64_t;
typedef __uint64_t tagged_int_t;

#define NUM_TAG_BITS 32
#define TAG_MASK ((1LL << NUM_TAG_BITS) - 1LL)

//#define TEST_WSDEQUEUE

typedef struct wsdq
{
    tagged_int_t head; // steal from the head
    uint32_t tail; // push and pop from the tail

    uint32_t size;
    work_t *work[0];
} wsdq_t;

static inline tagged_int_t tagged_int_create (uint32_t tag, int i)
{
    return (((uint64_t)i << NUM_TAG_BITS) | ((uint64_t)tag & TAG_MASK));
}

static inline uint32_t tagged_int_get_tag (tagged_int_t x)
{
    return (uint32_t)(x & TAG_MASK);
}

static inline int tagged_int_get_int (tagged_int_t x)
{
    return (int)(x >> NUM_TAG_BITS);
}

wsdq_t *wsdq_alloc (uint32_t size)
{
    uint32_t n = sizeof(wsdq_t) + sizeof(work_t) * size;
    wsdq_t *dq = (wsdq_t *)malloc(n);

    memset(dq, 0, n);
    dq->size = size;

    return dq;
}

static inline void wsdq_push (wsdq_t *dq, work_t *item)
{
    dq->work[dq->tail] = item;
    __asm__ __volatile__("sfence");
    dq->tail++;
}

static inline work_t *wsdq_pop (wsdq_t *dq)
{
    uint32_t old_tail = dq->tail;
    uint32_t new_tail = dq->tail - 1;

    if (old_tail == 0)
        return NULL;

    work_t *item = dq->work[new_tail];

    dq->tail = new_tail;
    __asm__ __volatile__("mfence");
    tagged_int_t old_head = dq->head;

    if (new_tail > tagged_int_get_int(old_head))
        return item;

    dq->tail = 0;
    tagged_int_t new_head = tagged_int_create(tagged_int_get_tag(old_head) + 1, 0);
    if (new_tail == tagged_int_get_int(old_head))
    {
        if (__sync_val_compare_and_swap(&dq->head, old_head, new_head) == old_head)
            return item;
    }

    dq->head = new_head;
    return NULL;
}


static inline work_t *wsdq_steal (wsdq_t *dq)
{
    tagged_int_t old_head = dq->head;
    tagged_int_t new_head = tagged_int_create(tagged_int_get_tag(old_head) + 1, tagged_int_get_int(old_head) + 1);

    __asm__ __volatile__("lfence");
    if (dq->tail <= tagged_int_get_int(old_head))
        return NULL;

    work_t *item = dq->work[tagged_int_get_int(old_head)];
    if (__sync_val_compare_and_swap(&dq->head, old_head, new_head) == old_head)
        return item;

    return NULL;
}

#ifdef TEST_WSDEQUEUE

#include <stdio.h>
#include <pthread.h>

#define NUM_ITERATIONS 10000000
#define MAX_WORK_ITEMS 16
#define NUM_WORKER_THREADS 2

static volatile int wait_;
static wsdq_t *dq_;
uint64_t num_pushes_ = 0;
uint64_t num_pops_ = 0;
static uint64_t num_steals_ = 0;

static inline unsigned int rdtsc_l(void)
{
    unsigned int u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u)); 
    return l; 
}

static void dowork (work_t *x)
{
    int i;
    int n = *x;
    for (i = 0; i < n * 100; +++i) { i = i % (i + 1); }
}

void *worker (void *arg)
{
    // Wait for all the worker threads to be ready.
    (void)__sync_fetch_and_add(&wait_, -1);
    do {} while (wait_); 

    int i;
    for (i = 0; i < NUM_ITERATIONS; ++i)
    {
        work_t *x = wsdq_steal(dq_);
        if (x)
        {
            (void)__sync_fetch_and_add(&num_steals_, 1);
            dowork(x);
        }
    }

    return NULL;
}

int main (void)
{
    pthread_t thread[NUM_WORKER_THREADS];
    work_t work_items[MAX_WORK_ITEMS] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    uint32_t rand_seed = rdtsc_l();
    dq_ = wsdq_alloc(MAX_WORK_ITEMS);  

    wait_ = NUM_WORKER_THREADS;

    int i;

    for (i = 0; i < NUM_WORKER_THREADS; ++i)
    {
        int rc = pthread_create(thread + i, NULL, worker, NULL);
        if (rc != 0) { perror("pthread_create"); return rc; }
    }

    for (i = 0; i < NUM_ITERATIONS; ++i)
    {
        int n = rand_r(&rand_seed);
        if (n & 0xF)
        {
            if (dq_->tail < MAX_WORK_ITEMS)
            {
                wsdq_push(dq_, &work_items[(n >> 2) & 0xF]);
                num_pushes_++;
            }
        }
        else
        {
            work_t *x = wsdq_pop(dq_); 
            if (x)
            {
                num_pops_++;
                dowork(x);
            }
        }
    }

    for (i = 0; i < NUM_WORKER_THREADS; ++i)
    {
        pthread_join(thread[i], NULL);
    }

    uint32_t remainder = dq_->tail - tagged_int_get_int(dq_->head);
    printf("pushes: %llu pops: %llu steals: %llu remainder: %u total: %llu\n", 
            (unsigned long long int)num_pushes_, 
            (unsigned long long int)num_pops_, 
            (unsigned long long int)num_steals_, remainder, 
            (unsigned long long int)num_pops_ + num_steals_ + remainder);
    fflush(stdout);
    return 0;
}

void printd(const char *s)
{
    __asm__ __volatile__("mfence");
    uint32_t remainder = dq_->tail - tagged_int_get_int(dq_->head);

    printf("%s\t(tag:%u head:%u tail:%u pushes:%llu pops:%llu steals:%llu remainder:%u total:%llu)\n", 
            s,
            tagged_int_get_tag(dq_->head), tagged_int_get_int(dq_->head), dq_->tail,
            (unsigned long long int)num_pushes_, 
            (unsigned long long int)num_pops_, 
            (unsigned long long int)num_steals_, 
            remainder, 
            (unsigned long long int)num_pops_ + num_steals_ + remainder
            );
}
#endif //TEST_WSDEQUEUE
