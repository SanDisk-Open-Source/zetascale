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

#include <pthread.h>
#include <limits.h>
#include "platform/assert.h"
#include "platform/string.h"
#include "platform/errno.h"
#include "platform/types.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"

#include "fthTrace.h"

#define MAX_NUM_THREADS 8

#define NUM_ITERATIONS 10000000

#define PLACE_MARK(x) ((typeof(x))(((size_t)(x))|1))
#define CLEAR_MARK(x) ((typeof(x))(((size_t)(x))&~(size_t)1))
#define IS_MARKED(x)  (((size_t)(x))&1)

typedef struct node {
    struct node *next;
    int key;
} node_t;

typedef struct list {
    node_t head[1];
    node_t last;
} list_t;

static void nodeInit (node_t *item, int key)
{
    memset(item, 0, sizeof(node_t));
    item->key = key;
}

node_t *nodeAlloc (int key)
{
    node_t *item = (node_t *)malloc(sizeof(node_t));
    TRACE("l3", "nodeAlloc: malloc returns %p", item);
    nodeInit(item, key);
    return item;
}

list_t *listAlloc (void)
{
    list_t *list = (list_t *)malloc(sizeof(list_t));
    nodeInit(list->head, INT_MIN);
    nodeInit(&list->last, INT_MAX);
    list->head->next = &list->last;
    return list;
}

static void listFindPredAndItem (node_t **predPtr, node_t **itemPtr, list_t *list, int key)
{
    node_t *pred = list->head;
    node_t *item = list->head->next; // head is never removed
    TRACE("l3", "listFindPredAndItem: searching for key %llu in list (head is %p)", key, pred);
    int count = 0;
    do {
        // skip removed items
        node_t *other, *next = item->next;
        TRACE("l3", "listFindPredAndItem: visiting item %p (next is %p)", item, next);
        while (PLAT_UNLIKELY(IS_MARKED(next))) {
            
            // assist in unlinking partially removed items
            if ((other = __sync_val_compare_and_swap(&pred->next, item, CLEAR_MARK(next))) != item)
            {
                TRACE("l3", "listFindPredAndItem: failed to unlink item from pred %p, pred's next pointer was changed to %p", pred, other);
                return listFindPredAndItem(predPtr, itemPtr, list, key); // retry
            }

            plat_assert(count++ < 18);
            item = CLEAR_MARK(next);
            next = item->next;
            TRACE("l3", "listFindPredAndItem: unlinked item, %p is the new item (next is %p)", item, next);
        }

        if (item->key >= key) {
            *predPtr = pred;
            *itemPtr = item;
            TRACE("l3", "listFindPredAndItem: key found, returning pred %p and item %p", pred, item);
            return;
        }

        plat_assert(count++ < 18);
        pred = item;
        item = next;

    } while (1);
}

int listInsert (list_t *list, node_t *item)
{
    TRACE("l3", "listInsert: inserting %p (with key %llu)", item, item->key);
    node_t *pred, *next, *other = (node_t *)-1;
    do {
        if (other != (node_t *)-1) {
            TRACE("l3", "listInsert: failed to swap item into list; pred's next was changed to %p", other, 0);
        }
        listFindPredAndItem(&pred, &next, list, item->key);

        // fail if item already exists in list
        if (next->key == item->key)
        {
            TRACE("l3", "listInsert: insert failed item with key already exists %p", next, 0);
            return 0;
        }

        item->next = next;
        TRACE("l3", "listInsert: attempting to insert item between %p and %p", pred, next);

    } while ((other = __sync_val_compare_and_swap(&pred->next, next, item)) != next);

    TRACE("l3", "listInsert: insert was successful", 0, 0);

    // success
    return 1;
}

node_t *listRemove (list_t *list, int key)
{
    node_t *pred, *item, *next;

    TRACE("l3", "listRemove: removing item with key %llu", key, 0);
    listFindPredAndItem(&pred, &item, list, key);
    if (item->key != key)
    {
        TRACE("l3", "listRemove: remove failed, key does not exist in list", 0, 0);
        return NULL;
    }

    // Mark <item> removed, must be atomic. If multiple threads try to remove the 
    // same item only one of them should succeed
    next = item->next;
    node_t *other = (node_t *)-1;
    if (IS_MARKED(next) || (other = __sync_val_compare_and_swap(&item->next, next, PLACE_MARK(next))) != next) {
        if (other == (node_t *)-1) {
            TRACE("l3", "listRemove: retry; %p is already marked for removal (it's next pointer is %p)", item, next);
        } else {
            TRACE("l3", "listRemove: retry; failed to mark %p for removal; it's next pointer was %p, but changed to %p", next, other);
        }
        return listRemove(list, key); // retry
    }

    // Remove <item> from list
    TRACE("l3", "listRemove: link item's pred %p to it's successor %p", pred, next);
    if ((other = __sync_val_compare_and_swap(&pred->next, item, next)) != item) {
        TRACE("l3", "listRemove: link failed; pred's link changed from %p to %p", item, other);

        // make sure item gets unlinked before returning it
        node_t *d1, *d2;
        listFindPredAndItem(&d1, &d2, list, key);
    } else {
        TRACE("l3", "listRemove: link succeeded; pred's link changed from %p to %p", item, next);
    }

    return item;
}

void listPrint (list_t *list)
{
    node_t *item;
    item = list->head;
    while (item) {
        printf("%d ", item->key);
        fflush(stdout);
        item = item->next;
    }
    printf("\n");
}

#define TEST_LFLIST
#ifdef TEST_LFLIST
static volatile int wait_;
static long numThreads_;
static list_t *list_;

void worker (uint64_t arg)
{
    unsigned int randSeed = (unsigned int)rdtsc();

    // Wait for all the worker threads to be ready.
    __sync_fetch_and_add(&wait_, -1);
    do {} while (wait_); 
    __asm__ __volatile__("lfence"); 

    int i;
    for (i = 0; i < NUM_ITERATIONS/numThreads_; ++i) {
        int n = rand_r(&randSeed);
        int key = (n & 0xF) + 1;
        if (n & (1 << 8)) {
            node_t *item = nodeAlloc(key);
            int success = listInsert(list_, item);
            if (!success) {
                free(item); 
            }
        } else {
            node_t *item = listRemove(list_, key);
            if (item) {
                fthRcuDeferFree(item);
            }
        }
        fthRcuUpdate();
    }
    if (__sync_add_and_fetch(&wait_, 1) == numThreads_) {
        fthKill(100);
    }
}

void *pthreadRoutine(void *arg) {
    fthSchedulerPthread(0);                  // This pthread becomes a scheduler
    return NULL;
}

int main (int argc, char **argv)
{
    pthread_t pthread[MAX_NUM_THREADS];

    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    plat_shmem_prototype_init(shmem_config);
    const char *path = plat_shmem_config_get_path(shmem_config);
    plat_shmem_attach(path);

    char* progName = argv[0];

    if (argc != 2) {
        fprintf(stderr, "Usage: %s num_threads\n", progName);
        return -1;
    }

    errno = 0;
    numThreads_ = strtol(argv[1], NULL, 10);
    if (errno) {
        fprintf(stderr, "%s: Invalid argument for number of threads\n", progName);
        return -1;
    }
    if (numThreads_ <= 0) {
        fprintf(stderr, "%s: Number of threads must be at least 1\n", progName);
        return -1;
    }
    if (numThreads_ > MAX_NUM_THREADS) {
        fprintf(stderr, "%s: Number of threads cannot be more than %d\n", progName, MAX_NUM_THREADS);
        return -1;
    }

    list_ = listAlloc();

    __asm__ __volatile__("sfence"); 
    wait_ = numThreads_;

    fthInit();

    struct timeval tv1, tv2;
    plat_gettimeofday(&tv1, NULL);

    size_t i;
    for (i = 0; i < numThreads_; ++i) {
        int rc = pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
        plat_assert(rc == 0);
        XResume(fthSpawn(&worker, 40960), i);
    }

    for (i = 0; i < numThreads_; ++i) {
        pthread_join(pthread[i], NULL);
    }

    plat_gettimeofday(&tv2, NULL);

    int ms = (int)(1000000*(tv2.tv_sec - tv1.tv_sec) + tv2.tv_usec - tv1.tv_usec) / 1000;
    printf("Th:%ld Time:%dms\n", numThreads_, ms);
    listPrint(list_);
    fthTraceDump("fthListTest.out");

    return 0;
}
#endif//TEST_LFLIST
