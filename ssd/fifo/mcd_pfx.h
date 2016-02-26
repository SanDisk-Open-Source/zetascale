/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_pfx.h
 * Author: Mingqiang Zhuang, Xiaonan Ma
 *
 * Created on Mar 20, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_pfx.h 14320 2010-07-08 21:33:59Z xiaonan $
 */

#ifndef __MCD_PFX_H_
#define __MCD_PFX_H_


#include <unistd.h>
#include <stdint.h>
#include <stdbool.h>

#include "platform/logging.h"
#include "platform/stdlib.h"
#include "platform/assert.h"
#include "platform/stats.h"
#include "platform/stdio.h"
#include "common/sdftypes.h"

#define MAX_CONTAINER_SIZE ((uint64_t)512 * 1024 * 1024 * 1024)
#define DEFAULT_SEGMENT_SIZE (32 * 1024 * 1024)
#define MIN_PREFIXES_TABLE_SIZE 64
#define MAX_PREFIXES_TABLE_SIZE 8192
#define MIN_PREFIX_LEN 0

/* maximal key size is 250, at less one delimiter */
#define MAX_PREFIX_LEN 249

typedef struct prefix_item prefix_item_t;
struct prefix_item {
    int                 prefix_len;    /* prefix length */
    uint32_t            start_time;    /* time when the prefix is registered */
    uint64_t            start_cursor;  /* start cursor when registered */
    prefix_item_t     * prefix_prev;   /* prev prefix entry in the table */
    prefix_item_t     * prefix_next;   /* next prefix entry in the table */
    prefix_item_t     * cursor_prev;   /* prev entry in the cursor table */
    prefix_item_t     * cursor_next;   /* next entry in the cursor table */
    char              * prefix[];      /* the prefix end with '\n' */
};

typedef struct prefix_handle prefix_handle_t;
struct prefix_handle {
    uint64_t            container_size;        /* container size */
    int                 segment_size;          /* segment size */
    int                 max_num_prefixes;      /* maximal number of prefixes */
    uint64_t            total_items_memory;    /* total memory used */
    int                 num_prefixes;          /* entries in prefix table */
    int                 total_prefixes_size;   /* total size in prefix table */
    int                 prefixes_table_size;   /* number of prefix buckets */
    int                 cursor_table_size;     /* number of cursor buckets */
    uint64_t            total_alloc;            /* total memory allocated */
    prefix_item_t     * prefixes_table;        /* prefixes table */
    prefix_item_t     * cursor_table;          /* cursor table */
};

#define ITEM_prefix(item) ((char*)&((item)->prefix[0]))
#define ITEM_SIZE(prefix_len) (sizeof(prefix_item_t) + prefix_len + 1)  /* one extra char for '\n' */


/**
 * Initialize the prefix hash table structure for delete with
 * prefix.Each container has one prefix hash table structure. So
 * each container need run this function only once.
 *
 * @param max_num_prefixes maximal number of prefixes the prefix
 *                         table can store.
 *
 * @return void* returns a handle to the prefix table
 */
void * mcd_prefix_init( int max_num_prefixes );

/**
 * Register one prefix. When memcache receive one delete prefix
 * command, need call this function once.
 *
 * @param handle Prefix table handle
 * @param prefix_ptr Pointer of prefix
 * @param prefix_len Prefix length
 * @param start_cursor current cursor of the container which the
 *                     auto delete thread handles currently.
 *
 * @return int If success, return 0, else return -1.
 */
SDF_status_t
mcd_prefix_register( void * handle, char * prefix_ptr, int prefix_len,
                     uint32_t start_time, uint64_t start_cursor,
                     uint64_t total_blks );

/**
 * Look up one prefix to know whether the prefix exists in the
 * prefix table.
 *
 * @param handle Prefix table handle
 * @param prefix_ptr Pointer of prefix
 * @param prefix_len Prefix length
 *
 * @return void* returns prefix item pointer when the prefix is
 *         found, NULL not found.
 */
void * mcd_prefix_lookup( void * handle, char * prefix_ptr, int prefix_len );

/**
 * Removes all prefixes whose start_cursor == curr_cursor
 *
 * @param handle Prefix table handle
 * @param curr_cursor current cursor of the container which the
 *                    auto delete thread will handle
 *                    immediately.
 *
 * @return int If success, return 0, else return -1.
 */
SDF_status_t
mcd_prefix_update( void * handle, uint64_t curr_cursor, uint64_t total_blks );

/**
 * Dump a list of all current prefixes.
 *
 * @param handle Prefix table handle
 * @param bufp Returned buffer with all the current prefixes.
 *
 * @return int If find prefixes, return number of prefixes,
 *         *bufp != NULL, else return 0, *bufp == NULL.
 */
int mcd_prefix_list( void * handle, char ** bufp, uint64_t curr_cursor,
                     uint64_t total_blks, uint64_t curr_blks );

/**
 * Reset the prefix table, all the prefixes will be deleted.
 *
 * @param handle Prefix table handle
 *
 * @return int If success, return 0, else return -1.
 */
int mcd_prefix_reset( void * handle );

/**
 * Clean up the prefix table.
 *
 * @param handle Prefix table handle
 *
 * @return int If success, return 0, else return -1.
 */
int mcd_prefix_cleanup( void * handle );

/**
 * Return total amount of memory allocated
 *
 * @param handle Prefix table handle
 *
 * @return uint64_t total size of memory allocated (in bytes)
 */
uint64_t mcd_prefix_total_alloc( void * handle );


#endif /* __MCD_PFX_H_ */
