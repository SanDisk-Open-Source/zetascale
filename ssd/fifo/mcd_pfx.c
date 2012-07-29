/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_pfx.h
 * Author: Mingqiang Zhuang, Xiaonan Ma
 *
 * Created on Mar 20, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_pfx.c 14320 2010-07-08 21:33:59Z xiaonan $
 */

#include "mcd_pfx.h"
//#include "memcached.h"

#define mcd_dbg_msg(...)        plat_log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_APP_MEMCACHED, __VA_ARGS__ )

#ifdef  MCD_PFX_DEBUGGING
#  define MCD_PFX_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_PFX_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DIAGNOSTIC
#  define MCD_PFX_LOG_LVL_INFO  PLAT_LOG_LEVEL_INFO
#else
#  define MCD_PFX_LOG_LVL_INFO  PLAT_LOG_LEVEL_DEBUG
#  define MCD_PFX_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_PFX_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DEBUG
#endif

/*
 * from assoc.c
 */
extern uint32_t mcd_hash( const void *key, size_t length,
                          const uint32_t initval );


static bool is_power_of_2( int n )
{
    return( ( n & ( n-1 ) ) == 0 );
}


static int min_power_of_2_g( int n )
{
    n |= n >> 16;
    n |= n >> 8;
    n |= n >> 4;
    n |= n >> 2;
    n |= n >> 1;

    return( n + 1 );
}


static int min_power_of_2_eg( int n )
{
    if ( is_power_of_2( n ) ) {
        return n;
    }
    return min_power_of_2_g( n );
}


void * mcd_prefix_init( int max_num_prefixes )
{
    int                         table_size = 0;
    prefix_handle_t           * handle = NULL;

    if ( max_num_prefixes <= 0 ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "maximal number of prefixes must be greater than 0" );
        return NULL;
    }

    handle = (prefix_handle_t *)plat_alloc( sizeof( prefix_handle_t ) );
    if ( handle == NULL ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "allocate memory for prefix handler fail" );
        return NULL;
    }
    memset( handle, 0, sizeof( prefix_handle_t ) );
    handle->container_size = MAX_CONTAINER_SIZE;
    handle->segment_size = DEFAULT_SEGMENT_SIZE;
    handle->max_num_prefixes = max_num_prefixes;
    handle->total_alloc = sizeof( prefix_handle_t );

    table_size = min_power_of_2_eg( max_num_prefixes / 4 );
    if ( table_size <= MIN_PREFIXES_TABLE_SIZE ) {
        handle->prefixes_table_size = MIN_PREFIXES_TABLE_SIZE;
    }
    else if ( table_size > MAX_PREFIXES_TABLE_SIZE ) {
        handle->prefixes_table_size = MAX_PREFIXES_TABLE_SIZE;
    }
    else {
        handle->prefixes_table_size = table_size;
    }

    /**
     * Each prefix bucket of the prefixes table has a header, it's
     * used to make delete or insert one prefix item easier. In the
     * other hand, when walking through one bucket of cursor table
     * and delete all the prefix items in the cursor bucket from the
     * prefixes table, we don't need to check whether the prefix
     * item is a header of a bucket in the prefixes table.
     */
    handle->prefixes_table = (prefix_item_t *)plat_alloc(
        handle->prefixes_table_size * sizeof( prefix_item_t ) );
    if ( handle->prefixes_table == NULL ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "allocate memory for prefixes table fail" );
        plat_free(handle);
        return NULL;
    }
    memset( handle->prefixes_table, 0,
            handle->prefixes_table_size * sizeof( prefix_item_t ) );
    handle->total_alloc +=
        handle->prefixes_table_size * sizeof( prefix_item_t );

    /**
     * Each cursor bucket of the cursor table has a header, it's
     * used to make delete or insert one prefix item easier.
     * Especially when delete several items in a for loop, we don't
     * need to check whether the prefix item is a header of a bucket
     * in the cursor table.
     */
    handle->cursor_table_size = handle->container_size / handle->segment_size;
    handle->cursor_table = (prefix_item_t *)plat_alloc(
        handle->cursor_table_size * sizeof( prefix_item_t ) );
    if ( handle->cursor_table == NULL ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "allocate memory for cursor table fail" );
        plat_free( handle->prefixes_table );
        plat_free( handle );
        return NULL;
    }
    memset( handle->cursor_table, 0,
            handle->cursor_table_size * sizeof( prefix_item_t ) );
    handle->total_alloc +=
        handle->cursor_table_size * sizeof( prefix_item_t );

    return handle;
}


static bool check_memory( prefix_handle_t * handle )
{
    return( handle->total_items_memory ==
            ( handle->total_prefixes_size +
              handle->num_prefixes * sizeof( prefix_item_t ) ) );
}


static prefix_item_t * prefix_item_alloc( prefix_handle_t * handle, int size )
{
    prefix_item_t             * item = NULL;

    plat_assert( handle != NULL );

    item = (prefix_item_t *)plat_alloc( size );
    if ( item == NULL ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "allocate memory for prefix item fail" );
        return NULL;
    }
    memset( item, 0, size );
    handle->total_items_memory += size;

    return item;
}


static void  prefix_item_free( prefix_handle_t * handle, prefix_item_t * item )
{
    plat_assert( handle != NULL );

    if ( item != NULL ) {
        handle->total_items_memory -= ITEM_SIZE( item->prefix_len );
        plat_free( item );
        plat_assert( check_memory( handle ) );  /* FIXME_PREFIX_DELETE */
    }
}


SDF_status_t
mcd_prefix_register( void * handle, char * prefix_ptr, int prefix_len,
                     uint32_t start_time, uint64_t start_cursor,
                     uint64_t total_blks )
{
    prefix_handle_t           * prefix_handle = (prefix_handle_t *)handle;
    prefix_item_t             * item = NULL;
    prefix_item_t             * prefix_bucket = NULL;
    int                         prefix_bucket_no = 0;
    prefix_item_t             * cursor_bucket = NULL;
    int                         cursor_bucket_no = 0;

    if ( NULL == prefix_handle || NULL == prefix_ptr ||
         prefix_len < MIN_PREFIX_LEN || prefix_len > MAX_PREFIX_LEN ||
         start_cursor >= prefix_handle->container_size ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR, "invalid parameter, "
                     "hdl=%p pfx=%p len=%d cur=%lu cntr_size=%lu",
                     prefix_handle, prefix_ptr, prefix_len, start_cursor,
                     NULL == prefix_handle ?
                     0 : prefix_handle->container_size );
        return SDF_INVALID_PARAMETER;
    }
    mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO, "ENTERING prefix=%s start_cursor=%lu",
                 prefix_ptr, start_cursor );

    /* check whether prefix is existent */
    if ( NULL !=
         ( item = mcd_prefix_lookup( handle, prefix_ptr, prefix_len ) ) ) {
        /* delete item from the old cursor bucket list */
        if ( item->cursor_next != NULL ) {
            item->cursor_next->cursor_prev = item->cursor_prev;
        }
        plat_assert( item->cursor_prev != NULL );
        item->cursor_prev->cursor_next = item->cursor_next;

        item->start_time = start_time;
        item->start_cursor = start_cursor;

        /* insert item into new cursor bucket list */
        cursor_bucket_no =
            ( start_cursor % total_blks ) / prefix_handle->segment_size;
        cursor_bucket = &prefix_handle->cursor_table[cursor_bucket_no];
        item->cursor_prev = cursor_bucket;
        item->cursor_next = cursor_bucket->cursor_next;
        if ( cursor_bucket->cursor_next != NULL ) {
            cursor_bucket->cursor_next->cursor_prev = item;
        }
        cursor_bucket->cursor_next = item;

        return SDF_SUCCESS;
    }

    if ( prefix_handle->num_prefixes >= prefix_handle->max_num_prefixes ) {
        return SDF_OUT_OF_MEM;
    }

    item = prefix_item_alloc( prefix_handle, ITEM_SIZE(prefix_len) );
    if ( item == NULL ) {
        return SDF_FAILURE_MEMORY_ALLOC;
    }

    prefix_bucket_no = mcd_hash( prefix_ptr, prefix_len, 0 ) %
        prefix_handle->prefixes_table_size;
    prefix_bucket = &prefix_handle->prefixes_table[prefix_bucket_no];
    item->prefix_len = prefix_len;
    memcpy( ITEM_prefix(item), prefix_ptr, prefix_len );
    ITEM_prefix(item)[prefix_len] = '\0';
    item->start_time = start_time;
    item->start_cursor = start_cursor;

    /* insert item into prefix bucket list */
    item->prefix_prev = prefix_bucket;
    item->prefix_next = prefix_bucket->prefix_next;
    if ( prefix_bucket->prefix_next != NULL ) {
        prefix_bucket->prefix_next->prefix_prev = item;
    }
    prefix_bucket->prefix_next = item;

    /* insert item into cursor bucket list */
    cursor_bucket_no =
        ( start_cursor % total_blks ) / prefix_handle->segment_size;
    cursor_bucket = &prefix_handle->cursor_table[cursor_bucket_no];
    item->cursor_prev = cursor_bucket;
    item->cursor_next = cursor_bucket->cursor_next;
    if ( cursor_bucket->cursor_next != NULL ) {
        cursor_bucket->cursor_next->cursor_prev = item;
    }
    cursor_bucket->cursor_next = item;

    prefix_handle->num_prefixes++;
    prefix_handle->total_prefixes_size += ( prefix_len + 1 );

    mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO, "prefix %s registered, bucket_no=%d",
                 ITEM_prefix(item), cursor_bucket_no );

    return SDF_SUCCESS;
}


void * mcd_prefix_lookup( void * handle, char * prefix_ptr, int prefix_len )
{
    prefix_handle_t           * prefix_handle = (prefix_handle_t *)handle;
    prefix_item_t             * item = NULL;
    prefix_item_t             * prefix_bucket = NULL;
    int                         prefix_bucket_no = 0;
    static uint64_t             count = 0;

    if ( NULL == prefix_handle || NULL == prefix_ptr ||
         prefix_len < MIN_PREFIX_LEN || prefix_len > MAX_PREFIX_LEN  ) {
        if ( 1000 > count ||
             ( 0 == ( count % 1024 ) && 1048576 > count ) ||
             ( 0 == ( count % 65536 ) && 1048576 <= count ) ) {
            mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                         "invalid parameter, hdl=%p pfx=%p len=%d count=%lu",
                         prefix_handle, prefix_ptr, prefix_len, count );
        }
        count++;
        return NULL;
    }

    prefix_bucket_no = mcd_hash( prefix_ptr, prefix_len, 0 ) %
        prefix_handle->prefixes_table_size;
    prefix_bucket = &prefix_handle->prefixes_table[prefix_bucket_no];

    for ( item = prefix_bucket->prefix_next; item != NULL;
          item = item->prefix_next ) {
        if ( item->prefix_len == prefix_len
             && strncmp( ITEM_prefix(item), prefix_ptr, prefix_len ) == 0 ) {
            return item;
        }
    }

    return NULL;
}


SDF_status_t mcd_prefix_update( void * handle, uint64_t curr_cursor,
                                uint64_t total_blks )
{
    prefix_handle_t           * prefix_handle = (prefix_handle_t *)handle;
    prefix_item_t             * item = NULL;
    prefix_item_t             * next_item = NULL;
    prefix_item_t             * cursor_bucket = NULL;
    int                         cursor_bucket_no = 0;

    if ( NULL == prefix_handle ||
         curr_cursor >= prefix_handle->container_size ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "invalid parameter, hdl=%p cur=%lu cntr_size=%lu",
                     prefix_handle, curr_cursor,
                     NULL == prefix_handle ?
                     0 : prefix_handle->container_size );
        return SDF_INVALID_PARAMETER;
    }

    cursor_bucket_no =
        ( curr_cursor % total_blks ) / prefix_handle->segment_size;
    cursor_bucket = &prefix_handle->cursor_table[cursor_bucket_no];

    mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO,
                 "ENTERING curr_cursor=%lu total_blks=%lu bucket_no=%d",
                 curr_cursor, total_blks, cursor_bucket_no );

    for ( item = cursor_bucket; item != NULL; item = next_item ) {
        next_item = item->cursor_next;
        /*
         * FIXME_PREFIX: this needs to be revised when we support dynamic
         * container resizing
         */
        if ( next_item != NULL &&
             curr_cursor > next_item->start_cursor &&
             curr_cursor - next_item->start_cursor > total_blks ) {

            mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO,
                         "deleting prefix %s", ITEM_prefix(next_item) );

            /* delete the item from the prefixes table */
            if ( next_item->prefix_next != NULL ) {
                next_item->prefix_next->prefix_prev = next_item->prefix_prev;
            }
            next_item->prefix_prev->prefix_next = next_item->prefix_next;

            /* delete item from the cursor table */
            if ( next_item->cursor_next != NULL ) {
                next_item->cursor_next->cursor_prev = next_item->cursor_prev;
            }
            plat_assert( next_item->cursor_prev == item );
            next_item->cursor_prev->cursor_next = next_item->cursor_next;

            prefix_handle->num_prefixes--;
            prefix_handle->total_prefixes_size -= (next_item->prefix_len + 1);
            prefix_item_free( prefix_handle, next_item );
            next_item = item;
        }
    }

    return SDF_SUCCESS;
}


#define MCD_NULL_PFX_STR        "(_null_)"

/*
 * the caller is responsible for freeing up the buffer
 */
int mcd_prefix_list( void * handle, char ** bufp, uint64_t curr_cursor,
                     uint64_t total_blks, uint64_t curr_blks )
{
    prefix_handle_t           * prefix_handle = (prefix_handle_t *)handle;
    prefix_item_t             * item = NULL;
    char                      * buf;
    int                         buf_len;
    char                      * pos;
    int                         num_prefixes = 0;
    int                         null_padding = 0;
    uint64_t                    distance;

    if ( NULL == prefix_handle || NULL == bufp ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "invalid parameter, hdl=%p buf=%p",
                     prefix_handle, bufp );
        return -EINVAL;
    }

    if ( prefix_handle->num_prefixes == 0
         || prefix_handle->total_prefixes_size == 0 ) {
        return num_prefixes;
    }
    num_prefixes = prefix_handle->num_prefixes;

    buf_len = prefix_handle->total_prefixes_size +
        prefix_handle->num_prefixes * strlen("0.001 \r\n") +
        strlen( "\r\nEND\r\n" ) + 1;

    for ( int i = 0; i < prefix_handle->prefixes_table_size; i++ ) {
        for ( item = prefix_handle->prefixes_table[i].prefix_next;
              item != NULL; item = item->prefix_next ) {
            if ( 0 == item->prefix_len ) {
                null_padding += strlen( MCD_NULL_PFX_STR );
            }
        }
    }
    buf_len += null_padding;
    mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO, "buf_len=%d padding=%d",
                 buf_len, null_padding );

    /*
     * use malloc here since write_and_free() is responsible for freeing
     * this buffer
     */
    if ( NULL == ( buf = plat_malloc( buf_len ) ) ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "failed to allocate memory for prefix list buffer" );
        *bufp = NULL;
        return -ENOMEM;
    }
    pos = buf;
    buf_len -= strlen( "\r\nEND\r\n" ) + 1;

    for ( int i = 0; i < prefix_handle->prefixes_table_size; i++ ) {

        for ( item = prefix_handle->prefixes_table[i].prefix_next;
              item != NULL; item = item->prefix_next ) {
            if ( ( curr_cursor / total_blks ) >
                 ( item->start_cursor / total_blks ) &&
                 ( curr_cursor % total_blks ) >=
                 ( item->start_cursor % total_blks ) ) {
                distance = curr_blks;
            }
            else {
                distance = ( ( curr_cursor % total_blks ) -
                             ( item->start_cursor % total_blks ) + curr_blks )
                    % curr_blks;
            }
            plat_snprintfcat( &pos, &buf_len,
                              "%.3lf %s\r\n", (double)distance / curr_blks,
                              0 != item->prefix_len ? ITEM_prefix(item) :
                              MCD_NULL_PFX_STR );

            mcd_dbg_msg( MCD_PFX_LOG_LVL_INFO,
                         "item found, prefix=%s len=%d "
                         "curr=%lu(%lu) start=%lu(%lu) total=%lu distance=%lu",
                         ITEM_prefix(item), item->prefix_len,
                         curr_cursor, curr_cursor % total_blks,
                         item->start_cursor, item->start_cursor % total_blks,
                         total_blks, distance );
        }
    }

    if ( NULL == pos ) {
        mcd_dbg_msg( PLAT_LOG_LEVEL_ERROR,
                     "prefix_delete list output truncated" );
        pos = buf + strlen(buf);
    }
    if ( 2 > ( pos - buf ) || 0 != strncmp( pos - 2, "\r\n", 2 ) ) {
        pos += sprintf( pos, "\r\nEND\r\n" );
    }
    else {
        pos += sprintf( pos, "END\r\n" );
    }

    *bufp = buf;
    return num_prefixes;
}


int mcd_prefix_reset( void * handle )
{
    prefix_handle_t     *prefix_handle = (prefix_handle_t *)handle;
    prefix_item_t       *item = NULL;
    prefix_item_t       *next_item = NULL;

    plat_assert( prefix_handle != NULL );

    if ( prefix_handle->num_prefixes > 0 ) {

        for ( int i = 0; i < prefix_handle->cursor_table_size; i++ ) {
            for ( item = &prefix_handle->cursor_table[i];

                item != NULL; item = next_item ) {
                next_item = item->cursor_next;

                if ( next_item != NULL ) {
                    /* delete item from the cursor table */
                    if ( next_item->cursor_next != NULL ) {
                        next_item->cursor_next->cursor_prev =
                            next_item->cursor_prev;
                    }
                    plat_assert( next_item->cursor_prev == item );
                    next_item->cursor_prev->cursor_next =
                        next_item->cursor_next;

                    prefix_handle->num_prefixes--;
                    prefix_handle->total_prefixes_size -=
                        (next_item->prefix_len + 1);
                    prefix_item_free( prefix_handle, next_item );
                    next_item = item;
                }
            }
        }
    }

    plat_assert( prefix_handle->num_prefixes == 0 );
    plat_assert( prefix_handle->total_items_memory == 0 );

    return 0;
}


int mcd_prefix_cleanup( void * handle )
{
    prefix_handle_t     *prefix_handle = (prefix_handle_t *)handle;

    if ( mcd_prefix_reset( prefix_handle ) != 0 ) {
        return -1;
    }

    plat_free( prefix_handle->prefixes_table );
    plat_free( prefix_handle->cursor_table );
    plat_free( prefix_handle );

    return 0;
}


uint64_t mcd_prefix_total_alloc( void * handle )
{
    prefix_handle_t           * prefix_handle = (prefix_handle_t *)handle;

    if ( NULL == prefix_handle ) {
        return 0;
    }
    return prefix_handle->total_alloc + prefix_handle->total_items_memory;
}
