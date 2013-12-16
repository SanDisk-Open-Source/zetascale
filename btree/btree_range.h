/************************************************************************
 * 
 *  btree_range.h  May 9, 2013   Brian O'Krafka
 * 
 *  NOTES: xxxzzz
 *    - persisting secondary index handles
 *    - get by seqno
 *    - return primary key in secondary key access
 * 
 ************************************************************************/

#ifndef __BTREE_RANGE_H
#define __BTREE_RANGE_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <inttypes.h>
#include "btree_raw.h"
#include "btree_internal.h"

/*********************************************************
 *
 *    RANGE QUERIES
 *
 *********************************************************/

typedef uint64_t btree_indexid_t;          // persistent opaque index handle

typedef enum btree_range_status {
	BTREE_RANGE_STATUS_NONE     = 0,
	BTREE_RANGE_SUCCESS         = 1,   // Kept as 1 to align with FDF for now
	BTREE_KEY_BUFFER_TOO_SMALL  = 2,
	BTREE_DATA_BUFFER_TOO_SMALL = 4,
	BTREE_RANGE_PAUSED          = 8
} btree_range_status_t;

typedef enum {
	RANGE_BUFFER_PROVIDED      = 1<<0,  // buffers for keys and data provided by application
	RANGE_ALLOC_IF_TOO_SMALL   = 1<<1,  // if supplied buffers are too small, FDF will allocate

	RANGE_SEQNO_EQ             = 1<<4,  // only return objects = seqno
	RANGE_SEQNO_LE             = 1<<5,  // only return objects with seqno <= end_seq
	RANGE_SEQNO_GT_LE          = 1<<6,  // only return objects with start_seq < seqno <= end_seq

	RANGE_START_GT             = 1<<7,  // keys must be >  key_start
	RANGE_START_GE             = 1<<8,  // keys must be >= key_start
	RANGE_START_LT             = 1<<9,  // keys must be <  key_start
	RANGE_START_LE             = 1<<10, // keys must be <= key_start

	RANGE_END_GT               = 1<<11, // keys must be >  key_end
	RANGE_END_GE               = 1<<12, // keys must be >= key_end
	RANGE_END_LT               = 1<<13, // keys must be <  key_end
	RANGE_END_LE               = 1<<14, // keys must be <= key_end

	RANGE_KEYS_ONLY            = 1<<15, // only return keys (data is not required)

	RANGE_PRIMARY_KEY          = 1<<16, // return primary keys in secondary index query

	RANGE_INPLACE_POINTERS     = 1<<18, // Return inplace pointers to key and data in the node
} btree_range_flags_t;

#define BTREE_RANGE_PRIMARY_INDEX   0

typedef int (btree_allowed_fn_t)(void *context_data,  // context data (opaque)
                                 char *key,           // key to check if allowed
                                 uint32_t len);       // length of the key

typedef struct btree_range_meta {
	btree_range_flags_t  flags;        // flags controlling type of range query (see above)
	uint32_t             keybuf_size;  // size of application provided key buffers (if applicable)
	uint64_t             databuf_size; // size of application provided data buffers (if applicable)
	char                *key_start;    // start key
	uint32_t             keylen_start; // length of start key
	char                *key_end;      // end key
	uint32_t             keylen_end;   // length of end key
	uint64_t             start_seq;    // starting sequence number (if applicable)
	uint64_t             end_seq;      // ending sequence number (if applicable)
	cmp_cb_t            *class_cmp_fn; // Fn to cmp two keys are in same equivalence class
	btree_allowed_fn_t  *allowed_fn;   // Fn to check if this key is allowed to put in range result
	void                *cb_data;      // Any data to be passed for this function
} btree_range_meta_t;

typedef struct {
	struct btree_raw              *btree;       // BTree we are operating on. We might need this for joins??
	btree_range_meta_t   query_meta;  // Metadata for this current search
	struct btree_raw_mem_node *node;
	int16_t cur_idx;
	int16_t end_idx;
	int16_t start_idx;
	char dir;
} btree_range_cursor_t;

/* Start an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_FAILURE if unsuccessful
 */
btree_status_t
btree_range_query_start(btree_t                 *btree,    //  Btree to query for
                        btree_indexid_t         indexid,   //  handle for index to use (use PRIMARY_INDEX for primary)
                        btree_range_cursor_t    **cursor,  //  returns opaque cursor for this query
                        btree_range_meta_t      *meta);

typedef struct btree_range_data {
	int           status;           // status
	char         *key;              // index key value
	uint32_t      keylen;           // index key length
	char         *data;             // data
	uint64_t      datalen;          // data length
	uint64_t      seqno;            // sequence number for last update
	uint64_t      syndrome;         // syndrome for key
	char         *primary_key;      // primary key value (if required)
	uint32_t      primary_keylen;   // primary key length (if required)
} btree_range_data_t;

/* Gets next n_in keys in the indexed query.
 *
 * The 'values' array must be allocated by the application, and must hold up to
 * 'n_in' entries.
 * If the BUFFER_PROVIDED flag is set, the key and data fields in 'values' must be
 * pre-populated with buffers provided by the application (with sizes that were
 * specified in 'meta' when the index query was started).  If the application provided
 * buffer is too small for returned item 'i', the status for that item will 
 * be FDF_BUFFER_TOO_SMALL; if the ALLOC_IF_TOO_SMALL flag is set, FDF will allocate
 * a new buffer whenever the provided buffer is too small.
 * 
 * If the SEQNO_LE flag is set, only items whose sequence number is less than or
 * equal to 'end_seq' from FDF_range_meta_t above are returned.
 * If there are multiple versions of an item that satisfy the inequality,
 * always return the most recent version.
 *
 * If the SEQNO_GT_LE flag is set, only items for which start_seq < item_seqno <= end_seq
 * are returned.  If there are multiple versions of an item that satisfy the inequality,
 * always return the most recent version.
 * 
 * SEQNO_LE and SEQNO_GT_LE are mutually exclusive and must not be set together.
 * If neither SEQNO_LE or SEQNO_GT_LE are set the most recent version of each key
 * is returned.
 * 
 * 
 * Returns: FDF_SUCCESS    if all statuses are successful
 *          FDF_QUERY_DONE if query is done (n_out will be set to 0)
 *          FDF_FAILURE    if one or more of the key fetches fails (see statuses for the
 *                         status of each fetched object)
 * 
 * statuses[i] returns: FDF_SUCCESS if the i'th data item was retrieved successfully
 *                      FDF_BUFFER_TOO_SMALL  if the i'th buffer was too small to retrieve the object
 */
btree_status_t
btree_range_get_next(btree_range_cursor_t *cursor,   //  cursor for this indexed search
                     int                   n_in,     //  size of 'values' array
                     int                  *n_out,    //  number of items returned
                     btree_range_data_t   *values);  //  array of returned key/data values

/* End an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_UNKNOWN_CURSOR if the cursor is invalid
 */
btree_status_t
btree_range_query_end(btree_range_cursor_t *cursor);

#endif
