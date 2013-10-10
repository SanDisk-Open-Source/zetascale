/************************************************************************
 * 
 *  btree_raw_range.c  May 9, 2013   Brian O'Krafka
 * 
 *  NOTES: xxxzzz
 *    - persisting secondary index handles
 *    - get by seqno
 *    - return primary key in secondary key access
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <inttypes.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include "btree_hash.h"
#include "btree_raw.h"
#include "btree_map.h"
#include "btree_raw_internal.h"

//  Define this to include detailed debugging code
//#define DEBUG_STUFF


/*********************************************************
 *
 *    RANGE QUERIES
 *
 *********************************************************/


typedef enum FDF_range_enums_t {
    BUFFER_PROVIDED      = 1<<0,  // buffers for keys and data provided by application
    ALLOC_IF_TOO_SMALL   = 1<<1,  // if supplied buffers are too small, FDF will allocate
    SEQNO_LE             = 1<<5,  // only return objects with seqno <= end_seq
    SEQNO_GT_LE          = 1<<6,  // only return objects with start_seq < seqno <= end_seq

    START_GT             = 1<<7,  // keys must be >  key_start
    START_GE             = 1<<8,  // keys must be >= key_start
    START_LT             = 1<<9,  // keys must be <  key_start
    START_LE             = 1<<10, // keys must be <= key_start

    END_GT               = 1<<11, // keys must be >  key_end
    END_GE               = 1<<12, // keys must be >= key_end
    END_LT               = 1<<13, // keys must be <  key_end
    END_LE               = 1<<14, // keys must be <= key_end

    KEYS_ONLY            = 1<<15, // only return keys (data is not required)

    PRIMARY_KEY          = 1<<16, // return primary keys in secondary index query
};

typedef struct FDF_range_meta {
    uint32_t       flags;        // flags controlling type of range query (see above)
    uint32_t       keybuf_size;  // size of application provided key buffers (if applicable)
    uint64_t       databuf_size; // size of application provided data buffers (if applicable)
    char          *key_start;    // start key
    uint32_t       keylen_start; // length of start key
    char          *key_end;      // end key
    uint32_t       keylen_end;   // length of end key
    uint64_t       start_seq;    // starting sequence number (if applicable)
    uint64_t       end_seq;      // ending sequence number (if applicable)
} FDF_range_meta_t;

struct FDF_cursor;       // opaque cursor handle
uint64_t FDF_indexid_t;  // persistent opaque index handle

/* Start an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_FAILURE if unsuccessful
 */
FDF_status_t FDFGetRange(struct FDF_thread_state *thrd_state, //  client thread FDF context
                         FDF_cguid_t              cguid,      //  container
                         FDF_indexid_t            indexid,    //  handle for index to use (use PRIMARY_INDEX for primary)
                         struct FDF_cursor      **cursor,     //  returns opaque cursor for this query
                         FDF_range_meta_t        *meta);      //  query attributes (see above)

typedef struct FDF_range_data {
    FDF_status_t  status;           // status
    char         *key;              // index key value
    uint32_t      keylen;           // index key length
    char         *data;             // data
    uint64_t      datalen;          // data length
    uint64_t      seqno;            // sequence number for last update
    uint64_t      syndrome;         // syndrome for key
    char         *primary_key;      // primary key value (if required)
    uint32_t      primary_keylen;   // primary key length (if required)
} FDF_range_data_t;

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
FDF_status_t
FDFGetNextRange(struct FDF_thread_state *thrd_state,  //  client thread FDF context
                struct FDF_cursor       *cursor,      //  cursor for this indexed search
                int                      n_in,        //  size of 'values' array
                int                     *n_out,       //  number of items returned
                FDF_range_data_t        *values);     //  array of returned key/data values


/* End an index query.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_UNKNOWN_CURSOR if the cursor is invalid
 */
FDF_status_t FDFGetRangeFinish(struct FDF_thread_state *thrd_state, 
                               struct FDF_cursor *cursor);


/*********************************************************
 *
 *    SECONDARY INDEXES
 *
 *********************************************************/

/*
 *   Function used to extract secondary key
 *   from primary key and/or data.
 *   This function must use FDFGetSecondaryKeyBuffer() below
 *   to allocate a buffer for the extracted key.
 *
 *   Returns:  0 if successful
 *             1 if fails (eg: FDFGetSecondaryKeyBuffer() fails)
 */
typedef int (FDF_index_fn_t)(void     *index_data,        //  opaque user data 
                             char     *key,               //  primary key of object
                             uint32_t  keylen,            //  length of primary key
                             char     *data,              //  object data (if required, see flags)
                             uint64_t  datalen,           //  length of object data (if required, see flags)
                             char    **secondary_key,     //  returned secondary key
                             uint32_t *keylen_secondary); //  returned length of secondary key

/*
 *  Allocate a buffer in which to place an extracted
 *  secondary key.
 * 
 *  Returns NULL if a buffer cannot be allocated.
 */
char * FDFGetSecondaryKeyBuffer(uint32_t len);



/*
 *  Function used to compare secondary index values
 *  to determine ordering in the index.
 *
 *  Returns: -1 if key1 comes before key2
 *            0 if key1 is the same as key2
 *            1 if key1 comes after key2
 */
typedef int (FDF_cmp_fn_t)(void     *index_data, //  opaque user data
                           char     *key1,       //  first secondary key
                           uint32_t  keylen1,    //  length of first secondary key
                           char     *key2,       //  second secondary key
                           uint32_t  keylen1);   //  length of second secondary key

typedef enum FDF_range_enums_t {
    INDEX_USES_DATA = 1<<0,  //  Indicates that secondary index key 
                             //  is derived from object data
};

typedef struct FDF_index_meta {
    uint32_t        flags;       //  flags (see FDF_range_enums_t)
    FDF_index_fn   *index_fn;    //  function used to extract secondary key
    FDF_cmp_fn     *cmp_fn;      //  function used to compare index values
    void           *index_data;  //  opaque user data for index/cmp functions
} FDF_index_meta_t;

/*
 * Create a secondary index
 * Index creation is done synchronously: the function does
 * not return until the index is fully created.
 * Secondary index creation in crash-safe: if FDF crashes while
 * index creation is in progress, index creation will be completed
 * when FDF restarts.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if FDF runs out of memory
 *          FDF_xxxzzz if FDF runs out of storage
 */
FDF_status_t
FDFAddSecondaryIndex(struct FDF_thread_state *thrd_state,   //  client thread FDF context
                     FDF_cguid_t              cguid,        //  container in which to add index
                     FDF_index_id_t          *indexid,      //  persistent opaque handle for new index
                     FDF_index_meta_t        *meta);        //  attributes of new index

/*
 * Delete a secondary index
 * 
 * Index deletion is done synchronously: the function does
 * not return until the index is fully deleted.
 * Secondary index deletion is crash-safe: if FDF crashes while
 * index deletion is in progress, index deletion will be completed
 * when FDF restarts.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_INVALID_INDEX if index is invalid
 */
FDF_status_t
FDFDeleteSecondaryIndex(struct FDF_thread_state *thrd_state, //  client thread FDF context
                        FDF_cguid_t              cguid,      //  container in which to add index
                        FDF_indexid_t            indexid);   //  persistent opaque handle of index to delete

/*
 * Get a list of all current secondary indices.
 * Array returned in index_ids is allocated by FDF and
 * must be freed by application.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if index_ids cannot be allocated
 */
FDF_status_t
FDFGetContainerIndices(struct FDF_thread_state *ts,         //  client thread FDF context
                         FDF_cguid_t            cguid,      //  container
                         uint32_t              *n_indices,  //  returns number of indices
                         FDF_indexid_t         *index_ids); //  returns array of index ids

/*
 * Get attributes for a particular indexid.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if indexid is invalid
 */
FDF_status_t
FDFGetIndexMeta(struct FDF_thread_state *ts,       //  client thread FDF context
                FDF_cguid_t              cguid,    //  container
                FDF_indexid_t            indexid,  //  index id
                FDF_index_meta_t        *meta);    //  attributes of index

/*********************************************************
 *
 *    SNAPSHOTS
 *
 *********************************************************/

/*
 * Create a snapshot for a container.  
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_TOO_MANY_SNAPSHOTS if snapshot limit is reached
 */
FDF_status_t
FDFCreateContainerSnapshot(struct FDF_thread_state *ts, //  client thread FDF context
                           FDF_cguid_t cguid,           //  container
                           uint64_t *snap_seq);         //  returns sequence number of snapshot
 
/*
 * Delete a snapshot.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_SNAPSHOT_NOT_FOUND if no snapshot for snap_seq is found
 */
FDF_status_t
FDFDeleteContainerSnapshot(struct FDF_thread_state *ts, //  client thread FDF context
                           FDF_cguid_t cguid,           //  container
                           uint64_t snap_seq);          //  sequence number of snapshot to delete

/*
 * Get a list of all current snapshots.
 * Array returned in snap_seqs is allocated by FDF and
 * must be freed by application.
 * 
 * Returns: FDF_SUCCESS if successful
 *          FDF_xxxzzz if snap_seqs cannot be allocated
 */
FDF_status_t
FDFGetContainerSnapshots(struct FDF_thread_state *ts, //  client thread FDF context
                         FDF_cguid_t cguid,           //  container
                         uint32_t *n_snapshots,       //  returns number of snapshots
                         uint64_t *snap_seqs);        //  returns array of snapshot sequence numbers


