/** @file hash.c
 *  @brief FDF translation(key <-> addr) module.
 *
 *  This contains the functions for FDF
 *   key to addr translation and vice-versa
 *
 *  @author Tomy Cheru (tcheru)
 *  SanDisk Proprietary Material, © Copyright 20123 SanDisk, all rights reserved..
 *  http://www.sandisk.com
 */

#include "mcd_osd.h"
#include "mcd_rec.h"
#include "hash.h"

/* external variables used in here */
extern uint64_t Mcd_osd_blk_size;
extern uint64_t Mcd_osd_segment_blks;

/* external functions used in here */
extern uint64_t 
hashb(const unsigned char *key, uint64_t keyLength, uint64_t level);

extern int
mcd_onflash_key_match(void *context, mcd_osd_shard_t * shard, uint32_t addr, char *key, int key_len);

uint32_t
mcd_osd_lba_to_blk( uint32_t blocks );

/*
 * Hash the key and the container id.
 */
uint64_t
hashck(const unsigned char *key, uint64_t key_len,
       uint64_t level, cntr_id_t cntr_id)
{
    return hashb(key, key_len, level) + cntr_id * OSD_HASH_BUCKET_SIZE;
}

/*
 * Internal Functions
 */

static int
unlock_iret(fthWaitEl_t *wait, int ret);

static int
log2i(uint64_t n);

void
map_bit_set(uint64_t *map, uint32_t pos);

void
map_bit_unset(uint64_t *map, uint32_t pos);

int
map_bit_isset(uint64_t *map, uint32_t pos);

/*
 * init translation book-keeping
 * called for each physical shard
 * on sucessful init, will returns a handle.
 * on error, do graceful backout and return NULL.
 */
hash_handle_t *
hash_table_init ( uint64_t total_size, uint64_t max_nobjs, int mode)
{
    int               i = 0;
    uint64_t          curr_alloc_sz = 0;
    hash_handle_t   * hdl = NULL;

    if ( !total_size && !max_nobjs ) {
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "invalid shard size" );
        goto bad;
    }

    if ( (mode != FIFO) && (mode != SLAB) ) {
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "invalid mode" );
        goto bad;
    }

    /*
     * allocate space for handle
     */
    curr_alloc_sz = sizeof(hash_handle_t);

    hdl = (hash_handle_t *) plat_alloc( curr_alloc_sz);

    if( hdl == NULL){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate hash table handle");
        goto bad;
    }

    memset( (void *)hdl, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "hash table handle initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    /*
     * update number of maximum supported objects.
     */
    hdl->hash_size = total_size / Mcd_osd_blk_size;
    //TODO: make a neat calculation
    hdl->hash_size += hdl->hash_size/4;

    if ( 0 < max_nobjs && max_nobjs < hdl->hash_size ) {
        hdl->hash_size = ( max_nobjs + Mcd_osd_segment_blks  - 1 )
            / Mcd_osd_segment_blks  * Mcd_osd_segment_blks ;
    }

    /*
     * Calculate the number of bits of the syndrome 
     * we can piece together from a hash index.
     */
    hdl->bkti_l2_size = log2i(hdl->hash_size / OSD_HASH_BUCKET_SIZE);
    hdl->bkti_l2_mask = (1 << hdl->bkti_l2_size) - 1;

    /*
     * initialize the address lookup table
     */
    curr_alloc_sz = total_size / Mcd_osd_blk_size * sizeof(uint32_t);

    hdl->addr_table = (uint32_t *) plat_alloc_large( curr_alloc_sz);

    if ( NULL == hdl->addr_table ) {
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate address lookup table");
        goto bad;
    }

    memset( (void *) hdl->addr_table, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "address lookup table initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    /*
     * initialize the bucket lock.
     */
    hdl->lock_bktsize = OSD_HASH_LOCKBKT_MINSIZE;
    hdl->lock_buckets = (hdl->hash_size + hdl->lock_bktsize - 1) / 
                            hdl->lock_bktsize;

    while ( OSD_HASH_LOCK_BUCKETS < hdl->lock_buckets ) {
        hdl->lock_bktsize *= 2;
        hdl->lock_buckets /= 2;
    }

    while ( 32768 > hdl->lock_buckets && 
                ((hdl->lock_bktsize/2) >= Mcd_osd_bucket_size)) {
        hdl->lock_bktsize /= 2;
        hdl->lock_buckets *= 2;
    }

    curr_alloc_sz =  hdl->lock_buckets * sizeof(fthLock_t);

    hdl->bucket_locks = (fthLock_t *)plat_alloc_large( curr_alloc_sz);

    if ( NULL == hdl->bucket_locks){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate lock buckets");
        goto bad;
    }

    //init bucket locks
    for ( i = 0; i < hdl->lock_buckets; i++ ) {
        fthLockInit( &hdl->bucket_locks[i] );
    }

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "lock buckets initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    if (hdl->hash_size < (hdl->lock_bktsize * hdl->lock_buckets)){
        hdl->hash_size = hdl->lock_bktsize * hdl->lock_buckets;
    }

    /*
     * allocate the bucket table
     */
    curr_alloc_sz =  hdl->hash_size /
        OSD_HASH_BUCKET_SIZE * sizeof(uint32_t);

    hdl->hash_buckets = (uint32_t *)plat_alloc_large( curr_alloc_sz);

    if ( NULL == hdl->hash_buckets ) {
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate bucket table");
        goto bad;
    }

    memset( (void *)hdl->hash_buckets, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "bucket table initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    // bucket lock free list
    curr_alloc_sz =  hdl->lock_buckets * sizeof(uint32_t);

    hdl->bucket_locks_free_list = 
            (uint32_t *)plat_alloc_large( curr_alloc_sz);

    if ( NULL == hdl->bucket_locks_free_list){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate bucket lock free list");
        goto bad;
    }

    memset( (void *)hdl->bucket_locks_free_list, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "bucket lock free list initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    // bucket lock free map
    curr_alloc_sz =  ((hdl->lock_buckets + 64 - 1) / 64) * sizeof(uint64_t);

    hdl->bucket_locks_free_map = 
                (uint64_t *)plat_alloc_large( curr_alloc_sz);

    if ( NULL == hdl->bucket_locks_free_map){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate bucket lock free map");
        goto bad;
    }

    memset( (void *)hdl->bucket_locks_free_map, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "bucket lock free map initialized, size=%lu", curr_alloc_sz);

    hdl->total_alloc += curr_alloc_sz;

    /*
     * initialize hash table.
     */
    curr_alloc_sz =  ((hdl->hash_size / OSD_HASH_ENTRY_PER_BUCKET_ENTRY) + 
        (hdl->hash_size / OSD_HASH_BUCKET_SIZE)) * sizeof(bucket_entry_t);

    hdl->hash_table = (bucket_entry_t *)plat_alloc_large(curr_alloc_sz);

    if ( NULL == hdl->hash_table){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "failed to allocate hash table");
        goto bad;
    }

    memset( (void *) hdl->hash_table, 0, curr_alloc_sz);

    log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
            "hash table initialized" );

    hdl->total_alloc += curr_alloc_sz;

    //reset index
    hdl->hash_table_idx = 0;

    return hdl;

bad:
    hash_table_cleanup( hdl);

    return NULL;
}

/*
 * hash tables cleaup on init error or shutdown.
 */
void
hash_table_cleanup ( hash_handle_t *hdl)
{
    if (!hdl){
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_ERROR,
                "invalid translation handle");
    } else {

        //hash table
        if( hdl->hash_table ) {
            plat_free_large( hdl->hash_table );
            hdl->hash_table = NULL;
        }

        //bucket lock free map
        if( hdl->bucket_locks_free_map ) {
            plat_free_large( hdl->bucket_locks_free_map );
            hdl->bucket_locks_free_map = NULL;
        }

        //bucket lock free list
        if( hdl->bucket_locks_free_list ) {
            plat_free_large( hdl->bucket_locks_free_list );
            hdl->bucket_locks_free_list = NULL;
        }

        // lock buckets
        if( hdl->bucket_locks ) {
            plat_free_large( hdl->bucket_locks );
            hdl->bucket_locks = NULL;
        }

        // bucket table
        if( hdl->hash_buckets ) {
            plat_free_large( hdl->hash_buckets );
            hdl->hash_buckets = NULL;
        }

        // addr_table
        if( hdl->addr_table ) {
            plat_free_large( hdl->addr_table);
            hdl->addr_table = NULL;
        }

        // translation handle
        if ( hdl) {
            plat_free( hdl);
        }
    }
}

/*
 * Search given hash table for required virtual container/key.
 * on finding match, returns corresponding hash_entry
 * otherwise return NULL.
 *
 * lookup in-memory hash tables first,on syndrome match
 * verify key with on-flash meta.
 */
hash_entry_t *
hash_table_get (void *context, hash_handle_t *hdl, char *key, int key_len, cntr_id_t cntr_id)
{
    uint32_t          bucket_idx;
    uint64_t          syndrome;
    hash_entry_t    * hash_entry = NULL;
    bucket_entry_t  * bucket_entry;
    int               i;

    if ( !context || !hdl || !key || !key_len || !cntr_id) {
        log_msg ( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_FATAL,
                "translation lookup failed, invalid parameter");
        return NULL;
    }

    //calculate syndrome
    syndrome = hashck( (unsigned char *)key, key_len, 0, cntr_id);

    //lookup
    bucket_idx = *(hdl->hash_buckets +
            ( ( syndrome % hdl->hash_size ) / OSD_HASH_BUCKET_SIZE ));

    for (; bucket_idx != 0; bucket_idx = bucket_entry->next) {
        bucket_entry = hdl->hash_table + (bucket_idx - 1 );
        for (i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            hash_entry = &bucket_entry->hash_entry[i];

            if (hash_entry->used == 0) {
                continue;
            }

            if (hash_entry->cntr_id != cntr_id){
                continue;
            }

            if ( (uint16_t)(syndrome >> 48) != hash_entry->syndrome ) {
                continue;
            }

#ifdef BTREE_HACK
            if(key_len == 8){
                if(*((uint64_t*)key) != hash_entry->key) {
                    continue;
                }
            } else {
#endif
                if (mcd_onflash_key_match(context, hdl->shard,
                            hash_entry->address, key, key_len) != TRUE){
                    continue;
                }
#ifdef BTREE_HACK
            }
#endif
            return hash_entry;
        }
    }
    return NULL;
}

/*
 * copies src hash_entry to dst hash_entry, on non-NULL src
 * otherwise reset dst.
 */
void
hash_entry_copy ( hash_entry_t *dst, hash_entry_t *src)
{
    // dst has to be valid, src can be NULL.
    if(dst){
        if (!src){
            dst->used       = 0;
            dst->deleted    = 0;
            dst->referenced = 0;
            dst->reserved   = 0;
            dst->blocks     = 0;
            dst->syndrome   = 0;
            dst->address    = 0;
            dst->cntr_id    = 0;
#ifdef BTREE_HACK
            dst->key        = 0;
#endif
        } else {
            dst->used       = src->used;
            dst->deleted    = src->deleted;
            dst->referenced = src->referenced;
            dst->reserved   = src->reserved;
            dst->blocks     = src->blocks;
            dst->syndrome   = src->syndrome;
            dst->address    = src->address;
            dst->cntr_id    = src->cntr_id;
#ifdef BTREE_HACK
            dst->key        = src->key;
#endif
        }
    }
}

/*
 * deletes given hash entry.
 * will do the rearrangement if required.
 */
void
hash_entry_delete ( hash_handle_t *hdl, hash_entry_t *he, uint32_t hash_idx)
{
    int               i;
    uint32_t        * bucket_head,head_bucket_idx;
    uint32_t          delete_bucket_idx;
    bucket_entry_t  * tmphbe, *tmpdbe ;
    hash_entry_t    * tmphhe, *tmpdhe;
    uint32_t          lock_idx;
    uint32_t        * lock_free_list;

    bucket_head = (hdl->hash_buckets +
            ( hash_idx / OSD_HASH_BUCKET_SIZE ));
    head_bucket_idx = *bucket_head;

    uint32_t hbi = *bucket_head;
    bucket_entry_t *b;
    delete_bucket_idx = 0;
    for(;hbi !=0;hbi = b->next){
        b = hdl->hash_table + (hbi - 1);
        for(i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            if(he == &b->hash_entry[i]){
                delete_bucket_idx = hbi;
                goto found;
            }
        }
    }
found:
    plat_assert(hbi != 0);
    //	delete_bucket_idx = (((uint64_t)he - (uint64_t)hdl->hash_table) / sizeof(bucket_entry_t)) + 1;

    lock_idx = ( hash_idx ) / hdl->lock_bktsize;
    lock_free_list = hdl->bucket_locks_free_list + lock_idx;

    hdl->addr_table[he->address] = 0;

    if(head_bucket_idx == delete_bucket_idx){ 
        // head bucket entry, just clean hash entry
        hash_entry_copy(he, NULL);
    } else { //need to move things around
        hash_entry_copy(he, NULL);

        int j = 0;
        tmpdbe = hdl->hash_table + (delete_bucket_idx - 1);	
        tmphbe = hdl->hash_table + (head_bucket_idx - 1);
        for (i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            tmpdhe = &tmpdbe->hash_entry[i];
            if(tmpdhe->used ==  0){ //possiblity of copying one he
                for (;j<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;j++){
                    tmphhe = &tmphbe->hash_entry[j];
                    if(tmphhe->used == 1){
                        hash_entry_copy(tmpdhe, tmphhe);
                        hash_entry_copy(tmphhe, NULL);
                        hdl->addr_table[tmpdhe->address] = hash_idx;
                        break;
                    }
                }
            }
        }
    }
    /*
     * if bucket entries are empty, need 
     * to put it back to lock bucket free list
     */
    int free = 1;
    tmphbe = hdl->hash_table + (head_bucket_idx - 1);
    for (i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
        tmphhe = &tmphbe->hash_entry[i];
        if(tmphhe->used ==  1){
            free = 0;
            break;
        }
    }
    if(free){
        *bucket_head = tmphbe->next;
        tmphbe->next = *lock_free_list;
        *lock_free_list = head_bucket_idx;
        map_bit_set(hdl->bucket_locks_free_map, lock_idx);
        plat_assert(*lock_free_list != 0);
    }
}

/*
 * given a syndrome, get a free hash entry from the given hash table.
 * logic is such that only the bucket entry pointed by the 
 * appropriate bucket where the syndrome belongs have the possiblity of 
 * having free hash_entry/s.
 * lookup for free entry is in following order,
 * 1. any free hash_entry in bucket_entry pointed by bucket of the syndrome
 * 2. if lock bucket free list corresponding to syndrome have a free 
 *    bucket_entry, move it to current bucket and pop one hash_entry
 * 3. if hash table have free bucket_entry, move one to current bucket
 *    and pop one hash_entry
 * 4. grab bucket_entry from OTHER lock bucket free list
 * 5. we hit the rock bottom, check-mate
 *
 * on sucess returns a valid hash_entry
 * returns NULL (with a FATAL message) otherwise
 * 
 */
hash_entry_t *
hash_entry_insert_by_key(hash_handle_t *hdl, uint64_t syndrome)
{
    uint32_t          pop_idx, *bucket_index;
    uint32_t          lock_idx;
    uint32_t        * lock_free_list;
    bucket_entry_t  * bucket_entry, *be;
    int               i;

    bucket_index = hdl->hash_buckets +
        ( ( syndrome % hdl->hash_size ) / OSD_HASH_BUCKET_SIZE );

    lock_idx = ( syndrome % hdl->hash_size ) / hdl->lock_bktsize;

    lock_free_list = hdl->bucket_locks_free_list + lock_idx;

    // get from bucket
    if ( *bucket_index != 0){
        bucket_entry = hdl->hash_table + ((*bucket_index) - 1);
        for(i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            if (bucket_entry->hash_entry[i].used == 0){
                return &bucket_entry->hash_entry[i];
            }
        }
    }

    //get from its lock bucket free list.
    if (map_bit_isset(hdl->bucket_locks_free_map, lock_idx) == TRUE){
        //pop one from lbfl
        if((pop_idx = *lock_free_list) != 0){
            bucket_entry = hdl->hash_table + (pop_idx - 1);
            if((*lock_free_list = bucket_entry->next) == 0){
                map_bit_unset(hdl->bucket_locks_free_map, lock_idx);
            }
            // insert to bucket
            bucket_entry->next = *bucket_index;
            *bucket_index = pop_idx;

            return &bucket_entry->hash_entry[0];	
        }
    }

    // get from hash table
    if (hdl->hash_table_idx < hdl->hash_size){
        pop_idx = atomic_get_inc(hdl->hash_table_idx);
        be = hdl->hash_table + pop_idx;
        be->next = *bucket_index;
        *bucket_index = (pop_idx + 1);
        return &be->hash_entry[0];
    }

    //grab OTHER lock bucket free list
    for(i=0;i<hdl->lock_bktsize;i++){
        if(map_bit_isset(hdl->bucket_locks_free_map, i) == TRUE){
            //lock new lock
            lock_free_list = hdl->bucket_locks_free_list + i;
            if((pop_idx = *lock_free_list) != 0 ){
                bucket_entry = hdl->hash_table + (pop_idx - 1);
                if((*lock_free_list = bucket_entry->next) == 0){
                    map_bit_unset(hdl->bucket_locks_free_map, i);
                }
                // insert to bucket
                bucket_entry->next = *bucket_index;
                *bucket_index = pop_idx;
                //unlock new lock
                return &bucket_entry->hash_entry[0];
            }
        }
    }

#if 0
    // to check if there were a possiblity of finding one by this time.
    int free = 0;
    for(i=0;i<hdl->lock_bktsize;i++){
        if(map_bit_isset(hdl->bucket_locks_free_map, i) == TRUE){
            free = 1;
            break;
        }
    }
    if( free ) {
        log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_FATAL,
                "other lbfl have free entries !!");
    }
#endif

    // time to do some diskclenup or add more space.
    log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_FATAL,
            "NO MORE HASH ENTRY AVAILABLE");
    return NULL;
}

/*
 * given a block address and syndrome, find appropriate hash_entry
 */
hash_entry_t *
hash_entry_insert_by_addr(hash_handle_t *hdl, uint32_t addr, uint64_t syndrome)
{
    uint32_t          bucket_idx;
    hash_entry_t    * hash_entry = NULL;
    bucket_entry_t  * bucket;
    int               i;

    bucket_idx = *(hdl->hash_buckets +
            ( ( syndrome % hdl->hash_size ) / OSD_HASH_BUCKET_SIZE ));

    for(;bucket_idx != 0;bucket_idx = bucket->next){
        bucket = hdl->hash_table + (bucket_idx - 1);
        for(i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            hash_entry = &bucket->hash_entry[i];

            if(hash_entry->used == 0){
                continue;
            }

            if ( addr == hash_entry->address ) {
                log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_DEBUG,
                        "reclaiming item: syndrome=%lx "
                        "syn=%x addr=%u blocks=%u",
                        syndrome, hash_entry->syndrome,
                        hash_entry->address, hash_entry->blocks );
                return hash_entry;
            }
        }
    }
    return NULL;
}

fthLock_t *
hash_table_find_lock ( hash_handle_t *hdl, uint64_t hint, int mode )
{
    if ( mode == SYN ){
        return hdl->bucket_locks +
            ( hint % hdl->hash_size ) / hdl->lock_bktsize;
    } else if (mode == ADDR) {
        return hdl->bucket_locks + (hdl->addr_table[(uint32_t)hint] / 
                    hdl->lock_bktsize);
    } else {
        return NULL;
    }
}

/*
 * find appropriate hash_entry
 * used in recovery
 * on success, return hash_entry
 * return NULL, with a FATAL message
 */
hash_entry_t *
hash_entry_recovery_insert(hash_handle_t *hdl, mcd_rec_flash_object_t *obj,
                uint64_t blk_offset)
{
    int               i;
    uint32_t          pop_idx;
    uint32_t        * bucket_head;
    hash_entry_t    * hash_entry = NULL;
    bucket_entry_t	* bucket_entry;
    bucket_entry_t  * be;

    // find the right hash entry to use
    bucket_head = hdl->hash_buckets +
        ( obj->bucket / OSD_HASH_BUCKET_SIZE );
    if( *bucket_head != 0 ){
        bucket_entry = hdl->hash_table + (*bucket_head - 1);
        for(i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            if (bucket_entry->hash_entry[i].used == 0){
                hash_entry =  &bucket_entry->hash_entry[i];
                goto found;
            }
        }
    }

    if (hdl->hash_table_idx < 
            (hdl->hash_size/OSD_HASH_ENTRY_PER_BUCKET_ENTRY) + 
            (hdl->hash_size / OSD_HASH_BUCKET_SIZE)){
        pop_idx = atomic_get_inc(hdl->hash_table_idx);
        be = hdl->hash_table + pop_idx;
        be->next = *bucket_head;
        *bucket_head = pop_idx + 1;
        hash_entry = &be->hash_entry[0];

    } else {
        // hard overflow for store mode shard, should not happen
        log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_FATAL,
                "recovery overflow for store mode shard!" );
        plat_abort();
    }
found:

    plat_assert( hash_entry->used == 0 );

    // update hash entry
    hash_entry->used       = 1;
    hash_entry->referenced = 1;
    hash_entry->deleted    = obj->deleted;
    hash_entry->blocks     = obj->blocks;
    hash_entry->syndrome   = obj->syndrome;
    hash_entry->address    = blk_offset;
    hash_entry->cntr_id    = obj->cntr_id;
#ifdef BTREE_HACK
    hash_entry->key        = 0;
#endif

    log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_TRACE,
            "<<<< upd_HT: syn=%u, blocks=%u, del=%u, bucket=%u, "
            "addr=%lu",
            obj->syndrome, mcd_osd_lba_to_blk( obj->blocks ),
            obj->deleted, obj->bucket, blk_offset );

    plat_assert( blk_offset / Mcd_osd_segment_blks <
            hdl->shard->total_segments );

    // update addr table entry
    hdl->addr_table[ blk_offset ] = obj->bucket;

    return hash_entry;
}	

typedef unsigned char uchar_t;
typedef fthWaitEl_t wait_t;

/*
 * validate existance of an object
 * return 1 for valid object
 *        0 otherwise
 */
int
obj_valid( hash_handle_t *hdl, mcd_osd_meta_t *meta, uint32_t addr)
{
    int               i;
    uchar_t         * key = (void *) &meta[1];
    uint64_t          syndrome = hashck(key, meta->key_len, 0, meta->cguid);
    hashsyn_t         hashsyn = syndrome >> OSD_HASH_SYN_SHIFT;
    uint64_t          hi = syndrome % hdl->hash_size;
    uint32_t          bucket_idx = *(hdl->hash_buckets + 
                                    ( hi / OSD_HASH_BUCKET_SIZE));
    fthLock_t       * lock = &hdl->bucket_locks[hi / hdl->lock_bktsize];
    wait_t          * wait = fthLock(lock, 0, NULL);
    bucket_entry_t	* bucket_entry;
    hash_entry_t    * hash_entry;

    for(;bucket_idx != 0;bucket_idx = bucket_entry->next){
        bucket_entry = hdl->hash_table + (bucket_idx - 1);
        for(i=0;i<OSD_HASH_ENTRY_PER_BUCKET_ENTRY;i++){
            hash_entry = &bucket_entry->hash_entry[i];

            if (hash_entry->used == 0)
                continue;

            if (hash_entry->syndrome != hashsyn)
                continue;

            if (hash_entry->address != addr)
                continue;

            return unlock_iret(wait, 1);	
        }
    }
    return unlock_iret(wait, 0);
}
		
static int
log2i(uint64_t n)
{
    int l = 0;

    n >>= 1;
    while (n) {
        l++;
        n >>= 1;
    }
    return l;
}

void
map_bit_set(uint64_t *map, uint32_t pos)
{
    uint64_t *word = &map[pos / 64];
    uint64_t mask = 1 << (pos % 64);
    *word = *word | mask;
}

void
map_bit_unset(uint64_t *map, uint32_t pos)
{
    uint64_t *word = &map[pos / 64];
    uint64_t mask = ~(1 << (pos % 64));
    *word = *word & mask;
}

int
map_bit_isset(uint64_t *map, uint32_t pos)
{
    uint64_t *word = &map[pos / 64];
    uint64_t mask = 1 << (pos % 64);
    if ( *word & mask) {
        return TRUE;
    } else {
        return FALSE;
    }
}

/*
 * Unlock a fth lock and return an integer.
 */
static int
unlock_iret(wait_t *wait, int ret)
{
    fthUnlock(wait);
    return ret;
}
