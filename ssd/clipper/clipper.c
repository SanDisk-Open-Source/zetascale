/*
 * File:   clipper.c
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: clipper.c 308 2008-02-20 22:34:58Z briano $
 */

#define _CLIPPER_C

#ifdef notdef
/*================================================================*/

   NOTES:
      - There are 3 indexing systems used in clipper:

          - flash indexing: page numbers from 0 to last page on flash.
	  - shard indexing: page numbers within a shard, which covers
	    a contiguous set of pages within the full set of flash pages.
	  - slab indexing: indexing relative to a slab.  A shard
	    is divided into slabs, with each slab maintaining a
	    separate list of free pages, set of bins for free pages,
	    and an lru list.  Indexing within a slab uses a reduced
	    sized unsigned integer (N_CLIPPER_SLABSIZE_BITS), 
	    with the largest integer reserved for
	    use as a "null_index".

      - You must careful to use the right indexing system when
        accessing various data structures:

	    - All ClipperEntry_t fields use slab indexes.
	    - pc->entries[] uses shard indexing.
	    - flash indexing is used when reading/writing flash pages

      - Conventions:

            - Variables starting with "ice" are shard indexes.
	      (Index to shard Clipper Entry).
	    - Variables starting with "lice" are slab indexes.
	      (Local slab Index to Clipper Entry)

/*================================================================*/
#endif

/*================================================================*/
    /*  Define this to use the local clipper aio functions (for debug)
     *  instead of those in ssd_aio.c.
     */
// #define CLIPPER_AIO
/*================================================================*/

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "flash/flash.h"
#include "ssd/ssd_local.h"
#include "ssd/ssd_aio.h"
#include "ssd/ssd_aio_local.h"
#include "clipper_aio.h"
#include "clipper.h"
#include "protocol/action/recovery.h"
#include "common/sdftypes.h"

/*================================================================*/
   /*   Define this to enable multiple fragments per object.
    *   If not defined, all objects must be contiguous sets of
    *   pages.
    */
// #define CLIPPER_ENABLE_FRAGMENTS
/*================================================================*/

/*================================================================*/
   /*   Define this to enable access logging.
    */
// #define DEBUG_CLIPPER
/*================================================================*/

/*================================================================*/
   /*   Define this to enable detailed checking
    *   that size statistics are accurate after
    *   each access
    */
// #define CHECK_SIZE

#ifdef CHECK_SIZE
static void check_size(Clipper_t *pc);
#endif // CHECK_SIZE

/*================================================================*/
   /*   Define this to enable detailed consistency
    *   checking of all data structures on each access.
    */
// #define CHECK_STRUCTURES

   /*   Set this to 1 to enable detailed cycle 
    *   checking for all lists.
    */

#ifdef CHECK_STRUCTURES
static int CheckCycles = 1; 
static void check_structures(Clipper_t *pc, uint32_t nbucket, int check_cycles);
#endif
/*================================================================*/

/*================================================================*/
   /*   Define this to enable tracing of all locks.
    *   The lock trace can be checked with a separate
    *   program to detect mismatched lock/unlock sequences.
    */
// #define CHECK_LOCKS

#ifdef CHECK_LOCKS
static void lock_trace(char *fmt, ...);
#endif
/*================================================================*/

//  for stats collection
#define incr(x) __sync_fetch_and_add(&(x), 1)

/*================================================================*/
   /*  Predeclarations */

static void init_mask(MASKDATA *pm, int nbits, int offset);
static uint32_t calc_npages(Clipper_t *pc, uint32_t databytes, uint16_t keybytes);
static int get_pow2(uint32_t npages);
static int get_object_entries(Clipper_t *pc, ClipperSlab_t *pslab, uint64_t syndrome, uint64_t nbucket, struct objMetaData *metaData, char *key, ClipperEntry_t **ppce, uint64_t *pice);
static int free_object_entries(Clipper_t *pc, ClipperSlab_t *pslab, struct objMetaData *metaData, char *key, uint64_t ice, int put_on_freelist, uint64_t bytes_needed, uint64_t pages_needed);
static int get_meta(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, char *pbuf, ClipperFlashMeta_t **ppmeta, uint64_t ice, uint32_t npages);
static int invalidate_meta(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, uint64_t ice, ClipperFlashMeta_t *pmeta, uint32_t npages);
static int put_object(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, char *pbuf, uint64_t ice, uint32_t npages_new, uint32_t npages_actual, struct objMetaData *metaData, char *key);
static int get_object(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, 
                      Clipper_t *pc, char **ppbuf, struct objMetaData *metaData,
		      uint64_t ice, uint32_t npages, char *key, int *pkey_match);

   /*  End of Predeclarations */
/*================================================================*/

    /*  Initialize the Clipper_t structure corresponding to one
     *  shard.
     */
static uint64_t clipperInit(Clipper_t *pc, shard_t *pshard, uint64_t size, uint64_t nslabs)
{
    uint64_t          i, j;
    ClipperBucket_t  *pbkt;
    ClipperSlab_t    *pslab;
    ClipperEntry_t   *pce;
    uint64_t          adjusted_size;

    plat_assert(N_CLIPPER_BUCKET_BITS >= N_CLIPPER_SLABSIZE_BITS);

    pc->pagesize = CLIPPER_PAGE_SIZE;
    pc->pshard = pshard;
    pc->flash_offset_index = (pshard->flash_offset/pc->pagesize);
    if (pshard->flash_offset % pc->pagesize) {
	plat_log_msg(21659, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "flash_offset is not an even multiple of the page size!");
	plat_abort();
    }

    pc->null_index = CLIPPER_NULL_INDEX;

    InitLock(pc->enum_lock);
    pc->enum_bucket = 0;

    pc->n_syndrome_bits = N_CLIPPER_SYNDROME_BITS;

    pc->npages = size/pc->pagesize;
    if (pc->npages < nslabs*CLIPPER_MIN_PAGES_PER_SLAB) {
        nslabs = pc->npages/CLIPPER_MIN_PAGES_PER_SLAB;
	if (nslabs < 1) {
	    nslabs = 1;
	}
    }

    pc->nslabs = nslabs;
    pc->pages_per_slab = pc->npages/nslabs;
    if (pc->pages_per_slab > CLIPPER_MAX_PAGES_PER_SLAB) {
	plat_log_msg(21660, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_WARN,
		     "pages per slab (%"PRIu64") is too large--forcing to %"PRIu64"!", 
		     pc->pages_per_slab, (uint64_t) CLIPPER_MAX_PAGES_PER_SLAB);
	pc->pages_per_slab = CLIPPER_MAX_PAGES_PER_SLAB;
	pc->nslabs = pc->npages/pc->pages_per_slab;
    }

    pc->npages = pc->pages_per_slab * pc->nslabs;
    adjusted_size = pc->npages * pc->pagesize;
    if (adjusted_size > size) {
	plat_log_msg(21661, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "adjusted_size > required_size!");
	plat_abort();
    }
    pc->slabsize = pc->pages_per_slab * pc->pagesize;

    pc->nbuckets = pc->npages/CLIPPER_BUCKET_RATIO;
    if (pc->npages % CLIPPER_BUCKET_RATIO) {
        (pc->nbuckets)++;
    }
    if (pc->nbuckets % pc->nslabs) {
        pc->nbuckets = (pc->nbuckets/pc->nslabs) * pc->nslabs;
    }
    pc->buckets_per_slab = pc->nbuckets/pc->nslabs;
    if (pc->nbuckets % pc->nslabs) {
	plat_log_msg(21662, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Problem computing buckets_per_slab");
	plat_abort();
    }

    /* round n_bucket_bits to the least power of 2 greater than or equal to it */
    for (i=0; i<64; i++) {
        if ((1<<i) >= pc->nbuckets) {
	    break;
	}
    }
    if (i >= 64) {
	plat_log_msg(21663, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "n_bucket_bits is too large!");
	plat_abort();
    }
    if ((1<<i) != pc->nbuckets) {
	plat_log_msg(21664, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_WARN,
		     "Number of buckets is not a power of 2.");
    }
    pc->n_bucket_bits = i;

    if (pc->n_bucket_bits > N_CLIPPER_BUCKET_BITS) {
	plat_log_msg(21665, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "n_bucket_bits is too large (%d is max)!", N_CLIPPER_BUCKET_BITS);
	plat_abort();
    }

    init_mask(&(pc->syndrome_mask), 
              pc->n_syndrome_bits, 
	      pc->n_bucket_bits);
    init_mask(&(pc->bucket_mask),   
              pc->n_bucket_bits,           
	      0);

    pc->buckets = plat_alloc(pc->nbuckets*sizeof(ClipperBucket_t));
    if (pc->buckets== NULL) {
	plat_log_msg(21666, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "plat_alloc failed in clipperInit for bucket allocation!");
	plat_abort();
    }
    memset(pc->buckets, 0, pc->nbuckets*sizeof(ClipperBucket_t));

    pc->slabs = plat_alloc(pc->nslabs*sizeof(ClipperSlab_t));
    if (pc->slabs== NULL) {
	plat_log_msg(21667, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "plat_alloc failed in clipperInit for slab allocation!");
	plat_abort();
    }
    memset(pc->slabs, 0, pc->nslabs*sizeof(ClipperSlab_t));

    pc->entries = plat_alloc(pc->npages*sizeof(ClipperEntry_t));
    if (pc->entries == NULL) {
	plat_log_msg(21668, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "plat_alloc failed in clipperInit for entry allocation!");
	plat_abort();
    }
    memset(pc->entries, 0, pc->npages*sizeof(ClipperEntry_t));

    /*  Power-of-2 free lists are initialized empty.
     *  All free pages are kept in an unused pages list
     *  from which they are initially allocated
     *  (analagous to "sbrk").
     */

    for (i=0; i<pc->npages; i++) {
        pce = &(pc->entries[i]);
	pce->free_header.flags         = (1<<CL_FLAGS_FREE);
	pce->free_header.n_contig      = 0;
	pce->free_header.bin_next      = pc->null_index;
	pce->free_header.free_next     = pc->null_index;
	pce->free_header.free_prev     = pc->null_index;
    }

    for (i=0; i<pc->nslabs; i++) {
        pslab = &(pc->slabs[i]);
	pslab->nslab = i;
	pslab->offset = i*pc->pages_per_slab;
	InitLock(pslab->lock);
	pslab->lru_head = pc->null_index;
	pslab->lru_tail = pc->null_index;
	for (j=0; j<=N_CLIPPER_SLABSIZE_BITS; j++) {
	    pslab->free_bins[j] = pc->null_index;
	}
	pslab->used_pages     = 0;
	pslab->nobjects       = 0;
	pslab->pages_needed   = 0;
	pslab->pages_in_use   = 0;
	pslab->bytes_needed   = 0;
    }

    for (i=0; i<pc->nbuckets; i++) {
        pbkt = &(pc->buckets[i]);
	pbkt->list_head = pc->null_index;
    }

    /*   Tell the world what we did.
     */

    plat_log_msg(21669, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		 "pagesize=%d, npages=%"PRIu64", pages/slab=%"PRIu64", nslabs=%"PRIu64", slabsize=%"PRIu64", nbuckets=%"PRIu64"", 
		 pc->pagesize, pc->npages, pc->pages_per_slab, pc->nslabs, pc->slabsize, pc->nbuckets);
    plat_log_msg(21670, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		 "n_syndrome_bits=%d, n_bucket_bits=%d",
		 pc->n_syndrome_bits, pc->n_bucket_bits);
    plat_log_msg(21671, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		 "requested ssd size = %"PRIu64", adjusted ssd size = %"PRIu64"",
		 size, adjusted_size);

    if (pc->pshard->flags & FLASH_SHARD_INIT_EVICTION_STORE) {
	plat_log_msg(21672, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "Shard configured as DATA STORE.");
    } else {
	plat_log_msg(21673, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "Shard configured for EVICTION.");
    }

    return(adjusted_size);
}


// xxxzzz extract common parts of flashOpen and shardCreate

    /*  Open a flash device (but don't create any shards yet).
     *
     */
struct flashDev *clipper_flashOpen(char *devName, flash_settings_t *flash_settings, int flags) 
{
    int                i, rc;
    struct flashDev   *pdev;

    pdev = plat_alloc(sizeof(struct flashDev));
    if (pdev == NULL) {
	plat_log_msg(21674, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "plat_alloc failed in flashOpen!");
        return(NULL);
    }
    for (i=0; i<FTH_MAX_SCHEDS; i++) {
	pdev->stats[i].flashOpCount = 0;
	pdev->stats[i].flashReadOpCount = 0;
	pdev->stats[i].flashBytesTransferred = 0;
    }
    pdev->shardList = NULL;
    InitLock(pdev->lock);

    // Initialize the aio subsystem

    #ifdef CLIPPER_AIO
	pdev->paio_state = (struct ssdaio_state *) plat_alloc(sizeof(struct clipper_aio_state));
    #else
	pdev->paio_state = plat_alloc(sizeof(struct ssdaio_state));
    #endif
    if (pdev->paio_state == NULL) {
	plat_log_msg(21674, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "plat_alloc failed in flashOpen!");
        return(NULL);
    }
    // devName = "/tmp/ssd_emulation_file0"; // xxxzzz this is temporary
    devName = "/tmp/clipper_briano"; // xxxzzz this is temporary

    #ifdef CLIPPER_AIO
	rc = clipper_aio_init((struct clipper_aio_state *) pdev->paio_state, devName);
    #else
	rc = ssdaio_init(pdev->paio_state, devName);
    #endif
    if (rc) {
	plat_log_msg(21675, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Failure initializing aio subsystem: devName='%s', rc=%d", devName, rc);
	return(NULL);
    }

    pdev->size = pdev->paio_state->size;
    pdev->used = 0;
    return(pdev);
}

    /*  Create a shard on an already opened flash device.
     *
     */
struct shard *clipper_shardCreate(struct flashDev *dev, uint64_t shardID, int flags, uint64_t quota, unsigned maxObjs) 
{
    int           i;
    struct shard *ps;
    uint64_t      adjusted_size;
    WaitType      w;
    Clipper_t    *pc;
    uint32_t      n_default_slabs;

    n_default_slabs = CLIPPER_DEFAULT_NSLABS;

    Lock(dev->lock, w);

    if ((dev->size - dev->used) < quota) {
	plat_log_msg(21676, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "Insufficient space on flash in shardCreate!  Reducing quota from %"PRIu64" to %"PRIu64"", quota, dev->size - dev->used);
	quota = dev->size - dev->used;
	// quota = 16*1024; // xxxzzz this is temporary!
	// n_default_slabs = 2;      // xxxzzz this is temporary too!
    }

    ps = plat_alloc(sizeof(struct shard));
    if (ps == NULL) {
	plat_log_msg(21677, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "plat_alloc failed in shardCreate!");
        return(NULL);
    }
    ps->dev               = dev;
    ps->flash_offset      = dev->used;
    ps->shardID           = shardID;
    ps->flags             = flags;
    ps->quota             = quota;
    ps->s_maxObjs           = maxObjs; // xxxzzz this isn't used for anything
    ps->usedSpace         = 0;
    ps->numObjects        = 0;
    ps->numDeadObjects    = 0;
    ps->numCreatedObjects = 0;

    pc = plat_alloc(sizeof(Clipper_t));
    if (pc == NULL) {
	plat_log_msg(21677, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "plat_alloc failed in shardCreate!");
        return(NULL);
    }

    adjusted_size = clipperInit(pc, ps, quota, n_default_slabs);
    dev->used += adjusted_size;

    for (i=0; i<FTH_MAX_SCHEDS; i++) {
	ps->stats[i].numEvictions = 0;
	ps->stats[i].numGetOps = 0;
	ps->stats[i].numPutOps = 0;
	ps->stats[i].numDeleteOps = 0;
    }

    ps->next = dev->shardList;
    dev->shardList = ps;

    ps->scheme.pclipper = pc;

    Unlock(dev->lock, w);

    return(ps);
}



    /*  Replacement for Jim's original flashGet.
     *
     *  If dataPtr==NULL, just do an existence check.
     */
int clipper_flashGet(ssdaio_ctxt_t *pctxt, struct shard *shard, 
     struct objMetaData *metaData, char *key, char **dataPtr, int flags)
{
    int                   rc, user_buffer, key_match;
    uint64_t              h;
    ClipperEntry_t       *pce, *pce_cont;
    ClipperSlab_t        *pslab;
    uint32_t              syndrome;
    uint32_t              nbucket;
    uint32_t              nslab;
    uint32_t              npages;
    uint64_t              ice, ice_cont;
    uint32_t              lice, lice_cont;
    uint64_t              offset;
    Clipper_t            *pc;
    WaitType              lock_wait;

    pc = shard->scheme.pclipper;

    incr(shard->stats[curSchedNum].numGetOps);

    /* flags don't affect anything for ClipperGet */

    #ifdef CHECK_SIZE
	check_size(pc);
    #endif

    h = hashck((const unsigned char *) key,
               metaData->keyLen, 0, metaData->cguid);

    syndrome = MASK(h, pc->syndrome_mask);
    nbucket  = (MASK(h, pc->bucket_mask)) % pc->nbuckets;

    nslab = nbucket / pc->buckets_per_slab;
    pslab = &(pc->slabs[nslab]);
    offset = pslab->offset;

    Lock(pslab->lock, lock_wait);

    /* check bucket list for matching syndrome */
    for (lice = pc->buckets[nbucket].list_head, ice = lice + offset; 
         lice != pc->null_index; 
	 lice = pc->entries[ice].used_header.next, ice = lice + offset) 
    {
        pce = &(pc->entries[ice]);
	plat_assert(!(pce->used_header.flags & (1<<CL_FLAGS_FREE)));
	plat_assert(pce->used_header.flags & (1<<CL_FLAGS_HEADER));

	if (pce->used_header.syndrome == syndrome) {
	    
	    #ifndef CLIPPER_ENABLE_FRAGMENTS
		/* compute size */
		if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
		    npages = pc->entries[ice+1].used_extension.n_used;
		} else {
		    npages = 1;
		}

		if (npages > 1) {

		    /* check that it is contiguous (xxxzzz this is temporary) */

		    plat_assert(pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED));
		    plat_assert(pce->used_header.bucket_or_cont != pc->null_index);
		    lice_cont = pce->used_header.bucket_or_cont;
		    ice_cont  = lice_cont + offset;
		    pce_cont = &(pc->entries[ice_cont]);
		    if ((pce_cont->used_extension.flags & (1<<CL_FLAGS_IS_EXTENDED)) ||
			(ice_cont != (ice + 1)))
		    {
			plat_log_msg(21678, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
				     "object is not entirely contiguous!");
			plat_abort();
		    }
		}
	    #endif // !CLIPPER_ENABLE_FRAGMENTS

	    /* Remember if this is a user-provided buffer */
	    user_buffer = 0;
	    if ((dataPtr != NULL) && (*dataPtr)) {
	        user_buffer = 1;
	    }

            // xxxzzz continue from here

	    // xxxzzz for flashGet with dataPtr==NULL, just get_meta, not entire object

	    rc = get_object(shard->dev, pctxt, pc, dataPtr, metaData, ice, npages, key, &key_match);
            if (rc) {
		#ifdef CHECK_STRUCTURES
		    check_structures(pc, nbucket, CheckCycles);
		#endif
		Unlock(pslab->lock, lock_wait);
		return(rc);
	    }

	    if (!key_match) {
	        /*  The key does not match, so I must free the buffer
		 *  if it was allocated in get_object.
		 */
	        if ((!user_buffer) && (dataPtr != NULL)) {
		    plat_free(*dataPtr);
		}

		/* fall through on purpose */
		continue;

	    } else {
		/* xxxzzz should LRU be updated on an existence check? */

		/* update LRU */

		if (pce->used_header.lru_prev != pc->null_index) {
		    /* I am NOT already at the head of the LRU list */
		    if (pslab->lru_head != pc->null_index) {
			pc->entries[pslab->lru_head + offset].used_header.lru_prev = lice;
		    }
		    pc->entries[pce->used_header.lru_prev + offset].used_header.lru_next = pce->used_header.lru_next;
		    if (pce->used_header.lru_next != pc->null_index) {
		        pc->entries[pce->used_header.lru_next + offset].used_header.lru_prev = pce->used_header.lru_prev;
		    } else {
		        /* I am the tail of the LRU list */
		        pslab->lru_tail = pce->used_header.lru_prev;
		    }
		    pce->used_header.lru_prev = pc->null_index;
		    pce->used_header.lru_next = pslab->lru_head;
		    pslab->lru_head = lice;
		}

		#ifdef CHECK_STRUCTURES
		    check_structures(pc, nbucket, CheckCycles);
		#endif
		Unlock(pslab->lock, lock_wait);
		return(0);
	    }
	}
    }

    #ifdef CHECK_STRUCTURES
	check_structures(pc, nbucket, CheckCycles);
    #endif

    Unlock(pslab->lock, lock_wait);
    return(FLASH_ENOENT);
}

    /*  Replacement for Jim's original flashPut.
     *
     *  If data == NULL, just do a delete.
     *  If flags&FLASH_PUT_TEST_EXIST: fail with EEXIST if key exists
     *  If flags&FLASH_PUT_TEST_NONEXIST: fail with ENOENT if key does NOT exist
     */

int clipper_flashPut(ssdaio_ctxt_t *pctxt, struct shard *shard, struct objMetaData *metaData,
     char *key, char *data, int flags)
{
    int                   rc;
    uint64_t              h;
    ClipperEntry_t       *pce;
    ClipperSlab_t        *pslab;
    uint64_t              syndrome;
    uint64_t              nbucket;
    uint32_t              nslab;
    uint32_t              npages_new;
    uint32_t              npages_actual;
    uint64_t              ice;
    uint32_t              lice;
    uint64_t              offset;
    ClipperFlashMeta_t   *pmeta;
    Clipper_t            *pc;
    WaitType              lock_wait;
    uint64_t              x;
    char                 *pbuf_aligned;

    // xxxzzz beware of stack overflows because of this
    char                  buf[2*CLIPPER_PAGE_SIZE];

    x = (uint64_t) buf + SSD_AIO_ALIGNMENT;
    // x &= 0xfffffffffffffe00ULL; // 512B alignment
    x &= 0xfffffffffffff000ULL; // 4096B alignment
    pbuf_aligned = (char *) x;

    pc = shard->scheme.pclipper;

    if (data == NULL) {
	incr(shard->stats[curSchedNum].numDeleteOps);
    } else {
	incr(shard->stats[curSchedNum].numPutOps);
    }

    #ifdef CHECK_SIZE
	check_size(pc);
    #endif

    h = hashck((const unsigned char *) key,
               metaData->keyLen, 0, metaData->cguid);

    syndrome = MASK(h, pc->syndrome_mask);
    nbucket  = (MASK(h, pc->bucket_mask)) % pc->nbuckets;

    nslab  = nbucket / pc->buckets_per_slab;
    pslab  = &(pc->slabs[nslab]);
    offset = pslab->offset;

    npages_new = calc_npages(pc, metaData->dataLen, metaData->keyLen);
    npages_actual = npages_new; // for a miss, npages_new == npages_actual

    Lock(pslab->lock, lock_wait);

    /* check bucket list for matching syndrome */
    for (lice = pc->buckets[nbucket].list_head, ice = lice + offset;
         lice != pc->null_index; 
	 lice = pc->entries[ice].used_header.next, ice = lice + offset) 
    {
        pce = &(pc->entries[ice]);
	plat_assert(!(pce->used_header.flags & (1<<CL_FLAGS_FREE)));
	plat_assert(pce->used_header.flags & (1<<CL_FLAGS_HEADER));

	if (pce->used_header.syndrome == syndrome) {
	    
            /* compute size */
            if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	        npages_actual = pc->entries[ice+1].used_extension.n_used;
	    } else {
	        npages_actual = 1;
	    }

	    rc = get_meta(shard->dev, pctxt, pc, pbuf_aligned, &pmeta, ice, npages_actual);

            if (rc) {
		#ifdef CHECK_STRUCTURES
		    check_structures(pc, nbucket, CheckCycles);
		#endif
		Unlock(pslab->lock, lock_wait);
		return(rc);
	    }

	    if (strcmp(key, pmeta->key) == 0) {

	        if (flags & FLASH_PUT_TEST_EXIST) {
		    /* expected object to NOT exist */
		    #ifdef CHECK_STRUCTURES
			check_structures(pc, nbucket, CheckCycles);
		    #endif
		    Unlock(pslab->lock, lock_wait);
		    return(FLASH_EEXIST);
		}

                // This assumes I should invalidate old metadata on flash first
                rc = invalidate_meta(shard->dev, pctxt, pc, ice, pmeta, npages_actual);
		if (rc) {
		    /* could not invalidate metadata on flash for some reason */
		    #ifdef CHECK_STRUCTURES
			check_structures(pc, nbucket, CheckCycles);
		    #endif
		    Unlock(pslab->lock, lock_wait);
		    return(rc);
		}

                if (data == NULL) {

		    /* just do a delete */

		    (pc->pshard->numObjects)--;

		    rc = free_object_entries(pc, pslab, metaData, key, ice, 1 /* put on free list */, pmeta->databytes + pmeta->key_len, pmeta->npages_used);
		    if (rc) {
			/* could not free object entries for some reason */
			#ifdef CHECK_STRUCTURES
			    check_structures(pc, nbucket, CheckCycles);
			#endif
			Unlock(pslab->lock, lock_wait);
			return(rc);
		    }

		    /* I can return here because I don't need to update LRU */
		    #ifdef CHECK_STRUCTURES
			check_structures(pc, nbucket, CheckCycles);
		    #endif
		    Unlock(pslab->lock, lock_wait);
		    return(0);

		} else {
		    /* replace the object */

		    if (pmeta->npages_actual >= npages_new) {

		        /*   Overwrite object in place.
			 */

			/* store object to flash */

			rc = put_object(shard->dev, pctxt, pc, data, ice, npages_new, npages_actual, metaData, key);
			if (rc) {
			    /* could not write object for some reason */
			    #ifdef CHECK_STRUCTURES
				check_structures(pc, nbucket, CheckCycles);
			    #endif
			    Unlock(pslab->lock, lock_wait);
			    return(rc);
			}

			/* update LRU */

			if (pce->used_header.lru_prev != pc->null_index) {
			    /* I am NOT already at the head of the LRU list */
			    if (pslab->lru_head != pc->null_index) {
				pc->entries[pslab->lru_head + offset].used_header.lru_prev = lice;
			    }
			    pc->entries[pce->used_header.lru_prev + offset].used_header.lru_next = pce->used_header.lru_next;
			    if (pce->used_header.lru_next != pc->null_index) {
				pc->entries[pce->used_header.lru_next + offset].used_header.lru_prev = pce->used_header.lru_prev;
			    } else {
				/* I am the tail of the LRU list */
				pslab->lru_tail = pce->used_header.lru_prev;
			    }
			    pce->used_header.lru_prev = pc->null_index;
			    pce->used_header.lru_next = pslab->lru_head;
			    pslab->lru_head = lice;
			}

			/* update slab stats */
			pslab->pages_needed += (npages_new - pmeta->npages_used);
			pslab->bytes_needed += ((metaData->dataLen + metaData->keyLen)- (pmeta->databytes+ pmeta->key_len));
			/* pslab->pages_in_use does not change */
			/* pslab->nobjects does not change     */

			#ifdef CHECK_STRUCTURES
			    check_structures(pc, nbucket, CheckCycles);
			#endif
			Unlock(pslab->lock, lock_wait);
			return(0);

		    } else {
		        /* need more pages for new object */

                        // ice_old  = ice; // remember the old ice for debugging
                        // lice_old = lice; // remember the old lice for debugging

			rc = free_object_entries(pc, pslab, metaData, key, ice, 1 /* put on free list */, pmeta->databytes + pmeta->key_len, pmeta->npages_used);
			if (rc) {
			    /* could not free object entries for some reason */
			    #ifdef CHECK_STRUCTURES
				check_structures(pc, nbucket, CheckCycles);
			    #endif
			    Unlock(pslab->lock, lock_wait);
			    return(rc);
			}

			rc = get_object_entries(pc, pslab, syndrome, nbucket, metaData, key, &pce, &ice);
			if (rc) {
			    /* could not allocate object entries for some reason */
			    #ifdef CHECK_STRUCTURES
				check_structures(pc, nbucket, CheckCycles);
			    #endif
			    Unlock(pslab->lock, lock_wait);
			    return(rc);
			}

			/* store object to flash */

			rc = put_object(shard->dev, pctxt, pc, data, ice, npages_new, npages_actual, metaData, key);
			if (rc) {
			    /* could not write object for some reason */
			    #ifdef CHECK_STRUCTURES
				check_structures(pc, nbucket, CheckCycles);
			    #endif
			    Unlock(pslab->lock, lock_wait);
			    return(rc);
			}

			/* LRU was already updated by get_object_entries */

			#ifdef CHECK_STRUCTURES
			    check_structures(pc, nbucket, CheckCycles);
			#endif
			Unlock(pslab->lock, lock_wait);
			return(0);
		    }
		}
	    }
	}
    }

    /* check flags */

    if (flags & FLASH_PUT_TEST_NONEXIST) {
        /* expected object to already exist */
	#ifdef CHECK_STRUCTURES
	    check_structures(pc, nbucket, CheckCycles);
	#endif
	Unlock(pslab->lock, lock_wait);
        return(FLASH_ENOENT);
    }

    if (data == NULL) {
        /* was just trying to do a delete */
	#ifdef CHECK_STRUCTURES
	    check_structures(pc, nbucket, CheckCycles);
	#endif
	Unlock(pslab->lock, lock_wait);
        return(FLASH_ENOENT);
    }

    /* miss--so just create a spot for the object */

    (pc->pshard->numObjects)++;
    (pc->pshard->numCreatedObjects)++;

    rc = get_object_entries(pc, pslab, syndrome, nbucket, metaData, key, &pce, &ice);
    if (rc) {
	/* could not allocate object entries for some reason */
	#ifdef CHECK_STRUCTURES
	    check_structures(pc, nbucket, CheckCycles);
	#endif
	Unlock(pslab->lock, lock_wait);
	return(rc);
    }

    /* store object to flash */

    if ((rc = put_object(shard->dev, pctxt, pc, data, ice, npages_new, npages_actual, metaData, key))) {
	/* could not write object for some reason */
	#ifdef CHECK_STRUCTURES
	    check_structures(pc, nbucket, CheckCycles);
	#endif
	Unlock(pslab->lock, lock_wait);
	return(rc);
    }

    /* LRU was already updated by get_object_entries */

    #ifdef CHECK_STRUCTURES
	check_structures(pc, nbucket, CheckCycles);
    #endif

    Unlock(pslab->lock, lock_wait);
    return(0);
}

static uint32_t calc_npages(Clipper_t *pc, uint32_t databytes, uint16_t keybytes)
{
    uint32_t   npages, totalbytes;

    totalbytes = databytes + keybytes + sizeof(ClipperFlashMeta_t);
    if ((keybytes + sizeof(ClipperFlashMeta_t)) >= pc->pagesize) {
	plat_log_msg(21679, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "key + metadata take up more than one page!");
	plat_abort();
    }
    npages = totalbytes/pc->pagesize;
    if (totalbytes % pc->pagesize) {
        npages++;
    }
    return(npages);
}

static int get_pow2(uint32_t npages)
{
    int   i;
    
    for (i=0; i<64; i++) {
        if ((1<<i) >= npages) {
	    break;
	}
    }
    if (i >= 64) {
        return(-1);
    } else {
	return(i);
    }
}

/***********************************************************************
                 Functions to Manipulate Object Entries
 ***********************************************************************/

/*   Get a chained list of pages (as contiguous as possible).
 *   Remove from free list, update LRU list, and put on hash list.
 */

static int get_object_entries(Clipper_t *pc, ClipperSlab_t *pslab, uint64_t syndrome, uint64_t nbucket, struct objMetaData *metaData, char *key, ClipperEntry_t **ppce, uint64_t *pice)
{
    uint32_t         npages_needed, npages_found;
    uint64_t         bytes_needed;
    uint64_t         ice;
    uint32_t         lice;
    uint64_t         offset;
    ClipperEntry_t  *pce, *pce_ext;
    int              rc, i, pow2;

    offset = pslab->offset;
    bytes_needed = metaData->dataLen + metaData->keyLen;
    npages_needed = calc_npages(pc, metaData->dataLen, metaData->keyLen);
    pow2 = get_pow2(npages_needed);

    pc->pshard->usedSpace += (npages_needed*pc->pagesize);

    if ((pow2 > N_CLIPPER_SLABSIZE_BITS) ||
        (pow2 < 0))
    {
	plat_log_msg(21680, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Object requires too many pages (%d)!", npages_needed);
	plat_abort();
    }

    /* look for best fit free region of pages */
    lice = pc->null_index;
    for (i=pow2; i<=N_CLIPPER_SLABSIZE_BITS; i++) {
        lice = pslab->free_bins[i];
        if (lice != pc->null_index) {
	    break;
	}
    }
    if (lice != pc->null_index) {
	ice = lice + offset;
        /* remove the region from the free list */
	pce = &(pc->entries[ice]);
	npages_found = pce->free_header.n_contig;
	pslab->free_bins[i] = pce->free_header.bin_next;
    } else {
        /* check if there are unallocated free pages */

        /* the "-1" ensures that the "null_index" page is never used */
	if ((pc->pages_per_slab - 1 - pslab->used_pages) >= npages_needed) {
	    /* xxxzzz should I force the size to a power of 2? */
	    lice                = pslab->used_pages;
	    ice                 = lice + offset;
	    pslab->used_pages  += npages_needed;
	    npages_found        = npages_needed;
	}

        if (lice == pc->null_index) {
	    if (pc->pshard->flags & FLASH_SHARD_INIT_EVICTION_STORE) {
		/* This is a store and there is no contiguous free space */

		return(FLASH_ENOMEM);
	    } else {
		/* This is a cache and I must evict to get some free space */

		/*  Search for the least recently used object that can contain
		 *  the new object.
		 */

		for (lice = pslab->lru_tail, ice = lice + offset;
		     lice != pc->null_index; 
		     lice = pc->entries[ice].used_header.lru_prev, ice = lice + offset) 
		{
		    pce = &(pc->entries[ice]);

		    if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
			npages_found = pc->entries[ice+1].used_extension.n_contiguous;
		    } else {
			npages_found = 1;
		    }

		    if (npages_found >= npages_needed) {
			/* this victim will do */
			break;
		    }
		}
		if (lice == pc->null_index) {
		    /* Panic: there are no victims big enough! */
		    return(FLASH_ENOMEM);
		}

		/* do the eviction */

		incr(pc->pshard->stats[curSchedNum].numEvictions);
		(pc->pshard->numObjects)--;

		rc = free_object_entries(pc, pslab, metaData, key, ice, 0 /* don't put on free list */, 0, 0);
		if (rc) {
		    /* could not free object entries for some reason */
		    return(rc);
		}
	    }
	}
    }

    /* change the entry metadata */

    pce = &(pc->entries[ice]);

    pce->used_header.flags    = (1<<CL_FLAGS_HEADER);
    pce->used_header.syndrome = syndrome;

    if (npages_found == 1) {
	pce->used_header.bucket_or_cont = nbucket;
    } else {
	pce->used_header.flags    |= (1<<CL_FLAGS_IS_EXTENDED);
	pce->used_header.bucket_or_cont = lice+1;

	pce_ext = &(pc->entries[ice+1]);
	pce_ext->used_extension.flags        = 0;
	pce_ext->used_extension.bucket       = nbucket;
	pce_ext->used_extension.n_contiguous = npages_found;
	pce_ext->used_extension.n_used       = npages_needed;
	pce_ext->used_extension.obj_prev     = lice;
	if (npages_found > 2) {
	    pce_ext->used_extension.obj_next = lice + 2;
	} else {
	    pce_ext->used_extension.obj_next = pc->null_index;
	}

	for (i=2; i<npages_found; i++) {
	    pce_ext = &(pc->entries[ice+i]);
	    pce_ext->used_extension.flags        = 0;
	    pce_ext->used_extension.bucket       = nbucket;
	    pce_ext->used_extension.n_contiguous = i;
	    pce_ext->used_extension.n_used       = 0;
	    pce_ext->used_extension.obj_prev     = lice + i - 1;
	    if (i == (npages_found - 1)) {
		pce_ext->used_extension.obj_next = pc->null_index;
	    } else {
		pce_ext->used_extension.obj_next = lice + i + 1;
	    }
	}
    }

    // update the slab statistics

    // xxxzzz make sure these are updated elsewhere as needed!
    pslab->pages_needed += npages_needed;
    pslab->pages_in_use += npages_found;
    pslab->bytes_needed += bytes_needed;
    (pslab->nobjects)++;

    /* put at head of hash chain */

    pce->used_header.next = pc->buckets[nbucket].list_head;
    pc->buckets[nbucket].list_head = lice;

    /* put at head of LRU chain */

    pce->used_header.lru_prev = pc->null_index;
    pce->used_header.lru_next = pslab->lru_head;
    if (pslab->lru_head != pc->null_index) {
	pc->entries[pslab->lru_head + offset].used_header.lru_prev = lice;
    }
    pslab->lru_head = lice;
    if (pslab->lru_tail == pc->null_index) {
	pslab->lru_tail = lice;
    }

    *ppce = &(pc->entries[ice]);
    *pice = ice;

    return(0);
}

/*   Free up a contiguous array of pages.
 *   Put on free list, remove from LRU list, and remove from hash list.
 */

static int free_object_entries(Clipper_t *pc, ClipperSlab_t *pslab, struct objMetaData *metaData, char *key, uint64_t ice, int put_on_freelist, uint64_t bytes_needed, uint64_t npages_needed)
{
    int              i, pow2;
    ClipperEntry_t  *pce, *pce_ext;
    uint64_t         nbucket;
    uint64_t         ice_hash=0, ice_prev=0;
    uint32_t         lice, lice_hash=0, lice_prev=0;
    uint64_t         offset;
    uint64_t         npages_used, npages_total;

    offset = pslab->offset;

    pce = &(pc->entries[ice]);
    plat_assert(!(pce->used_header.flags & (1<<CL_FLAGS_FREE)));
    plat_assert((pce->used_header.flags & (1<<CL_FLAGS_HEADER)));
    if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
        nbucket       = pc->entries[ice+1].used_extension.bucket;
        npages_total  = pc->entries[ice+1].used_extension.n_contiguous;
        npages_used   = pc->entries[ice+1].used_extension.n_used;
    } else {
        npages_used  = 1;
        npages_total = 1;
	nbucket = pce->used_header.bucket_or_cont;
    }

    pc->pshard->usedSpace -= (npages_total*pc->pagesize);

    /* remove from hash bucket list */

    lice_prev = pc->null_index;
    for (lice_hash = pc->buckets[nbucket].list_head, 
         ice_hash = lice_hash + offset;

         lice_hash != pc->null_index; 

	 lice_prev = lice_hash, ice_prev = ice_hash, 
	 lice_hash = pc->entries[ice_hash].used_header.next, 
	 ice_hash = lice_hash + offset) 
    {
        if (ice_hash == ice) {
	    lice = lice_hash;
	    break;
	}
    }
    if (lice_hash == pc->null_index) {
	plat_log_msg(21681, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		     "Could not find a page entry in the bucket it refers to!");
	plat_abort();
    }

    if (lice_prev == pc->null_index) {
	pc->buckets[nbucket].list_head = pce->used_header.next;
    } else {
	pc->entries[ice_prev].used_header.next = pce->used_header.next;
    }

    /* remove from LRU list */

    if (pce->used_header.lru_next == pc->null_index) {
	/* I am the tail of the LRU list */
	plat_assert(pslab->lru_tail == lice);
        pslab->lru_tail = pce->used_header.lru_prev;
    } else {
        pc->entries[pce->used_header.lru_next + offset].used_header.lru_prev = pce->used_header.lru_prev;
    }
    if (pce->used_header.lru_prev == pc->null_index) {
	/* I am at the head of the LRU list */
	plat_assert(pslab->lru_head == lice);
	pslab->lru_head = pce->used_header.lru_next;
    } else {
        pc->entries[pce->used_header.lru_prev + offset].used_header.lru_next = pce->used_header.lru_next;
    }

    /* update the entry metadata */

    pce->free_header.flags        = ((1<<CL_FLAGS_HEADER)|(1<<CL_FLAGS_FREE));
    pce->free_header.n_contig     = npages_total;
    pce->free_header.free_next    = (npages_total > 1) ? (lice + 1) : pc->null_index;
    pce->free_header.free_prev    = pc->null_index;

    for (i=1; i<npages_total; i++) {
	pce_ext = &(pc->entries[ice+i]);
	pce_ext->free_header.flags        = (1<<CL_FLAGS_FREE);
	pce_ext->free_header.n_contig     = i;
	pce_ext->free_header.bin_next     = pc->null_index;
	pce_ext->free_header.free_prev    = lice + i - 1;
	if (i == (npages_total - 1)) {
	    pce_ext->free_header.free_next = pc->null_index;
	} else {
	    pce_ext->free_header.free_next = lice + i + 1;
	}
    }

    if (put_on_freelist) {
	/* put the region on the free list */

	pow2 = get_pow2(npages_total);

	if (pow2 > N_CLIPPER_SLABSIZE_BITS) {
	    plat_log_msg(21682, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
			 "Contiguous region with too many pages (%"PRIu64")!", npages_total);
	    plat_abort();
	}

	pce->free_header.bin_next = pslab->free_bins[pow2];
	pslab->free_bins[pow2]    = lice;
    }

    /* update slab stats */
    pslab->pages_needed -= npages_used;
    pslab->pages_in_use -= npages_total;
    pslab->bytes_needed -= bytes_needed;
    (pslab->nobjects)--;

    return(0);
}

/***********************************************************************
                 Flash Meta-data Manipulation Functions
 ***********************************************************************/

static int get_meta(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, char *pbuf, ClipperFlashMeta_t **ppmeta, uint64_t ice, uint32_t npages)
{
    int                  rc;
    uint64_t             blknum;
    ClipperFlashMeta_t  *pmeta;
    char                *p;

    /* pbuf must be aligned already! */
    plat_assert((((uint64_t) pbuf) % SSD_AIO_ALIGNMENT) == 0);

    /* only get last page */
    blknum = pc->flash_offset_index + ice + (npages - 1);

#ifdef CLIPPER_AIO
    if ((rc = clipper_aio_read_flash(pdev, pbuf, blknum*pc->pagesize, pc->pagesize))) {
#else
    if ((rc = ssdaio_read_flash(pdev, pctxt, pbuf, blknum*pc->pagesize, pc->pagesize))) {
#endif
	plat_log_msg(21683, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "read_flash failed (rc=%d).", rc);
	*ppmeta = NULL;
	return(rc);
    }

    /*   Get the pointer to the metadata.
     *   Remember, metadata is at END of last block! 
     */

    p = pbuf + pc->pagesize;
    pmeta = (ClipperFlashMeta_t *) (p - sizeof(ClipperFlashMeta_t)); 
    pmeta->key = ((char *) pmeta) - pmeta->key_len - 1; // remember null at end
    *ppmeta = pmeta;

    return(0);
}

static int invalidate_meta(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, uint64_t ice, ClipperFlashMeta_t *pmeta, uint32_t npages)
{
    // xxxzzz finish me!
    // xxxzzz complete invalidate_meta for better flash consistency checking

    return(0);
}

static int get_object(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, 
                      Clipper_t *pc, char **ppbuf, struct objMetaData *metaData,
		      uint64_t ice, uint32_t npages, char *key, int *pkey_match)
{
    uint64_t             blknum;
    int                  rc, big_buffer;
    uint64_t             x;
    uint32_t             bufsize;
    ClipperFlashMeta_t  *pmeta_flash;
    char                *p, *pbuf=NULL, *pbuf_aligned;
    char                *pbuf_big = NULL;

    // xxxzzz watch out for stack overflows because of this buffer!
    char                 buf[MAX_DEFAULT_BUF_SIZE + SSD_AIO_ALIGNMENT];

    bufsize = metaData->dataLen; // max buffer size if a user buffer is provided

    if (npages*pc->pagesize > MAX_DEFAULT_BUF_SIZE) {
        big_buffer = 1;

	pbuf_big = plat_alloc(npages*pc->pagesize + SSD_AIO_ALIGNMENT);
	if (pbuf_big == NULL) {
	    return(FLASH_ENOMEM);
	}

	x = (uint64_t) pbuf_big + SSD_AIO_ALIGNMENT;
	// x &= 0xfffffffffffffe00ULL; // 512B alignment
	x &= 0xfffffffffffff000ULL; // 4096B alignment
	pbuf_aligned = (char *) x;
    } else {
        big_buffer = 0;
	x = (uint64_t) buf + SSD_AIO_ALIGNMENT;
	// x &= 0xfffffffffffffe00ULL; // 512B alignment
	x &= 0xfffffffffffff000ULL; // 4096B alignment
	pbuf_aligned = (char *) x;
    }

    blknum = pc->flash_offset_index + ice;

#ifdef CLIPPER_AIO
    if ((rc = clipper_aio_read_flash(pdev, pbuf_aligned, blknum*pc->pagesize, npages*pc->pagesize))) {
#else
    if ((rc = ssdaio_read_flash(pdev, pctxt, pbuf_aligned, blknum*pc->pagesize, npages*pc->pagesize))) {
#endif
	plat_log_msg(21683, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "read_flash failed (rc=%d).", rc);
	/* don't forget to free the big buffer! */
	if (big_buffer) {
	    plat_free(pbuf_big);
	}
	return(rc);
    }

    /*   Get the pointer to the metadata.
     *   Remember, metadata is at END of last block! 
     */

    p = pbuf_aligned + pc->pagesize*npages;
    pmeta_flash = (ClipperFlashMeta_t *) (p - sizeof(ClipperFlashMeta_t)); 
    pmeta_flash->key = ((char *) pmeta_flash) - pmeta_flash->key_len - 1; // remember null at end

    metaData->dataLen    = pmeta_flash->databytes;
    metaData->expTime    = pmeta_flash->exp_time;
    metaData->createTime = pmeta_flash->create_time;
    if (strcmp(key, pmeta_flash->key) == 0) {
        *pkey_match = 1;
    } else {
        *pkey_match = 0;
    }

    if ((ppbuf != NULL) && (*ppbuf)) {
        /* this is a user-provided buffer */
	if (bufsize < pmeta_flash->databytes) {
	    /* don't forget to free the big buffer! */
	    if (big_buffer) {
	        plat_free(pbuf_big);
	    }
	    return(FLASH_EDATASIZE); /* buffer not big enough! */
	}
	pbuf = *ppbuf;
    } else if (ppbuf != NULL) {
        /* I must allocate a buffer */
	pbuf = plat_alloc(pmeta_flash->databytes);
	*ppbuf = pbuf;
	if (pbuf == NULL) {
	    /* don't forget to free the big buffer! */
	    if (big_buffer) {
	        plat_free(pbuf_big);
	    }
	    return(FLASH_ENOMEM);
	}
    } else {
        pbuf = NULL;
    }

    #ifdef DEBUG_CLIPPER
	plat_log_msg(21684, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
	     "CLIPPER get_object: key='%s', ice=%"PRIu64", npages=%d, " 
	     "pmeta->databytes=%d, pmeta->npages_used=%d, pmeta->npages_actual=%d",
	     key, ice, npages, pmeta_flash->databytes, 
	     pmeta_flash->npages_used, pmeta_flash->npages_actual);
    #endif // DEBUG_CLIPPER

    if (pbuf != NULL) {
	/* copy to the final, unaligned buffer */
	memcpy(pbuf, pbuf_aligned, pmeta_flash->databytes);
    }

    /* don't forget to free the big buffer! */
    if (big_buffer) {
	plat_free(pbuf_big);
    }

    return(0);
}

static int put_object(struct flashDev *pdev, ssdaio_ctxt_t *pctxt, Clipper_t *pc, char *pbuf, uint64_t ice, uint32_t npages_new, uint32_t npages_actual, struct objMetaData *metaData, char *key)
{
    uint64_t             blknum;
    int                  rc, big_buffer;
    uint64_t             x;
    ClipperFlashMeta_t  *pmeta_flash;
    char                *p, *pbuf_aligned;
    char                *pbuf_big = NULL;

    // xxxzzz watch out for stack overflows because of this buffer!
    char                 buf[MAX_DEFAULT_BUF_SIZE + SSD_AIO_ALIGNMENT];

    if (npages_new*pc->pagesize > MAX_DEFAULT_BUF_SIZE) {
        big_buffer = 1;

	pbuf_big = plat_alloc(npages_new*pc->pagesize + SSD_AIO_ALIGNMENT);
	if (pbuf_big == NULL) {
	    return(FLASH_ENOMEM);
	}

	x = (uint64_t) pbuf_big + SSD_AIO_ALIGNMENT;
	// x &= 0xfffffffffffffe00ULL; // 512B alignment
	x &= 0xfffffffffffff000ULL; // 4096B alignment
	pbuf_aligned = (char *) x;
    } else {
        big_buffer = 0;
	x = (uint64_t) buf + SSD_AIO_ALIGNMENT;
	// x &= 0xfffffffffffffe00ULL; // 512B alignment
	x &= 0xfffffffffffff000ULL; // 4096B alignment
	pbuf_aligned = (char *) x;
    }

    blknum = pc->flash_offset_index + ice;

    // copy to an aligned buffer

    // memcpy(pbuf_aligned, pbuf, npages_new*pc->pagesize);
    memcpy(pbuf_aligned, pbuf, metaData->dataLen);

    /*   Set up metadata:
     *      Get the pointer to the metadata in the aligned buffer.
     *      Remember, metadata is at END of last block! 
     */

    p = pbuf_aligned + pc->pagesize*npages_new;
    pmeta_flash = (ClipperFlashMeta_t *) (p - sizeof(ClipperFlashMeta_t)); 

    pmeta_flash->databytes     = metaData->dataLen;
    pmeta_flash->npages_used   = npages_new; // includes metadata
    pmeta_flash->npages_actual = npages_actual; // includes unused pages at end
    pmeta_flash->state         = 0; // xxxzzz placeholder for recovery
    pmeta_flash->offset        = 0; // xxxzzz finish this for recovery
    pmeta_flash->exp_time      = metaData->expTime;
    pmeta_flash->create_time   = metaData->createTime;
    pmeta_flash->key_len       = metaData->keyLen;
    pmeta_flash->shard         = 0; // xxxzzz placeholder for recovery
    pmeta_flash->key = ((char *) pmeta_flash) - pmeta_flash->key_len - 1; // remember null at end
    (void) strcpy(pmeta_flash->key, key);
    pmeta_flash->magic         = 0; // xxxzzz placeholder for recovery

    #ifdef DEBUG_CLIPPER
	plat_log_msg(21685, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
	     "CLIPPER put_object: key='%s', ice=%"PRIu64", npages_new=%d, " 
	     "pmeta->databytes=%d, pmeta->npages_used=%d, pmeta->npages_actual=%d",
	     key, ice, npages_new, pmeta_flash->databytes, 
	     pmeta_flash->npages_used, pmeta_flash->npages_actual);
    #endif // DEBUG_CLIPPER

#ifdef CLIPPER_AIO
    if ((rc = clipper_aio_write_flash(pdev, pbuf_aligned, blknum*pc->pagesize, npages_new*pc->pagesize))) {
#else
    if ((rc = ssdaio_write_flash(pdev, pctxt, pbuf_aligned, blknum*pc->pagesize, npages_new*pc->pagesize))) {
#endif
	plat_log_msg(21686, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_DEBUG,
		     "write_flash failed (rc=%d).", rc);
	/* don't forget to free the big buffer! */
	if (big_buffer) {
	    plat_free(pbuf_big);
	}
	return(rc);
    }

    /* don't forget to free the big buffer! */
    if (big_buffer) {
	plat_free(pbuf_big);
    }

    return(0);
}

/***********************************************************************/

void ClipperStartEnumeration(Clipper_t *pc)
{
    plat_log_msg(21687, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "ClipperStartEnumeration is not currently supported!");
    plat_abort();
}

void ClipperEndEnumeration(Clipper_t *pc)
{
    plat_log_msg(21688, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "ClipperEndEnumeration is not currently supported!");
    plat_abort();
}

SDF_boolean_t ClipperNextEnumeration(Clipper_t *pc, SDF_simple_key_t *pkey)
{
    plat_log_msg(21689, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "ClipperNextEnumeration is not currently supported!");
    plat_abort();
}

void ClipperPrintAll(FILE *f, Clipper_t *pc)
{
    plat_log_msg(21690, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "ClipperRemove is not currently supported!");
    plat_abort();
}

void ClipperGetStats(Clipper_t *pc, ClipperStats_t *pstat)
{
    plat_log_msg(21690, PLAT_LOG_CAT_FLASH, PLAT_LOG_LEVEL_FATAL,
		 "ClipperRemove is not currently supported!");
    plat_abort();

#ifdef notdef
    =================================================================
    uint64_t          i;
    ClipperSlab_t   *ps;

    pstat->num_objects = 0;
    pstat->cursize     = 0;

    for (i = 0; i < pc->nslabs; i++) {
        ps = &(pc->slabs[i]);
	(pstat->num_objects) += ps->nobjects;
	pstat->cursize       += ps->cursize;
    }
    =================================================================
#endif
}

static void init_mask(MASKDATA *pm, int nbits, int offset)
{
   pm->nbits    = nbits;
   pm->offset   = offset;
   pm->mask     = ((1ULL<<nbits) - 1) << offset;
}

#ifdef CHECK_SIZE
static void check_size(Clipper_t *pc)
{
    uint64_t          i;
    ClipperSlab_t   *ps;
    ClipperEntry_t  *pce;
    uint64_t          asize, ssize;
    
    asize = 0;
    ssize = 0;
    for (i = 0; i < pc->nslabs; i++) {
        ps = &(pc->slabs[i]);
	ssize += ps->cursize;
	for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
	    asize += (pce->obj_size + pc->state_size + sizeof(ClipperEntry_t));
	}
    }
    if (asize != ssize) {
        fprintf(stderr, "================   asize(%"PRIu64") != ssize(%"PRIu64")   =============\n", asize, ssize);
    }
}
#endif // CHECK_SIZE

#ifdef CHECK_STRUCTURES

static void err_fprintf(FILE *f, char *fmt, ...);
static void oh_oh(char *s);

static char *flags_string(uint32_t flags, char *stmp)
{
    stmp[0] = '\0';
    if (flags & (1<<CL_FLAGS_FREE)) {
        strcat(stmp, "F");
    }
    if (flags & (1<<CL_FLAGS_HEADER)) {
        strcat(stmp, "H");
    }
    if (flags & (1<<CL_FLAGS_IS_EXTENDED)) {
        strcat(stmp, "E");
    }
    if (flags & (1<<CL_FLAGS_ENUM)) {
        strcat(stmp, "N");
    }
    return(stmp);
}

static int check_used_header(Clipper_t *pc, uint64_t ice, uint32_t *pnpages_total, uint32_t *pnpages_used, uint32_t *pnbucket, int printflag)
{
    ClipperEntry_t *pce, *pce_ext=NULL;
    uint32_t        nbucket, nslab, npages, lice;
    int             bug = 0;
    uint64_t        i, offset;
    char            stmp[128];
    ClipperSlab_t  *pslab;

    pce = &(pc->entries[ice]);

    if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	pce_ext = &(pc->entries[ice+1]);
        *pnbucket      = pce_ext->used_extension.bucket;
        *pnpages_total = pce_ext->used_extension.n_contiguous;
        *pnpages_used  = pce_ext->used_extension.n_used;
    } else {
        *pnbucket      = pce->used_header.bucket_or_cont;
        *pnpages_total = 1;
        *pnpages_used  = 1;
    }
    npages  = *pnpages_total;
    nbucket = *pnbucket;
    nslab   = nbucket / pc->buckets_per_slab;
    pslab   = &(pc->slabs[nslab]);
    offset  = pslab->offset;
    lice    = ice - offset;

    if (printflag) {
        fprintf(stderr, "U ice: %"PRIu64", bucket: %d, bucket_or_cont: %d, flags: %s, syndrome: %d, pages: %d/%d, next: %d, lru_next: %d, lru_prev: %d\n", 
	        ice, *pnbucket, pce->used_header.bucket_or_cont,
		flags_string(pce->used_header.flags, stmp), 
		pce->used_header.syndrome, 
		*pnpages_used, *pnpages_total,
		pce->used_header.next, 
		pce->used_header.lru_next, 
		pce->used_header.lru_prev);
    }

    /* check header flags */

    if (pce->used_header.flags & (1<<CL_FLAGS_FREE)) {
        if (printflag) {
	    err_fprintf(stderr, "ERROR: free flag is set!!\n");
	}
	bug = 1;
    }
    if (!(pce->used_header.flags & (1<<CL_FLAGS_HEADER))) {
        if (printflag) {
	    err_fprintf(stderr, "ERROR: header flag is NOT set!!\n");
	}
	bug = 1;
    }
    if (pce->used_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	pce_ext = &(pc->entries[ice+1]);
	if (pce->used_header.bucket_or_cont != (lice+1)) {
	    if (printflag) {
		err_fprintf(stderr, "ERROR: header extension page is NOT contiguous!!\n");
	    }
	    bug = 1;
	}
    }

    /* check continuity of this used region */

    if (npages > 1) {

	if (printflag) {
	    fprintf(stderr, "  ice: %"PRIu64", bucket: %d, flags: %s, n_contiguous: %d, obj_next: %d, obj_prev: %d\n", 
		    ice+1, pce_ext->used_extension.bucket, 
		    flags_string(pce_ext->used_extension.flags, stmp), 
		    pce_ext->used_extension.n_contiguous, 
		    pce_ext->used_extension.obj_next, 
		    pce_ext->used_extension.obj_prev);
	}

	if (pce_ext->used_extension.flags & (1<<CL_FLAGS_FREE)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: free flag is set for first extended used list entry %"PRIu64"!!\n", ice+1);
	    }
	}
	if (pce_ext->used_extension.flags & (1<<CL_FLAGS_HEADER)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: header flag is set for first extended used list entry %"PRIu64"!!\n", ice+1);
	    }
	}
	if (pce_ext->used_extension.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: is_extended flag is set for first extended used list entry %"PRIu64"!!\n", ice+1);
	    }
	}

	if (pce_ext->used_extension.obj_prev != lice) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: obj_prev is wrong for first used list extension %"PRIu64" (%d found, %d expected)!!\n", ice, pce_ext->used_extension.obj_prev, 0);
	    }
	}
	if (npages > 2) {
	    if (pce_ext->used_extension.obj_next != (lice + 2)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: obj_next is wrong for first used list header %"PRIu64" (%d found, %d expected)!!\n", ice, pce_ext->used_extension.obj_next, 2);
		}
	    }
	} else {
	    if (pce_ext->used_extension.obj_next != pc->null_index) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: obj_next is wrong for first used list header %"PRIu64" (%d found, %d expected)!!\n", ice, pce_ext->used_extension.obj_next, pc->null_index);
		}
	    }
	}

	for (i=ice+2; i<(ice + npages); i++) {
	    pce_ext = &(pc->entries[i]);

	    if (printflag) {
		fprintf(stderr, "  ice: %"PRIu64", bucket: %d, flags: %s, n_contiguous: %d, obj_next: %d, obj_prev: %d\n", 
			i, pce_ext->used_extension.bucket, 
			flags_string(pce_ext->used_extension.flags, stmp), 
			pce_ext->used_extension.n_contiguous, 
			pce_ext->used_extension.obj_next, 
			pce_ext->used_extension.obj_prev);
	    }

	    if (pce_ext->used_extension.flags & (1<<CL_FLAGS_FREE)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: free flag is set for extended used list entry %"PRIu64"!!\n", i);
		}
	    }
	    if (pce_ext->used_extension.flags & (1<<CL_FLAGS_HEADER)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: header flag is set for extended used list entry %"PRIu64"!!\n", i);
		}
	    }
	    if (pce_ext->used_extension.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: is_extended flag is set for extended used list entry %"PRIu64"!!\n", i);
		}
	    }

            if (pce_ext->used_extension.bucket != nbucket) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: nbucket does not match header for extended used list entry %"PRIu64" (%d found, %d expected)!!\n", i, pce_ext->used_extension.bucket, nbucket);
		}
	    }

	    if (pce_ext->used_extension.n_contiguous != (i - ice)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: n_contiguous is not right for extended used list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->used_extension.n_contiguous, (i - ice));
		}
	    }

	    if (pce_ext->used_extension.n_used != 0) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: n_used is not zero for extended used list entry %"PRIu64" (%d found, %d expected)!!\n", i, pce_ext->used_extension.n_used, 0);
		}
	    }

	    if (i < (ice + npages - 1)) {
		if (pce_ext->used_extension.obj_next != (i + 1 - offset)) {
		    bug = 1;
		    if (printflag) {
			err_fprintf(stderr, "ERROR: obj_next is wrong for extended used list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->used_extension.obj_next, (i - ice + 1));
		    }
		}
	    } else {
		if (pce_ext->used_extension.obj_next != pc->null_index) {
		    bug = 1;
		    if (printflag) {
			err_fprintf(stderr, "ERROR: obj_next is wrong for extended used list entry %"PRIu64" (%d found, %d expected)!!\n", i, pce_ext->used_extension.obj_next, pc->null_index);
		    }
		}
	    }
	    if (pce_ext->used_extension.obj_prev != (i - 1 - offset)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: obj_prev is wrong for extended used list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->used_extension.obj_prev, (i - ice - 1));
		}
	    }
	}
    }
    return(bug);
}

static int check_free_header(Clipper_t *pc, uint64_t ice, uint32_t *pnpages, int printflag)
{
    ClipperEntry_t *pce, *pce_ext;
    int             bug=0;
    uint64_t        i, offset;
    uint32_t        npages, nslab, lice;
    char            stmp[128];
    ClipperSlab_t  *pslab;

    pce = &(pc->entries[ice]);

    npages   = pce->free_header.n_contig;
    *pnpages = npages;

    nslab    = ice / pc->pages_per_slab;
    pslab    = &(pc->slabs[nslab]);
    offset   = pslab->offset;
    lice     = ice - offset;

    if (printflag) {
        fprintf(stderr, "F ice: %"PRIu64", flags: %s, npages: %d, bin_next: %d, obj_next: %d, obj_prev: %d\n",
	    ice, flags_string(pce->free_header.flags, stmp), npages, 
	    pce->free_header.bin_next, pce->free_header.free_next, 
	    pce->free_header.free_prev);
    }

    /* check header flags */
    if (!(pce->free_header.flags & (1<<CL_FLAGS_FREE))) {
	if (printflag) {
	    err_fprintf(stderr, "ERROR: free flag is NOT set in free header %"PRIu64"!!\n", ice);
	}
	bug = 1;
    }
    if (!(pce->free_header.flags & (1<<CL_FLAGS_HEADER))) {
	if (printflag) {
	    err_fprintf(stderr, "ERROR: header flag is NOT set in free header %"PRIu64"!!\n", ice);
	}
	bug = 1;
    }
    if (pce->free_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	if (printflag) {
	    err_fprintf(stderr, "ERROR: is_extended flag is set in free header %"PRIu64"!!\n", ice);
	}
	bug = 1;
    }

    /* check continuity of this free region */

    if (pce->free_header.free_prev != pc->null_index) {
	bug = 1;
	if (printflag) {
	    err_fprintf(stderr, "ERROR: free_prev is wrong for extended free list entry %"PRIu64" (%d found, %d expected)!!\n", ice, pce->free_header.free_prev, pc->null_index);
	}
    }
    if (npages > 1) {
	if (pce->free_header.free_next != (lice + 1)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: free_next is wrong for extended free list entry %"PRIu64" (%d found, %d expected)!!\n", ice, pce->free_header.free_next, 1);
	    }
	}
    } else {
	if (pce->free_header.free_next != pc->null_index) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: free_next is wrong for extended free list entry %"PRIu64" (%d found, %d expected)!!\n", ice, pce->free_header.free_next, pc->null_index);
	    }
	}
    }

    for (i=ice+1; i<(ice + npages); i++) {
        pce_ext = &(pc->entries[i]);
	if (!(pce_ext->free_header.flags & (1<<CL_FLAGS_FREE))) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: free flag is NOT set for extended free list entry %"PRIu64"!!\n", i);
	    }
	}
	if (pce_ext->free_header.flags & (1<<CL_FLAGS_HEADER)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: header flag is set for extended free list entry %"PRIu64"!!\n", i);
	    }
	}
	if (pce_ext->free_header.flags & (1<<CL_FLAGS_IS_EXTENDED)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: is_extended flag is set for extended free list entry %"PRIu64"!!\n", i);
	    }
	}
	if (pce_ext->free_header.n_contig != (i - ice)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: n_contiguous is not right for extended free list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->free_header.n_contig, (i - ice));
	    }
	}
	if (pce_ext->free_header.bin_next != pc->null_index) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: bin_next is not equal to null_index for extended free list entry %"PRIu64"!!\n", i);
	    }
	}
	if (i < (ice + npages - 1)) {
	    if (pce_ext->free_header.free_next != (i + 1 - offset)) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: free_next is wrong for extended free list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->free_header.free_next, (i - ice + 1));
		}
	    }
	} else {
	    if (pce_ext->free_header.free_next != pc->null_index) {
		bug = 1;
		if (printflag) {
		    err_fprintf(stderr, "ERROR: free_next is wrong for extended free list entry %"PRIu64" (%d found, %d expected)!!\n", i, pce_ext->free_header.free_next, pc->null_index);
		}
	    }
	}
	if (pce_ext->free_header.free_prev != (i - 1 - offset)) {
	    bug = 1;
	    if (printflag) {
		err_fprintf(stderr, "ERROR: free_prev is wrong for extended free list entry %"PRIu64" (%d found, %"PRIu64" expected)!!\n", i, pce_ext->free_header.free_prev, (i - ice - 1));
	    }
	}
    }

    return(bug);
}

static int check_lru_list(Clipper_t *pc, ClipperSlab_t *pslab, uint64_t ice_bad, char *s, int printflag)
{
    ClipperEntry_t *pce;
    uint64_t        ice;
    uint32_t        lice;
    uint32_t        nslab;
    uint32_t        nbucket, npages_total, npages_used;
    uint64_t        offset;
    int             bug=0;

    nslab  = pslab->nslab;
    offset = pslab->offset;

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "lru_head: %d, lru_tail: %d\n", pslab->lru_head, pslab->lru_tail);
    }
    for (lice = pslab->lru_head, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.lru_next, ice = lice+offset, pce = &(pc->entries[ice]))
     {
        if (check_used_header(pc, ice, &npages_total, &npages_used, &nbucket, printflag)) {
	    bug = 1;
	    if (!printflag) {
		/* print out bad entry */
		check_lru_list(pc, pslab, ice, "used header check failed", 1);
	    }
	}

        if (printflag) {
	    if (ice == ice_bad) {
		fprintf(stderr, "  ^^^^^^^^^^\n");
	    }
	}
    }

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	if (s != NULL) {
	    oh_oh(s);
	}
    }
    return(bug);
}

static int check_bucket_list(Clipper_t *pc, uint32_t nbucket, uint64_t ice_bad, char *s, int printflag)
{
    ClipperBucket_t  *pb;
    ClipperEntry_t   *pce;
    uint32_t          ice, lice, nslab;
    ClipperSlab_t    *pslab;
    uint64_t          offset;
    uint32_t          nbucket_entry, npages_total, npages_used;
    int               bug=0;

    nslab  = nbucket/pc->buckets_per_slab;
    pslab  = &(pc->slabs[nslab]);
    offset = pslab->offset;

    pb = &(pc->buckets[nbucket]);

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "bucket head: %d\n", pb->list_head);
    }
    for (lice = pb->list_head, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.next, ice = lice+offset, pce = &(pc->entries[ice]))
     {
        if (check_used_header(pc, ice, &npages_total, &npages_used, &nbucket_entry, printflag)) {
	    bug = 1;
	    if (!printflag) {
		/* print out bad entry */
		check_bucket_list(pc, nbucket, ice, "used header check failed", 1);
	    }
	}
	if (nbucket_entry != nbucket) {
	    bug = 1;
	    if (!printflag) {
		/* print out bad entry */
		check_bucket_list(pc, nbucket, ice, "nbucket in entry does not match nbucket for this bucket list", 1);
	    }
	}
	if (printflag) {
	    if (ice == ice_bad) {
		fprintf(stderr, "  ^^^^^^^^^^\n");
	    }
	}
    }

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	if (s != NULL) {
	    oh_oh(s);
	}
    }
    return(bug);
}

static int check_free_bin(Clipper_t *pc, uint32_t nslab, uint32_t i_bin, uint64_t ice_bad, char *s, int printflag)
{
    ClipperEntry_t   *pce;
    uint32_t          ice, lice;
    ClipperSlab_t    *pslab;
    uint64_t          offset;
    int               bug=0;
    uint32_t          npages;

    pslab  = &(pc->slabs[nslab]);
    offset = pslab->offset;

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	fprintf(stderr, "bin[%d] head: %d\n", i_bin, pslab->free_bins[i_bin]);
    }
    for (lice = pslab->free_bins[i_bin], ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->free_header.bin_next, ice = lice+offset, pce = &(pc->entries[ice]))
    {
        if (check_free_header(pc, ice, &npages, printflag)) {
	    bug = 1;
	    if (!printflag) {
	        /* print out bad entry */
	        check_free_bin(pc, nslab, i_bin, ice, "free header check failed", 1);
	    }
	}

	/* check that size of region is consistent with the bin index */

	if ((npages < (1<<i_bin)) ||
	    (npages > (1<<(i_bin+1))))
	{
	    if (!bug) {
		if (!printflag) {
		    /* print out bad entry */
		    check_free_bin(pc, nslab, i_bin, ice, "size of region inconsistent with bin", 1);
		}
	    }
	    if (printflag) {
		err_fprintf(stderr, "ERROR: size of region (%d) is not consistent with bin index (%d - %d)\n", npages, (1<<i_bin), (1<<(i_bin+1)) - 1);
	    }
	    bug = 1;
	}
    }

    if (printflag) {
	fprintf(stderr, "------------------------------------------------------------\n");
	if (s != NULL) {
	    oh_oh(s);
	}
    }
    return(bug);
}

#ifdef notdef
static int check_flash_consistency(Clipper_t *pc)
{
    /*  Go through DRAM structures and flash contents
     *  and ensure that they are consistent.
     */

    err_fprintf(stderr, "ERROR: check_flash_consistency is not implemented!\n");
    /* xxxzzz finish me */
    return(0);
}
#endif

static int check_for_lru_cycle(Clipper_t *pc, ClipperSlab_t *pslab)
{
    ClipperEntry_t *pce, *pce2;
    uint64_t        ice, ice2;
    uint32_t        lice, lice2, lice_last;
    uint32_t        nslab;
    uint64_t        offset;
    int             bug=0;

    nslab  = pslab->nslab;
    offset = pslab->offset;

    /* check forward list starting at lru_head */

    for (lice = pslab->lru_head, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.lru_next, ice = lice+offset, pce = &(pc->entries[ice]))
     {
	for (lice2 = pslab->lru_head, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]); 
	     lice2 != pc->null_index; 
	     lice2 = pce2->used_header.lru_next, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]))
	 {
	     if (pce->used_header.lru_next == lice2) {
	         bug = 1;
		 check_lru_list(pc, pslab, ice, "cycle in LRU head list!", 1);
		 goto lc_cont1;
	     }
	     if (pce == pce2) {
	         break;
	     }
	 }
     }

lc_cont1:

    /* check backward list starting at lru_tail */

    for (lice = pslab->lru_tail, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.lru_prev, ice = lice+offset, pce = &(pc->entries[ice]))
     {
	for (lice2 = pslab->lru_tail, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]); 
	     lice2 != pc->null_index; 
	     lice2 = pce2->used_header.lru_prev, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]))
	 {
	     if (pce->used_header.lru_prev == lice2) {
	         bug = 1;
		 check_lru_list(pc, pslab, ice, "cycle in LRU tail list!", 1);
		 goto lc_cont2;
	     }
	     if (pce == pce2) {
	         break;
	     }
	 }
     }

lc_cont2:

    /* check that forward and backward LRU lists are consistent */

    lice_last = pc->null_index;
    for (lice = pslab->lru_head, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.lru_next, ice = lice+offset, pce = &(pc->entries[ice]))
    {
	if (pce->used_header.lru_prev != lice_last) {
	    bug = 1;
	    check_lru_list(pc, pslab, ice, "LRU forward and backward lists are inconsistent!", 1);
	    break;
	}
	lice_last = lice;
    }

    return(bug);
}

static int check_for_hashlist_cycle(Clipper_t *pc, uint32_t nbucket)
{
    ClipperEntry_t   *pce;
    uint32_t          ice, lice;
    ClipperEntry_t   *pce2;
    uint32_t          ice2, lice2, nslab;
    ClipperBucket_t  *pb;
    ClipperSlab_t    *pslab;
    uint64_t          offset;
    int               bug=0;

    nslab  = nbucket/pc->buckets_per_slab;
    pslab  = &(pc->slabs[nslab]);
    offset = pslab->offset;

    pb = &(pc->buckets[nbucket]);

    for (lice = pb->list_head, ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->used_header.next, ice = lice+offset, pce = &(pc->entries[ice]))
     {
	for (lice2 = pb->list_head, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]); 
	     lice2 != pc->null_index; 
	     lice2 = pce2->used_header.next, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]))
	 {
	     if (pce->used_header.next == lice2) {
	         bug = 1;
		 check_bucket_list(pc, nbucket, ice, "cycle in bucket list!", 1);
		 goto hc_cont1;
	     }
	     if (pce == pce2) {
	         break;
	     }
	 }
     }
hc_cont1:
     return(bug);
}

static int check_for_bin_cycle(Clipper_t *pc, uint32_t nslab, uint32_t i_bin)
{
    ClipperEntry_t   *pce;
    uint32_t          ice, lice;
    ClipperEntry_t   *pce2;
    uint32_t          ice2, lice2;
    ClipperSlab_t    *pslab;
    uint64_t          offset;
    int               bug=0;

    pslab  = &(pc->slabs[nslab]);
    offset = pslab->offset;

    for (lice = pslab->free_bins[i_bin], ice = lice+offset, pce = &(pc->entries[ice]); 
         lice != pc->null_index; 
	 lice = pce->free_header.bin_next, ice = lice+offset, pce = &(pc->entries[ice]))
     {
	for (lice2 = pslab->free_bins[i_bin], ice2 = lice2+offset, pce2 = &(pc->entries[ice2]); 
	     lice2 != pc->null_index; 
	     lice2 = pce2->free_header.bin_next, ice2 = lice2+offset, pce2 = &(pc->entries[ice2]))
	 {
	     if (pce->free_header.bin_next == lice2) {
	         bug = 1;
		 check_free_bin(pc, nslab, i_bin, ice, "cycle in bucket list!", 1);
		 goto bc_cont1;
	     }
	     if (pce == pce2) {
	         break;
	     }
	 }
     }
bc_cont1:
     return(bug);
}

static void check_structures(Clipper_t *pc, uint32_t nbucket, int check_cycles)
{
    uint32_t          nslab;
    ClipperSlab_t    *pslab;
    int               i;

    nslab  = nbucket/pc->buckets_per_slab;
    pslab  = &(pc->slabs[nslab]);

    check_lru_list(pc, pslab, 0, NULL, 0);
    check_bucket_list(pc, nbucket, 0, NULL, 0);

    for (i=0; i<=N_CLIPPER_SLABSIZE_BITS; i++) {
	check_free_bin(pc, nslab, i, 0, NULL, 0);
    }

    if (check_cycles) {
	check_for_lru_cycle(pc, pslab);
        check_for_hashlist_cycle(pc, nbucket);

	for (i=0; i<=N_CLIPPER_SLABSIZE_BITS; i++) {
	    check_for_bin_cycle(pc, nslab, i);
	}
    }
}

static void oh_oh(char *s)
{
    err_fprintf(stderr, "%s", s);
}

static void err_fprintf(FILE *f, char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);

   vfprintf(f, fmt, args);

   va_end(args);
}


#endif

#ifdef CHECK_LOCKS

static void lock_trace(char *fmt, ...)
{
   va_list  args;

   va_start(args, fmt);

   vfprintf(stderr, fmt, args);
   fprintf(stderr, "\n");

   va_end(args);
}

#endif

