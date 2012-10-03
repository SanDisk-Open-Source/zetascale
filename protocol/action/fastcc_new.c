/*
 * File:   fastcc_new.c
 * Author: Brian O'Krafka
 *
 * Created on September 2, 2008
 *
 * New version of fastcc with shmem stuff removed.
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fastcc_new.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _FASTCC_NEW_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>
#include "platform/logging.h"
#include "shared/container.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_alloc.h"
#include "protocol/protocol_common.h"
#include "fastcc_new.h"
#include "utils/hash.h"

#define MIN_BACKGRND_FLUSH_SLEEP_MSEC   100

#define N_MAX_NSLABS_ITERS 1000

#define N_ALLOC_REQUEST_STRUCTS   1

static SDFNewCacheRequest_t *get_request_struct(SDFNewCacheSlab_t *ps);
static void free_request_struct(SDFNewCacheSlab_t *ps, SDFNewCacheRequest_t *pinv);
static void process_pending_remote_requests(SDFNewCache_t *pc, SDFNewCacheSlab_t *ps, void *wrbk_arg);
static void lru_remove(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void modlru_remove(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void add_lru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void add_modlru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void update_lru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void update_modlru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce);
static void check_heap(SDFNewCacheSlab_t *ps);
static void *fastcc_object_alloc(SDFNewCacheSlab_t *ps, uint64_t size, void *wrbk_arg);
static uint64_t fastcc_object_free(SDFNewCacheSlab_t *ps, void *obj);
static uint64_t check_nslabs(SDFNewCache_t *pc, uint64_t nslabs_in);
static void init_transient_entry(SDFNewCacheSlab_t *ps);
static void __attribute__((unused)) dump_bucket_list(SDFNewCacheBucket_t *pb, SDFNewCacheEntry_t *pce_bad, char *s);
static void oh_oh(char *s);
static void background_flusher(uint64_t arg);
static void background_flush_progress_fn(SDFNewCache_t *pc, void *flush_arg, uint32_t percent);

#define PAGE_POINTER(pc, p) ((char *) p + (pc)->page_size - 8)

// #define DUMP_CACHE

/*  This variable is used to dynamically enabled detailed
 *  self-checking.
 */
int SDFSelfTestCheckHeap = 0;

static void check_lru_list(SDFNewCacheSlab_t *ps, char *msg);
static void check_modlru_list(SDFNewCacheSlab_t *ps, char *msg);

// #define CHECK_CYCLES

#ifdef CHECK_CYCLES
// static void dump_lists(SDFNewCacheBucket_t *pb);
static void check_for_cycle(SDFNewCacheBucket_t *pb);
#endif

// #define CHECK_LOCKS

#ifdef CHECK_LOCKS
static void lock_trace(char *fmt, ...);
#endif

static void fastcc_init_alloc(SDFNewCache_t *pc)
{
    pc->pmem_start  = plat_alloc_steal_from_heap(pc->size_limit);
    pc->pmem_used   = pc->pmem_start;
    pc->mem_used    = 0;

    #ifdef MALLOC_TRACE
        UTMallocTrace("SDFNewCacheInit", TRUE, FALSE, FALSE, (void *) pc->pmem_start, pc->size_limit);
    #endif // MALLOC_TRACE
    if (pc->pmem_start == NULL) {
        plat_log_msg(21142, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
 		     "Not enough memory for cache (%"PRIu64"), plat_alloc() failed.", pc->size_limit);
        plat_abort();
    }
}

static void fastcc_destroy_alloc(SDFNewCache_t *pc)
{
    #ifdef MALLOC_TRACE
        UTMallocTrace("SDFNewCacheInit", FALSE, TRUE, FALSE, (void *) pc->pmem_start, 0);
    #endif // MALLOC_TRACE
    plat_free(pc->pmem_start);
}

static void *fastcc_alloc(SDFNewCache_t *pc, char *trace_tag, uint64_t size, int arena)
{
    int   x;
    void *p;

    if ((pc->size_limit - pc->mem_used) < size) {
        p = NULL;
    } else {
	plat_assert((((uint64_t) pc->pmem_used) % 8) == 0);
	p = pc->pmem_used;
	pc->pmem_used += size;

	/* keep pmem_used 8B aligned */
	/* xxxzzz is 8B alignment good enough? */
	x = (((uint64_t) pc->pmem_used) % 8);
	if (x != 0) {
	    pc->pmem_used += (8-x);
	}
	pc->mem_used += (pc->pmem_used - p);
    }

    #ifdef MALLOC_TRACE
        UTMallocTrace(trace_tag, TRUE, FALSE, FALSE, p, size);
    #endif // MALLOC_TRACE
    if (p == NULL) {
        plat_log_msg(21143, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
 		     "Not enough memory for cache.");
        plat_abort();
    }
    return(p);
}

static uint64_t check_nslabs(SDFNewCache_t *pc, uint64_t nslabs_in)
{
    uint64_t      npages;
    uint64_t      nslabs;
    uint64_t      max_nslabs;

    for (nslabs=nslabs_in; nslabs>0; nslabs--) {
	npages     = (pc->size_limit - pc->mem_used - nslabs*sizeof(SDFNewCacheSlab_t) + 8)/pc->page_size;
	max_nslabs = npages/ceil(((double) pc->max_obj_entry_size)/((double) pc->page_data_size));
	if (max_nslabs >= nslabs) {
	    break;
	}
    }
    if (nslabs == 0) {
        // There is insufficient memory for even a single slab!
        plat_log_msg(30576, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
 		     "There is insufficient memory for even a single slab!  "
		     "There must be at least enough memory to hold one max sized object.  "
		     "Try increasing the cache size (SDF_CC_MAXCACHESIZE).");
	plat_assert_always(0);
    }

    return(nslabs);
}


static void fastcc_init_object_alloc(SDFNewCache_t *pc)
{
    uint64_t             mem_slabs;
    void                *pmem_pages;
    void                *pnext_page;
    void                *ppage;
    int                  i, j;
    SDFNewCacheSlab_t   *ps;

    /*  Take remaining free cache memory and carve it into
     *  pages and distribute them among the slabs.
     */

    pc->pages_per_slab = (pc->size_limit - pc->mem_used)/pc->page_size/pc->nslabs;
    mem_slabs = pc->pages_per_slab*pc->page_size*pc->nslabs;
    pc->modsize_per_slab = pc->pages_per_slab*pc->page_data_size*pc->f_modified;

    if ((pc->pages_per_slab*pc->page_data_size) < pc->max_obj_entry_size) {
        plat_log_msg(21870, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
 		     "Not enough memory for cache: must have at least enough memory for a single maximum sized object per slab (%"PRIu64" required, %"PRIu64" available). Try increasing SDF_CC_MAXCACHESIZE or reducing SDF_CC_NSLABS", pc->max_obj_entry_size, pc->pages_per_slab*pc->page_data_size);
        plat_abort();
    }
    pmem_pages     = pc->pmem_used;
    pc->mem_used  += mem_slabs;
    pc->pmem_used += mem_slabs;

    pnext_page = pmem_pages;
    for (i=0; i<pc->nslabs; i++) {
        ps                   = &(pc->slabs[i]);
	ps->slabsize         = pc->pages_per_slab*pc->page_size;
	ps->free_pages       = NULL;
	for (j=0; j<pc->pages_per_slab; j++) {
	    ppage = pnext_page;
	    pnext_page += pc->page_size;
	    /* last 8B of a page holds the next page pointer */
	    *((void **) PAGE_POINTER(pc, ppage)) = ps->free_pages;
	    ps->free_pages = ppage;
	}
        init_transient_entry(ps);
    }
}

char *SDFNewCacheCopyKeyOutofObject(SDFNewCache_t *pc, char *pkey, SDFNewCacheEntry_t *pce)
{
    void      *p, *p_next;
    void      *pto;
    uint64_t   nbytes_left;
    uint64_t   n;

    // First skip header
    p = *((void **) PAGE_POINTER(pc, pce));

    pto         = pkey;
    nbytes_left = pce->key_len;  // this includes NULL termination!
    for (p = p; (p != NULL) && (nbytes_left > 0); p = p_next) {
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	memcpy(pto, (void *) (((char *) p)), n);
	nbytes_left -= n;
	pto += n;
    }

    return(pkey);
}

void SDFNewCacheCopyKeyIntoObject(SDFNewCache_t *pc, SDF_simple_key_t *pkey_simple, SDFNewCacheEntry_t *pce)
{
    void      *p, *p_next;
    void      *pfrom;
    uint64_t   nbytes_left;
    uint64_t   n;

    // First skip header
    p = *((void **) PAGE_POINTER(pc, pce));

    plat_assert(pkey_simple->len < pc->max_key_size);

    pfrom       = pkey_simple->key;
    nbytes_left = pkey_simple->len; // this includes NULL termination!
    for (p = p; (p != NULL) && (nbytes_left > 0); p = p_next) {
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	memcpy((void *) (((char *) p)), pfrom, n);
	nbytes_left -= n;
	pfrom += n;
    }
}

// Copy data out of linked list of pages
void SDFNewCacheCopyOutofObject(SDFNewCache_t *pc, void *pto_in, SDFNewCacheEntry_t *pce, uint64_t buf_size)
{
    void      *p, *p_next;
    void      *pto;
    uint64_t   nbytes_left;
    uint64_t   n = 0;
    uint64_t   size;

    size = pce->obj_size;
    if (size == 0) {
        return;
    }
    if (buf_size < pce->obj_size) {
        plat_assert(0);
    }
    pto = pto_in;

    // Skip header
    p = *((void **) PAGE_POINTER(pc, pce));

    // Skip key

    nbytes_left = pce->key_len;  // this includes NULL termination!
    for (p_next = p; (p != NULL) && (nbytes_left > 0);) {
        p = p_next;
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	nbytes_left -= n;
    }

    if (p != NULL) {
	if (n < pc->page_data_size) {
	    // Copy the data remnant out of the last page of the key
	    nbytes_left = pc->page_data_size - n;
	    if (nbytes_left > size) {
		nbytes_left = size;
	    } 
	    memcpy(pto, (void *) (((char *) p + n)), nbytes_left);
	    pto += nbytes_left;
	    size -= nbytes_left;
	}

	// Go to next page
        p = *((void **) PAGE_POINTER(pc, p));
    }

    nbytes_left = size;
    for (p = p; p != NULL; p = p_next) {
	plat_assert(nbytes_left > 0);
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	memcpy(pto, (void *) (((char *) p)), n);
	nbytes_left -= n;
	pto += n;
    }
    plat_assert(nbytes_left == 0);
}

// Copy data into linked list of pages
void SDFNewCacheCopyIntoObject(SDFNewCache_t *pc, void *pfrom_in, SDFNewCacheEntry_t *pce, uint64_t buf_size)
{
    void      *p, *p_next;
    void      *pfrom;
    uint64_t   nbytes_left;
    uint64_t   n = 0;
    uint64_t   size;

    size = pce->obj_size;
    if (size == 0) {
        return;
    }
    if (buf_size < pce->obj_size) {
        plat_assert(0);
    }
    pfrom = pfrom_in;

    // Skip header
    p = *((void **) PAGE_POINTER(pc, pce));

    // Skip key
    nbytes_left = pce->key_len;  // this includes NULL termination!
    for (p_next = p; (p != NULL) && (nbytes_left > 0);) {
        p = p_next;
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	nbytes_left -= n;
    }

    if (p != NULL) {
	if (n < pc->page_data_size) {
	    // Copy data into the remnant of space in the last page of the key
	    nbytes_left = pc->page_data_size - n;
	    if (nbytes_left > size) {
		nbytes_left = size;
	    } 
	    memcpy(p+n, pfrom, nbytes_left);
	    pfrom += nbytes_left;
	    size -= nbytes_left;
	}

	// Go to next page
	p = *((void **) PAGE_POINTER(pc, p));
    }

    nbytes_left = size;
    for (p = p; p != NULL; p = p_next) {
	plat_assert(nbytes_left > 0);
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(pc, p));
	if (nbytes_left >= pc->page_data_size) {
	    n = pc->page_data_size;
	} else {
	    n = nbytes_left;
	}
	memcpy((void *) (((char *) p)), pfrom, n);
	nbytes_left -= n;
	pfrom += n;
    }
    plat_assert(nbytes_left == 0);
}

void SDFNewCacheInit(SDFNewCache_t *pc, uint64_t nbuckets, uint64_t nslabs_in,
     uint64_t size, uint32_t max_key_size, uint64_t max_object_size,
     void (*init_state_fn)(SDFNewCacheEntry_t *pce, SDF_time_t curtime),
     int (*print_fn)(SDFNewCacheEntry_t *pce, char *sout, int max_len),
     void (*wrbk_fn)(SDFNewCacheEntry_t *pce, void *wrbk_arg),
     void (*flush_fn)(SDFNewCacheEntry_t *pce, void *flush_arg, SDF_boolean_t background_flush),
     uint32_t mod_state, uint32_t shared_state, uint32_t max_flushes_per_mod_check,
     double f_modified)
{
    uint64_t              i;
    uint64_t              nslabs;
    SDFNewCacheSlab_t    *ps;

    /*  The page is sized so that it can hold a single "entry"
     *  metadata structure, plus 8B for the next page pointer.
     *  The next page pointer is kept at the end of the page.
     */
    pc->page_size          = sizeof(SDFNewCacheEntry_t) + 8;
    pc->page_data_size     = pc->page_size - 8;
    pc->n_pending_requests = 0;

    pc->mod_state       = mod_state;
    pc->shared_state    = shared_state;
    pc->size_limit      = size;
    pc->nbuckets        = nbuckets;

    pc->max_flushes_per_mod_check = max_flushes_per_mod_check;
    pc->f_modified                = f_modified;

    if (init_state_fn == NULL) {
	plat_log_msg(30566, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "init_state_fn must be non-NULL!");
	plat_assert_always(0);
    }
    pc->init_state_fn   = init_state_fn;
    if (print_fn == NULL) {
	plat_log_msg(30568, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "print_fn must be non-NULL!");
	plat_assert_always(0);
    }
    pc->print_fn        = print_fn;
    if (wrbk_fn == NULL) {
	plat_log_msg(30574, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "wrbk_fn must be non-NULL!");
	plat_assert_always(0);
    }
    pc->wrbk_fn         = wrbk_fn;
    if (flush_fn == NULL) {
	plat_log_msg(30584, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "flush_fn must be non-NULL!");
	plat_assert_always(0);
    }
    pc->flush_fn        = flush_fn;
    // pc->cursize      = 0;
    pc->max_key_size    = max_key_size;

    #ifdef notdef
    if ((max_object_size %8) != 0) {
	plat_log_msg(21145, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "Maximum object size must be a multiple of 8B");
	plat_assert_always(0);
    }
    #endif

    pc->max_object_size = max_object_size;
    pc->max_obj_entry_size = sizeof(SDFNewCacheEntry_t) + pc->max_key_size + pc->max_object_size;

    if (pc->max_object_size < (sizeof(SDFNewCacheEntry_t) + pc->max_key_size)) {
	pc->max_object_size = (sizeof(SDFNewCacheEntry_t) + pc->max_key_size);
    }

    fastcc_init_alloc(pc);
   
    pc->buckets = fastcc_alloc(pc, "SDFNewCacheInit", nbuckets*(sizeof(SDFNewCacheBucket_t)), NonCacheObjectArena);

    nslabs = check_nslabs(pc, nslabs_in);
    if (nslabs != nslabs_in) {
	plat_log_msg(30560, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_INFO,
		     "nslabs had to be reduced from %"PRIu64" to %"PRIu64" so that each slab could hold at least one max sized object", nslabs_in, nslabs);
    }
    pc->slabs = fastcc_alloc(pc, "SDFNewCacheInit", nslabs*(sizeof(SDFNewCacheSlab_t)), NonCacheObjectArena);

    pc->nslabs           = nslabs;
    pc->buckets_per_slab = pc->nbuckets/pc->nslabs;
    if (pc->nbuckets % pc->nslabs) {
        (pc->buckets_per_slab)++;
    }

    for (i=0; i<nbuckets; i++) {
	pc->buckets[i].entry = NULL;
        ps                   = &(pc->slabs[i / pc->buckets_per_slab]);
	pc->buckets[i].slab  = ps;
	CacheInitLock(pc->buckets[i].lock);
    }

    for (i=0; i<nslabs; i++) {
        ps                     = &(pc->slabs[i]);
	ps->pc                 = pc;
        CacheInitLock(ps->lock);
	ps->lru_head           = NULL;
	ps->lru_tail           = NULL;
	ps->modlru_head        = NULL;
	ps->modlru_tail        = NULL;
	ps->free_request_structs = NULL;
        CacheInitLock(ps->free_request_struct_lock);
	ps->nobjects           = 0;
	ps->cursize            = 0;
	ps->cursize_w_keys     = 0;
	ps->n_mod              = 0;
	ps->modsize_w_keys     = 0;
	ps->n_mod_flushes      = 0;
	ps->n_mod_background_flushes = 0;
	ps->n_mod_recovery_enums = 0;
	fthMboxInit(&(ps->request_queue));
    }

    CacheInitLock(pc->enum_lock);
    CacheInitLock(pc->enum_flush_lock);
    pc->enum_bucket   = 0;

    fastcc_init_object_alloc(pc);

    /* The flush token pool will be loaded later when
     *  the pool of async write threads is initialized.
     */
    pc->n_flush_tokens  = 0;
    fthMboxInit(&(pc->flush_token_pool));

    /*  The background flush token pool will be loaded later when
     *  the pool of async write threads is initialized.
     */
    pc->n_background_flush_tokens  = 0;
    fthMboxInit(&(pc->background_flush_token_pool));
    /*  This is the amount of time that the background flush
     *  thread sleeps if it finds no dirty data in the cache.
     */
    pc->background_flush_sleep_msec = MIN_BACKGRND_FLUSH_SLEEP_MSEC;

    pc->background_flush_progress = 0;
    pc->n_background_flushes      = 0;
}

/*  SDFNewCacheSetFlushTokens assumes that the caller serializes
 *  this operation!
 */
void SDFNewCacheSetFlushTokens(SDFNewCache_t *pc, uint32_t ntokens)
{
    int i;

    if (ntokens < 1) {
       return;
    }
    if (pc->n_flush_tokens < ntokens) {
        // add some more tokens to the pool
	for (i=0; i<(ntokens - pc->n_flush_tokens); i++) {
	    fthMboxPost(&(pc->flush_token_pool), (uint64_t) 0);
	}
    } else if (pc->n_flush_tokens > ntokens) {
        // remove some tokens from the pool
	for (i=0; i<(pc->n_flush_tokens - ntokens); i++) {
	    (void) fthMboxWait(&(pc->flush_token_pool));
	}
    }
    pc->n_flush_tokens = ntokens;
}

/*  SDFNewCacheSetBackgroundFlushTokens assumes that the caller serializes
 *  this operation!
 *  Setting ntokens to 0 causes background flushing to stop completely.
 */
void SDFNewCacheSetBackgroundFlushTokens(SDFNewCache_t *pc, uint32_t ntokens, uint32_t sleep_msec)
{
    int i;

    if (sleep_msec >= MIN_BACKGRND_FLUSH_SLEEP_MSEC) {
	pc->background_flush_sleep_msec = sleep_msec;
    }

    if (pc->n_background_flush_tokens < ntokens) {
        // add some more tokens to the pool
	for (i=0; i<(ntokens - pc->n_background_flush_tokens); i++) {
	    fthMboxPost(&(pc->background_flush_token_pool), (uint64_t) 0);
	}
    } else if (pc->n_background_flush_tokens > ntokens) {
        // remove some tokens from the pool
	for (i=0; i<(pc->n_background_flush_tokens - ntokens); i++) {
	    (void) fthMboxWait(&(pc->background_flush_token_pool));
	}
    }
    pc->n_background_flush_tokens = ntokens;
}

/*  SDFNewCacheSetModifiedLimit assumes that the caller serializes
 *  this operation!
 */
void SDFNewCacheSetModifiedLimit(SDFNewCache_t *pc, double fmod)
{
    pc->f_modified = fmod;
    pc->modsize_per_slab = pc->pages_per_slab*pc->page_data_size*fmod;
}

void SDFNewCacheDestroy(SDFNewCache_t *pc)
{
    fastcc_destroy_alloc(pc);
}

void SDFNewCachePostRequest(SDFNewCache_t *pc, 
              SDF_app_request_type_t reqtype, SDF_cguid_t cguid, 
              SDF_simple_key_t *pkey, SDF_container_type_t ctype,
	      SDF_protocol_msg_t *pm)
{
    uint64_t                h, syndrome;
    SDFNewCacheBucket_t    *pb;
    SDFNewCacheSlab_t      *ps;
    SDFNewCacheRequest_t   *prqst;

    syndrome = hash((const unsigned char *) pkey->key, pkey->len, 0);
    h        = syndrome % pc->nbuckets;
    pb       = &(pc->buckets[h]);
    ps       = pb->slab;

    prqst          = get_request_struct(ps);
    prqst->reqtype = reqtype;
    prqst->key     = *pkey;
    prqst->cguid   = cguid;
    prqst->ctype   = ctype;
    prqst->pm      = pm;
    fthMboxPost(&(ps->request_queue), (uint64_t) prqst);
    __sync_fetch_and_add(&(pc->n_pending_requests), 1);
}

SDFNewCacheEntry_t *SDFNewCacheGetCreate(SDFNewCache_t *pc, SDF_cguid_t cguid, SDF_simple_key_t *pkey, SDF_container_type_t ctype, SDF_time_t curtime, SDF_boolean_t lock_slab, SDFNewCacheBucket_t **ppbucket, SDF_boolean_t try_lock, SDF_boolean_t *pnewflag, void *wrbk_arg)
{
    uint64_t              h, syndrome;
    SDFNewCacheEntry_t   *pce;
    SDFNewCacheSlab_t    *ps;
    SDFNewCacheBucket_t  *pb;
    CacheWaitType         lock_wait = NULL;
    SDF_simple_key_t      simple_key;

    *pnewflag = SDF_FALSE;

    syndrome = hash((const unsigned char *) pkey->key, pkey->len, 0);
    h = syndrome % pc->nbuckets;
    pb = &(pc->buckets[h]);
    *ppbucket = pb;
    ps = pb->slab;

    #ifdef DUMP_CACHE
	dump_bucket_list(pb, NULL, "GetCreate");
    #endif

    #ifdef notdef
    #ifdef CHECK_HEAP
	plat_log_msg(21150, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_WARN,
		     "================>   pslab:%p, key: '%s'", ps, pkey->key);
    #endif
    #endif

    if (lock_slab) {
        #ifdef CHECK_LOCKS
	    lock_trace("lock %d (%p)", (int) h, &(ps->lock));
	#endif
	if (try_lock) {
	    CacheLockTry(ps->lock, lock_wait);
	    if (lock_wait == NULL) {
	        return(NULL);
	    }
	    ps->lock_wait = lock_wait;
	} else {
	    CacheLock(ps->lock, ps->lock_wait);
	}

	/*  n_lockers counts the number of threads locked on this slab.
	 *  so that the slab is unlocked only when the memcached 
	 *  thread AND the asynchronous put thread are both done 
         *  their operations.
	 */
	ps->n_lockers = 1; 

	/*  Process pending remote requests for this slab before
	 *  doing anything else.
	 */
	process_pending_remote_requests(pc, ps, wrbk_arg);
    }

    for (pce = pb->entry; pce != NULL; pce = pce->next) {
	if ((pce->syndrome == syndrome) && 
	    (pce->key_len == pkey->len) && 
	    (pce->cguid == cguid) &&
	    (bcmp((const char *) SDFNewCacheCopyKeyOutofObject(pc, simple_key.key, pce), (const char *) pkey->key, pkey->len) == 0))
	{
	    break;
	}
    }

    if (pce != NULL) {
	/* update the LRU list */
	update_lru_list(ps, pce);
    } else {

        /* Use the transient entry. */

	*pnewflag = SDF_TRUE;
	pce = ps->tmp_entry;

	pce->cguid    = cguid;
	pce->obj_size = 0;
	pce->key_len  = pkey->len;
        SDFNewCacheCopyKeyIntoObject(pc, pkey, pce);
	pce->flags    = 0;
	if (ctype == SDF_OBJECT_CONTAINER) {
	    pce->flags |= CE_OBJ_CTNR;
	} else if (ctype == SDF_BLOCK_CONTAINER) {
	    pce->flags |= CE_BLK_CTNR;
	} else {
	    plat_log_msg(21151, 
			 PLAT_LOG_CAT_SDF_CC, 
			 PLAT_LOG_LEVEL_FATAL,
			 "Container type is neither OBJECT or BLOCK.");
	    plat_assert_always(0);
	}
	pce->syndrome = syndrome;
	pce->pbucket  = pb;

	// Set the initial state
	(*pc->init_state_fn)(pce, curtime);

	/* DON'T put myself on the bucket list yet! */
	pce->next = NULL;

	/* DON'T put myself on the LRU list yet! */
	pce->lru_next    = NULL;
	pce->lru_prev    = NULL;
	pce->modlru_next = NULL;
	pce->modlru_prev = NULL;

	#ifdef CHECK_CYCLES
	    check_for_cycle(pb);
	#endif
    }

    // Hold the slab lock (ps->lock) until we are done.
    // It will be unlocked with SDFNewUnlockBucket().
    return(pce);
}

static void add_lru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    if (ps->lru_head == NULL) {
	ps->lru_tail = pce;
    } else {
	ps->lru_head->lru_prev = pce;
    }
    pce->lru_next = ps->lru_head;
    ps->lru_head  = pce;
    pce->lru_prev = NULL;

    add_modlru_list(ps, pce);

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_lru_list(ps, "After add_lru_list");
    }
    #endif
}

static void add_modlru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    if ((pce == NULL) || (pce->state != ps->pc->mod_state)) {
        return;
    }

    if (ps->modlru_head == NULL) {
	ps->modlru_tail = pce;
    } else {
	ps->modlru_head->modlru_prev = pce;
    }
    pce->modlru_next = ps->modlru_head;
    ps->modlru_head  = pce;
    pce->modlru_prev = NULL;

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_modlru_list(ps, "After add_modlru_list");
    }
    #endif
}

static void update_lru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    if (ps->lru_head != pce) {

	if (ps->lru_tail == pce) {
	    ps->lru_tail = pce->lru_prev;
	}
	if (pce->lru_prev != NULL) {
	    pce->lru_prev->lru_next = pce->lru_next;
	}
	if (pce->lru_next != NULL) {
	    pce->lru_next->lru_prev = pce->lru_prev;
	}

	pce->lru_prev = NULL;
	pce->lru_next = ps->lru_head;
	ps->lru_head->lru_prev = pce;
	ps->lru_head = pce;

	#ifdef CHECK_HEAP
	if (SDFSelfTestCheckHeap) {
	    check_lru_list(ps, "After update_lru_list");
	}
	#endif
    }
    update_modlru_list(ps, pce);
}

static void update_modlru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    if ((pce == NULL) || (pce->state != ps->pc->mod_state)) {
        return;
    }

    if (ps->modlru_head != pce) {

	if (ps->modlru_tail == pce) {
	    ps->modlru_tail = pce->modlru_prev;
	}
	if (pce->modlru_prev != NULL) {
	    pce->modlru_prev->modlru_next = pce->modlru_next;
	}
	if (pce->modlru_next != NULL) {
	    pce->modlru_next->modlru_prev = pce->modlru_prev;
	}

	pce->modlru_prev = NULL;
	pce->modlru_next = ps->modlru_head;
	ps->modlru_head->modlru_prev = pce;
	ps->modlru_head = pce;

	#ifdef CHECK_HEAP
	if (SDFSelfTestCheckHeap) {
	    check_modlru_list(ps, "After update_modlru_list");
	}
	#endif
    }
}

static void lru_remove(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    /* remove an entry from the lru chain */

    if (pce == NULL) {
        return;
    }

    if (pce->lru_prev == NULL) {
       ps->lru_head = pce->lru_next;
    } else {
	pce->lru_prev->lru_next = pce->lru_next;
    }

    if (pce->lru_next == NULL) {
	ps->lru_tail = pce->lru_prev;
    } else {
	pce->lru_next->lru_prev = pce->lru_prev;
    }
    pce->lru_next = NULL;
    pce->lru_prev = NULL;

    modlru_remove(ps, pce);

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_lru_list(ps, "After lru_remove");
    }
    #endif
}

static void modlru_remove(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce)
{
    /* remove an entry from the mod lru chain */

    if ((pce == NULL) || (pce->state != ps->pc->mod_state)) {
        return;
    }

    if (pce->modlru_prev == NULL) {
       ps->modlru_head = pce->modlru_next;
    } else {
	pce->modlru_prev->modlru_next = pce->modlru_next;
    }

    if (pce->modlru_next == NULL) {
	ps->modlru_tail = pce->modlru_prev;
    } else {
	pce->modlru_next->modlru_prev = pce->modlru_prev;
    }
    pce->modlru_next = NULL;
    pce->modlru_prev = NULL;

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_modlru_list(ps, "After modlru_remove");
    }
    #endif
}

void SDFNewCacheAddLocker(SDFNewCacheBucket_t *pbucket)
{
    (void) __sync_fetch_and_add(&(pbucket->slab->n_lockers), 1);
}

void SDFNewUnlockBucket(SDFNewCache_t *pc, SDFNewCacheBucket_t *pbucket)
{
    int32_t   x;

    if ((x = __sync_fetch_and_add(&(pbucket->slab->n_lockers), -1)) == 1) {
	CacheUnlock(pbucket->slab->lock, pbucket->slab->lock_wait);
	#ifdef CHECK_LOCKS
	    lock_trace("unlock %p", &(pbucket->slab->lock));
	#endif
    }
}

uint64_t SDFNewCacheRemove(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, SDF_boolean_t wrbk_flag, void *wrbk_arg)
{
    SDFNewCacheEntry_t **ppce;
    SDFNewCacheSlab_t  *ps;
    uint64_t            npages_freed;

    /* this should only be called while we already hold a bucket lock! */

    ps = pce->pbucket->slab;
    if (pce == ps->tmp_entry) {
        /* This is a transient entry, so nothing has to be done. */
        return(0);
    }

    (ps->nobjects)--;
    ps->cursize -= pce->obj_size;
    ps->cursize_w_keys -= pce->obj_size;
    ps->cursize_w_keys -= pce->key_len;

    /* remove from the bucket list */
    for (ppce = &(pce->pbucket->entry); 
         (*ppce != NULL) && (*ppce != pce); 
	 ppce = &((*ppce)->next));
    if (*ppce == NULL) {
	plat_log_msg(21152, 
	             PLAT_LOG_CAT_SDF_CC, 
	             PLAT_LOG_LEVEL_FATAL,
		     "Internal inconsistency: "
		     "entry to be removed is not in bucket list.");
	plat_assert_always(0);
    } else {
	*ppce = pce->next;
    }

    // Update the counts of modified objects
    if (pce->state == pc->mod_state) {
        (ps->n_mod)--;
	(ps->modsize_w_keys) -= (pce->obj_size + pce->key_len);
    }

    // Remove from the LRU list
    lru_remove(ps, pce);

    /* Initiate an asynchronous writeback if:
     *   - wrbk_flag is true
     *   - the object is dirty
     */
    if (wrbk_flag && (pce->state == pc->mod_state)) {
	pc->wrbk_fn(pce, wrbk_arg);
    }

    // Free the pages
    npages_freed = fastcc_object_free(ps, (void *) pce);

    #ifdef CHECK_CYCLES
	check_for_cycle(pce->pbucket);
    #endif

    return(npages_freed);
}

void SDFNewCacheStartEnumeration(SDFNewCache_t *pc, SDF_cguid_t cguid)
{
    CacheLock(pc->enum_lock, pc->enum_lock_wait);
    pc->enum_bucket    = 0;
    pc->enum_cguid     = cguid;
}

void SDFNewCacheEndEnumeration(SDFNewCache_t *pc)
{
    CacheUnlock(pc->enum_lock, pc->enum_lock_wait);
}

SDF_boolean_t SDFNewCacheNextEnumeration(SDFNewCache_t *pc, SDF_cguid_t *pcguid, SDF_simple_key_t *pkey, SDF_container_type_t *pctype)
{
    uint64_t             i;
    int                  nslab;
    int                  nslab_locked;
    SDFNewCacheEntry_t  *pce = NULL;
    CacheWaitType        lock_wait = NULL;

    nslab_locked = pc->enum_bucket / pc->buckets_per_slab;
    CacheLock(pc->slabs[nslab_locked].lock, lock_wait);
    for (i=pc->enum_bucket; i<pc->nbuckets; i++) {
	nslab = i / pc->buckets_per_slab;
	if (nslab != nslab_locked) {
	    CacheUnlock(pc->slabs[nslab_locked].lock, lock_wait);
	    nslab_locked = nslab;
	    CacheLock(pc->slabs[nslab_locked].lock, lock_wait);
	}

	for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {
	    if (pce->flags & CE_ENUM) {
	        continue;
	    }
	    /*
	    ** We match on the following:
	    ** - Any cguid (pc->enum_cguid == 0)
	    ** - Specific cguid 
	    ** - Specific node 
	    */
	    if ((pc->enum_cguid == 0) || (pc->enum_cguid == pce->cguid)) {
	        break;
	    }
	}

	if (pce != NULL) {
	    /* I must load these while I hold the bucket lock */
	    *pcguid  = pce->cguid;
	    pkey->len = pce->key_len;
            SDFNewCacheCopyKeyOutofObject(pc, pkey->key, pce);
	    if (pce->flags & CE_OBJ_CTNR) {
		*pctype  = SDF_OBJECT_CONTAINER;
	    } else if (pce->flags & CE_BLK_CTNR) {
		*pctype  = SDF_BLOCK_CONTAINER;
	    } else {
		*pctype  = SDF_INVALID_CONTAINER;
	    }

	    pc->enum_bucket  = i;
	    pce->flags |= CE_ENUM;
	    break;
	} else {
	    /* clear the enum bits for this bucket */
	    for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {
		pce->flags &= (~CE_ENUM);
	    }
	}
    }
    CacheUnlock(pc->slabs[nslab_locked].lock, lock_wait);

    if (pce == NULL) {
       CacheUnlock(pc->enum_lock, pc->enum_lock_wait);
       return(SDF_FALSE);
    } else {
       return(SDF_TRUE);
    }
}

   /*  Start the background flush thread.
    *  The application must ensure that this is only done once!
    */
SDF_status_t SDFNewCacheStartBackgroundFlusher(SDFNewCache_t *pc, void *setup_arg, SDF_status_t (*setup_action_state)(void *setup_arg, void **pflush_arg))
{
    SDF_status_t    ret = SDF_SUCCESS;
    fthThread_t    *fth;

    pc->background_setup_arg = setup_arg;
    pc->background_setup_fn  = setup_action_state;

    fth = fthSpawn(&background_flusher, 40960);
    if (!fth) {
        ret = SDF_FAILURE;
    } else {
        fthResume(fth, (uint64_t) pc);
    }

    return(SDF_SUCCESS);
}

static void background_flusher(uint64_t arg)
{
    SDFNewCache_t   *pc;
    SDF_boolean_t    dirty_found;
    SDF_status_t     status;

    pc = (SDFNewCache_t *) arg;

    /*   This has to be done here so aio_ctxt can be setup
     *   correctly (it is assigned to this specific fthread).
     */
    status = pc->background_setup_fn(pc->background_setup_arg, &(pc->background_flush_arg));
    if (status != SDF_SUCCESS) {
	plat_log_msg(30619, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "background_flusher could not be started");
	plat_assert(0);
    }

    dirty_found = SDF_TRUE;
    fthNanoSleep(1000*pc->background_flush_sleep_msec);
    while (1) {
        if (!dirty_found) {
	    /*  No dirty was found the last time, so wait
	     *  for awhile before trying again.
	     */
	    fthNanoSleep(1000*pc->background_flush_sleep_msec);
	}
        SDFNewCacheFlushCache(pc, 
	                      0,                            /* cguid */
	                      SDF_TRUE,                     /* flush_flag */
	                      SDF_FALSE,                    /* inval_flag */
			      background_flush_progress_fn,
			      pc->background_flush_arg,
			      SDF_TRUE,                     /* background */
			      &dirty_found,
			      NULL,                         /* inval_prefix */
			      0);                           /* len_prefix */
	if (dirty_found) {
	    (pc->n_background_flushes)++;
	}
    }
}

static void background_flush_progress_fn(SDFNewCache_t *pc, void *flush_arg, uint32_t percent)
{
    pc->background_flush_progress = percent;
}

static SDF_boolean_t prefix_match(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, char *prefix, int len_prefix)
{
    SDF_status_t        ret = SDF_FALSE;
    SDF_simple_key_t    key;

    if (pce->key_len >= len_prefix) {
	SDFNewCacheCopyKeyOutofObject(pc, key.key, pce);
	if (memcmp(prefix, key.key, len_prefix) == 0) {
	    ret = SDF_TRUE;
	}
    }
    return(ret);
}

SDF_status_t SDFNewCacheFlushCache(SDFNewCache_t *pc,
				   SDF_cguid_t cguid,
				   SDF_boolean_t flush_flag,
				   SDF_boolean_t inval_flag,
				   void (*flush_progress_fn)(SDFNewCache_t *pc, void *wrbk_arg, uint32_t percent),
				   void *flush_arg,
				   SDF_boolean_t background_flush,
				   SDF_boolean_t *pdirty_found,
				   char *inval_prefix,
				   uint32_t len_prefix)
{
    uint64_t             i, j, nb;
    uint64_t             i_lock;
    uint32_t             k;
    int                  nslab;
    uint32_t             nflushed;
    SDFNewCacheEntry_t  *pce = NULL;
    SDFNewCacheSlab_t   *ps;
    SDF_boolean_t        dirty_found;
    SDF_boolean_t        objects_found;
    char                 slab_flags[100000];

    /*  Only do one bucket per slab at a time.
     *  Processing all of a slab's buckets at once
     *  would lock it out for a long time.
     */

    plat_assert(pc->nslabs <= 100000);
    memset((void *) slab_flags, 0, pc->nslabs);
    nb            = 0;
    dirty_found   = SDF_FALSE;
    objects_found = SDF_FALSE;
    flush_progress_fn(pc, flush_arg, 0);

    /*  First pass:
     *     - Process any pending remote requests.
     *     - Determine which slabs contain dirty data.
     *     - Do invalidations.
     */
    for (nslab=0; nslab<pc->nslabs; nslab++) {
	ps = &(pc->slabs[nslab]);
	i_lock = nslab*(pc->buckets_per_slab);
	if (i_lock >= pc->nbuckets) {
	    /* since nbuckets might not be an even multiple of nslabs! */
	    break;
	}
	CacheLock(ps->lock, ps->lock_wait);
	ps->n_lockers = 1; 

	/*  Make sure that any pending remote requests are processed.
	 *  This is necessary to ensure that any remote updates
	 *  prior to a remote flush make it to flash.
	 */
	process_pending_remote_requests(pc, ps, flush_arg);

        if (ps->nobjects > 0) {
	    objects_found = SDF_TRUE;
	}
        if (ps->n_mod > 0) {
	    dirty_found = SDF_TRUE;
	}
	if (inval_flag) {
	    if (ps->nobjects == 0) {
		/*  There are no objects in this slab.
		 *  Set the slab flag so we skip this slab
		 *  from now on.
		 */
		slab_flags[nslab] = 1;
	    } else {
	        /*  Invalidations are fast, so do the whole slab here.
		 */
		for (j=0; j<pc->buckets_per_slab; j++) {
		    i = nslab*(pc->buckets_per_slab) + j;
		    if (i >= pc->nbuckets) {
			/* since nbuckets might not be an even multiple of nslabs! */
			break;
		    }
		    for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {

			/*  Match on the following:
			 *  - Any cguid (cguid == 0)
			 *  - Specific cguid 
			 */

			if ((cguid == 0) || (cguid == pce->cguid)) {
			    if ((!inval_prefix) || 
				(prefix_match(pc, pce, inval_prefix, len_prefix))) 
			    {
				(void) SDFNewCacheRemove(pc, pce, SDF_FALSE /* wrbk_flag */, NULL);
			    }
			}
		    }
		}
	    }
	} else {
	    if (ps->n_mod == 0) {
		/*  There are no modified blocks in this slab.
		 *  Set the slab flag so we skip this slab
		 *  from now on.
		 */
		slab_flags[nslab] = 1;
	    }
	}
	SDFNewUnlockBucket(pc, &(pc->buckets[i_lock]));
    }

    if (inval_flag) {
	/*  inval is done */
	if ((!flush_flag) || (!dirty_found)) {
	    /*  Flushing is not required, or no dirty objects
	     *  were found.
	     */
	    flush_progress_fn(pc, flush_arg, 100);
	    *pdirty_found = dirty_found;
	    return(SDF_SUCCESS);
	}
    } else {
	if (!dirty_found) {
	    /*  There is nothing to flush! */
	    flush_progress_fn(pc, flush_arg, 100);
	    *pdirty_found = dirty_found;
	    return(SDF_SUCCESS);
	}
    }

    /*  Second pass: do the flush.
     */

    for (j=0; j<pc->buckets_per_slab; j++) {
	for (nslab=0; nslab<pc->nslabs; nslab++) {

	    if (slab_flags[nslab]) {
		/*  There are no modified objects in this slab.
		 */
		continue;
	    }
	    
	    i = nslab*(pc->buckets_per_slab) + j;

	    if (i >= pc->nbuckets) {
	        /* since nbuckets might not be an even multiple of nslabs! */
	        continue;
	    }

	    /*  Make sure I have the necessary tokens before
	     *  I hold any locks.
	     */
	    if (background_flush) {
		/*  Must get a background flush token before
		 *  proceeding.
		 */
		(void) fthMboxWait(&(pc->background_flush_token_pool));
		// plat_log_msg(30622, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Wait");
	    }
	    // Must get a flush token first (for flow control).
	    (void) fthMboxWait(&(pc->flush_token_pool));

	    ps = &(pc->slabs[nslab]);
	    CacheLock(ps->lock, ps->lock_wait);
	    if (inval_flag) {
		if (ps->nobjects == 0) {
		    /*  There are no objects in this slab.
		     *  Set the a slab flag so we skip this slab
		     *  from now on.
		     */
		    slab_flags[nslab] = 1;
		    CacheUnlock(ps->lock, ps->lock_wait);

		    /* free the token */
		    fthMboxPost(&(pc->flush_token_pool), (uint64_t) 0);
		    if (background_flush) {
			fthMboxPost(&(pc->background_flush_token_pool), (uint64_t) 0);
			// plat_log_msg(30623, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Post");
		    }
		    continue;
		}
	    } else {
		if (ps->n_mod == 0) {
		    /*  There are no modified objects in this slab.
		     *  Set the a slab flag so we skip this slab
		     *  from now on.
		     */
		    slab_flags[nslab] = 1;
		    CacheUnlock(ps->lock, ps->lock_wait);

		    /* free the token */
		    fthMboxPost(&(pc->flush_token_pool), (uint64_t) 0);
		    if (background_flush) {
			fthMboxPost(&(pc->background_flush_token_pool), (uint64_t) 0);
			// plat_log_msg(30623, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Post");
		    }
		    continue;
		}
	    }
	    ps->n_lockers = 1; 

	    nflushed = 0;
	    for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {

		/*  Match on the following:
		 *  - Any cguid (cguid == 0)
		 *  - Specific cguid 
		 */

		if ((cguid == 0) || (cguid == pce->cguid)) {
		    if (flush_flag && (pce->state == pc->mod_state)) {

			// flush this entry

			if (background_flush) {
			    (ps->n_mod_background_flushes)++;
			} else {
			    (ps->n_mod_flushes)++;
			}
			nflushed++;
			pc->flush_fn(pce, flush_arg, background_flush);
			// Set the state to unmodified to avoid unnecessary writebacks!
			// Update counts of modified objects/bytes
			SDFNewCacheSubtractModObjectStats(pce);
			pce->state = pc->shared_state;
		    }
		    if (inval_flag) {
		        if ((!inval_prefix) || 
			    (prefix_match(pc, pce, inval_prefix, len_prefix))) 
			{
			    (void) SDFNewCacheRemove(pc, pce, SDF_FALSE /* wrbk_flag */, NULL);
			}
		    }
		}
	    }
	    SDFNewUnlockBucket(pc, &(pc->buckets[i]));
	    nb++;
	    flush_progress_fn(pc, flush_arg, 100*nb/pc->nbuckets);

            for (k=1; k<nflushed; k++) {
		/*  Remove extra tokens posted above.
		 */
		if (background_flush) {
		    (void) fthMboxWait(&(pc->background_flush_token_pool));
		    // plat_log_msg(30622, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Wait");
		}
		(void) fthMboxWait(&(pc->flush_token_pool));
	    }

            if (nflushed == 0) {
		/* nothing was flushed, so free the primary token */
		fthMboxPost(&(pc->flush_token_pool), (uint64_t) 0);
		if (background_flush) {
		    fthMboxPost(&(pc->background_flush_token_pool), (uint64_t) 0);
		    // plat_log_msg(30623, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Post");
		}
	    }
	}
    }
    flush_progress_fn(pc, flush_arg, 100);

    *pdirty_found = dirty_found;
    return(SDF_SUCCESS);
}

void SDFNewCacheFlushComplete(SDFNewCache_t *pc, SDF_boolean_t background_flush)
{
    fthMboxPost(&(pc->flush_token_pool), (uint64_t) 0);
    if (background_flush) {
	fthMboxPost(&(pc->background_flush_token_pool), (uint64_t) 0);
	// plat_log_msg(30623, PLAT_LOG_CAT_SDF_CC, PLAT_LOG_LEVEL_FATAL, "Post");
    }
}

SDF_status_t SDFNewCacheGetModifiedObjects(SDFNewCache_t *pc, SDF_cache_enum_t *pes)
{
    int                       ret = SDF_OBJECT_UNKNOWN;
    uint64_t                  i;
    uint64_t                  n_bucket;
    int                       nslab;
    SDFNewCacheEntry_t       *pce = NULL;
    CacheWaitType             lock_wait = NULL;
    SDFNewCacheSlab_t        *ps;
    void                     *ptr;

    n_bucket = pes->enum_index1;

    // Continue an ongoing enumeration of modified objects
    for (i=n_bucket; i<pc->nbuckets; i++) {
	nslab = i / pc->buckets_per_slab;
	ps = &(pc->slabs[nslab]);
	CacheLock(ps->lock, lock_wait);

	for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {
	    if (pce->flags & CE_ENUM_RECOVERY) {
		continue;
	    }
	    if (pes->cguid == pce->cguid) {
		if (pce->state == pc->mod_state) {
		    // copy this object into a provided buffer
		    (ps->n_mod_recovery_enums)++;

		    pes->key_len     = pce->key_len - 1;
		    pes->data_len    = pce->obj_size;
		    pes->create_time = pce->createtime;
		    pes->expiry_time = pce->exptime;
		    ptr = pes->fill(pes);

		    if (!ptr) {
			// buffer is full, so return
			ret = SDF_SUCCESS;
			pes->enum_index1 = i;
			break;
		    } else {
			SDFNewCacheCopyKeyOutofObject(pc, ptr, pce);
			SDFNewCacheCopyOutofObject(pc, ptr+pce->key_len-1, pce, pce->obj_size);
		    }
		}
	    }
	    pce->flags |= CE_ENUM_RECOVERY;
	}

        if (pce == NULL) {
	    /* clear the enum bits for this bucket */
	    for (pce = pc->buckets[i].entry; pce != NULL; pce = pce->next) {
		pce->flags &= (~CE_ENUM_RECOVERY);
	    }
	    pes->enum_index1 = i + 1;
	    CacheUnlock(ps->lock, lock_wait);
	} else {
	    CacheUnlock(ps->lock, lock_wait);
	    break;
	}
    }

    return(ret);
}

SDF_status_t SDFNewCacheGetModifiedObjectsCleanup(SDFNewCache_t *pc, SDF_cache_enum_t *pes)
{
    int                       ret = SDF_SUCCESS;
    uint64_t                  n_bucket;
    int                       nslab;
    SDFNewCacheSlab_t        *ps;
    CacheWaitType             lock_wait = NULL;
    SDFNewCacheEntry_t       *pce = NULL;

    n_bucket = pes->enum_index1;
    nslab = n_bucket / pc->buckets_per_slab;
    ps = &(pc->slabs[nslab]);
    CacheLock(ps->lock, lock_wait);

    /* clear the enum bits for this bucket */
    for (pce = pc->buckets[n_bucket].entry; pce != NULL; pce = pce->next) {
	pce->flags &= (~CE_ENUM_RECOVERY);
    }
    CacheUnlock(ps->lock, lock_wait);
    return(ret);
}

void SDFNewCachePrintAll(FILE *f, SDFNewCache_t *pc)
{
    uint64_t          i;
    SDFNewCacheBucket_t *pb;
    SDFNewCacheEntry_t  *pce;
    CacheWaitType     lock_wait = NULL;
    char              s[MAX_PRINT_LEN];

    for (i = 0; i < pc->nbuckets; i++) {
	pb = &(pc->buckets[i]);

	CacheLock(pb->slab->lock, lock_wait);
	for (pce = pb->entry; pce != NULL; pce = pce->next) {
	    (pc->print_fn)(pce, s, MAX_PRINT_LEN);
	    (void) fprintf(f, "%s\n", s);
	}
	CacheUnlock(pb->slab->lock, lock_wait);
    }
}

void SDFNewCacheGetStats(SDFNewCache_t *pc, SDFNewCacheStats_t *pstat)
{
    uint64_t          i;
    SDFNewCacheSlab_t   *ps;

    pstat->num_objects     = 0;
    pstat->cursize         = 0;
    pstat->cursize_w_keys  = 0;
    pstat->n_mod           = 0;
    pstat->modsize_w_keys  = 0;
    pstat->n_mod_flushes   = 0;
    pstat->n_mod_background_flushes    = 0;
    pstat->n_mod_recovery_enums        = 0;
    pstat->n_pending_requests          = pc->n_pending_requests;
    pstat->background_flush_progress   = pc->background_flush_progress;
    pstat->n_background_flushes        = pc->n_background_flushes;
    pstat->n_flush_tokens              = pc->n_flush_tokens;
    pstat->n_background_flush_tokens   = pc->n_background_flush_tokens;
    pstat->background_flush_sleep_msec = pc->background_flush_sleep_msec;
    pstat->mod_percent                 = pc->f_modified*100;

    for (i = 0; i < pc->nslabs; i++) {
        ps = &(pc->slabs[i]);
	pstat->num_objects     += ps->nobjects;
	pstat->cursize         += ps->cursize;
	pstat->cursize_w_keys  += ps->cursize_w_keys;
	pstat->n_mod           += ps->n_mod;
	pstat->modsize_w_keys  += ps->modsize_w_keys;
	pstat->n_mod_flushes   += ps->n_mod_flushes;
	pstat->n_mod_background_flushes += ps->n_mod_background_flushes;
	pstat->n_mod_recovery_enums     += ps->n_mod_recovery_enums;
    }
}

static SDFNewCacheRequest_t *get_request_struct(SDFNewCacheSlab_t *ps)
{
    int                  i;
    SDFNewCacheRequest_t  *pinv;
    CacheWaitType        lock_wait = NULL;

    /*  I cannot use the normal slab lock here because a remote access
     *  needs to get a request struct because the slab is already
     *  locked!
     */
    CacheLock(ps->free_request_struct_lock, lock_wait);
    if (ps->free_request_structs == NULL) {
	ps->free_request_structs = proto_plat_alloc_arena(N_ALLOC_REQUEST_STRUCTS*(sizeof(SDFNewCacheRequest_t)), CacheObjectArena);
	#ifdef MALLOC_TRACE
	    UTMallocTrace("get_request_struct", TRUE, FALSE, FALSE, (void *) ps->free_request_structs, N_ALLOC_REQUEST_STRUCTS*sizeof(SDFNewCacheRequest_t));
	#endif // MALLOC_TRACE
	if (ps->free_request_structs == NULL) {
	    plat_log_msg(21106, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
			 "Not enough memory, plat_alloc() failed.");
	    plat_abort();
	}

	for (i=0; i<N_ALLOC_REQUEST_STRUCTS; i++) {
	    pinv = &(ps->free_request_structs[i]);
	    if (i < (N_ALLOC_REQUEST_STRUCTS - 1)) {
		pinv->next = &(ps->free_request_structs[i+1]);
	    } else {
		pinv->next = NULL;
	    }
	}
    }
    pinv = ps->free_request_structs;
    ps->free_request_structs = pinv->next;
    CacheUnlock(ps->free_request_struct_lock, lock_wait);
    return(pinv);
}

static void free_request_struct(SDFNewCacheSlab_t *ps, SDFNewCacheRequest_t *pinv)
{
    CacheWaitType        lock_wait = NULL;

    CacheLock(ps->free_request_struct_lock, lock_wait);
    pinv->next = ps->free_request_structs;
    ps->free_request_structs = pinv;
    CacheUnlock(ps->free_request_struct_lock, lock_wait);
}

static void process_pending_remote_requests(SDFNewCache_t *pc, SDFNewCacheSlab_t *ps, void *wrbk_arg)
{
    SDFNewCacheEntry_t     *pce;
    SDFNewCacheEntry_t     *pce_new;
    SDFNewCacheBucket_t    *pb;
    SDFNewCacheRequest_t   *preq;
    SDF_boolean_t           new_entry_flag;

    while ((preq = (SDFNewCacheRequest_t *) fthMboxTry(&(ps->request_queue)))) {
	__sync_fetch_and_add(&(pc->n_pending_requests), -1);

	switch (preq->reqtype) {
	case APRIV:
	    pce = SDFNewCacheGetCreate(pc, preq->cguid, &(preq->key), 
				       preq->ctype, 0,
				       SDF_FALSE /* lock the slab? */,
				       &pb, SDF_FALSE /* try flag */,
				       &new_entry_flag, wrbk_arg);
	    /*  Note: SDFNewCacheRemove correctly handles the case when pce is
	     *  a transient entry newly created by the call to
	     *  SDFNewCacheGetCreate above.
	     */
	    (void) SDFNewCacheRemove(pc, pce, SDF_FALSE /* wrbk_flag */, wrbk_arg);
	    break;

	case APRFL:
	case APRFI:
	    pce = SDFNewCacheGetCreate(pc, preq->cguid, &(preq->key), 
				       preq->ctype, 0,
				       SDF_FALSE /* lock the slab? */,
				       &pb, SDF_FALSE /* try flag */,
				       &new_entry_flag, wrbk_arg);

	    if (pce->state == pc->mod_state) {

	        /*  Flush the dirty data.
		 *  We use the "wrbk_fn" here because this is not a
		 *  bulk flush, so I need to add a locker for this
		 *  slab.
		 */
		pc->wrbk_fn(pce, wrbk_arg);
		// Update the counts of modified objects
		SDFNewCacheSubtractModObjectStats(pce);
		pce->state = pc->shared_state;
	    }

	    if (preq->reqtype == APRFI) {

	        /* The object must also be invalidated.
                 *
		 *  Note: SDFNewCacheRemove correctly handles the case when pce
		 *  is a transient entry newly created by the call to
		 *  SDFNewCacheGetCreate above.
		 */
		(void) SDFNewCacheRemove(pc, pce, SDF_FALSE /* wrbk_flag */, wrbk_arg);
	    }
	    break;

	case APRUP:
	    pce = SDFNewCacheGetCreate(pc, preq->cguid, &(preq->key), 
				       preq->ctype, preq->pm->createtime,
				       SDF_FALSE /* lock the slab? */,
				       &pb, SDF_FALSE /* try flag */,
				       &new_entry_flag, wrbk_arg);

	    if (!new_entry_flag) {
		pce_new = SDFNewCacheOverwriteCacheObject(pc, pce, preq->pm->data_size, wrbk_arg);
	    } else {
		pce_new = SDFNewCacheCreateCacheObject(pc, pce, preq->pm->data_size, wrbk_arg);
	    }
	    SDFNewCacheCopyIntoObject(pc, (char *) (preq->pm) + sizeof(SDF_protocol_msg_t), pce_new, preq->pm->data_size);

            /* Update modified states if the object is transitioning
	     * from shared to modified.
	     */
	    if (pce_new->state != pc->mod_state) {
		pce_new->state = pc->mod_state;
		SDFNewCacheAddModObjectStats(pce_new);
	    }

	    /*  Set the cache state (otherwise the update might not get
	     *  written to flash).
	     */

	    pce_new->createtime     = preq->pm->curtime;
	    pce_new->exptime        = preq->pm->exptime;

	    /*  Don't forget to free the message!
	     */
	    plat_free(preq->pm);
	    #ifdef MALLOC_TRACE
	        UTMallocTrace("pending_msg", SDF_FALSE, SDF_TRUE, SDF_FALSE, (void *) preq->pm, 0);
	    #endif // MALLOC_TRACE
	    break;

	default:
	    plat_log_msg(30570, 
			 PLAT_LOG_CAT_SDF_CC, 
			 PLAT_LOG_LEVEL_FATAL,
			 "Unknown request type '%d' found on remote request queue!", preq->reqtype);
	    plat_assert(0); // xxxzzz remove me!
	    break;
	}
	free_request_struct(ps, preq);
    }
}

void SDFNewCacheCheckHeap(SDFNewCache_t *pc, SDFNewCacheBucket_t *pbucket)
{
    check_heap(pbucket->slab);
}

static void check_heap(SDFNewCacheSlab_t *ps)
{
    SDFNewCacheEntry_t    *pce;
    uint64_t               entry_size;
    uint64_t               npages_used;
    uint64_t               npages_free;
    SDFNewCache_t         *pc;
    void                  *p;
    void                  *p_next;
    uint32_t               n;
    uint32_t               npages;
    char                   stmp[1024];

    pc = ps->pc;

    // check that all used pages are consistent

    npages_used = 0;
    for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
        
	entry_size = sizeof(SDFNewCacheEntry_t) + pce->key_len + pce->obj_size;
	npages = (entry_size + pc->page_data_size - 1)/pc->page_data_size;
	npages_used += npages;

        n = 0;
        for (p = pce; p != NULL; p = p_next) {
	    n++;
	    p_next = *((void **) PAGE_POINTER(pc, p));
	}
	if (n != npages) {
	    sprintf(stmp, "String of pages for pce=%p is not correct length: %d expected, %d found.\n", pce, npages, n);
	    oh_oh(stmp);
	}

  
        #ifdef notdef

	/* This check removed because it won't work for arbitrary
	 *  binary keys.
	 */

	// Check that key is null terminated in the right place.
	SDFNewCacheCopyKeyOutofObject(pc, key.key, pce);
	key.len = pce->key_len;
	if ((n=(1+strlen(key.key))) != pce->key_len) {
	    sprintf(stmp, "Key for pce=%p is not correct length ('%s'): %d expected, %d found.\n", pce, key.key, pce->key_len, n);
	    oh_oh(stmp);
	}
	#endif
    }

    // count the free pages
    npages_free = 0;
    for (p = ps->free_pages; p != NULL; p = p_next) {
	npages_free++;
	p_next = *((void **) PAGE_POINTER(pc, p));
    }
    if ((npages_free + npages_used) != pc->pages_per_slab) {
	sprintf(stmp, "Page counts for ps=%p are not consistent: %"PRIu64" used pages plus %"PRIu64" free pages is not equal to %"PRIu64" pages per slab!\n", ps, npages_used, npages_free, pc->pages_per_slab);
	oh_oh(stmp);
    }

    // check LRU list

    check_lru_list(ps, "check_heap");
}

static void dump_lru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce_bad, char *s)
{
    SDFNewCacheEntry_t *pce;
    uint64_t            obytes, sbytes;
    SDF_simple_key_t    key;

    fprintf(stderr, "------------------------------------------------------------\n");
    fprintf(stderr, "head: %p, tail: %p\n", ps->lru_head, ps->lru_tail);
    sbytes = 0;
    for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
        obytes = sizeof(SDFNewCacheEntry_t) + pce->key_len + pce->obj_size;
	sbytes += obytes;
	SDFNewCacheCopyKeyOutofObject(ps->pc, key.key, pce);
	key.len = pce->key_len;
        fprintf(stderr, "pce: %p, bucket: %p, prev: %p, next: %p, flags: %d, state: %d, keylen: %d, key: '%s', obj_size: %d, bytes: %"PRIu64", sum_bytes: %"PRIu64"", pce, pce->pbucket, pce->lru_prev, pce->lru_next, pce->flags, pce->state, pce->key_len, key.key, pce->obj_size, obytes, sbytes);
        if (pce == pce_bad) {
	    fprintf(stderr, "  <---------\n");
	    break;
	} else {
	    fprintf(stderr, "\n");
	}
    }
    if (s != NULL) {
	if (pce_bad != NULL) {
	    oh_oh(s);
	} else {
	    fprintf(stderr, "%s\n", s);
	}
    }
    fprintf(stderr, "------------------------------------------------------------\n");
}

static void dump_modlru_list(SDFNewCacheSlab_t *ps, SDFNewCacheEntry_t *pce_bad, char *s)
{
    SDFNewCacheEntry_t *pce;
    uint64_t            obytes, sbytes;
    SDF_simple_key_t    key;

    fprintf(stderr, "------------   START MOD LRU LIST   ----------------------------\n");
    fprintf(stderr, "head: %p, tail: %p\n", ps->modlru_head, ps->modlru_tail);
    sbytes = 0;
    for (pce = ps->modlru_head; pce != NULL; pce = pce->modlru_next) {
        obytes = sizeof(SDFNewCacheEntry_t) + pce->key_len + pce->obj_size;
	sbytes += obytes;
	SDFNewCacheCopyKeyOutofObject(ps->pc, key.key, pce);
	key.len = pce->key_len;
        fprintf(stderr, "pce: %p, bucket: %p, mprev: %p, mnext: %p, flags: %d, state: %d, keylen: %d, key: '%s', obj_size: %d, bytes: %"PRIu64", sum_bytes: %"PRIu64"", pce, pce->pbucket, pce->modlru_prev, pce->modlru_next, pce->flags, pce->state, pce->key_len, key.key, pce->obj_size, obytes, sbytes);
        if (pce == pce_bad) {
	    fprintf(stderr, "  <---------\n");
	    break;
	} else {
	    fprintf(stderr, "\n");
	}
    }
    if (s != NULL) {
	if (pce_bad != NULL) {
	    oh_oh(s);
	} else {
	    fprintf(stderr, "%s\n", s);
	}
    }
    fprintf(stderr, "------------   END MOD LRU LIST   ------------------------------\n");
}

static void check_lru_list(SDFNewCacheSlab_t *ps, char *msg)
{
    SDFNewCacheEntry_t *pce, *pce2, *pce_last;
    uint64_t            obytes, kbytes, nobjs;
    uint64_t            mobytes, mkbytes, mnobjs;
    char                stmp[1024];

    /* check LRU list */

    for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
        if (pce->state == ps->pc->mod_state) {
	    if (pce->modlru_next == NULL) {
	        if (ps->modlru_tail != pce) {
		    dump_lru_list(ps, pce, "modified object in LRU list is not on MOD LRU list");
		}
	    } else if (pce->modlru_prev == NULL) {
	        if (ps->modlru_head != pce) {
		    dump_lru_list(ps, pce, "modified object in LRU list is not on MOD LRU list");
		}
	    }
	} else {
	    if ((pce->modlru_next != NULL) || (pce->modlru_prev != NULL)) {
		dump_lru_list(ps, pce, "unmodified object in LRU list is on MOD LRU list");
	    }
	}

	for (pce2 = ps->lru_head; pce2 != NULL; pce2 = pce2->lru_next) {
	    if (pce->lru_next == pce2) {
		dump_lru_list(ps, pce, "cycle in forward LRU list");
		goto cont3;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont3:

    for (pce = ps->lru_tail; pce != NULL; pce = pce->lru_prev) {
	for (pce2 = ps->lru_tail; pce2 != NULL; pce2 = pce2->lru_prev) {
	    if (pce->lru_prev == pce2) {
		dump_lru_list(ps, pce, "cycle in backward LRU list");
		goto cont4;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont4:

    /* check that forward and backward LRU lists are consistent */

    pce_last = NULL;
    for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
        
	if (pce->lru_prev != pce_last) {
	    dump_lru_list(ps, pce, "inconsistent forward and backward LRU lists: lru_prev");
	    break;
	}
	pce_last = pce;
    }

    /* check size and object count*/
    kbytes  = 0;
    obytes  = 0;
    nobjs   = 0;
    mkbytes = 0;
    mobytes = 0;
    mnobjs  = 0;
    for (pce = ps->lru_head; pce != NULL; pce = pce->lru_next) {
        kbytes += pce->key_len;
        obytes += pce->obj_size;
	nobjs++;
	if (pce->state == ps->pc->mod_state) {
	    mkbytes += pce->key_len;
	    mobytes += pce->obj_size;
	    mnobjs++;
	}
    }
    if (obytes != ps->cursize) {
        (void) sprintf(stmp, "Sum of LRU object bytes (%"PRIu64") is not equal to ps->cursize (%"PRIu64")!\n", obytes, ps->cursize);
	dump_lru_list(ps, pce, "Inconsistent cursize");
	oh_oh(stmp);
    }
    if ((obytes + kbytes) != ps->cursize_w_keys) {
        (void) sprintf(stmp, "Sum of LRU object and key bytes (%"PRIu64") is not equal to ps->cursize_w_keys (%"PRIu64")!\n", obytes + kbytes, ps->cursize_w_keys);
	dump_lru_list(ps, pce, "Inconsistent cursize_w_keys");
	oh_oh(stmp);
    }
    if (nobjs != ps->nobjects) {
        (void) sprintf(stmp, "Number of LRU objects (%"PRIu64") is not equal to ps->nobjects (%"PRIu64")!\n", nobjs, ps->nobjects);
	dump_lru_list(ps, pce, "Inconsistent nobjects");
	oh_oh(stmp);
    }

    if ((mobytes + mkbytes) != ps->modsize_w_keys) {
        (void) sprintf(stmp, "Sum of modified LRU object and key bytes (%"PRIu64") is not equal to ps->modsize_w_keys (%"PRIu64")!\n", mobytes + mkbytes, ps->modsize_w_keys);
	dump_lru_list(ps, pce, "Inconsistent modsize_w_keys");
	oh_oh(stmp);
    }
    if (mnobjs != ps->n_mod) {
        (void) sprintf(stmp, "Number of modified LRU objects (%"PRIu64") is not equal to ps->n_mod (%"PRIu64")!\n", mnobjs, ps->n_mod);
	dump_lru_list(ps, pce, "Inconsistent n_mod");
	oh_oh(stmp);
    }

    dump_lru_list(ps, NULL, msg);
}

static void check_modlru_list(SDFNewCacheSlab_t *ps, char *msg)
{
    SDFNewCacheEntry_t *pce, *pce2, *pce_last;
    uint64_t            nobjs;
    char                stmp[1024];

    /* check LRU list */

    for (pce = ps->modlru_head; pce != NULL; pce = pce->modlru_next) {
        if (pce->state != ps->pc->mod_state) {
	    dump_modlru_list(ps, pce, "non-Modified object in forward mod LRU list");
	}
	for (pce2 = ps->modlru_head; pce2 != NULL; pce2 = pce2->modlru_next) {
	    if (pce->modlru_next == pce2) {
		dump_modlru_list(ps, pce, "cycle in forward mod LRU list");
		goto cont3;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont3:

    for (pce = ps->modlru_tail; pce != NULL; pce = pce->modlru_prev) {
	for (pce2 = ps->modlru_tail; pce2 != NULL; pce2 = pce2->modlru_prev) {
	    if (pce->modlru_prev == pce2) {
		dump_modlru_list(ps, pce, "cycle in backward mod LRU list");
		goto cont4;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont4:

    /* check that forward and backward LRU lists are consistent */

    pce_last = NULL;
    for (pce = ps->modlru_head; pce != NULL; pce = pce->modlru_next) {
        
	if (pce->modlru_prev != pce_last) {
	    dump_modlru_list(ps, pce, "inconsistent forward and backward mod LRU lists: modlru_prev");
	    break;
	}
	pce_last = pce;
    }

    /* check size and object count*/
    nobjs   = 0;
    for (pce = ps->modlru_head; pce != NULL; pce = pce->modlru_next) {
	nobjs++;
    }
    if (nobjs != ps->n_mod) {
        (void) sprintf(stmp, "Number of modified LRU objects (%"PRIu64") is not equal to ps->n_mod (%"PRIu64")!\n", nobjs, ps->n_mod);
	dump_modlru_list(ps, pce, "Inconsistent n_mod");
	oh_oh(stmp);
    }

    dump_modlru_list(ps, NULL, msg);
}

#ifdef notdef
static void dump_lists(SDFNewCacheBucket_t *pb)
{
    fprintf(stderr, "bucket list: ");
    dump_bucket_list(pb, NULL, NULL);
    fprintf(stderr, "LRU list: ");
    dump_lru_list(pb, NULL, NULL);
}
#endif

static void __attribute__((unused)) check_for_cycle(SDFNewCacheBucket_t *pb)
{
    SDFNewCacheEntry_t *pce, *pce2, *pce_last;

    /* check bucket list */

    for (pce = pb->entry; pce != NULL; pce = pce->next) {
	if (pce->pbucket != pb) {
	    dump_lru_list(pb->slab, pce, "bucket list entry with foreign bucket pointer!");
	    break;
	}
	for (pce2 = pb->entry; pce2 != NULL; pce2 = pce2->next) {
	    if (pce->next == pce2) {
		dump_bucket_list(pb, pce, "cycle in bucket list");
		goto cont2;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }

cont2:

    /* check LRU list */

    for (pce = pb->slab->lru_head; pce != NULL; pce = pce->lru_next) {
	if (pce->pbucket != pb) {
	    dump_lru_list(pb->slab, pce, "lru entry with foreign bucket pointer!");
	    break;
	}
	for (pce2 = pb->slab->lru_head; pce2 != NULL; pce2 = pce2->lru_next) {
	    if (pce->lru_next == pce2) {
		dump_lru_list(pb->slab, pce, "cycle in forward LRU list");
		goto cont3;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont3:

    for (pce = pb->slab->lru_tail; pce != NULL; pce = pce->lru_prev) {
	for (pce2 = pb->slab->lru_tail; pce2 != NULL; pce2 = pce2->lru_prev) {
	    if (pce->lru_prev == pce2) {
		dump_lru_list(pb->slab, pce, "cycle in backward LRU list");
		goto cont4;
	    }
	    if (pce == pce2) {
	        break;
	    }
	}
    }
cont4:

    /* check that forward and backward LRU lists are consistent */

    
    pce_last = NULL;
    for (pce = pb->slab->lru_head; pce != NULL; pce = pce->lru_next) {
        
	if (pce->lru_prev != pce_last) {
	    dump_lru_list(pb->slab, pce, "inconsistent forward and backward LRU lists: lru_prev");
	    break;
	}
	pce_last = pce;
    }
}

static void oh_oh(char *s)
{
    fprintf(stderr, "%s!\n", s);
    plat_assert(0);
}

static void __attribute__((unused)) dump_bucket_list(SDFNewCacheBucket_t *pb, SDFNewCacheEntry_t *pce_bad, char *s)
{
    SDFNewCacheEntry_t *pce;
    SDF_simple_key_t    key;

    fprintf(stderr, "------------------------------------------------------------\n");
    if (s != NULL) {
	fprintf(stderr, "%s ", s);
    }
    fprintf(stderr, "bucket %p: phead: %p\n", pb, pb->entry);
    for (pce = pb->entry; pce != NULL; pce = pce->next) {
	SDFNewCacheCopyKeyOutofObject(pb->slab->pc, key.key, pce);
	key.len = pce->key_len;
        fprintf(stderr, "pce: %p, bucket: %p, next: %p, flags: %d, state: %d, cguid:%"PRIu64", key:'%s', size:%d", pce, pce->pbucket, pce->next, pce->flags, pce->state, pce->cguid, key.key, pce->obj_size);
        if (pce == pce_bad) {
	    fprintf(stderr, "  <---------\n");
	} else {
	    fprintf(stderr, "\n");
	}
    }
    fprintf(stderr, "------------------------------------------------------------\n");

    #ifdef notdef
	if (s != NULL) {
	    oh_oh(s);
	}
    #endif
}

static void init_transient_entry(SDFNewCacheSlab_t *ps)
{
    void                *pnext_page;
    void                *ppage;
    int                  npages;
    int                  j;

    npages = (sizeof(SDFNewCacheEntry_t) + ps->pc->max_key_size) / ps->pc->page_data_size;
    if ((npages*ps->pc->page_data_size) < (sizeof(SDFNewCacheEntry_t) + ps->pc->max_key_size)) {
        npages++;
    }
    pnext_page = plat_alloc(npages*ps->pc->page_size);
    #ifdef MALLOC_TRACE
        UTMallocTrace("transient_entry", SDF_FALSE, SDF_FALSE, SDF_FALSE, (void *) pnext_page, npages*ps->pc->page_size);
    #endif // MALLOC_TRACE
    if (!pnext_page) {
	plat_log_msg(30577, 
	             PLAT_LOG_CAT_SDF_CC, 
	             PLAT_LOG_LEVEL_FATAL,
		     "Could not allocate SDF cache slab temporary entry");
	plat_assert_always(0);
    }

    ps->tmp_entry = NULL;
    for (j=0; j<npages; j++) {
	ppage = pnext_page;
	pnext_page += ps->pc->page_size;
	/* last 8B of a page holds the next page pointer */
	*((void **) PAGE_POINTER(ps->pc, ppage)) = ps->tmp_entry;
	ps->tmp_entry = ppage;
    }
}

static void *fastcc_object_alloc(SDFNewCacheSlab_t *ps, uint64_t size, void *wrbk_arg)
{
    void                 *p;
    char                 *ppage;
    uint64_t              size_found;
    uint64_t              size_found_reclaimed;
    SDFNewCacheEntry_t   *pce_ret;
    uint64_t              npages_removed;

    size_found = 0;
    for (p = NULL; (ps->free_pages != NULL) && (size_found < size); ) {
	/* last 8B of a page holds the next page pointer */
        ppage = ps->free_pages;
	ps->free_pages = *((void **) PAGE_POINTER(ps->pc, ppage));
	*((void **) PAGE_POINTER(ps->pc, ppage)) = p;
	p = ppage;
	size_found += ps->pc->page_data_size;
    }
    if (size_found < size) {
        /*  I still don't have enough pages,
	 *  so resort to evicting clean objects.
	 */
	size_found_reclaimed = size_found;
	for (pce_ret = ps->lru_tail; pce_ret != NULL; pce_ret = ps->lru_tail) {
	   npages_removed = SDFNewCacheRemove(ps->pc, pce_ret, SDF_TRUE /* wrbk_flag */, wrbk_arg);
	   size_found_reclaimed += (npages_removed*ps->pc->page_data_size);
	   if (size_found_reclaimed >= size) {
	       break;
	   }
	}
	plat_assert(size_found_reclaimed >= size);

	for (; (ps->free_pages != NULL) && (size_found < size); ) {
	    /* last 8B of a page holds the next page pointer */
	    ppage = ps->free_pages;
	    ps->free_pages = *((void **) PAGE_POINTER(ps->pc, ppage));
	    *((void **) PAGE_POINTER(ps->pc, ppage)) = p;
	    p = ppage;
	    size_found += ps->pc->page_data_size;
	}
	plat_assert(size_found >= size);
    }
    // ps->cursize += size;
    return(p);
}

static uint64_t fastcc_object_free(SDFNewCacheSlab_t *ps, void *obj)
{
    void      *p, *p_next;
    uint64_t   npages;

    npages = 0;
    for (p = obj; p != NULL; p = p_next) {
	/* last 8B of a page holds the next page pointer */
	p_next = *((void **) PAGE_POINTER(ps->pc, p));
	*((void **) PAGE_POINTER(ps->pc, p)) = ps->free_pages;
	ps->free_pages = p;
	npages++;
    }
    return(npages);
}

void SDFNewCacheAddModObjectStats(SDFNewCacheEntry_t *pce)
{
    // Update the counts of modified objects
    (pce->pbucket->slab->n_mod)++;
    (pce->pbucket->slab->modsize_w_keys) += (pce->obj_size + pce->key_len);

    add_modlru_list(pce->pbucket->slab, pce);
}

void SDFNewCacheSubtractModObjectStats(SDFNewCacheEntry_t *pce)
{
    // Update the counts of modified objects
    (pce->pbucket->slab->n_mod)--;
    (pce->pbucket->slab->modsize_w_keys) -= (pce->obj_size + pce->key_len);

    modlru_remove(pce->pbucket->slab, pce);
}

SDFNewCacheEntry_t *SDFNewCacheCreateCacheObject(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce, size_t size, void *wrbk_arg)
{
    SDFNewCacheEntry_t   *pce_new;
    SDFNewCacheSlab_t    *ps;
    size_t                entry_size;
    SDF_simple_key_t      simple_key;

    /*   Create a new cache entry, including space for the object.
     *   It assumes that this is the first time an object is
     *   being allocated for this cache entry.
     */

    plat_assert(pce->obj_size == 0);

    entry_size = sizeof(SDFNewCacheEntry_t) + pce->key_len + size;

    ps = pce->pbucket->slab;

    if (size > ps->pc->max_object_size) {
	plat_log_msg(21157, 
	             PLAT_LOG_CAT_SDF_CC, 
	             PLAT_LOG_LEVEL_FATAL,
		     "Object is larger than specified max. Check that SDF_MAX_OBJ_SIZE property is large enough");
	plat_assert_always(0);
    }

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_heap(ps);
    }
    #endif

    pce_new = (SDFNewCacheEntry_t *) fastcc_object_alloc(ps, entry_size, wrbk_arg);

    // Copy transient entry to final entry.
    memcpy(pce_new, pce, sizeof(SDFNewCacheEntry_t));
    SDFNewCacheCopyKeyOutofObject(pc, simple_key.key, pce);
    simple_key.len = pce->key_len;
    SDFNewCacheCopyKeyIntoObject(pc, &simple_key, pce_new);

    pce_new->obj_size   = size; // obj_size was zero for the old pce

    /*   Update slab stats.
     *   Do this here so that the checks in add_lru_list will pass
     *   when detailed self-checking is enabled.
     */ 
    (ps->nobjects)++;
    ps->cursize_w_keys += pce->key_len;
    ps->cursize        += size;
    ps->cursize_w_keys += size;

    if (pce_new->state == pc->mod_state) {
        (pce_new->pbucket->slab->n_mod)++;
	(pce_new->pbucket->slab->modsize_w_keys) += (pce_new->obj_size + pce_new->key_len);
    }

    // Add entry to LRU list.
    add_lru_list(ps, pce_new);

    // Add entry to the bucket list.
    pce_new->next           = pce_new->pbucket->entry;
    pce_new->pbucket->entry = pce_new;

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_heap(ps);
    }
    #endif

    #ifdef DUMP_CACHE
	dump_bucket_list(pce_new->pbucket, NULL, "after CreateCacheObject:");
    #endif

    return(pce_new);
}

SDFNewCacheEntry_t *SDFNewCacheOverwriteCacheObject(SDFNewCache_t *pc, SDFNewCacheEntry_t *pce_old, uint32_t size, void *wrbk_arg)
{
    SDFNewCacheEntry_t  *pce;
    SDFNewCacheEntry_t  *pce_new;
    SDFNewCacheSlab_t   *ps;
    SDF_simple_key_t     simple_key;

    ps = pce_old->pbucket->slab;

    pce = ps->tmp_entry;

    /*  Remember the contents of the old entry in case it gets
     *  clobbered as memory is reallocated.
     */
    memcpy(pce, pce_old, sizeof(SDFNewCacheEntry_t));
    SDFNewCacheCopyKeyOutofObject(pc, simple_key.key, pce_old);
    simple_key.len = pce_old->key_len;
    SDFNewCacheCopyKeyIntoObject(pc, &simple_key, pce);
    pce->key_len = pce_old->key_len;

    (void) SDFNewCacheRemove(pc, pce_old, SDF_FALSE /* wrbk_flag */, wrbk_arg);

    /*   Finish setting up the transient entry before calling
     *   SDFNewCacheCreateCacheObject.
     */

    pce->obj_size = 0; // since we haven't allocated the new object yet

    /* The old entry was taken off the bucket list! */
    pce->next = NULL;

    /* The old entry was taken off the LRU list! */
    pce->lru_next = NULL;
    pce->lru_prev = NULL;

    //   Allocate new entry.
    pce_new = SDFNewCacheCreateCacheObject(pc, pce, size, wrbk_arg);

    #ifdef CHECK_HEAP
    if (SDFSelfTestCheckHeap) {
	check_heap(ps);
    }
    #endif

    #ifdef DUMP_CACHE
	dump_bucket_list(pce_new->pbucket, NULL, "after OverwriteCacheObject:");
    #endif

    return(pce_new);
}

void SDFNewCacheCheckModifiedObjectLimit(SDFNewCacheBucket_t *pbucket, void *flush_arg)
{
    SDFNewCacheSlab_t    *ps;
    SDFNewCache_t        *pc;
    SDFNewCacheEntry_t   *pce_ret;
    SDFNewCacheEntry_t   *pce_ret_next;
    uint32_t              nflushed;

    ps = pbucket->slab;
    pc = ps->pc;

    nflushed = 0;
    pce_ret  = ps->modlru_tail;
    if (pce_ret != NULL) {
	while (ps->modsize_w_keys > pc->modsize_per_slab) {

	    //  Flush LRU modified objects until we are below the threshold.

	    plat_assert(pce_ret->state == pc->mod_state);
	    nflushed++;
	    if (nflushed > pc->max_flushes_per_mod_check) {
		pce_ret = NULL;
		break;
	    }
	    pc->wrbk_fn(pce_ret, flush_arg);
	    // Set the state to unmodified to avoid unnecessary writebacks!
	    // Update the counts of modified objects
            pce_ret_next = pce_ret->modlru_prev;
	    SDFNewCacheSubtractModObjectStats(pce_ret);
	    pce_ret->state = pc->shared_state;
	    pce_ret        = pce_ret_next;
	}
    }
}

void SDFNewCacheFlushLRUModObject(SDFNewCacheBucket_t *pbucket, void *flush_arg)
{
    SDFNewCacheSlab_t    *ps;
    SDFNewCache_t        *pc;
    SDFNewCacheEntry_t   *pce_ret;

    //  Flush the LRU modified object

    ps = pbucket->slab;
    pc = ps->pc;

    pce_ret  = ps->modlru_tail;
    if (pce_ret != NULL) {
	plat_assert(pce_ret->state == pc->mod_state);
	pc->wrbk_fn(pce_ret, flush_arg);
	// Set the state to unmodified to avoid unnecessary writebacks!
	// Update the counts of modified objects
	SDFNewCacheSubtractModObjectStats(pce_ret);
	pce_ret->state = pc->shared_state;
    }
}

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

/**************************************************************************************
 *
 *    Stuff added to support LRU
 *
 **************************************************************************************/

void SDFNewCacheTransientEntryCheck(SDFNewCacheBucket_t *pb, SDFNewCacheEntry_t *pce)
{
    if (pce == pb->slab->tmp_entry) {
	plat_log_msg(21158, 
		     PLAT_LOG_CAT_SDF_CC, 
		     PLAT_LOG_LEVEL_FATAL,
		     "================>  Transient SDF cache entry was not reclaimed!");
        plat_abort();
    }
}



