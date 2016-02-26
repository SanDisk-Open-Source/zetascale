/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   clipper.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: clipper.h 308 2008-02-20 22:34:58Z briano $
 */

#ifndef _CLIPPER_H
#define _CLIPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_PRINT_LEN      1024

/**************************************************************************/

  /*  Lock macros */

#define LockType       fthLock_t
#define WaitType       fthWaitEl_t *

#define InitLock(x)    fthLockInit(&(x))
#define Unlock(x, w)   fthUnlock(w)
#define Lock(x, w)     (w = fthLock(&(x), 1, NULL))
#define LockTry(x, w)  (w = fthTryLock(&(x), 1, NULL))

/**************************************************************************/

typedef enum {
    CL_FLAGS_FREE = 0, 
    CL_FLAGS_HEADER, 
    CL_FLAGS_IS_EXTENDED,
    CL_FLAGS_ENUM,
    CL_N_FLAGS
} ClipperEntryFlags;

#ifndef CLIPPER_C
    extern char *ClipperFlagNames[];
#else
    char *ClipperFlagNames[] = {
	"free",
	"header",
	"is_extended"
	"enum"
    };
#endif

    /*  These are carefully selected to achieve a minimally
     *  sized ClipperEntry_t!
     */
#define N_CLIPPER_FLAG_BITS        5
#define N_CLIPPER_SYNDROME_BITS   27
#define N_CLIPPER_BUCKET_BITS     32

// #define N_CLIPPER_PAGE_BITS    9
#define N_CLIPPER_PAGE_BITS       12
#define N_CLIPPER_SLABSIZE_BITS   20
#define CLIPPER_PAGE_SIZE         (1<<N_CLIPPER_PAGE_BITS)
#define CLIPPER_BUCKET_RATIO      16

#define CLIPPER_NULL_INDEX     ((1<<N_CLIPPER_SLABSIZE_BITS) - 1)
// #define CLIPPER_MAX_PAGES_PER_SLAB   (CLIPPER_NULL_INDEX - 1)
#define CLIPPER_MAX_PAGES_PER_SLAB   (CLIPPER_NULL_INDEX + 1)
#define CLIPPER_MIN_PAGES_PER_SLAB   256

#define CLIPPER_MAX_FRAGMENTS    1024

#define CLIPPER_MAX_LRU_SEARCH   16

#define CLIPPER_DEFAULT_NSLABS   1024

#define MAX_DEFAULT_BUF_SIZE   (32*1024)

typedef struct {
    uint64_t     flags:N_CLIPPER_FLAG_BITS;
    uint64_t     syndrome:N_CLIPPER_SYNDROME_BITS;
    uint64_t     bucket_or_cont:N_CLIPPER_BUCKET_BITS;
    uint64_t     next:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     lru_next:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     lru_prev:N_CLIPPER_SLABSIZE_BITS;
} ClipperUsedPageHeader;

typedef struct {
    uint64_t     flags:N_CLIPPER_FLAG_BITS;
    uint64_t     bucket:N_CLIPPER_BUCKET_BITS;
    uint64_t     n_contiguous:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     n_used:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     obj_next:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     obj_prev:N_CLIPPER_SLABSIZE_BITS;
} ClipperUsedPageExtension;

typedef struct {
    uint64_t     flags:N_CLIPPER_FLAG_BITS;
    uint64_t     n_contig:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     bin_next:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     free_next:N_CLIPPER_SLABSIZE_BITS;
    uint64_t     free_prev:N_CLIPPER_SLABSIZE_BITS;
} ClipperFreePageHeader;

typedef union {
    ClipperUsedPageHeader     used_header;
    ClipperUsedPageExtension  used_extension;
    ClipperFreePageHeader     free_header;
} ClipperEntry_t;

typedef uint32_t ClipperEntryIndex;

typedef struct ClipperSlab {
    uint64_t           nslab;
    uint64_t           offset;
    LockType           lock;
    ClipperEntryIndex  lru_head;
    ClipperEntryIndex  lru_tail;

       /*  Bins of contiguous regions of pages 
        *  that have been allocated at least once.
	*/
    ClipperEntryIndex  free_bins[N_CLIPPER_SLABSIZE_BITS+1];

	/*  Index of next unallocated page.
	 *  Starts at 0 and goes to pc->pages_per_slab.
	 *  (Analagous to "sbrk")
	 */
    ClipperEntryIndex  used_pages; 

    uint64_t           nobjects;
    uint64_t           pages_needed;
    uint64_t           pages_in_use;
    uint64_t           bytes_needed;
} ClipperSlab_t;

typedef struct ClipperBucket {
    ClipperEntryIndex  list_head;
} ClipperBucket_t;

typedef struct {
    int        nbits;
    int        offset;
    uint64_t   mask;
} MASKDATA;

#define MASK(x, m) (((x) & (m).mask)>>((m).offset))

typedef struct Clipper {
    uint32_t         pagesize;
    uint64_t         npages;
    uint32_t         null_index;
    uint64_t         slabsize;
    uint64_t         pages_per_slab;
    uint64_t         buckets_per_slab;
    uint32_t         n_syndrome_bits;
    uint64_t         nbuckets;
    uint32_t         n_bucket_bits;
    MASKDATA         syndrome_mask;
    MASKDATA         bucket_mask;
    ClipperEntry_t  *entries;
    ClipperBucket_t *buckets;
    uint64_t         nslabs;
    ClipperSlab_t   *slabs;
    LockType         enum_lock;
    uint64_t         enum_bucket;
    shard_t         *pshard;
    uint64_t         flash_offset_index;
} Clipper_t;

typedef struct ClipperStats {
    uint64_t     num_objects;
    uint64_t     cursize;
} ClipperStats_t;

typedef struct {
    uint32_t    databytes;
    uint32_t    npages_used;   // including metadata
    uint32_t    npages_actual; // page region may have unused pages at end
    uint8_t     state;
    uint16_t    offset; // to ensure no magic number "coincidences"
    uint64_t    exp_time;
    uint64_t    create_time;
    uint16_t    key_len;  // in bytes
    uint64_t    shard;
    char       *key;
    uint32_t    magic;
} ClipperFlashMeta_t;

extern struct flashDev *clipper_flashOpen(char *name, flash_settings_t *flash_settings, int flags);
extern struct shard *clipper_shardCreate(struct flashDev *dev, uint64_t shardID, 
	  int flags, uint64_t quota, unsigned maxObjs);
extern int clipper_flashGet(struct ssdaio_ctxt *pctxt, struct shard *shard, 
          struct objMetaData *metaData, char *key, char **dataPtr, int flags);
extern int clipper_flashPut(struct ssdaio_ctxt *pctxt, struct shard *shard, struct objMetaData *metaData, 
	  char *key, char *data, int flags);

#ifdef	__cplusplus
}
#endif

#endif /* _CLIPPER_H */
