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

#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>

/****************************************************************************/

// Level is an arbitrary salt for the hash.
uint64_t sdf_hash(const unsigned char *key, uint64_t keyLength, uint64_t level);

/*
--------------------------------------------------------------------
lookup8.c, by Bob Jenkins, January 4 1997, Public Domain.
hash(), hash2(), hash3, and mix() are externally useful functions.
You can use this free for any purpose.  It has no warranty.
--------------------------------------------------------------------

Gently modified by Jim to use SDF typedefs.

*/

typedef  uint64_t	 ub8;   /* unsigned 8-byte quantities */
typedef  uint32_t	 ub4;   /* unsigned 4-byte quantities */
typedef  unsigned char	 ub1;

/*
--------------------------------------------------------------------
mix -- mix 3 64-bit values reversibly.
mix() takes 48 machine instructions, but only 24 cycles on a superscalar
  machine (like Intel's new MMX architecture).  It requires 4 64-bit
  registers for 4::2 parallelism.
All 1-bit deltas, all 2-bit deltas, all deltas composed of top bits of
  (a,b,c), and all deltas of bottom bits were tested.  All deltas were
  tested both on random keys and on keys that were nearly all zero.
  These deltas all cause every bit of c to change between 1/3 and 2/3
  of the time (well, only 113/400 to 287/400 of the time for some
  2-bit delta).  These deltas all cause at least 80 bits to change
  among (a,b,c) when the mix is run either forward or backward (yes it
  is reversible).
This implies that a hash using mix64 has no funnels.  There may be
  characteristics with 3-bit deltas or bigger, I didn't test for
  those.
--------------------------------------------------------------------
*/
#define mix64(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>43); \
  b -= c; b -= a; b ^= (a<<9); \
  c -= a; c -= b; c ^= (b>>8); \
  a -= b; a -= c; a ^= (c>>38); \
  b -= c; b -= a; b ^= (a<<23); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>35); \
  b -= c; b -= a; b ^= (a<<49); \
  c -= a; c -= b; c ^= (b>>11); \
  a -= b; a -= c; a ^= (c>>12); \
  b -= c; b -= a; b ^= (a<<18); \
  c -= a; c -= b; c ^= (b>>22); \
}

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 64-bit value
  k     : the key (the unaligned variable-length array of bytes)
  len   : the length of the key, counting by bytes
  level : can be any 8-byte value
Returns a 64-bit value.  Every bit of the key affects every bit of
the return value.  No funnels.  Every 1-bit and 2-bit delta achieves
avalanche.  About 41+5len instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 64 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, Jan 4 1997.  bob_jenkins@burtleburtle.net.  You may
use this code any way you wish, private, educational, or commercial,
but I would appreciate if you give me credit.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^^64
is acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/


/*
--------------------------------------------------------------------
 This is identical to hash() on little-endian machines, and it is much
 faster than hash(), but a little slower than hash2(), and it requires
 -- that all your machines be little-endian, for example all Intel x86
    chips or all VAXen.  It gives wrong results on big-endian machines.
--------------------------------------------------------------------
*/

uint64_t sdf_hash( const ub1 *k, ub8 length, ub8 level)
// register const ub1 *k;        /* the key */
// register ub8  length;   /* the length of the key */
// register ub8  level;    /* the previous hash, or an arbitrary value */
{
  register ub8 a,b,c,len;

  /* Set up the internal state */
  len = length;
  a = b = level;                         /* the previous hash value */
  c = 0x9e3779b97f4a7c13LL; /* the golden ratio; an arbitrary value */

  /*---------------------------------------- handle most of the key */
  if (((size_t)k)&7)
  {
    while (len >= 24)
    {
      a += (k[0]        +((ub8)k[ 1]<< 8)+((ub8)k[ 2]<<16)+((ub8)k[ 3]<<24)
       +((ub8)k[4 ]<<32)+((ub8)k[ 5]<<40)+((ub8)k[ 6]<<48)+((ub8)k[ 7]<<56));
      b += (k[8]        +((ub8)k[ 9]<< 8)+((ub8)k[10]<<16)+((ub8)k[11]<<24)
       +((ub8)k[12]<<32)+((ub8)k[13]<<40)+((ub8)k[14]<<48)+((ub8)k[15]<<56));
      c += (k[16]       +((ub8)k[17]<< 8)+((ub8)k[18]<<16)+((ub8)k[19]<<24)
       +((ub8)k[20]<<32)+((ub8)k[21]<<40)+((ub8)k[22]<<48)+((ub8)k[23]<<56));
      mix64(a,b,c);
      k += 24; len -= 24;
    }
  }
  else
  {
    while (len >= 24)    /* aligned */
    {
      a += *(ub8 *)(k+0);
      b += *(ub8 *)(k+8);
      c += *(ub8 *)(k+16);
      mix64(a,b,c);
      k += 24; len -= 24;
    }
  }

  /*------------------------------------- handle the last 23 bytes */
  c += length;
  switch(len)              /* all the case statements fall through */
  {
  case 23: c+=((ub8)k[22]<<56);
  case 22: c+=((ub8)k[21]<<48);
  case 21: c+=((ub8)k[20]<<40);
  case 20: c+=((ub8)k[19]<<32);
  case 19: c+=((ub8)k[18]<<24);
  case 18: c+=((ub8)k[17]<<16);
  case 17: c+=((ub8)k[16]<<8);
    /* the first byte of c is reserved for the length */
  case 16: b+=((ub8)k[15]<<56);
  case 15: b+=((ub8)k[14]<<48);
  case 14: b+=((ub8)k[13]<<40);
  case 13: b+=((ub8)k[12]<<32);
  case 12: b+=((ub8)k[11]<<24);
  case 11: b+=((ub8)k[10]<<16);
  case 10: b+=((ub8)k[ 9]<<8);
  case  9: b+=((ub8)k[ 8]);
  case  8: a+=((ub8)k[ 7]<<56);
  case  7: a+=((ub8)k[ 6]<<48);
  case  6: a+=((ub8)k[ 5]<<40);
  case  5: a+=((ub8)k[ 4]<<32);
  case  4: a+=((ub8)k[ 3]<<24);
  case  3: a+=((ub8)k[ 2]<<16);
  case  2: a+=((ub8)k[ 1]<<8);
  case  1: a+=((ub8)k[ 0]);
    /* case 0: nothing left to add */
  }
  mix64(a,b,c);
  /*-------------------------------------------- report the result */
  return c;
}

/****************************************************************************/

struct MCTLIterator;
struct MCTLMap;
struct MCTLMapEntry;

typedef int bool;
#define true 1
#define false 0

struct MCTLMap *MCTLMapInit(uint64_t nbuckets, uint64_t max_entries, bool use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
void MCTLMapDestroy(struct MCTLMap *pm);
void MCTLMapClear(struct MCTLMap *pm);
struct MCTLMapEntry *MCTLMapCreate(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, bool incr_refcnt);
struct MCTLMapEntry *MCTLMapUpdate(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
struct MCTLMapEntry *MCTLMapSet(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
struct MCTLMapEntry *MCTLMapGet(struct MCTLMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
int MCTLMapReleaseEntry(struct MCTLMap *pm, struct MCTLMapEntry *pme);
int MCTLMapRelease(struct MCTLMap *pm, char *key, uint32_t keylen);
struct MCTLIterator *MCTLMapEnum(struct MCTLMap *pm);
void MCTLFinishEnum(struct MCTLMap *pm, struct MCTLIterator *iterator);
int MCTLMapNextEnum(struct MCTLMap *pm, struct MCTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
int MCTLMapDelete(struct MCTLMap *pm, char *key, uint32_t keylen);

#define N_ENTRIES_TO_MALLOC    100
#define N_ITERATORS_TO_MALLOC  100

typedef struct MCTLMapEntry {
    char                  *contents;
    uint64_t               datalen;
    int32_t                refcnt;
    char                  *key;
    uint32_t               keylen;
    struct MCTLMapEntry  *next;
    struct MCTLMapEntry  *next_lru;
    struct MCTLMapEntry  *prev_lru;
} MCTLMapEntry_t;

typedef struct MCTLMapBucket {
    MCTLMapEntry_t  *entry;
} MCTLMapBucket_t;

typedef struct MCTLIterator {
    uint64_t                enum_bucket;
    MCTLMapEntry_t        *enum_entry;
    struct MCTLIterator   *next;
} MCTLIterator_t;

typedef struct MCTLMap {
    uint64_t          nbuckets;
    uint64_t          max_entries;
    uint64_t          n_entries;
    bool              use_locks;
    MCTLMapBucket_t *buckets;
    pthread_mutex_t   mutex;
    MCTLMapEntry_t  *lru_head;
    MCTLMapEntry_t  *lru_tail;
    void              (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen);
    void             *replacement_callback_data;
    uint32_t          NEntries;
    uint32_t          NUsedEntries;
    MCTLMapEntry_t  *FreeEntries;
    uint32_t          NIterators;
    uint32_t          NUsedIterators;
    struct MCTLIterator *FreeIterators;
} MCTLMap_t;

/****************************************************************************/


static MCTLMap_t *malloc_map;
static int firstcall = 1;
static pthread_mutex_t bwo_mutex = PTHREAD_MUTEX_INITIALIZER;

void init_malloc_check()
{
    malloc_map = MCTLMapInit(1000000, 0, true, NULL, NULL);
}

extern void *bwo_calloc(size_t nmemb, size_t size);
extern void *bwo_malloc(size_t size);
extern void bwo_free(void *ptr);
extern void *bwo_realloc(void *ptr, size_t size);

#define check_first() {\
    if (firstcall) { \
        pthread_mutex_lock(&bwo_mutex); \
	if (firstcall) { \
	    firstcall = 0; \
	    malloc_map = MCTLMapInit(1000000, 0, 1, NULL, NULL); \
	    assert(malloc_map); \
	} \
	pthread_mutex_unlock(&bwo_mutex); \
    } \
}

static void mcmsg(char *fmt, ...)
{
   char     stmp[512];
   va_list  args;

   va_start(args, fmt);

   vsprintf(stmp, fmt, args);
   strcat(stmp, "\n");

   va_end(args);

   fprintf(stderr, "%s", stmp);
}


void *bwo_calloc(size_t nmemb, size_t size)
{
    void *p;

    check_first();

    p = calloc(nmemb, size);
    (void) MCTLMapDelete(malloc_map, (char *) &p, sizeof(void *));
    mcmsg("calloc %p[%ld]\n", p, size);
    return(p);
}

void *bwo_malloc(size_t size)
{
    void *p;

    check_first();

    p = malloc(size);
    (void) MCTLMapDelete(malloc_map, (char *) &p, sizeof(void *));
    mcmsg("malloc %p[%ld]\n", p, size);
    return(p);
}

void *bwo_realloc(void *ptr, size_t size)
{
    void *p;

    check_first();

    p = realloc(ptr, size);
    (void) MCTLMapDelete(malloc_map, (char *) &p, sizeof(void *));
    mcmsg("realloc %p[%ld]\n", p, size);
    return(p);
}

void bwo_free(void *ptr)
{
    check_first();

    if (MCTLMapCreate(malloc_map, (char *) &ptr, sizeof(void *), NULL, 0, false)) {
	mcmsg("free (DOUBLE!!!) %p\n", ptr);
	// assert(0);
    } else {
	mcmsg("free %p\n", ptr);
    }
    free(ptr);
}

/*
 * File:   sdf_tlmap.c
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * IMPORTANT NOTES:
 *    - Unlike tlmap in the sdf directory, sdf_tlmap does NOT
 *      automatically malloc and free the key and contest of
 *      a hashtable entry!
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

static void _sdftlmap_assert(int x) {
    if (x) {
        fprintf(stderr, "Assertion failure in sdf_tlmap!\n");
	assert(0);
    }
}
#define sdftlmap_assert(x) _sdftlmap_assert((x) == 0)

#define do_lock(x)  {if (pm->use_locks) { pthread_mutex_lock(x); }}
#define do_unlock(x) {if (pm->use_locks) { pthread_mutex_unlock(x); }}

//  Predeclarations
static void insert_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme);
static void remove_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme);
static void update_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme);
static void replace_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme);
static MCTLMapEntry_t *find_pme(struct MCTLMap *pm, char *pkey, uint32_t keylen, MCTLMapBucket_t **pb);
static MCTLMapEntry_t *create_pme(MCTLMap_t *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
static MCTLMapEntry_t *copy_pme(MCTLMap_t *pm, MCTLMapEntry_t *pme);
static MCTLMapEntry_t *copy_pme_list(MCTLMap_t *pm, MCTLMapEntry_t *pme);
static void free_pme(MCTLMap_t *pm, MCTLMapEntry_t *pme);
static void free_pme_list(MCTLMap_t *pm, MCTLMapEntry_t *pme);


static MCTLMapEntry_t *get_entry(MCTLMap_t *pm)
{
    int                i;
    MCTLMapEntry_t  *e;

    if (pm->FreeEntries == NULL) {
        e = (MCTLMapEntry_t *) malloc(N_ENTRIES_TO_MALLOC*sizeof(MCTLMapEntry_t));
	sdftlmap_assert(e);

	for (i=0; i<N_ENTRIES_TO_MALLOC; i++) {
	    e[i].next = pm->FreeEntries;
	    pm->FreeEntries = &(e[i]);
	}
        pm->NEntries += N_ENTRIES_TO_MALLOC;
    }
    e = pm->FreeEntries;
    pm->FreeEntries = e->next;

    pm->NUsedEntries++;
    return(e);
}

static void free_entry(MCTLMap_t *pm, MCTLMapEntry_t *e)
{
    e->next     = pm->FreeEntries;
    pm->FreeEntries = e;
    pm->NUsedEntries--;
}

static struct MCTLIterator *get_iterator(MCTLMap_t *pm)
{
    int                   i;
    struct MCTLIterator *it;

    if (pm->FreeIterators == NULL) {
        it = (struct MCTLIterator *) malloc(N_ITERATORS_TO_MALLOC*sizeof(struct MCTLIterator));
	sdftlmap_assert(it);

	for (i=0; i<N_ITERATORS_TO_MALLOC; i++) {
	    it[i].next = pm->FreeIterators;
	    pm->FreeIterators = &(it[i]);
	}
        pm->NIterators += N_ITERATORS_TO_MALLOC;
    }
    it = pm->FreeIterators;
    pm->FreeIterators = it->next;

    pm->NUsedIterators++;
    return(it);
}

static void free_iterator(MCTLMap_t *pm, struct MCTLIterator *it)
{
    it->next     = pm->FreeIterators;
    pm->FreeIterators = it;
    pm->NUsedIterators--;
}

struct MCTLMap *MCTLMapInit(uint64_t nbuckets, uint64_t max_entries, bool use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data)
{
    uint64_t          i;
    MCTLMap_t       *pm;

    pm = (MCTLMap_t *) malloc(sizeof(MCTLMap_t));
    sdftlmap_assert(pm);
    pm->lru_head       = NULL;
    pm->lru_tail       = NULL;
    pm->replacement_callback       = replacement_callback;
    pm->replacement_callback_data  = replacement_callback_data;
    pm->use_locks      = use_locks;
    pm->max_entries    = max_entries;
    pm->n_entries      = 0;
    pm->nbuckets       = nbuckets;
    pm->NEntries       = 0;
    pm->NUsedEntries   = 0;
    pm->FreeEntries    = NULL;
    pm->NIterators     = 0;
    pm->NUsedIterators = 0;
    pm->FreeIterators  = NULL;

    pm->buckets       = (MCTLMapBucket_t *) malloc(nbuckets*(sizeof(MCTLMapBucket_t)));
    sdftlmap_assert(pm->buckets);

    for (i=0; i<nbuckets; i++) {
	pm->buckets[i].entry = NULL;
    }

    pthread_mutex_init(&(pm->mutex), NULL);

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapInit: pm=%p\n", pm);
    #endif

    return(pm);
}

void MCTLMapDestroy(struct MCTLMap *pm)
{
    uint64_t          i;

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapDestroy: pm=%p\n", pm);
    #endif

    for (i=0; i<pm->nbuckets; i++) {
        free_pme_list(pm, pm->buckets[i].entry);
    }

    free(pm->buckets);
    pthread_mutex_destroy(&(pm->mutex));
    free(pm);
}

void MCTLMapClear(struct MCTLMap *pm)
{
    uint64_t          i;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapClear: pm=%p\n", pm);
    #endif

    for (i=0; i<pm->nbuckets; i++) {
        free_pme_list(pm, pm->buckets[i].entry);
	pm->buckets[i].entry = NULL;
    }
    pm->n_entries     = 0;
    pm->lru_head      = NULL;
    pm->lru_tail      = NULL;
    do_unlock(&(pm->mutex));
}

//  Return non-NULL if success, NULL if object exists
struct MCTLMapEntry *MCTLMapCreate(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, bool incr_refcnt)
{
    MCTLMapEntry_t   *pme;
    MCTLMapBucket_t  *pb;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapCreate: pm=%p, key=0x%lx, keylen=%d, pdata=%p, datalen=%ld, ", pm, *((uint64_t *) pkey), keylen, pdata, datalen);
    #endif

    pme = find_pme(pm, pkey, keylen, &pb);

    if (pme != NULL) {
	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, "pme=%p\n", pme);
	#endif

	do_unlock(&(pm->mutex));
        return(NULL);
    }

    /* Create a new entry. */

    pme = create_pme(pm, pkey, keylen, pdata, datalen);
    if (incr_refcnt) {
	pme->refcnt = 1;
    } else {
	pme->refcnt = 0;
    }

    /* put myself on the bucket list */
    pme->next = pb->entry;
    pb->entry = pme;

    pm->n_entries++;

    /* put myself on the lru list if necessary */
    if (pm->max_entries != 0) {
        insert_lru(pm, pme);

	if (pm->n_entries > pm->max_entries) {
	    // do an LRU replacement
	    replace_lru(pm, pme);
	}
    }

    do_unlock(&(pm->mutex));
    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "pme=%p\n", pme);
    #endif

    return(pme);
}

//  Return non-NULL if success, NULL if object does not exist
struct MCTLMapEntry *MCTLMapUpdate(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen)
{
    MCTLMapEntry_t   *pme;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapUpdate: pm=%p, key=0x%lx, keylen=%d, pdata=%p, datalen=%ld\n", pm, *((uint64_t *) pkey), keylen, pdata, datalen);
    #endif

    pme = find_pme(pm, pkey, keylen, NULL);

    if (pme == NULL) {
	do_unlock(&(pm->mutex));
        return(NULL);
    }

    /* Update an existing entry. */

    // update the LRU list if necessary
    if (pm->max_entries != 0) {
	update_lru(pm, pme);
    }

    if (pm->replacement_callback) {
        //  return NULL for key so that it is not freed!
	(pm->replacement_callback)(pm->replacement_callback_data, pme->key, pme->keylen, pme->contents, pme->datalen);
    }

    pme->contents = pdata;
    pme->datalen  = datalen;

    do_unlock(&(pm->mutex));
    return(pme);
}

//  Return non-NULL if success, NULL if object exists
struct MCTLMapEntry *MCTLMapSet(struct MCTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen)
{
    MCTLMapEntry_t   *pme;
    MCTLMapBucket_t  *pb;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapSet: pm=%p, key=0x%lx, keylen=%d, pdata=%p, datalen=%ld\n", pm, *((uint64_t *) pkey), keylen, pdata, datalen);
    #endif

    pme = find_pme(pm, pkey, keylen, &pb);

    if (pme != NULL) {

	/* Update an existing entry. */

	if (pm->replacement_callback) {
	    //  return NULL for key so that it is not freed!
	    (pm->replacement_callback)(pm->replacement_callback_data, NULL, 0, pme->contents, pme->datalen);
	}

	*old_pdata   = pme->contents;
	*old_datalen = pme->datalen;

	pme->contents = pdata;
	pme->datalen  = datalen;
	sdftlmap_assert(pme->refcnt == 0); // remove this! xxxzzz
	pme->refcnt   = 0;

	/* update the lru list if necessary */
	if (pm->max_entries != 0) {
	    update_lru(pm, pme);
	}

    } else {

	/* Create a new entry. */

	pme = create_pme(pm, pkey, keylen, pdata, datalen);
	pme->refcnt = 0;

	*old_pdata   = NULL;
	*old_datalen = 0;

	/* put myself on the bucket list */
	pme->next = pb->entry;
	pb->entry = pme;

	pm->n_entries++;

	/* put myself on the lru list if necessary */
	if (pm->max_entries != 0) {
	    update_lru(pm, pme);

	    if (pm->n_entries > pm->max_entries) {
		// do an LRU replacement
		replace_lru(pm, pme);
	    }
	}
    }

    do_unlock(&(pm->mutex));
    return(pme);
}

//  Returns non-NULL if successful, NULL otherwise
struct MCTLMapEntry *MCTLMapGet(struct MCTLMap *pm, char *key, uint32_t keylen, char **pdata, uint64_t *pdatalen)
{
    uint64_t           datalen;
    MCTLMapEntry_t   *pme;
    char              *data;

    do_lock(&(pm->mutex));

    pme = find_pme(pm, key, keylen, NULL);

    if (pme != NULL) {
        data    = pme->contents;
	datalen = pme->datalen;

	// update the LRU list if necessary
	if (pm->max_entries != 0) {
	    update_lru(pm, pme);
	}
        (pme->refcnt)++;

    } else {
        data    = NULL;
	datalen = 0;
    }

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapGet: pm=%p, key=0x%lx, keylen=%d, pdata=%p, datalen=%ld\n", pm, *((uint64_t *) key), keylen, data, datalen);
    #endif

    do_unlock(&(pm->mutex));
    *pdatalen = datalen;
    *pdata    = data;
    return(pme);
}

//  Decrement the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int MCTLMapRelease(struct MCTLMap *pm, char *key, uint32_t keylen)
{
    MCTLMapEntry_t   *pme;
    int                rc = 0;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapRelease: pm=%p, key=0x%lx, keylen=%d", pm, *((uint64_t *) key), keylen);
    #endif

    pme = find_pme(pm, key, keylen, NULL);

    if (pme != NULL) {
	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, ", refcnt=%d\n", pme->refcnt - 1);
	#endif
        (pme->refcnt)--;
	rc = 1;
    } else {
	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, " (KEY NOT FOUND!)\n");
	#endif
    }

    do_unlock(&(pm->mutex));
    return(rc);
}

//  Decrement the reference count for this entry
int MCTLMapReleaseEntry(struct MCTLMap *pm, struct MCTLMapEntry *pme)
{
    int rc = 0;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapReleaseEntry: pm=%p, pme=%p\n", pm, pme);
    #endif

    if (pme != NULL) {
        (pme->refcnt)--;
	rc = 1;
    }

    do_unlock(&(pm->mutex));
    return(rc);
}

struct MCTLIterator *MCTLMapEnum(struct MCTLMap *pm)
{
    MCTLIterator_t    *iterator;
    MCTLMapEntry_t    *pme = NULL;
    MCTLMapBucket_t   *pb;
    uint64_t            nb;

    do_lock(&(pm->mutex));

    iterator = get_iterator(pm);
    sdftlmap_assert(iterator);

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapEnum: pm=%p, iterator=%p, ", pm, iterator);
    #endif

    for (nb=0; nb < pm->nbuckets; nb++) {
	pb = &(pm->buckets[nb]);
	pme = pb->entry;
	if (pme != NULL) {
	    break;
	}
    }

    //  copy all objects in bucket list

    iterator->enum_bucket = nb;
    iterator->enum_entry  = copy_pme_list(pm, pme);

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "pme=%p, enum_entry=%p\n", pme, iterator->enum_entry);
    #endif

    do_unlock(&(pm->mutex));

    return(iterator);
}

void MCTLFinishEnum(struct MCTLMap *pm, struct MCTLIterator *iterator)
{
    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLFinishEnum: pm=%p, iterator=%p\n", pm, iterator);
    #endif

    if (iterator->enum_entry != NULL) {
	free_pme_list(pm, iterator->enum_entry);
    }
    free_iterator(pm, iterator);
    do_unlock(&(pm->mutex));
}

//  Returns 1 if successful, 0 otherwise
//  Caller is responsible for freeing key and data
int MCTLMapNextEnum(struct MCTLMap *pm, struct MCTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen) 
{
    MCTLMapEntry_t    *pme_return, *pme;
    MCTLMapBucket_t   *pb;
    uint64_t            nb;

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapNextEnum: pm=%p, iterator=%p, ", pm, iterator);
    #endif

    if (iterator->enum_entry == NULL) {
	do_unlock(&(pm->mutex));
	MCTLFinishEnum(pm, iterator);

	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, "pdata=NULL, datalen=0\n");
	#endif

        *key     = NULL;
	*keylen  = 0;
        *data    = NULL;
	*datalen = 0;
        return(0);
    }
    pme_return = iterator->enum_entry;

    if (pme_return->next != NULL) {
	iterator->enum_entry = pme_return->next;
    } else {

	pme = NULL;
	for (nb=iterator->enum_bucket + 1; nb < pm->nbuckets; nb++) {
	    pb = &(pm->buckets[nb]);
	    pme = pb->entry;
	    if (pme != NULL) {
		break;
	    }
	}
	iterator->enum_bucket = nb;
	iterator->enum_entry  = copy_pme_list(pm, pme);
    }

    if (pme_return != NULL) {
	*key     = pme_return->key;
	*keylen  = pme_return->keylen;

	*data    = (char *) pme_return->contents;
	*datalen = pme_return->datalen;

	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, "key=%p, keylen=%d, pdata=%p, datalen=%ld, pme_return=%p\n", *key, *keylen, *data, *datalen, pme_return);
	#endif

	free_pme(pm, pme_return);

	do_unlock(&(pm->mutex));
	return(1);
    } else {
	MCTLFinishEnum(pm, iterator);

	#ifdef SANDISK_PRINTSTUFF
	    fprintf(stderr, "pdata=NULL, datalen=0\n");
	#endif

        *key     = NULL;
	*keylen  = 0;
        *data    = NULL;
	*datalen = 0;
	do_unlock(&(pm->mutex));
	return(0);
    }
}

/*   Return 0 if succeeds, 1 if object doesn't exist.
 */
int MCTLMapDelete(struct MCTLMap *pm, char *key, uint32_t keylen)
{
    uint64_t            h;
    MCTLMapEntry_t   **ppme;
    MCTLMapEntry_t    *pme;
    MCTLMapBucket_t   *pb;
    char               *key2;

    key2 = (char *) *((uint64_t *) key);

    do_lock(&(pm->mutex));

    #ifdef SANDISK_PRINTSTUFF
	fprintf(stderr, "MCTLMapDelete: pm=%p, key=0x%lx, keylen=%d\n", pm, *((uint64_t *) key), keylen);
    #endif

    h = sdf_hash((const unsigned char *) key, sizeof(uint64_t), 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    sdftlmap_assert(keylen == 8); // remove this! xxxzzz

    for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
	pme = *ppme;
	if (pme->key == key2) {

	    //  Remove from the LRU list if necessary
	    if (pm->max_entries != 0) {
		if (pme->next_lru == NULL) {
		    sdftlmap_assert(pm->lru_tail == pme); // xxxzzz remove this!
		    pm->lru_tail = pme->prev_lru;
		} else {
		    pme->next_lru->prev_lru = pme->prev_lru;
		}
		if (pme->prev_lru == NULL) {
		    sdftlmap_assert(pm->lru_head == pme); // xxxzzz remove this!
		    pm->lru_head = pme->next_lru;
		} else {
		    pme->prev_lru->next_lru = pme->next_lru;
		}
	    }

            *ppme = pme->next;
            free_pme(pm, pme);
	    do_unlock(&(pm->mutex));
            return (0);
        }
    }
    do_unlock(&(pm->mutex));
    return (1);
}

static MCTLMapEntry_t *create_pme(MCTLMap_t *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen)
{
    MCTLMapEntry_t   *pme;

    pme = get_entry(pm);

    if (keylen == 8) {
	pme->key = (char *) *((uint64_t *) pkey);
    } else {
        sdftlmap_assert(0);
    }
    pme->keylen   = keylen;
    pme->refcnt   = 0;

    pme->contents  = pdata;
    pme->datalen   = datalen;

    pme->next      = NULL;

    return(pme);
}

static MCTLMapEntry_t *copy_pme(MCTLMap_t *pm, MCTLMapEntry_t *pme)
{
    MCTLMapEntry_t  *pme2;

    pme2 = get_entry(pm);

    pme2->key    = pme->key;
    pme2->keylen = pme->keylen;

    pme2->contents = pme->contents;
    pme2->datalen  = pme->datalen;

    fprintf(stderr, "copy_pme [from pme %p to %p] key=%p, keylen=%d, data=%p, datalen=%ld\n", pme, pme2, pme->key, pme->keylen, pme->contents, pme->datalen);

    pme2->next = NULL;

    return(pme2);
}

static MCTLMapEntry_t *copy_pme_list(MCTLMap_t *pm, MCTLMapEntry_t *pme_in)
{
    MCTLMapEntry_t  *pme, *pme2, *pme_out;

    pme_out = NULL;
    for (pme = pme_in; pme != NULL; pme = pme->next) {
        pme2 = copy_pme(pm, pme);
	pme2->next = pme_out;
	pme_out = pme2;
    }
    return(pme_out);
}

static void free_pme(MCTLMap_t *pm, MCTLMapEntry_t *pme)
{
    free_entry(pm, pme);
}

static void free_pme_list(MCTLMap_t *pm, MCTLMapEntry_t *pme_in)
{
    MCTLMapEntry_t  *pme, *pme_next;

    for (pme = pme_in; pme != NULL; pme = pme_next) {
        pme_next = pme->next;
        free_pme(pm, pme);
    }
}

static void insert_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme)
{
    pme->next_lru = pm->lru_head;
    if (pm->lru_head != NULL) {
        pm->lru_head->prev_lru = pme;
    }
    pme->prev_lru = NULL;
    pm->lru_head = pme;
    if (pm->lru_tail == NULL) {
	pm->lru_tail = pme;
    }
}

static void remove_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme)
{
    if (pme->next_lru == NULL) {
	sdftlmap_assert(pm->lru_tail == pme); // xxxzzz remove this!
	pm->lru_tail = pme->prev_lru;
    } else {
	pme->next_lru->prev_lru = pme->prev_lru;
    }
    if (pme->prev_lru == NULL) {
	sdftlmap_assert(pm->lru_head == pme); // xxxzzz remove this!
	pm->lru_head = pme->next_lru;
    } else {
	pme->prev_lru->next_lru = pme->next_lru;
    }
}

static void update_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme)
{
    //  remove pme from list
    remove_lru(pm, pme);
    //  insert_lru(pm, pme)
    insert_lru(pm, pme);
}

static void replace_lru(struct MCTLMap *pm, MCTLMapEntry_t *pme_in)
{
    MCTLMapEntry_t *pme;

    for (pme=pm->lru_tail; pme != NULL; pme = pme->prev_lru) {
        sdftlmap_assert(pme->refcnt >= 0); // xxxzzz remove this!
        if ((pme->refcnt == 0) && (pme != pme_in)) {
	    break;
	}
    }
    if (pme == NULL) {
	fprintf(stderr, "replace_lru could not find a victim!!!!\n");
	sdftlmap_assert(0);
    }

    remove_lru(pm, pme);
    (pm->replacement_callback)(pm->replacement_callback_data, pme->key, pme->keylen, pme->contents, pme->datalen);
    free_pme(pm, pme);

}

static MCTLMapEntry_t *find_pme(struct MCTLMap *pm, char *pkey, uint32_t keylen, MCTLMapBucket_t **pb_out)
{
    uint64_t           h;
    MCTLMapBucket_t  *pb;
    MCTLMapEntry_t   *pme;
    char              *key2;
    key2 = (char *) *((uint64_t *) pkey);

    h = sdf_hash((const unsigned char *) pkey, keylen, 0) % pm->nbuckets;
    pb = &(pm->buckets[h]);

    if (keylen == 8) {
	for (pme = pb->entry; pme != NULL; pme = pme->next) {
	    if (pme->key == key2) {
		break;
	    }
	}
    } else {
        sdftlmap_assert(0);
    }
    if (pb_out) {
        *pb_out = pb;
    }
    return(pme);
}




