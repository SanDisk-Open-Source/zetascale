/*
 * File:   btree_map.c
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * IMPORTANT NOTES:
 *    - Unlike tlmap in the fdf directory, fdf_tlmap does NOT
 *      automatically malloc and free the key and contest of
 *      a hashtable entry!
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

#include "btree_map.h"
#include "btree_hash.h"
#include <assert.h>

#define map_assert(x) assert(x)

struct PMap {
    struct Map** parts;
    uint32_t nparts;
};

struct PMap *PMapInit(uint64_t nparts, uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data)
{
    uint64_t i;
    struct PMap *pm;

    pm = (struct PMap *) malloc(sizeof(struct PMap));
    map_assert(pm);

    pm->nparts = nparts;
    pm->parts = (struct Map **) malloc(nparts * sizeof(struct Map*));
    map_assert(pm->parts);

    map_assert(use_locks);

    for (i=0; i<pm->nparts; i++) {
        pm->parts[i] = MapInit(nbuckets, max_entries, 1, replacement_callback, replacement_callback_data);
        map_assert(pm->parts[i]);
    }

    return(pm);
}

uint64_t PMapNEntries(struct PMap *pm)
{
    uint64_t i, n_entries = 0;

    for (i = 0; i < pm->nparts; i++)
        n_entries += MapNEntries(pm->parts[i]);

    return n_entries;
}

void PMapDestroy(struct PMap *pm)
{
    uint64_t i;

    for (i = 0; i < pm->nparts; i++)
        MapDestroy(pm->parts[i]);
}

inline static
struct Map* p(struct PMap *pm, char* pkey, uint32_t keylen)
{
    uint64_t idx = btree_hash((const unsigned char *) pkey, keylen, 0) % pm->nparts;

    return pm->parts[idx];
}

//  Return non-NULL if success, NULL if object exists
struct MapEntry *PMapCreate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen)
{
    return MapCreate(p(pm, pkey, keylen), pkey, keylen, pdata, datalen);
}

//  Return non-NULL if success, NULL if object does not exist
struct MapEntry *PMapUpdate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen)
{
    return MapUpdate(p(pm, pkey, keylen), pkey, keylen, pdata, datalen);
}

//  Return non-NULL if success, NULL if object exists
struct MapEntry *PMapSet(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen)
{
    return MapSet(p(pm, pkey, keylen), pkey, keylen, pdata, datalen, old_pdata, old_datalen);
}

//  Returns non-NULL if successful, NULL otherwise
struct MapEntry *PMapGet(struct PMap *pm, char *key, uint32_t keylen, char **pdata, uint64_t *pdatalen)
{
    return MapGet(p(pm, key, keylen), key, keylen, pdata, pdatalen);
}

//  Increment the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int PMapGetRefcnt(struct PMap *pm, char *key, uint32_t keylen)
{
    return MapGetRefcnt(p(pm, key, keylen), key, keylen);
}
//  Increment the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int PMapIncrRefcnt(struct PMap *pm, char *key, uint32_t keylen)
{
    return MapIncrRefcnt(p(pm, key, keylen), key, keylen);
}

//  Decrement the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int PMapRelease(struct PMap *pm, char *key, uint32_t keylen)
{
    return MapRelease(p(pm, key, keylen), key, keylen);
}

/*   Return 0 if succeeds, 1 if object doesn't exist.
 */
int PMapDelete(struct PMap *pm, char *key, uint32_t keylen)
{
    return MapDelete(p(pm, key, keylen), key, keylen);
}

#if 0
//  Decrement the reference count for this entry
int PMapReleaseEntry(struct PMap *pm, struct MapEntry *pme)
{
}

struct PIterator *PMapEnum(struct PMap *pm)
{

}

void FinishEnum(struct PMap *pm, struct PIterator *iterator)
{
}

//  Returns 1 if successful, 0 otherwise
//  Caller is responsible for freeing key and data
int PMapNextEnum(struct PMap *pm, struct PIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen) 
{
}
#endif

