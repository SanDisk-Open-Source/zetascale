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
 *    - Unlike tlmap in the zs directory, zs_tlmap does NOT
 *      automatically malloc and free the key and contest of
 *      a hashtable entry!
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

#include "btree_map.h"
#include "btree_hash.h"
#include <assert.h>

#ifndef _OPTIMIZE
#define map_assert(x) assert(x)
#else
#define map_assert(x)
#endif

struct PMap {
    struct Map** parts;
    uint32_t nparts;
};

struct PMap *PMapInit(uint64_t nparts, uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen))
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
        pm->parts[i] = MapInit(nbuckets, max_entries, 1, replacement_callback);
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

void PMapDestroy(struct PMap **pm)
{
    uint64_t i;

    for (i = 0; i < (*pm)->nparts; i++) {
        MapDestroy((*pm)->parts[i]);
	}
	free((*pm)->parts);
	free(*pm);
	*pm = NULL;
}

void PMapClean(struct PMap **pm, uint64_t cguid, void *replacement_callback_data)
{
	uint64_t i;

	for (i = 0; i < (*pm)->nparts; i++) {
		MapClean((*pm)->parts[i], cguid, replacement_callback_data);
	}
}

inline static
struct Map* p(struct PMap *pm, char* pkey, uint32_t keylen, uint64_t cguid)
{
    uint64_t idx = btree_hash((const unsigned char *) pkey, keylen, 0, cguid) % pm->nparts;

    return pm->parts[idx];
}

//  Return non-NULL if success, NULL if object exists
struct MapEntry *PMapCreate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapCreate(p(pm, pkey, keylen, cguid), pkey, keylen, pdata, datalen, cguid, robj, replacement_callback_data);
}

//  Return non-NULL if success, NULL if object does not exist
struct MapEntry *PMapUpdate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapUpdate(p(pm, pkey, keylen, cguid), pkey, keylen, pdata, datalen, cguid, robj, replacement_callback_data);
}

//  Return non-NULL if success, NULL if object exists
struct MapEntry *PMapSet(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapSet(p(pm, pkey, keylen, cguid), pkey, keylen, pdata, datalen, old_pdata, old_datalen, cguid, robj, replacement_callback_data);
}

//  Returns non-NULL if successful, NULL otherwise
struct MapEntry *PMapGet(struct PMap *pm, char *key, uint32_t keylen, char **pdata, uint64_t *pdatalen, uint64_t cguid, int robj)
{
    return MapGet(p(pm, key, keylen, cguid), key, keylen, pdata, pdatalen, cguid, robj);
}

//  Increment the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int PMapGetRefcnt(struct PMap *pm, char *key, uint32_t keylen, uint64_t cguid, int robj)
{
    return MapGetRefcnt(p(pm, key, keylen, cguid), key, keylen, cguid, robj);
}
//  Increment the reference count for this entry
//  rc=1 if entry is found, rc=0 otherwise
int PMapIncrRefcnt(struct PMap *pm, char *key, uint32_t keylen, uint64_t cguid, int robj)
{
    return MapIncrRefcnt(p(pm, key, keylen, cguid), key, keylen, cguid, robj);
}

//  rc=1 if entry is found, rc=0 otherwise
int PMapRelease(struct PMap *pm, char *key, uint32_t keylen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapRelease(p(pm, key, keylen, cguid), key, keylen, cguid, robj, replacement_callback_data);
}

int PMapReleaseAll(struct PMap *pm, char *key, uint32_t keylen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapReleaseAll(p(pm, key, keylen, cguid), key, keylen, cguid, robj, replacement_callback_data);
}

/*   Return 0 if succeeds, 1 if object doesn't exist.
 */
int PMapDelete(struct PMap *pm, char *key, uint32_t keylen, uint64_t cguid, int robj, void *replacement_callback_data)
{
    return MapDelete(p(pm, key, keylen, cguid), key, keylen, cguid, robj, replacement_callback_data);
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

