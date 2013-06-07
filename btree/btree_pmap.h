/*
 * File:   btree_map.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _BTREE_PMAP_H
#define _BTREE_PMAP_H

#include <stdint.h>
#include <inttypes.h>

#include "btree_map.h"

struct Iterator;
struct PMap;

extern struct PMap *PMapInit(uint32_t nparts, uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
extern void PMapDestroy(struct PMap *pm);
extern void PMapClear(struct PMap *pm);
extern struct MapEntry *PMapCreate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct MapEntry *PMapUpdate(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct MapEntry *PMapSet(struct PMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
extern struct MapEntry *PMapGet(struct PMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
extern int PMapIncrRefcnt(struct PMap *pm, char *key, uint32_t keylen);
extern int PMapGetRefcnt(struct PMap *pm, char *key, uint32_t keylen);
extern void PMapCheckRefcnts(struct PMap *pm);
extern int PMapRelease(struct PMap *pm, char *key, uint32_t keylen);
#if 0
extern int PMapReleaseEntry(struct PMap *pm, struct MapEntry *pme);
extern struct Iterator *PMapEnum(struct PMap *pm);
extern void FinishEnum(struct PMap *pm, struct Iterator *iterator);
extern int PMapNextEnum(struct PMap *pm, struct Iterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
#endif
extern int PMapDelete(struct PMap *pm, char *key, uint32_t keylen);
extern uint64_t PMapNEntries(struct PMap *pm);

#endif /* _BTREE_MAP_H */
