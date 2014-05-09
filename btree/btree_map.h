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

#ifndef _BTREE_MAP_H
#define _BTREE_MAP_H

#include <stdint.h>
#include <inttypes.h>

struct Iterator;
struct Map;
struct MapEntry;

extern struct Map *MapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen));
extern void MapDestroy(struct Map *pm);
extern void MapClean(struct Map *pm, uint64_t cguid, void *replacement_callback_data);
extern void MapClear(struct Map *pm);
extern struct MapEntry *MapCreate(struct Map *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, uint64_t cguid, void *replacement_callback_data);
extern struct MapEntry *MapUpdate(struct Map *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, uint64_t cguid, void *replacement_callback_data);
extern struct MapEntry *MapSet(struct Map *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen, uint64_t cguid, void *replacement_callback_data);
extern struct MapEntry *MapGet(struct Map *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen, uint64_t cguid);
extern int MapIncrRefcnt(struct Map *pm, char *key, uint32_t keylen, uint64_t cguid);
extern int MapGetRefcnt(struct Map *pm, char *key, uint32_t keylen, uint64_t cguid);
extern void MapCheckRefcnts(struct Map *pm);
extern int MapRelease(struct Map *pm, char *key, uint32_t keylen, uint64_t cguid, void *replacement_callback_data);
extern int MapReleaseAll(struct Map *pm, char *key, uint32_t keylen, uint64_t cguid, void *replacement_callback_data);
extern int MapReleaseEntry(struct Map *pm, struct MapEntry *pme);
extern struct Iterator *MapEnum(struct Map *pm);
extern void FinishEnum(struct Map *pm, struct Iterator *iterator);
extern int MapNextEnum(struct Map *pm, struct Iterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen, uint64_t cguid);
extern int MapDelete(struct Map *pm, char *key, uint32_t keylen, uint64_t cguid, void *replacement_callback_data);
extern uint64_t MapNEntries(struct Map *pm);

#endif /* _BTREE_MAP_H */
