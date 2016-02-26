/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fdf_tlmap.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _ZS_TLMAP_H
#define _ZS_TLMAP_H

#include <stdint.h>
#include <inttypes.h>

struct ZSTLIterator;
struct ZSTLMap;
struct ZSTLMapEntry;

extern struct ZSTLMap *ZSTLMapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
extern void ZSTLMapDestroy(struct ZSTLMap *pm);
extern void ZSTLMapClear(struct ZSTLMap *pm);
extern struct ZSTLMapEntry *ZSTLMapCreate(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct ZSTLMapEntry *ZSTLMapUpdate(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct ZSTLMapEntry *ZSTLMapSet(struct ZSTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
extern struct ZSTLMapEntry *ZSTLMapGet(struct ZSTLMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
extern int ZSTLMapIncrRefcnt(struct ZSTLMap *pm, char *key, uint32_t keylen);
extern void ZSTLMapCheckRefcnts(struct ZSTLMap *pm);
extern int ZSTLMapRelease(struct ZSTLMap *pm, char *key, uint32_t keylen);
extern int ZSTLMapReleaseEntry(struct ZSTLMap *pm, struct ZSTLMapEntry *pme);
extern struct ZSTLIterator *ZSTLMapEnum(struct ZSTLMap *pm);
extern void ZSTLFinishEnum(struct ZSTLMap *pm, struct ZSTLIterator *iterator);
extern int ZSTLMapNextEnum(struct ZSTLMap *pm, struct ZSTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
extern int ZSTLMapDelete(struct ZSTLMap *pm, char *key, uint32_t keylen);

#endif /* _ZS_TLMAP_H */
