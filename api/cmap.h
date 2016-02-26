/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   cmap.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _CMAP_H
#define _CMAP_H

#include <stdint.h>
#include <inttypes.h>

struct CMapIterator;
struct CMap;
struct CMapEntry;

extern struct CMap *CMapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data, 
								void (*delete_callback)(void *));
extern void CMapDestroy(struct CMap *pm);
extern void CMapClear(struct CMap *pm);
extern struct CMapEntry *CMapCreate(struct CMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct CMapEntry *CMapUpdate(struct CMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct CMapEntry *CMapSet(struct CMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
extern struct CMapEntry *CMapGet(struct CMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
extern int CMapIncrRefcnt(struct CMap *pm, char *key, uint32_t keylen);
extern int CMapGetRefcnt(struct CMap *pm, char *key, uint32_t keylen);
extern void CMapCheckRefcnts(struct CMap *pm);
extern int CMapRelease(struct CMap *pm, char *key, uint32_t keylen);
extern int CMapRelease_fix(struct CMapEntry *pme);
extern int CMapReleaseEntry(struct CMap *pm, struct CMapEntry *pme);
extern struct CMapIterator *CMapEnum(struct CMap *pm);
extern void CMapFinishEnum(struct CMap *pm, struct CMapIterator *iterator);
extern int CMapNextEnum(struct CMap *pm, struct CMapIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
extern int CMapDelete(struct CMap *pm, char *key, uint32_t keylen);
extern uint64_t CMapNEntries(struct CMap *pm);

#endif /* _CMAP_H */
