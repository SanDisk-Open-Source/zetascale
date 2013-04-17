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

#ifndef _FDF_TLMAP_H
#define _FDF_TLMAP_H

#include <stdint.h>
#include <inttypes.h>

struct FDFTLIterator;
struct FDFTLMap;
struct FDFTLMapEntry;

extern struct FDFTLMap *FDFTLMapInit(uint64_t nbuckets, uint64_t max_entries, char use_locks, void (*replacement_callback)(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen), void *replacement_callback_data);
extern void FDFTLMapDestroy(struct FDFTLMap *pm);
extern void FDFTLMapClear(struct FDFTLMap *pm);
extern struct FDFTLMapEntry *FDFTLMapCreate(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct FDFTLMapEntry *FDFTLMapUpdate(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen);
extern struct FDFTLMapEntry *FDFTLMapSet(struct FDFTLMap *pm, char *pkey, uint32_t keylen, char *pdata, uint64_t datalen, char **old_pdata, uint64_t *old_datalen);
extern struct FDFTLMapEntry *FDFTLMapGet(struct FDFTLMap *pc, char *key, uint32_t keylen, char** data, uint64_t *pdatalen);
extern int FDFTLMapIncrRefcnt(struct FDFTLMap *pm, char *key, uint32_t keylen);
extern void FDFTLMapCheckRefcnts(struct FDFTLMap *pm);
extern int FDFTLMapRelease(struct FDFTLMap *pm, char *key, uint32_t keylen);
extern int FDFTLMapReleaseEntry(struct FDFTLMap *pm, struct FDFTLMapEntry *pme);
extern struct FDFTLIterator *FDFTLMapEnum(struct FDFTLMap *pm);
extern void FDFTLFinishEnum(struct FDFTLMap *pm, struct FDFTLIterator *iterator);
extern int FDFTLMapNextEnum(struct FDFTLMap *pm, struct FDFTLIterator *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen);
extern int FDFTLMapDelete(struct FDFTLMap *pm, char *key, uint32_t keylen);

#endif /* _FDF_TLMAP_H */
