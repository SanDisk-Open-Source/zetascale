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
