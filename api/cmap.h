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
