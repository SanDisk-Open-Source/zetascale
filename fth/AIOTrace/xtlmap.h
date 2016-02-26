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
 * File:   xtlmap.h
 * Author: Brian O'Krafka
 *
 * Created on October 16, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z briano $
 */

#ifndef _XTLMAP_H
#define _XTLMAP_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDFxTLMapEntry {
    void                  *contents;
    uint64_t               key;
    struct SDFxTLMapEntry  *next;
} SDFxTLMapEntry_t;

typedef struct SDFxTLMapBucket {
    SDFxTLMapEntry_t  *entry;
} SDFxTLMapBucket_t;

typedef struct SDFxTLMap {
    uint64_t         nbuckets;
    SDFxTLMapBucket_t *buckets;
    int (*print_fn)(SDFxTLMapEntry_t *pce, char *sout, int max_len);
} SDFxTLMap_t;

extern void SDFxTLMapInit(SDFxTLMap_t *pc, uint64_t nbuckets, 
     int (*print_fn)(SDFxTLMapEntry_t *pce, char *sout, int max_len));
extern SDFxTLMapEntry_t *SDFxTLMapGetCreate(SDFxTLMap_t *pc, uint64_t key);
extern SDFxTLMapEntry_t *SDFxTLMapGet(SDFxTLMap_t *pc, uint64_t key);

#ifdef	__cplusplus
}
#endif

#endif /* _XTLMAP_H */
