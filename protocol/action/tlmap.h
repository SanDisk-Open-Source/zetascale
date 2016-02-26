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
 * File:   tlmap.h
 * Author: Brian O'Krafka
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _TLMAP_H
#define _TLMAP_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDFTLMapEntry {
    void                  *contents;
    char                  *key;
    int                    keylen;
    struct SDFTLMapEntry  *next;
} SDFTLMapEntry_t;

typedef struct SDFTLMapBucket {
    SDFTLMapEntry_t  *entry;
} SDFTLMapBucket_t;

typedef struct SDFTLMap {
    uint64_t         nbuckets;
    SDFTLMapBucket_t *buckets;
    int (*print_fn)(SDFTLMapEntry_t *pce, char *sout, int max_len);
} SDFTLMap_t;

extern void SDFTLMapInit(SDFTLMap_t *pc, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMapEntry_t *pce, char *sout, int max_len));
extern SDFTLMapEntry_t *SDFTLMapGetCreate(SDFTLMap_t *pc, char *pkey);

#ifdef	__cplusplus
}
#endif

#endif /* _TLMAP_H */
