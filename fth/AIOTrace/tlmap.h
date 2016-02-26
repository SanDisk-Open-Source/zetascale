/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
extern SDFTLMapEntry_t *SDFTLMapGet(SDFTLMap_t *pc, char *pkey);

#ifdef	__cplusplus
}
#endif

#endif /* _TLMAP_H */
