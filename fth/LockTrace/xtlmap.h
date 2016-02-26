/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
