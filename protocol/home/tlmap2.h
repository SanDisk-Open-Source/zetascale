/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   tlmap2.h
 * Author: Brian O'Krafka
 *
 * Created on May 15, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _TLMAP2_H
#define _TLMAP2_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDFTLMap2Entry {
    void                  *contents;
    uint64_t               key;
    struct SDFTLMap2Entry  *next;
} SDFTLMap2Entry_t;

typedef struct SDFTLMap2Bucket {
    SDFTLMap2Entry_t  *entry;
} SDFTLMap2Bucket_t;

typedef struct SDFTLMap2 {
    uint64_t         nbuckets;
    SDFTLMap2Bucket_t *buckets;
    int (*print_fn)(SDFTLMap2Entry_t *pce, char *sout, int max_len);
} SDFTLMap2_t;

extern void SDFTLMap2Init(SDFTLMap2_t *pc, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMap2Entry_t *pce, char *sout, int max_len));
extern void SDFTLMap2Destroy(SDFTLMap2_t *pc);
extern SDFTLMap2Entry_t *SDFTLMap2Create(SDFTLMap2_t *pc, uint64_t key);
extern int SDFTLMap2Delete(SDFTLMap2_t *pc, uint64_t key);
extern SDFTLMap2Entry_t *SDFTLMap2Get(SDFTLMap2_t *pc, uint64_t key);

#ifdef	__cplusplus
}
#endif

#endif /* _TLMAP2_H */
