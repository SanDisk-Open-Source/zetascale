/*
 * File:   tlmap4.h
 * Author: Brian O'Krafka
 *
 * Created on May 15, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _TLMAP4_H
#define _TLMAP4_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SDFTLMap4Entry {
    uint64_t               contents;
    uint64_t               key;
    struct SDFTLMap4Entry  *next;
} SDFTLMap4Entry_t;

typedef struct SDFTLMap4Bucket {
    SDFTLMap4Entry_t  *entry;
} SDFTLMap4Bucket_t;

typedef struct SDFTLMap4 {
    uint64_t         nbuckets;
    SDFTLMap4Bucket_t *buckets;
    int (*print_fn)(SDFTLMap4Entry_t *pce, char *sout, int max_len);
} SDFTLMap4_t;

extern void SDFTLMap4Init(SDFTLMap4_t *pc, uint64_t nbuckets, 
     int (*print_fn)(SDFTLMap4Entry_t *pce, char *sout, int max_len));
extern SDFTLMap4Entry_t *SDFTLMap4Create(SDFTLMap4_t *pc, uint64_t key);
extern SDFTLMap4Entry_t *SDFTLMap4Get(SDFTLMap4_t *pc, uint64_t key);

#ifdef	__cplusplus
}
#endif

#endif /* _TLMAP4_H */
