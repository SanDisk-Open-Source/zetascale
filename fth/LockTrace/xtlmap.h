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
