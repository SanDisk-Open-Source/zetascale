/*
 * File:   tlmap3.h
 * Author: Brian O'Krafka
 *
 * Created on May 20, 2009
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap3.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef _TLMAP3_H
#define _TLMAP3_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TLMap3Entry {
    void                  *contents;
    char                  *key;
    int                    keylen;
    uint64_t               seqno;
    struct TLMap3Entry    *next;
} TLMap3Entry_t;

typedef struct TLMap3Bucket {
    TLMap3Entry_t  *entry;
} TLMap3Bucket_t;

typedef struct TLMap3 {
    uint64_t         nbuckets;
    TLMap3Bucket_t *buckets;
    int (*print_fn)(TLMap3Entry_t *pce, char *sout, int max_len);
} TLMap3_t;

extern void TLMap3Init(TLMap3_t *pc, uint64_t nbuckets, 
     int (*print_fn)(TLMap3Entry_t *pce, char *sout, int max_len));
extern void TLMap3Destroy(TLMap3_t *pm);
extern TLMap3Entry_t *TLMap3Create(TLMap3_t *pc, char *pkey, int keylen);
extern TLMap3Entry_t *TLMap3Get(TLMap3_t *pc, char *pkey, int keylen);
extern int TLMap3Delete(TLMap3_t *pc, char *pkey, int keylen);
extern TLMap3Entry_t *TLMap3NextEnumeration(TLMap3_t *pc, uint64_t last_bucket_no, uint64_t last_seqno, uint64_t *pnext_bucket_no);

#ifdef	__cplusplus
}
#endif

#endif /* _TLMAP3_H */
