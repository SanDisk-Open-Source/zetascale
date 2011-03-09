/*
 * File:   sdf/sdf_msg_hmap.h
 * Author: Brian O'Krafka, altered for sdfmsg usage
 *
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.h 308 2008-02-20 22:34:58Z tomr $
 */

#ifndef SDF_MSG_HMAP_H
#define SDF_MSG_HMAP_H

#ifdef __cplusplus
extern "C" {
#endif

/* base struct */
typedef struct SDFMSGMapEntry {
    struct msg_resptrkr   *contents;
    char                  *key;
    int                    keylen;
    struct SDFMSGMapEntry  *next;
} SDFMSGMapEntry_t;

typedef struct SDFMSGMapBucket {
    SDFMSGMapEntry_t  *entry;
} SDFMSGMapBucket_t;


/* here is your basic struct that defines the buckets & entries */
typedef struct SDFMSGMap {
    int               nentries;
    long long         nbuckets;
    SDFMSGMapBucket_t *buckets;
    int (*print_fn)(SDFMSGMapEntry_t *pce, char *sout, int max_len);
    long long         enum_bucket;
    SDFMSGMapEntry_t *enum_entry;
} SDFMSGMap_t;

void SDFMSGMapInit(SDFMSGMap_t *pc, long long nbuckets,
                   int (*print_fn)(SDFMSGMapEntry_t *pce, char *sout, int max_len));
SDFMSGMapEntry_t *SDFMSGMapGetCreate(SDFMSGMap_t *pc, char *pkey);
SDFMSGMapEntry_t *SDFMSGMapCreate(SDFMSGMap_t *pc, char *pkey);
SDFMSGMapEntry_t *SDFMSGMapGet(SDFMSGMap_t *pc, char *pkey);
int SDFMSGMapDelete(SDFMSGMap_t *pc, char *pkey);
int SDFMSGMapNEntries(SDFMSGMap_t *pc);
void SDFMSGMapEnumerate(SDFMSGMap_t *pc);
SDFMSGMapEntry_t *SDFMSGMapNextEnumeration(SDFMSGMap_t *pc);

int sdf_msg_setuphash(uint32_t myid, uint32_t num_buckts);
SDFMSGMapEntry_t *sdf_msg_resptag(struct sdf_msg *msg);
int sdf_msg_hashchk(SDFMSGMapEntry_t *hresp, struct sdf_msg *msg);

#ifdef __cplusplus
}
#endif

#endif /* SDF_MSG_HMAP_H */
