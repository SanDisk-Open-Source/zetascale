/*
 * File:   sdf_msg_hmap.c
 * Author: Brian O'Krafka, retrofitted a bit for sdf_msg use by tomr
 * Created on September 11, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: tlmap.c 308 2008-02-20 22:34:58Z tomr $
 */

#define SDF_MSG_HMAP_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "platform/stdlib.h"
#include "platform/logging.h"
#include "sdfmsg/sdf_fth_mbx.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdf_msg_hmap.h"
#include "sdf_msg_hash.h"

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG

extern msg_state_t *sdf_msg_rtstate;     /* during runtime track simple msg engine state, allow sends */

struct SDFMSGMap respmsg_tracker;

static void dump_mtime(SDFMSGMapEntry_t *hresp);

void SDFMSGMapInit(SDFMSGMap_t *pm, long long nbuckets,
                   int (*print_fn)(SDFMSGMapEntry_t *pce, char *sout, int max_len)) {
    long long          i;

    pm->nentries      = 0;
    pm->nbuckets      = nbuckets;
    pm->print_fn      = print_fn;
    pm->buckets       = plat_alloc(nbuckets*(sizeof(SDFMSGMapBucket_t)));

    for (i = 0; i < nbuckets; i++) {
        pm->buckets[i].entry = NULL;
    }
}

int SDFMSGMapNEntries(SDFMSGMap_t *pm) {
    return (pm->nentries);
}

SDFMSGMapEntry_t *SDFMSGMapGetCreate(SDFMSGMap_t *pm, char *pkey) {
    int                keylen;
    uint64_t           h;
    SDFMSGMapEntry_t   *pme;
    SDFMSGMapBucket_t  *pb;

    keylen = strlen(pkey);
    h = hash((const unsigned char *)pkey, strlen(pkey), 0) % pm->nbuckets;
    plat_assert(h < NUM_HBUCKETS);
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
        if ((pme->keylen == keylen) &&
            (strcmp((const char *)pme->key, (const char *)pkey) == 0)) {
            break;
        }
    }

    if (pme == NULL) {

        /* Create a new entry. */

        (pm->nentries)++;

        pme = (SDFMSGMapEntry_t *)plat_alloc(sizeof(SDFMSGMapEntry_t));
        if (pme == NULL) {
            (void) fprintf(stderr, "Could not allocate a thread-local map entry.");
            plat_exit(1);
        }

        pme->contents = NULL;
        pme->key      = (char *)plat_alloc(keylen+1);
        strcpy(pme->key, pkey);
        pme->keylen   = keylen;

        /* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return (pme);
}

SDFMSGMapEntry_t *SDFMSGMapCreate(SDFMSGMap_t *pm, char *pkey) {
    int                keylen;
    uint64_t           h;
    SDFMSGMapEntry_t   *pme;
    SDFMSGMapBucket_t  *pb;

    keylen = strlen(pkey);
    h = hash((const unsigned char *)pkey, strlen(pkey), 0) % pm->nbuckets;
    plat_assert(h < NUM_HBUCKETS);
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
        if ((pme->keylen == keylen) &&
            (strcmp((const char *)pme->key, (const char *)pkey) == 0)) {
            return (NULL);
        }
    }

    if (pme == NULL) {

        /* Create a new entry. */

        (pm->nentries)++;

        pme = (SDFMSGMapEntry_t *)plat_alloc(sizeof(SDFMSGMapEntry_t));
        if (pme == NULL) {
            (void) fprintf(stderr, "Could not allocate a thread-local map entry.");
            plat_exit(1);
        }

        pme->contents = NULL;
        pme->key      = (char *)plat_alloc(keylen + 1);
        strcpy(pme->key, pkey);
        pme->keylen   = keylen;

        /* put myself on the bucket list */
        pme->next = pb->entry;
        pb->entry = pme;
    }

    return (pme);
}

SDFMSGMapEntry_t *SDFMSGMapGet(SDFMSGMap_t *pm, char *pkey) {
    int                keylen;
    uint64_t           h;
    SDFMSGMapEntry_t   *pme;
    SDFMSGMapBucket_t  *pb;

    keylen = strlen(pkey);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nHTable Debug pm %p pkey %s keylen %d nbuckets %lld\n", pm, pkey, keylen, pm->nbuckets);

    h = hash((const unsigned char *)pkey, strlen(pkey), 0) % pm->nbuckets;
    plat_assert(h < NUM_HBUCKETS);
    pb = &(pm->buckets[h]);

    for (pme = pb->entry; pme != NULL; pme = pme->next) {
        if ((pme->keylen == keylen) && (strcmp((const char *)pme->key, (const char *)pkey) == 0)) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nHTable Debug pme %p pkey %s \n", pme, pkey);
            break;
        }
    }

    return (pme);
}

/* return 0 if succeeds, 1 if object doesn't exist */
int SDFMSGMapDelete(SDFMSGMap_t *pm, char *pkey) {
    int                keylen;
    uint64_t           h;
    SDFMSGMapEntry_t   **ppme;
    SDFMSGMapEntry_t   *pme;
    SDFMSGMapBucket_t  *pb;

    keylen = strlen(pkey);
    h = hash((const unsigned char *)pkey, strlen(pkey), 0) % pm->nbuckets;
    plat_assert(h < NUM_HBUCKETS);
    pb = &(pm->buckets[h]);

#if 0
    /* Tom's changes that should be deprecated */
    for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nHash Table Delete ppme %p pb %p h %lu\n",
                     ppme, &(pb->entry), h);
	pme = *ppme;
        if ((pme->keylen == keylen) &&
            (strcmp((const char *)pme->key, (const char *)pkey) == 0)) {
 //           *ppme = pme->next;
            (pm->nentries)--;
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                         "\nHash Table Delete ppme %p mkey %s contents %p numentries %d\n",
                         *ppme, pkey, pme, pm->nentries);
            plat_free(pme->key);
            plat_free(pme->contents);
            plat_free(pme);
            pm->buckets[h].entry = NULL;
            return (0);
        }
    }
#else
/* Brians original function with the added numentries accounting */
    for (ppme = &(pb->entry); (*ppme) != NULL; ppme = &((*ppme)->next)) {
	pme = *ppme;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nHash Table Delete ppme %p pb %p h %lu\n",
                     ppme, &(pb->entry), h);
        if ((pme->keylen == keylen) &&
            (strcmp((const char *)pme->key, (const char *)pkey) == 0)) {
            *ppme = pme->next;
            (pm->nentries)--;
	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nHash Table Delete ppme %p mkey %s contents %p remaining numentries %d\n",
                         ppme, pkey, *ppme, pm->nentries);
            plat_free(pme->key);
            plat_free(pme->contents);
            plat_free(pme);
            return (0);
        }
    }
#endif
    return (1);
}


void
SDFMSGMapEnumerate(SDFMSGMap_t *pm) {
    SDFMSGMapEntry_t    *pme = NULL;
    SDFMSGMapBucket_t   *pb;
    uint64_t             nb;

    for (nb = 0; nb < pm->nbuckets; nb++) {
        pb = &(pm->buckets[nb]);
        pme = pb->entry;
        if (pme != NULL) {
            break;
        }
    }
    pm->enum_bucket = nb;
    pm->enum_entry  = pme;
}


SDFMSGMapEntry_t *
SDFMSGMapNextEnumeration(SDFMSGMap_t *pm) {
    SDFMSGMapEntry_t   *pme_return, *pme;
    SDFMSGMapBucket_t  *pb;
    uint64_t            nb;

    if (pm->enum_entry == NULL) {
        return (NULL);
    }
    pme_return = pm->enum_entry;

    if (pme_return->next != NULL) {
        pm->enum_entry = pme_return->next;
        return (pme_return);
    }

    pme = NULL;
    for (nb = pm->enum_bucket + 1; nb < pm->nbuckets; nb++) {
        pb = &(pm->buckets[nb]);
        pme = pb->entry;
        if (pme != NULL) {
            break;
        }
    }
    pm->enum_bucket = nb;
    pm->enum_entry  = pme;

    return (pme_return);
}


/**
 * brief@ sdf_msg_setuphash() basic setup of hash table to decouple the
 * fth mailbox pointers in a response message request and returned response
 * the total key-value buckets need to be sized appropriately for the useage
 * simple test and delete case is done here for return status
 * return@ 0 for success 1 for failure
 */

int sdf_msg_setuphash(uint32_t myid, uint32_t num_buckts) {
    SDFMSGMapEntry_t *testit, *checkit;
    char dummy[17] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    uint64_t tstptr = 0xdeadbeef12345678;

    msg_resptrkr_t *crpy =  plat_alloc(sizeof(struct msg_resptrkr));

    /* dump in some meaningless values for the msg tracker */
    crpy->msg_timeout = 2;
    crpy->msg_flags = 0xdead;
    crpy->msg_basetimestamp = 0x12345678feedface;
    sprintf(crpy->mkey, "%lx", tstptr); /* drop in a 64bit pointer value */
    (void) strcpy(dummy, crpy->mkey);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Setup HASH TABLE with num_bkts %d dummy key - %s\n",
                 myid, num_buckts, dummy);

    /* set up the table with count of num_buckts */
    SDFMSGMapInit(&(respmsg_tracker), num_buckts, NULL);

    /* lets do a simple test using the dummy key */
    testit = SDFMSGMapGetCreate(&respmsg_tracker, dummy);
    /* check the number of entries after creation */
    if (SDFMSGMapNEntries(&respmsg_tracker) != 1) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: number of entries != 1... FAILED\n", myid);
        return (1);
    }

    testit->contents = crpy;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: entry %p contents %p has mkey %s = to the one we sent with len %d -> %s", myid,
                 testit, testit->contents, testit->contents->mkey, testit->keylen, dummy);
    if (strcmp(testit->contents->mkey, dummy) == 0) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: entry %p created and verifed", myid, testit);
    } else {
        int ret1 = strcmp(testit->contents->mkey, dummy);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: entry created FAILED ret = %d", myid, ret1);
        return (ret1);
    }
    /* lets check the enum -- get that first spot */
    SDFMSGMapEnumerate(&respmsg_tracker);
    /* let's check the enum, should only have one enrty */
    checkit = SDFMSGMapNextEnumeration(&respmsg_tracker);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Hash Table Next Enum entry %p has ts %lu does it =  testit ts %lu\n", myid,
                 checkit, checkit->contents->msg_basetimestamp, testit->contents->msg_basetimestamp);
    plat_assert(checkit->contents->msg_basetimestamp == testit->contents->msg_basetimestamp);
    /* look for the next one that should be null */
    checkit = SDFMSGMapNextEnumeration(&respmsg_tracker);
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: Next Hash Table Enum checkit %p should be NULL\n", myid, checkit);
    plat_assert(checkit == NULL);
    /* now check the delete */
    int ret = SDFMSGMapDelete(&respmsg_tracker, dummy);
    if (!ret) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Delete key %s was successful ret %d\n", myid, dummy, ret);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: Delete key %s failed ret %d\n", myid, dummy, ret);
    }
    SDFMSGMapEnumerate(&respmsg_tracker);
    if (SDFMSGMapNextEnumeration(&respmsg_tracker) != NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: hash entry was deleted but enum check in not NULL\n", myid);
    }

    return (ret);
}


/**
 * @brief sdf_msg_resptag() alloc the response msg hash struct and fill it in
 * @returns the MapEntry
 */

SDFMSGMapEntry_t *
sdf_msg_resptag(struct sdf_msg *msg) {
    SDFMSGMapEntry_t *testit;
    char buf1[MSG_KEYSZE];
    seqnum_t mval = 0xffffffffffffffff;
    struct timeval cur_time;

    gettimeofday(&cur_time, NULL);

    msg_resptrkr_t *crpy =  plat_alloc(sizeof(struct msg_resptrkr));

    /*
     * use the incrementing msg_conversation to decrement the default key
     *
     * FIXME the default key should be created from a random number so
     * that on restarts we do not have a conflict. Also a move to a
     * binary hash would be performant.
     */
    mval -= msg->msg_conversation;
    sprintf(buf1, "%lx", mval);
    buf1[MSG_KEYSZE - 1] = '\0';
    testit = SDFMSGMapGetCreate(&respmsg_tracker, buf1);
    if (!testit) {
        plat_assert(testit);
    }

    testit->contents = crpy; /* set the key-value to the tracker */

    strcpy(crpy->mkey, buf1);
    crpy->mkey[MSG_KEYSZE - 1] = '\0';
    crpy->msg_timeout = cur_time.tv_sec;  /* grab the time in secs */
    crpy->msg_mstimeout = cur_time.tv_usec;  /* grab the time in usecs */
    crpy->respmbx = msg->akrpmbx->rbox;   /* the specific respmbx */
    crpy->ar_mbx = msg->akrpmbx;          /* the acknowledge response mbox struct */
    crpy->ar_mbx_from_req = msg->akrpmbx_from_req;  /* the returned mbox struct */
    crpy->msg_flags = msg->msg_flags;             /* save the msg_flags */
    crpy->msg_basetimestamp = msg->msg_timestamp;
    crpy->fthid = (uint64_t)fthId();
    crpy->msg_seqnum = msg->msg_conversation;
    crpy->msg_src_service = msg->msg_src_service;
    crpy->msg_dest_service = msg->msg_dest_service;
    crpy->msg_src_vnode = msg->msg_src_vnode;
    crpy->msg_dest_vnode = msg->msg_dest_vnode;
    crpy->msg_elaptm = 0;
    crpy->nxtmkr = 0;                     /* the interval marker flag for timemkr */
    plat_assert(sdf_msg_rtstate->mtime->timemkr <= crpy->msg_timeout);
    /* increment the tracking counters upon sends */
    if (crpy->msg_timeout == sdf_msg_rtstate->mtime->timemkr) {
        sdf_msg_rtstate->mtime->mcnts[C_CNT]++;
    } else if (crpy->msg_timeout == sdf_msg_rtstate->mtime->ntimemkr) {
        sdf_msg_rtstate->mtime->mcnts[N_CNT]++;
        crpy->nxtmkr = 1;
    } else {
        sdf_msg_rtstate->mtime->mcnts[A_CNT]++;
    }
    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: msg %p hresp %p dn %d mkey %s klen %d ar_mbx %p\n"
                 "        actual rbox %p hseqnum %lu msg_timeout %li ptimemkr %lu timemkr %lu ntimemkr %lu\n"
                 "        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n",
                 crpy->msg_src_vnode, msg, testit, crpy->msg_dest_vnode, crpy->mkey,
                 testit->keylen, crpy->ar_mbx, crpy->respmbx, crpy->msg_seqnum,
                 crpy->msg_timeout, sdf_msg_rtstate->mtime->ptimemkr,
                 sdf_msg_rtstate->mtime->timemkr, sdf_msg_rtstate->mtime->ntimemkr,
                 sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                 sdf_msg_rtstate->mtime->mcnts[C_CNT],
                 sdf_msg_rtstate->mtime->mcnts[A_CNT],
                 sdf_msg_rtstate->mtime->mcnts[N_CNT]);

    return (testit);
}

/*
 * sdf_msg_hashchk() we're here cause a msg response has returned and the mkey match has pulled the
 * hashed data struct that was stored when the msg was sent. The msg header gets back the mbx struct
 * pointer that was stored and it is reloaded into the msg header.
 * The initial timestamp in ns is used to calc the elasped time of the response itself
 */

int
sdf_msg_hashchk(SDFMSGMapEntry_t *hresp, struct sdf_msg *msg) {

    /* this is calculated in ns and we have a .5sec runway */
    hresp->contents->msg_elaptm = show_dtime(hresp->contents->msg_basetimestamp);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: HASH Retrive msg %p mhash %p hrmbx %p msg_timeout %lu hseqnum %lu\n"
                 "        har_mbx %p = mar_mbx %p hmbx_req %p = mmbx_req %p diff time %lu ms\n"
                 "        mkey %s ptimemkr %lu timemkr %lu ntimemkr %lu\n"
                 "        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n",
                 msg->msg_dest_vnode, msg, hresp,
                 hresp->contents->respmbx, hresp->contents->msg_timeout,
                 hresp->contents->msg_seqnum, hresp->contents->ar_mbx, msg->akrpmbx,
                 hresp->contents->ar_mbx_from_req, msg->akrpmbx_from_req,
                 hresp->contents->msg_elaptm/1000000, msg->mkey,
                 sdf_msg_rtstate->mtime->ptimemkr, sdf_msg_rtstate->mtime->timemkr,
                 sdf_msg_rtstate->mtime->ntimemkr,
                 sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                 sdf_msg_rtstate->mtime->mcnts[C_CNT],
                 sdf_msg_rtstate->mtime->mcnts[A_CNT],
                 sdf_msg_rtstate->mtime->mcnts[N_CNT]);


    msg->akrpmbx_from_req = hresp->contents->ar_mbx;

    /*
     * TOBEDEPRECATED this is a simple check for now to validate the hashed rbox with the
     * mbox that was transmitted this is a stepping stone before eliminating the over the
     * wire mbx completely
     */
    if ((hresp->contents->respmbx != msg->akrpmbx_from_req->rbox) ||
        (hresp->contents->ar_mbx != msg->akrpmbx_from_req)) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: HASH ERROR hrmbx %p != respmbox %p\n", msg->msg_dest_vnode,
                     hresp->contents->respmbx, msg->akrpmbx_from_req->rbox);
        plat_assert(0);
    }

    /*
     * responses received increment counters to faciliate faster checking for messages that could be in
     * timeout territory, we check for a negative value and assert if this is the case
     */

    if (hresp->contents->msg_timeout <= sdf_msg_rtstate->mtime->ptimemkr) {
        (sdf_msg_rtstate->mtime->mcnts[PC_CNT]) ? sdf_msg_rtstate->mtime->mcnts[PC_CNT]-- : dump_mtime(hresp);
    } else if (hresp->contents->msg_timeout < sdf_msg_rtstate->mtime->timemkr) {
        (sdf_msg_rtstate->mtime->mcnts[PA_CNT]) ? sdf_msg_rtstate->mtime->mcnts[PA_CNT]-- : dump_mtime(hresp);
    } else if (hresp->contents->msg_timeout == sdf_msg_rtstate->mtime->timemkr) {
        /* we could have had a interval transition so lets look at the flag */
        if (hresp->contents->nxtmkr) {
            (sdf_msg_rtstate->mtime->mcnts[PN_CNT]) ? sdf_msg_rtstate->mtime->mcnts[PN_CNT]-- : dump_mtime(hresp);
        } else {
            (sdf_msg_rtstate->mtime->mcnts[C_CNT]) ? sdf_msg_rtstate->mtime->mcnts[C_CNT]-- : dump_mtime(hresp);
        }
    } else if (hresp->contents->msg_timeout == sdf_msg_rtstate->mtime->ntimemkr) {
        (sdf_msg_rtstate->mtime->mcnts[N_CNT]) ? sdf_msg_rtstate->mtime->mcnts[N_CNT]-- : dump_mtime(hresp);
    } else {
        (sdf_msg_rtstate->mtime->mcnts[A_CNT]) ? sdf_msg_rtstate->mtime->mcnts[A_CNT]-- : dump_mtime(hresp);
    }
    return (0);

}

/* dump the data before asserting if we have a conflict in the counters */

static void
dump_mtime(SDFMSGMapEntry_t *hresp) {
    struct timeval cur_time;

    gettimeofday(&cur_time, NULL);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                 "\nNode %d: hresp_seqnum %lu tmout %d\n"
                 "        timemkr %lu ntimemkr %lu ptimemkr %lu diff %lu\n"
                 "        msg_timeout %lu actual tm %lu usec %lu\n"
                 "        PC_CNT %d PA_CNT %d PN_CNT %d C_CNT %d A_CNT %d N_CNT %d\n",
                 sdf_msg_rtstate->myid, hresp->contents->msg_seqnum,
                 sdf_msg_rtstate->mtime->tmout,
                 sdf_msg_rtstate->mtime->timemkr,
                 sdf_msg_rtstate->mtime->ntimemkr, sdf_msg_rtstate->mtime->ptimemkr,
                 sdf_msg_rtstate->mtime->diffmkr,
                 hresp->contents->msg_timeout,
                 cur_time.tv_sec, cur_time.tv_usec,
                 sdf_msg_rtstate->mtime->mcnts[PC_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PA_CNT],
                 sdf_msg_rtstate->mtime->mcnts[PN_CNT],
                 sdf_msg_rtstate->mtime->mcnts[C_CNT],
                 sdf_msg_rtstate->mtime->mcnts[A_CNT],
                 sdf_msg_rtstate->mtime->mcnts[N_CNT]);
    plat_assert(0);

}
