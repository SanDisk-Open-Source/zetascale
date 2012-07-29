/*
 * File: ht_winner_lose_win.c
 * Description:
 * Fill candidate lists with keys, and then these hot keys should be
 * stated from reporter. While once the if higher candidate arrived,
 * the winner with smallest ref-count should be shift to candidate list
 * again.
 * What's more, if the lose winner is visited with higher ref-count
 * later, it should kick-off the "last winner" to candidate list again.
 *
 * Author:  Hickey Liu Gengliang Wang
 */

#include "ht_report.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <inttypes.h>
#include "fth/fth.h"

#define SHM_SIZE         8 * 1024 *1024
#define NUM_PTHREADS     4

#define hashsize(x)      (x)
#define KEY_LEN          (64)
#define KEY_NUM          (64*hashsize(16))
#define NCANDIDATES      (16)
#define NKEY_PER_WINNER_LIST  (4)

struct _key {
    char key_str[KEY_LEN+1];
    uint64_t syndrome;
    int bucket;
    int count;
} key;

void
gen_str(char *key) {
    uint64_t i;
    for (i = 0; i < KEY_LEN; i++) {
        key[i] = random() % 26 + 'A';
    }
    key[i] = '\0';
}

int nfailed         = 0;
int done            = 0;

void threadTest(uint64_t arg) {
    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(4);
    int loops           = NCANDIDATES;

    int mm_size;
    void *buf;
    struct _key keys[NCANDIDATES];
    struct _key big_key, small_key;

    char *recv = (char *) plat_alloc(300*top);
    if (recv == NULL) {
        perror("failed to alloc");
    }

    mm_size = calc_hotkey_memory(maxtop, nbuckets, 0);
    buf     = plat_alloc(mm_size);
    Reporter_t *rpt;

    for (int i = 0; i < NCANDIDATES; i++) {
        gen_str(keys[i].key_str);
        keys[i].syndrome     = 10000 + i;
        keys[i].bucket       = i % 4;
    }

    gen_str(big_key.key_str);
    big_key.syndrome     = 10000 + NCANDIDATES - 1;
    big_key.bucket       = (NCANDIDATES - 1) % 4;
    gen_str(small_key.key_str);
    small_key.syndrome    = 10000 + 65536;
    small_key.bucket      = (NCANDIDATES - 1) % 4;


    rpt = hot_key_init(buf, mm_size, nbuckets, maxtop, 0);

    int ip = 127001;

    for (int i = 0; i < 2; i++) {
        loops = NCANDIDATES;
        while (loops--) {
            hot_key_update(rpt, keys[loops].key_str, KEY_LEN+1,
                           keys[loops].syndrome, keys[loops].bucket,
                           1+random()%11, ip);
        }
    }

    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    printf("keys:\n%s", recv);

    loops = NCANDIDATES;
    while (loops--) {
        if (!strstr(recv, keys[loops].key_str) &&
            loops >= NCANDIDATES - NKEY_PER_WINNER_LIST) {
            printf("fail to set key %s\n", keys[loops].key_str);
        }
    }

    // at this moment, winner list's threshold is updated and it should
    // forbiddn lower ref-count keys to be winner
    loops = 2;
    while (loops--) {
        hot_key_update(rpt, small_key.key_str, KEY_LEN+1,
                       small_key.syndrome, small_key.bucket,
                       1+random()%11, ip);
    }
    recv[0] = '\0';
    printf("keys:\n");
    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    if (strstr(recv, small_key.key_str)) {
        printf("small key should not be hot keys %s\n",
               small_key.key_str);
        nfailed++;
    }

    // continue with this key, it should be winner if
    // ref-count is larger than threshold
    hot_key_update(rpt, small_key.key_str, KEY_LEN+1,
                   small_key.syndrome, small_key.bucket,
                   1+random()%11, ip);

    recv[0] = '\0';
    printf("keys after small key increased:\n");
    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    if (!strstr(recv, small_key.key_str)) {
        printf("small key should be hot keys as it's no longer small %s\n",
               small_key.key_str);
        nfailed++;
    }

    // now access the "lose winner" to make it win again
    for (int i = 0; i < 2; i++) {
        loops = NCANDIDATES;
        while (loops--) {
            hot_key_update(rpt, keys[loops].key_str, KEY_LEN+1,
                           keys[loops].syndrome, keys[loops].bucket,
                           1+random()%11, ip);
        }
    }

    recv[0] = '\0';
    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    printf("keys after small key shift out, and all ref-count should be the same\n%s\n",
           recv);

    if (strstr(recv, small_key.key_str)) {
        printf("small key is should not be hot keys as should shift out again %s\n",
               small_key.key_str);
        nfailed++;
    }
    loops = NCANDIDATES;
    while (loops--) {
        if (!strstr(recv, keys[loops].key_str) &&
            loops >= NCANDIDATES - NKEY_PER_WINNER_LIST) {
            printf("fail to set key %s\n", keys[loops].key_str);
            nfailed++;
        }
    }

    recv[0] = '\0';
    hot_client_report(rpt, top, recv, 300*top);
    printf("clients:\n%s", recv);

    hot_key_reset(rpt);
    hot_key_cleanup(rpt);

    plat_free(recv);

    printf("failed = %d\n", nfailed);
    done = 1;
}
