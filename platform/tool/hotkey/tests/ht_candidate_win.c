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
 * File: ht_candidate_win.c
 * Description:
 * Fill candidate lists with keys, and then these hot keys should be
 * stated from reporter.
 * Author:  Hickey Liu Gengliang Wang
 */

#include "test_main.h"
#include "ht_report.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <unistd.h>
#include "fth/fth.h"

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

    rpt = hot_key_init(buf, mm_size, nbuckets, maxtop, 0);

    int ip = 127001;
    for (int i = 0; i < 2; i++) {
        loops = NCANDIDATES;
        while (loops--) {
            hot_key_update(rpt, keys[loops].key_str, KEY_LEN + 1,
                           keys[loops].syndrome, keys[loops].bucket,
                           1 + random()%11, ip);
        }
    }

    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    printf("keys:\n%s", recv);

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
