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
 * File: ht_all_rpts.c
 * Description:
 * Test all kinds of reporter handlers with ACCESS/SEPERATE modes.
 *
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

#define hashsize(x)     (x)
#define KEY_LEN     64
#define KEY_NUM     16

struct _key {
    char key_str[KEY_LEN+1];
    uint64_t syndrome;
    int bucket;
    int get_count, update_count, count;
} key;

struct _key keys[KEY_NUM];

void
gen_str(char *key, int t) {
    uint64_t i;
    for (i = 0; i < KEY_LEN; i++) {
        key[i] = random() % 26 + 'A';
    //    key[i] = (char)( 'A' + t);
    }
    key[i] = '\0';
}

void
init_key(int nbuckets) {
    for (int i = 0; i < KEY_NUM; i++) {
        gen_str(keys[i].key_str, i);
        keys[i].count = keys[i].get_count = keys[i].update_count = 0;
        keys[i].syndrome     = 10000 + i;
        keys[i].bucket       = i % nbuckets;
        printf("key[%d]=%s\n", i, keys[i].key_str);
    }
}


int
rnd_ip(int index) {
    return (27001 + index % 16);
}

int
rnd_cmd(int index) {
    if (index == 0)
        return (random() % 2 + 1);
    else
        return (random() % 9 + 3);
}

int nfailed         = 0;
int done            = 0;

void threadTest(uint64_t arg) {
    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(16);
//    uint64_t updates    = 160 * nbuckets;
    uint64_t updates    = 16;
    int i, t, cmd_type;
    int mm_size[4];
    void* buf[4];
    Reporter_t *rpt[4];
    char *recv          = plat_alloc(300*top);
    char *cmp_recv      = plat_alloc(300*top);
    
    for (int i = 0; i < 4; i++) {
        mm_size[i]   = calc_hotkey_memory(maxtop, nbuckets, i);
        buf[i] = plat_alloc(mm_size[i]);
        rpt[i] = hot_key_init(buf[i], mm_size[i], nbuckets, maxtop, i);
    }

    init_key(4);

    // case 0: 0x00 handler
    // keys are get/update, randomized ip, result should be
    // merged keys and 0 for ip

    // 16 keys with different ip
    for (i = 0; i < updates*2; i++) {
        t = i % KEY_NUM;
        int ip = rnd_ip(i);
        cmd_type = rnd_cmd(i % 2);
        printf("key_t=%s\n", keys[t].key_str);
        hot_key_update(rpt[0], keys[t].key_str, KEY_LEN+1,
                       keys[t].syndrome, keys[t].bucket, cmd_type, ip);
        cmd_type <= 2 ? (keys[t].get_count++): (keys[t].update_count++);
        keys[t].count++;
    }
    recv[0] = '\0';
    hot_key_report(rpt[0], top, recv, 300*top, 1, 0);
    printf("handle 0x00:get\n%s", recv);
    strcpy(cmp_recv, recv);

    recv[0] = '\0';
    hot_key_report(rpt[0], top, recv, 300*top, 2, 0);
    printf("handle 0x00:update\n%s", recv);

    if (strcmp(cmp_recv, recv)) {
        printf("error: get/update should be both access for 0x00 reporter\n");
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt[0], top, recv, 300*top);
    printf("handle 0x00: client\n%s", recv);
    hot_key_reset(rpt[0]);

    // case 1: 0x01 handler
    // keys are get/update, randomized ip, result should be
    // seperate keys and 0 for ip

    // each instance has 16 keys with different ip
    for (i = 0; i < updates*2; i++) {
        t = i % KEY_NUM;
        for (int j = 0; j < 2; j++) {
            int ip = rnd_ip(i);
            cmd_type = rnd_cmd(j);
            hot_key_update(rpt[1], keys[t].key_str, KEY_LEN+1,
                           keys[t].syndrome, keys[t].bucket, cmd_type, ip);
            cmd_type <= 2 ? (keys[t].get_count++):
                (keys[t].update_count++);
            keys[t].count++;
        }
    }

    recv[0] = '\0';
    hot_key_report(rpt[1], top, recv, 300*top, 1, 0);
    printf("handle 0x01:get\n%s", recv);
    strcpy(cmp_recv, recv);
    recv[0] = '\0';
    hot_key_report(rpt[1], top, recv, 300*top, 3, 0);
    printf("handle 0x01:update\n%s", recv);
    if (strcmp(cmp_recv, recv) != 0) {
        printf("error: get/update should be the same for 0x01 reporter\n");
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt[1], top, recv, 300*top);
    printf("handle 0x01: client\n%s", recv);
    hot_key_reset(rpt[1]);

    // case 2: 0x02 handler
    // keys are get/update, randomized ip, result should be
    // merged keys keys for get/set while different ip

    // 8 sets, and 8 gets, the winner is filled with 16 entries
    // with the same key but different ip
    for (i = 0; i < updates*4; i++) {
        int ip = rnd_ip(i);
        for (int j = 0; j < 2; j++) {
            t = j;
            int cmd_type = rnd_cmd(j);
            hot_key_update(rpt[2], keys[t].key_str, KEY_LEN+1,
                           keys[t].syndrome, keys[t].bucket, cmd_type, ip);
        }
    }

    recv[0] = '\0';
    hot_key_report(rpt[2], top, recv, 300*top, 1, 0);
    printf("handle 0x02:get\n%s", recv);
    strcpy(cmp_recv, recv);
    recv[0] = '\0';
    hot_key_report(rpt[2], top, recv, 300*top, 3, 0);
    printf("handle 0x02:update\n%s", recv);
    if (strcmp(cmp_recv, recv)) {
        printf("error: get/update should be same for 0x02 reporter\n");
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt[2], top, recv, 300*top);
    printf("handle 0x02: client\n%s", recv);
    hot_key_reset(rpt[2]);


    // case 3: 0x03 handler
    // keys are get/update, randomized ip, result should be
    // separated keys keys for get/set while different ip

    // 16 sets, and 16 gets, the winner is filled with 16 entries
    // with the same key but different ip
    for (i = 0; i < updates*4; i++) {
        t = 0;
        int ip = rnd_ip(i);
        for (int j = 0; j < 2; j++) {
            int cmd_type = rnd_cmd(j);
            hot_key_update(rpt[3], keys[t].key_str, KEY_LEN+1,
                           keys[t].syndrome, keys[t].bucket, cmd_type, ip);
        }
    }
    recv[0] = '\0';
    hot_key_report(rpt[3], top, recv, 300*top, 1, 0);
    printf("handle 0x03: get\n%s", recv);
    strcpy(cmp_recv, recv);
    recv[0] = '\0';
    hot_key_report(rpt[3], top, recv, 300*top, 3, 0);
    printf("handle 0x03: update\n%s", recv);
    if (strcmp(cmp_recv, recv) != 0) {
        printf("error: get/update should be the same for 0x03 reporter\n");
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt[3], top, recv, 300*top);
    printf("handle 0x03: client\n%s", recv);
    hot_key_reset(rpt[3]);

    plat_free(recv);
    plat_free(cmp_recv);

    printf("nfailed = %d\n", nfailed);
    done = 1;
}
