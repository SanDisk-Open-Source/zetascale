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

#define hashsize(x)      (x)
#define KEY_LEN          64
#define KEY_NUM          (64*hashsize(16))
#define NCANDIDATES      16
#define SHM_SIZE         8 * 1024 *1024
#define NUM_PTHREADS     4

struct _key {
    char key_str[KEY_LEN+1];
    uint64_t syndrome;
    int bucket;
    int count, get_count, update_count;
} key;

struct _key keys[KEY_NUM];

void
gen_str(char *key) {
    uint64_t i;
    for (i = 0; i < KEY_LEN; i++) {
        key[i] = random() % 26 + 'A';
    }
    key[i] = '\0';
}

void
init_key(int nbuckets) {
    for (int i = 0; i < KEY_NUM; i++) {
        gen_str(keys[i].key_str);
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

void *pthreadRoutine(void *arg) {
    fthSchedulerPthread(0);
    return (0);
}

int nfailed         = 0;

// All tests
#define TEST_ITEMS() \
    item(winner_lose) \
    item(all_rpts) \
    item(candidate_win) \
    item(complete_winners) \
    item(rpt)

#define item(name) static void name(uint64_t arg);
TEST_ITEMS()
#undef item

static struct test {
    char *name;
    void (*fn)(uint64_t arg);
    int nfailed;
} tests[] =  {
#define item(name) { #name, &name, 0 },
TEST_ITEMS()
#undef item
};

static struct test *parse_test(const char *name);
static void tests_usage();
static int run_test(struct test *test);

int
main(int argc, char *argv[]) {
    struct plat_shmem_config *shmem_config = plat_alloc(sizeof(struct plat_shmem_config));
    plat_shmem_config_init(shmem_config);
    shmem_config->size = SHM_SIZE;
    plat_shmem_prototype_init(shmem_config);
    int tmp = plat_shmem_attach(shmem_config->mmap);
    if (tmp) {
        plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                     PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                     shmem_config->mmap, plat_strerror(-tmp));
        plat_abort();
    }

    // Tell the scheduler code NUM_PTHREADS schedulers starting up
    fthInitMultiQ(1, NUM_PTHREADS);

    int total_failed = 0;
    if (argc == 1) {
        for (int i = 0; i < sizeof (tests) / sizeof (tests[0]); ++i) {
            total_failed += run_test(&tests[i]);
        }
    } else {
        int unknown = 0;
        for (int i = 1; i < argc; ++i) {
            struct test *run;
            run = parse_test(argv[i]);
            if (!run) {
                fprintf(stderr, "Unknown test: %s\n", argv[i]);
                ++total_failed;
                ++unknown;
            } else {
                total_failed += run_test(&tests[i]);
            }
        }

        if (unknown) {
            tests_usage();
        }
    }

    if (total_failed) {
        for (int i = 0; i < sizeof (tests) / sizeof (tests[0]); ++i) {
            if (tests[i].nfailed) {
                fprintf(stderr, "%s %d failures\n", tests[i].name,
                        tests[i].nfailed);
            }
        }
    }

    return (total_failed ? 1 : 0);
}

static struct test *
parse_test(const char *name) {
    struct test *check, *end;

    for (end = tests + sizeof (tests) / sizeof (tests[0]), check = tests;
         check != end && strcmp(name, check->name); ++check) {
    }

    return (check != end ? check : NULL);
}

static void
tests_usage() {
#define item(name_arg) " " #name_arg
    fprintf(stderr, "Legal test names include"
            TEST_ITEMS()
            "\n");
#undef item
}

static int
run_test(struct test *test) {

    printf("test %s", test->name);
    nfailed = 0;

#ifdef notyet
    /*
     * XXX: drew 2010-02-26 Without multiple fthreads this is 
     * meaningless because only one scheduler is used
     */
    pthread_t pthread[NUM_PTHREADS];
    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }

#endif
    fthResume(fthSpawn(test->fn, 64 * 1024), 0);

#ifdef notyet
    for (int i = 0; i < NUM_PTHREADS; i++) {
        pthread_join(pthread[i], NULL);
    }
#else
    fthSchedulerPthread(0);
#endif

    if (nfailed > 0) {
        printf("test %s failed", test->name);
    }

    test->nfailed += nfailed;

    return (nfailed);    
}

static void winner_lose(uint64_t arg) {

    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(4);
    int loops           = NCANDIDATES;

    int mm_size;
    void *buf;
    struct _key keys[NCANDIDATES];
    struct _key big_key, small_key;

    char *recv = (char *)malloc(300*top);
    if (recv == NULL) {
        perror("failed to alloc");
    }

    mm_size = calc_hotkey_memory(maxtop, nbuckets, 0);
    buf     = malloc(mm_size);
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
    small_key.syndrome    = 10000 + 20;
    small_key.bucket      = (NCANDIDATES - 1) % 4;


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
        if (!strstr(recv, keys[loops].key_str)) {
            printf("fail to set key %s\n", keys[loops].key_str);
            nfailed++;
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
    hot_key_update(rpt, small_key.key_str, KEY_LEN + 1,
                   small_key.syndrome, small_key.bucket,
                   1 + random()%11, ip);

    recv[0] = '\0';
    printf("keys after small key increased:\n");
    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    if (!strstr(recv, small_key.key_str)) {
        printf("small key should be hot keys as it's no longer small %s\n",
               small_key.key_str);
        nfailed++;
    }

//    small_key = keys[NCANDIDATES - 1];
    small_key.syndrome = keys[NCANDIDATES - 1].syndrome;
    small_key.bucket = keys[NCANDIDATES - 1].bucket;
    strcpy(small_key.key_str, keys[NCANDIDATES - 1].key_str);
    
    if (strstr(recv, small_key.key_str)) {
        printf("winner should be evicted out:%s\n",
               small_key.key_str);
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt, top, recv, 300*top);
    printf("clients:\n%s", recv);

    hot_key_reset(rpt);
    hot_key_cleanup(rpt);

    free(buf);
    free(recv);

    printf("failed = %d\n", nfailed);

    fthKill(1);
}

static void all_rpts(uint64_t arg) {
    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(16);
//    uint64_t updates    = 160 * nbuckets;
    uint64_t updates    = 16;
    int i, t, cmd_type;
    int mm_size[4];
    void* buf[4];
    Reporter_t *rpt[4];
    char *recv          = malloc(300*top);
    char *cmp_recv      = malloc(300*top);
    
    for (int i = 0; i < 4; i++) {
        mm_size[i]   = calc_hotkey_memory(maxtop, nbuckets, i);
        buf[i] = malloc(mm_size[i]);
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
    if (strcmp(cmp_recv, recv) == 0) {
        printf("error: get/update should be not the same for 0x01 reporter\n");
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
    if (strcmp(cmp_recv, recv) == 0) {
        printf("error: get/update should not same for 0x03 reporter\n");
        nfailed++;
    }

    recv[0] = '\0';
    hot_client_report(rpt[3], top, recv, 300*top);
    printf("handle 0x03: client\n%s", recv);
    hot_key_reset(rpt[3]);

    free(recv);
    free(cmp_recv);

    printf("nfailed = %d\n", nfailed);
    
    fthKill(1);
}

static void candidate_win(uint64_t arg) {
    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(4);
    int loops           = NCANDIDATES;

    int mm_size;
    void *buf;
    struct _key keys[NCANDIDATES];

    char *recv = (char *)malloc(300*top);
    if (recv == NULL) {
        perror("failed to alloc");
    }

    mm_size = calc_hotkey_memory(maxtop, nbuckets, 0);
    buf     = malloc(mm_size);
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
        if (!strstr(recv, keys[loops].key_str)) {
            printf("fail to set key %s\n", keys[loops].key_str);
            nfailed++;
        }
    }
    recv[0] = '\0';
    hot_client_report(rpt, top, recv, 300*top);
    printf("clients:\n%s", recv);

    hot_key_reset(rpt);
    hot_key_cleanup(rpt);

    free(buf);
    free(recv);

    printf("failed = %d\n", nfailed);

    fthKill(1);
}

static void complete_winners(uint64_t arg) {
    int maxtop          = 16;
    int top             = 16;
    int nbuckets        = hashsize(4);

    int loops;
    int mm_size;
    void *buf;
    struct _key keys[NCANDIDATES];

    char *recv = (char *)malloc(300*top);
    if (recv == NULL) {
        perror("failed to alloc");
    }

    mm_size = calc_hotkey_memory(maxtop, nbuckets, 0);
    buf     = malloc(mm_size);
    Reporter_t *rpt;

    for (int i = 0; i < NCANDIDATES; i++) {
        gen_str(keys[i].key_str);
        keys[i].syndrome     = 10000 + i;
        keys[i].bucket       = i % nbuckets;
        keys[i].count        = 0;
    }

    rpt = hot_key_init(buf, mm_size, nbuckets, maxtop, 0);

    int ip = 127001;

    // control for loops, ajust this parameter if you want to longer
    // running time for test
    loops = 100000;

    for (int i = 0; i < loops; i++) {
        int index = random()%(NCANDIDATES);
        hot_key_update(rpt, keys[index].key_str, KEY_LEN + 1,
                       keys[index].syndrome, keys[index].bucket,
                       1 + random()%11, ip);
        keys[index].count++;
    }
    hot_key_report(rpt, top, recv, 300*top, 1, 0);
    printf("keys:\n%s", recv);

   loops = NCANDIDATES;
   char item[KEY_LEN+10];
    while (loops--) {
        sprintf(item, "%d %s", keys[loops].count, keys[loops].key_str);
        if (!strstr(recv, item)) {
            printf("fail to match item %s\n", item);
            nfailed++;
        }
    }

    hot_key_reset(rpt);
    hot_key_cleanup(rpt);
    free(buf);
    free(recv);

    printf("failed = %d\n", nfailed);

    fthKill(1);
}

static void rpt(uint64_t arg) {

    int maxtop          = 10000;
    int top             = 1000;
    int nbuckets        = hashsize(16);
    uint64_t updates    = 160 * nbuckets;
    int loops           = 1;

    int mm_size1, mm_size2, mm_size3, mm_size4;
    mm_size1 = calc_hotkey_memory(maxtop, nbuckets, 0x00);
    void *buf1          = malloc(mm_size1);

    mm_size2 = calc_hotkey_memory(maxtop, nbuckets, 0x01);
    void *buf2          = malloc(mm_size2);

    mm_size3 = calc_hotkey_memory(maxtop, nbuckets, 0x02);
    void *buf3          = malloc(mm_size3);

    mm_size4 = calc_hotkey_memory(maxtop, nbuckets, 0x03);
    void *buf4          = malloc(mm_size4);

    char *recv          = malloc(300*top);

    /* Four reporters */
    Reporter_t *rpt[4];
    uint64_t i, t, l;
    for (i = 0; i < KEY_NUM; i++) {
        gen_str(key[i].key_str, i);
        key[i].count        = 0;
        key[i].syndrome     = 10000 + i;
        key[i].bucket        = i % nbuckets;
    }

   #define nsize     7 /* 1+2*(1+2) */
    FILE *fd[nsize];
    char fname[nsize][50];
    time_t begin, end;

    int *at = (int *)malloc(KEY_NUM * sizeof(int));

    for (l = 0; l < loops; l++) {

        rpt[0] = hot_key_init(buf1, mm_size1, nbuckets, maxtop, 0x00);
        rpt[1] = hot_key_init(buf2, mm_size2, nbuckets, maxtop, 0x01);
        rpt[2] = hot_key_init(buf3, mm_size3, nbuckets, maxtop, 0x02);
        rpt[3] = hot_key_init(buf4, mm_size4, nbuckets, maxtop, 0x03);

        for (i = 0; i < nsize; i++) {
            memset(fname[i], 0, 50);
            sprintf(fname[i], "test_hotkey_loop_%"PRIu64"_rpt_%"PRIu64, l, i);
            fd[i] = fopen(fname[i], "w+");
            if (fd[i] == NULL) {
                perror("fd open error");
            }
        }

        for (i = 0; i < KEY_NUM; i++) {
            at[i] = 0;
        }

        begin = clock();
        for (i = 0; i < updates; i++) {
//            srand((int)time(0));
            t = random() % KEY_NUM;
            at[t]++;
/*
 *           printf("=====access key (%s %d %d)\n", \
 *                   key[t].key_str, key[t].syndrome, key[t].bucket);
 */
            int ip = 127001 + i%10;

            hot_key_update(rpt[0], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 1, ip);

            hot_key_update(rpt[1], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 1, ip);
            hot_key_update(rpt[1], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 4, ip);

            hot_key_update(rpt[2], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 2, ip);

            hot_key_update(rpt[3], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 2, ip);
            hot_key_update(rpt[3], key[t].key_str, KEY_LEN+1,
                           key[t].syndrome, key[t].bucket, 5, ip);

            key[t].count++;
        }

        end = clock();
        hot_key_report(rpt[0], top, recv, 300*top, 1, 0);
        hot_client_report(rpt[0], 10, recv, 300*top);
        fprintf(fd[1], "%s", recv);
        printf("keys:\n%s", recv);
        recv[0] = '\0';

        hot_key_report(rpt[1], top, recv, 300*top, 2, 0);
        fprintf(fd[2], "%s", recv);
        recv[0] = '\0';
        hot_key_report(rpt[1], top, recv, 300*top, 3, 0);
        fprintf(fd[3], "%s", recv);
        recv[0] = '\0';

        hot_key_report(rpt[2], top, recv, 300*top, 1, 127003);
        fprintf(fd[4], "%s", recv);
        recv[0] = '\0';
        hot_key_report(rpt[3], top, recv, 300*top, 2, 127004);
        fprintf(fd[5], "%s", recv);
        recv[0] = '\0';
        hot_key_report(rpt[3], top, recv, 300*top, 5, 127005);
        fprintf(fd[6], "%s", recv);
        recv[0] = '\0';

        hot_client_report(rpt[0], top, recv, 300*top);
        fprintf(fd[1], "%s", recv);
        printf("clients:\n%s", recv);
        recv[0] = '\0';

        hot_client_report(rpt[1], top, recv, 300*top);
        fprintf(fd[2], "%s", recv);
        recv[0] = '\0';

        hot_client_report(rpt[2], top, recv, 300*top);
        fprintf(fd[3], "%s", recv);
        recv[0] = '\0';

        hot_client_report(rpt[3], top, recv, 300*top);
        fprintf(fd[4], "%s", recv);
        recv[0] = '\0';

        printf("Using time: %fs\n", (double)(end-begin)/CLOCKS_PER_SEC);
        printf("Totally : %"PRIu64" updates.\n", updates);
        printf("client got hotkey status dumped\n");
        qsort(key, KEY_NUM, sizeof(key[0]), cmp);

        for (i = 0; i < KEY_NUM; i++) {
            fprintf(fd[0], "%d %s\n", key[i].count, key[i].key_str);
        }

        for (i = 0; i < KEY_NUM; i++) {
            if (key[i].count > 0)
                key[i].count = 0;

        //    fprintf(fd1, "%d\n", at[i]);
        }

        for (i = 0; i < nsize; i++) {
            fclose(fd[i]);
        }

        hot_key_reset(rpt[0]);
        hot_key_reset(rpt[1]);
        hot_key_reset(rpt[2]);
        hot_key_reset(rpt[3]);

        hot_key_cleanup(rpt[0]);
        hot_key_cleanup(rpt[1]);
        hot_key_cleanup(rpt[2]);
        hot_key_cleanup(rpt[3]);
    }

    free(at);
    free(buf1);
    free(buf2);
    free(buf3);
    free(buf4);
    free(recv);

    fthKill(1);
}
