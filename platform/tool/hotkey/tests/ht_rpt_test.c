/*
 * File: ht_rpt_test.c
 * Description:
 * For 4 kinds of reporter handles, randomized commands are sent to
 * simulate the real situation.
 * Results are dumpped foreach loops and handlers.
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


#define hashsize(x)     (1<<(x))
// #define hashsize(x)     (16)

#define KEY_LEN     64
#define KEY_NUM  (64*hashsize(16))


struct _key {
    char key_str[KEY_LEN+1];
    uint64_t syndrome;
    int bucket;
    int count;
} key[KEY_NUM];

void
gen_str(char *key, int t) {
    uint64_t i;
    for (i = 0; i < KEY_LEN; i++) {
        key[i] = random() % 26 + 'A';
    //    key[i] = (char)( 'A' + t);
    }
    key[i] = '\0';
}

int
cmp(const void *a, const void *b)
{
    struct _key *c = (struct _key *)a;
    struct _key *d = (struct _key *)b;
    if (c->count != d->count) {
        return (d->count - c->count);
    } else {
        return (d->bucket - c->bucket);
    }
}

void *pthreadRoutine(void *arg) {
    fthSchedulerPthread(0);
    return (0);
}

int nfailed         = 0;
int done            = 0;
static void threadTest(uint64_t arg);


int
main() {
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

    pthread_t pthread[NUM_PTHREADS];
    for (uint64_t i = 0; i < NUM_PTHREADS; i++) {
        pthread_create(&pthread[i], NULL, &pthreadRoutine, (void *) i);
    }
    fthResume(fthSpawn(threadTest, 64 * 1024), 0);
    while (!done) 
        sleep(1);

    return (nfailed);    
}

static void threadTest(uint64_t arg) {

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
        key[i].bucket        = (10000 + i) % nbuckets;
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

    done = 1;
}
