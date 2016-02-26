/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf_msg_tester.c
 * Author: Tom Riddle
 *
 * Created on February 21, 2008, 11:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_tester.c 308 2008-02-20 22:34:58Z tomr $
 */

#define _POSIX_C_SOURCE 200112

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>

#include "sdf_msg.h"

struct sdf_queue_item global_item;

void *
thread_one(void *q)
{
    struct sdf_queue *queue = (struct sdf_queue *) q;

    global_item.q_taskid = 100;
    global_item.q_msg = NULL;

    sdf_post(queue, &global_item);
    pthread_exit(NULL);
    /* NOTREACHED */
}

void *
thread_two(void *q)
{
    struct sdf_queue_item *item;
    struct sdf_queue *queue = (struct sdf_queue *) q;

    do {
        item = sdf_fetch(queue, B_FALSE);
    } while (item == NULL);
    printf("Thread 2 received queue item with task id = %d\n", item->q_taskid);
    pthread_exit(NULL);
    /* NOTREACHED */
}

void *
thread_three(void *q)
{
    struct sdf_queue_item item, *itemp;
    struct sdf_queue_pair *q_pair = (struct sdf_queue_pair *) q;

    item.q_taskid = 200;
    item.q_msg = NULL;

    sdf_post(q_pair->q_out, &item);
    itemp = sdf_fetch(q_pair->q_in, B_TRUE);

    printf("Thread 3 received queue item with task id = %d\n", itemp->q_taskid);
    pthread_exit(NULL);
    /* NOTREACHED */
}

void *
thread_four(void *q)
{
    struct sdf_queue_item *item;
    struct sdf_queue_pair *q_pair = (struct sdf_queue_pair *) q;

    item = sdf_fetch(q_pair->q_out, B_TRUE);
    printf("Thread 4 received queue item with task id = %d\n", item->q_taskid);
    item->q_taskid++;
    sdf_post(q_pair->q_in, item);
    pthread_exit(NULL);
    /* NOTREACHED */
}

int
main(int argc, char *argv[])
{
    struct sdf_queue *queue;
    pthread_t tid1, tid2, tid3, tid4;
    int ret;
    struct sdf_queue_pair *q_pair;
    uint32_t src_node, dest_node, src_service, dest_service;

    queue = sdf_create_queue();
    if (queue == NULL) {
        fprintf(stderr, "Failed to create queue\n");
        plat_exit(1);
    }

    ret = pthread_create(&tid1, NULL, thread_one, (void *)queue);
    if (ret < 0) {
        fprintf(stderr, "Error %d creating thread one\n", errno);
        plat_exit(1);
    }
    printf("Thread one created with thread id %d\n", tid1);

    ret = pthread_create(&tid2, NULL, thread_two, (void *)queue);
    if (ret < 0) {
        fprintf(stderr, "Error %d creating thread two\n", errno);
        plat_exit(1);
    }
    printf("Thread two created with thread id %d\n", tid2);

    q_pair = sdf_create_queue_pair(src_node, dest_node, src_service,
                                    dest_service); 

    ret = pthread_create(&tid3, NULL, thread_three, (void *)q_pair);
    if (ret < 0) {
        fprintf(stderr, "Error %d creating thread three\n", errno);
        plat_exit(1);
    }
    printf("Thread three created with thread id %d\n", tid3);
    ret = pthread_create(&tid4, NULL, thread_four, (void *)q_pair);
    if (ret < 0) {
        fprintf(stderr, "Error %d creating thread four\n", errno);
        plat_exit(1);
    }
    printf("Thread four created with thread id %d\n", tid4);

    ret = pthread_join(tid1, NULL);
    if (ret != 0)
        fprintf(stderr, "pthread_join(%d) got error %d\n", tid1, ret);
    ret = pthread_join(tid2, NULL);
    if (ret != 0)
        fprintf(stderr, "pthread_join(%d) got error %d\n", tid2, ret);
    ret = pthread_join(tid3, NULL);
    if (ret != 0)
        fprintf(stderr, "pthread_join(%d) got error %d\n", tid3, ret);
    ret = pthread_join(tid4, NULL);
    if (ret != 0)
        fprintf(stderr, "pthread_join(%d) got error %d\n", tid4, ret);
    plat_exit(0);
    /* NOTREACHED */
}

