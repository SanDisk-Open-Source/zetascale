/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <pthread.h>
#include <stdio.h>

pthread_rwlock_t lock1;
pthread_rwlock_t lock2;
pthread_rwlock_t lock3;
pthread_rwlock_t lockStart;

int iterations;

void *threadRoutine1(void *arg) {
    int i;

    pthread_rwlock_wrlock(&lock1);
    pthread_rwlock_wrlock(&lockStart);
    pthread_rwlock_unlock(&lockStart);

    for (i = 0; i < iterations; i++) {
        pthread_rwlock_wrlock(&lock2);
        pthread_rwlock_unlock(&lock1);
        pthread_rwlock_wrlock(&lock3);
        pthread_rwlock_unlock(&lock2);
        pthread_rwlock_wrlock(&lock1);
        pthread_rwlock_unlock(&lock3);
        //        printf("In 1/%i\n", i);
    }

    pthread_exit(NULL);
}

void *threadRoutine2(void *arg) {
    int i;

    pthread_rwlock_wrlock(&lock2);
    pthread_rwlock_wrlock(&lockStart);
    pthread_rwlock_unlock(&lockStart);

    for (i = 0; i < iterations; i++) {
        pthread_rwlock_wrlock(&lock3);
        pthread_rwlock_unlock(&lock2);
        pthread_rwlock_wrlock(&lock1);
        pthread_rwlock_unlock(&lock3);
        pthread_rwlock_wrlock(&lock2);
        pthread_rwlock_unlock(&lock1);
        //        printf("In 2/%i\n", i);
    }

    pthread_exit(NULL);
}

int main(int argc, char **argv) {
    pthread_t thread1, thread2;
    int t1, t2;
    iterations = atoi(argv[1]);

    pthread_rwlock_init(&lock1, NULL);
    pthread_rwlock_init(&lock2, NULL);
    pthread_rwlock_init(&lockStart, NULL);

    pthread_rwlock_wrlock(&lockStart);

    t1 = pthread_create(&thread1, NULL, &threadRoutine1, (int *) 1);
    t2 = pthread_create(&thread2, NULL, &threadRoutine2, (int *) 2);
    sched_yield();

    pthread_rwlock_unlock(&lockStart);

    pthread_join(thread1, NULL);    
    pthread_join(thread2, NULL);
}
