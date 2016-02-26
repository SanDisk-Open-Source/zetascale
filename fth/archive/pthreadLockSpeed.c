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
