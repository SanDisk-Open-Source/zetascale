#include <pthread.h>
#include <stdio.h>

int iterations;

void *threadRoutine1(void *arg) {
    int i;

    for (i = 0; i < iterations; i++) {
        sched_yield();
        //        printf("In %i/%i\n", (int) arg, i);
    }

    pthread_exit(NULL);
}

int main(int argc, char **argv) {

    iterations = atoi(argv[1]);

    pthread_t thread1, thread2;

    int t1 = pthread_create(&thread1, NULL, &threadRoutine1, (int *) 1);
    int t2 = pthread_create(&thread2, NULL, &threadRoutine1, (int *) 2);

    pthread_join(thread1, NULL);    
    pthread_join(thread2, NULL);
}
