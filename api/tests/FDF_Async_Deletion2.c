#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "fdf.h"
#include "test.h"

static char *base = "container";
static int iterations = 200000;
static int threads = 2;
static long size = 1024 * 1024 * 1024;

pthread_mutex_t count_mutex   = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  condition_var = PTHREAD_COND_INITIALIZER;

unsigned event_flags = 2;

void* worker(void *arg)
{
    int i;

    FDF_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";

    t(fdf_init_thread(), FDF_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(fdf_create_container(cname, size, &cguid), FDF_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(fdf_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), FDF_SUCCESS);

        advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(fdf_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), FDF_SUCCESS);

        assert(!memcmp(data, key_data, datalen));	
        advance_spinner();
    }

    fprintf(stderr, "\n");

    pthread_mutex_lock(&count_mutex);
    event_flags--;
    if (event_flags == 0) { 
        fprintf(stderr, "Signalling the main thread\n");
        pthread_cond_signal(&condition_var);
    }
    pthread_mutex_unlock(&count_mutex);

    t(fdf_delete_container(cguid), FDF_SUCCESS);

    t(fdf_release_thread(), FDF_SUCCESS);

    sleep(1);

    return 0;
}

void writer() {
    int i;

    FDF_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";
    long int arg = 1234;

    t(fdf_init_thread(), FDF_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(fdf_create_container(cname, size, &cguid), FDF_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(fdf_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), FDF_SUCCESS);

        advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(fdf_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), FDF_SUCCESS);

        assert(!memcmp(data, key_data, datalen));	
        advance_spinner();
    }

    fprintf(stderr, "\n");

    t(fdf_delete_container(cguid), FDF_SUCCESS);

    t(fdf_release_thread(), FDF_SUCCESS);
}


int
main(int argc, char *argv[])
{
    int i;
    char 						 name[32];

    sprintf(name, "%s-foo", base);

    t(fdf_init(),  FDF_SUCCESS);

    pthread_t thread_id[threads];

    for (i = 0; i < threads; i++) {
        pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);
    }

    fprintf(stderr, "Awaiting signal from a worker\n");
    if (event_flags > 0) {
        pthread_mutex_lock(&count_mutex);
        pthread_cond_wait( &condition_var, &count_mutex);
        pthread_mutex_unlock(&count_mutex);
    }

    fprintf(stderr, "Got signals from worker threads\n");

    /*
     * Create a new thread
     */
    writer();
    
    for(i = 0; i < threads; i++)
        pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

    fdf_shutdown();

    return(0);
}
