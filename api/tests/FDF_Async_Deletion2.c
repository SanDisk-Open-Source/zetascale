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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "zs.h"
#include "test.h"

static char *base = "container";
static int iterations = 200000;
static int threads = 2;
static long size = (uint64_t) 1024 * 1024 * 1024 * 4;

pthread_mutex_t count_mutex   = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  condition_var = PTHREAD_COND_INITIALIZER;

unsigned event_flags = 2;

void* worker(void *arg)
{
    int i;

    ZS_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(zs_create_container_dur(cname, size, ZS_DURABILITY_SW_CRASH_SAFE, &cguid), ZS_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(zs_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), ZS_SUCCESS);

        advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(zs_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), ZS_SUCCESS);

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

    t(zs_delete_container(cguid), ZS_SUCCESS);

    t(zs_release_thread(), ZS_SUCCESS);

    sleep(1);

    return 0;
}

void writer() {
    int i;

    ZS_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";
    long int arg = 1234;

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(zs_create_container_dur(cname, size, ZS_DURABILITY_SW_CRASH_SAFE, &cguid), ZS_SUCCESS);

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(zs_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), ZS_SUCCESS);

        advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
        sprintf(key_str, "key%04d-%08d", 0, i);
        sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

        t(zs_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), ZS_SUCCESS);

        assert(!memcmp(data, key_data, datalen));	
        advance_spinner();
    }

    fprintf(stderr, "\n");

    t(zs_delete_container(cguid), ZS_SUCCESS);

    t(zs_release_thread(), ZS_SUCCESS);
}


int
main(int argc, char *argv[])
{
    int i;
    char 						 name[32];

    sprintf(name, "%s-foo", base);

    t(zs_init(),  ZS_SUCCESS);

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

    zs_shutdown();

    return(0);
}
