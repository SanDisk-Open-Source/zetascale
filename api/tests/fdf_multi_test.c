#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "zs.h"
#include "test.h"

static char *base = "container";
static int iterations = 1000;
static int threads = 1;
static long size = 1024 * 1024 * 1024;

void* worker(void *arg)
{
    int i;

    struct ZS_iterator 		*_zs_iterator;

    ZS_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char        				*key;
    uint32_t     				 keylen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";

    t(zs_init_thread(), ZS_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(zs_create_container(cname, size, &cguid), ZS_SUCCESS);

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

    t(zs_enumerate(cguid, &_zs_iterator), ZS_SUCCESS);

    while (zs_next_enumeration(cguid, _zs_iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
		fprintf(stderr, "%x zs_enum: key=%s, keylen=%d, data=%s, datalen=%lu\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    t(zs_finish_enumeration(cguid, _zs_iterator), ZS_SUCCESS);

    t(zs_delete_container(cguid), ZS_SUCCESS);

    t(zs_release_thread(), ZS_SUCCESS);

	sleep(1);

    return 0;
}

int main(int argc, char *argv[])
{
    int i;
    char 						 name[32];

	if ( argc < 4 ) {
		fprintf( stderr, "Usage: %s <size in gb> <threads> <iterations>\n", argv[0] );
		return 0;
	} else {
		size = atol( argv[1] ) * 1024 * 1024 * 1024;
		threads = atoi( argv[2] );
		iterations = atoi( argv[3] );
		fprintf(stderr, "size=%lu, threads=%d, iterations=%d\n", size, threads, iterations);
	}

    sprintf(name, "%s-foo", base);

    t(zs_init(),  ZS_SUCCESS);

    t(zs_init_thread(), ZS_SUCCESS);

    pthread_t thread_id[threads];

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

    t(zs_release_thread(), ZS_SUCCESS);

	zs_shutdown();

    return(0);
}
