#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "fdf.h"
#include "test.h"

static char *base = "container";
static int iterations = 1000;
static int threads = 1;
static long size = 1024 * 1024 * 1024;

static int debug = 0;					// Set to "1" to test cache keylen

void* worker(void *arg)
{
    int i;

    struct FDF_iterator 		*_fdf_iterator;

    FDF_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char        				*key;
    uint32_t     				 keylen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";

    t(fdf_init_thread(), FDF_SUCCESS);

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    t(fdf_create_container(cname, size, &cguid), FDF_SUCCESS);

	if ( debug ) fprintf(stderr, "***SET DATA\n");

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%04d-%08d", 0, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

		t(fdf_set(cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1), FDF_SUCCESS);
		if ( debug )
			fprintf(stderr, "%x fdf_set: key=%s, keylen=%u, data=%s, datalen=%u\n", 
				    (int)pthread_self(), key_str, (unsigned)strlen(key_str)+1, key_data, (unsigned)strlen(key_data)+1);

		advance_spinner();
    }

    for(i = 0; i < iterations; i++)
    {
		//sprintf(key_str, "key%04ld-%08d", (long) arg, i);
		sprintf(key_str, "key%04d-%08d", 0, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);

    	t(fdf_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), FDF_SUCCESS);

		assert(!memcmp(data, key_data, datalen));	
		advance_spinner();
    }

	// Enumerate while the container is open
	if ( debug ) fprintf(stderr, "***BEFORE CLOSE/OPEN\n");

    t(fdf_enumerate(cguid, &_fdf_iterator), FDF_SUCCESS);

    while (fdf_next_enumeration(cguid, _fdf_iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
		fprintf(stderr, "%x fdf_enum: key=%s, keylen=%d, data=%s, datalen=%lu\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    t(fdf_finish_enumeration(cguid, _fdf_iterator), FDF_SUCCESS);

	if ( debug ) {
		// Enumerate after close/open of the container
		t(fdf_close_container(cguid), FDF_SUCCESS);
    	t(fdf_open_container(cname, size, &cguid), FDF_SUCCESS);
	
		fprintf(stderr, "***AFTER CLOSE/OPEN\n");
    	t(fdf_enumerate(cguid, &_fdf_iterator), FDF_SUCCESS);

    	while (fdf_next_enumeration(cguid, _fdf_iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
        	fprintf(stderr, "%x fdf_enum: key=%s, keylen=%d, data=%s, datalen=%lu\n", (int)pthread_self(), key, keylen, data, datalen);
        	//advance_spinner();
    	}

    	fprintf(stderr, "\n");

    	t(fdf_finish_enumeration(cguid, _fdf_iterator), FDF_SUCCESS);

    	for(i = 0; i < iterations; i++)
    	{   
        	//sprintf(key_str, "key%04ld-%08d", (long) arg, i);
        	sprintf(key_str, "key%04d-%08d", 0, i); 
        	sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);
        
        	t(fdf_get(cguid, key_str, strlen(key_str) + 1, &data, &datalen), FDF_SUCCESS);
        
        	assert(!memcmp(data, key_data, 11));
        	advance_spinner();
    	}

    	fprintf(stderr, "***AFTER GET\n");
    	t(fdf_enumerate(cguid, &_fdf_iterator), FDF_SUCCESS);

    	while (fdf_next_enumeration(cguid, _fdf_iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
        	fprintf(stderr, "%x fdf_enum: key=%s, keylen=%d, data=%s, datalen=%lu\n", (int)pthread_self(), key, keylen, data, datalen);
        	//advance_spinner();
    	}

    	fprintf(stderr, "\n");

    	t(fdf_finish_enumeration(cguid, _fdf_iterator), FDF_SUCCESS);
	}

    t(fdf_delete_container(cguid), FDF_SUCCESS);

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

    t(fdf_init(),  FDF_SUCCESS);

    t(fdf_init_thread(), FDF_SUCCESS);

    pthread_t thread_id[threads];

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

	fdf_shutdown();

    return(0);
}
