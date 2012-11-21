#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "api/sdf.h"
#include "api/fdf.h"

static struct FDF_state *fdf_state;
static FDF_cguid_t  cguid_shared;
static char *base = "contatiner";
static int iterations = 1000;
static int threads = 1;
static long size = 1024 * 1024 * 1024;

void
advance_spinner() {
#if 0
    static char bars[] = { '/', '-', '\\', '|' };
    static int nbars = sizeof(bars) / sizeof(char);
    static int pos = 0;

    fprintf(stderr, "%c\r", bars[pos]);
    fflush(stderr);
    pos = (pos + 1) % nbars;
#endif
}

FDF_status_t fdf_create_container (
	struct FDF_thread_state *_fdf_thd_state,
	char                    *cname,
	FDF_cguid_t             *cguid
	)
{
    FDF_status_t            ret;
    FDF_container_props_t   props;
    uint32_t                flags		= FDF_CTNR_CREATE;

    props.size_kb                       = size / 1024;
    props.fifo_mode                     = SDF_FALSE;
    props.persistent                    = SDF_TRUE;
    props.evicting                      = SDF_FALSE;
    props.writethru                     = SDF_TRUE;
    props.durability_level              = SDF_FULL_DURABILITY;
    props.cguid                         = SDF_NULL_CGUID;
    props.num_shards                    = 1;

    ret = FDFOpenContainer (
			_fdf_thd_state, 
			cname, 
			&props,
			flags,
			cguid
			);

    if ( ret != FDF_SUCCESS ) {
		fprintf( stderr, "FDFOpenContainer: %s\n", SDF_Status_Strings[ret] );
		return ret;
    }

    return ret;
}

FDF_status_t fdf_get (
	struct FDF_thread_state	  * _fdf_thd_state,
	FDF_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	   )
{
    FDF_status_t  ret;

    //fprintf(stderr, "%x sdf_get before: key=%s, keylen=%d\n", (int)pthread_self(), key, keylen);
    ret = FDFReadObject(
			_fdf_thd_state, 
			cguid, 
			key,
			keylen,
			data,
			datalen
		);
    plat_assert(data && datalen);
    //fprintf(stderr, "%x sdf_get after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, *data, *datalen, (int)ret);
    return(ret);
}

FDF_status_t fdf_free_buffer(
	struct FDF_thread_state	*_fdf_thd_state,
	char 					*data
	) 
{
    FDF_status_t   ret;

    ret = FDFFreeBuffer( data );
    return ret;
}

FDF_status_t fdf_enumerate (
	struct FDF_thread_state	 *_fdf_thd_state,
	FDF_cguid_t 			  cguid,
	struct FDF_iterator		**_fdf_iterator
	)
{
	int i = 1000;
    FDF_status_t  ret;

	do{
    	ret = FDFEnumerateContainerObjects(
				_fdf_thd_state,
				cguid,
				_fdf_iterator 
				);
    } while (ret == FDF_FLASH_EBUSY && i--);

    //fprintf(stderr, "%x sdf_enumerate after: ret %d\n", (int)pthread_self(), ret);
    return ret;
}

FDF_status_t fdf_next_enumeration(
	struct FDF_thread_state	 *_fdf_thd_state,
	FDF_cguid_t               cguid,
	struct FDF_iterator		 *_fdf_iterator,
	char                     **key,
	uint32_t                  *keylen,
	char                     **data,
	uint64_t                  *datalen
	)
{
    FDF_status_t  ret;

    ret = FDFNextEnumeratedObject (
			_fdf_thd_state, 
			_fdf_iterator,
			key,
			keylen,
			data,
			datalen
		);
    //fprintf(stderr, "%x sdf_next_enumeration after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), *key, *keylen, *data, *datalen, (int)ret);
    return ret;
}

FDF_status_t fdf_finish_enumeration (
	struct FDF_thread_state	*_fdf_thd_state,
	FDF_cguid_t  			 cguid,
	struct FDF_iterator		*_fdf_iterator
	)
{
    FDF_status_t  ret;

    ret = FDFFinishEnumeration(
			_fdf_thd_state, 
			_fdf_iterator
			);
    //fprintf(stderr, "%x sdf_finish_enumeration after: ret %d\n", (int)pthread_self(), ret);
    return ret;
}

FDF_status_t fdf_set (
	struct FDF_thread_state	*_fdf_thd_state,
	FDF_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen,
	char					*data,
	uint64_t				 datalen
	)
{
    FDF_status_t  	ret;
	uint32_t		flags	= 0;

    //fprintf(stderr, "%x sdf_set before: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);

    ret = FDFWriteObject (
		_fdf_thd_state,
		cguid,
		key,
		keylen,
		data,
		datalen,
		flags
		);
    //fprintf(stderr, "%x sdf_set after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, data, datalen, (int)ret);
    return ret;
}

FDF_status_t fdf_delete(
	struct FDF_thread_state	*_fdf_thd_state,
	FDF_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen
	)
{
    FDF_status_t  ret;

    ret = FDFDeleteObject(
		  	_fdf_thd_state, 
			cguid,
			key,
			keylen
			);

    return ret;
}

void* worker(void *arg)
{
    int i;

    struct FDF_thread_state 	*_fdf_thd_state;
    struct FDF_iterator 		*_fdf_iterator;

    FDF_cguid_t  				 cguid;
    char 						 cname[32] 			= "cntr0";
    char        				*data;
    uint64_t     				 datalen;
    char        				*key;
    uint32_t     				 keylen;
    char 						 key_str[24] 		= "key00";
    char 						 key_data[24] 		= "key00_data";
    FDF_status_t 				 status 			= FDF_FAILURE;
    
    if ( FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
        fprintf( stderr, "FDF thread initialization failed!\n" );
        plat_assert( 0 );
	}

    fprintf(stderr, "%x before_create_container\n", (int)pthread_self());

    sprintf(cname, "%s-%x", base, (int)pthread_self());
    plat_assert(fdf_create_container(_fdf_thd_state, cname, &cguid) == SDF_SUCCESS);

    fprintf(stderr, "\n%x before_fdf_set\n", (int)pthread_self());

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%04ld-%08d", (long) arg, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);
		status = fdf_set(_fdf_thd_state, cguid, key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1);
		if (FDF_SUCCESS != status ) {
	    	fprintf(stderr, "fdf_set: %s - %s\n", key_str, SDF_Status_Strings[status]);
		}
		plat_assert(status == FDF_SUCCESS);
		advance_spinner();
    }

    fprintf(stderr, "\n%x before_fdf_get\n", (int)pthread_self());

    for(i = 0; i < iterations; i++)
    {
		sprintf(key_str, "key%04ld-%08d", (long) arg, i);
		sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);
    	status = fdf_get(_fdf_thd_state, cguid, key_str, strlen(key_str) + 1, &data, &datalen);
		if (FDF_SUCCESS != status ) {
	    	fprintf(stderr, "fdf_get: %s - %s\n", key_str, SDF_Status_Strings[status]);
		}
		plat_assert(status == FDF_SUCCESS);
		plat_assert(!memcmp(data, key_data, 11));	
		advance_spinner();
    }

    fprintf(stderr, "\n%x before enumeration start\n", (int)pthread_self());
    plat_assert(fdf_enumerate(_fdf_thd_state, cguid, &_fdf_iterator) == FDF_SUCCESS);

    fprintf(stderr, "%x before enumeration next\n", (int)pthread_self());
    while (fdf_next_enumeration(_fdf_thd_state, cguid, _fdf_iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
		fprintf(stderr, "%x sdf_enum: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);
		//advance_spinner();
    }

    fprintf(stderr, "\n");

    plat_assert(fdf_finish_enumeration(_fdf_thd_state, cguid, _fdf_iterator) == FDF_SUCCESS);

    return 0;
}

int main(int argc, char *argv[])
{
    struct FDF_thread_state 	*_fdf_thd_state;
    char 						 name[32];

	if ( argc < 4 ) {
		fprintf( stderr, "Usage: %s <size in gb> <threads> <iterations>\n", argv[0] );
		return 0;
	} else {
		size = atol( argv[1] ) * 1024 * 1024 * 1024;
		threads = atoi( argv[2] );
		iterations = atoi( argv[3] );
		fprintf(stderr, "size=%lu, hreads=%d, iterations=%d\n", size, threads, iterations);
	}

    FDFSetProperty("SDF_FLASH_FILENAME", "/schooner/data/schooner%d");
    FDFSetProperty("SDF_FLASH_SIZE", "12");
    FDFSetProperty("SDF_CC_MAXCACHESIZE", "1000000000");

    sprintf(name, "%s-foo", base);

    pthread_t thread_id[threads];

    int i;

    if ( FDFInit( &fdf_state ) != FDF_SUCCESS ) {
        fprintf( stderr, "FDF initialization failed!\n" );
        plat_assert( 0 );
    }

    fprintf( stderr, "FDF was initialized successfully!\n" );

    if ( FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
        fprintf( stderr, "FDF thread initialization failed!\n" );
        plat_assert( 0 );
    }

    plat_assert(fdf_create_container(_fdf_thd_state, name, &cguid_shared) == FDF_SUCCESS);

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

    return(0);
}
