#include <assert.h>
#include "api/fdf.h"

static struct FDF_state* fdf_state;
static __thread struct FDF_thread_state *_fdf_thd_state;

FDF_status_t fdf_init()
{
    return FDFInit( &fdf_state );
}

FDF_status_t fdf_init_thread()
{
    return FDFInitPerThreadState(fdf_state, &_fdf_thd_state);
}

FDF_status_t fdf_transaction_start()
{
    return FDFMiniTransactionStart(_fdf_thd_state);
}

FDF_status_t fdf_transaction_commit()
{
    return FDFMiniTransactionCommit(_fdf_thd_state);
}

FDF_status_t fdf_create_container (
	char                    *cname,
	uint64_t				size,
	FDF_cguid_t             *cguid
	)
{
    FDF_status_t            ret;
    FDF_container_props_t   props;
    uint32_t                flags		= FDF_CTNR_CREATE;

	FDFLoadCntrPropDefaults(&props);

	props.size_kb                       = size / 1024;

	ret = FDFOpenContainer (
			_fdf_thd_state, 
			cname, 
			&props,
			flags,
			cguid
			);

    if ( ret != FDF_SUCCESS ) {
		fprintf( stderr, "FDFOpenContainer: %s\n", FDFStrError(FDF_SUCCESS) );
		return ret;
    }

    return ret;
}

FDF_status_t fdf_delete_container (
    FDF_cguid_t                cguid
       )
{
    FDF_status_t  ret;

    ret = FDFDeleteContainer(
            _fdf_thd_state,
            cguid
        );

    return(ret);
}

FDF_status_t fdf_flush_container (
    FDF_cguid_t                cguid
       )
{
    FDF_status_t  ret;

    ret = FDFFlushContainer(
            _fdf_thd_state,
            cguid
        );

    return(ret);
}

FDF_status_t fdf_get (
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
    assert(data && datalen);
    //fprintf(stderr, "%x sdf_get after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, *data, *datalen, (int)ret);
    return(ret);
}

FDF_status_t fdf_free_buffer(
	char 					*data
	) 
{
    FDF_status_t   ret;

    ret = FDFFreeBuffer( data );
    return ret;
}

FDF_status_t fdf_enumerate (
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

FDF_status_t fdf_get_container_stats(FDF_cguid_t cguid, FDF_stats_t *stats)
{
    FDF_status_t  ret;

    ret = FDFGetContainerStats(
		  	_fdf_thd_state, 
			cguid,
			stats
			);

    return ret;
}

FDF_status_t fdf_get_stats(FDF_stats_t *stats)
{
    FDF_status_t  ret;

    ret = FDFGetStats(
		  	_fdf_thd_state, 
			stats
			);

    return ret;
}

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

