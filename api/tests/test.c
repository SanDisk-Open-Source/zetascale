#include <assert.h>
#include "zs.h"
#include "string.h"

static struct ZS_state* zs_state;
static __thread struct ZS_thread_state *_zs_thd_state;

ZS_status_t zs_init()
{
    return ZSInit( &zs_state );
}

ZS_status_t zs_shutdown()
{
    return ZSShutdown( zs_state );
}

ZS_status_t zs_init_thread()
{
    return ZSInitPerThreadState(zs_state, &_zs_thd_state);
}

ZS_status_t zs_release_thread()
{
    return ZSReleasePerThreadState(&_zs_thd_state);
}

ZS_status_t zs_transaction_start()
{
    return ZSTransactionStart(_zs_thd_state);
}

ZS_status_t zs_transaction_commit()
{
    return ZSTransactionCommit(_zs_thd_state);
}

ZS_status_t zs_create_container (
    char                    *cname,
    uint64_t                size,
    ZS_cguid_t             *cguid
    )
{
    ZS_status_t            ret;
    ZS_container_props_t   props;
    uint32_t                flags       = ZS_CTNR_CREATE;

    ZSLoadCntrPropDefaults(&props);

    props.size_kb   = size / 1024;

    // Hack!: GC is meant to work on a hash container, hence the check
    if (0 == strncmp("container-slab-gc", cname, sizeof("container-slab-gc") - 1)) {
        props.flags = 1;
    }
    ret = ZSOpenContainer (
            _zs_thd_state,
            cname, 
            &props,
            flags,
            cguid
            );
    
    if ( ret != ZS_SUCCESS ) 
        fprintf( stderr, "ZSOpenContainer: %s\n", ZSStrError(ret) );
    
    return ret;
}

ZS_status_t zs_open_container (
	char                    *cname,
	uint64_t				size,
	ZS_cguid_t             *cguid
	)
{
    ZS_status_t            ret;
    ZS_container_props_t   props;
    uint32_t                flags		= 0;

	ZSLoadCntrPropDefaults(&props);

	props.size_kb   = size / 1024;
	props.evicting	= ZS_TRUE;

	ret = ZSOpenContainer (
			_zs_thd_state, 
			cname, 
			&props,
			flags,
			cguid
			);

    if ( ret != ZS_SUCCESS ) 
		fprintf( stderr, "ZSOpenContainer: %s\n", ZSStrError(ret) );

    return ret;
}

ZS_status_t zs_close_container (
    ZS_cguid_t                cguid
       )
{
    ZS_status_t  ret;

    ret = ZSCloseContainer(
            _zs_thd_state,
            cguid
        );

    return(ret);
}

ZS_status_t zs_delete_container (
    ZS_cguid_t                cguid
       )
{
    ZS_status_t  ret;

    ret = ZSDeleteContainer(
            _zs_thd_state,
            cguid
        );

    return(ret);
}

ZS_status_t zs_flush_container (
    ZS_cguid_t                cguid
       )
{
    ZS_status_t  ret;

    ret = ZSFlushContainer(
            _zs_thd_state,
            cguid
        );

    return(ret);
}

ZS_status_t zs_get (
	ZS_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	   )
{
    ZS_status_t  ret;

    //fprintf(stderr, "%x sdf_get before: key=%s, keylen=%d\n", (int)pthread_self(), key, keylen);
    ret = ZSReadObject(
			_zs_thd_state, 
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

ZS_status_t zs_free_buffer(
	char 					*data
	) 
{
    ZS_status_t   ret;

    ret = ZSFreeBuffer( data );
    return ret;
}

ZS_status_t zs_enumerate (
	ZS_cguid_t 			  cguid,
	struct ZS_iterator		**_zs_iterator
	)
{
	int i = 1000;
    ZS_status_t  ret;

	do{
    	ret = ZSEnumerateContainerObjects(
				_zs_thd_state,
				cguid,
				_zs_iterator 
				);
    } while (ret == ZS_FLASH_EBUSY && i--);

    //fprintf(stderr, "%x sdf_enumerate after: ret %d\n", (int)pthread_self(), ret);
    return ret;
}

ZS_status_t zs_next_enumeration(
	ZS_cguid_t               cguid,
	struct ZS_iterator		 *_zs_iterator,
	char                     **key,
	uint32_t                  *keylen,
	char                     **data,
	uint64_t                  *datalen
	)
{
    ZS_status_t  ret;

    ret = ZSNextEnumeratedObject (
			_zs_thd_state, 
			_zs_iterator,
			key,
			keylen,
			data,
			datalen
		);
    //fprintf(stderr, "%x sdf_next_enumeration after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), *key, *keylen, *data, *datalen, (int)ret);
    return ret;
}

ZS_status_t zs_finish_enumeration (
	ZS_cguid_t  			 cguid,
	struct ZS_iterator		*_zs_iterator
	)
{
    ZS_status_t  ret;

    ret = ZSFinishEnumeration(
			_zs_thd_state, 
			_zs_iterator
			);
    //fprintf(stderr, "%x sdf_finish_enumeration after: ret %d\n", (int)pthread_self(), ret);
    return ret;
}

ZS_status_t zs_set (
	ZS_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen,
	char					*data,
	uint64_t				 datalen
	)
{
    ZS_status_t  	ret;
	uint32_t		flags	= 1;

    //fprintf(stderr, "%x sdf_set before: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);

    ret = ZSWriteObject (
		_zs_thd_state,
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

ZS_status_t zs_delete(
	ZS_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen
	)
{
    ZS_status_t  ret;

    ret = ZSDeleteObject(
		  	_zs_thd_state, 
			cguid,
			key,
			keylen
			);

    return ret;
}

ZS_status_t zs_flush(
    ZS_cguid_t              cguid,
    char                    *key,
    uint32_t                 keylen
    )
{
    ZS_status_t  ret;

    ret = ZSFlushObject(
            _zs_thd_state,
            cguid,
            key,
            keylen
            );

    return ret;
}

ZS_status_t zs_get_container_stats(ZS_cguid_t cguid, ZS_stats_t *stats)
{
    ZS_status_t  ret;

    ret = ZSGetContainerStats(
		  	_zs_thd_state, 
			cguid,
			stats
			);

    return ret;
}

ZS_status_t zs_get_stats(ZS_stats_t *stats)
{
    ZS_status_t  ret;

    ret = ZSGetStats(
		  	_zs_thd_state, 
			stats
			);

    return ret;
}

ZS_status_t zs_get_containers(
    ZS_cguid_t              *cguids,
    uint32_t                 *n_cguids
    )
{
	ZS_status_t	ret;

	ret = ZSGetContainers(
			_zs_thd_state,
			cguids,
			n_cguids
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

