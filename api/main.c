#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "sdf.h"

static struct SDF_state *sdf_state;
static struct SDF_thread_state *_sdf_thrd_state;

SDF_status_t sdf_create_container (
		   char                    *cname,
		   SDF_cguid_t             *cguid
	       )
{
    SDF_status_t            ret;
    SDF_container_props_t   props;

    props.durability_level = SDF_FULL_DURABILITY;
    props.fifo_mode = SDF_FALSE; // xxxzzz

    props.container_type.type = SDF_OBJECT_CONTAINER;
    props.container_type.caching_container = SDF_TRUE;
    props.container_type.persistence = SDF_TRUE;
    props.container_type.async_writes = SDF_FALSE;

    props.cache.writethru = SDF_TRUE;

    props.container_id.num_objs = 1000000; // is this enforced? xxxzzz
    // props.container_id.container_id = xxxzzz; // only used for replication?
     props.container_id.size = 1024; // unused?
    // props.container_id.owner = xxxzzz; // ????

    props.replication.num_replicas = 1;
    props.replication.num_meta_replicas = 0;
    props.replication.type = SDF_REPLICATION_NONE;
    props.replication.enabled = 0;
    props.replication.synchronous = 1;

    props.shard.num_shards = 1;

    ret = SDFCreateContainer (
			_sdf_thrd_state, 
			cname, 
			&props,
			cguid
		);
    if (ret != SDF_SUCCESS) {
	return(ret);
    }

    ret = SDFOpenContainer (
		       _sdf_thrd_state,
		       *cguid,
		       SDF_READ_WRITE_MODE
		   );
    if (ret != SDF_SUCCESS) {
	return(ret);
    }

    ret = SDFStartContainer (
		       _sdf_thrd_state,
		       *cguid
		   );

    return(ret);
}

SDF_status_t sdf_get (
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     **data,
	       uint64_t                 *datalen
	   )
{
    SDF_status_t  ret;
    SDF_time_t    texp;

    ret = SDFGetForReadBufferedObject(
			_sdf_thrd_state, 
			cguid, 
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			&texp // *expiry_time
		);
    return(ret);
}

SDF_status_t sdf_free_buffer(char *data) 
{
    SDF_status_t   ret;

    ret = SDFFreeBuffer(_sdf_thrd_state, data);
    return(ret);
}

SDF_status_t sdf_get_buffer(char **data, uint64_t datalen)
{
    SDF_status_t   ret;

    ret = SDFGetBuffer(_sdf_thrd_state, data, datalen);
    return(ret);
}

SDF_status_t sdf_enumerate (
	       SDF_cguid_t cguid
	   )
{
    SDF_status_t  ret;

    ret = SDFEnumerateContainerObjects (
			_sdf_thrd_state, 
			cguid
		);
    return(ret);
}

SDF_status_t sdf_next_enumeration (
	       SDF_cguid_t               cguid,
	       char                     **key,
	       uint32_t                 *keylen,
	       char                     **data,
	       uint64_t                 *datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFNextEnumeratedObject (
			_sdf_thrd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen
		);
    return(ret);
}

SDF_status_t sdf_finish_enumeration (
	       SDF_cguid_t  cguid
	   )
{
    SDF_status_t  ret;

    ret = SDFFinishEnumeration(
			_sdf_thrd_state, 
			cguid
		);
    return(ret);
}

SDF_status_t sdf_set (
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFSetBufferedObject (
			_sdf_thrd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			0     // *expiry_time
		);
    return(ret);
}

SDF_status_t sdf_put (
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFPutBufferedObject (
			_sdf_thrd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			0     // *expiry_time
		);
    return(ret);
}

SDF_status_t sdf_create (
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFCreateBufferedObject (
			_sdf_thrd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			0     // *expiry_time
		);
    return(ret);
}

SDF_status_t sdf_delete (
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen
	   )
{
    SDF_status_t  ret;

    ret = SDFRemoveObjectWithExpiry (
			_sdf_thrd_state, 
			cguid,
			key,
			keylen,
			0     //  current_time
		);
    return(ret);
}

int main()
{
    SDF_cguid_t  cguid;
    char        *data;
    uint64_t     datalen;

    if (SDFInit(&sdf_state, 0, NULL) != SDF_SUCCESS) {
	fprintf(stderr, "SDF initialization failed!\n");
	plat_assert(0);
    }
    fprintf(stderr, "SDF was initialized successfully!\n");

    _sdf_thrd_state    = SDFInitPerThreadState(sdf_state);

    // sleep(100);

    plat_assert(sdf_create_container("foobar1", &cguid) == SDF_SUCCESS);

    plat_assert(sdf_create(cguid, "key1", 5, "key1_data", 10) == SDF_SUCCESS);
    plat_assert(sdf_get(cguid, "key1", 5, &data, &datalen) == SDF_SUCCESS);

    fprintf(stderr, "sdf_get: data=%s, datalen=%ld\n", data, datalen);

    return(0);
}
