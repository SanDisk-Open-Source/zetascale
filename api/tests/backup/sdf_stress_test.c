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
#include "api/sdf.h"

static struct SDF_state *sdf_state;
static char 		*base			= "container";
static SDF_cguid_t	cguids[128];
static int			threads			= 16;
static int			items			= 1000;
static int			iterations		= 10;

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

SDF_status_t sdf_create_container (
	struct SDF_thread_state* _sdf_thd_state,
	char                    *cname,
	SDF_cguid_t             *cguid
	)
{
    SDF_status_t            ret;
    SDF_container_props_t   props;

    props.durability_level = SDF_FULL_DURABILITY;
    props.fifo_mode = SDF_TRUE; // xxxzzz

    props.container_type.type = SDF_OBJECT_CONTAINER;
    props.container_type.caching_container = SDF_FALSE;
    props.container_type.persistence = SDF_TRUE;
    props.container_type.async_writes = SDF_FALSE;

    props.cache.writethru = SDF_TRUE;

    props.container_id.num_objs = 1000000; // is this enforced? xxxzzz
    // props.container_id.container_id = xxxzzz; // only used for replication?
    props.container_id.size = 1024 * 1024; // unused?
    // props.container_id.owner = xxxzzz; // ????

    props.replication.num_replicas = 1;
    //props.replication.num_meta_replicas = 0;
    props.replication.type = SDF_REPLICATION_NONE;
    props.replication.enabled = 0;
    //props.replication.synchronous = 1;

    props.shard.num_shards = 1;

    ret = SDFCreateContainer (
			_sdf_thd_state, 
			cname, 
			&props,
			cguid
		);

    if (ret != SDF_SUCCESS) {
		return(ret);
    }

    ret = SDFOpenContainer (
		       _sdf_thd_state,
		       *cguid,
		       SDF_READ_WRITE_MODE
		   );

    if (ret != SDF_SUCCESS) {
		return(ret);
    }

    ret = SDFStartContainer (
		       _sdf_thd_state,
		       *cguid
		   );

    return(ret);
}

SDF_status_t sdf_close_container(
    struct SDF_thread_state *_sdf_thd_state,
    SDF_cguid_t              cguid
    )
{
    SDF_status_t     ret;

    ret = SDFStopContainer (
               _sdf_thd_state,
               cguid
           );

    if (ret != SDF_SUCCESS) {
        return(ret);
    }

    ret = SDFCloseContainer(
            _sdf_thd_state,
            cguid
            );

    fprintf( stderr, ">>>SDFCloseContainer: %lu - %s\n", cguid, SDF_Status_Strings[ret] );

    return ret;
}

SDF_status_t sdf_delete_container(
    struct SDF_thread_state *_sdf_thd_state,
    SDF_cguid_t              cguid      
    )
{   
    SDF_status_t  ret;                  
    
    ret = SDFDeleteContainer(
            _sdf_thd_state,
            cguid
            );
            
    fprintf( stderr, ">>>SDFDeleteContainer: %lu - %s\n", cguid, SDF_Status_Strings[ret] );
            
    return ret;
}

SDF_status_t sdf_get (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     **data,
	       uint64_t                 *datalen
	   )
{
    SDF_status_t  ret;
    SDF_time_t    texp;

    //fprintf(stderr, "%x sdf_get before: key=%s, keylen=%d\n", (int)pthread_self(), key, keylen);
    ret = SDFGetForReadBufferedObject(
			_sdf_thd_state, 
			cguid, 
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			&texp // *expiry_time
		);
    plat_assert(data && datalen);
    //fprintf(stderr, "%x sdf_get after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, *data, *datalen, (int)ret);
    return(ret);
}

SDF_status_t sdf_free_buffer(
			struct SDF_thread_state* _sdf_thd_state,
			char *data) 
{
    SDF_status_t   ret;

    ret = SDFFreeBuffer(_sdf_thd_state, data);
    return(ret);
}

SDF_status_t sdf_get_buffer(
			struct SDF_thread_state* _sdf_thd_state,
			char **data, uint64_t datalen)
{
    SDF_status_t   ret;

    ret = SDFGetBuffer(_sdf_thd_state, data, datalen);
    return(ret);
}

SDF_status_t sdf_enumerate (
			struct SDF_thread_state* _sdf_thd_state,
	       		SDF_cguid_t cguid,
			struct SDF_iterator** _sdf_iterator
	   )
{
	int i = 1000;
    SDF_status_t  ret;

do{
    ret = SDFEnumerateContainerObjects (
			_sdf_thd_state, 
			cguid,
			_sdf_iterator 
		);
    }while(ret == SDF_FLASH_EBUSY && i--);

    //fprintf(stderr, "%x sdf_enumerate after: ret %d\n", (int)pthread_self(), ret);
    return(ret);
}

SDF_status_t sdf_next_enumeration (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
			struct SDF_iterator* _sdf_iterator,
	       char                     **key,
	       uint32_t                 *keylen,
	       char                     **data,
	       uint64_t                 *datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFNextEnumeratedObject (
			_sdf_thd_state, 
			_sdf_iterator,
			key,
			keylen,
			data,
			datalen
		);
    //fprintf(stderr, "%x sdf_next_enumeration after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), *key, *keylen, *data, *datalen, (int)ret);
    return(ret);
}

SDF_status_t sdf_finish_enumeration (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t  cguid,
			struct SDF_iterator* _sdf_iterator
	   )
{
    SDF_status_t  ret;

    ret = SDFFinishEnumeration(
			_sdf_thd_state, 
			_sdf_iterator
		);
    //fprintf(stderr, "%x sdf_finish_enumeration after: ret %d\n", (int)pthread_self(), ret);
    return(ret);
}

SDF_status_t sdf_set (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    //fprintf(stderr, "%x sdf_set before: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);

    ret = SDFSetBufferedObject (
			_sdf_thd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			0     // *expiry_time
		);
    //fprintf(stderr, "%x sdf_set after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, data, datalen, (int)ret);
    return(ret);
}

SDF_status_t sdf_put (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    ret = SDFPutBufferedObject (
			_sdf_thd_state, 
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
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen,
	       char                     *data,
	       uint64_t                  datalen
	   )
{
    SDF_status_t  ret;

    //fprintf(stderr, "%x sdf_create before: key=%s, keylen=%d, data=%s, datalen=%ld\n", (int)pthread_self(), key, keylen, data, datalen);
    ret = SDFCreateBufferedObject (
			_sdf_thd_state, 
			cguid,
			key,
			keylen,
			data,
			datalen,
			0,    //  current_time
			0     // *expiry_time
		);
    //fprintf(stderr, "%x sdf_create after: key=%s, keylen=%d, data=%s, datalen=%ld ret %d\n", (int)pthread_self(), key, keylen, data, datalen, (int)ret);
    return(ret);
}

SDF_status_t sdf_delete (
			struct SDF_thread_state* _sdf_thd_state,
	       SDF_cguid_t               cguid,
	       char                     *key,
	       uint32_t                  keylen
	   )
{
    SDF_status_t  ret;

    ret = SDFRemoveObjectWithExpiry (
			_sdf_thd_state, 
			cguid,
			key,
			keylen,
			0     //  current_time
		);
    return(ret);
}

void* worker(void *arg)
{
    int i;
	long index = (long) arg;
    struct SDF_thread_state *_sdf_thd_state;
    char cname[32] = "cntr0";
    char        *data;
    uint64_t     datalen;
    char key_str[24] = "key00";
    char key_data[24] = "key00_data";
    SDF_status_t status = SDF_FAILURE;

    _sdf_thd_state    = SDFInitPerThreadState(sdf_state);

    sprintf(cname, "%s-%x", base, (int)pthread_self());

	for (int j = 0; j < iterations; j++) {
    	plat_assert(sdf_create_container(_sdf_thd_state, cname, &cguids[index]) == SDF_SUCCESS);

    	for(i = 0; i < items; i++) {
			sprintf(key_str, "key%04ld-%08d", (long) arg, i);
			sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);
			status = sdf_create(_sdf_thd_state, cguids[index], key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1);
			if (SDF_SUCCESS != status ) {
	    		fprintf(stderr, "sdf_create: %s - %s\n", key_str, SDF_Status_Strings[status]);
			}
			plat_assert(status == SDF_SUCCESS);
			advance_spinner();
    	}

    	for(i = 0; i < items; i++) {
			sprintf(key_str, "key%04ld-%08d", (long) arg, i);
			sprintf(key_data, "key%04ld-%08d_data", (long) arg, i);
    		status = sdf_get(_sdf_thd_state, cguids[index], key_str, strlen(key_str) + 1, &data, &datalen);
			if (SDF_SUCCESS != status ) {
	    		fprintf(stderr, "sdf_get: %s - %s\n", key_str, SDF_Status_Strings[status]);
			}
			plat_assert(status == SDF_SUCCESS);
			plat_assert(!memcmp(data, key_data, 11));	
			advance_spinner();
    	}

    	for(i = 0; i < items; i++) {
			sprintf(key_str, "key%04ld-%08d", (long) arg, i);
			sprintf(key_data, "KEY%04ld-%08d_data", (long) arg, i);
    		status = sdf_put(_sdf_thd_state, cguids[index], key_str, strlen(key_str) + 1, key_data, strlen(key_data) + 1);
			if (SDF_SUCCESS != status ) {
	    		fprintf(stderr, "sdf_put: %s - %s\n", key_str, SDF_Status_Strings[status]);
			}
			plat_assert(status == SDF_SUCCESS);
			advance_spinner();
    	}

		// Simulate "format container"
    	plat_assert(sdf_close_container(_sdf_thd_state, cguids[index]) == SDF_SUCCESS);
    	plat_assert(sdf_delete_container(_sdf_thd_state, cguids[index]) == SDF_SUCCESS);
    	plat_assert(sdf_create_container(_sdf_thd_state, cname, &cguids[index]) == SDF_SUCCESS);

    	plat_assert(sdf_close_container(_sdf_thd_state, cguids[index]) == SDF_SUCCESS);
    	plat_assert(sdf_delete_container(_sdf_thd_state, cguids[index]) == SDF_SUCCESS);
	}

    return(0);
}

int main(int argc, char *argv[])
{
    int i;

    if ( argc < 3 ) {
        fprintf( stderr, "Usage: %s <threads> <iterations>\n", argv[0] );
        return 0;
    } else {
        threads = atoi( argv[1] );
        iterations = atoi( argv[2] );
    }

    pthread_t thread_id[threads];

    if (SDFInit(&sdf_state, 0, NULL) != SDF_SUCCESS) {
		fprintf(stderr, "SDF initialization failed!\n");
		plat_assert(0);
    }

    fprintf(stderr, "SDF was initialized successfully!\n");

    for(i = 0; i < threads; i++)
		pthread_create(&thread_id[i], NULL, worker, (void*)(long)i);

    for(i = 0; i < threads; i++)
		pthread_join(thread_id[i], NULL);

    fprintf(stderr, "DONE\n");

    return(0);
}
