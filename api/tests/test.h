/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef __ZS_TEST__
#define __ZS_TEST__

ZS_status_t
zs_init();

ZS_status_t zs_shutdown();

ZS_status_t zs_init_thread();

ZS_status_t zs_release_thread();

ZS_status_t zs_transaction_start();
ZS_status_t zs_transaction_commit();

ZS_status_t zs_create_container (
	char                    *cname,
	uint64_t				size,
	ZS_cguid_t             *cguid
	);

ZS_status_t zs_create_container_dur (
	char                    *cname,
	uint64_t				size,
	ZS_durability_level_t  dur,
	ZS_cguid_t             *cguid
	);

ZS_status_t zs_open_container (
	char                    *cname,
	uint64_t				size,
	ZS_cguid_t             *cguid
	);

ZS_status_t zs_close_container (
	ZS_cguid_t             cguid
	);

ZS_status_t zs_delete_container (
	ZS_cguid_t             cguid
	);

ZS_status_t zs_rename_container (
	ZS_cguid_t             cguid,
        char                  *cname
	);

ZS_status_t zs_flush_container (
    ZS_cguid_t                cguid
       );

ZS_status_t zs_get (
	ZS_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	   );

ZS_status_t zs_free_buffer(
	char 					*data
	);

ZS_status_t zs_enumerate (
	ZS_cguid_t 			  cguid,
	struct ZS_iterator		**_zs_iterator
	);

ZS_status_t zs_next_enumeration(
	ZS_cguid_t               cguid,
	struct ZS_iterator		 *_zs_iterator,
	char                     **key,
	uint32_t                  *keylen,
	char                     **data,
	uint64_t                  *datalen
	);

ZS_status_t zs_finish_enumeration (
	ZS_cguid_t  			 cguid,
	struct ZS_iterator		*_zs_iterator
	);

ZS_status_t zs_set (
	ZS_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen,
	char					*data,
	uint64_t				 datalen
	);

ZS_status_t zs_delete(
	ZS_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen
	);

ZS_status_t zs_flush(
	ZS_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen
	);

ZS_status_t zs_get_containers(
	ZS_cguid_t              *cguids,
	uint32_t				 *n_cguids
	);

ZS_status_t zs_get_stats(ZS_stats_t *stats);
ZS_status_t zs_get_container_stats(ZS_cguid_t cguid, ZS_stats_t *stats);

void
advance_spinner();

#define t(func, res) ({ \
	ZS_status_t r = func; \
	if(r != res) \
		fprintf(stderr, "%x %s:%d %s %s=%s - Expected: %s - %s\n", \
		(int)pthread_self(), basename(__FILE__), __LINE__, __FUNCTION__, \
		#func, ZSStrError(r), #res, r == res ? "OK": "FAILED"); \
	if(r != res) \
		exit(1); \
	r; })

int set_objs(ZS_cguid_t cguid, long thr, int size, int start_id, int count, int step);
int del_objs(ZS_cguid_t cguid, long thr, int start_id, int count, int step);
int get_objs(ZS_cguid_t cguid, long thr, int start_id, int count, int step);
int enum_objs(ZS_cguid_t cguid);

#endif /* __ZS_TEST__ */

