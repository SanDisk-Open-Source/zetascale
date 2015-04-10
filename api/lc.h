/*
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 */
#include "common/zstypes.h"

ZS_status_t	lc_init(struct ZS_state *zs_state, bool),
		lc_open( struct ZS_thread_state *, ZS_cguid_t),
		lc_write( struct ZS_thread_state *, ZS_cguid_t, char *, uint32_t, char *, uint64_t),
		lc_delete( struct ZS_thread_state *, ZS_cguid_t, char *, uint32_t),
		lc_trim( struct ZS_thread_state *, ZS_cguid_t, char *, uint32_t),
		lc_read( struct ZS_thread_state *, ZS_cguid_t, char *, uint32_t, char **, uint64_t *),
		lc_enum_start( struct ZS_thread_state *, ZS_cguid_t, void *, char *, uint32_t),
		lc_enum_next( void *, char **, uint32_t *, char **, uint64_t *),
		lc_enum_finish( void *), 
		lc_mput(struct ZS_thread_state *zs_ts, ZS_cguid_t cguid, uint32_t num_objs, ZS_obj_t *objs,
					        uint32_t flags, uint32_t *objs_written);

ZS_status_t LC_init(void **lc, ZS_cguid_t cguid);
