/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

//  sdf_internal.h

#ifndef __SDF_INTERNAL_H
#define __SDF_INTERNAL_H

#include <stdint.h>
#include <inttypes.h>
#include "sdftcp/locks.h"
#include "common/zstypes.h"
#include "sdf.h"
#include "zs.h"


/*
 * Statistics returned from enumeration.
 *  num_total          - Total enumerations completed.
 *  num_active         - Active enumerations.
 *  num_cached         - Number of objects enumerated.
 *  num_cached_objects - Number of objects enumerated that were cached.
 */
typedef struct {
    uint64_t num_total;
    uint64_t num_active;
    uint64_t num_objects;
    uint64_t num_cached_objects;
} enum_stats_t;

/*
 * Per container flash statistics.     .
 *  num_evictions      - Number of objects evicted.
 */
typedef struct {
    uint64_t num_evictions;
} ZS_container_stats_t;

typedef enum {
    ZS_CONTAINER_STATE_UNINIT = 0,   /* Container is uninitialized */
    ZS_CONTAINER_STATE_CLOSED,       /* Container is closed */
    ZS_CONTAINER_STATE_OPEN,         /* Container is Open */
    ZS_CONTAINER_STATE_DELETE_PROG,  /* Container submitted for async delete */
    ZS_CONTAINER_STATE_DELETE_OPEN,  /* Container submitted for async delete */
    ZS_CONTAINER_STATE_DELETE_CLOSED, /* Container submitted for async delete */
}ZS_CONTAINER_STATE;

typedef struct cntr_map {
	char			cname[CONTAINER_NAME_MAXLEN];	/* Container name */
	int			io_count;			/* IO in flight count */
	ZS_cguid_t		cguid;				/* Container ID */
	SDF_CONTAINER		sdf_container;			/* Open container handle */
	uint64_t		size_kb;			/* Container size KB */
	uint64_t		current_size;			/* Current container size */
	uint64_t		num_obj;			/* Current number of objects */
	ZS_CONTAINER_STATE	state;				/* Container state */
	ZS_boolean_t		evicting;			/* Eviction mode */
	enum_stats_t		enum_stats;			/* Enumeration stats */
	ZS_container_stats_t	container_stats;		/* Container stats */
	ZS_boolean_t		read_only;			/* Set if Read-Only */
	ZS_boolean_t		lc;				/* Set if logging container */
} cntr_map_t;

typedef struct SDF_state {
    uint64_t           cguid_cntr;

} SDF_state_t;

typedef struct SDF_iterator {
    uint64_t          addr;
    uint64_t          prev_seq;
    uint64_t          curr_seq;
    struct shard     *shard;
    SDF_cguid_t       cguid;
} SDF_iterator_t;

extern int get_ctnr_from_cguid(ZS_cguid_t cguid);
extern int get_ctnr_from_cname(char *cname);

void rel_cntr_map(cntr_map_t *cmap);
cntr_map_t *get_cntr_map(cntr_id_t cntr_id);
int inc_cntr_map(cntr_id_t cntr_id, int64_t objs, int64_t blks, int check);
int inc_cntr_map_by_map(cntr_map_t *cmap, cntr_id_t cntr_id, int64_t objs, int64_t blks, int check);

/*
 * Get information about a container.  Returns 1 on success and 0 on error.
 * Any of the parameters may be null if we are not interested.
 * Defined in fdf.c
 */
int
get_cntr_info(cntr_id_t cntr_id,
              char *name,
              int name_len,
              uint64_t *objs,
              uint64_t *used,
              uint64_t *size,
              ZS_boolean_t *evicting);

/*
 * Container metadata cache
*/
struct cmap_iterator;

ZS_status_t zs_cmap_init( void );

ZS_status_t zs_cmap_destroy( void );

/**
 * @brief Create a metadata map entry
 *
 * @param cname <IN> container name
 * @param cguid <IN> container id
 * @param size_kb <IN> max size in kb
 * @param state <IN> container state
 * @param evicting <IN> container eviction mode
 * @return ZS_SUCCESS on success
 *		   ZS_FAILURE on failure
 */
ZS_status_t zs_cmap_create(
    char                    *cname,
    ZS_cguid_t              cguid,
    uint64_t                 size_kb,
	ZS_CONTAINER_STATE      state,
    ZS_boolean_t            evicting
#if 1//Rico - lc
                            ,
    ZS_boolean_t            lc
#endif
	);

ZS_status_t zs_cmap_update(
    cntr_map_t *cmap
    );

ZS_status_t zs_cmap_replace(
    char                    *cname,
    ZS_cguid_t              cguid,
    uint64_t                 size_kb,
	ZS_CONTAINER_STATE      state,
    ZS_boolean_t            evicting
	);

cntr_map_t *zs_cmap_get_by_cguid(
    ZS_cguid_t cguid
    );

void zs_cmap_rel(
    cntr_map_t *
    );

cntr_map_t *zs_cmap_get_by_cname(
    char *cname
    );

char *zs_cmap_get_cname(
    ZS_cguid_t cguid;
    );

ZS_status_t zs_cmap_delete(
    ZS_cguid_t cguid;
    );

void zs_cmap_destroy_map(
	cntr_map_t *cmap
	);
	
struct cmap_iterator *zs_cmap_enum(void);

int zs_cmap_next_enum( struct cmap_iterator *iterator,
	                    char **key,
	                    uint32_t *keylen,
	                    char **data,
	                    uint64_t *datalen
	                  );

void zs_cmap_finish_enum( struct cmap_iterator *iterator );
#endif // __SDF_INTERNAL_H
