//  sdf_internal.h

#ifndef __SDF_INTERNAL_H
#define __SDF_INTERNAL_H

#include <stdint.h>
#include <inttypes.h>
#include "sdftcp/locks.h"
#include "common/fdftypes.h"
#include "sdf.h"
#include "fdf.h"

#define MAX_CONTAINERS  1000


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
} FDF_container_stats_t;

typedef enum {
    FDF_CONTAINER_STATE_UNINIT,  	  /* Container is uninitialized */
    FDF_CONTAINER_STATE_CLOSED,       /* Container is closed */
    FDF_CONTAINER_STATE_OPEN,         /* Container is Open */
    FDF_CONTAINER_STATE_DELETE_PROG,  /* Container submitted for async delete */
    FDF_CONTAINER_STATE_DELETE_OPEN,  /* Container submitted for async delete */
    FDF_CONTAINER_STATE_DELETE_CLOSED, /* Container submitted for async delete */
}FDF_CONTAINER_STATE;

typedef struct ctnr_map {
    char            	 	cname[CONTAINER_NAME_MAXLEN];	/* Container name */
	int     			 	io_count;						/* IO in flight count */
    FDF_cguid_t     	 	cguid;							/* Container ID */
    SDF_CONTAINER   	 	sdf_container;					/* Open container handle */
	uint64_t			 	size_kb;						/* Container size KB */
	uint64_t			 	current_size;					/* Current container size */
	uint64_t			 	num_obj;						/* Current number of objects */
    FDF_CONTAINER_STATE  	state;							/* Container state */
	FDF_boolean_t   	 	evicting;						/* Eviction mode */
    enum_stats_t 		 	enum_stats;						/* Enumeration stats */
    FDF_container_stats_t 	container_stats;				/* Container stats */
} ctnr_map_t;


typedef struct SDF_state {
#if 0
    struct SDFTLMap   *ctnr_map_by_name;
    struct SDFTLMap   *ctnr_map_by_cguid[MAX_CONTAINERS];
#endif
    uint64_t           cguid_cntr;

} SDF_state_t;

typedef struct SDF_iterator {
    uint64_t          addr;
    uint64_t          prev_seq;
    uint64_t          curr_seq;
    struct shard     *shard;
    SDF_cguid_t       cguid;
} SDF_iterator_t;

extern int get_ctnr_from_cguid(FDF_cguid_t cguid);
extern int get_ctnr_from_cname(char *cname);

void rel_cntr_map(ctnr_map_t *cmap);
ctnr_map_t *get_cntr_map(cntr_id_t cntr_id);
int inc_cntr_map(cntr_id_t cntr_id, int64_t objs, int64_t blks, int check);

// Container metadata map API

/**
 * @brief Allocate a metadata map entry
 *
 * @param cguid <IN> container id
 * @param cname <IN> container name
 * @param size_kb <IN> max size in kb
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_allocate(
	FDF_cguid_t		 cguid,
	char			*cname,
	uint64_t		size_kb
	);

/**
 * @brief Deallocate a metadata map entry
 *
 * @param cguid <IN> container id
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_deallocate(
	FDF_cguid_t	cguid 
	);

/**
 * @brief Find a metadata map entry by cguid
 *
 * @param cguid <IN> container id
 * @return pointer to copy of map (caller must free)
 *		   
 */
ctnr_map_t *fdf_cmap_find_by_cguid(
	FDF_cguid_t	 cguid
	);

/**
 * @brief Find a metadata map entry by cname
 *
 * @param cname <IN> container name
 * @return pointer to copy of map (caller must free)
 */
ctnr_map_t *fdf_cmap_find_by_cname(
	char *cname
	);

/**
 * @brief Free a metadata map copy
 *
 * @param map <IN> metadata map copy
 */
void fdf_cmap_free_copy(
	ctnr_map_t *map
	);

/**
 * @brief Update a metadata map entry open container handle
 *
 * @param cguid <IN> container id
 * @param sdf_container <IN> open container handle 
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_set_container(
	FDF_cguid_t		cguid,
	SDF_CONTAINER	sdf_container
	);

/**
 * @brief Update a metadata map entry max size field
 *
 * @param cguid <IN> container id
 * @param size <IN> max size in kb
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_set_max_size(
	FDF_cguid_t	cguid,
	uint64_t	size_kb
	);

/**
 * @brief Update a metadata map entry current size field
 *
 * @param cguid <IN> container id
 * @param current_size <IN> current size in bytes
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_set_current_size(
	FDF_cguid_t	cguid,
	uint64_t	current_size
	);

/**
 * @brief Update a metadata map entry current num objects field
 *
 * @param cguid <IN> container id
 * @param num_obj <IN> current number of objects
 * @return FDF_SUCCESS on success
 *		   FDF_FAILURE on failure
 */
FDF_status_t fdf_cmap_set_num_obj(
	FDF_cguid_t	cguid,
	uint64_t	num_obj
	);

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
			  FDF_boolean_t *evicting);
#endif // __SDF_INTERNAL_H
