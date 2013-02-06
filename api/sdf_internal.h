//  sdf_internal.h

#ifndef __SDF_INTERNAL_H
#define __SDF_INTERNAL_H

#include <stdint.h>
#include <inttypes.h>
#include "common/fdftypes.h"
#include "sdf.h"
#include "fdf.h"

#define MAX_CONTAINERS  1000

typedef struct ctnr_map {
	FDF_boolean_t	allocated;
    char            cname[CONTAINER_NAME_MAXLEN];
    FDF_cguid_t     cguid;
    SDF_CONTAINER   sdf_container;
	uint64_t		size_kb;
	uint64_t		current_size;
	uint64_t		num_obj;
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
#endif // __SDF_INTERNAL_H
