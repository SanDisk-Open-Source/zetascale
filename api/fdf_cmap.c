/*
 * File:   fdf_meta_cache.c
 * Author: Darryl Ouye
 *
 * Created on February 5, 2013
 *
 * SanDisk Proprietary Material, © Copyright 2013 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#include "sdf_internal.h"
#include "fdf.h"

FDF_status_t fdf_cmap_allocate(
	FDF_cguid_t		 cguid,
	char			*cname,
	uint64_t		size_kb
	)
{
	return FDF_SUCCESS;
}

FDF_status_t fdf_cmap_deallocate(
	FDF_cguid_t	cguid 
	)
{
	return FDF_SUCCESS;
}

ctnr_map_t *fdf_cmap_find_by_cguid(
	FDF_cguid_t	 cguid
	)
{
	return NULL;
}

ctnr_map_t *fdf_cmap_find_by_cname(
	char *cname
	)
{
	return NULL;
}

void fdf_cmap_free_copy(
	ctnr_map_t *map
	)
{
}

FDF_status_t fdf_cmap_set_container(
	FDF_cguid_t		cguid,
	SDF_CONTAINER	sdf_container
	)
{
	return FDF_SUCCESS;
}

FDF_status_t fdf_cmap_set_max_size(
	FDF_cguid_t	cguid,
	uint64_t	size_kb
	)
{
	return FDF_SUCCESS;
}

FDF_status_t fdf_cmap_set_current_size(
	FDF_cguid_t	cguid,
	uint64_t	current_size
	)
{
	return FDF_SUCCESS;
}

FDF_status_t fdf_cmap_set_num_obj(
	FDF_cguid_t	cguid,
	uint64_t	num_obj
	)
{
	return FDF_SUCCESS;
}
