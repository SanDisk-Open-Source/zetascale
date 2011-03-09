/*
 * File:   sdfclient/open_container_map.h
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 3:06 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: open_container_map.h 609 2008-03-16 08:23:39Z darpan $
 */

#ifndef _OPEN_CONTAINER_H
#define _OPEN_CONTAINER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "container.h"
#include "utils/hashmap.h"

/**
 * Get the parent container structure from the map. Usually done at every create
 * open, close and remove of a container.
 * @param <IN> path path of the container name
 * @return pointer to the parent container structure that <B>should not
 * be freed by the caller</B>. Use removeParent() to remove and free.
 */
SDF_CONTAINER_PARENT containerMap_getParent(const char *path);

/**
 * Add a parent container structure to the map. Usually done when the
 * corresponding container is created or opened for the first time or reopened
 * after all instances of it were closed.
 * @param <IN> path path of the container name
 * @param <IN> ptr pointer to the parent container structure to add to the map.
 * The structure <B>should not be freed by the caller</B>. Use removeParent() to
 * remove and free.
 * @return 1 on success
 */
int containerMap_addParent(const char *path, SDF_CONTAINER_PARENT ptr);

/**
 * Remove a parent container structure from the map. Usually done when the
 * corresponding container is deleted or when all instances of it are closed.
 * @param <IN> path path of the container name
 * @return ptr pointer to the parent container structure that <B>should be
 * freed by the caller</B>.
 */
SDF_CONTAINER_PARENT containerMap_removeParent(const char *path);

/**
 * @brief Initialize the container map data structure.
 *
 * @return 0 on success
 */
int cmap_init();

/**
 * @brief Reset and free the container map data structure.
 */
void cmap_reset();

#ifdef __cplusplus
}
#endif

#endif /* _OPEN_CONTAINER_H */
