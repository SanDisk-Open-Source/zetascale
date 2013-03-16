/*
 * File:   sdfclient/open_container_mgr.c
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 10:40 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: open_container_mgr.c 10527 2009-12-12 01:55:08Z drew $
 */
#include "common/sdftypes.h"
#include "name_service.h"
#include "platform/string.h"
#include "platform/logging.h"
#include "name_service.h"
#include "open_container_mgr.h"
#include "open_container_map.h"


int
doesContainerExistInBackend(SDF_internal_ctxt_t *pai, const char *path)
{
    int code = 0;

    if (name_service_meta_exists(pai, path) == SDF_SUCCESS) {
        code = 1;
        plat_log_msg(21617,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "Container %s exists in backend", path);
    } else {
        plat_log_msg(21618,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "Container %s does not exist in backend", path);
    }

    return (code);
}

SDF_CONTAINER_PARENT
isParentContainerOpened(const char *path)
{
    SDF_CONTAINER_PARENT parent;

    if (!isContainerParentNull(parent = containerMap_getParent(path))) {
        // _sdf_print_parent_container_structure(parent); // DEBUG
    }

    return (parent);
}


SDF_CONTAINER_PARENT
addContainerParent(SDF_internal_ctxt_t *pai, const char *path)
{
    SDF_CONTAINER_PARENT parent = createContainerParent();
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
    SDF_container_props_t props;
    SDF_container_meta_t meta;

    // { Initialize parent
    if ((name_service_get_meta_from_cname(pai, path, &meta)) != SDF_SUCCESS) {
        plat_log_msg(21619,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_ERROR,
                     "failed to get meta - %s", path);
    }

    props = meta.properties;

    lparent->blockSize = props.specific.block_props.blockSize;
    lparent->container_type = props.container_type.type;

    // TODO separate out dir and name
    lparent->dir = plat_strdup(path);
    lparent->name = plat_strdup(path);
    lparent->node = meta.node;  // Home node
    lparent->cguid = meta.cguid;  // Container guid
    lparent->num_shards = props.shard.num_shards; // Number of shards in container
    lparent->num_open_descriptors = 0;
    lparent->delete_pending = SDF_FALSE;
    lparent->open_containers = containerNull;
    lparent->bucket_next = containerParentNull;
    // } Initialize parent
    
    releaseLocalContainerParent(&lparent);

    // _sdf_print_parent_container_structure(parent); // DEBUG
    return (parent);
}

// STEP 1: Create the descriptor
// STEP 2: add it to the parent structure
SDF_CONTAINER
addContainerDescriptor(SDF_CONTAINER_PARENT parent)
{
    // _sdf_print_parent_container_structure(parent); // DEBUG
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
    
    // STEP 1
    // { Create SDF_CONTAINER structure
    SDF_CONTAINER container = createContainer();
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
    // add this container to the list of open containers
    lc->next = lparent->open_containers;
    lc->parent = parent;
    lc->ptr = descrChangesNull;
    lc->node = lparent->node;     // Home node
    lc->cguid = lparent->cguid;     // Container guid
    lc->num_shards = lparent->num_shards; // Num shards/container
    lc->blockSize = lparent->blockSize;
    lc->info = NULL;
    lc->info_index = 0;

    // } Create SDF_CONTAINER structure

    // STEP 2
    __sync_fetch_and_add(&(lparent->num_open_descriptors), 1);
    lparent->open_containers = container;

    releaseLocalContainer(&lc);
    releaseLocalContainerParent(&lparent);
    
    return (container);
}

// STEP 1: Find the parent structure
// STEP 2: Remove the container descriptor from the parent
int
delContainerDescriptor(const char *path, SDF_CONTAINER container)
{
    int status = 0;
    SDF_CONTAINER prev = containerNull;
    SDF_CONTAINER this = containerNull;
    SDF_CONTAINER_PARENT parent = containerParentNull;

    if (ISEMPTY(path)) {
        plat_log_msg(21620,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "delContainerDescriptor: invalid path");
    } else  if (isContainerParentNull(parent = containerMap_getParent(path))) {
        plat_log_msg(21621,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "delContainerDescriptor: failed to find parent for %s", path);
    } else {
        // _sdf_print_parent_container_structure(parent); // DEBUG
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
        // Find the open container descriptor in the parent's list
        this = lparent->open_containers;
        while (!isContainerNull(this)) {
            local_SDF_CONTAINER lthis = getLocalContainer(&lthis, this);
            if (containerPtrEqual(this, container)) {
                // Found it - unlink
                
                if (isContainerNull(prev)) {
                    // Removing 1st descriptor in the list
                    lparent->open_containers = lthis->next;
                } else {
                    local_SDF_CONTAINER lprev = getLocalContainer(&lprev, prev);
                    lprev->next = lthis->next;
                    releaseLocalContainer(&lprev);
                }
                
		__sync_fetch_and_sub(&(lparent->num_open_descriptors), 1);
                freeContainer(this);
                this = containerNull;
                status = 1;
            } else {
                prev = this;
                this = lthis->next;
            }
            releaseLocalContainer(&lthis);
        }
        releaseLocalContainerParent(&lparent);
    }
    return (status);
}

// STEP 1: check if parent exists in map
// STEP 2: create parent
SDF_CONTAINER_PARENT
createParentContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_meta_t *meta)
{
    SDF_CONTAINER_PARENT parent = containerParentNull;
    SDF_container_props_t properties = meta->properties;

    if (!isContainerParentNull(parent = containerMap_getParent(path))) { // STEP 1
        plat_log_msg(21622,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "createParentContainer: container exist for %s in map", path);
    } else { // STEP 2
        parent = addContainerParent(pai, path); // Create a parent object
        local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
        lparent->container_type = properties.container_type.type;
        lparent->node = meta->node; // SET THE CONTAINER HOME NODE
        lparent->cguid = meta->cguid; // SET THE CONTAINER GUID
        lparent->num_shards = properties.shard.num_shards; // SET SHARDS/CONTAINER
        lparent->container_id = properties.container_id.container_id;

        plat_log_msg(21623,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "NODE/GUID for %s = %lu/%llu", 
		     path, (unsigned long)lparent->node, 
		     (unsigned long long)lparent->cguid);

        releaseLocalContainerParent(&lparent);
        // _sdf_print_parent_container_structure(parent); // DEBUG
        if (!containerMap_addParent(path, parent)) { // Add to map
            // TODO log error
            freeContainerParent(parent);
            parent = containerParentNull;
            plat_log_msg(21624,
                         PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_TRACE,
                         "failed to add map for %s", path);
        } else {
            plat_log_msg(21625,
                         PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_TRACE,
                         "successfully added map for %s", path);
        }
    }

    return (parent);
}

// STEP 1: check if parent exists in map
// STEP 2: check with backend if container already exists
// STEP 3: create parent, add container descriptor to it
SDF_CONTAINER
openParentContainer(SDF_internal_ctxt_t *pai, const char *path)
{
    SDF_CONTAINER_PARENT parent = containerParentNull;
    SDF_CONTAINER container = containerNull;

    if (!isContainerParentNull(parent = isParentContainerOpened(path))) { // STEP 1
        // _sdf_print_parent_container_structure(parent); // DEBUG
        container = addContainerDescriptor(parent);
    } else if (!doesContainerExistInBackend(pai, path)) { // STEP 2
        container = containerNull;
    } else {
        parent = addContainerParent(pai, path);
        if (!containerMap_addParent(path, parent)) {
            // TODO log error
            freeContainerParent(parent);
            parent = containerParentNull;
        } else {
            container = addContainerDescriptor(parent);
        }
    }

    return (container);
}

// STEP 1: check if parent exists in map
// STEP 2: delete descriptor from parent chain
// STEP 3: remove the parent if all children are closed
int
closeParentContainer(SDF_CONTAINER container)
{
    int status = 0;
    SDF_CONTAINER_PARENT parent = containerParentNull;
    local_SDF_CONTAINER lc = getLocalContainer(&lc, container);
    parent = lc->parent;
    local_SDF_CONTAINER_PARENT lparent = getLocalContainerParent(&lparent, parent);
    const char *path = lparent->dir; // TODO actually lparent->dir + lparent->name

    // _sdf_print_parent_container_structure(parent); // DEBUG

    if (!delContainerDescriptor(path, container)) {
        plat_log_msg(21626,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "closeParentContainer: failed to close %s", path);
    } else if (lparent->num_open_descriptors <= 0) {

        if (!isContainerParentNull(parent = containerMap_removeParent(path))) {
	    // All children closed - remove parent
	    if (lparent->name) {
		plat_free(lparent->name);
	    }
	    if (lparent->dir) {
		plat_free(lparent->dir);
	    }

            freeContainerParent(parent);
            status = 1;
            plat_log_msg(21627,
                         PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_TRACE,
                         "closeParentContainer: closed parent");
        } else {
            plat_log_msg(21628,
                         PLAT_LOG_CAT_SDF_SHARED,
                         PLAT_LOG_LEVEL_TRACE,
                         "closeParentContainer: failed to remove parent for %s", path);
        }
    } else {
        status = 1;
        plat_log_msg(21629,
                     PLAT_LOG_CAT_SDF_SHARED,
                     PLAT_LOG_LEVEL_TRACE,
                     "closeParentContainer: closed descriptor for %s", path);
    }
    
    releaseLocalContainerParent(&lparent);
    releaseLocalContainer(&lc);
    return (status);
}
