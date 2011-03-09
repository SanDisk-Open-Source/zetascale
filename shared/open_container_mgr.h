/*
 * File:   open_container_mgr.h
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 11:49 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: open_container_mgr.h 4936 2008-12-09 20:14:15Z darryl $
 */

#ifndef _OPEN_CONTAINER_MGR_H
#define _OPEN_CONTAINER_MGR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "container.h"

SDF_CONTAINER_PARENT 
createParentContainer(SDF_internal_ctxt_t *pai, const char *path, SDF_container_meta_t *meta);
SDF_CONTAINER openParentContainer(SDF_internal_ctxt_t *pai, const char *path);
int doesContainerExistInBackend(SDF_internal_ctxt_t *pai, const char *path);
int closeParentContainer(SDF_CONTAINER container);

SDF_CONTAINER_PARENT
isParentContainerOpened(const char *path);


#ifdef __cplusplus
}
#endif

#endif /* _OPEN_CONTAINER_MGR_H */
