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
