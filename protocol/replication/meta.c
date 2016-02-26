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
 * File:   sdf/protocol/replication/meta.c
 *
 * Author: drew
 *
 * Created on October 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: meta.c 4001 2008-10-23 23:12:09Z drew $
 */

/**
 * Encapsulate the replication layer meta-data
 */

#include "meta.h"

int
replication_meta_need_meta_shard(const SDF_container_props_t *props) {
    return ((props->replication.enabled &&
            props->replication.type != SDF_REPLICATION_SIMPLE &&
            props->replication.type != SDF_REPLICATION_NONE) ? 1 : 0);
}
