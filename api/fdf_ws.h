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

/**********************************************************************
 *
 *  fdf_ws.h   8/31/16   Brian O'Krafka   
 *
 *  Code to initialize Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 *  Notes:
 *
 **********************************************************************/

#ifndef _FDF_WS_H
#define _FDF_WS_H

#define WS_METADATA_CONTAINER_NAME "__WS_METADATA_CTNR__"

int init_write_serializer_subsystem(struct ZS_thread_state *pzst, struct ZS_state *zs_state, ZS_cguid_t md_cguid, const char *sdev, const char *sbatchdev, int mb, int format_flag, uint32_t sectors_per_node, uint32_t sector_size, int use_new_gc);

#endif
