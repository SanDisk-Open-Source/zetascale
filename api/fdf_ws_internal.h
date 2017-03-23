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
 *  fdf_ws_internal.h   8/31/16   Brian O'Krafka   
 *
 *  Code to initialize Write serializer module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 *  Notes:
 *
 **********************************************************************/

#ifndef _FDF_WS_INTERNAL_H
#define _FDF_WS_INTERNAL_H

#include "api/fdf_ws.h"

typedef struct gc_ws_state {
    struct ZS_state           *pzs;
    ZS_cguid_t                 md_cguid;
    struct ws_state           *ps;
} gc_ws_state_t;

typedef struct dataptrs {
    uint32_t      n;
    uint32_t      n_in_stripe;
    char        **keybufs;
    uint64_t     *keyptrs;
    uint32_t     *keylens;
    uint32_t     *keydatasizes;
    int          *copyflags;
} dataptrs_t;
            
typedef struct ht_handle {
    fthWaitEl_t            *lk_wait;
    hash_entry_t           *hash_entry;
    ZS_cguid_t              cguid;
    mcd_osd_shard_t        *shard;
    uint64_t                syndrome;
    uint64_t                logical_id;
    uint32_t                nodesize;
    struct ZS_thread_state *pzs;
} ht_handle_t;

typedef struct bv {
    uint32_t    n_ints;
    uint32_t   *ints;
} bv_t;

typedef struct gc_read_cb_data {
    void         **pp_to;
    dataptrs_t    *dp;
    dataptrs_t    *dp2;
    bv_t          *accounted;
    void          *pbuf;
    uint32_t       sector_bytes;
    uint32_t       sectors_per_node;
    uint64_t       old_logical_id;
} gc_read_cb_data_t;

#endif
