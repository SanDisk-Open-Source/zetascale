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
 *  destage_internal.h   11/29/16   Brian O'Krafka   
 *
 *  Code for destaging ZetaScale writes.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _DESTAGE_INTERNAL_H
#define _DESTAGE_INTERNAL_H

#include "latd.h"
#include "destage.h"
#include "wshash.h"
#include "mbox.h"
#include "threadpool.h"

#define MAX_BUF_SIZE    (1024*1024)

#define DSBUF_STATE_ITEMS() \
    item(Free) \
    item(BatchQueued) \
    item(WriteQueued) \
    item(Writing) \
    item(CrashSafe)

typedef enum dsbuf_states {
#define item(v) v,
    DSBUF_STATE_ITEMS()
#undef item
} dsbuf_state_t;

#ifdef DESTAGE_C
const char *DSBufStateString(dsbuf_state_t n) {
#define item(v) case v: return (#v);
    switch (n) {
    DSBUF_STATE_ITEMS()
    default:
        return ("INVALID");
    }
#undef item
}
#endif

#define DS_OP_ITEMS() \
    item(Write) \
    item(Delete) \
    item(TxnStart) \
    item(TxnCommit) \
    item(TxnAbort)

typedef enum ds_ops {
#define item(v) v,
    DS_OP_ITEMS()
#undef item
} ds_ops_t;

#ifdef DESTAGE_C
const char *DSOpString(ds_ops_t n) {
#define item(v) case v: return (#v);
    switch (n) {
    DS_OP_ITEMS()
    default:
        return ("INVALID");
    }
#undef item
}
#endif

typedef struct destage_buf {
    uint64_t       seqno;
    dsbuf_state_t  state;
    struct btree  *btree;
    char           key[MAX_KEY_LEN];
    uint32_t       key_len;
    char           buf[MAX_BUF_SIZE];
    uint64_t       size;
    int32_t        n_batch_state;
    uint64_t       cguid;
    ds_ops_t       op;
    uint32_t       flags;
    uint64_t       checksum;
} destage_buf_t;

typedef struct latd_data {
    destage_buf_t    *dsbuf;
} latd_data_t;

typedef struct mbx_entry {
    struct btree *btree;
    char         *key;
    uint32_t      key_len;
    void         *pdata;
    uint64_t      size;
    mbox_t       *client_ack_mbox;
    mbox_t       *batch_ack_mbox;
    uint64_t      cguid;
    uint32_t      op;
    uint64_t      seqno;
    int32_t       n_batch_state;
    uint32_t      flags;
} mbx_entry_t;

typedef struct batch_state {
    uint32_t      i;
    uint64_t      done_cnt;
    uint32_t      batch_size;
    uint64_t      offset;
} batch_state_t;

typedef struct md_entry {
    uint64_t      checksum; //  this must be first!!!
    uint32_t      batch_size;
    uint32_t      key_len;
    uint64_t      size;
    int           op;
    uint64_t      seqno;
    char          key[MAX_KEY_LEN];
    char          data[0];
} md_entry_t;

typedef struct destager {
    struct ZS_state     *pzs;
    uint64_t             master_seqno;
    destager_config_t    config;
    mbox_t               batch_mbox;
    mbox_t              *async_mbox;
    mbox_t               batch_bufs_mbox;
    struct threadpool   *writer_pool;
    struct threadpool   *batch_pool;
    struct threadpool   *stats_pool;
    struct latd         *lat;
    int                  fd_batch;
    batch_state_t       *batch_states;
    uint32_t             user_hz;
    FILE                *f_stats;
    ds_stats_t           stats;
} destager_t;

#endif
