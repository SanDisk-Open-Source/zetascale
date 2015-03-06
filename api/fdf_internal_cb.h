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
 * File  : zs_internal_btree.h
 * Author: Manavalan Krishnan
 *
 * Created on June 12
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#ifndef __ZS_INTERNAL_CB_H
#define __ZS_INTERNAL_CB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdint.h>
#include "common/zstypes.h"
#include "common/fdfstats.h"


/* Data structure to exchange stats from Btree layer */
typedef struct ZS_ext_stat {
    uint64_t       estat; /* external stat */
    uint64_t       fstat; /* ZS stat */
    uint64_t       ftype;  /* ZS Type of the state */
    uint64_t       value; /* Value */
}ZS_ext_stat_t;

struct cmd_token;

/* Structure to hold callbacks registered by external modules and ZS apps
   that ZS can call in to external libraries to manage them */
typedef ZS_status_t (ext_cb_stats_t)(ZS_cguid_t cguid,ZS_ext_stat_t **stats, uint32_t *n_stats );
typedef ZS_status_t (ext_cb_admin_t)(struct ZS_thread_state *thd_state,
                                      FILE *fp, struct cmd_token *tokens, size_t ntokens);
typedef ZS_status_t (ext_cb_flash_stats_t)(uint64_t *alloc_blks, uint64_t *free_segs, uint64_t *consumed_blks, 
                                            uint64_t *, uint64_t *, uint64_t blk_size, uint64_t seg_size);
typedef ZS_status_t (ext_cb_functions_t)(void *log_func);
typedef ZS_status_t (ext_cb_licvalid_t)(int state);
typedef ZS_status_t (ext_cb_raw_t)(int mode, uint64_t rawobjsz, int ratio );
#ifdef FLIP_ENABLED
typedef ZS_status_t (ext_cb_nodetype_t)(ZS_cguid_t cguid, char *data, uint64_t datalen,
                                        uint32_t *nodetype, bool *isroot, uint64_t *logical_id);
#endif

typedef enum {
    ZS_EXT_MODULE_BTREE,
    ZS_EXT_MODULE_APP
}ZS_EXT_MODULE;

typedef struct ZS_ext_cb { 
    /* Call back to get stats from external module */
    ext_cb_stats_t *stats_cb;

    /* Call back to run admin command */
    ext_cb_admin_t *admin_cb;

    /* Callback command to initialize zs buffers that hold flash stats */
    ext_cb_flash_stats_t *flash_stats_buf_cb;

   /* Call back function to pass function pointers to Btree layer */
   ext_cb_functions_t *zs_funcs_cb;
    
   /* Call back function to pass function pointers to Btree layer */
   ext_cb_licvalid_t *zs_lic_cb;

   /* Call back function to check raw object supported */
   ext_cb_raw_t *zs_raw_cb;

#ifdef FLIP_ENABLED
   ext_cb_nodetype_t *zs_node_cb;
#endif
}ZS_ext_cb_t;

ZS_status_t ZSRegisterCallbacks(struct ZS_state *zs_state, ZS_ext_cb_t *cb);
ZS_status_t ZSLicenseCheck(int *state);
ZS_status_t ZSCreateRawObject(struct ZS_thread_state *sdf_thread_state, ZS_cguid_t cguid,
								baddr_t *key, uint64_t datalen, uint32_t flags);
ZS_status_t ZSWriteRawObject(struct ZS_thread_state  *zs_thread_state, ZS_cguid_t cguid,
							 char *key, uint32_t keylen, char *data, uint64_t datalen, uint32_t flags);
ZS_status_t ZSDeleteRawObject(struct ZS_thread_state *sdf_thread_state, ZS_cguid_t cguid,
								baddr_t key, uint32_t keylen, uint32_t flags);
ZS_status_t ZSReadRawObject(struct ZS_thread_state *ZS_thread_state, ZS_cguid_t cguid, baddr_t key,
							char **data, uint64_t *datalen, uint32_t flags);
ZS_status_t ZSGetBtDelContainers(struct ZS_thread_state *ZS_thread_state, ZS_cguid_t *cguid, uint32_t *ncguid);
ZS_status_t ZSRenameContainer(struct ZS_thread_state *ZS_thread_state, ZS_cguid_t cguid, const char *name);

#ifdef __cplusplus
}
#endif
#endif // __ZS_INTERNAL_CB_H
