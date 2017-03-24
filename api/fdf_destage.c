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
 *  fdf_destage.c   12/8/16   Brian O'Krafka   
 *
 *  Code to initialize Write destager module for ZetaScale b-tree.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 *  Notes:
 *
 **********************************************************************/

#include "btree/btree_raw_internal.h"
#include "ws/destage.h"
#include "api/zs.h"
#include "fth/fthLock.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/hash.h"
#include "protocol/action/action_internal_ctxt.h"
#include "protocol/action/action_new.h"
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <aio.h>
#include <fcntl.h>

//  imported from btree/fdf_wrapper.c:
extern ZS_status_t _ZSInitPerThreadState(struct ZS_state *zs_state, struct ZS_thread_state **thd_state);

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_TRACE PLAT_LOG_LEVEL_TRACE
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_DIAG PLAT_LOG_LEVEL_DIAGNOSTIC
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR

static int ds_per_thread_state_cb(struct ZS_state *pzs, struct ZS_thread_state **pzst);
static int ds_client_ops_cb(struct ZS_thread_state *pzs, void *state, uint64_t *n_rd, uint64_t *n_wr, uint64_t *n_del);

int init_write_destager_subsystem(struct ZS_state *pzs)
{
    destager_config_t     cfg;
    struct destager      *pds;

    ds_load_default_config(&cfg);

    cfg.per_thread_cb       = ds_per_thread_state_cb;
    cfg.client_ops_cb       = ds_client_ops_cb;

    pds = destage_init(pzs, &cfg);
    if (pds == NULL) {
        return(1);
    }
    destage_dump_config(stderr, pds);

    return(0);
}

/*  Callback for allocating per-thread ZS state.
 *  Returns 0 for success, 1 for error.
 */
static int ds_per_thread_state_cb(struct ZS_state *pzs, struct ZS_thread_state **pzst)
{
    ZS_status_t       status;

    //Initialize per-thread ZS state for this thread.
    if ((status = _ZSInitPerThreadState(pzs, pzst)) != ZS_SUCCESS) {
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_ERR, "In ws_per_thread_state_cb, ZSInitPerThreadState failed with error %s\n", ZSStrError(status));
	return(1);
    }
    return(0);
}


/*  Callback for getting number of client ops.
 *  Returns 0 for success, 1 for error.
 */

static int ds_client_ops_cb(struct ZS_thread_state *pzs, void *state, uint64_t *n_rd, uint64_t *n_wr, uint64_t *n_del)
{
    ZS_stats_t    stats;
    ZS_status_t   status;
    int           i;
    ZS_cguid_t    cguids[64*1024];
    uint32_t      n_cguids;

    *n_rd  = 0;
    *n_wr  = 0;
    *n_del = 0;

    status = ZSGetContainers(pzs, cguids, &n_cguids);
    if (status != ZS_SUCCESS) {
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_ERR, "ws_client_ops_cb failed in call to ZSGetContainers");
	return(1);
    }

    for (i=0; i<n_cguids; i++) {
	status = ZSGetContainerStats(pzs, cguids[i], &stats);
	if (status != ZS_SUCCESS) {
	    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, LOG_ERR, "ws_client_ops_cb failed in call to ZSGetContainerStats");
	    return(1);
	}

	*n_rd  += stats.n_accesses[ZS_ACCESS_TYPES_READ];
	*n_wr  += stats.n_accesses[ZS_ACCESS_TYPES_WRITE];
	*n_del += stats.n_accesses[ZS_ACCESS_TYPES_DELETE];
    }

    return(0);
}

