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
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_aio.h
 * Author: Xiaonan Ma
 *
 * Created on Mar 11, 2010
 *
 * (c) Copyright 2010, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_aio.c 11778 2010-02-23 00:09:13Z hiney $
 */

#ifndef __MCD_AIO_H__
#define __MCD_AIO_H__

#include "platform/aio_api.h"
#include "mcd_aio_internal.h"


/*
 * By default, we synchronize data to flash.
 */
#define SYNC_DATA 1


/*
 * constants
 */
#define MCD_AIO_FNAME           "/mnt/ssd/schooner%d"
#define MCD_AIO_MAX_NFILES      8
#define MCD_AIO_MAX_NSUBFILES   16

#define abort_on_io_error(r) if (r == FLASH_EIO) plat_abort()

/*
 * aio related
 */
typedef enum {
    MCD_AIO_READ = 1,
    MCD_AIO_WRITE,
} mcd_aio_type_t;

typedef struct mcd_aio_cb {
    /* XXX: this must be first because of the event->obj cast */
    struct iocb         iocb;
    mcd_aio_type_t      type;
    int                 bytes;
    int                 error;
    void              * ctxt;
} mcd_aio_cb_t;

typedef struct mcd_aio_ctxt {
    io_context_t        io_ctxt;

    /* Platform aio state */
    struct {
        /** @brief api parent for all context instantiations */
        struct paio_api *api;

        /** @brief Underlying api for write-combining test */
        struct paio_api *wrapped_api;

        /** @brief platform/aio_api context used in place of io_ctxt */
        struct paio_context *ctxt;

#ifdef MEMCACHED_DEBUG
        /** @brief Error injection api */
        struct paio_api *error_api;

        /** @brief Side-band interface for error injection */
        struct paio_error_control *error_control;
#endif /* def MEMCACHED_DEBUG */
    } paio;

    unsigned use_paio : 1;
} mcd_aio_ctxt_t;

typedef struct aio_state {
    void              * aio_mbox;
    int                 aio_ready;
    uint32_t            aio_pending;
    int                 aio_error;
    fthThread_t       * aio_self;
} aio_state_t;

/************************************************************************
 *                                                                      *
 *                      MCD AIO globals                                 *
 *                                                                      *
 ************************************************************************/

extern int                      Mcd_aio_num_files;
extern int                      Mcd_aio_raid_device;
extern uint64_t                 Mcd_aio_total_size;
extern uint64_t                 Mcd_aio_real_size;
extern uint64_t                 Mcd_aio_strip_size;
extern mcd_aio_ctxt_t           Mcd_aio_ctxt;


/************************************************************************
 *                                                                      *
 *                      MCD AIO routines                                *
 *                                                                      *
 ************************************************************************/

extern aio_state_t *mcd_aio_init_state();
extern void mcd_aio_free_state(aio_state_t *aio_state);

extern int mcd_aio_init( void * state, char * dname );

struct osd_state;
extern int mcd_fth_aio_blk_read( struct osd_state * context, char * buf, uint64_t offset, int nbytes );

extern int mcd_fth_aio_blk_write( struct osd_state * context, char * buf, uint64_t offset, int nbytes );
extern int mcd_fth_aio_blk_write_low( struct osd_state * context, char * buf, uint64_t offset, int nbytes, char sync );

extern void mcd_aio_set_fds( int order[] );

extern void mcd_aio_register_ops( void );

extern int mcd_aio_sync_device_offset( uint64_t offset, int nbytes );

extern int mcd_aio_sync_devices( void );

extern int mcd_aio_get_fd( uint64_t offset, uint64_t bytes );

#endif  /* __MCD_AIO_H__ */
