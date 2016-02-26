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

/************************************************************************
 *
 * File:   ssd_aio.h
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd_aio.h 12705 2010-04-02 21:57:14Z xiaonan $
 ************************************************************************/

#ifndef _SSD_AIO_H
#define _SSD_AIO_H

#include "flash/flash.h"

#ifdef __cplusplus
extern "C" {
#endif

    typedef enum {
        SSD_AIO_CTXT_MCD_WORKER = 0,                   /* mcd_fth worker */
        SSD_AIO_CTXT_MCD_POLLER,
        SSD_AIO_CTXT_MCD_WRITER,
        SSD_AIO_CTXT_MCD_HELPER,
        SSD_AIO_CTXT_MCD_REC_INIT,
        SSD_AIO_CTXT_MCD_REC_BLOB,
        SSD_AIO_CTXT_MCD_REC_LABEL,
        SSD_AIO_CTXT_MCD_REC_SPBLK,
        SSD_AIO_CTXT_MCD_REC_CLASS,
        SSD_AIO_CTXT_MCD_REC_RCVR,
        SSD_AIO_CTXT_MCD_REC_UPDT,
        SSD_AIO_CTXT_MCD_REC_LGWR,
        SSD_AIO_CTXT_MCD_REC_FRMT,
        SSD_AIO_CTXT_MCD_REP_ITER,
        SSD_AIO_CTXT_MCD_REP_READ,
        SSD_AIO_CTXT_MCD_REP_LGRD,
        SSD_AIO_CTXT_REC_FTH_ONE,
        SSD_AIO_CTXT_REC_FTH_BIG,
        SSD_AIO_CTXT_REC_FLASH,
        SSD_AIO_CTXT_ACTION_INIT,
        SSD_AIO_CTXT_SYNC_CNTR,
        SSD_AIO_CTXT_SYNC_CNTR_NEW,
        SSD_AIO_CTXT_MAX_COUNT,
    } ssd_aio_ctxt_cat_t;

    static __attribute__((unused))
        const char * Ssd_aio_ctxt_names[] = {
        "mcd_fth_worker",
        "mcd_fth_poller",
        "mcd_fth_writer",
        "mcd_fth_helper",
        "mcd_rec_init",
        "mcd_rec_blob",
        "mcd_rec_label",
        "mcd_rec_superblock",
        "mcd_rec_update_class",
        "mcd_rec_recover",
        "mcd_rec_updater",
        "mcd_rec_log_writer",
        "mcd_rec_format",
        "mcd_rep_iterater",
        "mcd_rep_obj_read",
        "mcd_rep_log_read",
        "recovery_fth_one",
        "recovery_fth_big",
        "recovery_mkmsg_flash",
        "action_new_init_state",
        "simple_rep_sync_cntr",
        "simple_rep_sync_new",
    };

    typedef struct ssdaio_state ssdaio_state_t;

    typedef struct ssdaio_ctxt ssdaio_ctxt_t;

    struct osd_state;

    typedef struct ssd_aio_ops {

        int                 (*aio_init)( void * state, char * dname );

        struct osd_state  * (*aio_init_context)( int category );

        int                 (*aio_free_context)( struct osd_state * context,
                                                 int category );

        int                 (*aio_blk_write)( struct osd_state * context, char * buf,
                                              uint64_t offset, int nbytes );

        int                 (*aio_blk_read)( struct osd_state * context, char * buf,
                                             uint64_t offset, int nbytes );
    } ssd_aio_ops_t;


    extern ssd_aio_ops_t    Ssd_aio_ops;


    extern int ssdaio_init( ssdaio_state_t * psas, char * devName );

    extern ssdaio_ctxt_t * ssdaio_init_ctxt( int category );

    /*
     * @return int If success, return 0.
     */
    extern int ssdaio_free_ctxt( ssdaio_ctxt_t * context , int category );

    extern int
    ssdaio_read_flash( struct flashDev * dev, ssdaio_ctxt_t * pcxt,
                       char * pbuf, uint64_t offset, uint64_t size );

    extern int
    ssdaio_write_flash( struct flashDev * dev, ssdaio_ctxt_t * pcxt,
                        char * pbuf, uint64_t offset, uint64_t size );

#ifdef	__cplusplus
}
#endif

#endif /* _SSD_AIO_H */
