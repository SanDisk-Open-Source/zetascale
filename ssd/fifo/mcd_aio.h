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


/************************************************************************
 *                                                                      *
 *                      MCD AIO routines                                *
 *                                                                      *
 ************************************************************************/

extern int mcd_aio_init( void * state, char * dname );

struct osd_state;
extern int mcd_aio_blk_read(struct osd_state *context, char * buf, uint64_t offset, int nbytes );

extern int mcd_aio_blk_write(struct osd_state *context, char * buf, uint64_t offset, int nbytes );
extern int mcd_aio_blk_write_low(struct osd_state *context, char * buf, uint64_t offset, int nbytes, char sync );

extern void mcd_aio_set_fds( int order[] );

extern void mcd_aio_register_ops( void );

extern int mcd_aio_get_fd( uint64_t offset, uint64_t bytes );

#endif  /* __MCD_AIO_H__ */
