/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/************************************************************************
 *
 * File:   ssd_aio.c    Functions to Read/Write SSD Flash Drives
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: ssd_aio.c 12705 2010-04-02 21:57:14Z xiaonan $
 ************************************************************************/

#define _SSD_AIO_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "ssd.h"
#include "ssd_aio.h"
#include "ssd_aio_local.h"
#include "ssd_local.h"
#include "utils/hash.h"


ssd_aio_ops_t   Ssd_aio_ops = {
    .aio_init           = NULL,
    .aio_init_context   = NULL,
    .aio_blk_write      = NULL,
    .aio_blk_read       = NULL,
};

//  for stats collection
#define incr(x) __sync_fetch_and_add(&(x), 1)
#define incrn(x, n) __sync_fetch_and_add(&(x), (n))

int ssdaio_init( ssdaio_state_t * psas, char * devName )
{
    if ( NULL == Ssd_aio_ops.aio_init ) {
        plat_log_msg(21718,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "aio_init not implemented!" );
        plat_abort();
    }

    return Ssd_aio_ops.aio_init( (void *)psas, devName );
}


ssdaio_ctxt_t *ssdaio_init_ctxt( int category )
{
    if ( NULL == Ssd_aio_ops.aio_init_context ) {
        plat_log_msg(21719,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "aio_init_context not implemented!" );
        plat_abort();
    }

    return (ssdaio_ctxt_t *)Ssd_aio_ops.aio_init_context( category );
}


int ssdaio_free_ctxt( ssdaio_ctxt_t * context , int category )
{
    if ( NULL == Ssd_aio_ops.aio_free_context ) {
        plat_log_msg(50025,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "aio_free_context not implemented!" );
        plat_abort();
    }

    return Ssd_aio_ops.aio_free_context( (void *)context, category );
}


int ssdaio_read_flash( struct flashDev * pdev, ssdaio_ctxt_t * pctxt,
                       char * pbuf, uint64_t offset, uint64_t size )
{
    if ( NULL == Ssd_aio_ops.aio_blk_read ) {
        plat_log_msg(21720,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "aio_blk_read not implemented!" );
        plat_abort();
    }

    incr(pdev->stats[curSchedNum].flashOpCount);
    incr(pdev->stats[curSchedNum].flashReadOpCount);
    incrn(pdev->stats[curSchedNum].flashBytesTransferred, size);

    return Ssd_aio_ops.aio_blk_read( (void *)pctxt, pbuf, offset, (int)size );
}


int ssdaio_write_flash( struct flashDev * pdev, ssdaio_ctxt_t * pctxt,
                        char * pbuf, uint64_t offset, uint64_t size )
{
    if ( NULL == Ssd_aio_ops.aio_blk_write ) {
        plat_log_msg(21721,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "aio_blk_write not implemented!" );
        plat_abort();
    }

    incr(pdev->stats[curSchedNum].flashOpCount);
    incrn(pdev->stats[curSchedNum].flashBytesTransferred, size);

    return Ssd_aio_ops.aio_blk_write( (void *)pctxt, pbuf, offset, (int)size );
}
