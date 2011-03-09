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

    (pdev->stats[curSchedNum].flashOpCount)++;
    (pdev->stats[curSchedNum].flashReadOpCount)++;
    pdev->stats[curSchedNum].flashBytesTransferred += size;

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

    (pdev->stats[curSchedNum].flashOpCount)++;
    pdev->stats[curSchedNum].flashBytesTransferred += size;

    return Ssd_aio_ops.aio_blk_write( (void *)pctxt, pbuf, offset, (int)size );
}
