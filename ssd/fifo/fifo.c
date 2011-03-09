/*
 * File:   fifo.c
 * Author: Brian O'Krafka
 *
 * Created on January 8, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: fifo.c 10527 2009-12-12 01:55:08Z drew $
 */

#define _FIFO_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <aio.h>
#include "platform/logging.h"
#include "fth/fth.h"
#include "ssd/ssd.h"
#include "ssd/ssd_local.h"
#include "ssd/ssd_aio.h"
#include "ssd/ssd_aio_local.h"
#include "fifo.h"
#include "utils/hash.h"
#include "common/sdftypes.h"


#undef  flashOpen
#undef  shardCreate
#undef  shardOpen
#undef  shardSync
#undef  shardDelete
#undef  shardStart
#undef  shardStop
#undef  flashGet
#undef  flashPut
#undef  flashFreeBuf
#undef  flashStats
#undef  flashGetHighSequence
#undef  flashSetSyncedSequence
#undef  flashGetIterationCursors
#undef  flashGetByCursor
#undef  flashGetRetainedTombstoneGuarantee
#undef  flashRegisterSetRetainedTombstoneGuaranteeCallback

/*
 * fifo specific operations
 */
ssd_fifo_ops_t      Ssd_fifo_ops = {
    .flashOpen          = NULL,
    .shardCreate        = NULL,
    .shardOpen          = NULL,
    .flashGet           = NULL,
    .flashPut           = NULL,
    .flashFreeBuf       = NULL,
    .flashStats         = NULL,
    .shardSync          = NULL,
    .shardDelete        = NULL,
    .shardStart         = NULL,
    .shardStop          = NULL,
    .flashGetHighSequence     = NULL,
    .flashSetSyncedSequence   = NULL,
    .flashGetIterationCursors = NULL,
    .flashGetByCursor         = NULL,
    .flashGetRetainedTombstoneGuarantee = NULL,
    .flashRegisterSetRetainedTombstoneGuaranteeCallback = NULL,
};


struct flashDev * fifo_flashOpen( char * devName, int flags ) 
{
    int                 i;
    int                 rc;
    struct flashDev   * pdev;

    plat_log_msg(21691, 
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                  PLAT_LOG_LEVEL_INFO,
                  "ENTERING, devName=%s", devName );
    
    pdev = plat_alloc( sizeof(struct flashDev) );
    if ( NULL == pdev ) {
	plat_log_msg(21692, PLAT_LOG_CAT_FLASH, 
                     PLAT_LOG_LEVEL_ERROR, "failed to alloc dev");
        return NULL;
    }

    for ( i = 0; i < FTH_MAX_SCHEDS; i++ ) {
	pdev->stats[i].flashOpCount = 0;
	pdev->stats[i].flashReadOpCount = 0;
	pdev->stats[i].flashBytesTransferred = 0;
    }
    pdev->shardList = NULL;
    InitLock( pdev->lock );

    /*
     * initialize the aio subsystem
     */
    pdev->paio_state = plat_alloc( sizeof(struct ssdaio_state) );
    if ( NULL == pdev->paio_state ) {
	plat_log_msg(21693, PLAT_LOG_CAT_FLASH, 
                     PLAT_LOG_LEVEL_ERROR, "failed to alloc aio state");
	plat_free(pdev);
        return NULL;
    }
    
    rc = ssdaio_init( pdev->paio_state, devName );
    if ( 0 != rc ) {
	plat_log_msg(21694, PLAT_LOG_CAT_FLASH, 
                     PLAT_LOG_LEVEL_ERROR, "failed to init aio");
	plat_free(pdev->paio_state);
	plat_free(pdev);
        return NULL;
    }
    
    pdev->size = pdev->paio_state->size;
    pdev->used = 0;
    plat_log_msg(21695, 
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                  PLAT_LOG_LEVEL_INFO,
                  "dev size is %lu", pdev->size );
    
    if ( NULL == Ssd_fifo_ops.flashOpen ) {
        plat_log_msg(21696, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashOpen not implemented!" );
        plat_abort();
    }
        
    Ssd_fifo_ops.flashOpen( devName, flags );

    return pdev;
}


struct shard *fifo_shardCreate( struct flashDev * dev, uint64_t shardID, 
                                int flags, uint64_t quota, unsigned maxObjs ) 
{
    plat_log_msg(21697, 
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                  PLAT_LOG_LEVEL_INFO,
                  "ENTERING, shardID=%lu max_nobjs=%u", 
                  shardID, maxObjs );
    
    if ( NULL == Ssd_fifo_ops.shardCreate ) {
        plat_log_msg(21698, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardCreate not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.shardCreate( dev, shardID, flags, quota, maxObjs );
}


struct shard *fifo_shardOpen( struct flashDev * dev, uint64_t shardID )
{
    plat_log_msg(20065, 
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                  PLAT_LOG_LEVEL_INFO,
                  "ENTERING, shardID=%lu", shardID );
    
    if ( NULL == Ssd_fifo_ops.shardOpen ) {
        plat_log_msg(21699, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardOpen not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.shardOpen( dev, shardID );
}


int fifo_flashGet( struct ssdaio_ctxt * pctxt, struct shard * shard, 
                   struct objMetaData * metaData, char * key, 
                   char ** dataPtr, int flags )
{
    if ( NULL == Ssd_fifo_ops.flashGet ) {
        plat_log_msg(21700, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashGet not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.flashGet( pctxt, shard, metaData, key, dataPtr, 
                                  flags );
}


int fifo_flashPut( struct ssdaio_ctxt * pctxt, struct shard * shard, 
                   struct objMetaData * metaData, char * key, char * data, 
                   int flags )
{
    if ( NULL == Ssd_fifo_ops.flashPut ) {
        plat_log_msg(21701, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashPut not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.flashPut( pctxt, shard, metaData, key, data, flags );
}


int fifo_flashFreeBuf( void * buf )
{
    if ( NULL == Ssd_fifo_ops.flashFreeBuf ) {
        plat_log_msg(21702, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashFreeBuf not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.flashFreeBuf( buf );
}


uint64_t fifo_flashStats( struct shard * shard, int key )
{
    if ( NULL == Ssd_fifo_ops.flashStats ) {
        plat_log_msg(21703, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashStats not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.flashStats( shard, key );
}

void fifo_shardSync( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.shardSync ) {
        plat_log_msg(21704, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardSync not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.shardSync( shard );
}

int fifo_shardDelete( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.shardDelete ) {
        plat_log_msg(21705, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardDelete not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.shardDelete( shard );
}

int fifo_shardStart( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.shardStart ) {
        plat_log_msg(21706, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardStart not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.shardStart( shard );
}

int fifo_shardStop( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.shardStop ) {
        plat_log_msg(21707, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_shardStop not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.shardStop( shard );
}

uint64_t fifo_flashGetHighSequence( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.flashGetHighSequence ) {
        plat_log_msg(21708, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashGetHighSequence not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.flashGetHighSequence( shard );
}

void fifo_flashSetSyncedSequence( struct shard * shard, uint64_t seqno )
{
    if ( NULL == Ssd_fifo_ops.flashSetSyncedSequence ) {
        plat_log_msg(21709, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashSetSyncedSequence not implemented!" );
        plat_abort();
    }
    
    return Ssd_fifo_ops.flashSetSyncedSequence( shard, seqno );
}

int fifo_flashGetIterationCursors(struct shard *shard, uint64_t seqno_start,
                                  uint64_t seqno_len, uint64_t seqno_max,
                                  const resume_cursor_t * resume_cursor_in,
                                  struct flashGetIterationOutput ** cursors_out)
{
    if ( NULL == Ssd_fifo_ops.flashGetIterationCursors ) {
        plat_log_msg(21710, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashGetIterationCursrors not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.flashGetIterationCursors( shard, seqno_start, seqno_len, seqno_max,
                                                  resume_cursor_in, cursors_out );
}

int fifo_flashGetByCursor( struct shard *shard, int cursor_len, const void *cursor,
                           struct objMetaData *metaData, char **key, void **data, int flags, time_t flush_time )
{
    if ( NULL == Ssd_fifo_ops.flashGetByCursor ) {
        plat_log_msg(21711, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashGetByCursor not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.flashGetByCursor( shard, cursor_len, cursor, metaData, key, data, flags, flush_time );
}

uint64_t fifo_flashGetRetainedTombstoneGuarantee( struct shard * shard )
{
    if ( NULL == Ssd_fifo_ops.flashGetRetainedTombstoneGuarantee ) {
        plat_log_msg(21712, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashGetRetainedTombstoneGuarantee not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.flashGetRetainedTombstoneGuarantee( shard );
}

void fifo_flashRegisterSetRetainedTombstoneGuaranteeCallback( void (*callback)(uint64_t shardID, uint64_t seqno) )
{
    if ( NULL == Ssd_fifo_ops.flashRegisterSetRetainedTombstoneGuaranteeCallback ) {
        plat_log_msg(21713, 
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      PLAT_LOG_LEVEL_FATAL,
                      "fifo_flashRegisterSetRRTGCallback not implemented!" );
        plat_abort();
    }
        
    return Ssd_fifo_ops.flashRegisterSetRetainedTombstoneGuaranteeCallback( callback );
}

