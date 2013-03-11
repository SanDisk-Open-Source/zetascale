/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/mcd_aio.c
 * Author: Xiaonan Ma
 *
 * Created on Dec 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mcd_aio.c 14162 2010-06-14 22:35:33Z hiney $
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <aio.h>
#include <libaio.h>
#include <linux/fs.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include "common/sdftypes.h"
#include "platform/assert.h"
#include "platform/errno.h"
#include "platform/logging.h"
#include "platform/shmem.h"
#include "platform/signal.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/time.h"
#include "platform/unistd.h"
#include "utils/hash.h"
#include "fth/fthMbox.h"
#include "utils/properties.h"

extern int fdf_instance_id;

#include "fth/fth.h"
#include "ssd/ssd_aio.h"
#include "ssd/ssd_aio_local.h"

//#include "memcached.h"
//#include "command.h"
//#include "mcd_sdf.h"
#include "mcd_osd.h"
#include "mcd_aio_internal.h"

#define mcd_log_msg(id, args...)        plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED, ##args )
#define mcd_dbg_msg(...)                plat_log_msg( PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_APP_MEMCACHED, __VA_ARGS__ )

typedef struct mcd_aio_meta {
    SDF_size_t          real_len;
    uint8_t             key_len;
} mcd_aio_meta_t;


// #define MCD_AIO_DEBUGGING

#ifdef  MCD_AIO_DEBUGGING
#  define MCD_AIO_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_AIO_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DIAGNOSTIC
#  define MCD_AIO_LOG_LVL_INFO  PLAT_LOG_LEVEL_INFO
#else
#  define MCD_AIO_LOG_LVL_INFO  PLAT_LOG_LEVEL_DEBUG
#  define MCD_AIO_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_AIO_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DEBUG
#endif


#define FLASH_EIO       FLASH_EAGAIN            /* FIXME */


/*
 * use realtime signal instead of SIGIO
 */
#define SIG_AIO                 (SIGRTMIN + 8)


/*
 * globals
 */
char                    Mcd_aio_base[PATH_MAX + 1] = MCD_AIO_FNAME;
int                     Mcd_aio_num_files       = MCD_AIO_MAX_NFILES; //default
int                     Mcd_aio_sub_files       = 0;
int                     Mcd_aio_raid_device     = 0;
uint64_t                Mcd_aio_total_size      = 0;
uint64_t                Mcd_aio_real_size       = 0;

/*
 * number of AIO errors encourntered
 */
uint64_t        Mcd_fth_aio_errors  = 0;


/*
 * by default this will be set to total_size / num_files to realize
 * a software JBOD
 */
uint64_t                Mcd_aio_strip_size      = 0;

/*
 * AIO context (also used by the poller)
 */
mcd_aio_ctxt_t          Mcd_aio_ctxt;

/*
 * local globals
 */
static int              Mcd_aio_fds[MCD_AIO_MAX_NFILES];
static int              Mcd_aio_sub_fds[MCD_AIO_MAX_NFILES][MCD_AIO_MAX_NSUBFILES];
static fthMbox_t        Mcd_aio_device_sync_mbox[MCD_AIO_MAX_NFILES];
static int              Mcd_aio_sync_enabled    = 0;
static int              Mcd_aio_sync_all        = 0;

/*
 * debugging stats
 */
uint64_t                Mcd_num_aio_ops         = 0;
uint64_t                Mcd_num_aio_signals     = 0;
uint64_t                Mcd_aio_read_ops        = 0;
uint64_t                Mcd_aio_write_ops       = 0;

/*
 * debugging stats
 */
uint64_t         Mcd_num_pending_ios = 0;
uint64_t         Mcd_fth_waiting_io = 0;

/*
 *  Predeclarations
 */

static SDF_status_t mcd_aio_paio_init();

int
mcd_fth_aio_blk_read( osd_state_t * context, char * buf, uint64_t offset, int nbytes )
{
    int                         aio_err = 0;
    int                         aio_fd = Mcd_aio_fds[0];
    int                         submitted;
    int                         strip;
    int                         sub_file;
    uint64_t                    aio_nbytes = nbytes;
    uint64_t                    aio_offset;
    uint64_t                    cur_offset;
    uint32_t                    pending;
    uint64_t                    mail;
    mcd_aio_cb_t                aio_cbs[MCD_AIO_MAX_NFILES];
    mcd_aio_cb_t              * acb = &aio_cbs[0];
    mcd_aio_cb_t              * acbs[MCD_AIO_MAX_NFILES];
    aio_state_t               * aio_state = context->osd_aio_state;

    mcd_log_msg(20010, PLAT_LOG_LEVEL_DEBUG, "ENTERING, offset=%lu nbytes=%d",
                 offset, nbytes );

    /*
     * buffer and offset must be at least 512-byte aligned
     */
    if ( 0 != (uint64_t)buf % 512 || 0 != offset % 512 ) {
        mcd_log_msg(20011, PLAT_LOG_LEVEL_ERROR,
                     "buffer/offset not aligned, buf=%p offset=%lu",
                     buf, offset );
        return FLASH_EINVAL;
    }

    if ( offset + nbytes > Mcd_aio_real_size ) {
        mcd_log_msg(20012, PLAT_LOG_LEVEL_ERROR,
                     "read beyond limit, offset=%lu nbytes=%d limit=%lu",
                     offset, nbytes, Mcd_aio_real_size );
        return FLASH_EINVAL;
    }

    pending = 0;
    submitted = 0;
    do {
        cur_offset = offset + submitted;
        aio_offset = cur_offset;

        if ( 0 ) { /*TRAC 10303. We always use flash as single stripe. So this logic is not required */

            strip = ( cur_offset / Mcd_aio_strip_size ) % Mcd_aio_num_files;

            acb = &aio_cbs[strip];
            aio_fd = Mcd_aio_fds[strip];

            aio_offset = (cur_offset / Mcd_aio_strip_size / Mcd_aio_num_files)
                * Mcd_aio_strip_size + cur_offset % Mcd_aio_strip_size;

            aio_nbytes =
                Mcd_aio_strip_size - (cur_offset % Mcd_aio_strip_size);

            if ( aio_nbytes + submitted > nbytes ) {
                aio_nbytes = nbytes - submitted;
            }

            if ( 0 != Mcd_aio_sub_files ) {
                sub_file =
                    ( aio_offset / Mcd_aio_strip_size ) % Mcd_aio_sub_files;
                aio_fd = Mcd_aio_sub_fds[strip][sub_file];
                aio_offset =
                    ( aio_offset / Mcd_aio_strip_size / Mcd_aio_sub_files )
                    * Mcd_aio_strip_size + aio_offset % Mcd_aio_strip_size;
                mcd_log_msg(20013, PLAT_LOG_LEVEL_DEBUG,
                             "sub_fds[%d][%x]=%d aio_offset=%lu",
                             strip, sub_file, aio_fd, aio_offset );
            }
        }

	if (pread(aio_fd, buf+submitted, aio_nbytes, aio_offset) != aio_nbytes) {
            mcd_log_msg(150031, PLAT_LOG_LEVEL_ERROR, "pread failed - %s",
			plat_strerror(errno));
	    plat_exit(1);
	}

        // io_prep_pread( &acb->iocb, aio_fd, buf + submitted, aio_nbytes, aio_offset );

        acb->bytes = aio_nbytes;
        acb->type = MCD_AIO_READ;
        acb->ctxt = aio_state;

        *(acbs + pending) = acb;
        submitted += aio_nbytes;

        if ( Mcd_aio_num_files < ++pending ) {
            mcd_log_msg(20014, PLAT_LOG_LEVEL_ERROR,
                         "request size beyond strip limit, off=%lu nbytes=%d",
                         offset, nbytes );
            plat_abort();
        }

        mcd_log_msg(20015, MCD_AIO_LOG_LVL_DEBUG,
                     "read iocb ready: fd=%d nbytes=%lu offset=%lu",
                     aio_fd, aio_nbytes, aio_offset );

        if ( nbytes == submitted ) {
            break;
        }
    } while ( 1 );

    return FLASH_EOK;   /* SUCCESS */

    aio_state->aio_pending = pending;
    aio_state->aio_error = 0;

    submitted = Mcd_aio_ctxt.use_paio ?
        paio_submit( Mcd_aio_ctxt.paio.ctxt, pending, (struct iocb **)acbs ) :
        io_submit( Mcd_aio_ctxt.io_ctxt, pending, (struct iocb **)acbs );

    /*
     * Keep track of the pending IOs
     */
    if (submitted > 0) {
	(void) __sync_fetch_and_add( &Mcd_num_pending_ios, submitted );
    }

    if ( pending != submitted ) {
        mcd_log_msg(20016, PLAT_LOG_LEVEL_ERROR,
                     "failed to submit all requests, submitted=%d",
                     submitted );

        pending = __sync_sub_and_fetch(
            &aio_state->aio_pending, (pending - submitted) );

        if ( 0 == pending ) {
            /*
             * no other pending aio requests
             */
            return FLASH_EIO;
        }

        aio_err = 1;
    }
    else {
        mcd_log_msg(20017, PLAT_LOG_LEVEL_DEBUG,
                     "read submitted, off=%lu bytes=%d fd=%d a_off=%lu "
                     "a_bytes=%lu pending=%u",
                     offset, nbytes, aio_fd, aio_offset,
                     aio_nbytes, pending );

        (void) __sync_fetch_and_add( &Mcd_aio_read_ops, pending );
    }

    if ( 0 != submitted ) {
	#ifdef AIO_TRACE
	    uint64_t                 aio_trace_t_start;
	    uint64_t                 aio_trace_t_end;
	    aio_trace_sched_state_t *aio_trace_sched_start;
	    aio_trace_sched_state_t *aio_trace_sched_end;
	    aio_trace_rec_t          aio_trace_rec;

	    aio_trace_t_start     = rdtsc()/fthTscTicksPerMicro;
	    aio_trace_sched_start = fthGetAIOTraceData();
	#endif // AIO_TRACE

        /*
         * Count the number of fthreads waiting on IO
         */
        (void) __sync_fetch_and_add( &Mcd_fth_waiting_io, 1 );

        mail = fthMboxWait( aio_state->aio_mbox );
        mcd_log_msg(20018, PLAT_LOG_LEVEL_DEBUG, "got aio mail" );

	#ifdef AIO_TRACE
	    aio_trace_t_end     = rdtsc()/fthTscTicksPerMicro;
	    aio_trace_sched_end = fthGetAIOTraceData();

	    aio_trace_rec.t_start    =  aio_trace_t_start;
	    aio_trace_rec.t_end      =  aio_trace_t_end;
	    aio_trace_rec.fth        =  (uint64_t) fthSelf();
	    aio_trace_rec.fd         =  aio_fd;
	    aio_trace_rec.size       =  aio_nbytes;
	    aio_trace_rec.submitted  =  submitted;
	    aio_trace_rec.flags      =  0;
	    aio_trace_rec.nsched     =  aio_trace_sched_start->schedNum;

	    if (aio_err) {
		aio_trace_rec.flags     |=  AIO_TRACE_ERR_FLAG;
	    }

	    if (aio_trace_sched_start !=  aio_trace_sched_end) {
		aio_trace_rec.flags  |=  AIO_TRACE_SCHED_MISMATCH_FLAG;
	    }
	    fthLogAIOTraceRec(aio_trace_sched_start, &aio_trace_rec);
	#endif // AIO_TRACE

        aio_err = mail ? mail : aio_err;
    }

    /*
     * FIXME: check for error from submitted requests
     */
    if ( 0 != aio_err ) {
        return FLASH_EIO;
    }

    return FLASH_EOK;   /* SUCCESS */
}


int
mcd_fth_aio_blk_write_low( osd_state_t * context, char * buf, uint64_t offset,
                       int nbytes, char sync )
{
    int                         aio_err = 0;
    int                         aio_fd = Mcd_aio_fds[0];
    int                         submitted;
    int                         strip;
    int                         sub_file;
    uint64_t                    aio_nbytes = nbytes;
    uint64_t                    aio_offset;
    uint64_t                    cur_offset;
    uint32_t                    pending = 0;
    uint64_t                    mail;
    mcd_aio_cb_t                aio_cbs[MCD_AIO_MAX_NFILES];
    mcd_aio_cb_t              * acb = &aio_cbs[0];
    mcd_aio_cb_t              * acbs[MCD_AIO_MAX_NFILES];
    aio_state_t               * aio_state = context->osd_aio_state;

    mcd_log_msg(20010, PLAT_LOG_LEVEL_DEBUG, "ENTERING, offset=%lu nbytes=%d",
                 offset, nbytes );

    /*
     * buffer and offset must be at least 512-byte aligned
     */
    if ( 0 != (uint64_t)buf % 512 || 0 != offset % 512 ) {
        mcd_log_msg(20011, PLAT_LOG_LEVEL_ERROR,
                     "buffer/offset not aligned, buf=%p offset=%lu",
                     buf, offset );
        return FLASH_EINVAL;
    }

    if ( offset + nbytes > Mcd_aio_real_size ) {
        mcd_log_msg(20019, PLAT_LOG_LEVEL_ERROR,
                     "write beyond limit, offset=%lu nbytes=%d limit=%lu",
                     offset, nbytes, Mcd_aio_real_size );
        return FLASH_EINVAL;
    }

    pending = 0;
    submitted = 0;
    do {
        cur_offset = offset + submitted;
        aio_offset = cur_offset;

        if ( 0 ) { /*TRAC 10303. We always use flash as single stripe. So this logic is not required */

            strip = ( cur_offset / Mcd_aio_strip_size ) % Mcd_aio_num_files;

            acb = &aio_cbs[strip];
            aio_fd = Mcd_aio_fds[strip];

            aio_offset = (cur_offset / Mcd_aio_strip_size / Mcd_aio_num_files)
                * Mcd_aio_strip_size + cur_offset % Mcd_aio_strip_size;

            aio_nbytes =
                Mcd_aio_strip_size - (cur_offset % Mcd_aio_strip_size);

            if ( aio_nbytes + submitted > nbytes ) {
                aio_nbytes = nbytes - submitted;
            }

            if ( 0 != Mcd_aio_sub_files ) {
                sub_file =
                    ( aio_offset / Mcd_aio_strip_size ) % Mcd_aio_sub_files;
                aio_fd = Mcd_aio_sub_fds[strip][sub_file];
                aio_offset =
                    ( aio_offset / Mcd_aio_strip_size / Mcd_aio_sub_files )
                    * Mcd_aio_strip_size + aio_offset % Mcd_aio_strip_size;
                mcd_log_msg(20013, PLAT_LOG_LEVEL_DEBUG,
                             "sub_fds[%d][%x]=%d aio_offset=%lu",
                             strip, sub_file, aio_fd, aio_offset );
            }
        }

	if (pwrite(aio_fd, buf+submitted, aio_nbytes, aio_offset) != aio_nbytes) {
            mcd_log_msg(180002, PLAT_LOG_LEVEL_ERROR, "pwrite failed!(%s)", plat_strerror(errno));
	    plat_exit(1);
	}

        // io_prep_pwrite( &acb->iocb, aio_fd, buf + submitted, aio_nbytes, aio_offset );

        acb->bytes = aio_nbytes;
        acb->type = MCD_AIO_WRITE;
        acb->ctxt = aio_state;

        *(acbs + pending) = acb;
        submitted += aio_nbytes;

        if ( Mcd_aio_num_files < ++pending ) {
            mcd_log_msg(20014, PLAT_LOG_LEVEL_ERROR,
                         "request size beyond strip limit, off=%lu nbytes=%d",
                         offset, nbytes );
            plat_abort();
        }

        if ( nbytes == submitted ) {
            break;
        }
    } while ( 1 );

	if(sync)
		fdatasync(aio_fd);

    return FLASH_EOK;   /* SUCCESS */

    aio_state->aio_pending = pending;
    aio_state->aio_error = 0;

    submitted = Mcd_aio_ctxt.use_paio ?
        paio_submit( Mcd_aio_ctxt.paio.ctxt, pending, (struct iocb **)acbs ) :
        io_submit( Mcd_aio_ctxt.io_ctxt, pending, (struct iocb **)acbs );

    /*
     * Keep track of outstanding IOs
     */
    if (submitted > 0) {
	(void) __sync_fetch_and_add( &Mcd_num_pending_ios, submitted );
    }

    if ( pending != submitted ) {
        mcd_log_msg(20016, PLAT_LOG_LEVEL_ERROR,
                     "failed to submit all requests, submitted=%d",
                     submitted );

        pending = __sync_sub_and_fetch(
            &aio_state->aio_pending, (pending - submitted) );

        if ( 0 == pending ) {
            /*
             * no other pending aio requests
             */
            return FLASH_EIO;
        }

        aio_err = 1;
    }
    else {
        mcd_log_msg(20020, PLAT_LOG_LEVEL_DEBUG,
                     "write submitted, off=%lu bytes=%d fd=%d a_off=%lu "
                     "a_bytes=%lu pending=%u",
                     offset, nbytes, aio_fd, aio_offset,
                     aio_nbytes, pending );

        (void) __sync_fetch_and_add( &Mcd_aio_write_ops, pending );
    }

    if ( 0 != submitted ) {

	#ifdef AIO_TRACE
	    uint64_t                 aio_trace_t_start;
	    uint64_t                 aio_trace_t_end;
	    aio_trace_sched_state_t *aio_trace_sched_start;
	    aio_trace_sched_state_t *aio_trace_sched_end;
	    aio_trace_rec_t          aio_trace_rec;

	    aio_trace_t_start     = rdtsc()/fthTscTicksPerMicro;
	    aio_trace_sched_start = fthGetAIOTraceData();
	#endif // AIO_TRACE

        /*
         * Count the number of fthreads waiting on IO
         */
        (void) __sync_fetch_and_add( &Mcd_fth_waiting_io, 1 );

        mail = fthMboxWait( aio_state->aio_mbox );
        mcd_log_msg(20018, PLAT_LOG_LEVEL_DEBUG, "got aio mail" );

        aio_err = mail ? mail : aio_err;

	#ifdef AIO_TRACE
	    aio_trace_t_end     = rdtsc()/fthTscTicksPerMicro;
	    aio_trace_sched_end = fthGetAIOTraceData();

	    aio_trace_rec.t_start    =  aio_trace_t_start;
	    aio_trace_rec.t_end      =  aio_trace_t_end;
	    aio_trace_rec.fth        =  (uint64_t) fthSelf();
	    aio_trace_rec.fd         =  aio_fd;
	    aio_trace_rec.size       =  aio_nbytes;
	    aio_trace_rec.submitted  =  submitted;
	    aio_trace_rec.flags      =  0;
	    aio_trace_rec.nsched     =  aio_trace_sched_start->schedNum;

	    aio_trace_rec.flags     |=  AIO_TRACE_WRITE_FLAG;

	    if (aio_err) {
		aio_trace_rec.flags     |=  AIO_TRACE_ERR_FLAG;
	    }

	    if (aio_trace_sched_start !=  aio_trace_sched_end) {
		aio_trace_rec.flags  |=  AIO_TRACE_SCHED_MISMATCH_FLAG;
	    }
	    fthLogAIOTraceRec(aio_trace_sched_start, &aio_trace_rec);
	#endif // AIO_TRACE
    }

    /*
     * FIXME: check for error from submitted requests
     */
    if ( 0 != aio_err ) {
        return FLASH_EIO;
    }

    return FLASH_EOK;   /* SUCCESS */
}

int
mcd_fth_aio_blk_write( osd_state_t * context, char * buf, uint64_t offset,
                       int nbytes )
{
	return mcd_fth_aio_blk_write_low(context, buf, offset, nbytes, 1);
}


extern int do_drive_sync( int fd );


int
find_sync_device_block( uint64_t arg, dev_t block_dev, char * devname )
{
    int                         fd = -1;
    DIR                       * dir;
    struct dirent             * dirent;
    char                      * devdir = "/dev/";
    struct stat                 st;
    char                        dname[PATH_MAX + 1];

    // open device directory
    dir = opendir( devdir );
    if ( NULL == dir ) {
        mcd_log_msg(20021, PLAT_LOG_LEVEL_FATAL, "couldn't open '%s': %s",
                     devdir, plat_strerror( errno ) );
        plat_abort();
    }

    // read device names
    while ( NULL != (dirent = readdir( dir )) ) {

        // filter criteria
        if ( dirent->d_type != DT_BLK ||
             _D_EXACT_NAMLEN( dirent ) != 3 ||
             strncmp( dirent->d_name, "sd", 2 ) != 0 ) {
            continue;
        }

        // stat device
        sprintf( dname, "%s%s", devdir, dirent->d_name );
        if ( 0 != stat( dname, &st ) ) {
            mcd_log_msg(20022, PLAT_LOG_LEVEL_FATAL,
                         "thread %lu: stat failed for, '%s%s': %s",
                         arg, dname, dirent->d_name,
                         plat_strerror( errno ) );
            plat_abort();
        }

        // found block device when major/minor match
        if ( st.st_rdev == block_dev ) {

            // open block device
            fd = open( dname, O_WRONLY );
            if ( 0 > fd ) {
                mcd_log_msg(20023, PLAT_LOG_LEVEL_FATAL,
                             "thread %lu: couldn't open device '%s': %s",
                             arg, dname, plat_strerror( errno ) );
                plat_abort();
            }

            // return device name
            strcpy( devname, dname );

            mcd_log_msg(20024, PLAT_LOG_LEVEL_INFO,
                         "thread %lu: name=%s, st_dev=0x%x, st_rdev=0x%x",
                         arg, devname, (int)st.st_dev, (int)st.st_rdev );
            break;
        }
    }

    // close device directory
    if ( 0 != closedir( dir ) ) {
        mcd_log_msg(20025, PLAT_LOG_LEVEL_ERROR, "couldn't close '%s': %s",
                     devdir, plat_strerror( errno ) );
    }

    if ( 0 > fd ) {
        mcd_log_msg(20026, PLAT_LOG_LEVEL_FATAL,
                     "thread %lu: couldn't find sync device for 0x%x",
                     arg, (int)block_dev );
        plat_abort();
    }

    return fd;
}


int
find_sync_device_raid( uint64_t arg, int old_fd, char * devname )
{
    int                         fd;
    char                      * devstr;
    char                      * devprefix = "dev-";
    char                      * linkpath = "/sys/block/md0/md/rd";
    char                        linkname[PATH_MAX + 1];
    char                        linktarget[PATH_MAX + 1];
    char                        chkname[PATH_MAX + 1];

    memset( linktarget, 0, sizeof( linktarget ) );
    memset( linkname, 0, sizeof( linkname ) );
    memset( chkname, 0, sizeof( chkname ) );

    sprintf( linkname, "%s%lu", linkpath, arg );

    // read the symlink
    ssize_t len = readlink( linkname, linktarget, sizeof( linktarget ) - 1 );
    if ( 0 > len ) {
        if ( devname[0] != '\0' ) {
            mcd_log_msg(20027, PLAT_LOG_LEVEL_ERROR,
                         "readlink failed for thread %lu, '%s': %s",
                         arg, linkname, plat_strerror( errno ) );
            if ( -1 != old_fd ) {
                mcd_log_msg(20028, PLAT_LOG_LEVEL_INFO,
                             "thread %lu: closing old fd=%d, devname='%s'",
                             arg, old_fd, devname );
                close( old_fd );      // close old device
            }
        }
        devname[0] = '\0';
        return -1;
    }

    // find the device name: dev-sdX
    devstr = strstr( linktarget, devprefix );
    if ( NULL == devstr ) {
        mcd_log_msg(20029, PLAT_LOG_LEVEL_FATAL,
                     "thread %lu, cannot find device: '%s' -> '%s'",
                     arg, linkname, linktarget );
        plat_abort();
    }

    // save old device name (if there is one)
    strncpy( chkname, devname, PATH_MAX );

    // return device name as /dev/sdX
    strcpy( devname, "/dev/" );
    strcat( devname, devstr + strlen( devprefix ) );

    if ( devname[0] == '\0' ||
         0 != strcmp( devname, chkname ) ) {

        mcd_log_msg(20030, PLAT_LOG_LEVEL_INFO,
                     "thread %lu: opening raid block device '%s'",
                     arg, devname );

        // open underlying block device
        fd = open( devname, O_WRONLY );
        if ( 0 > fd ) {
            mcd_log_msg(20031, PLAT_LOG_LEVEL_FATAL,
                         "couldn't open device '%s': %s",
                         devname, plat_strerror( errno ) );
            plat_abort();
        }
    } else {
        fd = old_fd;
    }

    mcd_log_msg(20032, PLAT_LOG_LEVEL_DEBUG,
                 "thread %lu: devname='%s', old fd=%d, new fd=%d",
                 arg, devname, old_fd, fd );

    return fd;
}


void
mcd_aio_sync_device_thread( uint64_t arg )
{
    int                         rc = 0;
    int                         fd = -1;
    dev_t                       dev;
    uint64_t                    mail;
    fthMbox_t                 * reply_mbox;
    struct stat                 st;
    char                        devname[PATH_MAX + 1];
    enum {
        DEV_TYPE_INIT      = 0,
        DEV_TYPE_BLOCK_FS  = 1,
        DEV_TYPE_BLOCK_RAW = 2,
        DEV_TYPE_BLOCK_LVM = 3,
        DEV_TYPE_RAID      = 4
    } dev_type = DEV_TYPE_INIT;

    mcd_log_msg(20033, PLAT_LOG_LEVEL_DEBUG, "ENTERING, thread=%lu", arg );

    memset( devname, 0, sizeof( devname ) );

    // stat file descriptor for corresponding device
    if ( 0 != fstat( Mcd_aio_fds[arg], &st ) ) {
        mcd_log_msg(20034, PLAT_LOG_LEVEL_FATAL, "fstat failed for thread %lu: %s",
                     arg, plat_strerror( errno ) );
        plat_abort();
    }

    // block device under a file system (beta 1 method)
    if ( 8 == major( st.st_dev ) ) {
        mcd_log_msg(20035, PLAT_LOG_LEVEL_INFO,
                     "thread %lu: filesystem on block device, "
                     "st_dev=0x%x, st_rdev=0x%x",
                     arg, (int)st.st_dev, (int)st.st_rdev );
        // find underlying block device using the same major/minor as the
        // base device, which is a partition
        dev = makedev( major( st.st_dev ), minor( st.st_dev & 0xf0 ) );

        // find device to sync
        fd = find_sync_device_block( arg, dev, devname );
        dev_type = DEV_TYPE_BLOCK_FS; // fd never changes;
                                      // doesn't support device re-ordering
    }

    // real device block special file
    else if ( 8 == major( st.st_rdev ) ) {
        mcd_log_msg(20036, PLAT_LOG_LEVEL_INFO,
                     "thread %lu: raw block device st_dev=0x%x, st_rdev=0x%x",
                     arg, (int)st.st_dev, (int)st.st_rdev );
        dev_type = DEV_TYPE_BLOCK_RAW;
    }

    // raid device
    else if ( 259 == major( st.st_rdev ) ) {
        // sync all devices all the time
        Mcd_aio_sync_all = 1;

        mcd_log_msg(20037, PLAT_LOG_LEVEL_INFO,
                     "thread %lu: raid array on block device, "
                     "st_dev=0x%x, st_rdev=0x%x",
                     arg, (int)st.st_dev, (int)st.st_rdev );

        // find block device to sync
        fd = find_sync_device_raid( arg, fd, devname );
        Mcd_aio_raid_device = 1;
        dev_type = DEV_TYPE_RAID;
    }

    // Note: there is another case where the block device under a
    // filesystem is a logical volume, major number 253 (0xfd),
    // where we let it fall through and do fdatasync for sync.
    else {
        dev_type = DEV_TYPE_BLOCK_LVM;
    }

    // loop waiting for signals to sync device
    while ( 1 ) {

        mcd_log_msg(20038, PLAT_LOG_LEVEL_TRACE,
                     "device sync thread %lu, devname=%s waiting",
                     arg, devname );

        mail = fthMboxWait( &Mcd_aio_device_sync_mbox[arg] );
        if ( 0 == mail ) {
            break;
        }
        reply_mbox = (fthMbox_t *)mail;

        switch ( dev_type ) {

        // real block device, or block device under fs
        case DEV_TYPE_BLOCK_FS:
        case DEV_TYPE_BLOCK_RAW:
            // for raw device use fd for this thread
            if ( dev_type == DEV_TYPE_BLOCK_RAW ) {
                fd = Mcd_aio_fds[arg];
            }
            // issue sync command to device
            rc = do_drive_sync( fd );
            if ( 0 != rc ) {
                mcd_log_msg(20039, PLAT_LOG_LEVEL_ERROR,
                             "device sync failed, thread %lu, devname=%s, "
                             "rc=%d, errno=%d",
                             arg, devname, rc, errno );
            }
            break;

        // block device in raid array
        case DEV_TYPE_RAID:

            // find block device to sync
            fd = find_sync_device_raid( arg, fd, devname );
            if ( 0 > fd ) {
                // dead device, do nothing
                rc = 0;
                mcd_log_msg(20040, PLAT_LOG_LEVEL_DEBUG,
                             "thread %lu: device sync on dead device", arg );
            } else {
                // issue sync command to underlying device
                rc = do_drive_sync( fd );
                if ( 0 != rc ) {
                    mcd_log_msg(20039, PLAT_LOG_LEVEL_ERROR,
                                 "device sync failed, thread %lu, devname=%s, "
                                 "rc=%d, errno=%d",
                                 arg, devname, rc, errno );
                } else {
                    mcd_log_msg(20041, PLAT_LOG_LEVEL_DEBUG,
                                 "thread %lu: device sync fd=%d, devname=%s, "
                                 "rc=%d", arg, fd, devname, rc );
                }
            }
            break;

        // simulated block device using a file in a fs on a logical volume
        case DEV_TYPE_BLOCK_LVM:
            // issue fdatasync command to file
            rc = fdatasync( Mcd_aio_fds[arg] );
            if ( 0 != rc ) {
                mcd_log_msg(20042, PLAT_LOG_LEVEL_ERROR,
                             "sync failed, thread %lu, devname=%s: %s",
                             arg, devname, plat_strerror( errno ) );
            }
            break;

        // unknown or uninitialized
        default:
            mcd_dbg_msg( PLAT_LOG_LEVEL_FATAL,
                         "Unknown device type %d", dev_type );
            plat_abort();
        }

        mcd_log_msg(20043, PLAT_LOG_LEVEL_DEBUG,
                     "device sync thread %lu, devname=%s sending reply, rc=%d",
                     arg, devname, rc );
        fthMboxPost( reply_mbox, (rc != 0) );

    }

    if ( fd >= 0 ) {
        close( fd );
    }

    mcd_log_msg(20044, PLAT_LOG_LEVEL_DEBUG,
                 "device sync thread %lu, devname=%s halting", arg, devname );

    return;
}


int
mcd_aio_sync_devices( void )
{
    int                         result = 0;
    fthMbox_t                   reply_mbox;

    mcd_log_msg(20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    if ( 1 == Mcd_aio_sync_enabled ) {

        fthMboxInit( &reply_mbox );

        // signal all device sync threads
        for ( int i = 0; i < Mcd_aio_num_files; i++ ) {
            fthMboxPost( &Mcd_aio_device_sync_mbox[i],
                         (uint64_t)(&reply_mbox) );
        }

        // wait for sync complete from all threads
        for ( int i = 0; i < Mcd_aio_num_files; i++ ) {
            result += fthMboxWait( &reply_mbox );
        }

        if ( result == 0 ) {
            mcd_log_msg(20045, PLAT_LOG_LEVEL_DEBUG, "all devices synced" );
        } else {
            mcd_log_msg(20046, PLAT_LOG_LEVEL_ERROR,
                         "an error occurred syncing %d of %d devices",
                         result, Mcd_aio_num_files );
        }
    } else {
        mcd_log_msg(20047, PLAT_LOG_LEVEL_DEBUG, "sync disabled" );
    }

    return (result != 0);
}


int
mcd_aio_sync_device_offset( uint64_t sync_offset, int sync_bytes )
{
    int                         i;
    int                         num = 0;
    int                         result = 0;
    uint64_t                    offset = sync_offset;
    uint64_t                    device;
    fthMbox_t                   reply_mbox;

    mcd_log_msg(20000, PLAT_LOG_LEVEL_DEBUG, "ENTERING" );

    if ( 1 == Mcd_aio_sync_all ) {
        result = mcd_aio_sync_devices();
    }

    else if ( 1 == Mcd_aio_sync_enabled ) {

        fthMboxInit( &reply_mbox );

        // signal threads for all devices in the range
        for ( i = 0; i < Mcd_aio_num_files; i++ ) {

            // find device
            device = ( offset / Mcd_aio_strip_size ) % Mcd_aio_num_files;

            // signal device sync thread
            fthMboxPost( &Mcd_aio_device_sync_mbox[device],
                         (uint64_t)(&reply_mbox) );

            num++;
            offset += Mcd_aio_strip_size;
            if ( offset > sync_offset + sync_bytes ) {
                break;
            }
        }

        // wait for sync complete from thread(s)
        for ( i = 0; i < num; i++ ) {
            result += fthMboxWait( &reply_mbox );
        }

        if ( result == 0 ) {
            mcd_log_msg(20048, PLAT_LOG_LEVEL_DEBUG, "%d device(s) synced", num );
        } else {
            mcd_log_msg(20049, PLAT_LOG_LEVEL_ERROR,
                         "an error occurred syncing %d of %d device(s)",
                         result, num );
        }

    } else {
        mcd_log_msg(20047, PLAT_LOG_LEVEL_DEBUG, "sync disabled" );
    }

    return (result != 0);
}


int
mcd_aio_fstat( int fd, struct stat * st )
{
    FILE * fp;

#define _FILE_OFFSET_BITS 64

    // stat the file
    if ( 0 != fstat( fd, st ) ) {
        return -1;
    }

    // if size is not filled in, try alternate method
    if ( st->st_size == 0 ) {
        fp = fdopen( fd, "r+" );
        if ( fp == NULL ) {
            return -1;
        }
        // seek to the end, and get the file pointer
        fseeko( fp, (off_t)0, SEEK_END );
        st->st_size = ftello( fp );
        // fp intentionally not closed (closes fd, too!)
    }

    return 0;
}

static inline void mcd_aio_poll( void )
{
    static uint32_t             count = 0;
    int                         num_events;
    uint32_t                    pending;
    struct io_event             io_events[32];          /* FIXME */
    struct io_event           * event;
    mcd_aio_cb_t              * acb;
    aio_state_t               * aio_state;
    int                         error;

    num_events = Mcd_aio_ctxt.use_paio ?
        paio_getevents( Mcd_aio_ctxt.paio.ctxt, 1, 32, io_events, NULL) :
        io_getevents( Mcd_aio_ctxt.io_ctxt, 1, 32, io_events, NULL );

    if ( 0 > num_events ) {
        if ( 0 == (count++ % 1048576 ) ) {
            mcd_log_msg( 20133, PLAT_LOG_LEVEL_ERROR, "failed to get events" );
        }
    }
    if ( 0 < num_events ) {
        mcd_log_msg( 20134, PLAT_LOG_LEVEL_DEBUG,
                     "got io events, num_events=%d", num_events );
        (void) __sync_fetch_and_sub( &Mcd_num_pending_ios, num_events );
    }

    for ( int i = 0; i < num_events; i++ ) {

        event = &io_events[i];
        acb = (mcd_aio_cb_t *)event->obj;
        aio_state = acb->ctxt;
        error = event->res2 != 0 || event->res != acb->bytes;
        acb->error = error;

        if ( error ) {
            aio_state->aio_error = error;
            if ( SDF_TRUE != flash_settings.is_node_independent &&
                 flash_settings.max_aio_errors < (++Mcd_fth_aio_errors) ) {
                mcd_log_msg( 20135, PLAT_LOG_LEVEL_FATAL,
                             "too many aio errors (> %d), time to abort",
                             flash_settings.max_aio_errors );
                plat_abort();
            }
        }

        pending = __sync_sub_and_fetch( &aio_state->aio_pending, 1 );
        if ( 0 == pending ) {
            (void) __sync_fetch_and_sub( &Mcd_fth_waiting_io, 1 );
            fthMboxPost( aio_state->aio_mbox, aio_state->aio_error );
        }
    }
}

static void mcd_aio_poller( uint64_t arg )
{
    while ( 1 ) {
        mcd_aio_poll();
        // fthYield( -1 );
    }
}

void mcd_aio_free_state(aio_state_t *aio_state)
{
    if ( aio_state->aio_mbox ) {
        fthMboxTerm( aio_state->aio_mbox );
        plat_free( aio_state->aio_mbox );
    }
    memset( (void *)aio_state, 0, sizeof(aio_state_t) );
}

aio_state_t *mcd_aio_init_state()
{
    fthMbox_t * aio_mbox = NULL;
    aio_state_t  *aio_state;

    mcd_log_msg(20000, PLAT_LOG_LEVEL_INFO, "ENTERING");

    aio_state = (aio_state_t *) plat_alloc(sizeof(aio_state_t));
    if (aio_state == NULL) {
	mcd_log_msg( 20066, PLAT_LOG_LEVEL_ERROR, "plat_alloc failed" );
	plat_assert_always( 0 == 1 );
    }

    aio_mbox = (fthMbox_t *)plat_alloc( sizeof(fthMbox_t) );
    if ( NULL == aio_mbox ) {
	mcd_log_msg( 20066, PLAT_LOG_LEVEL_ERROR, "plat_alloc failed" );
	plat_assert_always( 0 == 1 );
    }
    aio_state->aio_mbox = aio_mbox;
    fthMboxInit( aio_state->aio_mbox );
    aio_state->aio_ready   = 0;
    aio_state->aio_pending = 0;
    aio_state->aio_error   = 0;
    aio_state->aio_self    = fthSelf();
    return(aio_state);
}

int mcd_aio_init( void * state, char * dname )
{
    int                         rc = -1;
    int                         fbase = 0;
    int                         open_flags;
    char                      * first;
    char                        fname[PATH_MAX + 1];
    struct stat                 st;
    struct ssdaio_state       * aio_state = (struct ssdaio_state *)state;
    bool                        paio_enabled;

    mcd_log_msg(20050, PLAT_LOG_LEVEL_INFO, "ENTERING, dname=%s", dname );

    /*
     * aio poller fthread
     */
    fthResume( fthSpawn( &mcd_aio_poller, 81920),
               (uint64_t) 0);
    mcd_log_msg( 20144, PLAT_LOG_LEVEL_INFO,
                 "aio init helper fthread spawned" );

    /*
     * get total flash size from property file
     */
    Mcd_aio_total_size = getProperty_Int( "AIO_FLASH_SIZE_TOTAL", 0 );
    if ( 0 != flash_settings.aio_total_size ) {
        Mcd_aio_total_size = flash_settings.aio_total_size;
    }
    Mcd_aio_total_size *= ( 1024 * 1024 * 1024 );
    if ( 0 == Mcd_aio_total_size ) {
        mcd_log_msg(20051, PLAT_LOG_LEVEL_FATAL,
                     "Please specify total flash size with the property file"
                     " or --aio_flash_size" );
        plat_abort();
    }
    Mcd_aio_real_size  = Mcd_aio_total_size;

    /*
     * get sync flash_settings from the property file
     */
    Mcd_aio_sync_enabled = getProperty_Int( "AIO_SYNC_ENABLED", 1 );
    if ( -1 != flash_settings.aio_sync_enabled ) {
        Mcd_aio_sync_enabled = flash_settings.aio_sync_enabled;
    }

#ifdef MEMCACHED_DEBUG
    paio_enabled = (flash_settings.aio_wc || flash_settings.aio_error_injection) ? 
        true : false;
#else
    paio_enabled = flash_settings.aio_wc ? true : false;
#endif

    /*
     * the queue len should not be smaller than the highest number
     * returned by get_aio_context
     */
    if ((!paio_enabled &&
         0 != (rc=io_queue_init(flash_settings.aio_queue_len, &Mcd_aio_ctxt.io_ctxt))) ||
        (paio_enabled && SDF_SUCCESS != mcd_aio_paio_init())) {
        mcd_log_msg(30660, PLAT_LOG_LEVEL_FATAL,
                     "failed to initialize the aio context (rc=%d '%s')", rc, plat_strerror(-rc));
    /*
     * XXX: drew 2010-03-23 This is bad because it will create
     * a core dump.  Errors should propagate to the caller which
     * can add information and _exit(1) or whatever.
     *
     * I have no clue that whatever is calling this via a
     * function pointer is going to do anything sensible with
     * a non-zero return though.
     */
        plat_abort();
    }
    mcd_log_msg(20053, PLAT_LOG_LEVEL_INFO, "aio context initialized" );

    /*
     * open the aio backing files
     */
    if ( 0 != flash_settings.aio_base[0] ) {
        first = strchr(flash_settings.aio_base, '%');
        if (!first) {
            mcd_log_msg(10011, PLAT_LOG_LEVEL_FATAL,
                         "aio_base lacks %%d, %%x, or %%o" );
            plat_abort();
        } else if ( 'd' != first[1] && 'x' != first[1] && 'o' != first[1]) {
            mcd_log_msg(10018, PLAT_LOG_LEVEL_FATAL,
                         "aio_base has %%%c not  %%d, %%x, or %%o",
                         *first);
            plat_abort();
        } else if ( strchr( first + 1, '%' ) ) {
            mcd_log_msg(10012, PLAT_LOG_LEVEL_FATAL,
                         "aio_base has too many %% entries" );
            plat_abort();
        } else {
            strncpy( Mcd_aio_base, flash_settings.aio_base,
                     sizeof (Mcd_aio_base) - 1 );
        }
    }

    if ( MCD_AIO_MAX_NFILES < flash_settings.aio_num_files ) {
        mcd_log_msg(20054, PLAT_LOG_LEVEL_FATAL,
                     "too many aio files, max is %d", MCD_AIO_MAX_NFILES );
        plat_abort();
    }
    if ( 0 != flash_settings.aio_num_files ) {
        Mcd_aio_num_files = flash_settings.aio_num_files;
    }

    if( Mcd_aio_num_files != 1 ) {
        mcd_log_msg(150090,PLAT_LOG_LEVEL_FATAL,
            "Incorrect value(%d) for Mcd_aio_num_files. It must be set to 1",Mcd_aio_num_files);
        plat_abort();
    }

    if ( MCD_AIO_MAX_NSUBFILES < flash_settings.aio_sub_files ) {
        mcd_log_msg(20055, PLAT_LOG_LEVEL_FATAL,
                     "too many sub files, max is %d", MCD_AIO_MAX_NSUBFILES );
        plat_abort();
    }
    Mcd_aio_sub_files = flash_settings.aio_sub_files;

    if ( flash_settings.aio_first_file >= 0 ) {
        fbase = flash_settings.aio_first_file;
    } else if ( 1 < Mcd_aio_num_files ) {
        fbase = 1;
    }

    open_flags = O_RDWR;
    if ( 0 == flash_settings.no_direct_io ) {
        open_flags |= O_DIRECT;
    }
    if ( 0 != flash_settings.aio_create ) {
        open_flags |= O_CREAT;
    }

    if (getProperty_Int("FDF_O_SYNC", 0)) {
        open_flags |= O_SYNC;
        mcd_log_msg(180005, PLAT_LOG_LEVEL_INFO,
                    "FDF_O_SYNC is set");
    }

    for ( int i = 0; i < Mcd_aio_num_files; i++ ) {

        if ( 0 == Mcd_aio_sub_files ) {

            sprintf( fname, Mcd_aio_base, fbase + i );

			if(fdf_instance_id)
			{
				char temp[PATH_MAX + 1];
				sprintf(temp, "%s.%d", fname, fdf_instance_id);
				Mcd_aio_fds[i] = open( temp, open_flags, 00600 );
				/* Remove from FS name space immediately, e.g. limit lifetime by this process */
				if(getProperty_Int("FDF_TEST_MODE", 0))
					unlink(temp);
			}
			else
				Mcd_aio_fds[i] = open( fname, open_flags, 00600 );

            if ( 0 > Mcd_aio_fds[i] ) {
                mcd_log_msg(20056, PLAT_LOG_LEVEL_FATAL,
                             "failed to open file %s: %s",
                             fname, plat_strerror( errno ) );
                plat_abort();
            }
            if ( 0 != mcd_aio_fstat( Mcd_aio_fds[i], &st ) ) {
                mcd_log_msg(20057, PLAT_LOG_LEVEL_FATAL,
                            "fstat failed for file %s: %s",
                            fname, plat_strerror( errno ) );
                plat_abort();
            }

            if ( 0 == flash_settings.bypass_aio_check && 0 == flash_settings.aio_create ) {
                if ( Mcd_aio_total_size / Mcd_aio_num_files > st.st_size ) {
                    mcd_log_msg(20058, PLAT_LOG_LEVEL_FATAL,
                                 "file %s is too small", fname );
                    plat_abort();
                }
            }
            else if ( flash_settings.aio_create && 0 == st.st_size ) {
                if ( ftruncate( Mcd_aio_fds[i],
                                Mcd_aio_total_size / Mcd_aio_num_files ) ) {
                    mcd_log_msg(21863, PLAT_LOG_LEVEL_FATAL,
                                "cannot truncate %s: %s", fname,
                                plat_strerror( errno ) );
                    plat_abort();
                }
            }

            mcd_log_msg(20059, PLAT_LOG_LEVEL_INFO,
                         "aio file %s opened successfully", fname );

            if ( 1 == Mcd_aio_sync_enabled ) {
                fthMboxInit( &Mcd_aio_device_sync_mbox[i] );
                fthResume( fthSpawn( &mcd_aio_sync_device_thread, 40960 ), i );
            }
        }
        else {
            for ( int j = 0; j < Mcd_aio_sub_files; j++ ) {

                sprintf( fname, Mcd_aio_base, fbase + i );

                if ( strlen(fname) + 3 > sizeof(fname) ) {
                    mcd_log_msg(20060, PLAT_LOG_LEVEL_FATAL, "aio path too long" );
                    plat_abort();
                }
                sprintf( fname + strlen(fname), "%x", j );

                Mcd_aio_sub_fds[i][j] = open( fname, open_flags, 00600 );
                if ( 0 > Mcd_aio_sub_fds[i][j] ) {
                    mcd_log_msg(20056, PLAT_LOG_LEVEL_FATAL,
                                 "failed to open file %s: %s",
                                 fname, plat_strerror( errno ) );
                    plat_abort();
                }

                if ( 0 == flash_settings.bypass_aio_check &&
                     0 == flash_settings.aio_create ) {
                    if ( 0 != fstat( Mcd_aio_sub_fds[i][j], &st ) ) {
                        mcd_log_msg(20057, PLAT_LOG_LEVEL_FATAL,
                                     "fstat failed for file %s: %s",
                                     fname, plat_strerror( errno ) );
                        plat_abort();
                    }
                    if ( st.st_size * Mcd_aio_num_files * Mcd_aio_sub_files <
                         Mcd_aio_total_size ) {
                        mcd_log_msg(20058, PLAT_LOG_LEVEL_FATAL,
                                     "file %s is too small", fname );
                        plat_abort();
                    }
                }
                mcd_log_msg(20059, PLAT_LOG_LEVEL_INFO,
                             "aio file %s opened successfully", fname );

                /*
                 * FIXME
                 */
                if ( 1 == Mcd_aio_sync_enabled ) {
                    plat_abort();
                }
            }
        }
    }

    Mcd_aio_strip_size = Mcd_aio_total_size / Mcd_aio_num_files;

    /*
     * set up aio_state
     */
    if ( NULL != aio_state ) {
        aio_state->size = Mcd_aio_total_size;
    }

    /*
     * ensure sync threads are up and running
     */
    if ( 0 != mcd_aio_sync_devices() ) {
        mcd_log_msg(20061, PLAT_LOG_LEVEL_FATAL,
                     "cannot sync devices, try running with --no_sync" );
        plat_abort();
    }

    return 0;   /* SUCCESS */
}

static SDF_status_t
mcd_aio_paio_init()
{
    SDF_status_t ret;
    int status;

#ifdef MEMCACHED_DEBUG
    plat_assert(flash_settings.aio_wc || flash_settings.aio_error_injection);
#else /* ndef MEMCACHED_DEBUG */
    plat_assert(flash_settings.aio_wc);
#endif /* else def MEMCACHED_DEBUG */

    Mcd_aio_ctxt.paio.wrapped_api =
        paio_libaio_create(&flash_settings.aio_libaio_config);
    ret = Mcd_aio_ctxt.paio.wrapped_api ? SDF_SUCCESS : SDF_OUT_OF_MEM;

#ifdef MEMCACHED_DEBUG
    if (ret == SDF_SUCCESS && flash_settings.aio_error_injection) {
        Mcd_aio_ctxt.paio.error_api =
            paio_error_bdb_create(&Mcd_aio_ctxt.paio.error_control,
                                  &flash_settings.aio_error_bdb_config,
                                  Mcd_aio_ctxt.paio.wrapped_api);
        if (!Mcd_aio_ctxt.paio.error_api) {
            ret = SDF_OUT_OF_MEM;
        }
    }

    if (ret != SDF_SUCCESS) {
    } else if (flash_settings.aio_wc) {
        Mcd_aio_ctxt.paio.api =
            paio_wc_create(&flash_settings.aio_wc_config,
                           flash_settings.aio_error_injection ? 
                           Mcd_aio_ctxt.paio.error_api :
                           Mcd_aio_ctxt.paio.wrapped_api);
    } else {
        plat_assert(flash_settings.aio_error_injection);
        Mcd_aio_ctxt.paio.api = Mcd_aio_ctxt.paio.error_api;
    }
#else /* def MEMCACHED_DEBUG */
    if (ret == SDF_SUCCESS) {
        Mcd_aio_ctxt.paio.api = paio_wc_create(&flash_settings.aio_wc_config,
                                               Mcd_aio_ctxt.paio.wrapped_api);
        ret = Mcd_aio_ctxt.paio.api ? SDF_SUCCESS : SDF_OUT_OF_MEM;
    }
#endif /* else def MEMCACHED_DEBUG */

    if (ret == SDF_SUCCESS) {
        status = paio_setup(Mcd_aio_ctxt.paio.api, flash_settings.aio_queue_len,
                            &Mcd_aio_ctxt.paio.ctxt);
        if (status == -1) {
            mcd_log_msg(21834, PLAT_LOG_LEVEL_FATAL,
                        "paio_setup failed: %s", plat_strerror(errno));
            ret = SDF_FAILURE;
        }
    }

    if (ret == SDF_SUCCESS) {
        Mcd_aio_ctxt.use_paio = 1;
    }

    return (ret);
}

void
mcd_aio_set_fds( int order[] )
{
    int                 i;
    int                 fds[MCD_AIO_MAX_NFILES];

    // save current list of file descriptors
    for ( i = 0; i < Mcd_aio_num_files; i++ ) {
        fds[i] = Mcd_aio_fds[i];
    }

    // reinstall file descriptors in specified order
    for ( i = 0; i < Mcd_aio_num_files; i++ ) {
        Mcd_aio_fds[i] = fds[ order[i] ];
    }
}


/**
 * @brief Return open file descriptor for given byte range
 *
 * @param offset <IN> byte offset
 * @param bytes <IN> byte count
 * @return fd, or -1 if byte range spans devices
 */
int
mcd_aio_get_fd( uint64_t offset, uint64_t bytes )
{
    int                 device;

    // find device for range
    device = ( offset / Mcd_aio_strip_size ) % Mcd_aio_num_files;

    // range must not span devices
    if ( device !=
         ((offset + bytes - 1) / Mcd_aio_strip_size) % Mcd_aio_num_files ) {
        return -1;
    }

    return Mcd_aio_fds[ device ];
}

   // from mcd_osd.c
   // xxxzzz FIXME this linkage
struct osd_state;
extern struct osd_state *mcd_fth_init_aio_ctxt( int category );
extern int mcd_fth_free_aio_ctxt( struct osd_state * osd_state, int category );

void
mcd_aio_register_ops( void )
{
    Ssd_aio_ops.aio_init                = mcd_aio_init;
    Ssd_aio_ops.aio_init_context        = mcd_fth_init_aio_ctxt;
    Ssd_aio_ops.aio_free_context        = mcd_fth_free_aio_ctxt;
    Ssd_aio_ops.aio_blk_read            = mcd_fth_aio_blk_read;
    Ssd_aio_ops.aio_blk_write           = mcd_fth_aio_blk_write;

    //mcd_log_msg(20062, PLAT_LOG_LEVEL_INFO, "mcd_aio ops registered" );

    return;
}
