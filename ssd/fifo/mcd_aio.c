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

#include <fcntl.h>
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
#  define MCD_AIO_LOG_LVL_TRACE PLAT_LOG_LEVEL_TRACE
#else
#  define MCD_AIO_LOG_LVL_INFO  PLAT_LOG_LEVEL_INFO
#  define MCD_AIO_LOG_LVL_DEBUG PLAT_LOG_LEVEL_DEBUG
#  define MCD_AIO_LOG_LVL_DIAG  PLAT_LOG_LEVEL_DIAGNOSTIC
#  define MCD_AIO_LOG_LVL_TRACE PLAT_LOG_LEVEL_TRACE
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
 * local globals
 */
static int              Mcd_aio_fds[MCD_AIO_MAX_NFILES];
static int              Mcd_aio_sub_fds[MCD_AIO_MAX_NFILES][MCD_AIO_MAX_NSUBFILES];

static bool		failenabled;		/* write fault injector enabled */

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

static void		init_write_fault_injector( ),
			write_fault_injector( int, char *, size_t, off_t);
static int		write_fault_sync( int, ulong);


int
mcd_aio_blk_read(struct osd_state *context, char * buf, uint64_t offset, int nbytes )
{
    int                         aio_fd = Mcd_aio_fds[0];
    int                         submitted;
    int                         strip;
    int                         sub_file;
    uint64_t                    aio_nbytes = nbytes;
    uint64_t                    aio_offset;
    uint64_t                    cur_offset;
    uint32_t                    pending;

    mcd_log_msg(20010, PLAT_LOG_LEVEL_TRACE, "ENTERING, offset=%lu nbytes=%d",
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
                mcd_log_msg(20013, PLAT_LOG_LEVEL_TRACE,
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

        submitted += aio_nbytes;

        if ( Mcd_aio_num_files < ++pending ) {
            mcd_log_msg(20014, PLAT_LOG_LEVEL_ERROR,
                         "request size beyond strip limit, off=%lu nbytes=%d",
                         offset, nbytes );
            plat_abort();
        }

        mcd_log_msg(20015, MCD_AIO_LOG_LVL_TRACE,
                     "read iocb ready: fd=%d nbytes=%lu offset=%lu",
                     aio_fd, aio_nbytes, aio_offset );

        if ( nbytes == submitted ) {
            break;
        }
    } while ( 1 );

    return FLASH_EOK;   /* SUCCESS */
}


int
mcd_aio_blk_write_low(struct osd_state *context, char * buf, uint64_t offset, int nbytes, char sync )
{
    int                         aio_fd = Mcd_aio_fds[0];
    int                         submitted;
    int                         strip;
    int                         sub_file;
    uint64_t                    aio_nbytes = nbytes;
    uint64_t                    aio_offset;
    uint64_t                    cur_offset;
    uint32_t                    pending = 0;

    mcd_log_msg(20010, PLAT_LOG_LEVEL_TRACE, "ENTERING, offset=%lu nbytes=%d",
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
                mcd_log_msg(20013, PLAT_LOG_LEVEL_TRACE,
                             "sub_fds[%d][%x]=%d aio_offset=%lu",
                             strip, sub_file, aio_fd, aio_offset );
            }
        }

#if 1//Rico - official fault injection
	if (failenabled)
		write_fault_injector( aio_fd, buf+submitted, aio_nbytes, aio_offset);
	else
#endif
	if (pwrite(aio_fd, buf+submitted, aio_nbytes, aio_offset) != aio_nbytes) {
            mcd_log_msg(180002, PLAT_LOG_LEVEL_ERROR, "pwrite failed!(%s)", plat_strerror(errno));
	    plat_exit(1);
	}

        // io_prep_pwrite( &acb->iocb, aio_fd, buf + submitted, aio_nbytes, aio_offset );

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

#if 1//Rico - official fault injection
	if (sync)
		write_fault_sync( aio_fd, -1);
#else
	if(sync)
		fdatasync(aio_fd);
#endif

    return FLASH_EOK;   /* SUCCESS */
}

int
mcd_aio_blk_write(struct osd_state *context, char * buf, uint64_t offset, int nbytes )
{
	return mcd_aio_blk_write_low(context, buf, offset, nbytes, 1);
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

int mcd_aio_init( void * state, char * dname )
{
    //int                         fbase = 0;
    int                         open_flags;
    //char                      * first;
    char                        fname[PATH_MAX + 1];
    struct stat                 st;

    mcd_log_msg(20050, PLAT_LOG_LEVEL_TRACE, "ENTERING, dname=%s", dname );

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

    init_write_fault_injector( );

    /*
     * open the aio backing files
     * only one flash file supported.
     */
    if ( 0 != flash_settings.aio_base[0] ) {
        strncpy( Mcd_aio_base, flash_settings.aio_base, sizeof (Mcd_aio_base) - 1 );
    }

    if ( 0 != flash_settings.aio_num_files ) {
        Mcd_aio_num_files = flash_settings.aio_num_files;
    }

    if( Mcd_aio_num_files != 1 ) {
        mcd_log_msg(150090,PLAT_LOG_LEVEL_FATAL,
            "Incorrect value(%d) for Mcd_aio_num_files. It must be set to 1",Mcd_aio_num_files);
        plat_abort();
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
        mcd_log_msg(180020, PLAT_LOG_LEVEL_TRACE,
                    "FDF_O_SYNC is set");
    }
    int i = 0;
    strncpy( fname, Mcd_aio_base, sizeof (Mcd_aio_base));

    if(fdf_instance_id) {
        char temp[PATH_MAX + 1];
        sprintf(temp, "%s.%d", fname, fdf_instance_id);
        Mcd_aio_fds[i] = open( temp, open_flags, 00600 );
        /* Remove from FS name space immediately, e.g. limit lifetime by this process */
        if(getProperty_Int("FDF_TEST_MODE", 0))
            unlink(temp);
        } else {
            Mcd_aio_fds[i] = open( fname, open_flags, 00600 );
        }

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

            mcd_log_msg(80041, PLAT_LOG_LEVEL_INFO,
                         "Flash file %s opened successfully", fname );

    Mcd_aio_strip_size = Mcd_aio_total_size / Mcd_aio_num_files;

    return 0;   /* SUCCESS */
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
extern struct osd_state *mcd_init_aio_ctxt( int category );
extern int mcd_free_aio_ctxt( struct osd_state * osd_state, int category );

void
mcd_aio_register_ops( void )
{
    Ssd_aio_ops.aio_init                = mcd_aio_init;
    Ssd_aio_ops.aio_init_context        = mcd_init_aio_ctxt;
    Ssd_aio_ops.aio_free_context        = mcd_free_aio_ctxt;
    Ssd_aio_ops.aio_blk_read            = mcd_aio_blk_read;
    Ssd_aio_ops.aio_blk_write           = mcd_aio_blk_write;

    //mcd_log_msg(20062, PLAT_LOG_LEVEL_TRACE, "mcd_aio ops registered" );

    return;
}


/*
 * section for write fault injection
 * =================================
 *
 * Abort FDF with selective device corruption via property.
 */

#include	<ctype.h>
#include	"utils/rico.h"


static void	dumpsignature( ulong),
		flushsignature( ),
		_flushsignature( );


static ulong		failstart,
			failcount;
static ushort		failfraction,
			failmode;
static plat_mutex_t	siglock		= PLAT_MUTEX_INITIALIZER;
static ulong		sigcurrent;
static uint		sigcount,
			writecounter;


/*
 * initialize the write fault injector
 *
 * Property AIO_WRITE_FAULT_INJECTOR enables faulty writes, and integer
 * parameters are loaded from it according to the format
 *
 *	start/count/fraction/mode/seed
 *
 * Faulty writes can also be enabled by an environmental variable of the
 * same name, and its parameters take priority.  Parameter 'start' is the
 * failure point in ops, 'count' is the # of ops under the failure regime,
 * fraction is the percentage of ops to be corrupted, mode is the type
 * of corruption to be visited on the data block, and 'seed' sets the
 * random number generator.  If seed is omitted, the process ID is used.
 * Settings are reported on FDF startup (PLAT_LOG_LEVEL_INFO).  With the
 * same seed and a single-threaded app, reproducible crashes are possible.
 *
 * Mode settings:
 *
 *	0	write normally (no corruption)
 *	1	write nothing (enterprise-grade SSD crash bahavior)
 *	2	clear all bits
 *	3	set all bits
 *	4	complement all bits
 *	5	increment a random byte
 *
 * Example:
 * 	AIO_WRITE_FAULT_INJECTOR = 70000/10000/10/5/0
 * Meaning:
 * 	Starting at FDF write operation 70000, increment a random
 * 	byte in 10% of the blocks written, and attempt reproducibility.
 * 	Crash just before write operation 80000.
 */
static void
init_write_fault_injector( )
{
	const char	*s,
			*s2;
	uint		seed;

	s = getProperty_String( "AIO_WRITE_FAULT_INJECTOR", "");
	if (s2 = getenv( "AIO_WRITE_FAULT_INJECTOR"))
		s = s2;
	switch (sscanf( s, "%lu/%lu/%hu/%hu/%u", &failstart, &failcount, &failfraction, &failmode, &seed)) {
	case 4:
		seed = getpid( );
	case 5:
		srandom( seed);
		mcd_log_msg( 170035, PLAT_LOG_LEVEL_INFO, "enabled with parameters %lu/%lu/%u/%u/%u", failstart, failcount, failfraction, failmode, seed);
		failenabled = TRUE;
	}
}


/*
 * operate the write fault injector
 *
 * Corrupt traffic outbound to flash according to the parameters above.
 * A brief diagnostic is printed to stderr, giving the ASCII signature of
 * the block or, alternatively, the first word in hex.  When the op stream
 * reaches failstart+failcount, abort FDF.
 */
static void
write_fault_injector( int fd, char *buf, size_t nbyte, off_t offset)
{

	char *p = buf;
	ulong z = __sync_fetch_and_add( &writecounter, 1);
	unless (z < failstart) {
		if (z == failstart+failcount) {
			flushsignature( );
			mcd_log_msg( 170030, PLAT_LOG_LEVEL_FATAL, "simulated hardware crash triggered");
			plat_abort( );
		}
		static plat_mutex_t l = PLAT_MUTEX_INITIALIZER;
		plat_mutex_lock( &l);
		uint r0 = random( );
		uint r1 = random( );
		plat_mutex_unlock( &l);
		if (r0 < RAND_MAX/100*failfraction) {
			dumpsignature( *(uint *)buf);
			switch (failmode) {
			/*
			 * write normally (no corruption)
			 */
			case 0:
				break;
			/*
			 * write nothing (enterprise-grade SSD crash bahavior)
			 */
			case 1:
				return;
			/*
			 * clear all bits
			 */
			case 2:
				p = malloc( nbyte);
				memset( p, 0, nbyte);
				break;
			/*
			 * set all bits
			 */
			case 3:
				p = malloc( nbyte);
				memset( p, ~0, nbyte);
				break;
			/*
			 * complement all bits
			 */
			case 4:
				p = malloc( nbyte);
				for (uint i=0; i<nbyte/sizeof( ulong); ++i)
					((ulong *)p)[i] = ~ ((ulong *)buf)[i];
				break;
			/*
			 * increment a random byte
			 */
			case 5:
				p = malloc( nbyte);
				memcpy( p, buf, nbyte);
				++p[r1%nbyte];
			}
		}
	}
	ssize_t i = pwrite( fd, p, nbyte, offset);
	unless (i == nbyte) {
		if (i < 0)
			mcd_log_msg( 180002, PLAT_LOG_LEVEL_ERROR, "pwrite failed!(%s)", plat_strerror( errno));
		else
			mcd_log_msg( 180002, PLAT_LOG_LEVEL_ERROR, "pwrite failed!(%s)", "short write");
		plat_exit( 1);
	}
	unless (p == buf)
		free( p);
}


/*
 * sync flash device
 *
 * Call fdatasync() on the given descriptor and, if the write stream
 * lies within the fault-injection range, print an indicator to stderr.
 * The indicators are (####) and ($$$$) for 'sig' -1 and -2, respectively.
 * While the injector can corrupt any write range of the FDF run, recovery
 * from a hardware crash is only defined for corruption after the last device
 * sync.  Therefore, these indicators should not be seen in the diagnostic
 * stream during h/w crash simulations.  Adjust AIO_WRITE_FAULT_INJECTOR
 * accordingly.
 */
static int
write_fault_sync( int fd, ulong sig)
{

	int i = fdatasync( fd);
	if (failenabled) {
		ulong z = __sync_fetch_and_add( &writecounter, 0);
		if (failstart<=z && z<failstart+failcount)
			dumpsignature( sig);
	}
	return (i);
}


/*
 * display block type
 *
 * Display the "eye catcher" of the block, if present, otherwise the value
 * in hex of the first 32 bits.  Count a repeating type, typically META for
 * slab.  Ensure the last repeat is flushed on FDF exit or injector abort.
 */
static void
dumpsignature( ulong sig)
{
	static bool	initialized;

	plat_mutex_lock( &siglock);
	unless (initialized) {
		initialized = TRUE;
		atexit( flushsignature);
	}
	unless (sig == sigcurrent) {
		_flushsignature( );
		sigcurrent = sig;
		sigcount = 0;
	}
	++sigcount;
	plat_mutex_unlock( &siglock);
}


static void
flushsignature( )
{

	plat_mutex_lock( &siglock);
	_flushsignature( );
	fprintf( stderr, "\n");
	plat_mutex_unlock( &siglock);
}


static void
_flushsignature( )
{

	if (sigcount) {
		switch (sigcurrent) {
		default:;
			uint i = 0;
			loop {
				if (i == sizeof( uint)) {
					if (sigcount == 1)
						fprintf( stderr, "(%.4s)", (char *)&sigcurrent);
					else
						fprintf( stderr, "(%u\327%.4s)", sigcount, (char *)&sigcurrent);
					break;
				}
				unless (isalnum( ((uchar *)&sigcurrent)[i])) {
					if (sigcount == 1)
						fprintf( stderr, "(%04lX)", sigcurrent);
					else
						fprintf( stderr, "(%u\327%04lX)", sigcount, sigcurrent);
					break;
				}
				++i;
			}
			break;
		case -1:
			if (sigcount == 1)
				fprintf( stderr, "(####)");
			else
				fprintf( stderr, "(%u\327####)", sigcount);
			break;
		case -2:
			if (sigcount == 1)
				fprintf( stderr, "($$$$)");
			else
				fprintf( stderr, "(%u\327$$$$)", sigcount);
		}
		sigcount = 0;
	}
}
