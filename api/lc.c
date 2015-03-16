/*
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 */

#include	<stdlib.h>
#include	<stdio.h>
#include	<pthread.h>
#include	"zs.h"
#include	"lc.h"
#include	"platform/logging.h"
#include	"utils/rico.h"


/* resource calculations for one ZS instance
 */
#define bytes_per_nvram				(1L << 24)
#define bytes_per_streaming_object		(1L << 16)
#define placement_groups_per_flash_array	(1L << 13)
#define bytes_per_stream_buffer			(1L << 12)
#define streams_per_flash_array			(bytes_per_nvram / bytes_per_streaming_object)
#define placement_groups_per_stream		(placement_groups_per_flash_array / streams_per_flash_array)
#define bytes_of_stream_buffers			(streams_per_flash_array * bytes_per_stream_buffer)

#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)

/*
 * designated writer must use atomic assignment when sharing scalers
 */
#define	atomic_assign( dst, val)	((void) __sync_add_and_fetch( (dst), (val)-(*(dst))))


typedef struct threadstructure		thread_t;
typedef struct streamstructure		stream_t;
typedef struct pgstructure		pg_t;
typedef struct containerstructure	container_t;
typedef uint64_t			lo_seqno_t,
					stream_seqno_t,
					trxid_t;

/*
 * A small number of logging containers are supported.  Client threads
 * are tracked passively to determine their current and previous trx.
 * Reference to a new PG by a client causes a structure to be allocated
 * for tracking the state, and the structure is hashed into the container
 * and linked into a stream (selected in round-robin fashion).  The oldest
 * streaming object is deleted when all occupant PGs in the object have been
 * sufficiently trimmed: after reclaiming that space, the next streaming
 * object is read and applicable PG structures prepared afresh.
 */
struct threadstructure {
	thread_t	*next;
	trxid_t		trxcurrent,
			trxprevious;
};
struct streamstructure {
	stream_seqno_t	newest,
			oldest;
	pg_t		*pg;
	uint		pgcount;
};
struct pgstructure {
	pg_t		*containerlink,
			*streamlink;
	uint		stream;
	lo_seqno_t	newest,
			trimposition,
			reclaimable,
			oldest;
};
struct containerstructure {
	ZS_cguid_t	cguid;
	pg_t		*(*z)[1000];
};


extern trxid_t __thread		trx_bracket_id;
static thread_t __thread	*thread;
static pthread_mutex_t		threadlistlock	= PTHREAD_MUTEX_INITIALIZER;
static thread_t			*threadlist;
static container_t		ctable[10];

static void	updatetrx( ),
		reclaim( ),
		dumpparams( );
static char	*prettynumber( ulong);


void
lc_init( )
{

	dumpparams( );
}


void
lc_delete( )
{

	updatetrx( );
	reclaim( );
}


/*
 * track trx brackets
 */
static void
updatetrx( )
{

	if (thread) {
		unless (trx_bracket_id) {
			atomic_assign( &thread->trxcurrent, 0);
			atomic_assign( &thread->trxprevious, 0);
		}
		else unless (trx_bracket_id == thread->trxcurrent) {
			atomic_assign( &thread->trxprevious, thread->trxcurrent);
			atomic_assign( &thread->trxcurrent, trx_bracket_id);
		}
	}
	else if (trx_bracket_id) {
		unless (thread = calloc( 1, sizeof *thread)) {
			msg( INITIAL, FATAL, "out of memory");
			abort( );
		}
		thread->trxcurrent = trx_bracket_id;
		pthread_mutex_lock( &threadlistlock);
		thread->next = threadlist;
		threadlist = thread;
		pthread_mutex_unlock( &threadlistlock);
	}
}


/*
 * reclaim flash space
 */
static void
reclaim( )
{

}


static void
dumpparams( )
{

	msg( INITIAL, INFO, "Nominal sizes for Logging Container subsystem:");
	msg( INITIAL, INFO, "bytes_per_nvram = %s", prettynumber( bytes_per_nvram));
	msg( INITIAL, INFO, "bytes_per_streaming_object = %s", prettynumber( bytes_per_streaming_object));
	msg( INITIAL, INFO, "placement_groups_per_flash_array = %s", prettynumber( placement_groups_per_flash_array));
	msg( INITIAL, INFO, "bytes_per_stream_buffer = %s", prettynumber( bytes_per_stream_buffer));
	msg( INITIAL, INFO, "streams_per_flash_array = %s", prettynumber( streams_per_flash_array));
	msg( INITIAL, INFO, "placement_groups_per_stream = %s", prettynumber( placement_groups_per_stream));
	msg( INITIAL, INFO, "bytes_of_stream_buffers = %s", prettynumber( bytes_of_stream_buffers));
}


/*
 * convert number to a pretty string
 *
 * String is returned in a static buffer.  Powers with base 1024 are defined
 * by the International Electrotechnical Commission (IEC).
 */
static char	*
prettynumber( ulong n)
{
	static char	nbuf[100];

	unless (n)
		return ("0");
	uint i = 0;
	until (n & 1L<<i)
		++i;
	i /= 10;
	sprintf( nbuf, "%lu%.2s", n/(1L<<i*10), &"\0\0KiMiGiTiPiEi"[2*i]);
	return (nbuf);
}
