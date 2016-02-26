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
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 *
 * A small number of logging containers are supported.  Client threads
 * are tracked passively to determine their current and previous trx.
 * Reference to a new PG by a client causes a structure to be allocated
 * for tracking the state, and the structure is hashed into the container
 * and linked into a stream (selected in round-robin fashion).  The oldest
 * streaming object is deleted when all occupant PGs in the object have been
 * sufficiently trimmed: after reclaiming that space, the next streaming
 * object is read and applicable PG structures prepared afresh.
 *
 * Locking strategy.  For maximum reliabilty, locking is relatively simple
 * with the only optimized call path being lc_write/lc_mput, which are
 * engineered to promote write-combining at nvr level.  The locks in LC:
 *
 *	threadlist_lock		rwlock of local scope to change trx tracking list
 *	lc_lock			rwlock of global scope to change anything
 *	containerstructure.lock	rwlock for changes restricted to one container
 *	streamstructure.buflock	mutex to the stream buffer in fast-write mode
 *
 * Fast-write mode:
 *	This mode uses a specific protocol for locking and data access
 *	to maximize performance of multiple writers, including parallel
 *	calls into nvr services for batch-commit opportunities, by taking
 *	the containerstructure.lock for READING.  The mode mandates
 *	read-only access to LC structures, except those protected
 *	by locks for localized modifications.  Specifically, the two
 *	permitted modifications in fast-write mode are those to track
 *	trx and to append data to a stream.  Stream append in this mode
 *	is not allowed if the buffer is empty or would need flushing.
 *	When fast-write mode cannot proceed due to the these restrictions,
 *	the execution path must be abandoned, and normal write protocol
 *	(containerstructure.lock taken for WRITING) used.
 *
 *	Helpful tips for fast-write mode implementors: use
 *	pglookup_readonly(); access bufnexti only when holding
 *	WRITE containerstructure.lock, or when holding both READ
 *	containerstructure.lock and streamstructure.buflock.
 *
 * To do:
 *	- more messages: errors, init status/stats
 *
 * Reclamation notes:
 *	- if a stream is non-empty on flash, always track status of the oldest SO
 *	- when SO written is sole SO of a stream, calculate status on-the-spot
 *	- when (oldest) SO is reclaimed, immediately scan status of the next SO if present
 *	- reclamation of a stream is driven by writes to that stream as given by rate R
 *	- to reclaim a SO, every contained lrec must be obsolete
 *	- obsolete means lrec has been trimmed, and is not part of a possibly active trx
 *	- trx IDs are unique and increment monotonically forever
 */

#include 	<sys/types.h>
#include 	<sys/stat.h>
#include 	<fcntl.h>
#include	<sys/types.h>
#include	<unistd.h>
#include	<stdlib.h>
#include	<stdio.h>
#include	<pthread.h>
#include	"zs.h"
#include	"lc.h"
#include	"nvr_svc.h"
#include	"ssd/fifo/mcd_osd.h"
#include 	"utils/properties.h"
#include	"platform/logging.h"
#include	"utils/hash.h"
#include 	"common/sdftypes.h"
#include	"sdftcp/locks.h"
#include	"ssd/ssd.h"
#include	"api/fdf_internal.h"
#include	"protocol/action/recovery.h"
#include	"utils/rico.h"


/* resource control for one ZS instance
 */
#define	ZS_MAX_NUM_LC_DEFAULT	16
#define	bytes_per_logging_object		4000

//#define bytes_per_NVR				nvr_len
//#define placement_groups_per_flash_array	(1L << 13)
//#define bytes_per_stream_buffer			nvr_blksize
//#define streams_per_flash_array			(bytes_per_NVR / bytes_per_streaming_object)
//#define placement_groups_per_stream		(placement_groups_per_flash_array / streams_per_flash_array)
//#define bytes_of_stream_buffers			(streams_per_flash_array * bytes_per_stream_buffer)

#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define DEBUG			PLAT_LOG_LEVEL_DEBUG
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)

#define	COUNTER_UNKNOWN		(~0)
#define	LC_MAGIC		0x81F191E1A2C2B292
#define	LOTABLESIZE		(1uL << 10)		/* buckets of LOs for enumeration */


/*
 * prevent tearing of shared scalers when assigned
 */
#define	atomic_assign( dst, val)	__sync_lock_test_and_set( (dst), (val))

ZS_ext_stat_t log_to_zs_stats_map[] = {
	{LOGSTAT_NUM_OBJS,		ZS_BTREE_NUM_OBJS,	ZS_STATS_TYPE_BTREE,0},
	{LOGSTAT_DELETE_CNT,		ZS_ACCESS_TYPES_DELETE,	ZS_STATS_TYPE_APP_REQ, 0},
	{LOGSTAT_FLUSH_CNT,		ZS_BTREE_FLUSH_CNT,	ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_IO_SAVED,		ZS_BTREE_MPUT_IO_SAVED,	ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_CNT,		ZS_ACCESS_TYPES_MPUT,	ZS_STATS_TYPE_APP_REQ, 0},
	{LOGSTAT_NUM_MPUT_OBJS,		ZS_BTREE_NUM_MPUT_OBJS,	ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_READ_CNT,		ZS_ACCESS_TYPES_READ,	ZS_STATS_TYPE_APP_REQ, 0},
	{LOGSTAT_WRITE_CNT,		ZS_ACCESS_TYPES_WRITE,	ZS_STATS_TYPE_APP_REQ, 0},
	{LOGSTAT_WRITE_SLOWPATH,	ZS_BTREE_LEAF_L1_MISSES, ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_SLOWPATH_TMPBUF,	ZS_BTREE_NONLEAF_L1_MISSES, ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_SLOWPATH_GTTMPBUF,ZS_BTREE_OVERFLOW_L1_MISSES, ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_SLOWPATH_FIRSTREC,ZS_BTREE_BACKUP_L1_MISSES, ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_SLOWPATH_NOSPC, 	ZS_BTREE_BACKUP_L1_HITS,ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_MPUT_SLOWPATH_DIFFSTR,	ZS_BTREE_LEAF_L1_WRITES,ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_SYNC_DONE,		ZS_BTREE_NONLEAF_L1_WRITES,ZS_STATS_TYPE_BTREE, 0},
	{LOGSTAT_SYNC_SAVED,		ZS_BTREE_OVERFLOW_L1_WRITES,ZS_STATS_TYPE_BTREE, 0},
};

typedef struct ZS_thread_state		ts_t;
typedef pthread_mutex_t			mutex_t;
typedef pthread_cond_t			cond_t;
typedef pthread_rwlock_t		lock_t;
typedef struct threadstructure		thread_t;
typedef struct streamstructure		stream_t;
typedef struct pgstructure		pg_t;
typedef struct pgnamestructure		pgname_t;
typedef struct containerstructure	container_t;
typedef struct lrecstructure		lrec_t;
typedef struct iterstructure		iter_t;
typedef struct lostructure		lo_t;
typedef struct sostructure		so_t;
typedef struct trxrevstructure		trxrev_t;
typedef struct nvstructure		nv_t;
typedef uint32_t			pghash_t;
typedef uint64_t			stream_seqno_t,
					trxid_t,
					counter_t;

/*
 * LC structures
 *
 * threadstructure:
 *	Used to track active trx of an incoming LC client.
 * sostructure:
 *	Structure of a streaming object as stored on flash as a regular
 *	ZS object, preferably of the same slab class as the raw objects
 *	used by Storm.	The open-ended buffer is packed with a whole
 *	number of logging records (lrec_t).  Size of the SO is recorded by
 *	the hosting facility (ZS hash table or nvr services).  Header is
 *	self-describing to allow full recovery of memory structures
 *	on restart.
 * streamstructure:
 *	Used to track stream activities, and associations with container
 *	and aging PGs.	Has a dedicated SO buffer, and an attached
 *	nvr_buffer for persisting LOs with low write amp.
 * pgnamestructure:
 *	Records the offset of counter, pg and osd parts of LO key.
 *	May store the key itself in an internal buffer.
 * pgstructure:
 *	Used to track activities on a PG. The first linked list allows
 *	lookup by the container.  A PG represented in the oldest SO
 *	of its stream is "aging" and linked into the second list for
 *	reclamation processing.
 * containerstructure:
 *	Allows PG lookup, and assignment of its dedicated streams to
 *	new PGs.
 * lrecstructure:
 *	A logging record efficiently encapsulates LOs written by the
 *	client, and also delete/trim operations.  Header is sufficiently
 *	descriptive to allow reclamation decisions.
 * iterstructure:
 *	Constructed and returned to client for object enumeration of a PG.
 * lostructure:
 *	Used to process LOs for enumeration.
 * trxrevstructure:
 *	Used to convey trx IDs into the LC for crash roll back.
 * struct nvstructure:
 *	Manages allocation of NVRAM to containers.  Buffers with live
 *	data are reserved until the owning container is opened and
 *	flushes them to flash.	Buffer with no data can be allocated.
 */
struct threadstructure {
	thread_t	*next;
	trxid_t		trxcurrent,
			trxprevious;
};
struct sostructure {
	ulong		magic;
	so_t		*next;
	ZS_cguid_t	cguid;
	uint		stream;
	stream_seqno_t	seqno;
	char		buffer[];
};
struct streamstructure {
	stream_t	*next;
	uint		stream;
	container_t	*container;
	nvr_buffer_t	*nb;
	stream_seqno_t	seqnext,
			seqoldest;
	pg_t		*pg;
	mutex_t		buflock;
	uint		bufnexti;
	trxid_t		newest;
	so_t		so;
};
struct pgnamestructure {
	uint		pgbase,
			pglen,
			len;
	char		buf[0];
};
struct pgstructure {
	bool		aging;
	pghash_t	hash;
	pg_t		*containerlink,
			*streamlink;
	stream_t	*stream;
	counter_t	trimposition,
			newest;
	pgname_t	name;
};

/*=============================================================================
 * Batch commit in logging container layer when streams_per_container = 1
 *=============================================================================
 *
 * iocount - Number of IO requests in progress and pipelined for this container
 * synccount - Number of syncs to NVRAM buffer in pipeline
 * insync - A sync to NVRAM is in progress. Done by only one thread at a time
 * buffull - The stream buffer is full, data needs to be written to flash.
 *
 * synccv - CV for threads waiting to do sync
 * flashcv - CV for sync threads to wait for buffer getting committed to flash
 * fullcv - CV for IO thread waiting for pipelined syncs to complete 
 *
 * Working:
 * a. Each thread incr iocount as soon as they make sure container exists.
 *    This is done even before acquiring the lock of container so that the
 *    io in progress doesnt initiate a sync to NVRAM.
 * b. On completing the copy to NVRAM buffer, synccount is incremented and
 *    iocount is decremented. This is done before releasing lock of 
 *    container, so that incase the IO starts on container and buffer is full,
 *    the thread has to wait all syncs enqueued is complete.
 * c. If there are no pipelined IO, a thread will set insync=1 and proceeds
 *    with sync. Then it wakes up all othre threads waiting for sync and any
 *    thread waiting for writing buffeer contents to flash.
 * d. If the buffer is full, the thread wakes up any syncs pipelined and waits
 *    till synccount becomes 0. Only streamwrite() can do this functionality.
 *
 * IMPORTANT:
 * Always call lc_sync_buf() once the iocount is incremented irrespective of
 * write succeeds or not. There could be sync thread waiting for IO to complete
 */
 

struct containerstructure {
	ZS_cguid_t	cguid;
	lock_t		lock;
	uint		stream;
	stream_t	*streams,
			*percontainerstream,
			*streamhand;
	char		*trxrevfile;
	pg_t		*pgtable[1000];
	mutex_t		synclock;
	cond_t		synccv, flashcv, fullcv;
	uint		iocount, synccount, insync,buffull;
	log_stats_t	lgstats;
	off_t		syncoff;
	ZS_status_t	syncstatus;
};
struct lrecstructure {
	ushort		type,
			klen,
			dlen;
	trxid_t		trx;
	counter_t	counter;
	char		payload[bytes_per_logging_object];
};
struct iterstructure {
	ulong		magic;
	ZS_cguid_t	cguid;			/* must match offset within ZS_iterator */
	lo_t		*lo[LOTABLESIZE];
	so_t		*so;
	uint		objecti,
			nobject;
	lrec_t		**objects;
};
struct lostructure {
	lo_t		*next;
	lrec_t		*lrec;
};
struct trxrevstructure {
	trxid_t 	*vec;
	uint		num;
};
struct nvstructure {
	nvr_buffer_t	*nb;
	ZS_cguid_t	cguid;
};

/* lrecstructure.type
 */
enum {
	LR_TOMBSTONE,
	LR_WRITE,
	LR_DELETE,
	LR_TRIM,
};


extern trxid_t __thread		trx_bracket_id;
static uint			streams_per_container,
				logging_containers_per_instance,
				bytes_per_streaming_object,
				bytes_per_so_buffer,
				nvtablen,
				always_sync;
static nv_t			*nvtab;
static thread_t __thread	*thread;
static thread_t			*threadlist;
static lock_t			lc_lock			= PTHREAD_RWLOCK_INITIALIZER,
				threadlist_lock		= PTHREAD_RWLOCK_INITIALIZER;
static container_t		*ctable;
static stream_t			*streamfreelist;

static container_t	*clookup( ZS_cguid_t),
			*callocate( );
static ZS_status_t	recover( ts_t *, container_t *),
			sostatusrecover( ts_t *t, container_t *),
			write_record( ts_t *, ZS_cguid_t, uint, char *, uint32_t, char *, uint64_t),
			write_record_one_spc( ts_t *, ZS_cguid_t, uint, char *, uint32_t, char *, uint64_t),
			write_records( ts_t *ts, lrec_t **lrecs, int num_recs, ZS_cguid_t cguid, int *written),
			write_records_one_spc( ts_t *ts, lrec_t **lrecs, int num_recs, ZS_cguid_t cguid, int *written),
			streaminit( uint),
			streamwrite( ts_t *, stream_t *, lrec_t *, off_t *off),
			streamwrite_one_spc( ts_t *, stream_t *, lrec_t *, off_t *off),
			streamreclaim( ts_t *, stream_t *),
			soscan( container_t *, stream_t *, trxrev_t *, so_t *, ulong),
			pgstreamscan( ts_t *, pg_t *, char *, uint32_t, char **, uint64_t *),
			pgstreamenumerate( ts_t *, pg_t *, iter_t *),
			nvinit( ),
			nvflush( ts_t *, ZS_cguid_t),
			keydecode( char *, uint32_t, pgname_t *, pghash_t *, counter_t *);
static nvr_buffer_t	*nvalloc( ZS_cguid_t);
static pg_t		*pglookup( container_t *, char *, pgname_t, pghash_t),
			*pglookup_readonly( container_t *, char *, pgname_t, pghash_t),
			*pglookup2( container_t *, stream_t *, char *, pgname_t, pghash_t, counter_t);
static stream_t		*streamalloc( ZS_cguid_t),
			*streamlookup( container_t *, uint);
static bool		trxtrack( ),
			trxrevdistribute( ),
			sostatus( stream_t *, so_t *, uint),
			sostatusreclaimable( stream_t *),
			pgnameequal( pgname_t *, char *, pgname_t),
			lrecbuild( lrec_t *, uint, char *, uint32_t, char *, uint64_t, counter_t),
			keyequal( char *, uint, char *, uint),
			trimmed( counter_t, counter_t);
static trxrev_t		*trxreversions( container_t *),
			*trxrevfree( trxrev_t *);
static uint		streamcount( stream_t *),
			lrecsize( lrec_t *);
static void		free_iterator( iter_t *),
			cdeallocate( container_t *),
			streamdealloc( stream_t *),
			sostatusclear( stream_t *),
			nvdealloc( nvr_buffer_t *),
			dumpparams( );
//static void		streamdump();
static char		*trxrevfilename( ZS_cguid_t),
			*make_sokey( char [], uint, stream_seqno_t);
static int		compar( const void *, const void *);
//static char		*prettynumber( ulong);
static counter_t	newest( counter_t, counter_t);
static int	 	lc_sync_buf( container_t *c, off_t off);
static ZS_status_t	lc_write_one_spc( struct ZS_thread_state *, ZS_cguid_t, char *, uint32_t, char *, uint64_t);

/*
 * LC entrypoint - initialize LC system
 *
 * Called at end of ZSInit().
 */
ZS_status_t
lc_init( struct ZS_state *zs_state, bool reformat)
{

	unless (trxrevdistribute( ))
		return (ZS_FAILURE);
	ZS_status_t r = nvr_init( zs_state);
	unless (r == ZS_SUCCESS)
		return (r);
	if (reformat)
		nvr_reset( );
	nvinit( );
	dumpparams( );
	bytes_per_streaming_object = nvr_bytes_in_buffer( );
	if (bytes_per_streaming_object < sizeof( so_t)+sizeof( lrec_t))
		return (ZS_FAILURE);
	bytes_per_so_buffer = bytes_per_streaming_object - sizeof( so_t);
	uint streams_per_instance = nvr_buffer_count( );
	unless (logging_containers_per_instance = getProperty_Int( "ZS_MAX_NUM_LC", ZS_MAX_NUM_LC_DEFAULT))
		return (ZS_FAILURE);
	unless (ctable = calloc( logging_containers_per_instance, sizeof *ctable))
		return (ZS_OUT_OF_MEM);
	streams_per_container = divideup( streams_per_instance, logging_containers_per_instance);
	always_sync = !(streams_per_container == 1);
	r = streaminit( streams_per_instance);
	unless (r == ZS_SUCCESS)
		return (r);
	return (ZS_SUCCESS);
}


/*
 * LC entrypoint - open a container
 *
 * On open, LC containers are recovered entirely from the regular ZS objects
 * in the underlying hash-mode container.  An object of this kind is called
 * a streaming object (SO).
 *
 * On first open after ZS restart, additional SOs for this container are
 * retrieved from NVRAM hardware, and persisted to flash.  Opening all
 * existing LCs in this way will empty the NVRAM, allowing the hardware
 * to be physically removed.  Also on first open after a ZS restart,
 * uncommitted outer trx are rolled back by means of a recovery packet
 * stored in the file system as generated earlier.  Until the LC is opened,
 * the respective packet must not be disturbed.  Crash recovery is itself
 * crash-safe because the steps are idempotent.
 *
 * After recovery, remaining structures and tracking state for the container
 * are initialized.  This includes the addition of new streams to reach the
 * specified total.  Newly encountered PGs will be assigned to a stream in
 * round-robin order.
 */
ZS_status_t
lc_open( ts_t *t, ZS_cguid_t cguid)
{
	uint	i,
		j;

	char *a = trxrevfilename( cguid);
	unless (a)
		return (ZS_OUT_OF_MEM);
	pthread_rwlock_wrlock( &lc_lock);
	if (clookup( cguid)) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_SUCCESS);
	}
	container_t *c = callocate( );
	unless (c) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	c->cguid = cguid;
	c->trxrevfile = a;
	ZS_status_t r = nvflush( t, cguid);
	unless (r == ZS_SUCCESS) {
		cdeallocate( c);
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	r = recover( t, c);
	switch (r) {
	case ZS_SUCCESS:
	case ZS_CONTAINER_UNKNOWN:
		for (j=streamcount( c->streams); j<streams_per_container; ++j) {
			stream_t *s = streamalloc( cguid);
			unless (s) {
				unless (j) {
					pthread_rwlock_unlock( &lc_lock);
					return (ZS_FAILURE);
				}
				break;
			}
			s->stream = ++c->stream;
			s->next = c->streams;
			c->streams = s;
			if (streams_per_container == 1) {
				c->percontainerstream = s;
			} else {
				c->percontainerstream = NULL;
			}
			s->container = c;
			s->so.cguid = cguid;
			s->so.stream = s->stream;
		}
		break;
	default:
		cdeallocate( c);
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	unless (c->streamhand = c->streams) {
		cdeallocate( c);
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	r = sostatusrecover( t, c);
	unless (r == ZS_SUCCESS) {
		cdeallocate( c);
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	pthread_rwlock_unlock( &lc_lock);
	if (c->trxrevfile)
		unlink( c->trxrevfile);
	return (ZS_SUCCESS);
}


ZS_status_t
lc_delete_container( ZS_cguid_t cguid)
{
	uint	i;

	pthread_rwlock_wrlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless (c) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_CONTAINER_UNKNOWN);
	}
	for (i=0; i<nel( c->pgtable); ++i) {
		pg_t *pg = c->pgtable[i];
		while (pg) {
			pg_t *pgnext = pg->containerlink;
			free( pg);
			pg = pgnext;
		}
	}
	stream_t *s = c->streams;
	while (s) {
		nvr_reset_buffer( s->nb);
		stream_t *snext = s->next;
		streamdealloc( s);
		s = snext;
	}
	cdeallocate( c);
	pthread_rwlock_unlock( &lc_lock);
	return (ZS_SUCCESS);
}


/*
 * LC entrypoint - write a logging object
 *
 * LO is inserted or updated without distinction.  Fast-write mode is
 * used to improve CPU efficiency and, more importantly, to encourage
 * batch-commit optimization.  As mentioned earlier, fast-mode write is
 * possible under conditions that are quite common: PG already existing,
 * and lrec able to drop into the middle of the stream buffer.  The mutex
 * protecting stream access is held for a short time.
 */
ZS_status_t
lc_write( ts_t *t, ZS_cguid_t cguid, char *k, uint32_t kl, char *d, uint64_t dl)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;
	lrec_t		lr;
	size_t		sz;
	off_t		off;
	stream_t	*s;

	if (streams_per_container == 1) {
		return lc_write_one_spc(t, cguid, k, kl, d, dl);
	}

	ZS_status_t r = keydecode( k, kl, &n, &h, &counter);
	unless (r == ZS_SUCCESS)
		return (r);
	unless (lrecbuild( &lr, LR_WRITE, k, kl, d, dl, counter))
		return (ZS_OBJECT_TOO_BIG);
	const uint i = lrecsize( &lr);
	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}

	pthread_rwlock_rdlock( &c->lock);
	atomic_inc(c->lgstats.stat[LOGSTAT_WRITE_CNT]);
	pg_t *pg = pglookup_readonly( c, k, n, h);
	if (pg) {
		s = pg->stream;
		pthread_mutex_lock( &s->buflock);
		unless ((s->bufnexti == 0)
		or (bytes_per_so_buffer < i + s->bufnexti)) {
			uint bufnexti = s->bufnexti;
			s->bufnexti += i;
			memcpy( s->so.buffer+bufnexti, &lr, i);
			pthread_mutex_unlock( &s->buflock);
			if ((sz = nvr_write_buffer_partial( s->nb, &s->so.buffer[bufnexti], i, 1, NULL)) == -1) {
				r = ZS_FAILURE;
			} else {
				//atomic_inc(c->lgstats.stat[LOGSTAT_NUM_OBJS]);
			}
			pthread_rwlock_unlock( &c->lock);
			pthread_rwlock_unlock( &lc_lock);
			return (r);
		}
		pthread_mutex_unlock( &s->buflock);
	}
	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);

	r = write_record( t, cguid, LR_WRITE, k, kl, d, dl);
	return (r);
}

ZS_status_t
lc_write_one_spc( ts_t *t, ZS_cguid_t cguid, char *k, uint32_t kl, char *d, uint64_t dl)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;
	lrec_t		lr;
	size_t		sz;
	off_t		off;
	stream_t	*s;

	plat_assert(streams_per_container == 1);

	ZS_status_t r = keydecode( k, kl, &n, &h, &counter);
	unless (r == ZS_SUCCESS)
		return (r);
	unless (lrecbuild( &lr, LR_WRITE, k, kl, d, dl, counter))
		return (ZS_OBJECT_TOO_BIG);
	const uint i = lrecsize( &lr);

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}

	/* 
	 * Increment iocount to make sync wait for this IO to complete
	 * and do batch commit. Need to do lc_sync_buf() always now for
	 * waking up sync threads irrespective of NVRAM API fails or succeeds.
	 */
	int ioc = atomic_add_get(c->iocount, 1);
	plat_assert(ioc > 0);

	pthread_rwlock_rdlock( &c->lock);
	atomic_inc(c->lgstats.stat[LOGSTAT_WRITE_CNT]);
	pg_t *pg = pglookup_readonly( c, k, n, h);
	if (pg) {
		s = pg->stream;
		pthread_mutex_lock( &s->buflock);
		unless ((s->bufnexti == 0)
		or (bytes_per_so_buffer < i + s->bufnexti)) {
			uint bufnexti = s->bufnexti;
			s->bufnexti += i;
			memcpy( s->so.buffer+bufnexti, &lr, i);
			pthread_mutex_unlock( &s->buflock);
			if ((sz = nvr_write_buffer_partial( s->nb, &s->so.buffer[bufnexti], i, always_sync, &off)) == -1) {
				r = ZS_FAILURE;
			} else {
				//atomic_inc(c->lgstats.stat[LOGSTAT_NUM_OBJS]);
			}
					
			goto out;
		}
		pthread_mutex_unlock( &s->buflock);
	}
	pthread_rwlock_unlock( &c->lock);

	pthread_rwlock_wrlock( &c->lock);
	pg = pglookup( c, k, n, h);
	unless (pg) {
		r = ZS_FAILURE;
		goto out;
	}
	atomic_inc(c->lgstats.stat[LOGSTAT_WRITE_SLOWPATH]);
	r = streamwrite_one_spc( t, pg->stream, &lr, &off);
	//atomic_inc(c->lgstats.stat[LOGSTAT_NUM_OBJS]);
out:
	{
		/*
		 * Increment sync count before releasing the lock. If a write 
		 * gets pipelined and buffer is full, it should wait for sync
		 * to complete.
		 */
		int sc = atomic_inc_get(c->synccount);
		plat_assert(sc > 0);
		int ioc = atomic_dec_get(c->iocount);
		plat_assert(ioc >= 0);
	}

	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);

	if (r != ZS_SUCCESS) {
		off = -1;
		lc_sync_buf(c, off);
	} else {
		r = lc_sync_buf(c, off);
	}


	return (r);
}

/*
 * LC entrypoint - delete a logging object
 *
 * LO is deleted if it exists, otherwise no effect.  Flash space is never
 * reclaimed by this operation (see lc_trim).
 */
ZS_status_t
lc_delete( ts_t *t, ZS_cguid_t cguid, char *k, uint32_t kl)
{

	return (write_record( t, cguid, LR_DELETE, k, kl, 0, 0));
}


/*
 * LC entrypoint - trim a PG
 *
 * The specified PG is trimmed at position given by the counter.  All LOs
 * before the trim position are considered deleted.  With sufficient trimming
 * of PGs, flash space will be freed without write amp for further ZS use.
 */
ZS_status_t
lc_trim( ts_t *t, ZS_cguid_t cguid, char *k, uint32_t kl)
{
	return (write_record( t, cguid, LR_TRIM, k, kl, 0, 0));
}


/*
 * LC entrypoint - read a logging object
 *
 * Object is returned if present, otherwise ZS_FAILURE.
 */
ZS_status_t
lc_read( ts_t *t, ZS_cguid_t cguid, char *k, uint32_t kl, char **d, uint64_t *dl)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	ZS_status_t r = keydecode( k, kl, &n, &h, &counter);
	unless (r == ZS_SUCCESS) {
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	pthread_rwlock_rdlock( &c->lock);
	pg_t *pg = pglookup( c, k, n, h);
	unless (pg) {
		pthread_rwlock_unlock( &c->lock);
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	atomic_inc(c->lgstats.stat[LOGSTAT_READ_CNT]);
	r = pgstreamscan( t, pg, k, kl, d, dl);
	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);
	return (r);
}


/*
 * LC entrypoint - start enumeration of LOs in a PG
 *
 * Use the returned iterator to enumerate LOs.
 */
ZS_status_t
lc_enum_start( ts_t *t, ZS_cguid_t cguid, void *iterator, char *k, uint32_t kl)
{
	pghash_t	h;
	pgname_t	n;

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	ZS_status_t r = keydecode( k, kl, &n, &h, 0);
	unless (r == ZS_SUCCESS) {
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	pthread_rwlock_rdlock( &c->lock);
	pg_t *pg = pglookup( c, k, n, h);
	unless (pg) {
		pthread_rwlock_unlock( &c->lock);
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	iter_t *iter = calloc( sizeof *iter, 1);
	unless (iter) {
		pthread_rwlock_unlock( &c->lock);
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_OUT_OF_MEM);
	}
	r = pgstreamenumerate( t, pg, iter);
	unless (r == ZS_SUCCESS) {
		free_iterator( iter);
		pthread_rwlock_unlock( &c->lock);
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);
	iter->magic = LC_MAGIC;
	iter->cguid = cguid;
	*(iter_t **)iterator = iter;
	return (ZS_SUCCESS);
}


/*
 * LC entrypoint - continue enumeration of a PG
 *
 * The next LO is returned, otherwise ZS_OBJECT_UNKNOWN.
 */
ZS_status_t
lc_enum_next( void *iterator, char **key, uint32_t *keylen, char **data, uint64_t *datalen)
{

	iter_t *iter = (iter_t *)iterator;
	unless (iter->magic == LC_MAGIC)
		return (ZS_FAILURE);
	if (iter->objecti < iter->nobject) {
		lrec_t *lr = iter->objects[iter->objecti++];
		unless (*key = malloc( lr->klen))
			return (ZS_OUT_OF_MEM);
		unless (*data = malloc( lr->dlen)) {
			free( *key);
			return (ZS_OUT_OF_MEM);
		}
		memcpy( *key, lr->payload, lr->klen);
		*keylen = lr->klen;
		memcpy( *data, &lr->payload[lr->klen], lr->dlen);
		*datalen = lr->dlen;
		return (ZS_SUCCESS);
	}
	return (ZS_OBJECT_UNKNOWN);
}


/*
 * LC entrypoint - finish enumeration of a PG
 *
 * The iterator is freed.
 */
ZS_status_t
lc_enum_finish( void *iterator)
{

	iter_t *iter = (iter_t *)iterator;
	unless (iter->magic == LC_MAGIC)
		return (ZS_FAILURE);
	iter->magic = 0;
	free_iterator( iter);
	return (ZS_SUCCESS);
}
ZS_status_t
lc_mput( ts_t *t, ZS_cguid_t cguid, uint32_t num_objs, ZS_obj_t *objs, uint32_t flags,
					        uint32_t *objs_written)
{

#define TMPBUFSZ	65536

	counter_t	counter;
	pghash_t	h;
	pgname_t	n;
	ZS_status_t	ret = ZS_SUCCESS;
	char		tmpbuf[TMPBUFSZ];
	lrec_t		lr, *lrecs[TMPBUFSZ / (sizeof(lrec_t) - bytes_per_logging_object)];
	uint		syncoff=0, sz;
	stream_t	*s = 0;
	int		i = 0, committed_keys = 0, szintmpbuf = 0, objsinbuf;
	pg_t		*pg, *spg = NULL;
	int		written;
	int		flag = 0;

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	if (c) {
		atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_CNT]);
		atomic_add(c->lgstats.stat[LOGSTAT_NUM_MPUT_OBJS], num_objs);
		pthread_rwlock_unlock(&lc_lock);
	} else {
		pthread_rwlock_unlock( &lc_lock);
		return ZS_CONTAINER_UNKNOWN;
	}
	i = 0;
//restart:
	objsinbuf = 0;
	for (; i < num_objs; i++) {
		ZS_status_t r = keydecode( objs[i].key, objs[i].key_len, &n, &h, &counter);
		unless (r == ZS_SUCCESS) {
			ret = ZS_FAILURE;
			goto out;
		}
		unless (lrecbuild( &lr, LR_WRITE, objs[i].key, objs[i].key_len, 
						objs[i].data, objs[i].data_len, counter)) {
			ret = ZS_FAILURE;
			goto out;
		}

		sz = lrecsize( &lr);
		if (sz <= TMPBUFSZ) {
			if ((sz + szintmpbuf) > TMPBUFSZ) {
				/* Handle if record cant be accomodated in tmpbuf */
				flag = 1;
				ret = write_records(t, lrecs, objsinbuf, cguid, &written);
				if (ret == ZS_SUCCESS) {
					plat_assert(objsinbuf == written);
					committed_keys += written;
				} else {
					goto out;
				}
				szintmpbuf = 0;
				objsinbuf = 0;
			}
			memcpy(tmpbuf + szintmpbuf, &lr, sz);
			lrecs[objsinbuf] = (lrec_t *)(tmpbuf + szintmpbuf);
			szintmpbuf += sz;
			objsinbuf++;
			continue;
		} else {
			/* Handle record greater than tmpbuf sz */
			atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_GTTMPBUF]);
			ret = write_records(t, lrecs, objsinbuf, cguid, &written);
			if (ret == ZS_SUCCESS) {
				plat_assert(objsinbuf == written);
				committed_keys += written;
			} else {
				goto out;
			}
			szintmpbuf = 0;
			objsinbuf = 0;
			ret = lc_write(t, cguid, objs[i].key, objs[i].key_len,
							objs[i].data, objs[i].data_len);
			if (ret == ZS_SUCCESS) {
				committed_keys += 1;
			} else {
				goto out;
			}
		}
	}

	if (objsinbuf) {
		if (flag == 1){
			atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_TMPBUF]);
		}
		ret = write_records(t, lrecs, objsinbuf, cguid, &written);
		if (ret == ZS_SUCCESS) {
			plat_assert(objsinbuf == written);
			committed_keys += written;
		}
	}
out:
	*objs_written = committed_keys;
	return ret;
}

/*
 * Make sure the key/value is written to NVRAM
 */
static int
lc_sync_buf( container_t *c, off_t off)
{
	off_t		soff;
	int		cnt = 0;
	stream_t	*s = c->percontainerstream;
	ZS_status_t	status;

	plat_assert(streams_per_container == 1);

	pthread_mutex_lock(&c->synclock);

	/*
	 * IO had failed before sync. So, just wake up the pipelined
	 * IO thread.
	 */
	if (off == -1) {
		int sc = atomic_dec_get(c->synccount);
		if (sc == 0) {
			pthread_cond_broadcast(&c->flashcv);
		}
		//pthread_cond_broadcast(&c->synccv);
		pthread_mutex_unlock(&c->synclock);
		return ZS_SUCCESS;
	}

restart:

	/*
	 * Sync in progress. Wait for it to finish.
	 */
	while (c->insync && c->syncoff < off) {
		//msg(INITIAL, DEBUG, "sync in progress: nso_syncoff (%d) off (%d)", (int)so->nso_syncoff, (int)off);
		pthread_cond_wait(&c->synccv, &c->synclock);
	}

	/*
	 * Data was already synced by previous sync. Return.
	 */
	if (c->syncoff >= off) {
		int sc = atomic_dec_get(c->synccount);
		if (sc == 0) {
			pthread_cond_broadcast(&c->flashcv);
		}
		//pthread_cond_broadcast(&c->synccv);
		atomic_inc(c->lgstats.stat[LOGSTAT_SYNC_SAVED]);
		pthread_mutex_unlock(&c->synclock);
		return ZS_SUCCESS;
	}

	/*
	 * While there is pipelined IO, donot sync.
	 */
	while ((cnt = atomic_add_get(c->iocount, 0)) > 0) {
		if (c->buffull == 1) {
			/*
			 * Buffer is full. No use of sync, lets write to flash itself.
			 * Wake up the IO thread waiting for sync to complete.
			 */
			int sc = atomic_dec_get(c->synccount);
			if (sc == 0) {
				pthread_cond_broadcast(&c->flashcv);
			}
			//pthread_cond_broadcast(&c->synccv);
			pthread_cond_wait(&c->fullcv, &c->synclock);
			atomic_inc(c->lgstats.stat[LOGSTAT_SYNC_SAVED]);
			status = c->syncstatus;
			pthread_mutex_unlock(&c->synclock);
			return status; 
		} else {
			//break;
			//pthread_cond_broadcast(&c->flashcv);
			//pthread_cond_broadcast(&c->synccv);
			pthread_cond_wait(&c->synccv, &c->synclock);
			goto restart;
		}
	}

	/*
	 * Buffer is full. No use of sync, lets write to flash itself.
	 * Wake up the IO thread waiting for sync to complete.
	 */
	if (c->buffull == 1) {
		int sc = atomic_dec_get(c->synccount);
		if (sc == 0) {
			pthread_cond_broadcast(&c->flashcv);
		}
		//pthread_cond_broadcast(&c->synccv);
		pthread_cond_wait(&c->fullcv, &c->synclock);
		atomic_inc(c->lgstats.stat[LOGSTAT_SYNC_SAVED]);
		status = c->syncstatus;
		pthread_mutex_unlock(&c->synclock);
		return status;
	}

	plat_assert(!c->insync);
	c->insync = 1;

	pthread_mutex_unlock(&c->synclock);
	atomic_inc(c->lgstats.stat[LOGSTAT_SYNC_DONE]);
	soff = nvr_sync_buf_aligned(s->nb, off);
	pthread_mutex_lock(&c->synclock);

	if (soff == -1) {
		c->syncstatus = ZS_FAILURE;
	} else {
		c->syncstatus = ZS_SUCCESS;
		c->syncoff = soff;
	}
	c->insync = 0;

	if (soff != -1) {
		if (soff < off) {
			/* Aligned write was done by NVRAM, restart sync. */
			if (atomic_add_get(c->synccount, 0) > 1) {
				pthread_cond_broadcast(&c->synccv);
				pthread_mutex_unlock(&c->synclock);
				//sched_yield();
				pthread_mutex_lock(&c->synclock);
			}
			//pthread_cond_broadcast(&c->flashcv);
			goto restart;
		}
	}
	int sc = atomic_dec_get(c->synccount);
	if (sc == 0) {
		pthread_cond_broadcast(&c->flashcv);
	}
	pthread_cond_broadcast(&c->synccv);
	status = c->syncstatus;
	pthread_mutex_unlock(&c->synclock);

	return status;
}

/*
 * recover container state from flash
 *
 * The hash-mode container for this LC is enumerated: the SOs found
 * allow reconstruction of memory state.  If present, a recovery packet
 * is processed to tombstone any logging records that were part of
 * an uncommitted outer trx.  Affected SOs are written back to flash.
 * Discovered streams are attached to the container.
 */
static ZS_status_t
recover( ts_t *t, container_t *c)
{
	struct ZS_iterator	*iterator;
	char			*k,
				*d;
	uint64_t		kl64,
				dl;

	ZS_status_t r = ZSEnumerateContainerObjects( t, c->cguid, &iterator);
	unless (r == ZS_SUCCESS)
		return (r);
	trxrev_t *trxrev = trxreversions( c);
	unless (trxrev)
		return (ZS_FAILURE);
	loop
		switch (r = enumerate_next( (void *)t, iterator, &k, &kl64, &d, &dl)) {
		default:
			trxrevfree( trxrev);
			if (r == ZS_OBJECT_UNKNOWN) {
				r = enumerate_done( (void *)t, iterator);
				unless (r == ZS_SUCCESS)
					return (r);
				return (c->streams? ZS_SUCCESS: ZS_CONTAINER_UNKNOWN);
			}
			enumerate_done( (void *)t, iterator);
			return (r);
		case ZS_SUCCESS:;
			so_t *so = (so_t *)d;
			stream_t *s = streamlookup( c, so->stream);
			unless (s) {
				unless (s = streamalloc( c->cguid)) {
					trxrevfree( trxrev);
					return (ZS_FAILURE);
				}
				c->stream = max( c->stream, so->stream);
				s->stream = so->stream;
				s->seqoldest = ~0;
				s->next = c->streams;
				c->streams = s;
				s->container = c;
				if (streams_per_container == 1) {
					c->percontainerstream = s;
				} else {
					c->percontainerstream = NULL;
				}
				s->so.cguid = c->cguid;
				s->so.stream = s->stream;
			}
			s->seqnext = max( s->seqnext, so->seqno+1);
			s->seqoldest = min( s->seqoldest, so->seqno);
			r = soscan( c, s, trxrev, so, dl);
			switch (r) {
			case ZS_OBJECT_DELETED:
				r = zs_write_object_lc( t, c->cguid, k, kl64, d, dl);
			case ZS_SUCCESS:
				break;
			default:
				trxrevfree( trxrev);
				return (ZS_FAILURE);
			}
			ZSFreeBuffer( k);
			ZSFreeBuffer( d);
		}
}


/*
 * write client request to stream 
 *
 * The request is packed into an lrec_t and written to the stream associated
 * with the given PG.  If a trim request, the position is updated in the
 * PG tracking info.
 */
static ZS_status_t
write_record( ts_t *t, ZS_cguid_t cguid, uint type, char *k, uint32_t kl, char *d, uint64_t dl)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;
	lrec_t		lr;
	off_t		off;
	pg_t		*pg = NULL;

	if (streams_per_container == 1) {
		return write_record_one_spc(t, cguid, type, k, kl, d, dl);
	}

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	ZS_status_t r = keydecode( k, kl, &n, &h, &counter);
	unless (r == ZS_SUCCESS) {
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}
	pthread_rwlock_wrlock( &c->lock);
	pg = pglookup( c, k, n, h);
	unless ((pg)
	and (lrecbuild( &lr, type, k, kl, d, dl, counter))) {
		r = ZS_FAILURE;
		goto fail;
	}
	atomic_inc(c->lgstats.stat[LOGSTAT_WRITE_SLOWPATH]);
	if (type == LR_TRIM || type == LR_DELETE) {
		atomic_inc(c->lgstats.stat[LOGSTAT_DELETE_CNT]);
	}
	r = streamwrite( t, pg->stream, &lr, &off);
	unless (r == ZS_SUCCESS) {
		goto fail;
	}
	if (type == LR_TRIM) {
		pg->trimposition = newest( pg->trimposition, counter);
	}

	//atomic_inc(c->lgstats.stat[LOGSTAT_NUM_OBJS]);
fail:
	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);
	return r;
}

static ZS_status_t
write_record_one_spc( ts_t *t, ZS_cguid_t cguid, uint type, char *k, uint32_t kl, char *d, uint64_t dl)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;
	lrec_t		lr;
	off_t		off;
	pg_t		*pg = NULL;

	plat_assert(streams_per_container == 1);

	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
	and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	ZS_status_t r = keydecode( k, kl, &n, &h, &counter);
	unless (r == ZS_SUCCESS) {
		pthread_rwlock_unlock( &lc_lock);
		return (r);
	}

	/* 
	 * Increment iocount to make sync wait for this IO to complete
	 * and do batch commit. Need to do lc_sync_buf() always now for
	 * waking up sync threads irrespective of NVRAM API fails or succeeds.
	 */
	int ioc = atomic_add_get(c->iocount, 1);
	plat_assert(ioc > 0);

	pthread_rwlock_wrlock( &c->lock);
	pg = pglookup( c, k, n, h);
	unless ((pg)
	and (lrecbuild( &lr, type, k, kl, d, dl, counter))) {
		r = ZS_FAILURE;
		goto fail;
	}
	atomic_inc(c->lgstats.stat[LOGSTAT_WRITE_SLOWPATH]);
	if (type == LR_TRIM || type == LR_DELETE) {
		atomic_inc(c->lgstats.stat[LOGSTAT_DELETE_CNT]);
	}
	r = streamwrite_one_spc( t, pg->stream, &lr, &off);
	unless (r == ZS_SUCCESS) {
		goto fail;
	}
	if (type == LR_TRIM) {
		pg->trimposition = newest( pg->trimposition, counter);
	}

	//atomic_inc(c->lgstats.stat[LOGSTAT_NUM_OBJS]);
fail:
	{
		/*
		 * Increment sync count before releasing the lock. If a write 
		 * gets pipelined and buffer is full, it should wait for sync
		 * to complete.
		 */
		int sc = atomic_inc_get(c->synccount);
		plat_assert(sc > 0);
		int ioc = atomic_dec_get(c->iocount);
		plat_assert(ioc >= 0);
	}

	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);
	if ( r != ZS_SUCCESS) {
		off = -1;
		lc_sync_buf(c, off);
	} else {
		r = lc_sync_buf(c, off);
	}
	return r;
}

static ZS_status_t
write_records( ts_t *ts, lrec_t **lrecs, int num_recs, ZS_cguid_t cguid, int *written)
{
	pg_t 		*pg;
	pgname_t 	n;
	pghash_t 	h;
	counter_t 	counter;
	stream_t 	*ss;
	char 		*k;
	int 		i = 0, objs_copied;
	uint 		startoff;
	ZS_status_t 	r = ZS_SUCCESS;
	size_t		sz;
	off_t		off;

	if (streams_per_container == 1) {
		return write_records_one_spc(ts, lrecs, num_recs, cguid, written);
	}

	*written = 0;
	objs_copied = 0;
	ss = NULL;
	startoff = 0;
	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
		and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}

	pthread_rwlock_rdlock( &c->lock);
	atomic_add(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED], num_recs);
	for (i = 0; i < num_recs; i++) {
		k = (char *)(lrecs[i]->payload);
		r = keydecode(k, lrecs[i]->klen, &n, &h, &counter);
		uint sz = lrecsize( lrecs[i]);
	        unless (r == ZS_SUCCESS) {
			break;
		}
		pg = pglookup_readonly( c, k, n, h);
		if (pg) {
			if (!ss) {
				/* Save the stream to which first record goes */
				ss = pg->stream;
				pthread_mutex_lock( &ss->buflock);
				startoff = ss->bufnexti;
			}
			if (pg->stream == ss) {
				/* 
				 * If the records belongs to same PG stream as others belong, 
				 * just add it if the stream has space and initialized. Else
				 * commit the existing contents of buffer to NVRAM and initialize
				 * the stream taking slow path.
				 */
				if ((ss->bufnexti != 0) &&
					(bytes_per_so_buffer >= sz + ss->bufnexti)) {
					uint bufnexti = ss->bufnexti;
					ss->bufnexti += sz;
					memcpy( ss->so.buffer+bufnexti, lrecs[i], sz);
					objs_copied++;
					continue;
				} else if (ss->bufnexti == 0) {
					atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_FIRSTREC]);
				} else if (bytes_per_so_buffer < (sz + ss->bufnexti)) {
					atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_NOSPC]);
				}	
			} else {
				atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_DIFFSTR]);
			}
		} else {
			atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_FIRSTREC]);
		}
		/*
		 * If there are any records in the buffer, commit it before 
		 * taking slow path.
		 */
		if (objs_copied) {
			uint bufnexti = ss->bufnexti;
			pthread_mutex_unlock(&ss->buflock);

			if ((sz = nvr_write_buffer_partial( ss->nb, &ss->so.buffer[startoff], 
					bufnexti - startoff, 1, NULL)) == -1) {
				r = ZS_FAILURE;
			}
			atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
			if (r == ZS_SUCCESS) {
				*written += objs_copied;
				ss = NULL;
				objs_copied = 0;
				startoff = 0;
			} else {
				ss = NULL;
				objs_copied = 0;
				startoff = 0;
				goto out;
			}
		} else if (ss) {
			pthread_mutex_unlock(&ss->buflock);
			ss = NULL;
			objs_copied = 0;
			startoff = 0;
		}

		pthread_rwlock_unlock( &c->lock);
		/* 
		 * Take slow path, commit only one record.
		 *	a. Either this is the first record to stream.
		 *	b. There is no space in stream, so need to commit stream to flash and 
		 *	   and initialize it again.
		 *	c. This record belongs to a different stream.
		 */

		pthread_rwlock_wrlock( &c->lock);
		pg_t *pg = pglookup( c, k, n, h);
		if (pg) {
			r = streamwrite( ts, pg->stream, lrecs[i], &off);
			atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
			if (r != ZS_SUCCESS) {
				break;
			} else {
				pthread_rwlock_unlock( &c->lock);
				*written += 1;
				/* Switch to fast path again */
				pthread_rwlock_rdlock( &c->lock);
			}
		} else {
			break;
		}

	}

	if (objs_copied) {
		size_t sz;
		uint bufnexti = ss->bufnexti;
		pthread_mutex_unlock(&ss->buflock);
		if ((sz = nvr_write_buffer_partial( ss->nb, &ss->so.buffer[startoff], 
				bufnexti - startoff, 1, NULL)) == -1) {
			r = ZS_FAILURE;
		}
		atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
		if (r == ZS_SUCCESS) {
			*written += objs_copied;
		}
	}
out:
	atomic_sub(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED], num_recs - *written);
	//atomic_add(c->lgstats.stat[LOGSTAT_NUM_OBJS], *written);
	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);
	return r;
}

static ZS_status_t
write_records_one_spc( ts_t *ts, lrec_t **lrecs, int num_recs, ZS_cguid_t cguid, int *written)
{
	pg_t 		*pg;
	pgname_t 	n;
	pghash_t 	h;
	counter_t 	counter;
	stream_t 	*ss;
	char 		*k;
	int 		i = 0, objs_copied;
	uint 		startoff;
	ZS_status_t 	r = ZS_SUCCESS;
	size_t		sz;
	off_t		off;

	plat_assert(streams_per_container == 1);

	*written = 0;
	objs_copied = 0;
	ss = NULL;
	startoff = 0;
	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless ((c)
		and (trxtrack( ))) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}

	/* 
	 * Increment iocount to make sync wait for this IO to complete
	 * and do batch commit. Need to do lc_sync_buf() always now for
	 * waking up sync threads irrespective of NVRAM API fails or succeeds.
	 */
	int ioc = atomic_inc_get(c->iocount);
	plat_assert(ioc > 0);

	pthread_rwlock_rdlock( &c->lock);
	atomic_add(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED], num_recs);
	for (i = 0; i < num_recs; i++) {
		k = (char *)(lrecs[i]->payload);
		r = keydecode(k, lrecs[i]->klen, &n, &h, &counter);
		uint sz = lrecsize( lrecs[i]);
	        unless (r == ZS_SUCCESS) {
			break;
		}
		pg = pglookup_readonly( c, k, n, h);
		if (pg) {
			if (!ss) {
				/* Save the stream to which first record goes */
				ss = pg->stream;
				pthread_mutex_lock( &ss->buflock);
				startoff = ss->bufnexti;
			}

			if ((ss->bufnexti != 0) &&
					(bytes_per_so_buffer >= sz + ss->bufnexti)) {
				uint bufnexti = ss->bufnexti;
				ss->bufnexti += sz;
				memcpy( ss->so.buffer+bufnexti, lrecs[i], sz);
				objs_copied++;
				continue;
			} else if (ss->bufnexti == 0) {
				atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_FIRSTREC]);
			} else if (bytes_per_so_buffer < (sz + ss->bufnexti)) {
				atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_NOSPC]);
			}	
		} else {
			atomic_inc(c->lgstats.stat[LOGSTAT_MPUT_SLOWPATH_FIRSTREC]);
		}
		/*
		 * If there are any records in the buffer, commit it before 
		 * taking slow path.
		 */
		if (objs_copied) {
			uint bufnexti = ss->bufnexti;
			pthread_mutex_unlock(&ss->buflock);

			if ((sz = nvr_write_buffer_partial( ss->nb, &ss->so.buffer[startoff], 
					bufnexti - startoff, always_sync, &off)) == -1) {
				r = ZS_FAILURE;
			}
			atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
			if (r == ZS_SUCCESS) {
				*written += objs_copied;
				ss = NULL;
				objs_copied = 0;
				startoff = 0;
			} else {
				ss = NULL;
				objs_copied = 0;
				startoff = 0;
				goto out;
			}
		} else if (ss) {
			pthread_mutex_unlock(&ss->buflock);
			ss = NULL;
			objs_copied = 0;
			startoff = 0;
		}

		pthread_rwlock_unlock( &c->lock);
		/* 
		 * Take slow path, commit only one record.
		 *	a. Either this is the first record to stream.
		 *	b. There is no space in stream, so need to commit stream to flash and 
		 *	   and initialize it again.
		 */

		pthread_rwlock_wrlock( &c->lock);
		pg_t *pg = pglookup( c, k, n, h);
		if (pg) {
			r = streamwrite_one_spc( ts, pg->stream, lrecs[i], &off);
			atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
			if (r != ZS_SUCCESS) {
				break;
			} else {
				pthread_rwlock_unlock( &c->lock);
				*written += 1;
				/* Switch to fast path again */
				pthread_rwlock_rdlock( &c->lock);
			}
		} else {
			break;
		}

	}

	if (objs_copied) {
		size_t sz;
		uint bufnexti = ss->bufnexti;
		pthread_mutex_unlock(&ss->buflock);
		if ((sz = nvr_write_buffer_partial( ss->nb, &ss->so.buffer[startoff], 
				bufnexti - startoff, always_sync, &off)) == -1) {
			r = ZS_FAILURE;
		}
		atomic_dec(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED]);
		if (r == ZS_SUCCESS) {
			*written += objs_copied;
		}
	}
out:
	atomic_sub(c->lgstats.stat[LOGSTAT_MPUT_IO_SAVED], num_recs - *written);
	//atomic_add(c->lgstats.stat[LOGSTAT_NUM_OBJS], *written);

	/*
	 * Increment sync count before releasing the lock. If a write 
	 * gets pipelined and buffer is full, it should wait for sync
	 * to complete.
	 */

	int sc = atomic_inc_get(c->synccount);
	plat_assert(sc > 0);
	ioc = atomic_dec_get(c->iocount);
	plat_assert(ioc >= 0);

	pthread_rwlock_unlock( &c->lock);
	pthread_rwlock_unlock( &lc_lock);

	if ( r != ZS_SUCCESS) {
		off = -1;
		lc_sync_buf(c, off);
	} else {
		r = lc_sync_buf(c, off);
	}

	return r;
}

/*
 * track trx brackets by client thread
 *
 * If the client's thread is operating under a trx, both the current and
 * previous trx must be tracked.  Either trx is considered active for the
 * purpose of SO reclamation due to the possibility of a crash roll back.
 *
 * Fields are updated in situ under the read lock, and atomic_assign prevents
 * a torn read if the value straddles a cache line.  There is no race
 * condition for the value because an older trx ID merely delays reclamation.
 */
static bool
trxtrack( )
{

	if (thread) {
		pthread_rwlock_rdlock( &threadlist_lock);
		unless (trx_bracket_id) {
			atomic_assign( &thread->trxcurrent, 0);
			atomic_assign( &thread->trxprevious, 0);
		}
		else unless (trx_bracket_id == thread->trxcurrent) {
			atomic_assign( &thread->trxprevious, thread->trxcurrent);
			atomic_assign( &thread->trxcurrent, trx_bracket_id);
		}
		pthread_rwlock_unlock( &threadlist_lock);
	}
	else if (trx_bracket_id) {
		unless (thread = calloc( 1, sizeof *thread))
			return (FALSE);
		thread->trxcurrent = trx_bracket_id;
		pthread_rwlock_wrlock( &threadlist_lock);
		thread->next = threadlist;
		threadlist = thread;
		pthread_rwlock_unlock( &threadlist_lock);
	}
	return (TRUE);
}


/*
 * return ID of oldest active trx, or ~0 if no trx are active
 */
static trxid_t
trxoldest( )
{

	trxid_t trxid = ~0;
	pthread_rwlock_rdlock( &threadlist_lock);
	thread_t *th = threadlist;
	while (th) {
		if (th->trxcurrent)
			trxid = min( trxid, th->trxcurrent);
		if (th->trxprevious)
			trxid = min( trxid, th->trxprevious);
		th = th->next;
	}
	pthread_rwlock_unlock( &threadlist_lock);
	return (trxid);
}


/*
 * name of the recovery packet for the given cguid
 */
static char	*
trxrevfilename( ZS_cguid_t cguid)
{
	char	*a;

	if (asprintf( &a, "%s/lc-trx-cguid-%u", getProperty_String( "ZS_CRASH_DIR", "/tmp/fdf.crash-recovery"), (uint)cguid) < 0)
		return (0);
	return (a);
}


/*
 * reverted trx IDs for the given container
 */
static trxrev_t	*
trxreversions( container_t *c)
{

	trxrev_t *rev = malloc( sizeof *rev);
	unless (rev)
		return (0);
	rev->vec = 0;
	rev->num = 0;
	uint e = 0;
	FILE *f = fopen( c->trxrevfile, "r");
	unless (f)
		return (rev);
	loop {
		unless (rev->num < e) {
			e = (e+1) * 2;
			unless (rev->vec = realloc( rev->vec, e*sizeof( *rev->vec))) {
				fclose( f);
				return (trxrevfree( rev));
			}
		}
		unless (fscanf( f, "%lu", &rev->vec[rev->num]) == 1) {
			fclose( f);
			return (rev);
		}
		++rev->num;
	}
}


/*
 * deliver recovery packets to all LC
 *
 * The master packet from mcd_rec level is cloned for each existing LC.
 * If an LC has a packet already waiting, then the LC has not recovered
 * from its previous shutdown and a new packet must NOT be delivered for it.
 *
 * Function is called at LC init time, and is crash safe.
 */
static bool
trxrevdistribute( )
{
	cntr_map_t	*cmap;
	uint64_t	cmaplen;
	char		*k;
	uint32_t	kl;
	char		iobuf[1024];

	char *ifile = trxrevfilename( 0);
	unless (ifile)
		return (FALSE);
	FILE *fi = fopen( ifile, "r");
	unless (fi) {
		free( ifile);
		return (TRUE);
	}
	struct cmap_iterator *iterator = zs_cmap_enum( );
	unless (iterator)
		return (FALSE);
	while (zs_cmap_next_enum( iterator, &k, &kl, (char **)&cmap, &cmaplen)) {
		char *ofile = trxrevfilename( cmap->cguid);
		unless (ofile)
			return (FALSE);
		if ((cmap->lc)
		and (access( ofile, F_OK) < 0)) {
			FILE *fo = fopen( ofile, "w");
			unless (fo)
				return (FALSE);
			loop {
				size_t i = fread( iobuf, 1, sizeof iobuf, fi);
				unless (i) {
					if (ferror( fi))
						return (FALSE);
					break;
				}
				fwrite( iobuf, 1, i, fo);
			}
			fflush( fo);
			if ((ferror( fo))
			or (fdatasync( fileno( fo)) < 0))
				return (FALSE);
			fclose( fo);
			rewind( fi);
		}
		free( ofile);
	}
	zs_cmap_finish_enum( iterator);
	fclose( fi);
	if (unlink( ifile) < 0)
		return (FALSE);
	free( ifile);
	return (TRUE);
}


/*
 * Return TRUE if this trx ID was reverted after a crash
 */
static bool
trxreverted( trxrev_t *tr, trxid_t id)
{
	uint	i;

	for (i=0; i<tr->num; ++i)
		if (tr->vec[i] == id)
			return (TRUE);
	return (FALSE);
}


static trxrev_t	*
trxrevfree( trxrev_t *rev)
{

	if (rev) {
		free( rev->vec);
		free( rev);
	}
	return (0);
}


/*
 * allocate an unused slot in the container table
 */
static container_t	*
callocate( )
{
	container_t	*c;

	for (c=ctable; c<ctable+logging_containers_per_instance; ++c)
		unless (c->cguid) {
			c->stream = 0;
			c->streams = 0;
			c->streamhand = 0;
			c->trxrevfile = 0;
			c->syncoff = 0;
			c->syncstatus = ZS_SUCCESS;
			c->insync = 0;
			c->buffull = 0;
			c->percontainerstream = NULL;
			pthread_rwlock_init( &c->lock, 0);
			pthread_mutex_init( &c->synclock, 0);
			pthread_cond_init( &c->synccv, NULL);
			pthread_cond_init( &c->flashcv, NULL);
			pthread_cond_init( &c->fullcv, NULL);
			memset( c->pgtable, 0, sizeof c->pgtable);
			memset( &c->lgstats, 0, sizeof(log_stats_t));
			return (c);
		}
	return (0);
}


static void
cdeallocate( container_t *c)
{

	if (c->trxrevfile)
		free( c->trxrevfile);
	pthread_rwlock_destroy( &c->lock);
	c->cguid = 0;
}


/*
 * return container_t for this cguid
 */
static container_t	*
clookup( ZS_cguid_t cguid)
{
	container_t	*c;

	for (c=ctable; c<ctable+logging_containers_per_instance; ++c)
		if (c->cguid == cguid)
			return (c);
	return (0);
}


/*
 * initialize the stream_t pool for the LC system
 *
 * Called at LC init time.
 */
static ZS_status_t
streaminit( uint n)
{
	uint	i;

	for (i=0; i<n; ++i) {
		stream_t *s = malloc( sizeof *s + bytes_per_so_buffer);
		unless (s)
			return (ZS_OUT_OF_MEM);
		s->next = streamfreelist;
		streamfreelist = s;
		s->stream = 0;
		pthread_mutex_init( &s->buflock, 0);
		s->so.magic = LC_MAGIC;
		s->so.stream = s->stream;
	}
	return (ZS_SUCCESS);
}


/*
 * allocate stream_t from the free pool
 *
 * Caller must assign the stream ID.
 */ 
static stream_t	*
streamalloc( ZS_cguid_t cguid)
{
	stream_t	*s;

	if ((s = streamfreelist)
	and (s->nb = nvalloc( cguid))) {
		streamfreelist = s->next;
		s->seqnext = 0;
		s->seqoldest = 0;
		s->bufnexti = 0;
		s->newest = 0;
		s->pg = 0;
		return (s);
	}
	return (0);
}


/*
 * return stream_t to freelist
 */
static void
streamdealloc( stream_t *s)
{

	s->next = streamfreelist;
	streamfreelist = s;
	s->stream = 0;
	s->so.stream = s->stream;
	nvdealloc( s->nb);
}


static stream_t	*
streamlookup( container_t *c, uint stream)
{
	stream_t	*s;

	for (s=c->streams; s; s=s->next)
		if (s->stream == stream)
			return (s);
	return (0);
}


/*
 * write logging record to a stream
 *
 * This function is the I/O nexus of the LC implementation.  It maintains
 * its own copy of the attached nvr_buffer, updating NVRAM content in
 * parallel; writes to flash when an NVRAM buffer is full; and schedules
 * the reclamation of old SOs.
 */
static ZS_status_t
streamwrite( ts_t *t, stream_t *s, lrec_t *lr, off_t *off)
{
	ZS_status_t	r;
	char		k[100];
	size_t		sz;
	container_t	*c = s->container;

	if (streams_per_container == 1) {
		return streamwrite_one_spc(t, s, lr, off);
	}

	const uint i = lrecsize( lr);
	if (bytes_per_so_buffer < i + s->bufnexti) {
		uint32_t kl = strlen( make_sokey( k, s->stream, s->seqnext));
		r = zs_write_object_lc( t, c->cguid, k, kl, (char *)&s->so, sizeof( s->so)+s->bufnexti);

		unless (r == ZS_SUCCESS) {
			return (r);
		}

		nvr_reset_buffer( s->nb);
		if ((s->seqoldest == s->seqnext)
		and (not sostatus( s, &s->so, s->bufnexti))) {
			return (ZS_FAILURE);
		}

		s->bufnexti = 0;
		++s->seqnext;
		r = streamreclaim( t, s);
		unless (r == ZS_SUCCESS)
			return (r);
	}
	memcpy( s->so.buffer+s->bufnexti, lr, i);
	if (s->bufnexti) {
		sz = nvr_write_buffer_partial( s->nb, &s->so.buffer[s->bufnexti], i, 1, NULL);
		r = (sz == -1) ? ZS_FAILURE: ZS_SUCCESS;
	} else {
		s->so.seqno = s->seqnext;
		sz = nvr_write_buffer_partial( s->nb, (char *)&s->so, sizeof( s->so)+i, 1, NULL);
		r = (sz == -1) ? ZS_FAILURE: ZS_SUCCESS;
	}
	if (r == ZS_SUCCESS)
		s->bufnexti += i;
	return (r);
}

static ZS_status_t
streamwrite_one_spc( ts_t *t, stream_t *s, lrec_t *lr, off_t *off)
{
	ZS_status_t	r;
	char		k[100];
	size_t		sz;
	container_t	*c = s->container;

	plat_assert(streams_per_container == 1);
	plat_assert(off);

	const uint i = lrecsize( lr);
	if (bytes_per_so_buffer < i + s->bufnexti) {

		pthread_mutex_lock(&c->synclock);
		atomic_inc(c->buffull);
		while (atomic_add_get(c->synccount, 0) > 0) {
			pthread_cond_broadcast(&c->synccv);
			pthread_cond_wait(&c->flashcv, &c->synclock);
		}
		pthread_mutex_unlock(&c->synclock);

		uint32_t kl = strlen( make_sokey( k, s->stream, s->seqnext));
		r = zs_write_object_lc( t, c->cguid, k, kl, (char *)&s->so, sizeof( s->so)+s->bufnexti);

		plat_assert(c->synccount == 0);
		pthread_mutex_lock(&c->synclock);
		c->syncstatus = r;
		c->syncoff = 0;
		atomic_dec(c->buffull);
		pthread_cond_broadcast(&c->fullcv);
		//pthread_cond_broadcast(&c->synccv);
		pthread_mutex_unlock(&c->synclock);

		unless (r == ZS_SUCCESS) {
			return (r);
		}

		nvr_reset_buffer( s->nb);
		if ((s->seqoldest == s->seqnext)
		and (not sostatus( s, &s->so, s->bufnexti))) {
			return (ZS_FAILURE);
		}

		s->bufnexti = 0;
		++s->seqnext;
		r = streamreclaim( t, s);
		unless (r == ZS_SUCCESS)
			return (r);
	}
	memcpy( s->so.buffer+s->bufnexti, lr, i);
	if (s->bufnexti) {
		sz = nvr_write_buffer_partial( s->nb, &s->so.buffer[s->bufnexti], i, always_sync, off );
		r = (sz == -1) ? ZS_FAILURE: ZS_SUCCESS;
	} else {
		s->so.seqno = s->seqnext;
		sz = nvr_write_buffer_partial( s->nb, (char *)&s->so, sizeof( s->so)+i, always_sync, off);
		r = (sz == -1) ? ZS_FAILURE: ZS_SUCCESS;
	}
	if (r == ZS_SUCCESS)
		s->bufnexti += i;
	return (r);
}

/*
 * reclaim flash space
 *
 * Determine if oldest SOs of a stream are unused and can be deleted to
 * reduce flash allocated by this LC.  Reclamation rate of checking, R,
 * is an inverse factor of write rate.  A smaller value of R causes more
 * frequent checking: less flash space wasted, more CPU wasted.
 */
static ZS_status_t
streamreclaim( ts_t *t, stream_t *s)
{
	const uint	R		= 10;
	char		sokey[100];

	if (s->seqnext % R)
		return (ZS_SUCCESS);
	char *buffer = malloc( bytes_per_streaming_object);
	unless (buffer)
		return (ZS_OUT_OF_MEM);
	while ((sostatusreclaimable( s))
	and (s->seqoldest+1 < s->seqnext)) {
		uint32_t sokeylen = strlen( make_sokey( sokey, s->stream, s->seqoldest+1));
		char *sodata = buffer;
		uint64_t sodatalen = bytes_per_streaming_object;
		ZS_status_t r = zs_read_object_lc( t, s->container->cguid, sokey, sokeylen, &sodata, &sodatalen, TRUE);
		unless (r == ZS_SUCCESS)
			return (r);
		sokeylen = strlen( make_sokey( sokey, s->stream, s->seqoldest));
		r = zs_delete_object_lc( t, s->container->cguid, sokey, sokeylen);
		unless (r == ZS_SUCCESS)
			return (r);
		++s->seqoldest;
		sostatusclear( s);
		if ((sodatalen < sizeof( so_t))
		or (not sostatus( s, (so_t *)sodata, sodatalen-sizeof( so_t))))
			return (ZS_FAILURE);
	}
	free( buffer);
	return (ZS_SUCCESS);
}


/*
 * number of stream associated with a container
 */
static uint
streamcount( stream_t *s)
{

	uint n = 0;
	while (s) {
		++n;
		s = s->next;
	}
	return (n);
}


/*
 * scan streaming object
 *
 * Called to incrementally rebuild PG state after a restart.
 */
static ZS_status_t
soscan( container_t *c, stream_t *s, trxrev_t *tr, so_t *so, ulong dl)
{
	pghash_t	hash;
	pgname_t	n;
	counter_t	counter;

	uint buflen = dl - sizeof *so;
	ZS_status_t ret = ZS_SUCCESS;
	uint i = 0;
	while (i < buflen) {
		lrec_t *lr = (lrec_t *)&so->buffer[i];
		switch (lr->type) {
		case LR_WRITE:
		case LR_DELETE:
		case LR_TRIM:
			if (trxreverted( tr, lr->trx)) {
				lr->type = LR_TOMBSTONE;
				ret = ZS_OBJECT_DELETED;
				break;
			}
			ZS_status_t r = keydecode( lr->payload, lr->klen, &n, &hash, &counter);
			unless (r == ZS_SUCCESS)
				return (r);
			pg_t *pg = pglookup2( c, s, lr->payload, n, hash, counter);
			unless (pg)
				return (ZS_OUT_OF_MEM);
			if (lr->type == LR_TRIM)
				pg->trimposition = newest( pg->trimposition, counter);
			break;
		}
		i += lrecsize( lr);
	}
	return (ret);
}


/*
 * streaming object disposition
 *
 * Scan the given SO and calculate the static reclamation status.  The status
 * is recorded in the given stream (not necessarily associated with the SO).
 */
static bool
sostatus( stream_t *s, so_t *so, uint buflen)
{
	counter_t	counter;
	pghash_t	h;
	pgname_t	n;

	uint i = 0;
	while (i < buflen) {
		lrec_t *lr = (lrec_t *)&so->buffer[i];
		switch (lr->type) {
		case LR_WRITE:
		case LR_DELETE:
		case LR_TRIM:
			unless (keydecode( lr->payload, lr->klen, &n, &h, &counter) == ZS_SUCCESS)
				return (FALSE);
			pg_t *pg = pglookup( s->container, lr->payload, n, h);
			unless (pg)
				return (FALSE);
			if (pg->aging)
				pg->newest = newest( pg->newest, lr->counter);
			else {
				pg->newest = lr->counter;
				pg->streamlink = s->pg;
				s->pg = pg;
				pg->aging = TRUE;
			}
			s->newest = max( s->newest, lr->trx);
			break;
		}
		i += lrecsize( lr);
	}
	return (TRUE);
}


/*
 * state if SO is reclaimable
 *
 * Based on precalculated status of the oldest SO of the stream, and current
 * status of active trx, return TRUE if that SO can be deleted.
 */
static bool
sostatusreclaimable( stream_t *s)
{

	unless (s->newest < trxoldest( ))
		return (FALSE);
	pg_t *pg = s->pg;
	while (pg) {
		pg_t *pgnext = pg->streamlink;
		unless (pg->newest < newest( 0, pg->trimposition))
			return (FALSE);
		pg = pgnext;
	}
	return (TRUE);
}


/*
 * clear the reclamation status
 */
static void
sostatusclear( stream_t *s)
{

	pg_t *pg = s->pg;
	while (pg) {
		pg_t *pgnext = pg->streamlink;
		pg->streamlink = 0;
		pg->aging = FALSE;
		pg = pgnext;
	}
	s->newest = 0;
	s->pg = 0;
}


/*
 * calculate SO status for all non-empty streams
 *
 * Part of the LC recovery procedure.
 */
static ZS_status_t
sostatusrecover( ts_t *t, container_t *c)
{
	stream_t	*s;
	char		sokey[100],
			*buffer1;

	unless (buffer1 = malloc( bytes_per_streaming_object))
		return (ZS_OUT_OF_MEM);
	for (s=c->streams; s; s=s->next)
		unless (s->seqoldest == s->seqnext) {
			uint32_t sokeylen = strlen( make_sokey( sokey, s->stream, s->seqoldest));
			char *sodata = buffer1;
			uint64_t sodatalen = bytes_per_streaming_object;
			ZS_status_t r = zs_read_object_lc( t, s->container->cguid, sokey, sokeylen, &sodata, &sodatalen, TRUE);
			unless (r)
				return (r);
			if ((sodatalen < sizeof( so_t))
			or (not sostatus( s, (so_t *)sodata, sodatalen-sizeof( so_t))))
				return (ZS_FAILURE);
		}
	free( buffer1);
	return (ZS_SUCCESS);
}


/*
 * read a logging object
 *
 * Given the LO key, map to PG, process the associated stream, and return
 * the current value.  This function uses limited memory, but is slow.
 */
static ZS_status_t
pgstreamscan( ts_t *t, pg_t *pg, char *k, uint32_t kl, char **d, uint64_t *dl)
{
	char	sokey[100];
	uint	buflen;
	char	*buffer,
		*buffer1,
		*buffer2;

	unless (buffer1 = malloc( bytes_per_streaming_object))
		return (ZS_OUT_OF_MEM);
	unless (buffer2 = malloc( bytes_per_streaming_object)) {
		free( buffer1);
		return (ZS_OUT_OF_MEM);
	}
	ZS_status_t ret = ZS_OBJECT_UNKNOWN;
	stream_t *s = pg->stream;
	stream_seqno_t seq = s->seqoldest;
	loop {
		if (seq < s->seqnext) {
			uint32_t sokeylen = strlen( make_sokey( sokey, s->stream, seq));
			char *sodata = buffer1;
			uint64_t sodatalen = bytes_per_streaming_object;
			ZS_status_t r = zs_read_object_lc( t, s->container->cguid, sokey, sokeylen, &sodata, &sodatalen, TRUE);
			unless (r == ZS_SUCCESS) {
				free( buffer2);
				free( buffer1);
				return (r);
			}
			buffer = ((so_t *)buffer1)->buffer;
			if (sodatalen < sizeof( so_t))
				buflen = 0;
			else
				buflen = sodatalen - sizeof( so_t);
		}
		else if (seq == s->seqnext) {
			pthread_mutex_lock( &s->buflock);
			buffer = buffer1;
			memcpy( buffer, s->so.buffer, s->bufnexti);
			buflen = s->bufnexti;
			pthread_mutex_unlock( &s->buflock);
		}
		else
			break;
		uint i = 0;
		while (i < buflen) {
			lrec_t *lr = (lrec_t *)&buffer[i];
			switch (lr->type) {
			case LR_WRITE:
				if (keyequal( lr->payload, lr->klen, k, kl)) {
					counter_t c;
					keydecode( lr->payload, lr->klen, 0, 0, &c);
					unless (trimmed( pg->trimposition, c)) {
						memcpy( buffer2, &lr->payload[lr->klen], lr->dlen);
						*dl = lr->dlen;
						ret = ZS_SUCCESS;
					}
				}
				break;
			case LR_DELETE:
				if (keyequal( lr->payload, lr->klen, k, kl))
					ret = ZS_OBJECT_UNKNOWN;
			}
			i += lrecsize( lr);
		}
		++seq;
	}
	free( buffer1);
	if (ret == ZS_SUCCESS) {
		unless (*d = malloc( *dl)) {
			free( buffer2);
			return (ZS_OUT_OF_MEM);
		}
		memcpy( *d, buffer2, *dl);
	}
	free( buffer2);
	return (ret);
}


/*
 * read all logging objects of a PG
 *
 * Given the LO key, map to PG, process the associated stream, and return
 * the current value of all LOs in the PG.  The returned iterator is used to
 * enumerate the LOs.  Counter is optional (and ignored) for the key, but the
 * '_' separator is required, e.g.  "_thisismyownnameforaplacementgroup".
 * This function uses more memory, but is fast per LO enumerated.
 */
static ZS_status_t
pgstreamenumerate( ts_t *t, pg_t *pg, iter_t *iter)
{
	so_t	*so;
	uint	buflen;

	stream_t *s = pg->stream;
	ZS_status_t ret = ZS_OBJECT_UNKNOWN;
	stream_seqno_t seq = s->seqoldest;
	while (so = malloc( bytes_per_streaming_object)) {
		if (seq < s->seqnext) {
			char sokey[100];
			uint32_t sokeylen = strlen( make_sokey( sokey, s->stream, seq));
			char *sodata = (char *)so;
			uint64_t sodatalen = bytes_per_streaming_object;
			ZS_status_t r = zs_read_object_lc( t, s->container->cguid, sokey, sokeylen, &sodata, &sodatalen, TRUE);
			unless (r == ZS_SUCCESS)
				return (r);
			if (sodatalen < sizeof *so)
				buflen = 0;
			else
				buflen = sodatalen - sizeof *so;
		}
		else if (seq == s->seqnext) {
			pthread_mutex_lock( &s->buflock);
			memcpy( so, &s->so, sizeof( s->so)+s->bufnexti);
			buflen = s->bufnexti;
			pthread_mutex_unlock( &s->buflock);
		}
		else {
			free( so);
			unless (iter->objects = malloc( sizeof( *iter->objects)*iter->nobject))
				return (ZS_OUT_OF_MEM);
			uint n = 0;
			uint i;
			for (i=0; i<LOTABLESIZE; ++i) {
				lo_t *lo;
				for (lo=iter->lo[i]; lo; lo=lo->next)
					iter->objects[n++] = lo->lrec;
			}
			qsort( iter->objects, iter->nobject, sizeof *iter->objects, compar);
			return (ZS_SUCCESS);
		}
		so->next = iter->so;
		iter->so = so;
		uint i = 0;
		while (i < buflen) {
			lrec_t *lr = (lrec_t *)&so->buffer[i];
			counter_t counter;
			pgname_t n;
			switch (lr->type) {
			case LR_WRITE:
				keydecode( lr->payload, lr->klen, &n, 0, &counter);
				if ((pgnameequal( &pg->name, lr->payload, n))
				and (not trimmed( pg->trimposition, counter))) {
					lo_t *lo = iter->lo[counter%LOTABLESIZE];
					loop {
						unless (lo) {
							unless (lo = malloc( sizeof *lo))
								return (ZS_OUT_OF_MEM);
							lo->next = iter->lo[counter%LOTABLESIZE];
							iter->lo[counter%LOTABLESIZE] = lo;
							lo->lrec = lr;
							++iter->nobject;
							break;
						}
						if (keyequal( lo->lrec->payload, lo->lrec->klen, lr->payload, lr->klen)) {
							lo->lrec = lr;
							break;
						}
						lo = lo->next;
					}
				}
				break;
			case LR_DELETE:;
				keydecode( lr->payload, lr->klen, &n, 0, &counter);
				if ((pgnameequal( &pg->name, lr->payload, n))
				and (not trimmed( pg->trimposition, counter))) {
					lo_t *lo = iter->lo[counter%LOTABLESIZE];
					unless (lo)
						break;
					if (keyequal( lo->lrec->payload, lo->lrec->klen, lr->payload, lr->klen)) {
						iter->lo[counter%LOTABLESIZE] = lo->next;
						free( lo);
						--iter->nobject;
						break;
					}
					lo_t *lonext;
					while (lonext = lo->next) {
						if (keyequal( lonext->lrec->payload, lonext->lrec->klen, lr->payload, lr->klen)) {
							lo->next = lonext->next;
							free( lonext);
							--iter->nobject;
							break;
						}
						lo = lonext;
					}
				}
			}
			i += lrecsize( lr);
		}
		++seq;
	}
	return (ZS_OUT_OF_MEM);
}


/*
 * free iterator memory
 */
static void
free_iterator( iter_t *iter)
{
	uint	i;

	so_t *so = iter->so;
	while (so) {
		so_t *so2 = so->next;
		free( so);
		so = so2;
	}
	for (i=0; i<LOTABLESIZE; ++i) {
		lo_t *lo = iter->lo[i];
		while (lo) {
			lo_t *lo2 = lo->next;
			free( lo);
			lo = lo2;
		}
	}
	free( iter->objects);
	free( iter);
}


/*
 * compare PG name fields of two pgnames
 *
 * Return TRUE if they match.  First pgname has its data encapsulated,
 * second has data passed in separate arg.
 */
static bool
pgnameequal( pgname_t *n, char *k1, pgname_t n1)
{

	if ((n->pglen == n1.pglen)
	and (memcmp( &n->buf[n->pgbase], &k1[n1.pgbase], n1.pglen) == 0))
		return (TRUE);
	return (FALSE);
}


/*
 * look up PG info
 *
 * Called during normal LC operations.  If newly encountered, a PG structure
 * is created to track activity.  The PG is assigned to a stream of the
 * container in round-robin fashion.
 */
static pg_t	*
pglookup( container_t *c, char *k, pgname_t n, pghash_t hash)
{

	stream_t *s = c->streamhand;
	unless (s)
		return (0);
	pg_t *pg = c->pgtable[hash%nel( c->pgtable)];
	while (pg) {
		if ((pg->hash == hash)
		and (pgnameequal( &pg->name, k, n)))
			return (pg);
		pg = pg->containerlink;
	}
	unless (pg = malloc( sizeof( *pg)+n.len))
		return (0);
	pg->name = n;
	memcpy( pg->name.buf, k, n.len);
	pg->hash = hash;
	const uint i = hash % nel( c->pgtable);
	pg->containerlink = c->pgtable[i];
	c->pgtable[i] = pg;
	unless (c->streamhand = s->next)
		c->streamhand = c->streams;
	pg->stream = s;
	pg->streamlink = 0;
	pg->aging = FALSE;
	pg->trimposition = COUNTER_UNKNOWN;
	pg->newest = 0;
	return (pg);
}


/*
 * look up PG info
 *
 * Like pglookup(), but does not allocate new PGs.  Used in fast-write mode.
 */
static pg_t	*
pglookup_readonly( container_t *c, char *k, pgname_t n, pghash_t hash)
{

	pg_t *pg = c->pgtable[hash%nel( c->pgtable)];
	while (pg) {
		if ((pg->hash == hash)
		and (pgnameequal( &pg->name, k, n)))
			return (pg);
		pg = pg->containerlink;
	}
	return (pg);
}


/*
 * look up PG info
 *
 * Like pglookup(), but called during container recovery only.
 */
static pg_t	*
pglookup2( container_t *c, stream_t *s, char *k, pgname_t n, pghash_t hash, counter_t counter)
{

	pg_t *pg = c->pgtable[hash%nel( c->pgtable)];
	while (pg) {
		if ((pg->hash == hash)
		and (pgnameequal( &pg->name, k, n)))
			return (pg);
		pg = pg->containerlink;
	}
	unless (pg = malloc( sizeof( *pg)+n.len))
		return (0);
	pg->name = n;
	memcpy( pg->name.buf, k, n.len);
	pg->hash = hash;
	const uint i = hash % nel( c->pgtable);
	pg->containerlink = c->pgtable[i];
	c->pgtable[i] = pg;
	pg->stream = s;
	pg->streamlink = 0;
	pg->aging = FALSE;
	pg->trimposition = COUNTER_UNKNOWN;
	pg->newest = 0;
	return (pg);
}


/*
 * initialize NVRAM usage
 *
 * NVRAM is fully allocated, and indexed by existing container assignment.
 *
 * Called at LC init time.
 */
static ZS_status_t
nvinit( )
{
	uint	i;
	char	*d;
	int	dl;

	nvtablen = nvr_buffer_count( );
	unless (nvtab = calloc( nvtablen, sizeof *nvtab))
		return (ZS_OUT_OF_MEM);
	for (i=0; i<nvtablen; ++i) {
		nvr_buffer_t *nb = nvr_alloc_buffer( );
		ZS_status_t r = nvr_read_buffer( nb, &d, &dl);
		unless (r == ZS_SUCCESS)
			return (r);
		so_t *so = (so_t *)d;
		if ((dl)
		and (so)
		and (sizeof *so < dl)
		and (so->magic == LC_MAGIC))
			nvtab[i].cguid = so->cguid;
		else
			nvr_reset_buffer( nb);
		free( d);
		nvtab[i].nb = nb;
	}
	return (ZS_SUCCESS);
}


/*
 * flush NVRAM content for this container to flash
 *
 * All NVRAM-resident SOs with this cguid are written to flash, and the
 * respective NVRAM buffers reset.
 */
static ZS_status_t
nvflush( ts_t *t, ZS_cguid_t cguid)
{
	uint	i;

	for (i=0; i<nvtablen; ++i) {
		if (nvtab[i].cguid == cguid) {
			char *d;
			int dl;
			ZS_status_t r = nvr_read_buffer( nvtab[i].nb, &d, &dl);
			unless (r == ZS_SUCCESS)
				return (r);
			so_t *so = (so_t *)d;
			unless ((dl)
			and (so)
			and (sizeof *so < dl)
			and (so->magic == LC_MAGIC)
			and (so->cguid == cguid))
				return (ZS_FAILURE);
			char sokey[100];
			uint64_t sokeylen64 = strlen( make_sokey( sokey, so->stream, so->seqno));
			r = zs_write_object_lc( t, cguid, sokey, sokeylen64, d, dl);
			unless (r == ZS_SUCCESS)
				return (r);
			free( d);
			nvr_reset_buffer( nvtab[i].nb);
			nvtab[i].cguid = 0;
		}
	}
	return (ZS_SUCCESS);
}


/*
 * return nvr_buffer
 *
 * An unused NVRAM buffer is assigned to cguid, and returned.
 */
static nvr_buffer_t	*
nvalloc( ZS_cguid_t cguid)
{
	uint	i;

	for (i=0; i<nvtablen; ++i) {
		unless (nvtab[i].cguid) {
			nvtab[i].cguid = cguid;
			nvr_reset_buffer(nvtab[i].nb);
			return (nvtab[i].nb);
		}
	}
	return (0);
}


/*
 * make nvr_buffer_t available
 */
static void
nvdealloc( nvr_buffer_t *nb)
{
	uint	i;

	for (i=0; i<nvtablen; ++i)
		if (nvtab[i].nb == nb) {
			nvtab[i].cguid = 0;
			return;
		}
}


/*
 * number of bytes in the lrec_t (a struct of dynamic size)
 */
static uint
lrecsize( lrec_t *lr)
{

	return (sizeof *lr - sizeof lr->payload + lr->klen + lr->dlen);
}


/*
 * construct provided lrec_t from supplied args
 *
 * Also included is the current trx ID, if any.  Return FALSE for exceeded size.
 */
static bool
lrecbuild( lrec_t *lr, uint type, char *k, uint32_t kl, char *d, uint64_t dl, counter_t counter)
{

	if (sizeof lr->payload < kl+dl)
		return (FALSE);
	lr->type = type;
	lr->klen = kl;
	lr->dlen = dl;
	lr->trx = trx_bracket_id;
	lr->counter = counter;
	memcpy( lr->payload, k, kl);
	memcpy( lr->payload+kl, d, dl);
	return (TRUE);
}


/*
 * decode incoming key
 *
 * Syntax is counter_pg_other where "counter" is a digit string, '_' is a
 * separator, "pg" is the placement group, and "other" is that part of the
 * key not used for group operations (enumeration, trim).
 *
 * Heed return status for keys fresh from the client.
 */
static ZS_status_t
keydecode( char *k, uint32_t kl, pgname_t *n, pghash_t *hash, counter_t *counter)
{

	unless (kl < bytes_per_logging_object)
		return (ZS_KEY_TOO_LONG);
	pghash_t h = 1;
	counter_t c = 0;
	uint ki = 0;
	uint ni = 0;
	while (ki < kl) {
		if (k[ki] == '_') {
			uint pi = ++ki;
			while (ki < kl) {
				if (k[ki] == '_') {
					if (n) {
						n->pgbase = pi;
						n->pglen = ki - pi;
						n->len = kl;
					}
					if (hash)
						*hash = h;
					if (counter)
						*counter = c;
					return (ZS_SUCCESS);
				}
				h = h*1021+k[ki]+67 ^ h>>8;
				++ki;
			}
			if (n) {
				n->pgbase = pi;
				n->pglen = ki - pi;
				n->len = kl;
			}
			if (hash)
				*hash = h;
			if (counter)
				*counter = c;
			return (ZS_SUCCESS);
		}
		unless ('0'<=k[ki] && k[ki]<='9')
			break;
		c = c*10 + k[ki++] - '0';
	}
	return (ZS_BAD_KEY);
}


/*
 * compare two keys
 *
 * Return TRUE if they match.  Leading '0's of the counter field are skipped
 * to ensure a correct comparison.
 */
static bool
keyequal( char *k0, uint kl0, char *k1, uint kl1)
{

	uint i0 = 0;
	uint i1 = 0;
	while ((i0 < kl0)
	and (k0[i0] == '0'))
		++i0;
	while ((i1 < kl1)
	and (k1[i1] == '0'))
		++i1;
	uint n0 = kl0 - i0;
	uint n1 = kl1 - i1;
	if ((n0 == n1)
	and (memcmp( &k0[i0], &k1[i1], n0) == 0))
		return (TRUE);
	return (FALSE);
}


/*
 * generate the standard SO key from the args
 */
static char	*
make_sokey( char k[], uint stream, stream_seqno_t seq)
{

	sprintf( k, "SO_%u_%lu", stream, seq);
	return (k);
}


/*
 * used for enumeration processing
 */
static int
compar( const void *v0, const void *v1)
{
	counter_t	c0,
			c1;
	pgname_t	n0,
			n1;
	int		r;

	lrec_t *lr0 = *(lrec_t **)v0;
	lrec_t *lr1 = *(lrec_t **)v1;
	keydecode( lr0->payload, lr0->klen, &n0, 0, &c0);
	keydecode( lr1->payload, lr1->klen, &n1, 0, &c1);
	uint i0 = n0.len - n0.pgbase;
	uint i1 = n1.len - n1.pgbase;
	if (i0 == i1) {
		r = memcmp( lr0->payload+n0.pgbase, lr1->payload+n1.pgbase, i0);
		if (r == 0) {
			if (c0 < c1)
				return (-1);
			if (c0 == c1)
				return (0);
			return (1);
		}
		return (r);
	}
	r = memcmp( lr0->payload+n0.pgbase, lr1->payload+n1.pgbase, min( i0, i1));
	if (r == 0) {
		if (i0 < i1)
			return (-1);
		return (1);
	}
	return (r);
}


/*
 * counter lies in the trimmed region
 *
 * Handles counters with unknown value.
 */
static bool
trimmed( counter_t trimpos, counter_t c)
{

	return (trimpos!=COUNTER_UNKNOWN && c<=trimpos);
}


/*
 * return the newest (numerically largest) counter of two
 *
 * Handles counters with unknown value.
 */
static counter_t
newest( counter_t a, counter_t b)
{

	if (a == COUNTER_UNKNOWN)
		if (b == COUNTER_UNKNOWN)
			return (a);
		else
			return (b);
	else
		if (b == COUNTER_UNKNOWN)
			return (a);
		else
			return (max( a, b));
}


ZS_status_t
get_lc_num_objs(ZS_cguid_t cguid, uint64_t *num_objs)
{
	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless (c) {
		pthread_rwlock_unlock( &lc_lock);
		*num_objs = 0;
		return (ZS_FAILURE);
	}
	*num_objs = c->lgstats.stat[LOGSTAT_NUM_OBJS];
	pthread_rwlock_unlock( &lc_lock);
	return ZS_SUCCESS;
}

ZS_status_t
get_log_container_stats(ZS_cguid_t cguid, ZS_stats_t *stats)
{
	pthread_rwlock_rdlock( &lc_lock);
	container_t *c = clookup( cguid);
	unless (c) {
		pthread_rwlock_unlock( &lc_lock);
		return (ZS_FAILURE);
	}
	for (int i = 0; i < N_LOGSTATS; i++) {
		if( log_to_zs_stats_map[i].ftype == ZS_STATS_TYPE_APP_REQ ) {
			stats->n_accesses[log_to_zs_stats_map[i].fstat] = c->lgstats.stat[i];
		}
		else if(log_to_zs_stats_map[i].ftype == ZS_STATS_TYPE_BTREE ) {
			stats->btree_stats[log_to_zs_stats_map[i].fstat] = c->lgstats.stat[i];
		}
		else {
			fprintf(stderr,"Invalid zs type(%lu) for btree stats:%d\n",log_to_zs_stats_map[i].ftype,i);
		}
	}
	pthread_rwlock_unlock( &lc_lock);
	return (ZS_SUCCESS);
}


static void
dumpparams( )
{
#if 0
	msg( INITIAL, INFO, "Nominal sizes for Logging Container subsystem:");
	msg( INITIAL, INFO, "bytes_per_NVR = %s", prettynumber( bytes_per_NVR));
	msg( INITIAL, INFO, "bytes_per_streaming_object = %s", prettynumber( bytes_per_streaming_object));
	msg( INITIAL, INFO, "placement_groups_per_flash_array = %s", prettynumber( placement_groups_per_flash_array));
	msg( INITIAL, INFO, "bytes_per_stream_buffer = %s", prettynumber( bytes_per_stream_buffer));
	msg( INITIAL, INFO, "streams_per_flash_array = %s", prettynumber( streams_per_flash_array));
	msg( INITIAL, INFO, "placement_groups_per_stream = %s", prettynumber( placement_groups_per_stream));
	msg( INITIAL, INFO, "bytes_of_stream_buffers = %s", prettynumber( bytes_of_stream_buffers));
#endif
}


/*
 * convert number to a pretty string
 *
 * String is returned in a static buffer.  Powers with base 1024 are defined
 * by the International Electrotechnical Commission (IEC).
 */
#if 0
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


static uint
log2i( ulong l)
{
	return (bitsof( l) - __builtin_clzl( l) - 1);
}


static ulong
power_of_two_roundup( ulong l)
{
	return (log2i( l-1) + 1);
}
#endif

#if 	0//Rico - debug
static void
streamdump( )
{
	uint    i;

	uint n = nvr_buffer_count( );
	nvr_buffer_t **v = malloc( n*sizeof( *v));
	printf( "streamdump:");
	for (i=0; i<n; ++i) {
		bool empty = TRUE;
		v[i] = nvr_alloc_buffer( );
		if (v[i] == NULL) {
			printf( "\n streamdump: nvr_alloc_buffer failed\n");
			return;
		}
		char *d;
		int dl;
		ZS_status_t r = nvr_read_buffer( v[i], &d, &dl);
		unless (r == ZS_SUCCESS) {
			printf( "\n streamdump: nvr_read_buffer failed (%s)\n", ZSStrError( r));
			return;
		}
		if (dl)
			empty = FALSE;
		else
			d = "";
		printf( " (%.7s)%d:", d, dl);

		const uint N = 30000;
		d = malloc( N);
		sprintf( d, "fred%03u", i);
		r = nvr_write_buffer_partial( v[i], d, N);
		unless (r == ZS_SUCCESS) {
			printf( "streamdump: nvr_write_buffer_partial failed (%s)\n", ZSStrError( r));
			return;
		}
		r = nvr_read_buffer( v[i], &d, &dl);
		unless (r == ZS_SUCCESS) {
			printf( "\n streamdump: nvr_read_buffer failed (%s)\n", ZSStrError( r));
			return;
		}
		printf( "(%.7s)%d", d, dl);
		unless (empty)
			nvr_reset_buffer( v[i]);
	}
	printf( "\n");
}
#endif
