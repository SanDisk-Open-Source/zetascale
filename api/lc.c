/*
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
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
#include	"ssd/fifo/mcd_osd.h"
#include 	"utils/properties.h"
#include	"platform/logging.h"
#include	"utils/rico.h"
#include 	"common/sdftypes.h"


/* resource calculations for one ZS instance
 */
#define		NVR_DEFAULT_OFFSET				0
#define		NVR_DEFAULT_LENGTH				(0x1ULL << 24)
#define		NVR_DEFAULT_STREAM_OBJ_SIZE		65536
#define		NVR_DEFAULT_BLOCK_SIZE			4096


#define bytes_per_NVR						nvr_len
#define bytes_per_streaming_object			nvr_objsize
#define placement_groups_per_flash_array	(1L << 13)
#define bytes_per_stream_buffer				nvr_blksize
#define streams_per_flash_array			(bytes_per_NVR / bytes_per_streaming_object)
#define placement_groups_per_stream		(placement_groups_per_flash_array / streams_per_flash_array)
#define bytes_of_stream_buffers			(streams_per_flash_array * bytes_per_stream_buffer)

#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define DEBUG			PLAT_LOG_LEVEL_DEBUG
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)

int 				nvr_fd = -1;
int 				nvr_oflags = O_RDWR;
uint64_t			nvr_off = 0;	
uint64_t			nvr_len, partition_size;
int				nvr_data_in_obj, nvr_objsize;
int				nvr_blksize;
int				nvr_numobjs;
int				n_partitions = 1, hw_durable = 1;
/*
 * designated writer must use atomic assignment when sharing scalers
 */
#define	atomic_assign( dst, val)	((void) __sync_add_and_fetch( (dst), (val)-(*(dst))))

typedef struct logging_container {
	pthread_mutex_t				lc_mutex;
	pthread_cond_t				lc_sync_cond;
	ZS_cguid_t				lc_cguid;
	uint64_t				lc_lsn;
	uint64_t				lc_key;
	struct nvr_stream_object		*lc_nso;
	int					lc_partition;
} lc_t;

typedef struct nvr_stream_buffer {
	uint64_t				nsb_cksum;
	uint64_t				nsb_lsn;
	int					nsb_off;
} nvrsbuf_t;

#define NSB_METASIZE		((size_t)sizeof(nvrsbuf_t))

typedef struct nvr_stream_object {
	off_t				nso_fileoff;
	off_t				nso_off, nso_syncoff;
	ZS_cguid_t			nso_cguid;
	pthread_mutex_t			nso_mutex, nso_sync_mutex;
	pthread_cond_t			nso_sync_cond;
	char				*nso_data;
	int				nso_refcnt, nso_insync;
	struct nvr_stream_object	*nso_nxtfree;
} nvrso_t;

nvrso_t			*nvr_objs, *nvr_flhead, *nvr_fltail;
char			*nvr_databuf;

pthread_cond_t  nvr_fl_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t nvr_fl_mutex = PTHREAD_MUTEX_INITIALIZER;

static void	dumpparams( );

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
static off_t NVR_buf(lc_t *lc, char *key, int keylen, char *data, int datalen);
static int NVR_sync_buf(lc_t *lc, off_t off);
static ZS_status_t NVR_commit_obj(lc_t *lc, nvrso_t *actso, uint64_t key);
static ZS_status_t NVR_init(void);


void
lc_init( )
{
	(void)NVR_init();
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
			plat_abort(0);
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
	msg( INITIAL, INFO, "bytes_per_NVR = %s", prettynumber( bytes_per_NVR));
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


ZS_status_t
LC_init(void **lc_arg, ZS_cguid_t cguid)
{
	lc_t	*lc;

	lc = (lc_t *)plat_malloc(sizeof(lc_t));
	if (lc == NULL) {
		*lc_arg = NULL;
		return ZS_FAILURE;
	} else {
		lc->lc_nso = NULL;
		lc->lc_cguid = cguid;
		lc->lc_lsn = 1;
		lc->lc_key = 1;
		lc->lc_partition = 0;
		pthread_mutex_init(&lc->lc_mutex, NULL);
		pthread_cond_init(&lc->lc_sync_cond, NULL);
	}

	*lc_arg = lc;
	msg(INITIAL, INFO, "Logging container intitialized for %"PRId64, cguid);
	return ZS_SUCCESS;
}

static ZS_status_t
NVR_init(void)
{
	char	*NVR_file;

	NVR_file = (char *)getProperty_String("ZS_NVR_FILENAME", NULL);

	if (NVR_file == NULL)	{
		return ZS_SUCCESS;
	} else {
		nvr_fd = open(NVR_file, nvr_oflags, 0600);
		if (nvr_fd == -1) {
			perror("open");
			msg(INITIAL, FATAL, 
					"Failed to open NVR file %s\n", NVR_file);
			return ZS_FAILURE;
		}
	}

	nvr_off = (off_t)getProperty_Int("ZS_NVR_OFFSET", NVR_DEFAULT_OFFSET);
	if (lseek(nvr_fd, nvr_off, SEEK_SET) == -1) {
		perror("lseek");
		msg(INITIAL, FATAL,
				"Failed to seek to offset %d on NVR file %s", 
								(int)nvr_off, NVR_file);
		return ZS_FAILURE;
	}

	nvr_len = getProperty_uLongLong("ZS_NVR_LENGTH", NVR_DEFAULT_LENGTH);
	if (nvr_len <= 0) {
		msg(INITIAL, FATAL,
				"Not valid length (%d) specified for NVR file %s\n", 
								(int)nvr_len, NVR_file);
		return ZS_FAILURE;
	}
	partition_size = nvr_len / n_partitions;

	nvr_objsize = (off_t)getProperty_Int("ZS_NVR_OBJECT_SIZE", NVR_DEFAULT_STREAM_OBJ_SIZE);
	n_partitions = getProperty_Int("ZS_NVR_PARTITIONS", 1);

	nvr_numobjs = nvr_len / nvr_objsize / n_partitions;
	nvr_objs = (nvrso_t *)plat_malloc(sizeof(nvrso_t) * nvr_numobjs);
	if (nvr_objs == NULL) {
		return ZS_FAILURE;
	}
	memset(nvr_objs, 0, sizeof(nvrso_t) * nvr_numobjs);

	nvr_databuf = (char *)plat_malloc(nvr_objsize * nvr_numobjs);
	if (nvr_databuf == NULL) {
		return ZS_FAILURE;
	}
	memset(nvr_databuf, 0, nvr_objsize * nvr_numobjs);

	for (int i = 0; i < nvr_numobjs; i++) {
		nvr_objs[i].nso_fileoff = nvr_off + i * nvr_objsize;
		nvr_objs[i].nso_data = (char *)(nvr_databuf + i * nvr_objsize);
		nvr_objs[i].nso_nxtfree = NULL;
		pthread_mutex_init(&(nvr_objs[i].nso_mutex), NULL);
		pthread_mutex_init(&(nvr_objs[i].nso_sync_mutex), NULL);
		pthread_cond_init(&(nvr_objs[i].nso_sync_cond), NULL);
	}
	
	nvr_flhead = &nvr_objs[0];
	nvr_fltail = &nvr_objs[nvr_numobjs-1];
	for (int i = 0; i < nvr_numobjs - 1; i++) {
		nvr_objs[i].nso_nxtfree = &nvr_objs[i + 1];
	}

	nvr_blksize = (off_t)getProperty_Int("ZS_NVR_BLOCK_SIZE", NVR_DEFAULT_BLOCK_SIZE);
	nvr_data_in_obj = nvr_objsize - sizeof( mcd_osd_meta_t) - sizeof( uint64_t);

	dumpparams( );

	return ZS_SUCCESS;
}	

/*
 * Allocate a stream object from the list
 */
static nvrso_t *
NVR_obj_alloc(ZS_cguid_t cguid)
{
	nvrso_t		*obj;

	pthread_mutex_lock(&nvr_fl_mutex);
	while (nvr_flhead == NULL) {
		 pthread_cond_wait(&nvr_fl_cv, &nvr_fl_mutex);
	}
	obj = nvr_flhead;
	nvr_flhead = nvr_flhead->nso_nxtfree;
	obj->nso_nxtfree = NULL;
	obj->nso_off = obj->nso_syncoff = nvr_blksize;
	//obj->nso_off = obj->nso_syncoff = 0;
	obj->nso_cguid = cguid;
	obj->nso_refcnt = 1;
	obj->nso_insync = 0;
	//obj->nso_buf = obj->nso_data;

	if (nvr_fltail == obj) {
		nvr_fltail = NULL;
		plat_assert(nvr_flhead == NULL);
	}
	pthread_mutex_unlock(&nvr_fl_mutex);

	return obj;
}

/*
 * Free the stream object from the list
 */
static void
NVR_obj_free(nvrso_t *obj)
{
	pthread_mutex_lock(&nvr_fl_mutex);
	obj->nso_nxtfree = NULL;
	if (nvr_fltail == NULL) {
		plat_assert(nvr_flhead == NULL);
		nvr_fltail = obj;
		nvr_flhead = obj;
		pthread_cond_broadcast(&nvr_fl_cv);
	} else {
		nvr_fltail->nso_nxtfree = obj;
	}
	pthread_mutex_unlock(&nvr_fl_mutex);
}

/*
 * Make sure the current object has space for key/value. If there is
 * no space, flush the current one and allocate new.
 */
static void
NVR_ensure_space_in_obj(lc_t *lc, int keylen, int datalen)
{
	nvrso_t			*so;
	size_t			remspace, tocpy;
	off_t			nso_off;
	nvrso_t			*actnso;
	uint64_t		key;

retry:
	so = lc->lc_nso;

	nso_off = so->nso_off;
	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	remspace = nvr_blksize - nso_off % nvr_blksize;
	if (remspace < sizeof(keylen) + sizeof(datalen)) {
		nso_off +=  remspace;
		goto retry;
	}
	nso_off += sizeof(keylen);
	nso_off += sizeof(datalen);

	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	while (keylen > 0) {
		tocpy = min(keylen, nvr_blksize - nso_off % nvr_blksize);
		keylen -= tocpy;
		nso_off += tocpy;
		if (keylen) {
			nso_off += NSB_METASIZE;
		}
	}
	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	while (datalen > 0) {
		tocpy = min(datalen, nvr_blksize - nso_off % nvr_blksize);
		datalen -= tocpy;
		nso_off += tocpy;
		if (datalen) {
			nso_off += NSB_METASIZE;
		}
	}

	if (nso_off  > nvr_data_in_obj) {
		actnso = lc->lc_nso;
		lc->lc_nso = NULL;
		key = lc->lc_key++;
		pthread_mutex_unlock(&lc->lc_mutex);

		NVR_commit_obj(lc, actnso, key);
		NVR_obj_free(actnso);

		pthread_mutex_lock(&lc->lc_mutex);
		if (lc->lc_nso == NULL) {
			lc->lc_nso = NVR_obj_alloc(lc->lc_cguid);
		}
		goto retry;
	}
}

ZS_status_t
NVR_write(void *lc_arg, char *key, int keylen, char *data, int datalen)
{
	lc_t		*lc = (lc_t *)lc_arg;
	off_t		offset;

	pthread_mutex_lock(&lc->lc_mutex);

	if (lc->lc_nso == NULL) {
		lc->lc_nso = NVR_obj_alloc(lc->lc_cguid);
	}
	//actnso->nso_refcnt++;

	NVR_ensure_space_in_obj(lc, keylen, datalen);

	plat_assert(lc->lc_nso);

	offset = NVR_buf(lc, key, keylen, data, datalen);
	pthread_mutex_unlock(&lc->lc_mutex);

	NVR_sync_buf(lc, offset);
	pthread_mutex_unlock(&lc->lc_mutex);

	return ZS_SUCCESS;
}

/*
 * Appnd the key/value in to current streaming object.
 */
static off_t
NVR_buf(lc_t *lc, char *key, int keylen, char *data, int datalen)
{
	nvrso_t			*so;
	size_t			tocpy;
	char 			*tmp, *nso_data;
	int 			remspace;
	off_t			nso_off;

	so = lc->lc_nso;
	nso_off = so->nso_off;
	nso_data = so->nso_data;

retry:

	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	remspace = nvr_blksize - nso_off % nvr_blksize;
	if (remspace < sizeof(keylen) + sizeof(datalen)) {
		memset(nso_data + nso_off, 0, remspace);
		nso_off +=  remspace;
		goto retry;
	}
	memcpy(nso_data + nso_off, &keylen, sizeof(keylen));
	nso_off += sizeof(keylen);

	memcpy(nso_data + nso_off, &datalen, sizeof(datalen));
	nso_off += sizeof(datalen);

	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	tmp = key;	
	while (keylen > 0) {
		tocpy = min(keylen, nvr_blksize - nso_off % nvr_blksize);
		memcpy(nso_data + nso_off, tmp, tocpy);
		keylen -= tocpy;
		tmp += tocpy;
		nso_off += tocpy;
		if (keylen) {
			nso_off += NSB_METASIZE;
		}
	}

	if (nso_off % nvr_blksize == 0) {
		nso_off += NSB_METASIZE;
	}

	tmp = data;	
	while (datalen > 0) {
		tocpy = min(datalen, nvr_blksize - nso_off % nvr_blksize);
		memcpy(nso_data + nso_off, tmp, tocpy);
		datalen -= tocpy;
		tmp += tocpy;
		nso_off += tocpy;
		if (datalen) {
			nso_off += NSB_METASIZE;
		}
	}

	so->nso_off = nso_off;
	return nso_off;
}

/*
 * Make sure the key/value is written to NVRAM
 */
static int
NVR_sync_buf(lc_t *lc, off_t off)
{
	nvrso_t		*so;
	nvrsbuf_t	*sbuf;
	off_t			tmp_syncoff, write_off;
	size_t		write_len;
	int ret, partition;
	

	so = lc->lc_nso;

	if (so->nso_syncoff >= off) {
		return 0;
	}

	pthread_mutex_lock(&so->nso_sync_mutex);
	while (so->nso_insync && so->nso_syncoff < off) {
		pthread_cond_wait(&so->nso_sync_cond, &so->nso_sync_mutex);
	}

	if (so->nso_syncoff >= off) {
		pthread_mutex_unlock(&so->nso_sync_mutex);
		return 0;
	}

	plat_assert(!so->nso_insync);
	so->nso_insync = 1;

	pthread_mutex_unlock(&so->nso_sync_mutex);

	pthread_mutex_lock(&so->nso_mutex);
	tmp_syncoff = so->nso_off;
	pthread_mutex_unlock(&so->nso_mutex);

	write_off = so->nso_syncoff / nvr_blksize * nvr_blksize;
	write_len = (tmp_syncoff + nvr_blksize -1) / nvr_blksize * nvr_blksize - so->nso_syncoff / nvr_blksize * nvr_blksize;

	partition = lc->lc_partition;
	lc->lc_partition = (partition + 1) % n_partitions;

	for (int i = 0; i < write_len; i += nvr_blksize) {
		sbuf = (nvrsbuf_t *)(&so->nso_data[write_off + i]);
		sbuf->nsb_lsn = lc->lc_lsn++;
	}

	off_t offset_to_write = partition * partition_size + so->nso_fileoff + write_off;
	ret = pwrite(nvr_fd, so->nso_data + write_off, write_len, offset_to_write);

	if (hw_durable) {
		ret = fdatasync(nvr_fd);
	}
	pthread_mutex_lock(&so->nso_sync_mutex);
	so->nso_syncoff = tmp_syncoff;
	so->nso_insync = 0;
	pthread_cond_broadcast(&so->nso_sync_cond);
	pthread_mutex_unlock(&so->nso_sync_mutex);

	return ret;
}

static ZS_status_t
NVR_commit_obj(lc_t *lc, nvrso_t *so, uint64_t key)
{
	ZS_status_t		status;
#if 0
	if (so->nso_syncoff < so->nso_off) {
		NVR_sync_buf(lc, so);
	}

	//status = ZSWriteObject(thdstate, key, sizeof(key), so->databuf, nvr_data_in_obj);
#endif

	return status;
}



#if 0
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
