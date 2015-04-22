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
#include	"nvr_svc.h"
#include	"ssd/fifo/mcd_osd.h"
#include 	"utils/properties.h"
#include	"platform/logging.h"
#include	"utils/rico.h"
#include	"utils/hash.h"
#include 	"common/sdftypes.h"
#include	"sdftcp/locks.h"
#include	"ssd/ssd.h"
#include	"api/fdf_internal.h"

/* resource calculations for one ZS instance
 */
#define		NVR_DEFAULT_OFFSET			0
#define		NVR_DEFAULT_LENGTH			(0x1ULL << 24)
#define		NVR_DEFAULT_STREAM_OBJ_SIZE		65536
#define		NVR_DEFAULT_BLOCK_SIZE			4096
#define		NVR_MAX_BLOCK_SIZE			16384
#define		NVR_MAX_BLOCK_SIZE_MASK			0xffffffffffffc000
#define		NVR_DEFAULT_DEVICE			"/tmp/zs_log_container"



#define	logging_containers_per_array		8
#define	streams_per_container			8
#define bytes_per_NVR				nvr_len
#define bytes_per_streaming_object		nvr_objsize
#define placement_groups_per_flash_array	(1L << 13)
#define bytes_per_stream_buffer			nvr_blksize
#define streams_per_flash_array			(bytes_per_NVR / bytes_per_streaming_object)
#define placement_groups_per_stream		(placement_groups_per_flash_array / streams_per_flash_array)
#define bytes_of_stream_buffers			(streams_per_flash_array * bytes_per_stream_buffer)

#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define DEBUG			PLAT_LOG_LEVEL_DEBUG
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	TRACE			PLAT_LOG_LEVEL_TRACE
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)

int 				nvr_fd = -1;
int 				nvr_oflags = O_RDWR | O_CREAT;
uint64_t			nvr_off = 0;	
uint64_t			nvr_len, partition_size;
int				nvr_data_in_obj, nvr_objsize;
int				nvr_blksize;
int				nvr_numobjs;
int				n_partitions = 1, hw_durable = 1;
uint64_t			nvr_lsn = 1;
static int			bytes_per_nvr_buffer;
char				nvr_zero[65536];
int				nvr_disabled = 0;

/* NVRAM stats */
uint64_t			nvr_write_reqs, nvr_write_saved, nvr_sync_reqs;
uint64_t			nvr_data_in, nvr_data_out;
uint64_t			nvr_reset_req, nvr_no_space, nvr_sync_splits, nvr_sync_restarts;
/*
 * designated writer must use atomic assignment when sharing scalers
 */
#define	atomic_assign( dst, val)	((void) __sync_add_and_fetch( (dst), (val)-(*(dst))))

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
	int				nso_refcnt, nso_insync, nso_commit, nso_partition;
	int				nso_writecnt;
	struct nvr_stream_object	*nso_nxtfree;
} nvrso_t;

nvrso_t			*nvr_objs, *nvr_flhead, *nvr_fltail;
char			*nvr_databuf, *nvr_databuf_ua;

pthread_cond_t  nvr_fl_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t nvr_fl_mutex = PTHREAD_MUTEX_INITIALIZER;
static ZS_status_t nvr_rebuild(struct ZS_state *zs_state);
static bool nvr_space_available_in_obj(nvrso_t *so, int count);
static off_t nvr_buf(nvrso_t *so, char *data, uint count, int reset, size_t *sz);
static int nvr_sync_buf_internal(nvrso_t *so, off_t off, int aligned_down, int reseet);
static size_t nvr_write_buffer_internal( nvr_buffer_t *buf, char *data, uint count, int reset, int sync, off_t *off);


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
#endif

int odirect;

ZS_status_t
nvr_init(struct ZS_state *zs_state)
{
	char	*NVR_file;

	if (getProperty_Int("ZS_NVR_ODIRECT", 0)) {
		nvr_oflags |= O_DIRECT;
		odirect = 1;
	}

	NVR_file = (char *)getProperty_String("ZS_NVR_FILENAME", NVR_DEFAULT_DEVICE);

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

	nvr_objsize = (off_t)getProperty_Int("ZS_NVR_OBJECT_SIZE", NVR_DEFAULT_STREAM_OBJ_SIZE);
	n_partitions = getProperty_Int("ZS_NVR_PARTITIONS", 1);

	partition_size = nvr_len / n_partitions;

	nvr_numobjs = nvr_len / nvr_objsize / n_partitions;
	nvr_objs = (nvrso_t *)plat_malloc(sizeof(nvrso_t) * nvr_numobjs);
	if (nvr_objs == NULL) {
		return ZS_FAILURE;
	} memset(nvr_objs, 0, sizeof(nvrso_t) * nvr_numobjs);

	nvr_databuf_ua = (char *)plat_malloc(nvr_objsize * nvr_numobjs + 2 * NVR_MAX_BLOCK_SIZE - 1);
	if (nvr_databuf_ua == NULL) {
		return ZS_FAILURE;
	}
	nvr_databuf =(char *) (((uint64_t)nvr_databuf_ua + NVR_MAX_BLOCK_SIZE - 1) & NVR_MAX_BLOCK_SIZE_MASK);
	memset(nvr_databuf, 0, nvr_objsize * nvr_numobjs);

	for (int i = 0; i < nvr_numobjs; i++) {
		nvr_objs[i].nso_fileoff = nvr_off + i * nvr_objsize;
		nvr_objs[i].nso_data = (char *)(nvr_databuf + i * nvr_objsize);
		nvr_objs[i].nso_nxtfree = NULL;
		nvr_objs[i].nso_off = nvr_objs[i].nso_syncoff = 0;
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

	bytes_per_nvr_buffer = nvr_data_in_obj - (nvr_objsize / nvr_blksize ) * NSB_METASIZE;
	memset(&nvr_zero, 0, sizeof(nvr_zero));

	dumpparams();

	if ( (1 == getProperty_Int("SDF_REFORMAT", 0)) || (1 == getProperty_Int("ZS_REFORMAT", 0))
			|| (1 == getProperty_Int("ZS_NVR_REFORMAT", 0))) {       // Default to recover
		nvr_reset();
		return ZS_SUCCESS;
	} else {
		return nvr_rebuild(zs_state);
	}

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
	//obj->nso_off = obj->nso_syncoff = 0;
	obj->nso_cguid = cguid;
	obj->nso_refcnt = 1;
	obj->nso_insync = 0;
	obj->nso_partition = 0;
	obj->nso_writecnt = 0;

	if (nvr_fltail == obj) {
		nvr_fltail = NULL;
		plat_assert(nvr_flhead == NULL);
	}
	pthread_mutex_unlock(&nvr_fl_mutex);

	return obj;
}

ZS_status_t
nvr_reset_buffer(nvr_buffer_t *buf)
{
	ZS_status_t	ret;
	nvrso_t		*obj = (nvrso_t *)buf;

	plat_assert(obj->nso_nxtfree == NULL);
	plat_assert(obj->nso_refcnt);

	atomic_inc(nvr_reset_req);
	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return ZS_FAILURE;
	}

	obj->nso_off = obj->nso_syncoff = 0;
	if (nvr_write_buffer_internal(buf, nvr_zero, nvr_blksize - NSB_METASIZE , 1, 1, NULL) == -1) {
		ret = ZS_FAILURE;
	}
	obj->nso_off = obj->nso_syncoff = 0;

	return ret;
}

ZS_status_t
nvr_format_buffer(nvr_buffer_t *buf)
{
	ZS_status_t	ret;
	nvrso_t		*obj = (nvrso_t *)buf;

	plat_assert(obj->nso_nxtfree == NULL);
	plat_assert(obj->nso_refcnt);

	atomic_inc(nvr_reset_req);
	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return ZS_FAILURE;
	}

	obj->nso_off = obj->nso_syncoff = 0;
	if (nvr_write_buffer_internal(buf, nvr_zero, bytes_per_nvr_buffer, 1, 1, NULL) == -1) {
		ret = ZS_FAILURE;
	}
	obj->nso_off = obj->nso_syncoff = 0;

	return ret;
}

nvr_buffer_t *
nvr_alloc_buffer()
{
	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return NULL;
	}
	return NVR_obj_alloc(0);
}

/*
 * Free the stream object from the list
 */
static void
NVR_obj_free(nvrso_t *obj)
{
	plat_assert(obj->nso_refcnt == 0);

	pthread_mutex_lock(&nvr_fl_mutex);
	obj->nso_nxtfree = NULL;
	obj->nso_commit = 0;
	if (nvr_fltail == NULL) {
		plat_assert(nvr_flhead == NULL);
		nvr_fltail = obj;
		nvr_flhead = obj;
		pthread_cond_broadcast(&nvr_fl_cv);
	} else {
		nvr_fltail->nso_nxtfree = obj;
		nvr_fltail = obj;
	}
	pthread_mutex_unlock(&nvr_fl_mutex);
}
void
nvr_buffer_release(nvr_buffer_t *buf)
{
	nvrso_t *so = (nvrso_t *)buf;
	int refcnt;

	plat_assert(so->nso_nxtfree == NULL);
	refcnt = atomic_dec_get(so->nso_refcnt);
	if (!refcnt) {
		NVR_obj_free(so);
	}
}
void
nvr_free_buffer( nvr_buffer_t *buf)
{
	nvr_buffer_release(buf);
}


void
nvr_buffer_hold(nvr_buffer_t *buf)
{
	nvrso_t *so = (nvrso_t *)buf;
	plat_assert(so->nso_nxtfree == NULL);
	(void)atomic_inc_get(so->nso_refcnt);
}


ZS_status_t
nvr_read_buffer(nvr_buffer_t *buf, char **out, int *len)
{
	nvrso_t			*so = (nvrso_t *)buf;
	off_t			off = 0;
	off_t			dstoff = 0;
	uint64_t		seq = 0;
	char			*data = NULL;
	int			numblks = nvr_objsize / nvr_blksize;

	if (so->nso_off == 0) {
		goto out;
	}
	data = (char *)malloc(bytes_per_nvr_buffer);
	if (data == NULL) {
		msg(INITIAL, DEBUG, "malloc failed");
		return ZS_FAILURE;
	}

	plat_assert(so->nso_nxtfree == NULL);

	for (int i = 0; i < numblks ; i++, off += nvr_blksize) {
		nvrsbuf_t	*sb = (nvrsbuf_t *)(so->nso_data + off);

		if (sb->nsb_off == 0) {
			break;
		}

		if (sb->nsb_lsn > seq) {
			memcpy(data + dstoff, so->nso_data + off + NSB_METASIZE, sb->nsb_off - NSB_METASIZE);
			dstoff += sb->nsb_off - NSB_METASIZE;
			seq = sb->nsb_lsn;
		} else {
			break;
		}
	}
out:
	*len = dstoff;
	*out = data;

	return ZS_SUCCESS;
}
static int
compar( const void *v0, const void *v1)
{
	nvrsbuf_t *sb;
	uint64_t c0, c1;

	nvrso_t *s0 = *(nvrso_t **)v0;
	nvrso_t *s1 = *(nvrso_t **)v1;

	sb = (nvrsbuf_t *)(s0->nso_data + nvr_blksize);
	c0 = sb->nsb_lsn;
	sb = (nvrsbuf_t *)(s1->nso_data + nvr_blksize);
	c1 = sb->nsb_lsn;
	if (c0 < c1)
		return (-1);
	if (c0 == c1)
		return (0);
	return (1);
}

static ZS_status_t
nvr_rebuild(struct ZS_state *zs_state)
{
	size_t			bytes;
	char			*tmpbuf;
	size_t			sz = nvr_numobjs * nvr_objsize;
	int			i, numblks = nvr_objsize / nvr_blksize, remspace;
	char 			*b1, *b2;
	char			*key, *data;
	nvrsbuf_t		*sb;
	uint64_t		cksum;
	struct ZS_thread_state	*zs_thread_state = NULL;
	ZS_status_t		status = ZS_FAILURE;
	struct ZS_iterator	*_zs_iterator = NULL;
	uint32_t		keylen;
	uint64_t		datalen, *logkey;
	cntr_map_t		*cmap;
	ZS_cguid_t		cguid;
	ZS_container_props_t	props;
	uint64_t 		maxkey = 0, maxseqno;
	nvrso_t			**objects;

	bytes = pread(nvr_fd, nvr_databuf, sz, nvr_off);
	if (bytes < sz) {
		perror("pread");
		msg(INITIAL, FATAL, "Read from NVRAM failed");
		return ZS_FAILURE;
	}

	if (n_partitions == 2) {
		tmpbuf = (char *)malloc(sizeof(char) * sz);
		bytes = pread(nvr_fd, tmpbuf, sz, nvr_off + sz);
		if (bytes < sz) {
			perror("pread");
			msg(INITIAL, FATAL, "Read from NVRAM failed");
			return ZS_FAILURE;
		}

		for (i = 0; i < nvr_numobjs; i++) {
			b1 = (char *)(nvr_databuf + i * nvr_objsize);
			b2 = (char *)(tmpbuf + i * nvr_objsize);

			for (int j = 0; j < numblks; j++) {
				nvrsbuf_t	*s1, *s2;
				s1 = (nvrsbuf_t *)b1;
				s2 = (nvrsbuf_t *)b2;

				cksum = s1->nsb_cksum;
				s1->nsb_cksum = 0;
				if (cksum != hashb((uint8_t *)s1, nvr_blksize, 0)) {
					msg(INITIAL, FATAL, "cksum mismatch of nvr buffer");
					nvr_disabled = 1;
					return ZS_FAILURE;
				}
				cksum = s2->nsb_cksum;
				s2->nsb_cksum = 0;
				if (cksum != hashb((uint8_t *)s2, nvr_blksize, 0)) {
					msg(INITIAL, FATAL, "cksum mismatch of nvr buffer");
					nvr_disabled = 1;
					return ZS_FAILURE;
				}

				msg(INITIAL, DEBUG, "off:%d s1:%"PRId64" s2:%"PRId64, (int)(b1 - nvr_databuf), s1->nsb_lsn, s2->nsb_lsn);
				if (s2->nsb_lsn > s1->nsb_lsn) {
					memcpy(s1, s2, nvr_blksize);
					if (nvr_lsn < s2->nsb_lsn) {
						nvr_lsn = s2->nsb_lsn;
					}
				} else {
					if (nvr_lsn < s1->nsb_lsn) {
						nvr_lsn = s1->nsb_lsn;
					}
				}

				b1 += nvr_blksize;
				b2 += nvr_blksize;

			}
		}
		free(tmpbuf);
	} else {
		for (i = 0; i < nvr_numobjs; i++) {
			b1 = (char *)(nvr_databuf + i * nvr_objsize);

			for (int j = 0; j < numblks; j++) {
				nvrsbuf_t	*s1;
				s1 = (nvrsbuf_t *)b1;

				cksum = s1->nsb_cksum;
				s1->nsb_cksum = 0;
				if (cksum != hashb((uint8_t *)s1, nvr_blksize, 0)) {
					msg(INITIAL, FATAL, "cksum mismatch of nvr buffer");
					nvr_disabled = 1;
					return ZS_FAILURE;
				}

				if (nvr_lsn < s1->nsb_lsn) {
					nvr_lsn = s1->nsb_lsn;
				}

				b1 += nvr_blksize;

			}
		}
	}


	nvr_lsn++;

	for (i = 0; i < nvr_numobjs; i++) {
		nvrso_t		*so = &nvr_objs[i];
		so->nso_off = so->nso_syncoff = 0;
		for (int j = 0; j < numblks ; j++) {
			nvrsbuf_t *sb = (nvrsbuf_t *)(so->nso_data + nvr_blksize * j);
			if (sb->nsb_off) {
				so->nso_off += sb->nsb_off;
				so->nso_syncoff += sb->nsb_off;
			} else {
				break;
			}
		}
	}



	objects = (nvrso_t **)malloc(sizeof(nvrso_t *) * nvr_numobjs);
	for (i = 0; i < nvr_numobjs; i++) {
		objects[i] = &nvr_objs[i];
	}
	qsort(objects, nvr_numobjs, sizeof (*objects), compar); 
	//copy the contents to a buffer to return contents in order

	free(objects);


	return ZS_SUCCESS;
}

size_t
nvr_write_buffer_partial( nvr_buffer_t *buf, char *data, uint count, int sync, off_t *off)
{
	return nvr_write_buffer_internal(buf, data, count, 0, sync, off);
}

int
nvr_sync_buf(nvr_buffer_t *buf, off_t off)
{
	msg(INITIAL, DEBUG, "off: %d syncoff:%d", (int)off, (int)(((nvrso_t *)buf)->nso_syncoff));
	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return -1;
	}
	return nvr_sync_buf_internal((nvrso_t *)buf, off, 0, 0);
}

int
nvr_sync_buf_aligned(nvr_buffer_t *buf, off_t off)
{
	msg(INITIAL, DEBUG, "off: %d syncoff:%d", (int)off, (int)(((nvrso_t *)buf)->nso_syncoff));
	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return -1;
	}
	return nvr_sync_buf_internal((nvrso_t *)buf, off, 1, 0);
}

size_t
nvr_write_buffer_internal( nvr_buffer_t *buf, char *data, uint count, int reset, int sync, off_t *off)
{

	nvrso_t			*so = (nvrso_t *)buf;
	off_t			offset;
	size_t			sz;

	if (nvr_disabled) {
		msg(INITIAL, DEBUG, "NVRAM is disabled");
		return -1;
	}

	atomic_inc(so->nso_writecnt);
	pthread_mutex_lock(&so->nso_mutex);

	if ((offset =nvr_buf(so, data, count, reset, &sz)) == -1) {
		atomic_dec(so->nso_writecnt);
		msg(INITIAL, DEBUG, "Failed to copy contents to NVRAM buffer");
		return -1;
	}
	if (off) {
		*off = offset;
	}

	atomic_dec(so->nso_writecnt);
	pthread_mutex_unlock(&so->nso_mutex);

	if (sync) {
		if (nvr_sync_buf_internal(so, offset, 0, reset) != -1) {
			return sz;
		} else {
			return -1;
		}
	//NVR_obj_rel(so);
	} else {
		return sz;
	}
}

/*
 * Appnd the key/value in to current streaming object.
 */
static off_t
nvr_buf(nvrso_t *so, char *data, uint count, int reset, size_t *sz)
{
	nvrsbuf_t		*sb;	
	size_t			tocpy;
	char 			*tmp, *nso_data;
	off_t			nso_off;

	if (!reset) {
		atomic_inc(nvr_write_reqs);
	}

	if (false == nvr_space_available_in_obj(so, count)) {
		atomic_inc(nvr_no_space);
		return -1;
	}

	if (!reset) {
		atomic_add(nvr_data_in, count);
	}

	nso_off = so->nso_off;
	sb = (nvrsbuf_t *)((char *)so->nso_data + nso_off / nvr_blksize * nvr_blksize);
	nso_data = so->nso_data;
	if (nso_off % nvr_blksize == 0) {
		sb = (nvrsbuf_t *)((char *)so->nso_data + nso_off);
		nso_off += NSB_METASIZE;
	}

	tmp = data;
	while (count > 0) {
		tocpy = min(count, nvr_blksize - nso_off % nvr_blksize);
		memcpy(nso_data + nso_off, tmp, tocpy);
		count -= tocpy;
		tmp += tocpy;
		nso_off += tocpy;
		if (count) {
			sb->nsb_off = reset ? 0 : nvr_blksize;
			sb = (nvrsbuf_t *)((char *)so->nso_data + nso_off);
			nso_off += NSB_METASIZE;
		}
	}

	plat_assert(nso_off <= nvr_data_in_obj);
	if (reset) {
		sb->nsb_off = 0;
	} else {
		sb->nsb_off = nso_off % nvr_blksize ? nso_off % nvr_blksize : nvr_blksize;
		plat_assert(sb->nsb_off);
	}
	*sz = nso_off - so->nso_off;
	so->nso_off = nso_off;
	return nso_off;
}

static bool
nvr_space_available_in_obj(nvrso_t *so, int count)
{
	size_t			tocpy;
	off_t			off;

	off = so->nso_off;
	if (off % nvr_blksize == 0) {
		off += NSB_METASIZE;
	}

	while (count > 0) {
		tocpy = min(count, nvr_blksize - off % nvr_blksize);
		count -= tocpy;
		off += tocpy;
		if (count) {
			off += NSB_METASIZE;
		}
	}

	if (off > nvr_data_in_obj) {
		msg(INITIAL, DEBUG, "space unavailable in buffer (count:%d off:%d)", count, (int)(so->nso_off));
		return false;
	}

	return true;
}

/*
 * Make sure the key/value is written to NVRAM
 */
static int
nvr_sync_buf_internal(nvrso_t *so, off_t off, int aligned_down, int reset)
{
	nvrsbuf_t	*sbuf;
	off_t		tmp_syncoff, write_off;
	size_t		write_len;
	int 		ret, partition;
	int 		cpinprog = 0, flag = 0;
	
	if (!reset) {
		atomic_inc(nvr_sync_reqs);
	}
restart:
	if (off != -1) {
		if (so->nso_syncoff >= off) {
			msg(INITIAL, DEBUG, "saved io: nso_syncoff (%d) >= off (%d)", (int)so->nso_syncoff, (int)off);
			if (!reset) {
				atomic_inc(nvr_write_saved);
			}
			return so->nso_syncoff;
		}
	} else {
		if (so->nso_syncoff == atomic_add_get(so->nso_off, 0)) {
			msg(INITIAL, DEBUG, "saved io: nso_syncoff (%d) >= off (%d)", (int)so->nso_syncoff, (int)off);
			if (!reset) {
				atomic_inc(nvr_write_saved);
			}
			return so->nso_syncoff;
		}
	}


	pthread_mutex_lock(&so->nso_sync_mutex);
	if (off != -1) {
		while (so->nso_insync && so->nso_syncoff < off) {
			//msg(INITIAL, DEBUG, "sync in progress: nso_syncoff (%d) off (%d)", (int)so->nso_syncoff, (int)off);
			pthread_cond_wait(&so->nso_sync_cond, &so->nso_sync_mutex);
		}

		if (so->nso_syncoff >= off) {
			msg(INITIAL, TRACE, "saved io: nso_syncoff (%d) >= off (%d)", (int)so->nso_syncoff, (int)off);
			if (!reset) {
				atomic_inc(nvr_write_saved);
			}
			pthread_mutex_unlock(&so->nso_sync_mutex);
			return so->nso_syncoff;
		}
	} else {
		while (so->nso_insync) {
			pthread_cond_wait(&so->nso_sync_cond, &so->nso_sync_mutex);
		}
		if (so->nso_syncoff == atomic_add_get(so->nso_off, 0)) {
			msg(INITIAL, TRACE, "saved io: nso_syncoff (%d) >= off (%d)", (int)so->nso_syncoff, (int)off);
			if (!reset) {
				atomic_inc(nvr_write_saved);
			}
			pthread_mutex_unlock(&so->nso_sync_mutex);
			return so->nso_syncoff;
		}
	}


	plat_assert(!so->nso_insync);
	so->nso_insync = 1;

	pthread_mutex_unlock(&so->nso_sync_mutex);

	/*
	 * If the offset at which the new copy is with in same block, lets wait the flush.
	 * lets take a chance and see whether another copy starts.
	 */
	tmp_syncoff = atomic_add_get(so->nso_off, 0);
	if (!reset) {
		while (tmp_syncoff < roundup(off, nvr_blksize)) {
			sched_yield();
			if ((cpinprog = atomic_add_get(so->nso_writecnt, 0)) > 0) {
				atomic_inc(nvr_sync_restarts);
				tmp_syncoff = atomic_add_get(so->nso_off, 0);
			} else {
				break;
			}
		}

	}
	/*
	 * If there is a copy in progress and the flush is not block aligned, check
	 * whether we can split it into two parts, one till block boundary and another
	 * residual part we can combine with the data whose copy is in progress.
	 */
	if (!reset && !flag) {
		if((tmp_syncoff % nvr_blksize)) {
			if (((so->nso_syncoff % nvr_blksize == 0) &&
				(tmp_syncoff > (so->nso_syncoff + nvr_blksize))) ||
			    (( so->nso_syncoff % nvr_blksize != 0) &&
			     	(tmp_syncoff > roundup(so->nso_syncoff, nvr_blksize)))) {
				flag =1;
				tmp_syncoff = tmp_syncoff / nvr_blksize * nvr_blksize;
				off = tmp_syncoff + 1;
			}
		}
	}

	write_off = so->nso_syncoff / nvr_blksize * nvr_blksize;
	write_len = roundup(tmp_syncoff, nvr_blksize) - so->nso_syncoff / nvr_blksize * nvr_blksize;
	//write_len = (tmp_syncoff + nvr_blksize -1) / nvr_blksize * nvr_blksize - so->nso_syncoff / nvr_blksize * nvr_blksize;

	partition = so->nso_partition;
	so->nso_partition = (partition + 1) % n_partitions;

	for (int i = 0; i < write_len; i += nvr_blksize) {
		sbuf = (nvrsbuf_t *)(&so->nso_data[write_off + i]);
		sbuf->nsb_cksum = 0;
		sbuf->nsb_lsn = atomic_add_get(nvr_lsn, 1);
		sbuf->nsb_cksum = hashb((uint8_t *)sbuf, nvr_blksize, 0);
	}

	msg(INITIAL, DEBUG, "Doint pwrite off: %d write_off:%d syncoff:%d", (int)write_off, (int)(so->nso_syncoff), (int)tmp_syncoff);
	
	off_t offset_to_write = partition * partition_size + so->nso_fileoff + write_off;
	ret = pwrite(nvr_fd, so->nso_data + write_off, write_len, offset_to_write);

	if (!reset) {
		atomic_add(nvr_data_out, write_len);
	}

	if (ret == -1 || ret < write_len) {
		msg(INITIAL, FATAL, "Write to NVRAM failed, NVRAM disabled");
		nvr_disabled = 1;
		ret = -1;
	} else {
		ret = tmp_syncoff;
	}

	if (ret == ZS_SUCCESS) {
		if (hw_durable && !odirect) {
			if ((ret = fdatasync(nvr_fd)) == -1) {
				msg(INITIAL, FATAL, "fdatasync to NVRAM failed, NVRAM disabled");
				nvr_disabled = 1;
				ret = -1;
			} else {
				ret = tmp_syncoff;
			}
		}
	}
	pthread_mutex_lock(&so->nso_sync_mutex);
	so->nso_syncoff = tmp_syncoff;
	//msg(INITIAL, DEBUG, "synced till: nso_syncoff (%d)", (int)so->nso_syncoff);
	so->nso_insync = 0;
	pthread_cond_broadcast(&so->nso_sync_cond);
	pthread_mutex_unlock(&so->nso_sync_mutex);

	/*
	 * flag is 1, so we have split the IOs. Now lets restart sync of residual write.
	 */
	if ((ret == ZS_SUCCESS) && (flag == 1)) {
		if (!aligned_down) {
			sched_yield();
			flag = 2;
			atomic_inc(nvr_sync_splits);
			goto restart;
		}
	}


	return tmp_syncoff;
}

int
nvr_bytes_in_buffer(void)
{
	return bytes_per_nvr_buffer;
}

uint64_t
nvr_buffer_count(void)
{
	return (uint64_t)nvr_numobjs;
}

void
nvr_reset(void)
{
	for (int i = 0; i < nvr_numobjs; i++ ) {
		(void)nvr_alloc_buffer();
	}

	plat_assert(nvr_flhead == NULL && nvr_fltail == NULL);

	for (int i = 0; i < nvr_numobjs; i++ ) {
		for (int j = 0; j < n_partitions; j++) {
			(void)nvr_format_buffer(&nvr_objs[i]);
		}
	}
	for (int i = 0; i < nvr_numobjs; i++ ) {
		(void)nvr_free_buffer(&nvr_objs[i]);
	}
}


void
get_nvram_stats(ZS_stats_t *stat)
{
	stat->nvr_stats[ZS_NVR_WRITE_REQS] = nvr_write_reqs;
	stat->nvr_stats[ZS_NVR_WRITE_SAVED] = nvr_write_saved;
	stat->nvr_stats[ZS_NVR_DATA_IN] = nvr_data_in;
	stat->nvr_stats[ZS_NVR_DATA_OUT] = nvr_data_out;
	stat->nvr_stats[ZS_NVR_NOSPC] = nvr_no_space;
	stat->nvr_stats[ZS_NVR_SYNC_REQS] = nvr_sync_reqs;
	stat->nvr_stats[ZS_NVR_SYNC_RESTARTS] = nvr_sync_restarts;
	stat->nvr_stats[ZS_NVR_SYNC_SPLITS] = nvr_sync_splits;

}


