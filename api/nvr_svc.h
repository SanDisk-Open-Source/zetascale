/*
 * NVRAM services
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 *
 *	typedef	void		nvr_buffer_t;
 *
 *	static ZS_status_t	nvr_init( );
 *	static uint		nvr_buffer_count( );
 *	static nvr_buffer_t	*nvr_attach_active_buffer( );
 *	static nvr_buffer_t	*nvr_alloc_buffer( );
 *	static void		nvr_free_buffer( nvr_buffer_t *);
 *	static ZS_status_t	nvr_write_buffer_partial( nvr_buffer_t *, char [], uint base, uint count);
 *	static ZS_status_t	nvr_read_buffer( nvr_buffer_t *, char []);
 *
 *	An NVRAM buffer has size bytes_per_nvr_buffer and is persistent.
 *	It is referenced by clients using an opaque pointer to nvr_buffer_t.
 *	Buffers can be active (contains live data), or available for allocation.
 *
 *	nvr_init() will be called once at ZS initialization.
 *
 *	nvr_reset() will initialize the contents of NVRAM to 0
 *
 *	nvr_buffer_count() returns number of buffers available on NVRAM.
 *
 *	nvr_alloc_buffer() returns an unused buffer, or 0 if none available.
 *
 *	nvr_free_buffer() makes the given buffer available for future allocation.
 *	The prior contents are lost.
 *
 *	nvr_reset_buffer() clears the existing contents of NVRAM buffer
 *
 *	nvr_buffer_hold() increments refernce count of the active buffer.
 *
 *	nvr_buffer_release() decrements reference count and adds it to free list
 *	for reuse if reference count is 0.
 *
 *	nvr_write_buffer_partial() writes into the buffer according to the
 *	arguments.  There are no alignment requirements.  Return status may
 *	indicate device errors, base/count out of range, etc.
 *
 *	nvr_read_buffer() fills the caller's buffer.
 *
 *	nvr_bytes_in_buffer() returns amount of space available in NVRAM buffer
 */
#include "common/zstypes.h"

typedef	void		nvr_buffer_t;

ZS_status_t		nvr_init( );
nvr_buffer_t		*nvr_alloc_buffer( );
void			nvr_free_buffer( nvr_buffer_t *);
size_t			nvr_write_buffer_partial( nvr_buffer_t *, char *, uint count, int sync, off_t *off);
void			nvr_buffer_hold(nvr_buffer_t *buf);
void			nvr_buffer_release(nvr_buffer_t *buf);
ZS_status_t		nvr_reset_buffer(nvr_buffer_t *buf);
int			nvr_bytes_in_buffer(void) ;
ZS_status_t		nvr_read_buffer( nvr_buffer_t *, char **, int *);
uint64_t		nvr_buffer_count(void);
void			nvr_reset(void);
void			get_nvram_stats(ZS_stats_t *);
int 			nvr_sync_buf(nvr_buffer_t *so, off_t off);
int 			nvr_sync_buf_aligned(nvr_buffer_t *so, off_t off);
