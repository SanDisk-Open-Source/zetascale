

/*
 * Support for lean memory (Storm)
 *
 * In this design, active pages of the POT (persistent object table) are
 * tracked and remain memory-resident.  One page holds the number of POT
 * elements needed to span one segment: this number is always equal to the
 * number of device blocks per segment.  Tracking is provided by the the
 * POT bitmap.  Slabs used by Storm objects are tracked because that are
 * not recorded in the hash table.  All these structures are persistent,
 * and are updated at the end of each log merge.  Note that only active
 * pages of the POT are updated, giving a performance boost of 500x.
 *
 * Service is enabled with property FDF_STORM_MODE=1
 */

#include	<stdio.h>
#include	<stdlib.h>
#include	<memory.h>
#include	<malloc.h>
#include	<stdarg.h>
#include	"common/sdftypes.h"
#include	"utils/properties.h"
#include	"utils/hash.h"
#include	"ssd/ssd_aio.h"
#include	"hash.h"
#include	"mcd_osd.h"
#include	"mcd_rec.h"
#include	"utils/rico.h"
#include	"mcd_rec2.h"


#define	bytes_per_storm_key		(1uL << 8)
#define	bytes_per_second		(1uL << 31)
#define	bytes_per_segment		(1uL << 25)
#define	bytes_per_pot_element		(1uL << 4)
#define	bytes_per_log_record		(1uL << 6)
#define	device_blocks_per_storm_object	(bytes_per_storm_object / bytes_per_device_block)
#define	device_blocks_per_segment	(bytes_per_segment / bytes_per_device_block)
#define	pot_elements_per_page		device_blocks_per_segment
#define	bytes_per_page			(pot_elements_per_page * bytes_per_pot_element)
#define leaf_occupancy_pct		75
#define	regobj_scale_pct		120

/*
 * This rather arbitrary byte count is used to size and align flash
 * structures, and must equal or exceed bytes_per_device_block.
 */
#define	bytes_per_flash_block		(1L << 14)

#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)

int pot_checksum_enabled;
int rawobjratio;

typedef struct mcdstructure		mcd_t;
typedef struct potstructure		pot_t;
typedef void				packet_t;

struct mcdstructure {
	ulong	bytes_per_flash_array,
		bytes_per_log,
		storm_objects_per_array,
		storm_objects_per_leaf,
		leaves_per_array,
		POT_elements_per_POT,
		bytes_per_POT,
		leaf_segments_per_array,
		bytes_per_bitmap,
		log_records_per_log,
		merge_secs_for_gross_POT_write,
		merge_secs_for_efficient_POT_write,
		merge_secs_for_log_read,
		merge_secs_for_bitmap_write,
		storm_bytes_written_per_log_merge,
		storm_seconds_per_log_merge,
		bytes_per_POT_bitmap;
	float	log_size_factor,
		maximum_POT_utilization;
};

struct potstructure {
	mcd_rec_flash_object_t	data[device_blocks_per_segment];
};


extern flash_settings_t		flash_settings;
extern uint64_t			Mcd_rec_update_bufsize,
				Mcd_rec_update_segment_size,
				Mcd_rec_update_segment_blks,
				Mcd_rec_log_segment_blks;
static struct mcdstructure	mcd;
static ulong			bytes_per_storm_object;
static ulong			bytes_per_leaf;
static uint			segments_per_flash_array;


void		mcd_fth_osd_slab_load_slabbm( osd_state_t *, mcd_osd_shard_t *, uchar [], ulong),
		delete_object( mcd_rec_flash_object_t *),
		*context_alloc( int),
		context_free( void *),
		attach_buffer_segments( mcd_osd_shard_t *, int, int *, char **),
		detach_buffer_segments( mcd_osd_shard_t *, int, char **),
		recovery_checkpoint( osd_state_t *, mcd_osd_shard_t *, uint64_t),
		stats_packet_save( mcd_rec_obj_state_t *, void *, mcd_osd_shard_t *),
		recovery_packet_save( packet_t *, void *, mcd_osd_shard_t *),
		filter_cs_initialize( mcd_rec_obj_state_t *),
		filter_cs_swap_log( mcd_rec_obj_state_t *),
		filter_cs_flush( mcd_rec_obj_state_t *);
int		filter_cs_apply_logrec( mcd_rec_obj_state_t *, mcd_logrec_object_t *),
		filter_cs_rewind_log( mcd_rec_obj_state_t *),
		read_log_segment( void *, int, mcd_osd_shard_t *, mcd_rec_log_state_t *, char *);
uint64_t	read_log_page( osd_state_t *, mcd_osd_shard_t *, int, uint64_t),
		blk_to_use( mcd_osd_shard_t *, uint64_t);
bool		match_potbm_checksum( potbm_t *, uint),
		match_slabbm_checksum( slabbm_t *, uint),
		empty( void *, uint);
static bool	nomem( ),
		corruptpotbm( ),
		corruptslabbm( ),
		complain( char *, ...);
static ulong	power_of_two_roundup( ulong),
		pot_base( mcd_rec_shard_t *);
ulong		potbm_base( mcd_rec_shard_t *),
		slabbm_base( mcd_rec_shard_t *);
static void	pot_cache_shutdown( mcd_osd_shard_t *),
		pot_bitmap_shutdown( mcd_osd_shard_t *),
		assign_potbm_checksum( potbm_t *, uint),
		assign_slabbm_checksum( slabbm_t *, uint),
		readerror( int),
		writeerror( int),
		invalidblock( ulong),
		calc_params( ulong, float),
		dump_params( );
static char	*prettynumber( ulong);

void pot_checksum_set(char* buf, uint32_t sum);
uint32_t pot_checksum_get(char* buf);

bool
check_storm_mode( )
{

	if (flash_settings.storm_mode) {
		unless (flash_settings.os_blk_size == bytes_per_device_block) {
			complain( "ZS_BLOCK_SIZE is not %lu--Storm Mode disabled", bytes_per_device_block);
			flash_settings.storm_mode = 0;
		}
	}
	storm_mode = flash_settings.storm_mode;
	return (storm_mode);
}


/*
 * initialize Storm extension
 *
 * Storm Mode must be enabled to activate services.
 *
 * Node size (in bytes) as used at btree level is given by property
 * ZS_BTREE_NODE_SIZE, default value 8100.  With overhead included, this
 * default allows the node to be stored in an 8KB slab: bear that overhead
 * in mind when assigning a different size.
 *
 * The size in bytes of a raw object is given by property ZS_RAW_OBJECT_SIZE,
 * default 64KiB: note that the initial value is immediately rounded to the
 * next power-of-two for slab-allocation purposes.  The net available space
 * for a raw object is less due to overhead.  The property is updated to
 * reflect these calculations with a string of the form S:N, where S is the
 * slab size and N is the net size.
 */
bool
mcd_rec2_init( ulong bytes_per_flash_array)
{

	msg( INITIAL, INFO, "Storm Mode = %d   Storm Test = 0x%04X", flash_settings.storm_mode, flash_settings.storm_test);
	if (flash_settings.storm_mode) {
		bytes_per_storm_object = 1uL << power_of_two_roundup( getProperty_Int( "ZS_RAW_OBJECT_SIZE", 65000));
		bytes_per_leaf = 1uL << power_of_two_roundup( getProperty_Int( "ZS_BTREE_NODE_SIZE", 8100));
		unless (Mcd_osd_blk_size == bytes_per_device_block) {
			complain( "ZS_BLOCK_SIZE is not %lu--Storm Mode disabled", bytes_per_device_block);
			flash_settings.storm_mode = 0;
			storm_mode = 0;
			return (TRUE);
		}
		calc_params( bytes_per_flash_array, .5);
		dump_params( );
	}
	storm_mode = flash_settings.storm_mode;
	return (TRUE);
}


/*
 * free resources of the Storm extension before shutdown
 */
void
mcd_rec2_shutdown( mcd_osd_shard_t *s)
{

	if (flash_settings.storm_mode) {
		pot_cache_shutdown( s);
		pot_bitmap_shutdown( s);
	}
}


/*
 * size of one log in bytes
 *
 * Also try to accommodate ten full-size transaction brackets.
 * Valid for Storm Mode only.
 */
ulong
mcd_rec2_log_size( ulong bytes_per_shard)
{

	ulong pot_elements_per_pot = bytes_per_shard / bytes_per_device_block;
	ulong bytes_per_pot = pot_elements_per_pot * bytes_per_pot_element;
	ulong bytes_per_log = mcd.maximum_POT_utilization * mcd.log_size_factor * bytes_per_pot;
	return (roundup( max( bytes_per_log, 67108864), bytes_per_flash_block));
}


/*
 * initialize the POT cache
 *
 * Cache is filled and pinned as directed by the POT bitmap.  A cache page
 * contains the POT elements necessary to describe one segment of slabs.
 * Note that one POT element exists for each device block.  Return 0 on
 * memory exhaustion, abort on read error.
 */
bool
mcd_rec2_potcache_init( mcd_osd_shard_t *s, osd_state_t *context)
{

	if (flash_settings.storm_mode) {
		mcd_rec_log_t *l = s->log;
		uint npage = divideup( s->total_blks, device_blocks_per_segment);
		size_t nbyte = npage * sizeof( mcd_rec_flash_object_t *);
		l->potcachesize = 0;
		unless (l->potcache = malloc( nbyte))
			return (nomem( ));
		memset( l->potcache, 0, nbyte);
		l->potcachesize = npage;
		const mcd_rec_flash_object_t **c = l->potcache;
		for (ulong page=0; page<l->potcachesize; ++page)
			if (mcd_rec2_potbitmap_query( s, page)) {
				ulong o = pot_base( s->pshard) + page*bytes_per_page;
				mcd_rec_flash_object_t *e = memalign( MCD_OSD_META_BLK_SIZE, bytes_per_page);
				unless (e)
					return ((void *)nomem( ));
				int rc = mcd_fth_aio_blk_read( context, (char *)e, o, bytes_per_page);
				if (rc) {
					abort_on_io_error(rc);
					readerror( rc);
				}

				if(pot_checksum_enabled) {
					uint32_t c, sum = pot_checksum_get((char*)e);
					pot_checksum_set((char*)e, 0);
					if((c = checksum((char*)e, bytes_per_page, 0)) != sum)
					{
						if(sum || !empty(e, bytes_per_page)) {
							mcd_log_msg(160284, PLAT_LOG_LEVEL_FATAL,
									"POT checksum error. expected=%x, read_from_disk=%x, start_blk=%ld num_blks=%d", c, sum,
									page, 1);
							return FALSE;
						}
					}
				}

				c[page] = e;
			}
	}
	return (TRUE);
}


/*
 * save active POT pages to flash
 *
 * Aborts on write error.
 */
bool
mcd_rec2_potcache_save( mcd_osd_shard_t *s, void *context)
{

	mcd_rec_log_t *l = s->log;
	mcd_rec_flash_object_t **c = l->potcache;
	if (c)
		for (uint i=0; i<l->potcachesize; ++i)
			if (c[i]) {
				mcd_rec_shard_t *p = s->pshard;
				ulong o = pot_base( p);
				o += i * bytes_per_page;
				pot_checksum_set((char*)c[i], 0);
				uint32_t sum = checksum((char*)c[i], bytes_per_page, 0);
				pot_checksum_set((char*)c[i], sum);
				int rc = mcd_fth_aio_blk_write( context, (char *)c[i], o, bytes_per_page);
				if (rc) {
					abort_on_io_error(rc);
					writeerror( rc);
				}
			}
	return (TRUE);
}


/*
 * return address of POT element given by device blkno
 *
 * The POT bitmap indicates the active pages of the POT.  Since the POT cache
 * is preloaded on startup, newly active pages are empty by definition and
 * can, therefore, by zeroed rather than read from flash.  This optimization
 * is actually mandatory because, in Storm Mode, the POT is not cleared at
 * time of flash format, and is likely filled with debris.
 *
 * Return 0 on memory exhaustion.
 */
mcd_rec_flash_object_t	*
mcd_rec2_potcache_access( mcd_osd_shard_t *s, void *context, ulong blkno)
{

	uint page = blkno / device_blocks_per_segment;
	mcd_rec_log_t *l = s->log;
	mcd_rec_flash_object_t **c = l->potcache;
	unless (c[page]) {
		mcd_rec_flash_object_t *e = memalign( MCD_OSD_META_BLK_SIZE, bytes_per_page);
		unless (e)
			return ((void *)nomem( ));
		memset( e, 0, bytes_per_page);
		mcd_rec2_potbitmap_set( s, page);
		c[page] = e;
	}
	return (&c[page][blkno%device_blocks_per_segment]);
}


/*
 * POT bitmap size in bytes
 */
ulong
mcd_rec2_potbitmap_size( ulong bytes_per_shard)
{

	unless (flash_settings.storm_mode)
		return (0);
	ulong segments_per_shard = divideup( bytes_per_shard, bytes_per_segment);
	ulong bytes_per_potbitmap = divideup( segments_per_shard, bits_per_byte);
	return (roundup( sizeof( potbm_t)+bytes_per_potbitmap, bytes_per_flash_block));
}


/*
 * load POT bitmap from flash
 *
 * Aborts on read errors, returns FALSE on other errors.
 */
bool
mcd_rec2_potbitmap_load( mcd_osd_shard_t *s, void *context)
{

	if (flash_settings.storm_mode) {
		mcd_rec_shard_t *p = s->pshard;
		mcd_rec_log_t *l = s->log;
		size_t n = p->rec_potbm_blks * bytes_per_device_block;
		unless (l->potbm = memalign( MCD_OSD_META_BLK_SIZE, n))
			return (nomem( ));
		int rc = mcd_fth_aio_blk_read( context, (char *)l->potbm, potbm_base( p), n);
		unless (rc == FLASH_EOK) {
			abort_on_io_error(rc);
			readerror( rc);
		}
		potbm_t *potbm = l->potbm;
		if (potbm->eye_catcher == 0) {
			unless (empty( potbm, n))
				return (corruptpotbm( ));
			potbm->eye_catcher = MCD_REC_POTBM_EYE_CATCHER;
			potbm->nbit = (n-sizeof( *potbm)) * bits_per_byte;
		}
		else unless ((potbm->eye_catcher==MCD_REC_POTBM_EYE_CATCHER)
		and (match_potbm_checksum( potbm, n)))
			return (corruptpotbm( ));
	}
	return (TRUE);
}


/*
 * save POT bitmap to flash
 *
 * Aborts on write error.
 */
bool
mcd_rec2_potbitmap_save( mcd_osd_shard_t *s, void *context)
{

	if (flash_settings.storm_mode) {
		mcd_rec_shard_t *p = s->pshard;
		mcd_rec_log_t *l = s->log;
		size_t n = p->rec_potbm_blks * bytes_per_device_block;
		potbm_t *potbm = l->potbm;
		potbm->eye_catcher = MCD_REC_POTBM_EYE_CATCHER;
		assign_potbm_checksum( potbm, n);
		int rc = mcd_fth_aio_blk_write( context, (char *)potbm, potbm_base( p), n);
		unless (rc == FLASH_EOK) {
			abort_on_io_error(rc);
			writeerror( rc);
		}
	}
	return (TRUE);
}


/*
 * set bit in the POT bitmap
 *
 * Segment 'n' is marked as active, and the corresponding POT page will be
 * loaded on future restarts.
 */
void
mcd_rec2_potbitmap_set( mcd_osd_shard_t *s, uint n)
{

	potbm_t *potbm = s->log->potbm;
	if ((flash_settings.storm_mode)
	and (n < potbm->nbit))
		potbm->bits[bitbase( n)] |= bitmask( n);
}


/*
 * query bit in the POT bitmap
 *
 * If 'n' is not a valid segment number, return FALSE.
 */
bool
mcd_rec2_potbitmap_query( mcd_osd_shard_t *s, uint n)
{

	potbm_t *potbm = s->log->potbm;
	unless ((flash_settings.storm_mode)
	and (n < potbm->nbit))
		return (FALSE);
	return (potbm->bits[bitbase( n)] & bitmask( n));
}


/*
 * slab bitmap size in bytes
 *
 * We only track Storm slabs, so the bitmap is scaled accordingly.  Each bit
 * corresponds to bytes_per_storm_object (constant with nominal value 64KB),
 * and the entire slab area of the shard is mapped.
 */
ulong
mcd_rec2_slabbitmap_size( ulong bytes_per_shard)
{

	unless (flash_settings.storm_mode)
		return (0);
	ulong storm_objects_per_shard = divideup( bytes_per_shard, bytes_per_storm_object);
	return (roundup( sizeof( slabbm_t)+storm_objects_per_shard/bits_per_byte, bytes_per_flash_block));
}


/*
 * load slab bitmap from flash
 *
 * Aborts on read errors, returns FALSE on other errors.
 */
bool
mcd_rec2_slabbitmap_load( mcd_osd_shard_t *s, void *context)
{

	mcd_rec_shard_t *p = s->pshard;
	mcd_rec_log_t *l = s->log;
	size_t n = p->rec_slabbm_blks * bytes_per_device_block;
	unless (l->slabbm = memalign( MCD_OSD_META_BLK_SIZE, n))
		return (nomem( ));
	int rc = mcd_fth_aio_blk_read( context, (char *)l->slabbm, slabbm_base( p), n);
	unless (rc == FLASH_EOK) {
		abort_on_io_error(rc);
		readerror( rc);
	}
	slabbm_t *slabbm = l->slabbm;
	if (slabbm->eye_catcher == 0) {
		unless (empty( slabbm, n))
			return (corruptslabbm( ));
		slabbm->eye_catcher = MCD_REC_SLABBM_EYE_CATCHER;
		slabbm->nbit = (n-sizeof( *slabbm)) * bits_per_byte;
	}
	else unless ((slabbm->eye_catcher == MCD_REC_SLABBM_EYE_CATCHER)
	and (match_slabbm_checksum( slabbm, n)))
		return (corruptslabbm( ));
	return (TRUE);
}


/*
 * save slab bitmap to flash
 *
 * Returns FALSE on write error.
 */
bool
mcd_rec2_slabbitmap_save( mcd_osd_shard_t *s, void *context)
{

	if (flash_settings.storm_mode) {
		mcd_rec_shard_t *p = s->pshard;
		mcd_rec_log_t *l = s->log;
		size_t n = p->rec_slabbm_blks * bytes_per_device_block;
		slabbm_t *slabbm = l->slabbm;
		assign_slabbm_checksum( slabbm, n);
		int rc = mcd_fth_aio_blk_write( context, (char *)slabbm, slabbm_base( p), n);
		unless (rc == FLASH_EOK) {
			abort_on_io_error(rc);
			writeerror( rc);
		}
	}
	return (TRUE);
}


/*
 * set bit for specified block in the slab bitmap
 *
 * Defined for Storm Mode only.
 */
void
slab_bitmap_set( mcd_osd_shard_t *s, uint64_t blk_offset)
{
	ulong x = blk_offset / device_blocks_per_storm_object;

	slabbm_t *slabbm = s->log->slabbm;
	unless (x < slabbm->nbit)
		invalidblock( blk_offset);
	slabbm->bits[bitbase( x)] |= bitmask( x);
}


/*
 * clear bit for specified block in the slab bitmap
 *
 * Defined for Storm Mode only.
 */
void
slab_bitmap_clear( mcd_osd_shard_t *s, uint64_t blk_offset)
{
	ulong x = blk_offset / device_blocks_per_storm_object;

	slabbm_t *slabbm = s->log->slabbm;
	unless (x < slabbm->nbit)
		invalidblock( blk_offset);
	slabbm->bits[bitbase( x)] &= ~ bitmask( x);
}


static void
pot_cache_shutdown( mcd_osd_shard_t *s)
{

	mcd_rec_log_t *l = s->log;
	mcd_rec_flash_object_t **c = l->potcache;
	if (c) {
		for (uint i=0; i<l->potcachesize; ++i)
			if (c[i])
				free( c[i]);
		free( c);
		l->potcache = 0;
	}
}


static void
pot_bitmap_shutdown( mcd_osd_shard_t *s)
{

	free( s->log->potbm);
	s->log->potbm = 0;
}


/*
 * calculate and install checksum into potbm
 */
static void
assign_potbm_checksum( potbm_t *potbm, uint n)
{

	potbm->checksum = 0;
	potbm->checksum = hashb( (uchar *)potbm, n, 0);
}


/*
 * check that potbm checksum is valid
 */
bool
match_potbm_checksum( potbm_t *potbm, uint n)
{

	ulong cs0 = potbm->checksum;
	potbm->checksum = 0;
	ulong cs1 = hashb( (uchar *)potbm, n, 0);
	potbm->checksum = cs0;
	return (cs0 == cs1);
}


/*
 * return byte address on flash of shard's POT
 */
static ulong
pot_base( mcd_rec_shard_t *p)
{

	ulong o = p->blk_offset;
	o += p->rec_md_blks;
	o *= bytes_per_device_block;
	return (o);
}


/*
 * return byte address on flash of shard's POT bitmap
 */
ulong
potbm_base( mcd_rec_shard_t *p)
{

	ulong o = p->blk_offset;
	o += p->rec_md_blks;
	o += p->rec_table_blks;
	o += p->rec_table_pad;
	o += MCD_REC_NUM_LOGS * (p->rec_log_blks+p->rec_log_pad);
	o *= bytes_per_device_block;
	return (o);
}


/*
 * calculate and install checksum into slabbm
 */
static void
assign_slabbm_checksum( slabbm_t *slabbm, uint n)
{

	slabbm->checksum = 0;
	slabbm->checksum = hashb( (uchar *)slabbm, n, 0);
}


/*
 * check that potbm checksum is valid
 */
bool
match_slabbm_checksum( slabbm_t *slabbm, uint n)
{

	ulong cs0 = slabbm->checksum;
	slabbm->checksum = 0;
	ulong cs1 = hashb( (uchar *)slabbm, n, 0);
	slabbm->checksum = cs0;
	return (cs0 == cs1);
}


/*
 * return byte address on flash of shard's slab bitmap
 */
ulong
slabbm_base( mcd_rec_shard_t *p)
{

	ulong o = p->blk_offset;
	o += p->rec_md_blks;
	o += p->rec_table_blks;
	o += p->rec_table_pad;
	o += MCD_REC_NUM_LOGS * (p->rec_log_blks+p->rec_log_pad);
	o += p->rec_potbm_blks;
	o *= bytes_per_device_block;
	return (o);
}


/*
 * return TRUE if memory block is null
 */
bool
empty( void *p, uint n)
{

	uint c = 0;
	for (uint i=0; i<n and not c; ++i)
		c |= ((char *)p)[i];
	return (not c);
}


static bool
nomem( )
{

	msg( INITIAL, FATAL, "out of memory");
	return (FALSE);
}


static void
readerror( int rc)
{

	msg( INITIAL, FATAL, "read error (%d)", rc);
	plat_abort( );
}


static void
writeerror( int rc)
{

	msg( INITIAL, FATAL, "write error (%d)", rc);
	plat_abort( );
}


static bool
corruptpotbm( )
{

	msg( INITIAL, FATAL, "POT bitmap is corrupt");
	return (FALSE);
}


static bool
corruptslabbm( )
{

	msg( INITIAL, FATAL, "slab bitmap is corrupt");
	return (FALSE);
}


static void
invalidblock( ulong blk_offset)
{

	msg( INITIAL, FATAL, "cannot update slab bitmap with invalid block #%lu", blk_offset);
	plat_abort( );
}


static bool
complain( char *mesg, ...)
{
	va_list	va;
	char	*a;

	va_start( va, mesg);
	if (vasprintf( &a, mesg, va) < 0)
		return (nomem( ));
	va_end( va);
	msg( INITIAL, ERROR, "%s", a);
	free( a);
	return (FALSE);
}


/*
 * calculate canonical sizes for flash layout and performance
 *
 * Log size is denominated as a factor of the POT size.  Note that
 * calculations round down and, therefore, may be unsuitable for all uses.
 */
static void
calc_params( ulong bytes_per_flash_array, float log_size_factor)
{

	mcd.bytes_per_flash_array = bytes_per_flash_array;
	mcd.storm_objects_per_array = mcd.bytes_per_flash_array / bytes_per_storm_object;
	mcd.storm_objects_per_leaf = bytes_per_leaf / bytes_per_storm_key;
	mcd.leaves_per_array = mcd.storm_objects_per_array / mcd.storm_objects_per_leaf;
	mcd.leaves_per_array = (mcd.leaves_per_array * 100) / leaf_occupancy_pct;
	mcd.POT_elements_per_POT = mcd.bytes_per_flash_array / bytes_per_device_block;
	mcd.bytes_per_POT = mcd.POT_elements_per_POT * bytes_per_pot_element;
	mcd.maximum_POT_utilization = (float)mcd.leaves_per_array / mcd.POT_elements_per_POT;
	mcd.leaf_segments_per_array = mcd.leaves_per_array * bytes_per_leaf / bytes_per_segment;
	mcd.bytes_per_bitmap = mcd.storm_objects_per_array / bits_per_byte;
	mcd.bytes_per_log = log_size_factor * mcd.bytes_per_POT;
	mcd.log_size_factor = log_size_factor;
	mcd.log_records_per_log = mcd.bytes_per_log / bytes_per_log_record;
	mcd.merge_secs_for_gross_POT_write = mcd.bytes_per_POT / bytes_per_second;
	mcd.merge_secs_for_efficient_POT_write = mcd.bytes_per_POT * mcd.maximum_POT_utilization / bytes_per_second;
	mcd.merge_secs_for_log_read = mcd.bytes_per_log / bytes_per_second;
	mcd.merge_secs_for_bitmap_write = mcd.bytes_per_bitmap / bytes_per_second;
	mcd.storm_bytes_written_per_log_merge = mcd.log_records_per_log * bytes_per_storm_object / 2;
	mcd.storm_seconds_per_log_merge = mcd.storm_bytes_written_per_log_merge / bytes_per_second;
	mcd.bytes_per_POT_bitmap = mcd.bytes_per_flash_array / bytes_per_segment / bits_per_byte;
	segments_per_flash_array = mcd.bytes_per_flash_array / bytes_per_segment;
};


static void
dump_params( )
{

	msg( INITIAL, INFO, "Nominal sizes for this Storm configuration:");
	msg( INITIAL, INFO, "bytes_per_flash_array = %s", prettynumber( mcd.bytes_per_flash_array));
	msg( INITIAL, INFO, "bytes_per_device_block = %s", prettynumber( bytes_per_device_block));
	msg( INITIAL, INFO, "bytes_per_storm_object = %s", prettynumber( bytes_per_storm_object));
	msg( INITIAL, INFO, "bytes_per_leaf = %s", prettynumber( bytes_per_leaf));
	msg( INITIAL, INFO, "bytes_per_storm_key = %s", prettynumber( bytes_per_storm_key));
	msg( INITIAL, INFO, "bytes_per_second = %s", prettynumber( bytes_per_second));
	msg( INITIAL, INFO, "bytes_per_segment = %s", prettynumber( bytes_per_segment));
	msg( INITIAL, INFO, "bytes_per_pot_element = %s", prettynumber( bytes_per_pot_element));
	msg( INITIAL, INFO, "bytes_per_log_record = %s", prettynumber( bytes_per_log_record));
	msg( INITIAL, INFO, "bytes_per_flash_array = %s", prettynumber( mcd.bytes_per_flash_array));
	msg( INITIAL, INFO, "bytes_per_log = %s", prettynumber( mcd.bytes_per_log));
	msg( INITIAL, INFO, "storm_objects_per_array = %s", prettynumber( mcd.storm_objects_per_array));
	msg( INITIAL, INFO, "storm_objects_per_leaf = %s", prettynumber( mcd.storm_objects_per_leaf));
	msg( INITIAL, INFO, "leaves_per_array = %s", prettynumber( mcd.leaves_per_array));
	msg( INITIAL, INFO, "POT_elements_per_POT = %s", prettynumber( mcd.POT_elements_per_POT));
	msg( INITIAL, INFO, "bytes_per_POT = %s", prettynumber( mcd.bytes_per_POT));
	msg( INITIAL, INFO, "maximum_POT_utilization = %f", mcd.maximum_POT_utilization);
	msg( INITIAL, INFO, "log_size_factor = %f", mcd.log_size_factor);
	msg( INITIAL, INFO, "leaf_segments_per_array = %s", prettynumber( mcd.leaf_segments_per_array));
	msg( INITIAL, INFO, "bytes_per_bitmap = %s", prettynumber( mcd.bytes_per_bitmap));
	msg( INITIAL, INFO, "log_records_per_log = %s", prettynumber( mcd.log_records_per_log));
	msg( INITIAL, INFO, "merge_secs_for_gross_POT_write = %s", prettynumber( mcd.merge_secs_for_gross_POT_write));
	msg( INITIAL, INFO, "merge_secs_for_efficient_POT_write = %s", prettynumber( mcd.merge_secs_for_efficient_POT_write));
	msg( INITIAL, INFO, "merge_secs_for_log_read = %s", prettynumber( mcd.merge_secs_for_log_read));
	msg( INITIAL, INFO, "merge_secs_for_bitmap_write = %s", prettynumber( mcd.merge_secs_for_bitmap_write));
	msg( INITIAL, INFO, "storm_bytes_written_per_log_merge = %s", prettynumber( mcd.storm_bytes_written_per_log_merge));
	msg( INITIAL, INFO, "storm_seconds_per_log_merge = %s", prettynumber( mcd.storm_seconds_per_log_merge));
	msg( INITIAL, INFO, "bytes_per_POT_bitmap = %s", prettynumber( mcd.bytes_per_POT_bitmap));
	msg( INITIAL, INFO, "segments_per_flash_array = %s", prettynumber( segments_per_flash_array));
	msg( INITIAL, INFO, "leaf_occupancy_pct = %s", prettynumber( leaf_occupancy_pct));
	msg( INITIAL, INFO, "regobj_scale_pct = %s", prettynumber( regobj_scale_pct));
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


/*
 * net raw object size
 *
 * Calculate net size in bytes of a raw object after internal overhead is
 * subtracted, and relay the value into the supplied uint64.  Return FALSE if
 * the value would be too small, otherwise true.  If true, the value is also
 * accessible to higher levels by referring to property ZS_RAW_OBJECT_SIZE,
 * second field (first field is the raw-object slab size).
 */
bool
get_rawobjsz( uint64_t *nbyte)
{

	*nbyte = 0;
	if (storm_mode) {
		long n = bytes_per_storm_object - sizeof( mcd_osd_meta_t) - sizeof( baddr_t);
		if (n < 0) {
			msg( INITIAL, FATAL, "bytes_per_storm_object (property ZS_RAW_OBJECT_SIZE) is too small");
			return (FALSE);
		}
		*nbyte = n;
		char *s;
		asprintf( &s, "%lu:%lu", bytes_per_storm_object, n);
		setProperty( "ZS_RAW_OBJECT_SIZE", s);
	}
	return (TRUE);
}

int
get_rawobjratio()
{
	if (storm_mode) {
		return (int)(bytes_per_storm_object / Mcd_osd_blk_size);
	} else {
		return 0;
	}
}

void
process_log_storm( void * context, mcd_osd_shard_t * shard, mcd_rec_obj_state_t * state, mcd_rec_log_state_t * log_state )
{
	bool		end_of_log	= false;
	int		s, p, r;
	char		*buf;
	uint64_t	blk_count	= 0;
	uint64_t	page_offset;
	uint64_t	rec_offset;
	uint64_t	prev_LSN	= 0;
	uint64_t	applied		= 0;
	uint64_t	checksum;

	ulong reclogblks = VAR_BLKS_TO_META_BLKS( shard->pshard->rec_log_blks);
	uint seg_count = divideup( reclogblks, Mcd_rec_log_segment_blks);
	for ( s = 0; s < seg_count && !end_of_log; s++, blk_count += Mcd_rec_log_segment_blks ) {
		buf = log_state->segments[0];
		read_log_segment( context, s, shard, log_state, buf );
		// process each log page (512-byte block) in segment
		for ( p = 0; p < Mcd_rec_log_segment_blks; p++ ) {
			page_offset = p * MCD_OSD_META_BLK_SIZE;
			mcd_rec_logpage_hdr_t *page_hdr = (mcd_rec_logpage_hdr_t *)( buf + page_offset );
			// verify page header
			checksum = page_hdr->checksum;
			if ( checksum == 0 ) {
				plat_assert( page_hdr->LSN == 0 );
				plat_assert( page_hdr->eye_catcher == 0 );
				plat_assert( page_hdr->version == 0 );
			} else {
				// verify checksum
				page_hdr->checksum = 0;
				page_hdr->checksum = hashb((unsigned char *)(buf+page_offset), MCD_OSD_META_BLK_SIZE, page_hdr->LSN);
				if ( page_hdr->checksum != checksum ) {
					msg( 40036, ERROR, "Invalid log page checksum, shardID=%lu, found=%lu, calc=%lu, boff=%lu, poff=%d", shard->id, checksum, page_hdr->checksum, blk_count, p );
					memset( buf+page_offset, 0, MCD_OSD_META_BLK_SIZE);
					end_of_log = true;
					p = Mcd_rec_log_segment_blks;
					break;
				}
				// verify header
				if ( page_hdr->eye_catcher != MCD_REC_LOGHDR_EYE_CATCHER ||
					 page_hdr->version != MCD_REC_LOGHDR_VERSION ) {
					msg( 20508, FATAL, "Invalid log page header, shardID=%lu, " "magic=0x%x, version=%d, boff=%lu, poff=%d", shard->id, page_hdr->eye_catcher, page_hdr->version, blk_count, p);
					plat_abort();
				}
			}
			// end of log?
			if ( page_hdr->LSN < prev_LSN ) {
				end_of_log = true;
				break;
			}
			// LSN must advance by one
			else if ( page_hdr->LSN != prev_LSN + 1 && prev_LSN > 0 ) {
				msg( 40110, FATAL,
							 "Unexpected LSN, shardID=%lu, LSN=%lu, "
							 "prev_LSN=%lu; seg=%d, page=%d",
							 shard->id, page_hdr->LSN, prev_LSN, s, p );
				plat_abort();
			}
			prev_LSN = page_hdr->LSN;
			// Note: the following condition is hit during the first log
			// read, before the high and low LSNs have been established.
			// If we change the log to start writing immediately after the
			// checkpoint page following a recovery, then we can't do this
			// (or the same check at the top of this function).
			// log records in this page already applied?
			if ( page_hdr->LSN <= shard->ckpt->LSN ) {
				end_of_log = true;
				break;
			}
			// apply each log record in this page to current metadata chunk
			// Note: log page header and log record sizes are the same!
			for ( r = 1; r < MCD_REC_LOG_BLK_SLOTS; r++ ) { // skip page hdr
				// get record offset
				rec_offset = page_offset + (r * sizeof( mcd_logrec_object_t ));
				// apply log record
				applied += filter_cs_apply_logrec( state, (mcd_logrec_object_t *)(buf+rec_offset));
			}
		}
	}
	applied += filter_cs_rewind_log( state);
	log_state->high_LSN = prev_LSN;
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


/*
 * load hash table entry from persistent object `e'
 *
 * Loaded object resides on slab at `blk_offset'.
 */
static void
update_hash_entry( mcd_osd_shard_t *shard, void *context, ulong blk_offset, const mcd_rec_flash_object_t *e)
{

	mcd_osd_segment_t *segment = shard->segment_table[blk_offset / Mcd_osd_segment_blks];
	// find the right hash entry to use
	hash_entry_recovery_insert( shard->hash_handle, (mcd_rec_flash_object_t *)e, blk_offset);
	unless (segment)
	{
		uint64_t seg_blk_offset = (blk_offset / Mcd_osd_segment_blks) * Mcd_osd_segment_blks;
		mcd_osd_slab_class_t* seg_class = &shard->slab_classes[shard->class_table[device_blocks_per_storm_object]];
		segment = mcd_osd_assign_segment(shard, seg_class, seg_blk_offset);
		mcd_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_INFO, "persisting segment on recovery(slabbm), shard_id=%ld seg_offset=%lu, obj_offs=%lu, slab_size=%d", shard->id, seg_blk_offset, blk_offset, seg_class->slab_blksize );
		plat_assert(segment);
	}

	// housekeeping
	shard->blk_consumed += blk_to_use( shard, e->blocks);
	shard->num_objects += 1;
	shard->total_objects += 1;
	mcd_osd_slab_class_t *class = segment->class;
	plat_assert(class);
	class->used_slabs += 1;
	// update bitmap for class segment
	plat_assert(segment->class == class);
	ulong map_offset = (blk_offset - segment->blk_offset) / class->slab_blksize;
	segment->mos_bitmap[map_offset / 64] |= Mcd_osd_bitmap_masks[map_offset % 64];
#if 0
	segment->alloc_map[map_offset / 64] |= Mcd_osd_bitmap_masks[map_offset % 64];	// used by full backup
#endif
	segment->used_slabs += 1;
	// if this segment is the last segment allocated for this class
	// then keep track of the highest slab allocated
	if (segment == class->segments[class->num_segments-1]) {
		uint temp = ((map_offset % (Mcd_aio_strip_size/(class->slab_blksize*Mcd_osd_blk_size))) + 1) * Mcd_aio_num_files;
		if (Mcd_aio_strip_size < class->slab_blksize*Mcd_osd_blk_size)
			temp = map_offset + 1;	/* FIXME_8MB */
		segment->next_slab = max( segment->next_slab, temp);
		plat_assert_always(segment->next_slab <= class->slabs_per_segment);
	}
	else
		segment->next_slab = class->slabs_per_segment;
	// save highest sequence number found
	shard->sequence = max( shard->sequence, e->seqno);
}


/*
 * merge non-empty logs, upload hash table, upload raw-object allocations to slab system
 */
static void
recover( mcd_osd_shard_t *s, osd_state_t *context, char **buf_segments)
{
	mcd_rec_obj_state_t	state;
	mcd_rec_log_state_t	old_log_state;
	mcd_rec_log_state_t	curr_log_state;
	uint64_t		LSN[2]		= { 0, 0 };
	bool			old_merged;

	mcd_rec_log_t *l = s->log;
	potbm_t *potbm = l->potbm;
	s->rec_num_updates += 1;
	s->rec_upd_running = 1;
	s->rec_upd_prev_usec = s->rec_upd_usec;
	s->rec_log_reads_cum += s->rec_log_reads;
	s->rec_table_reads_cum += s->rec_table_reads;
	s->rec_table_writes_cum += s->rec_table_writes;
	s->rec_log_reads = 0;
	s->rec_table_reads = 0;
	s->rec_table_writes = 0;
	memset(&state, 0, sizeof(mcd_rec_obj_state_t));
	memset(&old_log_state, 0, sizeof(mcd_rec_log_state_t));
	memset(&curr_log_state, 0, sizeof(mcd_rec_log_state_t));
	// Merge "old" log with the object table
	old_merged = false;
	state.in_recovery = 1;
	state.pass = 1;
	state.passes = state.in_recovery ? 2 : 1;
	state.start_blk = 0;
	state.num_blks = VAR_BLKS_TO_META_BLKS( s->pshard->rec_table_blks);
	state.start_obj = 0;
	state.num_objs = 0;
	state.seg_objects = (Mcd_rec_update_segment_size / sizeof(mcd_rec_flash_object_t));
	state.seg_count = 0;
	state.segments = buf_segments;
	// attach object table buffer segments for this update/recovery
	attach_buffer_segments(s, state.in_recovery, &state.seg_count, state.segments);
	state.chunk = 0;
	state.chunk_blks = state.seg_count * Mcd_rec_update_segment_blks;
	state.num_chunks = (state.num_blks + state.chunk_blks - 1) / state.chunk_blks;
	// FIXME
	context->osd_buf = state.segments[0];
	mcd_rec_shard_t *pshard = s->pshard;
	mcd_rec_ckpt_t *ckpt = s->ckpt;
	ulong reclogblks = VAR_BLKS_TO_META_BLKS( pshard->rec_log_blks);
	old_log_state.log = 0;
	old_log_state.start_blk = 0;
	old_log_state.num_blks = reclogblks;
	old_log_state.high_LSN = 0;
	old_log_state.seg_count = s->log->segment_count;
	old_log_state.segments = s->log->segments;
	// get LSN of page 0 from both logs
	LSN[0] = read_log_page(context, s, 0, 0);
	LSN[1] = read_log_page(context, s, 1, 0);
	old_log_state.log = 1;	// Assume log 1 is older
	// compare LSNs to find older log
	if (LSN[0] < LSN[1])
		old_log_state.log = 0;	// Log 0 is older
	// no valid checkpoint (transient condition)
	if (ckpt->LSN == 0) {
		// case 1: both logs empty, nothing to recover
		if (LSN[old_log_state.log] == 0 && LSN[1 - old_log_state.log] == 0) {
			s->rec_upd_running = 0;
			detach_buffer_segments( s, state.seg_count, state.segments);
			context->osd_buf = 0;
			return;
		}
		// case 2: old log empty, new log not empty
		else if (LSN[old_log_state.log] == 0)
			old_merged = true;	// new_merged = false;
		// case 3: both logs not empty
		else
			old_merged = false;	// new_merged = false;
	}
	// checkpoint exists
	else {
		int ckpt_log = (((ckpt->LSN - 1) / reclogblks) % MCD_REC_NUM_LOGS);
		// case 5: ckpt in old log, new log not empty
		if (ckpt_log == old_log_state.log)
			old_merged = true;	// new_merged = false;
		// case 4: ckpt in new log, no new log records since ckpt
		else if (LSN[ckpt_log] <= ckpt->LSN)
			old_merged = true;	// new_merged = true;
		// case 6: ckpt in new log, new log records since ckpt
		else
			old_merged = false;	// new_merged = false;
		/* #11425 - scan 1st log regardless so the "st" filter works */
		ckpt->LSN = 0;
		old_merged = false;
	}
	if (s->cntr->cguid == VDC_CGUID)
		filter_cs_initialize( &state);
	state.context = context;
	state.shard = s;
	state.high_obj_offset = 0;
	state.low_obj_offset = ~0uL;
	unless (old_merged)
		process_log_storm( context, s, &state, &old_log_state);
	state.pass = 2;
	curr_log_state.log = 1 - old_log_state.log;
	curr_log_state.start_blk = 0;
	curr_log_state.num_blks = reclogblks;
	curr_log_state.high_LSN = 0;
	curr_log_state.seg_count = l->segment_count;
	curr_log_state.segments = l->segments;
	state.high_obj_offset = 0;
	state.low_obj_offset = ~0uL;
	filter_cs_swap_log( &state);
	process_log_storm( context, s, &state, &curr_log_state);
	filter_cs_flush( &state);
	/*
	 * generate packets for btree container and stats recovery
	 */
	if (s->cntr->cguid == VDC_CGUID) {
		recovery_packet_save( state.otpacket, context, s);
		stats_packet_save( &state, context, s);
		plat_free( state.statbuf);
		plat_free( state.otpacket);
	}
	mcd_rec2_potcache_save( s, context);
	mcd_rec2_potbitmap_save( s, context);
	mcd_rec2_slabbitmap_save( s, context);
	slabbm_t *slabbm = l->slabbm;
	mcd_fth_osd_slab_load_slabbm( context, s, slabbm->bits, slabbm->nbit);
	for (ulong i=0; i<potbm->nbit; ++i)
		if (mcd_rec2_potbitmap_query( s, i)) {
			ulong bno = 0;
			while (bno < device_blocks_per_segment) {
				const ulong blk_offset = i*device_blocks_per_segment + bno;
				const mcd_rec_flash_object_t *e = mcd_rec2_potcache_access( s, context, blk_offset);
				unless (e)
					plat_abort( );
				if ((e->blocks)
				and (blk_offset < s->total_blks)) {
					update_hash_entry( s, context, blk_offset, e);
					bno += power_of_two_roundup( e->blocks);
				}
				else
					bno += 1;
			}
		}
	if (curr_log_state.high_LSN > ckpt->LSN) {
		plat_assert( curr_log_state.high_LSN >= old_log_state.high_LSN);
		recovery_checkpoint( context, s, curr_log_state.high_LSN);
	}
	detach_buffer_segments( s, state.seg_count, state.segments);
	context->osd_buf = NULL;
}


/*
 * merge specified log into the POT
 */
static void
merge_log( mcd_osd_shard_t *s, osd_state_t *context, char **buf_segments, int lognum)
{
	mcd_rec_obj_state_t	state;
	mcd_rec_log_state_t	old_log_state;
	mcd_rec_log_state_t	curr_log_state;

	mcd_rec_shard_t *pshard = s->pshard;
	mcd_rec_ckpt_t *ckpt = s->ckpt;
	mcd_rec_log_t *log = s->log;
	uint64_t reclogblks = VAR_BLKS_TO_META_BLKS(pshard->rec_log_blks);
	s->rec_num_updates += 1;
	s->rec_upd_running = 1;
	s->rec_log_reads_cum += s->rec_log_reads;
	s->rec_table_reads_cum += s->rec_table_reads;
	s->rec_table_writes_cum += s->rec_table_writes;
	s->rec_log_reads = 0;
	s->rec_table_reads = 0;
	s->rec_table_writes = 0;
	memset( &state, 0, sizeof( mcd_rec_obj_state_t ) );
	memset( &old_log_state, 0, sizeof( mcd_rec_log_state_t ) );
	memset( &curr_log_state, 0, sizeof( mcd_rec_log_state_t ) );
	state.in_recovery = 0;
	state.pass = 1;
	state.passes = state.in_recovery ? 2 : 1;
	state.start_blk = 0;
	state.num_blks = VAR_BLKS_TO_META_BLKS(pshard->rec_table_blks);
	state.start_obj = 0;
	state.num_objs = 0;
	state.seg_objects = (Mcd_rec_update_segment_size / sizeof( mcd_rec_flash_object_t ));
	state.seg_count = 0;
	state.segments = buf_segments;
	// attach object table buffer segments for this update/recovery
	attach_buffer_segments( s, state.in_recovery, &state.seg_count, state.segments );
	state.chunk = 0;
	state.chunk_blks = state.seg_count * Mcd_rec_update_segment_blks;
	state.num_chunks = (state.num_blks + state.chunk_blks - 1) / state.chunk_blks;
	context->osd_buf = state.segments[0];
	old_log_state.log = lognum;
	old_log_state.start_blk = 0;
	old_log_state.num_blks = reclogblks;
	old_log_state.high_LSN = 0;
	old_log_state.seg_count = log->segment_count;
	old_log_state.segments = log->segments;
	state.context = context;
	state.shard = s;
	state.high_obj_offset = 0;
	state.low_obj_offset = ~0uL;
	process_log_storm( context, s, &state, &old_log_state);
	mcd_rec2_potcache_save( s, context);
	mcd_rec2_potbitmap_save( s, context);
	mcd_rec2_slabbitmap_save( s, context);
	if (old_log_state.high_LSN > ckpt->LSN)
		recovery_checkpoint( context, s, old_log_state.high_LSN);
	detach_buffer_segments( s, state.seg_count, state.segments);
	context->osd_buf = NULL;
}


/*
 * main loop for updater thread in Storm Mode
 */
void
updater_thread2( void *arg)
{

	mcd_osd_shard_t *s = arg;
	osd_state_t *context = context_alloc( SSD_AIO_CTXT_MCD_REC_UPDT);
	if (context->osd_buf) {
		mcd_fth_osd_iobuf_free( context->osd_buf);
		context->osd_buf = 0;
	}
	unless ((mcd_rec2_slabbitmap_load( s, context))
	and (mcd_rec2_potbitmap_load( s, context))
	and (mcd_rec2_potcache_init( s, context)))
		plat_abort( );
	mcd_rec_log_t *log = s->log;
	int real_log = -1;
	ulong buf_size = ((Mcd_rec_update_bufsize / Mcd_rec_update_segment_size) * sizeof(char *));
	char **buf_segments = plat_alloc(buf_size);
	memset( buf_segments, 0, buf_size);
	(void) __sync_add_and_fetch( &s->refcount, 1);
	log->updater_started = 1;
	loop {
		mcd_rec_update_t *mail = (mcd_rec_update_t *)fthMboxWait( &log->update_mbox);
		unless (mail)
			break;
		if (mail->log == -1) {
			if (mail->updated_mbox)
				fthMboxPost( mail->updated_mbox, 0);
			continue;
		}
		if (mail->in_recovery)
			recover( s, context, buf_segments);
		else {
			if (real_log < 0)
				real_log = mail->log;
			else {
				unless (mail->log == real_log)
					msg( 170042, DIAGNOSTIC, "Bogus log # from log_writer");
				mail->log = real_log;			// always ignore incoming log #
			}
			real_log = (real_log+1) % MCD_REC_NUM_LOGS;
			merge_log( s, context, buf_segments, mail->log);
		}
		s->rec_upd_running = 0;
		if (mail->updated_sem)
			fthSemUp( mail->updated_sem, 1);
		if (mail->updated_mbox)
			fthMboxPost( mail->updated_mbox, 0);
	}
	(void) __sync_sub_and_fetch( &s->refcount, 1);
	log->updater_started = 0;
	fthMboxPost( &log->update_stop_mbox, 0);
	plat_free( buf_segments);
	context_free( context);
}


/*
 * apply a logrec to the POT
 */
int
apply_log_record_mp_storm( mcd_rec_obj_state_t *state, mcd_logrec_object_t *rec)
{

	mcd_osd_shard_t *s = state->shard;
	if (rec->raw) {
		if (rec->blocks) {
			slab_bitmap_set( s, rec->mlo_blk_offset);
			if (rec->mlo_old_offset)
				slab_bitmap_clear( s, ~( rec->mlo_old_offset) & 0x0000ffffffffffffull);
		}
		else
			slab_bitmap_clear( s, rec->mlo_blk_offset);
		return (1);
	}
	mcd_rec_flash_object_t *e = mcd_rec2_potcache_access( s, state->context, rec->mlo_blk_offset);
	if (rec->blocks) {
		unless (state->in_recovery) {
			unless ((e->blocks == 0)
			and (e->obucket == 0)
			and (e->osyndrome == 0)
			and (e->tombstone == 0)
			and (e->seqno == 0)) {
				msg( 160271, FATAL, "rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%lu, ooff=%lu, seq=%lu, tseq=%lu, obj: syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, toff=%lu, seq=%lu, hwm_seqno=%lu", rec->syndrome, rec->blocks, rec->deleted, rec->rbucket, (ulong)rec->mlo_blk_offset, (ulong)rec->mlo_old_offset, (uint64_t) rec->seqno, (ulong)rec->target_seqno, e->osyndrome, e->tombstone, e->blocks, e->deleted, e->obucket, 0L, (uint64_t) e->seqno, 0uL);
				plat_abort();
			}
		}
		e->osyndrome = rec->syndrome;
		e->deleted = rec->deleted;
		e->blocks = rec->blocks;
		e->obucket = rec->rbucket;
		e->cntr_id = rec->cntr_id;
		e->seqno = rec->seqno;
		e->tombstone = 0;
		if (rec->mlo_old_offset)
			delete_object( mcd_rec2_potcache_access( s, state->context, (~ rec->mlo_old_offset) & 0x0000ffffffffffffull));
	}
	else {
		unless (state->in_recovery) {
			unless ((e->obucket/Mcd_osd_bucket_size == rec->rbucket/Mcd_osd_bucket_size)
			and (e->osyndrome == rec->syndrome)
			and (rec->target_seqno==0 || rec->target_seqno==e->seqno || s->evict_to_free)) {
				msg( 160271, FATAL, "rec: syn=%u, blocks=%u, del=%u, bucket=%u, " "boff=%lu, ooff=%lu, seq=%lu, tseq=%lu, obj: " "syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, " "toff=%lu, seq=%lu, hwm_seqno=%lu", rec->syndrome, rec->blocks, rec->deleted, rec->rbucket, (ulong)rec->mlo_blk_offset, (ulong)rec->mlo_old_offset, (uint64_t) rec->seqno, (ulong)rec->target_seqno, e->osyndrome, e->tombstone, e->blocks, e->deleted, e->obucket, 0L, (uint64_t) e->seqno, 0uL);
				plat_abort();
			}
		}
		delete_object( e);
	}
	return (1);
}


void
mcd_fth_osd_slab_load_slabbm( osd_state_t *context, mcd_osd_shard_t *shard, uchar bits[], ulong nbit)
{

	uint64_t blk_offset = 0;
	for (ulong i=0; i<nbit; ++i) {
		const uint bPERB = 8;
		if (bits[i/bPERB] & 1<<i%bPERB) {
			mcd_osd_segment_t *segment = shard->segment_table[blk_offset/Mcd_osd_segment_blks];

			if(!segment)
			{
				uint64_t seg_blk_offset = (blk_offset / Mcd_osd_segment_blks) * Mcd_osd_segment_blks;
				mcd_osd_slab_class_t* seg_class = &shard->slab_classes[shard->class_table[device_blocks_per_storm_object]];
				segment = mcd_osd_assign_segment(shard, seg_class, seg_blk_offset);
				mcd_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_LEVEL_INFO, "persisting segment on recovery(slabbm), shard_id=%ld seg_offset=%lu, obj_offs=%lu, slab_size=%d", shard->id, seg_blk_offset, blk_offset, seg_class->slab_blksize );
				plat_assert(segment);
			}

			uint64_t blks = shard->slab_classes[shard->class_table[device_blocks_per_storm_object]].slab_blksize;
			shard->blk_consumed += blks;
			shard->num_objects += 1;
			shard->total_objects += 1;
			mcd_osd_slab_class_t *class = segment->class;
			//int class_index = shard->class_table[ class->slab_blksize];
			class->used_slabs += 1;
			// update bitmap for class segment
			plat_assert( segment->class == class);
			uint64_t map_offset = (blk_offset - segment->blk_offset) / class->slab_blksize;
			segment->mos_bitmap[ map_offset / 64] |=
				Mcd_osd_bitmap_masks[ map_offset % 64];
#if 0
			segment->alloc_map[ map_offset / 64] |=
				Mcd_osd_bitmap_masks[ map_offset % 64]; // used by full backup
#endif
			segment->used_slabs += 1;
			// if this segment is the last segment allocated for this class
			// then keep track of the highest slab allocated
			if (segment == class->segments[ class->num_segments - 1]) {
				uint temp = map_offset + 1;
				if (class->slab_blksize * Mcd_osd_blk_size <= Mcd_aio_strip_size)
					temp = (map_offset % (Mcd_aio_strip_size/(class->slab_blksize*Mcd_osd_blk_size)) + 1) * Mcd_aio_num_files;
				if (temp > segment->next_slab)
					segment->next_slab = temp;
				plat_assert_always( segment->next_slab <= class->slabs_per_segment);
			}
			else
				segment->next_slab = class->slabs_per_segment;
		}
		blk_offset += device_blocks_per_storm_object;
	}
}


uint64_t
get_regobj_storm_mode( )
{

	return (roundup( mcd.leaves_per_array*regobj_scale_pct/100, Mcd_osd_segment_blks));
}
