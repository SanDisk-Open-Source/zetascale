

/*
 * POT element
 *
 * Currently equivalent to mcd_rec_flash_object_t, but likely to diverge.
 */
struct mcdpotelemstructure {
	uint16_t	osyndrome,	// 16-bit syndrome
			tombstone:1,	// 1=entry is a tombstone
			deleted:1,	// 1=marked for delete-in-future
			reserved:2,	// reserved
			blocks:12;	// number of 512-byte blocks occupied
	uint32_t	obucket;	// hash bucket
	uint64_t	cntr_id:16,	// container id
			seqno:48;	// sequence number
};


ulong		mcd_rec2_standard_slab_segments( ulong),
		mcd_rec2_log_size( ulong),
		mcd_rec2_potbitmap_size( ulong),
		mcd_rec2_slabbitmap_size( ulong);
bool		mcd_rec2_init( ulong),
		mcd_rec2_potcache_init( mcd_osd_shard_t *),
		mcd_rec2_potcache_save( mcd_osd_shard_t *, void *),
		mcd_rec2_potbitmap_load( mcd_osd_shard_t *, void *),
		mcd_rec2_potbitmap_save( mcd_osd_shard_t *, void *),
		mcd_rec2_potbitmap_query( mcd_osd_shard_t *, uint),
		mcd_rec2_slabbitmap_load( mcd_osd_shard_t *, void *),
		mcd_rec2_slabbitmap_save( mcd_osd_shard_t *, void *);
void		mcd_rec2_potbitmap_set( mcd_osd_shard_t *, uint),
		mcd_rec2_shutdown( mcd_osd_shard_t *);
uint8_t check_storm_mode();
uint64_t get_rawobjsz();
uint64_t get_regobj_storm_mode();
