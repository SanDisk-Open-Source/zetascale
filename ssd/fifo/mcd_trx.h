

typedef enum {
	MCD_TRX_OKAY,
	MCD_TRX_TOO_MANY,
	MCD_TRX_NO_TRANS,
	MCD_TRX_NO_MEM,
	MCD_TRX_TOO_BIG,
	MCD_TRX_BAD_SHARD,
	MCD_TRX_HASHTABLE_FULL
} mcd_trx_t;

typedef struct {
	uint64_t	transactions,
			operations,
			failures;
} mcd_trx_stats_t;

uint64_t	mcd_trx_id( void);
mcd_trx_t	mcd_trx_start( void),
		mcd_trx_commit( void *),
		mcd_trx_rollback( void *);
mcd_trx_stats_t	mcd_trx_get_stats( void);
void		mcd_trx_print_stats( FILE *);
