/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */



typedef enum {
	MCD_TRX_OKAY,
	MCD_TRX_TOO_MANY,
	MCD_TRX_TRANS_ACTIVE,
	MCD_TRX_NO_TRANS,
	MCD_TRX_NO_MEM,
	MCD_TRX_TOO_BIG,
	MCD_TRX_BAD_SHARD,
	MCD_TRX_HASHTABLE_FULL,
	MCD_TRX_BAD_CMD
} mcd_trx_t;

typedef struct {
	uint64_t	transactions,
			operations,
			failures;
} mcd_trx_stats_t;

uint64_t	mcd_trx_id( void);
mcd_trx_t	mcd_trx_start( void),
		mcd_trx_commit( void *),
		mcd_trx_rollback( void *),
		mcd_trx_detach( ),
		mcd_trx_attach( uint64_t),
		mcd_trx_commit_id( void *, uint64_t),
		mcd_trx_service( void *, int, void *);
mcd_trx_stats_t	mcd_trx_get_stats( void);
void		mcd_trx_print_stats( FILE *);
