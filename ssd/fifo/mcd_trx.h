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
