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



#define	trxstart( ts)		(trxenabled? _trxstart( ts): ZS_SUCCESS)
#define	trxcommit( ts)		(trxenabled? _trxcommit( ts): ZS_SUCCESS)

#ifdef TRX_20_ACID
#define	trxenter( cg)		(trxenabled? _trxenter( cg): ZS_SUCCESS)
#define	trxleave( cg)		(trxenabled? _trxleave( cg): ZS_SUCCESS)
#define	trxtrackwrite( cg, n)	(trxenabled? _trxtrackwrite( cg, n): ZS_SUCCESS)
#define	trxtrackread( cg, n)	(trxenabled? _trxtrackread( cg, n): ZS_SUCCESS)
#else
#define	trxenter( cg)		((void) ZS_SUCCESS)
#define	trxleave( cg)		((void) ZS_SUCCESS)
#define	trxtrackwrite( cg, n)	((void) ZS_SUCCESS)
#define	trxtrackread( cg, n)	((void) ZS_SUCCESS)
#endif


void		trxinit( ),
		trxdeletecontainer( struct ZS_thread_state *, ZS_cguid_t);
int		trx_cmd_cb( int, ...);
ZS_status_t	_trxenter( ZS_cguid_t),
		_trxleave( ZS_cguid_t),
		_trxstart( struct ZS_thread_state *),
		_trxcommit( struct ZS_thread_state *),
		trxrollback( struct ZS_thread_state *),
		trxquit( struct ZS_thread_state *),
		_trxtrackwrite( ZS_cguid_t, uint64_t),
		_trxtrackread( ZS_cguid_t, uint64_t);
uint64_t	trxid( struct ZS_thread_state *);

int		trxenabled;
