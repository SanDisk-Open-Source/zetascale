

#define	trxstart( ts)		(trxenabled? _trxstart( ts): FDF_SUCCESS)
#define	trxcommit( ts)		(trxenabled? _trxcommit( ts): FDF_SUCCESS)

#ifdef TRX_20_ACID
#define	trxenter( cg)		(trxenabled? _trxenter( cg): FDF_SUCCESS)
#define	trxleave( cg)		(trxenabled? _trxleave( cg): FDF_SUCCESS)
#define	trxtrackwrite( cg, n)	(trxenabled? _trxtrackwrite( cg, n): FDF_SUCCESS)
#define	trxtrackread( cg, n)	(trxenabled? _trxtrackread( cg, n): FDF_SUCCESS)
#else
#define	trxenter( cg)		((void) FDF_SUCCESS)
#define	trxleave( cg)		((void) FDF_SUCCESS)
#define	trxtrackwrite( cg, n)	((void) FDF_SUCCESS)
#define	trxtrackread( cg, n)	((void) FDF_SUCCESS)
#endif


void		trxinit( ),
		trxdeletecontainer( struct FDF_thread_state *, FDF_cguid_t);
int		trx_cmd_cb( int, ...);
FDF_status_t	_trxenter( FDF_cguid_t),
		_trxleave( FDF_cguid_t),
		_trxstart( struct FDF_thread_state *),
		_trxcommit( struct FDF_thread_state *),
		trxrollback( struct FDF_thread_state *),
		trxquit( struct FDF_thread_state *),
		_trxtrackwrite( FDF_cguid_t, uint64_t),
		_trxtrackread( FDF_cguid_t, uint64_t);
uint64_t	trxid( struct FDF_thread_state *);

int		trxenabled;
