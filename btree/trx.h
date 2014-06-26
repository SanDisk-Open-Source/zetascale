

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
