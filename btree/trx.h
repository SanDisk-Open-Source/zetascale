

void		trxinit( ),
		trxenter( FDF_cguid_t),
		trxleave( FDF_cguid_t),
		trxdeletecontainer( struct FDF_thread_state *, FDF_cguid_t),
		trxtrack( FDF_cguid_t, uint64_t, void *),
		trx_cmd_cb( int, void *, void *);
FDF_status_t	trxstart( struct FDF_thread_state *),
		trxcommit( struct FDF_thread_state *),
		trxrollback( struct FDF_thread_state *),
		trxquit( struct FDF_thread_state *);
uint64_t	trxid( struct FDF_thread_state *);
