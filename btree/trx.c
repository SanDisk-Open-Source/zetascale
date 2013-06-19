

/*
 * transaction support for btree
 *
 * Full isolation between trx is provided; solo writes are wrapped in a trx.
 * Outside of a bracketing trx, point queries and range queries may return
 * "dirty reads".
 */

#include	"fdf.h"
#include	"trx.h"


FDF_status_t
trxstart( struct FDF_thread_state *t)
{

	return (FDFTransactionStart( t));
}


FDF_status_t 
trxcommit( struct FDF_thread_state *t)
{

	return (FDFTransactionCommit( t));
}


FDF_status_t 
trxrollback( struct FDF_thread_state *t)
{

	return (FDFTransactionRollback( t));
}


FDF_status_t 
trxquit( struct FDF_thread_state *t)
{

	return (FDFTransactionQuit( t));
}


uint64_t
trxid( struct FDF_thread_state *t)
{

	return (FDFTransactionID( t));
}
