

/*
 * commands for trx callback from btree
 *
 * These commands are used by btree to coordinate with the transaction
 * facility.
 *
 *	TRX_ENABLED
 *		If the trx facility is enabled, 1 is returned, otherwise
 *		0.  The setting is controlled by ZS integer property
 *		"ZS_TRX", bit 0.
 *	TRX_START
 *		Start trx.  Nesting and roll back is not supported.
 *	TRX_COMMIT
 *		Commit trx.
 *	TRX_START_MULTI
 *		Start a multiple put.  Multiple trx can subsequently be
 *		started and committed.	Must be balanced by a concluding
 *		TRX_COMMIT_MULTI call.
 *	TRX_COMMIT_MULTI
 *		To be called after the last trx of the current mput
 *		sequence is committed with TRX_COMMIT.
 *
 * Miscellaneous services.
 *
 *	TRX_SEQNOALLOC
 *		Allocate a 64-bit eternal sequence number.
 *
 * Following calls are for ACID supported.
 *
 *	TRX_CACHE_ADD
 *		Inform the trx facility that the object specified
 *		by cguid/node is being entered into the L1 cache.
 *		Return value is always 0.
 *	TRX_CACHE_DEL
 *		Inform the trx facility that the object specified
 *		by cguid/node is being purged from the L1 cache.
 *		Return value is always 0.
 *	TRX_CACHE_QUERY
 *		Query the trx facility about validity of the object
 *		specified by cguid/node.  If 0 is returned, the object is
 *		not valid, and must be purged from L1 by the btree layer.
 *		If 1 is returned, the object in L1 is valid.
 */

enum trx_cmd {
	TRX_ENABLED,
	TRX_START,
	TRX_COMMIT,
	TRX_START_MULTI,
	TRX_COMMIT_MULTI,
	TRX_SEQNOALLOC,
	TRX_CACHE_ADD,
	TRX_CACHE_QUERY,
	TRX_CACHE_DEL,
	TRX_CMD___END
};
