

/*
 * commands for trx callback from btree
 *
 * These commands are used by btree to coordinate with the transaction
 * facility.
 *
 *	TRX_ENABLED
 *		If the trx facility is enabled, 1 is returned, otherwise
 *		0.  The setting is controlled by FDF integer property
 *		"FDF_TRX", bit 0.
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
	TRX_CACHE_ADD,
	TRX_CACHE_QUERY,
	TRX_CACHE_DEL
};
