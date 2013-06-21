

/*
 * transaction support for btree
 *
 * Full isolation between trx is provided; solo writes are wrapped in a trx.
 * Outside of a bracketing trx, point queries and range queries may return
 * "dirty reads".
 */

#include	<assert.h>
#include	<pthread.h>
#include	"fdf.h"
#include	"platform/rwlock.h"
#include	"btree_hash.h"
#include	"btree_raw.h"
#include	"utils/rico.h"
#include	"trx.h"


enum {
	OKAY
};

typedef struct {
	void		*next;
	uint		refc;
	FDF_cguid_t	c;
	uint64_t	nid;
} listelem_t;
typedef struct {
	FDF_cguid_t	c;
	uint64_t	nid;
	uchar		type;	
	listelem_t	*l;
} item_t;
typedef struct {
	struct FDF_thread_state	*ts;
	uchar			level,
				status;
	uint			nitem;
	item_t			items[100000];
} trx_t;


static pthread_spinlock_t
			entrysl		= 1,
			listsl		= 1;
static listelem_t	*listpool,
			*listbase;
static plat_rwlock_t	entrylock;
static uint		entrycounter;
static __thread trx_t	*trx;

static listelem_t	*listfind( listelem_t *, FDF_cguid_t, uint64_t),
			*listadd( listelem_t *, FDF_cguid_t, uint64_t),
			*listdel( listelem_t *, listelem_t *),
			*listalloc( );
static void		trackfree( ),
			listfree( listelem_t *),
			trxdump( char *);


void
trxinit( )
{

	plat_rwlock_init( &entrylock);
}


void
trxenter( FDF_cguid_t c)
{

	plat_rwlock_rdlock( &entrylock);
	__sync_add_and_fetch( &entrycounter, 1);
}


void
trxleave( FDF_cguid_t c)
{

	__sync_add_and_fetch( &entrycounter, -1);
	plat_rwlock_unlock( &entrylock);
}


/*
 * (No nested support right now)
 */
FDF_status_t
trxstart( struct FDF_thread_state *t)
{

	if (trx) {
		++trx->level;
		return (FDF_SUCCESS);
	}
	unless (trx = malloc( sizeof *trx))
		return (FDF_OUT_OF_MEM);
	trx->ts = t;
	trx->level = 0;
	trx->status = OKAY;
	trx->nitem = 0;
	FDF_status_t s = FDFTransactionStart( t);
	if (s == FDF_SUCCESS)
		++trx->level;
	else {
		free( trx);
		trx = 0;
	}
	trxdump( "trxstart");
	return (s);
}


FDF_status_t 
trxcommit( struct FDF_thread_state *t)
{

	unless (trx)
		return (FDF_FAILURE);
	trxdump( "trxcommit");
	if (--trx->level)
		return (FDF_SUCCESS);
	FDF_status_t s = FDFTransactionCommit( t);
	trackfree( );
	trx = 0;
	return (s);
}


FDF_status_t 
trxrollback( struct FDF_thread_state *t)
{

	unless (trx)
		return (FDF_FAILURE);
	trxdump( "trxrollback");
	if (--trx->level)
		return (FDF_SUCCESS);
	FDF_status_t s = FDFTransactionRollback( t);
	trackfree( );
	trx = 0;
	return (s);
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


void
trxdeletecontainer( struct FDF_thread_state *t, FDF_cguid_t c)
{

}


void
trxtrack( FDF_cguid_t c, uint64_t nid, void *d)
{

	if ((trx)
	and (trx->nitem < nel( trx->items))) {
		uint i = 0;
		loop {
			if (i == trx->nitem) {
				pthread_spin_lock( &listsl);
				listelem_t *l = listfind( listbase, c, nid);
				unless (l) {
					listbase = listadd( listbase, c, nid);
					l = listbase;
				}
				++l->refc;
				trx->items[i].c = c;
				trx->items[i].nid = nid;
				trx->items[i].type = 0;	
				trx->items[i].l = l;	
				++trx->nitem;
				pthread_spin_unlock( &listsl);
				break;
			}
			if ((trx->items[i].c == c)
			and (trx->items[i].nid == nid))
				break;
			++i;
		}
	}
}


static void
trackfree( )
{
	uint	i;

	for (i=0; i<trx->nitem; ++i) {
		listelem_t *l = trx->items[i].l;
		pthread_spin_lock( &listsl);
		unless (--l->refc)
			listbase = listdel( listbase, l);
		pthread_spin_unlock( &listsl);
	}
}


static listelem_t	*
listfind( listelem_t *base, FDF_cguid_t c, uint64_t nid)
{
	listelem_t	*l;

	for (l=base; l; l=l->next)
		if ((l->c == c)
		and (l->nid == nid))
			break;
	return (l);
}


static listelem_t	*
listadd( listelem_t *base, FDF_cguid_t c, uint64_t nid)
{
	listelem_t	*l;

	if (l = listalloc( )) {
		l->refc = 0;
		l->c = c;
		l->nid = nid;
		l->next = base;
	}
	return (l);
}


static listelem_t	*
listdel( listelem_t *base, listelem_t *l)
{

	if (l == base) {
		base = base->next;
		listfree( l);
	}
	else {
		listelem_t *a = base;
		while (a->next) {
			if (a->next == l) {
				a->next = l->next;
				break;
			}
			a = a->next;
		}
	}
	return (base);
}


static listelem_t	*
listalloc( )
{

	listelem_t *l = listpool;
	if (l)
		listpool = l->next;
	else
		l = malloc( sizeof *l);
	return (l);
}


static void
listfree( listelem_t *l)
{

	l->next = listpool;
	listpool = l;
	
}


void
trx_cmd_cb( void *cb_data, int cmd_type)
{

}


// *** hash table service here ***
//
//ub8 btree_hash( k, length, level)
//register const ub1 *k;        /* the key */
//register ub8  length;   /* the length of the key */
//register ub8  level;    /* the previous hash, or an arbitrary value */


static void
trxdump( char *label)
{
#if 0
	uint	i;

	if (trx) {
		printf( "%9s: trxid=%ld level=%d status=%d nitem=%d", label, trxid( trx->ts), trx->level, trx->status, trx->nitem);
		for (i=0; i<trx->nitem; ++i)
			printf( " (%ld,%ld,%d)", trx->items[i].c, trx->items[i].nid, trx->items[i].type);
		printf( "\n");
	}
	else
		printf( "trxdump: no active trx\n");
#endif
}
