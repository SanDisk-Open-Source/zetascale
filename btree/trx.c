

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
#include	"utils/rico.h"
#include	"trx.h"


enum trxstate {
	PROGRESSING,
	COMMITTING,
	ROLLINGBACK,
	SUCCESSFUL,
	FAILED
};

typedef struct listelem		listelem_t;
typedef struct itemstructure	item_t;
typedef struct trxstructure	trx_t;
typedef pthread_spinlock_t	spinlock_t;
typedef pthread_mutex_t		mutex_t;
typedef struct FDF_thread_state	thread_state_t;

struct listelem {
	listelem_t	*next;
	uint		refc;
	FDF_cguid_t	c;
	uint64_t	nid;
};
struct itemstructure {
	FDF_cguid_t	c;
	uint64_t	nid;
	uchar		type;	
	listelem_t	*l;
};
struct trxstructure {
	thread_state_t	*ts;
	ushort		level;
	enum trxstate	state;
	uint		nitem;
	item_t		items[100000];
	trx_t		*anext;
	mutex_t		endlock;
};


static spinlock_t	listsl		= 1;
static listelem_t	*listpool,
			*listbase;
static plat_rwlock_t	entrylock;
static mutex_t		activetrxlock		= PTHREAD_MUTEX_INITIALIZER;
static trx_t		*activetrxlist;
static uint		activetrxtotal,
			activetrxconcluding;
static __thread trx_t	*trx;

static listelem_t	*listfind( listelem_t *, FDF_cguid_t, uint64_t),
			*listadd( listelem_t *, FDF_cguid_t, uint64_t),
			*listdel( listelem_t *, listelem_t *),
			*listalloc( );
static void		trackfree( ),
			listfree( listelem_t *),
			dump( char *);
static FDF_status_t	conclude( ),
			concludeself( );
static uint64_t		reltime( );
static trx_t		*activetrxlistadd( trx_t *, trx_t *),
			*activetrxlistdel( trx_t *, trx_t *);


void
trxinit( )
{

	reltime( );
	plat_rwlock_init( &entrylock);
}


void
trxenter( FDF_cguid_t c)
{

	plat_rwlock_rdlock( &entrylock);
}


void
trxleave( FDF_cguid_t c)
{

	plat_rwlock_unlock( &entrylock);
}


FDF_status_t
trxstart( thread_state_t *ts)
{

	if (trx) {
		++trx->level;
		return (FDF_SUCCESS);
	}
	unless (trx = malloc( sizeof *trx))
		return (FDF_OUT_OF_MEM);
	trx->ts = ts;
	trx->level = 0;
	trx->state = PROGRESSING;
	trx->nitem = 0;
	pthread_mutex_init( &trx->endlock, 0);
	pthread_mutex_lock( &trx->endlock);
	FDF_status_t s = FDFTransactionStart( ts);
	if (s == FDF_SUCCESS) {
		++trx->level;
		pthread_mutex_lock( &activetrxlock);
		activetrxlist = activetrxlistadd( activetrxlist, trx);
		++activetrxtotal;
		pthread_mutex_unlock( &activetrxlock);
	}
	else {
		free( trx);
		trx = 0;
	}
	dump( "trxstart");
	return (s);
}


FDF_status_t 
trxcommit( thread_state_t *ts)
{

	unless (trx)
		return (FDF_FAILURE);
	dump( "trxcommit");
	if (--trx->level)
		return (FDF_SUCCESS);
	trx->state = COMMITTING;
	FDF_status_t s = conclude( );
	free( trx);
	trx = 0;
	return (s);
}


/*
 * (No nested rollback right now)
 */
FDF_status_t 
trxrollback( thread_state_t *ts)
{

	unless (trx)
		return (FDF_FAILURE);
	dump( "trxrollback");
	if (--trx->level)
		return (FDF_SUCCESS);
	trx->state = ROLLINGBACK;
	FDF_status_t s = conclude( );
	free( trx);
	trx = 0;
	return (s);
}


FDF_status_t 
trxquit( thread_state_t *ts)
{

	return (FDFTransactionQuit( ts));
}


uint64_t
trxid( thread_state_t *ts)
{

	return (FDFTransactionID( ts));
}


void
trxdeletecontainer( thread_state_t *ts, FDF_cguid_t c)
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


static FDF_status_t
conclude( )
{
	trx_t	*t;

	pthread_mutex_lock( &activetrxlock);
	if (++activetrxconcluding < activetrxtotal) {
		pthread_mutex_unlock( &activetrxlock);
		return (concludeself( ));
	}
	plat_rwlock_wrlock( &entrylock);
	while (t = activetrxlist) {
		activetrxlist = activetrxlistdel( activetrxlist, t);
		pthread_mutex_unlock( &t->endlock);
	}
	FDF_status_t s = concludeself( );
	plat_rwlock_unlock( &entrylock);
	pthread_mutex_unlock( &activetrxlock);
	return (s);
}


static FDF_status_t
concludeself( )
{
	FDF_status_t	s;

	pthread_mutex_lock( &trx->endlock);
	if (trx->state == COMMITTING)
		s = FDFTransactionCommit( trx->ts);
	else
		s = FDFTransactionRollback( trx->ts);
	trackfree( );
	return (s);
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


static trx_t	*
activetrxlistadd( trx_t *alist, trx_t *a)
{

	a->anext = alist;
	return (a);
}


static trx_t	*
activetrxlistdel( trx_t *alist, trx_t *a)
{

	if (a == alist)
		alist = alist->anext;
	else {
		trx_t *al = alist;
		while (al->anext) {
			if (al->anext == a) {
				al->anext = a->anext;
				break;
			}
			al = al->anext;
		}
	}
	return (alist);
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
trx_cmd_cb( int cmd, void *v0, void *v1)
{

}


/*
 * relative time (ns)
 */
static uint64_t
reltime( )
{
	struct timespec	ts;

	clock_gettime( CLOCK_MONOTONIC, &ts);
	return (ts.tv_sec*1000000000L + ts.tv_nsec);
}


// *** hash table service here ***
//
//ub8 btree_hash( k, length, level)
//register const ub1 *k;        /* the key */
//register ub8  length;   /* the length of the key */
//register ub8  level;    /* the previous hash, or an arbitrary value */


static void
dump( char *label)
{
#if 0//Rico
	uint	i;

	if (trx) {
		printf( "%9s: trxid=%ld level=%d state=%d nitem=%d", label, trxid( trx->ts), trx->level, trx->state, trx->nitem);
		for (i=0; i<trx->nitem; ++i)
			printf( " (%ld,%lu,%d)", trx->items[i].c, trx->items[i].nid, trx->items[i].type);
		printf( "\n");
	}
	else
		printf( "%9s: no active trx\n", label);
#endif
}
