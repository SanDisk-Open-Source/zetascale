

/*
 * transaction support for btree
 *
 * Full isolation between trx is provided; solo writes are wrapped in a trx.
 * When outside of a bracketing trx, point queries and range queries may
 * return "dirty reads".
 *
 * Data Structures
 *
 * trx_t
 * 	holds main per-trx info
 * 	allocated at trx start by the thread
 * 	deallocated by thread at end of trx
 * 	referenced by:
 * 		owning thread only (before trx resolution)
 * 		scheduler during trx resolution
 * 	linked to other trx_t headed by "activetrxlist"
 * 	"trx" points to per-thread trx_t
 * 	"ntrxstarted" is # trx started (monotonically increasing)
 * 	"ntrxconcluded" is # trx concluded (monotonically increasing)
 * 	ntrxstarted-ntrxconcluded is # trx underway
 * 	has list of trxnode_t to reach all referenced nodes
 * trxnode_t
 * 	list element used by trx_t to identify those node_t accessed
 * 	allocated by thread incoming from btree
 * 	deallocated by thread at trx end
 * 	referenced by:
 * 		owning thread only (before trx resolution)
 * 		scheduler (during trx resolution)
 * node_t
 * 	holds per-node info
 * 	allocated by threads incoming from btree
 * 	deallocated by btree cache purge/invalidation, or trx end 
 * 	organized in a global hashtable
 * 	referenced by:
 *		any thread incoming from btree
 * 		scheduler during trx resolution
 *		any thread at end of trx
 * 	has linked list to reach nodetrx_t
 * nodetrx_t
 * 	list element used by node_t to record node usage by this trx
 * 	allocated by thread incoming from btree when not found
 * 	deallocated by thread at end of trx
 * 	referenced by:
 * 		incoming thread from btree to record usage
 * 		scheduler during trx resolution
 *		any thread at end of trx
 * 	has pointer to reach trx_t
 *
 * Main Data Access Paths
 *
 * 	btree callback:		     (cguid,nid)     (trx)
 *			node hashtable	 ->    node_t ->  nodetrx_t
 *
 *	new node/trx:	      (trx)		    (cguid,nid)
 *			node_t ->  nodetrx_t -> trx_t    ->   trxnode_t
 *	scheduler:
 *			activetrxlist -> trx_t -> trxnode_t -> node_t -> nodetrx_t
 *	end of trx:
 *			trx -> trx_t -> trxnode_t -> node_t -> nodetrx_t
 *
 * Two Operating Phases
 *
 *	Transacting Phase
 *		Multiple threads can enter the trx layer to start/conclude
 *		trx, to coodinate action with the btree cache, and
 *		to record object I/O of the btree layer.  Threads are
 *		blocked only briefly as they access data structures.
 *
 *	Scheduling Phase
 *		Entered when all threads with outstanding trx have entered
 *		the trx layer to conclude their trx.  Waits for all
 *		btrees to go inactive, then locks them.  Threads wanting
 *		to start new trx are blocked.  Schedule evaluates which
 *		trx can be committed, per thread request.  All other will
 *		be rolled back.  Order of trx disposition is determined,
 *		then each trx is concluded serially.
 *
 * Locks
 *
 *	Global "nodetablesl" is used during Transacting Phase to efficiently
 *	serialize all access to tracking data.	Not needed during Scheduling
 *	Phase.

 *	Global "activetrxlock" is used during Transacting Phase to
 *	serialize trx starts and, during Scheduling Phase, to block new
 *	trx starts.
 *
 *	Global "entrylock" is used during Scheduling Phase to prevent
 *	entry into the btree layer while potential roll backs are
 *	underway.  Has no effect during Transacting Phase.
 *
 *	Global "endlock" serializes resolution of trx, so is only used
 *	at the end of the Scheduling Phase.
 *
 *	Per-trx "endlock" serializes conclusion vis-a-vis other trx.
 *
 * To Do:
 *
 *	- propagate malloc failures
 *	- deal with container deletions
 *	- refine the scheduler, a lot
 */

#include	<assert.h>
#include	<pthread.h>
#include	"fdf.h"
#include	"platform/rwlock.h"
#include	"btree_hash.h"
#include	"utils/rico.h"
#include	"trx.h"
#include	"trxcmd.h"


enum trxstate {
	PROGRESSING,
	COMMITTING,
	ROLLINGBACK,
	ABORTING
};
enum cachestate {
	UNCACHED,
	CACHED,
	INVALID
};

typedef struct trxstructure	trx_t;
typedef struct trxnodestructure	trxnode_t;
typedef struct nodestructure	node_t;
typedef struct nodetrxstructure	nodetrx_t;
typedef pthread_spinlock_t	spinlock_t;
typedef pthread_mutex_t		mutex_t;
typedef struct FDF_thread_state	thread_state_t;
typedef uint64_t		timestamp_t;

struct trxstructure {
	trx_t		*next;
	thread_state_t	*ts;
	ushort		level;
	enum trxstate	state;
	trxnode_t	*nlist;
	mutex_t		endlock;
};
struct trxnodestructure {
	trxnode_t	*next;
	node_t		*node;
	nodetrx_t	*nodetrx;
};
struct nodestructure {
	node_t		*next;
	FDF_cguid_t	c;
	uint64_t	nid;
	enum cachestate	cache;
	nodetrx_t	*tlist;
};
struct nodetrxstructure {
	nodetrx_t	*next;
	trx_t		*trx;
	timestamp_t	lo,
			hi;
	bool		written;
};


static plat_rwlock_t	entrylock;
static mutex_t		activetrxlock		= PTHREAD_MUTEX_INITIALIZER,
			endlock			= PTHREAD_MUTEX_INITIALIZER;
static trx_t		*activetrxlist;
static uint		ntrxstarted,
			ntrxconcluded;
static mutex_t		nodetablelock		= PTHREAD_MUTEX_INITIALIZER;
static trx_t		*trxpool;
static trxnode_t	*trxnodepool;
static node_t		*nodetable[1<<23],
			*nodepool;
static nodetrx_t	*nodetrxpool;
static __thread trx_t	*trx;

static bool		conflicting( trx_t *);
static FDF_status_t	conclude( ),
			concludeself( );
static uint64_t		reltime( );
static trx_t		*tadd( trx_t *, trx_t *),
			*tdel( trx_t *, trx_t *),
			*talloc( );
static trxnode_t	*tnsearch( node_t *, nodetrx_t *),
			*tnadd( trxnode_t *, trxnode_t *),
			*tndel( trxnode_t *, trxnode_t *),
			*tnalloc( );
static node_t		*nsearch( FDF_cguid_t c, uint64_t),
			*nadd( node_t *, node_t *),
			*ndel( node_t *, node_t *),
			*nalloc( );
static nodetrx_t	*ntsearch( node_t *),
			*ntadd( nodetrx_t *, nodetrx_t *),
			*ntdel( nodetrx_t *, nodetrx_t *),
			*ntalloc( );
static void		purgecache( ),
			tdestroy( ),
			tfree( trx_t *),
			tnfree( trxnode_t *),
			nfree( node_t *),
			ntfree( nodetrx_t *),
			track( FDF_cguid_t, uint64_t, bool),
			dump( char *);
static uint		hash( FDF_cguid_t, uint64_t);
static timestamp_t	timestamp( );


void
trxinit( )
{

	reltime( );
	pthread_mutex_lock( &endlock);
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
	unless (trx = talloc( ))
		return (FDF_OUT_OF_MEM);
	trx->ts = ts;
	trx->level = 0;
	trx->state = PROGRESSING;
	trx->nlist = 0;
	pthread_mutex_init( &trx->endlock, 0);
	pthread_mutex_lock( &trx->endlock);
	FDF_status_t s = FDFTransactionStart( ts);
	if (s == FDF_SUCCESS) {
		++trx->level;
		pthread_mutex_lock( &activetrxlock);
		activetrxlist = tadd( activetrxlist, trx);
		++ntrxstarted;
		pthread_mutex_unlock( &activetrxlock);
	}
	else {
		tfree( trx);
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
	tfree( trx);
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
	tfree( trx);
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


/*
 * container deleted
 *
 * Called to signal that a container has been deleted.  This action
 * is not permitted if the container is participating in a transaction,
 * but we nonetheless want to protect against corruption from a roll back.
 * One option is to mark this trx as poisoned, return an error to the caller
 * at trx conclusion, and leave the trx unconcluded forever (and no more
 * trx for this thread).
 */
void
trxdeletecontainer( thread_state_t *ts, FDF_cguid_t c)
{

	/* to do */
}


void
trxtrackread( FDF_cguid_t c, uint64_t nid)
{

	track( c, nid, FALSE);
}


void
trxtrackwrite( FDF_cguid_t c, uint64_t nid)
{

	track( c, nid, TRUE);
}


static void
track( FDF_cguid_t c, uint64_t nid, bool written)
{

	if (trx) {
		pthread_mutex_lock( &nodetablelock);
		node_t *n = nsearch( c, nid);
		nodetrx_t *nt = ntsearch( n);
		nt->written |= written;
		nt->hi = timestamp( );
		pthread_mutex_unlock( &nodetablelock);
	}
}


/*
 * trx scheduler
 *
 * Determine dependencies (DAGs), execute roll backs, execute DAGs.
 */
static FDF_status_t
conclude( )
{
	trx_t	*t;

	pthread_mutex_lock( &activetrxlock);
	if (++ntrxconcluded < ntrxstarted) {
		pthread_mutex_unlock( &activetrxlock);
		return (concludeself( ));
	}
	plat_rwlock_wrlock( &entrylock);
	while (t = activetrxlist) {
		activetrxlist = tdel( activetrxlist, t);
		if (conflicting( t))
			t->state = ABORTING;
		unless (t == trx) {
			pthread_mutex_unlock( &t->endlock);
			pthread_mutex_lock( &endlock);
		}
	}
	pthread_mutex_unlock( &trx->endlock);
	FDF_status_t s = concludeself( );
	pthread_mutex_lock( &endlock);
	plat_rwlock_unlock( &entrylock);
	pthread_mutex_unlock( &activetrxlock);
	return (s);
}


static FDF_status_t
concludeself( )
{
	FDF_status_t	s;

	pthread_mutex_lock( &trx->endlock);
	switch (trx->state) {
	case COMMITTING:
		s = FDFTransactionCommit( trx->ts);
		unless (s == FDF_SUCCESS)
			purgecache( );
		break;
	case ROLLINGBACK:
		s = FDFTransactionRollback( trx->ts);
		purgecache( );
		break;
	case ABORTING:
		s = FDFTransactionRollback( trx->ts);
		if (s == FDF_SUCCESS)
			s = FDF_FAILURE;
		purgecache( );
		break;
	default:
		abort( );
	}
	tdestroy( );
	pthread_mutex_unlock( &endlock);
	return (s);
}


/*
 * detect trx conflict
 *
 * Returns TRUE if this transaction conflicts with another.  This is the
 * Release 0.1 Super Junior version of the algorithm.  Locking assumed.
 */
static bool
conflicting( trx_t *t)
{
	trxnode_t	*tn;
	nodetrx_t	*nt;

	for (tn=t->nlist; tn; tn=tn->next) {
		bool iwrote = FALSE;
		bool theywrote = FALSE;
		for (nt=tn->node->tlist; nt; nt=nt->next)
			if (nt->written) {
				if (nt->trx == trx)
					iwrote = TRUE;
				else
					theywrote = TRUE;
			}
		if ((iwrote)
		and (theywrote))
			return (TRUE);
	}
	return (FALSE);
}


static void
purgecache( )
{
	trxnode_t	*tn;

	for (tn=trx->nlist; tn; tn=tn->next)
		tn->node->cache = INVALID;
}


/*
 * fully eliminate current trx
 *
 * Locking assumed.
 */
static void
tdestroy( )
{
	trxnode_t	*tn;

	while (tn = trx->nlist) {
		tn->node->tlist = ntdel( tn->node->tlist, tn->nodetrx);
		ntfree( tn->nodetrx);
		trx->nlist = tndel( trx->nlist, tn);
		tnfree( tn);
	}
}


/*
 * add trx to list
 *
 * Locking assumed.
 */
static trx_t	*
tadd( trx_t *head, trx_t *t)
{

	t->next = head;
	return (t);
}


/*
 * unlink trx from list
 *
 * Locking assumed.
 */
static trx_t	*
tdel( trx_t *head, trx_t *t)
{

	if (t == head)
		head = head->next;
	else {
		trx_t *a = head;
		while (a->next) {
			if (a->next == t) {
				a->next = t->next;
				break;
			}
			a = a->next;
		}
	}
	return (head);
}


/*
 * return fresh trx from pool, or malloc
 *
 * Locking assumed.
 */
static trx_t	*
talloc( )
{

	trx_t *t = trxpool;
	if (t)
		trxpool = t->next;
	else
		t = malloc( sizeof *t);
	return (t);
}


/*
 * free trx to pool
 *
 * Locking assumed.
 */
static void
tfree( trx_t *t)
{

	t->next = trxpool;
	trxpool = t;
}


/*
 * in current trx, find trxnode for node, or create it
 *
 * Locking assumed.
 */
static trxnode_t	*
tnsearch( node_t *n, nodetrx_t *nt)
{

	trxnode_t *tn = trx->nlist;
	loop {
		unless (tn) {
			tn = tnalloc( );
			tn->node = n;
			tn->nodetrx = nt;
			trx->nlist = tnadd( trx->nlist, tn);
			break;
		}
		if (tn->node == n)
			break;
		tn = tn->next;
	}
	return (tn);
}


/*
 * add trxnode to list
 *
 * Locking assumed.
 */
static trxnode_t	*
tnadd( trxnode_t *head, trxnode_t *tn)
{

	tn->next = head;
	return (tn);
}


/*
 * unlink trxnode from list
 *
 * Locking assumed.
 */
static trxnode_t	*
tndel( trxnode_t *head, trxnode_t *tn)
{

	if (tn == head)
		head = head->next;
	else {
		trxnode_t *a = head;
		while (a->next) {
			if (a->next == tn) {
				a->next = tn->next;
				break;
			}
			a = a->next;
		}
	}
	return (head);
}


/*
 * return fresh trxnode from pool, or malloc
 *
 * Locking assumed.
 */
static trxnode_t	*
tnalloc( )
{

	trxnode_t *tn = trxnodepool;
	if (tn)
		trxnodepool = tn->next;
	else
		tn = malloc( sizeof *tn);
	return (tn);
}


/*
 * free trxnode to pool
 *
 * Locking assumed.
 */
static void
tnfree( trxnode_t *tn)
{

	tn->next = trxnodepool;
	trxnodepool = tn;
}


/*
 * find node, or create it
 *
 * Locking assumed.
 */
static node_t	*
nsearch( FDF_cguid_t c, uint64_t nid)
{

	uint h = hash( c, nid) % nel( nodetable);
	node_t *n = nodetable[h];
	loop {
		unless (n) {
			n = nalloc( );
			n->c = c;
			n->nid = nid;
			n->cache = UNCACHED;
			n->tlist = 0;
			nodetable[h] = nadd( nodetable[h], n);
			break;
		}
		if ((n->c == c)
		and (n->nid == nid))
			break;
		n = n->next;
	}
	return (n);
}


/*
 * find node, and fully eliminate it
 *
 * Do nothing if node not found.  The nodetrx list (tlist) should be 0 at
 * this point.  Locking assumed.
 */
static void
ndestroy( FDF_cguid_t c, uint64_t nid)
{
	node_t		*n;

	uint h = hash( c, nid) % nel( nodetable);
	for (n=nodetable[h]; n; n=n->next)
		if ((n->c == c)
		and (n->nid == nid)) {
			if (n->tlist)
				/* hey, you, read the manual */;
			nodetable[h] = ndel( nodetable[h], n);
			nfree( n);
			break;
		}
}


/*
 * add node to list
 *
 * Locking assumed.
 */
static node_t	*
nadd( node_t *head, node_t *n)
{

	n->next = head;
	return (n);
}


/*
 * unlink node from list
 *
 * Locking assumed.
 */
static node_t	*
ndel( node_t *head, node_t *n)
{

	if (n == head)
		head = head->next;
	else {
		node_t *a = head;
		while (a->next) {
			if (a->next == n) {
				a->next = n->next;
				break;
			}
			a = a->next;
		}
	}
	return (head);
}


/*
 * return fresh node from pool, or malloc
 *
 * Space from malloc is chunked for performance.  Locking assumed.
 */
static node_t	*
nalloc( )
{
	node_t	*n;
	uint	i;

	if (n = nodepool) {
		nodepool = n->next;
		return (n);
	}
	const uint chunksize = 30;		// tuned
	n = malloc( chunksize * sizeof *n);
	for (i=0; i<chunksize; ++i)
		nfree( n++);
	return (nalloc( ));
}


/*
 * free node to pool
 *
 * Locking assumed.
 */
static void
nfree( node_t *n)
{

	n->next = nodepool;
	nodepool = n;
}


/*
 * in node, find nodetrx of current trx, or create it
 *
 * Ensure full linkage to trx side.  Locking assumed.
 */
static nodetrx_t	*
ntsearch( node_t *n)
{

	nodetrx_t *nt = n->tlist;
	loop {
		unless (nt) {
			nt = ntalloc( );
			nt->written = FALSE;
			nt->lo = timestamp( );
			nt->hi = nt->lo;
			n->tlist = ntadd( n->tlist, nt);
			nt->trx = trx;
			tnsearch( n, nt);
			break;
		}
		if (nt->trx == trx)
			break;
		nt = nt->next;
	}
	return (nt);
}


/*
 * add nodetrx to list
 *
 * Locking assumed.
 */
static nodetrx_t	*
ntadd( nodetrx_t *head, nodetrx_t *nt)
{

	nt->next = head;
	return (nt);
}


/*
 * unlink nodetrx from list
 *
 * Locking assumed.
 */
static nodetrx_t	*
ntdel( nodetrx_t *head, nodetrx_t *nt)
{

	if (nt == head)
		head = head->next;
	else {
		nodetrx_t *a = head;
		while (a->next) {
			if (a->next == nt) {
				a->next = nt->next;
				break;
			}
			a = a->next;
		}
	}
	return (head);
}


/*
 * return fresh nodetrx from pool, or malloc
 *
 * Locking assumed.
 */
static nodetrx_t	*
ntalloc( )
{

	nodetrx_t *nt = nodetrxpool;
	if (nt)
		nodetrxpool = nt->next;
	else
		nt = malloc( sizeof *nt);
	return (nt);
}


/*
 * free nodetrx to pool
 *
 * Locking assumed.
 */
static void
ntfree( nodetrx_t *nt)
{

	nt->next = nodetrxpool;
	nodetrxpool = nt;
}


int
trx_cmd_cb( int cmd, void *v0, void *v1)
{
	node_t	*n;

	int r = 0;
	pthread_mutex_lock( &nodetablelock);
	switch (cmd) {
	case TRX_CACHE_ADD:
		n = nsearch( *(FDF_cguid_t *)v0, (uint64_t)v1);
		n->cache = CACHED;
		break;
	case TRX_CACHE_DEL:
		n = nsearch( *(FDF_cguid_t *)v0, (uint64_t)v1);
		n->cache = UNCACHED;
		unless (n->tlist)
			ndestroy( *(FDF_cguid_t *)v0, (uint64_t)v1);
		break;
	case TRX_CACHE_QUERY:
		if (n = nsearch( *(FDF_cguid_t *)v0, (uint64_t)v1)) {
			if (n->cache == INVALID)
				if (n->tlist)
					n->cache = UNCACHED;
				else
					ndestroy( *(FDF_cguid_t *)v0, (uint64_t)v1);
			else
				r = 1;
		}
		else
			r = 1;
		break;
	default:
		abort( );
	}
	pthread_mutex_unlock( &nodetablelock);
	return (r);
}


static uint
hash( FDF_cguid_t c, uint64_t nid)
{

	return (c ^ nid);
}


/*
 * return unique number
 *
 * Increments monotonically across all threads.  Locking assumed.
 */
static timestamp_t
timestamp( )
{
	static timestamp_t	ts;

	return (++ts);
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
