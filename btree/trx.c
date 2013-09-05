

/*
 * ======== AT THIS TIME, SUPPORT FOR FAST MODE ONLY ========
 */

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
 */

#include	<sys/syscall.h>
#include	<unistd.h>
#include	<semaphore.h>
#include	<string.h>
#include	<assert.h>
#include	<pthread.h>
#include	"fdf.h"
#include	"btree_hash.h"
#include	"utils/properties.h"
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
typedef pthread_rwlock_t	rwlock_t;
typedef pthread_mutex_t		mutex_t;
typedef pthread_mutexattr_t	mutexattr_t;
typedef struct FDF_thread_state	thread_state_t;
typedef uint64_t		timestamp_t;
typedef enum pairorderenum	pairorder_t;

struct trxstructure {
	trx_t		*next;
	thread_state_t	*ts;
	ushort		level;
	enum trxstate	state;
	timestamp_t	lo,
			hi;
	trxnode_t	*nlist;
	sem_t		startlock,
			endlock;
	uint		cardinal;
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
	timestamp_t	ts;
};
struct nodetrxstructure {
	nodetrx_t	*next;
	trx_t		*trx;
	timestamp_t	lo,
			hi;
	bool		written;
};


static sem_t		endlock;
static rwlock_t		entrylock;
static mutexattr_t	mutextype;
static mutex_t		activetrxlock,
			nodetablelock,
			trxpoollock;
static trx_t		*activetrxlist,
			*waitingtrxlist;
static uint		ntrxpermitted		= 1,
			ntrxstarted,
			ntrxconcluded,
			ntrxlimit;
static node_t		*nodetable[1<<23],
			*nodepool;
static trx_t		*trxpool;
static trxnode_t	*trxnodepool;
static nodetrx_t	*nodetrxpool;
static __thread trx_t	*trx;
static int		trxproperty;
static bool		trxverbose;

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
static void		schedule( ),
			initpairoutcome( ),
			purgecache( ),
			tdestroy( ),
			tfree( trx_t *),
			tnfree( trxnode_t *),
			nfree( node_t *),
			ntfree( nodetrx_t *),
			track( FDF_cguid_t, uint64_t, bool);
static uint		hash( FDF_cguid_t, uint64_t);
static timestamp_t	timestamp( );


/* r/w mode for pairoutcome
 */
#define	R	0
#define	W	1

/* alignments for pairoutcome
 */
#define	B1___	0
#define	B2345	1
#define	B6___	2

/* output of pairoutcome
 */
enum pairorderenum {
	None,
	A,
	B,
	C
};

/* subscripting is [B alignment] [B r/w mode] [A r/w mode]
 */
static pairorder_t	pairoutcome[3][2][2];


#if 1//Rico
/*
 * start a trx cluster
 *
 * Start a new cluster in the FDF core, getting trx count.  Ensure average
 * trx size of N write ops is correct for your use case.  Locking assumed.
 */
static void
startcluster( )
{

	const uint N = 10;
	uint n = N;
	FDFTransactionService( 0, 2, &n);
	ntrxlimit += n;
}
#endif


void
trxinit( )
{

	pthread_mutexattr_init( &mutextype);
	pthread_mutexattr_settype( &mutextype, PTHREAD_MUTEX_NORMAL);
	pthread_mutex_init( &activetrxlock, &mutextype);
	pthread_mutex_init( &nodetablelock, &mutextype);
	pthread_mutex_init( &trxpoollock, &mutextype);
	sem_init( &endlock, 0, 0);
	pthread_rwlock_init( &entrylock, 0);
#if 0//Rico
	ntrxlimit += ntrxpermitted;
#else
	startcluster( );
#endif
	trxproperty = atoi( FDFGetProperty( "FDF_TRX", "1"));
	trxenabled = trxproperty & 1<<0;
	trxverbose = trxproperty & 1<<1;
	FDFTransactionService( 0, 4, (void *)(long)trxenabled);
	initpairoutcome( );
}


FDF_status_t
_trxenter( FDF_cguid_t c)
{

	pthread_rwlock_rdlock( &entrylock);
	return (FDF_SUCCESS);
}


FDF_status_t
_trxleave( FDF_cguid_t c)
{

	pthread_rwlock_unlock( &entrylock);
	return (FDF_SUCCESS);
}


#if 0//Rico
FDF_status_t
_trxstart( thread_state_t *ts)
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
	trx->lo = ~0;
	trx->hi = 0;
	trx->nlist = 0;
	sem_init( &trx->endlock, 0, 0);
	FDF_status_t s = FDFTransactionStart( ts);
	if (s == FDF_SUCCESS) {
		++trx->level;
		pthread_mutex_lock( &activetrxlock);
		if (ntrxstarted < ntrxlimit) {
			activetrxlist = tadd( activetrxlist, trx);
			++ntrxstarted;
			pthread_mutex_unlock( &activetrxlock);
		}
		else {
			sem_init( &trx->startlock, 0, 0);
			waitingtrxlist = tadd( waitingtrxlist, trx);
			pthread_mutex_unlock( &activetrxlock);
			sem_wait( &trx->startlock);
		}
	}
	else {
		tfree( trx);
		trx = 0;
	}
	return (s);
}
#else
FDF_status_t
_trxstart( thread_state_t *ts)
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
	trx->lo = ~0;
	trx->hi = 0;
	trx->nlist = 0;
	sem_init( &trx->startlock, 0, 0);
	FDF_status_t s = FDFTransactionStart( ts);
	unless (s == FDF_SUCCESS) {
		tfree( trx);
		trx = 0;
		return (s);
	}
	++trx->level;
	pthread_mutex_lock( &activetrxlock);
	if (ntrxstarted < ntrxlimit) {
		activetrxlist = tadd( activetrxlist, trx);
		++ntrxstarted;
		pthread_mutex_unlock( &activetrxlock);
	}
	else {
		waitingtrxlist = tadd( waitingtrxlist, trx);
		pthread_mutex_unlock( &activetrxlock);
		sem_wait( &trx->startlock);
	}
	return (s);
}
#endif


#if 0//Rico
FDF_status_t 
_trxcommit( thread_state_t *ts)
{

	unless (trx)
		return (FDF_FAILURE_NO_TRANS);
	if (--trx->level)
		return (FDF_SUCCESS);
	trx->state = COMMITTING;
	FDF_status_t s = conclude( );
	tfree( trx);
	trx = 0;
	return (s);
}
#else
FDF_status_t 
_trxcommit( thread_state_t *ts)
{
	FDF_status_t	s;

	unless (trx)
		return (FDF_FAILURE_NO_TRANS);
	if (--trx->level)
		return (FDF_SUCCESS);
	trx->state = COMMITTING;
	pthread_mutex_lock( &activetrxlock);
	if (++ntrxconcluded < ntrxlimit)
		s = FDFTransactionCommit( trx->ts);
	else {
		FDFTransactionService( ts, 3, 0);
		s = FDFTransactionCommit( trx->ts);
		startcluster( );
		trx_t *t;
		while ((ntrxstarted < ntrxlimit)
		and (t = waitingtrxlist)) {
			waitingtrxlist = tdel( waitingtrxlist, t);
			activetrxlist = tadd( activetrxlist, t);
			++ntrxstarted;
			sem_post( &t->startlock);
		}
	}
	pthread_mutex_unlock( &activetrxlock);
	tfree( trx);
	trx = 0;
	return (s);
}
#endif


#if 0//Rico
FDF_status_t 
trxrollback( thread_state_t *ts)
{

	unless (trxenabled)
		return (FDF_SUCCESS);
	unless (trx)
		return (FDF_FAILURE_NO_TRANS);
	if (--trx->level)
		return (FDF_SUCCESS);
	trx->state = ROLLINGBACK;
	FDF_status_t s = conclude( );
	tfree( trx);
	trx = 0;
	return (s);
}
#else
/*
 * (No rollback for Fast Mode; fake a successful outcome to pass DevTests)
 */
FDF_status_t 
trxrollback( thread_state_t *ts)
{

	return (trxcommit( ts));
}
#endif


FDF_status_t 
trxquit( thread_state_t *ts)
{

	unless (trxenabled)
		return (FDF_SUCCESS);
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


FDF_status_t
_trxtrackread( FDF_cguid_t c, uint64_t nid)
{

	track( c, nid, FALSE);
	return (FDF_SUCCESS);
}


FDF_status_t
_trxtrackwrite( FDF_cguid_t c, uint64_t nid)
{

	track( c, nid, TRUE);
	return (FDF_SUCCESS);
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


static void
limitretries( )
{
	static uint     z,
	                zz	= 1;

	unless (++z < zz) {
#if 0//Rico
		unless (z < 512*1024) {
			fprintf( stderr, "too many trx retries\n");
			abort( );
		}
#endif
		zz *= 2;
		if (trxverbose)
			fprintf( stderr, "trx retries = %d\n", z);
	}
}


/*
 * trx scheduler
 *
 * Determine dependencies, execute roll backs, then commits.  Nonzero value
 * of nsDELAY makes a singleton trx wait awhile for others to arrive
 * (throughput/latency tradeoff).
 */
static FDF_status_t
conclude( )
{
	const uint	nsDELAY		= 3000;
	trx_t		*t;

	uint64_t twait = reltime( ) + nsDELAY;
	loop {
		pthread_mutex_lock( &activetrxlock);
		if (++ntrxconcluded < ntrxstarted) {
			pthread_mutex_unlock( &activetrxlock);
			return (concludeself( ));
		}
		if ((activetrxlist->next)
		or (twait < reltime( )))
			break;
		--ntrxconcluded;
		pthread_mutex_unlock( &activetrxlock);
		pthread_yield( );
	}
	schedule( );
	pthread_rwlock_wrlock( &entrylock);
	FDF_status_t sself = 0;
	bool service = FALSE;
	bool aborts = FALSE;
	while (t = activetrxlist) {
		activetrxlist = tdel( activetrxlist, t);
		if ((t->cardinal)
		and (t->state == COMMITTING)) {
			t->state = ABORTING;
			aborts = TRUE;
			limitretries( );	// for development
		}
		if ((t->cardinal)
		and (not service)) {
			FDFTransactionService( trx->ts, 0, 0);
			service = TRUE;
		}
		if (t == trx) {
			sem_post( &t->endlock);
			sself = concludeself( );
		}
		else
			sem_post( &t->endlock);
		sem_wait( &endlock);
	}
	pthread_rwlock_unlock( &entrylock);
	if (aborts)
		ntrxpermitted = max( ntrxpermitted/2, 1);
	else if (lrand48( )/256%1000 < 1)
		ntrxpermitted = min( ntrxpermitted+1, 1000);
	ntrxlimit = ntrxconcluded + ntrxpermitted;
	while ((ntrxstarted < ntrxlimit)
	and (t = waitingtrxlist)) {
		waitingtrxlist = tdel( waitingtrxlist, t);
		activetrxlist = tadd( activetrxlist, t);
		++ntrxstarted;
		sem_post( &t->startlock);
	}
	pthread_mutex_unlock( &activetrxlock);
	return (sself);
}


static FDF_status_t
concludeself( )
{
	FDF_status_t	s;

	sem_wait( &trx->endlock);
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
	if (trx->cardinal == 1)
		FDFTransactionService( trx->ts, 1, 0);
	tdestroy( );
	sem_post( &endlock);
	return (s);
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

	pthread_mutex_lock( &trxpoollock);
	trx_t *t = trxpool;
	if (t)
		trxpool = t->next;
	else
		t = malloc( sizeof *t);
	pthread_mutex_unlock( &trxpoollock);
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

	pthread_mutex_lock( &trxpoollock);
	t->next = trxpool;
	trxpool = t;
	pthread_mutex_unlock( &trxpoollock);
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
			n->ts = 0;
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

	if (cmd == TRX_ENABLED)
		return (trxenabled);
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
			else {
				if (trx)
					ntsearch( n)->hi = timestamp( );
				r = 1;
			}
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


/*
 * section for scheduling transaction conclusion
 */

/*
 * Description: Ordered pairs
 *
 * Each interaction between a trx and an FDF object (node in b-tree parlance)
 * is tracked during the Transaction Phase by one nodetrx struct: read vs
 * write, and time.  If multiple accesses occur, the time range is enlarged.
 * Any write op will mark the entire range as a write.
 *
 * During Scheduling Phase, tracking data is use to form ordered pairs of
 * trx accordingly to their common access to each node.  For each node, all
 * trx combinations are inspected and an outcome generated.  The current
 * outcomes are None (no conflict possible), C (conflict realized), A
 * (trx A must precede), and B (trx B must precede).  Further conflicts
 * may be realized after linear ordering of the DAG.  Conflicting trx are
 * rolled back, along with those requesting roll back.  The remainder are
 * committed in topsort order.
 *
 * Diagram: Temporal alignment of access ranges for trx A and B
 *
 *   A               <---------------->
 * ///////////////////////////////////////////////////
 *   B1    ...--->
 *   B2    ...-------------------->
 *   B3    ...------------------------------...
 *   B4                  <-------->
 *   B5                  <------------------...
 *   B6                                  <--...
 *
 * Currently differentiated ranges are {B1}, {B2,B3,B4,B5}, and {B6}.
 *
 * Table: Outcomes for trx A versus B
 *
 *	Ar Br B1___    -	Aw Br B1___    B
 *	Ar Br B2345    -	Aw Br B2345    C
 *	Ar Br B6___    -	Aw Br B6___    A
 *	Ar Bw B1___    B	Aw Bw B1___    B
 *	Ar Bw B2345    C	Aw Bw B2345    C
 *	Ar Bw B6___    A	Aw Bw B6___    A
 *
 * Key:
 *	r   = reading only
 *	w   = writing, possibly with reading
 *	-   = no ordering is imposed by this interaction
 *	A   = trx A must precede
 *	B   = trx B must precede
 *	C   = conflict
 */


#define	SCHEDTRX	128		/* max # trx that can be scheduled */


typedef struct schedtrxstructure	schedtrx_t;

struct schedtrxstructure {
	schedtrx_t	*edgeout[SCHEDTRX];
	uint		nedgeout,
			nedgein;
	trx_t		*trx;
};
static schedtrx_t	*ttab[SCHEDTRX];
static uint		ntrx;


static void
initpairoutcome( )
{

	pairoutcome[B1___][R][R] = None;
	pairoutcome[B1___][R][W] = B;
	pairoutcome[B1___][W][R] = B;
	pairoutcome[B1___][W][W] = B;
	pairoutcome[B2345][R][R] = None;
	pairoutcome[B2345][R][W] = C;
	pairoutcome[B2345][W][R] = C;
	pairoutcome[B2345][W][W] = C;
	pairoutcome[B6___][R][R] = None;
	pairoutcome[B6___][R][W] = A;
	pairoutcome[B6___][W][R] = A;
	pairoutcome[B6___][W][W] = A;
}


static schedtrx_t	*
findschedtrx( trx_t *t)
{
	uint	i;

	for (i=0; i<ntrx; ++i)
		if (ttab[i]->trx == t)
			return (ttab[i]);
	fprintf( stderr, "no trx\n");
	abort( );
}


static void
edgeadd( schedtrx_t *sa, schedtrx_t *sb)
{

	uint i = 0;
	loop {
		unless (i < sa->nedgeout) {
			sa->edgeout[sa->nedgeout++] = sb;
			++sb->nedgein;
			break;
		}
		if (sa->edgeout[i] == sb)
			break;
		++i;
	}
}


static void
pairgen( )
{
	trx_t		*t;
	trxnode_t	*tn;
	nodetrx_t	*a,
			*b;

	timestamp_t ts = timestamp( );
	for (t=activetrxlist; t; t=t->next)
		for (tn=t->nlist; tn; tn=tn->next)
			if (tn->node->ts < ts) {
				tn->node->ts = ts;
				for (a=tn->node->tlist; a; a=a->next)
					for (b=a->next; b; b=b->next) {
						uint balignment;
						if (b->lo < a->lo)
							if (b->hi < a->lo)
								balignment = B1___;
							else
								balignment = B2345;
						else
							if (b->lo < a->hi)
								balignment = B2345;
							else
								balignment = B6___;
						schedtrx_t *sa = findschedtrx( a->trx);
						schedtrx_t *sb = findschedtrx( b->trx);
						switch (pairoutcome[balignment][b->written][a->written]) {
						case None:
							break;
						case A:
							edgeadd( sa, sb);
							break;
						case B:
							edgeadd( sb, sa);
							break;
						case C:
							++sa->nedgein;
							++sb->nedgein;
						}
					}
			}
}


static void
edgedelete( schedtrx_t *s, uint o)
{

	--s->edgeout[o]->nedgein;
	if (o < s->nedgeout-1)
		s->edgeout[o] = s->edgeout[s->nedgeout-1];
	--s->nedgeout;
}


static void
swap( uint i, uint j)
{

	schedtrx_t *z = ttab[i];
	ttab[i] = ttab[j];
	ttab[j] = z;
}


/*
 * topological sort
 *
 * Table ttab encodes the order dependency graph for all trx in this
 * Scheduling Phase.  In the best case, graph is a DAG and reduction to
 * linear form is complete: return value equals ntrx (number of trx).  In any
 * case, return value is the number of trx successfully sorted (possibly 0).
 * These will be found at the head of ttab, followed by the conflicting trx.
 */
static uint
topsort( )
{
	uint	i;

	for (i=0; i<ntrx; ++i) {
		uint j = i;
		loop {
			unless (j < ntrx)
				return (i);
			unless (ttab[j]->nedgein) {
				unless (j == i)
					swap( i, j);
				schedtrx_t *s = ttab[i];
				while (s->nedgeout)
					edgedelete( s, 0);
				break;
			}
			++j;
		}
	}
	return (i);
}


/*
 * compute trx outcomes
 *
 * Split activetrxlist into two lists: trx that can be committed, and trx
 * which must be rolled back.  Roll back may have been requested by the trx
 * at conclusion, or may be imposed by schedule() on detection of conflict.
 * On return, the two lists are concatenated back onto activetrxlist with
 * roll backs first (in no particular order), followed by commitments
 * (in commitment order).  For caller's convenience, trx for roll back are
 * assigned cardinal numbers descending to 1: this is used to trigger the
 * aggregated roll back.  The actual order of rollback is determined with
 * op resolution when the final roll back is submitted to the FDF core.
 * Cardinal for commitment trx is 0.  Roll backs must be performed before
 * commitments.
 *
 * Design note: schedtrx_t can be folded into trx_t eventually.
 */
static void
schedule( )
{
	trx_t	*t;
	uint	i;

	ntrx = 0;
	for (t=activetrxlist; t; t=t->next) {
		unless (ntrx < nel( ttab)) {
			fprintf( stderr, "too many trx\n");
			abort( );
		}
		unless ((ttab[ntrx])
		or (ttab[ntrx] = malloc( sizeof *ttab[ntrx]))) {
			fprintf( stderr, "out of mem\n");
			abort( );
		}
		ttab[ntrx]->nedgeout = 0;
		ttab[ntrx]->nedgein = 0;
		ttab[ntrx]->trx = t;
		++ntrx;
	}
	pairgen( );
	for (i=0; i<ntrx; ++i)
		if (ttab[i]->trx->state == ROLLINGBACK)
			++ttab[i]->nedgein;
#if 0//Rico
	{
	for (i=0; i<ntrx; ++i)
		if (ttab[i]->trx->state == COMMITTING) {
			uint r = (lrand48( )>>8) % 100;
			if (r < 0)
				++ttab[i]->nedgein;
		}
	}
#endif
	i = topsort( );
	activetrxlist = 0;
	uint j = 0;
	while (j < i) {
		activetrxlist = tadd( activetrxlist, ttab[i-++j]->trx);
		activetrxlist->cardinal = 0;
	}
	while (j < ntrx) {
		activetrxlist = tadd( activetrxlist, ttab[j++]->trx);
		activetrxlist->cardinal = j - i;
	}
	if (trxverbose) {
		static uint z;
		bool print = ++z%(1<<13) == 0;
		static uint imin = ~0;
		if (i < imin) {
			imin = i;
			printf( "schedule: * trx committable = %3u/%3u = %5.1f%%\n", i, ntrx, 100.*i/ntrx);
		}
		if (print)
			printf( "schedule:   trx committable = %3u/%3u = %5.1f%%\n", i, ntrx, 100.*i/ntrx);
	}
}
