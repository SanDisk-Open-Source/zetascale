/************************************************************************
 * 
 *  fdf_wrapper.c  Mar. 31, 2013   Brian O'Krafka
 * 
 *  FDF wrapper functions for btree layer.
 * 
 * NOTES: xxxzzz
 *     - xxxzzz create_node_cb has no datalen arg?
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <inttypes.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include "fdf.h"
#include "fdf_internal_cb.h"
#include "btree.h"
#include "btree_range.h"
#include "trx.h"
#include "btree_raw_internal.h"

#define MAX_NODE_SIZE   128*1024

#include "fdf_internal.h"

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

static char Create_Data[MAX_NODE_SIZE];

uint64_t n_global_l1cache_buckets;
struct PMap *global_l1cache;
extern int init_l1cache();
extern void destroy_l1cache();

// xxxzzz temporary: used for dumping btree stats
static uint64_t  n_reads = 0;

__thread struct FDF_thread_state *my_thd_state;

typedef struct read_node {
    FDF_cguid_t              cguid;
    uint64_t                 nodesize;
} read_node_t;


static void* read_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid);
static void write_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen);
static void flush_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid);
static int freebuf_cb(void *data, char *buf);
static void* create_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid);
static btree_status_t delete_node_cb(void *data, uint64_t lnodeid);
static void                   log_cb(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree);
static int                    lex_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2);
static void                   msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...);
static uint64_t               seqnoalloc( struct FDF_thread_state *);
FDF_status_t btree_get_all_stats(FDF_cguid_t cguid,
                                FDF_ext_stat_t **estat, uint32_t *n_stats) ;

FDF_status_t
_FDFGetRange(struct FDF_thread_state *fdf_thread_state,
             FDF_cguid_t              cguid,
             FDF_indexid_t            indexid,
             struct FDF_cursor      **cursor,
             FDF_range_meta_t        *rmeta);

FDF_status_t
_FDFGetNextRange(struct FDF_thread_state *fdf_thread_state,
                 struct FDF_cursor       *cursor,
                 int                      n_in,
                 int                     *n_out,
                 FDF_range_data_t        *values);

FDF_status_t
_FDFGetRangeFinish(struct FDF_thread_state *fdf_thread_state,
                   struct FDF_cursor *cursor);


FDF_status_t
_FDFMPut(struct FDF_thread_state *fdf_ts,
        FDF_cguid_t cguid,
        uint32_t num_objs,
        FDF_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_done);

FDF_status_t
_FDFRangeUpdate(struct FDF_thread_state *fdf_thread_state, 
	       FDF_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       FDF_range_update_cb_t callback_func,
	       void * callback_args,	
	       FDF_range_cmp_cb_t range_cmp_cb,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated);


#define Error(msg, args...) \
	msg_cb(0, NULL, __FILE__, __LINE__, msg, ##args);

#define msg(msg, args...) \
	msg_cb(2, NULL, __FILE__, __LINE__, msg, ##args);

#define Notice(msg, args...) \
	msg_cb(4, NULL, __FILE__, __LINE__, msg, ##args);

#define DEFAULT_N_PARTITIONS      1
// #define DEFAULT_N_PARTITIONS      128
// #define DEFAULT_N_PARTITIONS      4096
// #define DEFAULT_N_PARTITIONS      512
// #define DEFAULT_MAX_KEY_SIZE      10
#define DEFAULT_MAX_KEY_SIZE      256
// #define DEFAULT_NODE_SIZE         4000
// #define DEFAULT_NODE_SIZE         1900
#define DEFAULT_NODE_SIZE         8100
// #define DEFAULT_NODE_SIZE         1990
// #define DEFAULT_NODE_SIZE         2100
// #define DEFAULT_N_L1CACHE_BUCKETS 1000
// #define DEFAULT_N_L1CACHE_BUCKETS 1000
// #define DEFAULT_N_L1CACHE_BUCKETS 9600
#define DEFAULT_MIN_KEYS_PER_NODE 4

    // Counts of number of times callbacks are invoked:
static uint64_t N_read_node   = 0;
static uint64_t N_write_node  = 0;
static uint64_t N_flush_node  = 0;
static uint64_t N_freebuf     = 0;
static uint64_t N_create_node = 0;
static uint64_t N_delete_node = 0;
static uint64_t N_log         = 0;
static uint64_t N_cmp         = 0;

//  xxxzzz these are temporary:

typedef enum {
	 BT_CNTR_UNUSED,		/* cguid will be NULL for unused entries */
	 BT_CNTR_INIT,			/* Initializing */
	 BT_CNTR_OPEN,			/* Opened */
	 BT_CNTR_CLOSING,		/* Closing in progress */
	 BT_CNTR_CLOSED,		/* Closed */
	 BT_CNTR_DELETING,		/* Deletion in progress */
} BT_CNTR_STATE;

typedef struct cmap {
    char				cname[CONTAINER_NAME_MAXLEN];
    uint64_t			cguid;
    struct btree		*btree;
    int					read_by_rquery;
    read_node_t			node_data;
	bool				read_only;
	BT_CNTR_STATE		bt_state;
	int					bt_io_count;
	pthread_rwlock_t	bt_cm_rwlock;
} ctrmap_t;

#define MAX_OPEN_CONTAINERS   (UINT16_MAX - 1 - 9)
#define FIRST_VALID_CGUID		3
static ctrmap_t 	Container_Map[MAX_OPEN_CONTAINERS];
static int 			N_Open_Containers = 0;
pthread_rwlock_t	ctnrmap_rwlock = PTHREAD_RWLOCK_INITIALIZER;

static int
bt_add_cguid(FDF_cguid_t cguid)
{
    int i, empty_slot = -1;
    int i_ctnr = -1;

	pthread_rwlock_wrlock(&ctnrmap_rwlock);
	if (N_Open_Containers < MAX_OPEN_CONTAINERS) {
		for ( i = 0; i < MAX_OPEN_CONTAINERS; i++ ) {
			if ( Container_Map[i].cguid == cguid ) { 
				i_ctnr = i; 
				break; 
			} else if ((empty_slot == -1) && 
							(Container_Map[i].cguid == FDF_NULL_CGUID)) {
				//save empty slot, so that we need not search again	
				empty_slot = i;
			}
		}

		if (i_ctnr == -1) {
			assert(empty_slot >= 0);
			i_ctnr = empty_slot;	
			Container_Map[i_ctnr].cguid = cguid;
			Container_Map[i_ctnr].bt_state = BT_CNTR_INIT;
			(void) __sync_add_and_fetch(&N_Open_Containers, 1);
		}
	}
	pthread_rwlock_unlock(&ctnrmap_rwlock);

    return i_ctnr;
}

static int
bt_get_ctnr_from_cguid( 
    FDF_cguid_t cguid
    )
{
    int i;
    int i_ctnr = -1;

	pthread_rwlock_rdlock(&ctnrmap_rwlock);
    for ( i = 0; i < MAX_OPEN_CONTAINERS; i++ ) {
        if ( Container_Map[i].cguid == cguid ) { 
            i_ctnr = i; 
            break; 
        }
    }
	pthread_rwlock_unlock(&ctnrmap_rwlock);

    return i_ctnr;
}

/*
 * I M P O R T A N T:
 * This routine returns the index and btree with READ LOCK of the entry
 * held. Caller need to release the lock using bt_rel_entry() routine.
 */
static btree_t *
bt_get_btree_from_cguid(FDF_cguid_t cguid, int *index, FDF_status_t *error)
{
	int i;
	FDF_status_t	err = FDF_SUCCESS;
	btree_t			*bt = NULL;

	assert(index);
	assert(error);

	i = bt_get_ctnr_from_cguid(cguid);
	if (i == -1) {
		*error = FDF_FAILURE_CONTAINER_NOT_FOUND;
		return NULL;
	}

	pthread_rwlock_rdlock(&(Container_Map[i].bt_cm_rwlock));
	/* There could be a delete when we were trying to acquire lock */
	if ((Container_Map[i].cguid == cguid) &&
			(Container_Map[i].bt_state == BT_CNTR_OPEN)) {
		*index = i;
		bt = Container_Map[i].btree;
		(void) __sync_add_and_fetch(&(Container_Map[i].bt_io_count), 1);
	} else {
		pthread_rwlock_unlock(&(Container_Map[i].bt_cm_rwlock));
		/* The container has been deleted while we were acquiring the lock */
		if (Container_Map[i].cguid != cguid) {
			err = FDF_FAILURE_CONTAINER_NOT_FOUND;
		} else {
			err = FDF_FAILURE_CONTAINER_NOT_OPEN;
		}
	}
	*error = err;
	return bt;
}

static void
bt_rel_entry(int i)
{
	(void) __sync_sub_and_fetch(&(Container_Map[i].bt_io_count), 1);
	pthread_rwlock_unlock(&(Container_Map[i].bt_cm_rwlock));
	return;
}

static FDF_status_t
bt_is_valid_cguid(FDF_cguid_t cguid)
{
	if (cguid <= FIRST_VALID_CGUID) {
		return FDF_FAILURE_ILLEGAL_CONTAINER_ID;
	} else {
		return FDF_SUCCESS;
	}
}

static void dump_btree_stats(FILE *f, FDF_cguid_t cguid);

//  xxxzzz end of temporary stuff!


/*
 * Get a FDF property.
 */
const char *_FDFGetProperty(const char *key, const char *def)
{
    return(FDFGetProperty(key, def));
}


/**
 * @brief set FDF property
 *
 * @param propery <IN> property name
 * @param value <IN> pointer to value
 * 
 */
void _FDFSetProperty(
	const char* property,
	const char* value
	)
{
    FDFSetProperty(property, value);
}

/**
 * @brief Load properties from specified file
 *
 * @param proper_file <IN> properties file
 * @return FDF_SUCCESS on success
 * 
 */
FDF_status_t _FDFLoadProperties(
	const char *prop_file
	)
{
    return(FDFLoadProperties(prop_file));
}

/**
 * @brief FDF initialization
 *
 * @param fdf_state <OUT> FDF state variable
 * @param prop_file <IN> FDF property file or NULL
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFInit(
	struct FDF_state	**fdf_state
	)
{
    char         *stest;
    int           i = 0;
    FDF_status_t  ret;
    FDF_ext_cb_t  *cbs;

    // Initialize the map
    for (i=0; i<MAX_OPEN_CONTAINERS; i++) {
        Container_Map[i].cguid = FDF_NULL_CGUID;
        Container_Map[i].btree = NULL;
		pthread_rwlock_init(&(Container_Map[i].bt_cm_rwlock), NULL);
    }

	fprintf(stderr,"Number of cache buckets:%lu\n",n_global_l1cache_buckets);

	if( init_l1cache() ){
		fprintf(stderr, "Coundn't init global l1 cache.\n");
		return FDF_FAILURE;
	}

    ret = FDFInit(fdf_state);
    if ( ret == FDF_FAILURE ) {
        return FDF_FAILURE;
    }

    cbs = malloc(sizeof(FDF_ext_cb_t));
    if( cbs == NULL ) {
        return FDF_FAILURE;
    }
    memset(cbs,0,sizeof(FDF_ext_cb_t));
    cbs->stats_cb = btree_get_all_stats;
    trxinit( );
    ret = FDFRegisterCallbacks(*fdf_state, cbs);
    return(ret);
}

/**
 * @brief FDF per thread state initialization
 *
 * @param fdf_state <IN> FDF state variable
 * @param thd_state <OUT> FDF per thread state variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFInitPerThreadState(
	struct FDF_state		 *fdf_state,
	struct FDF_thread_state	**thd_state
	)
{
    return(FDFInitPerThreadState(fdf_state, thd_state));
}

/**
 * @brief FDF release per thread state initialization
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFReleasePerThreadState(
	struct FDF_thread_state	**thd_state
	)
{
	release_per_thread_keybuf();
    return(FDFReleasePerThreadState(thd_state));
}

/**
 * @brief FDF shutdown
 *
 * @param fdf_state <IN> FDF state variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFShutdown(
	struct FDF_state	*fdf_state
	)
{
    return(FDFShutdown(fdf_state));
}

/**
 * @brief FDF load default container properties
 *
 * @param props <IN> FDF container properties pointer
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFLoadCntrPropDefaults(
	FDF_container_props_t *props
	)
{
	FDF_status_t status = FDFLoadCntrPropDefaults(props);

	props->flash_only = FDF_TRUE;

	return status;
}

 /**
 * @brief Create and open a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cmeta <IN> container open meta
 * @param cguid <OUT> container GUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFOpenContainerSpecial(
	struct FDF_thread_state	  *fdf_thread_state, 
	char                      *cname, 
	FDF_container_props_t 	  *properties, 
	uint32_t                  flags_in,
	FDF_container_meta_t      *cmeta,
	FDF_cguid_t               *cguid
	)
{
    FDF_status_t  ret;
    struct btree *bt;
    uint32_t      flags;
    uint32_t      n_partitions;
    uint32_t      max_key_size, node_meta;
    uint32_t      min_keys_per_node;
    uint32_t      nodesize;
    void         *create_node_cb_data;
    void         *read_node_cb_data;
    void         *write_node_cb_data;
    void         *flush_node_cb_data;
    void         *freebuf_cb_data;
    void         *delete_node_cb_data;
    void         *log_cb_data;
    void         *msg_cb_data;
    cmp_cb_t     *cmp_cb;
    void         *cmp_cb_data;
    read_node_t  *prn;
    int           index = -1;
    char         *env;

    my_thd_state = fdf_thread_state;;

    if (!cname)
        return(FDF_INVALID_PARAMETER);

restart:
	if(getenv("FDF_CACHE_FORCE_ENABLE"))
		properties->flash_only = FDF_FALSE;

	fprintf(stderr, "FDF cache %s for container: %s\n", properties->flash_only ? "disabled" : "enabled", cname);

    ret = FDFOpenContainer(fdf_thread_state, cname, properties, flags_in, cguid);
    if (ret != FDF_SUCCESS)
        return(ret);

    // See if metadata exists (recovered container or opened already)
    index = bt_add_cguid(*cguid);

	/* This shouldnt happen, how FDF could create container but we exhausted map */
	if (index == -1) {
		FDFCloseContainer(fdf_thread_state, *cguid);
		return FDF_TOO_MANY_CONTAINERS;
	}

	pthread_rwlock_wrlock(&(Container_Map[index].bt_cm_rwlock));

	/* Some one deleted the container which we were re-opening */
	if (Container_Map[index].cguid != *cguid) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		goto restart;
	}

	assert ((Container_Map[index].bt_state == BT_CNTR_INIT) || /* Create/First Open */
			(Container_Map[index].bt_state == BT_CNTR_OPEN) || /* Re-open */
			(Container_Map[index].bt_state == BT_CNTR_CLOSED)); /* Opening closed one */

	if (flags_in & FDF_CTNR_RO_MODE) {
		Container_Map[index].read_only = false;
	} else if (flags_in & FDF_CTNR_RW_MODE) {
		Container_Map[index].read_only = false;
	} else {
		Container_Map[index].read_only = false;
	}

    // Metadata exists, just return if btree is not empty
    if (Container_Map[index].btree) {	
		Container_Map[index].bt_state = BT_CNTR_OPEN;
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
    	return(FDF_SUCCESS);
	}
    
    prn = &(Container_Map[index].node_data);

    create_node_cb_data = (void *) prn;
    read_node_cb_data   = (void *) prn;
    write_node_cb_data  = (void *) prn;
    flush_node_cb_data  = (void *) prn;
    freebuf_cb_data     = (void *) prn;
    delete_node_cb_data = (void *) prn;
    log_cb_data         = (void *) prn;
    msg_cb_data         = (void *) prn;
    cmp_cb_data         = (void *) prn;

    //flags = SYNDROME_INDEX;
    // flags |= IN_MEMORY; // use in-memory b-tree for this test
    flags = SECONDARY_INDEX;
    if ((flags_in&FDF_CTNR_CREATE) == 0)
        flags |= RELOAD;

    env = getenv("N_PARTITIONS");
    n_partitions = env ? atoi(env) : 0;

    if(!n_partitions)
        n_partitions = DEFAULT_N_PARTITIONS;

    env = getenv("BTREE_NODE_SIZE");
    nodesize = env ? atoi(env) : 0;
    if (!nodesize) {
	nodesize            = DEFAULT_NODE_SIZE;
    }

    min_keys_per_node   = DEFAULT_MIN_KEYS_PER_NODE;

    env = getenv("BTREE_MAX_KEY_SIZE");
    max_key_size = env ? atoi(env) : 0;
    if (!max_key_size) {
	node_meta =  sizeof(node_vkey_t);
	if (node_meta < sizeof(node_vlkey_t)) {
		node_meta =  sizeof(node_vlkey_t);
	}
	max_key_size = ((nodesize - sizeof(btree_raw_node_t))/min_keys_per_node) - node_meta;
    }

    prn->cguid            = *cguid;
    prn->nodesize         = nodesize;

#ifdef notdef
    msg("Creating a b-tree in FDFOpenContainer...\n");
    msg("n_partitions = %d\n",      n_partitions);
    msg("flags = %d\n",             flags);
    msg("max_key_size = %d\n",      max_key_size);
    msg("min_keys_per_node = %d\n", min_keys_per_node);
    msg("nodesize = %d\n",          nodesize);
#endif

    if (nodesize > MAX_NODE_SIZE) {
		msg("nodesize must <= %d\n", MAX_NODE_SIZE);
		goto fail;
    }

    cmp_cb = lex_cmp_cb;
    if (cmeta) {
        if (cmeta->sort_cmp_fn) cmp_cb = (cmp_cb_t *)cmeta->sort_cmp_fn;
        if (cmeta->cmp_data) cmp_cb_data = cmeta->cmp_data;
    }

    bt = btree_init(n_partitions, 
                    flags, 
                    max_key_size, 
                    min_keys_per_node, 
                    nodesize, 
                    (create_node_cb_t *)create_node_cb, 
                    create_node_cb_data, 
                    (read_node_cb_t *)read_node_cb, 
                    read_node_cb_data, 
                    (write_node_cb_t *)write_node_cb, 
                    write_node_cb_data, 
                    (flush_node_cb_t *)flush_node_cb, 
                    flush_node_cb_data, 
                    freebuf_cb, 
                    freebuf_cb_data, 
                    (delete_node_cb_t *)delete_node_cb, 
                    delete_node_cb_data, 
                    (log_cb_t *)log_cb, 
                    log_cb_data, 
                    msg_cb, 
                    msg_cb_data, 
                    cmp_cb, 
                    cmp_cb_data,
                    trx_cmd_cb,
					*cguid
		    );

    if (bt == NULL) {
        msg("Could not create btree in FDFOpenContainer!");
        //FDFDeleteContainer(fdf_thread_state, *cguid);
		FDFCloseContainer(fdf_thread_state, *cguid);
		goto fail;
    }

    env = getenv("BTREE_READ_BY_RQUERY");
    if (env && (atoi(env) == 1)) {
        Container_Map[index].read_by_rquery = 1;
        fprintf(stderr,"Reads will be done through range query\n");
    } else {
        Container_Map[index].read_by_rquery = 0;
    }

    // xxxzzz we should remember the btree info in a persistent place
    // xxxzzz for now, for performance testing, just use a hank

    Container_Map[index].btree = bt;
	Container_Map[index].bt_state = BT_CNTR_OPEN;
	pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));

	//Flush root node, so that it exists irrespective of durability level
	if (flags_in&FDF_CTNR_CREATE) {
		FDFFlushContainer(fdf_thread_state, *cguid);
	}
    return(FDF_SUCCESS);

fail:
	if (Container_Map[index].bt_state == BT_CNTR_INIT) {
		Container_Map[index].bt_state = BT_CNTR_CLOSED;
	}
	pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
	return(ret);
}

FDF_status_t _FDFOpenContainer(
	struct FDF_thread_state	*fdf_thread_state, 
	char                    *cname, 
	FDF_container_props_t 	*properties, 
	uint32_t                flags_in,
	FDF_cguid_t             *cguid
	)
{
	if (!cname) {
		return(FDF_INVALID_PARAMETER);
	}

	if (properties) {
		if (properties->writethru == FDF_FALSE) {
			properties->writethru = FDF_TRUE;
			Notice("WriteBack Mode is not supported, writethru is set to true for Container %s\n",cname);
		}
		if (properties->evicting == FDF_TRUE) {
			Notice("Eviction is not supported, evicting is reset to false for container %s\n", cname);
			properties->evicting = FDF_FALSE;
		}
		if (properties->durability_level == FDF_DURABILITY_PERIODIC) {
			Notice("PERIODIC durability is not supported, set to SW_CRASH_SAFE for %s\n", cname);
			properties->durability_level = FDF_DURABILITY_SW_CRASH_SAFE;
		}
		if (properties->async_writes == 1) {
		    Notice("Async Writes feature is not supported. So disabling it for the container %s\n", cname);
                    properties->async_writes = 0; 
                }
	}

	if (flags_in & FDF_CTNR_RO_MODE) {
		Notice("Read-only is not supported, set to read-write mode for container %s\n", cname);
		flags_in &= ~FDF_CTNR_RO_MODE;
		flags_in |= FDF_CTNR_RW_MODE;
	}

	return (_FDFOpenContainerSpecial(fdf_thread_state,
				cname,
				properties,
				flags_in,
				NULL,
				cguid));
}

/**
 * @brief Close a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFCloseContainer(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t				 cguid
	)
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	status = FDF_FAILURE;
	int				index;

restart:
	if ((status = bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return status;
	}

	if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
		return FDF_FAILURE_CONTAINER_NOT_FOUND;
	}

	pthread_rwlock_wrlock(&(Container_Map[index].bt_cm_rwlock));

	/* Some one might have deleted it, while we were trying to acquire lock */
	if (Container_Map[index].cguid != cguid) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		return FDF_FAILURE_CONTAINER_NOT_FOUND;
	}
	
	if (Container_Map[index].bt_state != BT_CNTR_OPEN) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		return FDF_FAILURE_CONTAINER_NOT_OPEN;
	}

	/* IO must not exist on this entry since we have write lock now */
	assert(Container_Map[index].bt_io_count == 0);
	if (Container_Map[index].bt_io_count) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		while (Container_Map[index].bt_io_count) {
			pthread_yield();
		}
		goto restart;
	}

	/* Lets block further IOs */
	Container_Map[index].bt_state = BT_CNTR_CLOSING;


    //msg("FDFCloseContainer is not currently supported!"); // xxxzzz
    status = FDFCloseContainer(fdf_thread_state, cguid);
	
	Container_Map[index].bt_state = BT_CNTR_CLOSED;
	pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
	return status;
}

/**
 * @brief Delete a virtual container
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFDeleteContainer(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t				 cguid
	)
{
    int				index;
    FDF_status_t 	status = FDF_FAILURE;
    my_thd_state 	= fdf_thread_state;;
	struct btree	*btree = NULL;

restart:
	if ((status = bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return status;
	}

	if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
		return FDF_FAILURE_CONTAINER_NOT_FOUND;
	}

	pthread_rwlock_wrlock(&(Container_Map[index].bt_cm_rwlock));

	/* Some one might have deleted it, while we were trying to acquire lock */
	if (Container_Map[index].cguid != cguid) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		return FDF_FAILURE_CONTAINER_NOT_FOUND;
	}
	
	/* IO might have started on this container. Lets wait and retry */
	assert(Container_Map[index].bt_io_count == 0);
	if (Container_Map[index].bt_io_count) {
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
		while (Container_Map[index].bt_io_count) {
			pthread_yield();
		}
		goto restart;
	}

	/* Lets block further IOs */
	Container_Map[index].bt_state = BT_CNTR_DELETING;

	/* Let us mark the entry NULL and then delete, so that we dont get the same
	   cguid if there is a create container happening */

	btree = Container_Map[index].btree;

	Container_Map[index].cguid = FDF_NULL_CGUID;
	Container_Map[index].btree = NULL;
	Container_Map[index].bt_state = BT_CNTR_UNUSED;
	(void) __sync_sub_and_fetch(&N_Open_Containers, 1);
	pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));

    trxdeletecontainer( fdf_thread_state, cguid);
    status = FDFDeleteContainer(fdf_thread_state, cguid);

    if (btree) {
        btree_destroy(btree);
    }
    return(status);
}

/**
 * @brief Get container list
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies.
 * @param cguids  <OUT> pointer to container GUID array
 * @param n_cguids <OUT> pointer to container GUID count
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFGetContainers(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t             *cguids,
	uint32_t                *n_cguids
	)
{
    my_thd_state = fdf_thread_state;;

    return(FDFGetContainers(fdf_thread_state, cguids, n_cguids));
}

/**
 * @brief Get container properties
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFGetContainerProps(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t            	 cguid,
	FDF_container_props_t	*pprops
	)
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    return(FDFGetContainerProps(fdf_thread_state, cguid, pprops));
}

/**
 * @brief Set container properties
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFSetContainerProps(
	struct FDF_thread_state 	*fdf_thread_state,
	FDF_cguid_t              	 cguid,
	FDF_container_props_t   	*pprops
	)
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

	if (pprops && (pprops->durability_level == FDF_DURABILITY_PERIODIC)) {
		pprops->durability_level == FDF_DURABILITY_SW_CRASH_SAFE;
	}

    return(FDFSetContainerProps(fdf_thread_state, cguid, pprops));
}

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param datalen <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFReadObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen
	)
{
    FDF_status_t      ret = FDF_SUCCESS;
    btree_status_t    btree_ret = BTREE_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;
    int index;
    btree_range_meta_t   rmeta;
    btree_range_cursor_t *cursor;
    btree_range_data_t   values[1];
    int n_out;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    {
        // xxxzzz this is temporary!
	__sync_fetch_and_add(&(n_reads), 1);
	if (0 && (n_reads % 200000) == 0) {
	    dump_btree_stats(stderr, cguid);
	}
    }

    my_thd_state = fdf_thread_state;;

    if (FDF_SUCCESS != (ret = FDFOperationAllowed())) {
        msg("Shutdown in Progress. Read object not allowed\n");
        return (ret);
    }

    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
    }

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
		bt_rel_entry(index);
        return (FDF_KEY_TOO_LONG);
    }

    meta.flags = 0;

    if (Container_Map[index].read_by_rquery) {
        rmeta.key_start    = key;
        rmeta.keylen_start = keylen;
        rmeta.key_end      = key;
        rmeta.keylen_end   = keylen;
        rmeta.flags        = RANGE_START_GE | RANGE_END_LE;

	trxenter( cguid);
        btree_ret = btree_range_query_start(bt, BTREE_RANGE_PRIMARY_INDEX,
                                            &cursor, &rmeta);
		if (btree_ret != BTREE_SUCCESS) {
				msg("Could not create start range query in FDFReadObject!");
				goto done;
		}

        btree_ret = btree_range_get_next(cursor, 1, &n_out, &values[0]);
        if (btree_ret == BTREE_SUCCESS) {
            *data    = values[0].data;
            *datalen = values[0].datalen;
        } else if (n_out != 1) {
            msg("btree_get_next_range: Failed to return object for key %s keylen=%d. Status=%d\n",
                 key, keylen, btree_ret);
            btree_ret = (btree_ret == BTREE_QUERY_DONE) ? 
                         BTREE_KEY_NOT_FOUND: BTREE_FAILURE;
        }

        (void)btree_range_query_end(cursor);
    } else {
	trxenter( cguid);
        btree_ret = btree_get(bt, key, keylen, data, datalen, &meta);
    }

done:
    trxleave( cguid);
    switch(btree_ret) {
        case BTREE_SUCCESS:
            ret = FDF_SUCCESS;
            break;
        case BTREE_FAILURE:
            ret = FDF_FAILURE;
            break;
        case BTREE_KEY_NOT_FOUND:
            ret = FDF_OBJECT_UNKNOWN;
            break;
        default:
            ret = FDF_FAILURE;
            break;
    }

	bt_rel_entry(index);
    return(ret);
}

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param fdf_thread_state <IN> The FDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param robj <IN> Identity of a read object structure
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFReadObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_readobject_t         *robj
    )
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    return(FDFReadObjectExpiry(fdf_thread_state, cguid, robj));
}


/**
 * @brief Free an object buffer
 *
 * @param buf <IN> object buffer
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFFreeBuffer(
	char *buf
	)
{
    // pid_t  tid = syscall(SYS_gettid);

    // xxxzzz SEGFAULT
    // fprintf(stderr, "SEGFAULT FDFFreeBuffer: %p [tid=%d]\n", buf, tid);
    return(FDFFreeBuffer(buf));
}

/**
 *  @brief Write entire object, creating it if necessary.  
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param fdf_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param flags <IN> create/update flags
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OUT_OF_MEM: there is insufficient memory/flash.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFWriteObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t	     flags
	)
{
    FDF_status_t      ret = FDF_FAILURE;
    btree_status_t    btree_ret = BTREE_FAILURE;
    btree_metadata_t  meta;
    struct btree     *bt;
	int				  index;

    my_thd_state = fdf_thread_state;;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
    }

	if (Container_Map[index].read_only == true) {
		bt_rel_entry(index);
		return FDF_FAILURE;
	}

    if (!bt) 
		assert(0);

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
		pthread_rwlock_unlock(&(Container_Map[index].bt_cm_rwlock));
        return (FDF_KEY_TOO_LONG);
    }

    meta.flags = 0;
    meta.seqno = seqnoalloc( fdf_thread_state);
    if (meta.seqno == -1) {
		bt_rel_entry(index);
        return (FDF_FAILURE);
	}

    trxenter( cguid);
    trxstart( fdf_thread_state);
    if (flags & FDF_WRITE_MUST_NOT_EXIST) {
		btree_ret = btree_insert(bt, key, keylen, data, datalen, &meta);
    } else if (flags & FDF_WRITE_MUST_EXIST) {
		btree_ret = btree_update(bt, key, keylen, data, datalen, &meta);
    } else {
		btree_ret = btree_set(bt, key, keylen, data, datalen, &meta);
    }
    trxcommit( fdf_thread_state);
    trxleave( cguid);

    switch(btree_ret) {
        case BTREE_SUCCESS:
            ret = FDF_SUCCESS;
            break;
        case BTREE_FAILURE:
            ret = FDF_FAILURE_STORAGE_WRITE;
            break;
        case BTREE_KEY_NOT_FOUND:
	        if (flags & FDF_WRITE_MUST_NOT_EXIST) {
	            ret = FDF_OBJECT_EXISTS;
	        } else if (flags & FDF_WRITE_MUST_EXIST) {
	            ret = FDF_OBJECT_UNKNOWN;
	        } else {
	            assert(0); // this should never happen!
	            ret = FDF_FAILURE; // xxxzzz fix this!
	        }
            break;
        default:
	        msg("btree_insert/update failed for key '%s' with ret=%s!\n", key, FDFStrError(ret));
	        ret = FDF_FAILURE; // xxxzzz fix this!
            break;
    }
	bt_rel_entry(index);
    return(ret);
}

/**
 *  @brief Write entire object, creating it if necessary.  
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param fdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param wobj <IN> Identity of a write object structure
 *  @param flags <IN> create/update flags
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OUT_OF_MEM: there is insufficient memory/flash.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFWriteObjectExpiry(
    struct FDF_thread_state  *fdf_thread_state,
    FDF_cguid_t               cguid,
    FDF_writeobject_t        *wobj,
    uint32_t                  flags
    )
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    //msg("FDFWriteObjectExpiry is not currently supported!");
    return(FDFWriteObjectExpiry(fdf_thread_state, cguid, wobj, flags));
}

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param fdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFDeleteObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	)
{
    FDF_status_t      ret = FDF_SUCCESS;
    btree_status_t    btree_ret = BTREE_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;
	int				index;

    my_thd_state = fdf_thread_state;;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
    }

	if (Container_Map[index].read_only == true) {
		bt_rel_entry(index);
		return FDF_FAILURE;
	}

    bt = Container_Map[index].btree;

    meta.flags = 0;

    trxenter( cguid);
    btree_ret = btree_delete(bt, key, keylen, &meta);
    trxleave( cguid);
    switch(btree_ret) {
        case BTREE_SUCCESS:
            ret = FDF_SUCCESS;
            break;
        case BTREE_FAILURE:
            ret = FDF_FAILURE;
            break;
        case BTREE_KEY_NOT_FOUND:
            ret = FDF_OBJECT_UNKNOWN;
            break;
        default:
            ret = FDF_FAILURE;
            break;
    }

	bt_rel_entry(index);
    return(ret);
}

/**
 * @brief Enumerate container objects
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFEnumerateContainerObjects(
	struct FDF_thread_state *fdf_thread_state,
	FDF_cguid_t              cguid,
	struct FDF_iterator    **iterator
	)
{
    FDF_range_meta_t rmeta;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

	bzero(&rmeta, sizeof(FDF_range_meta_t));
    return _FDFGetRange(fdf_thread_state, 
	                    cguid, 
                        FDF_RANGE_PRIMARY_INDEX, 
                        (struct FDF_cursor **) iterator, 
                        &rmeta);
}

/**
 * @brief Container object enumration iterator
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @param cguid  <IN> container global identifier
 * @param key <OUT> pointer to key variable
 * @param keylen <OUT> pointer to key length variable
 * @param data <OUT> pointer to data variable
 * @param datalen <OUT> pointer to data length variable
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFNextEnumeratedObject(
	struct FDF_thread_state *fdf_thread_state,
	struct FDF_iterator     *iterator,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	)
{ 
	FDF_status_t     status = FDF_FAILURE;
	int              count = 0;
	FDF_range_data_t values;

	values.key = NULL;
	values.data = NULL;
	status = _FDFGetNextRange(fdf_thread_state,
                              (struct FDF_cursor *) iterator,
                              1,
                              &count,
                              &values);

	if (FDF_SUCCESS == status &&
            FDF_RANGE_DATA_SUCCESS == values.status && count)
        {
            assert(count); // Hack
	    	*key = (char *) malloc(values.keylen);
            assert(*key);
	    	strncpy(*key, values.key, values.keylen);
	    	*keylen = values.keylen;
	    	*data = (char *) malloc(values.datalen);
            assert(*data);
	    	strncpy(*data, values.data, values.datalen);
	    	*datalen = values.datalen;
	    //if (values.primary_key)
	        //free(values.primary_key);
	} else {
	    *key = NULL;
	    *keylen = 0;
	    *data = NULL;
	    *datalen = 0;
	    status = FDF_OBJECT_UNKNOWN;
	}

	if (values.key) {
		free(values.key);
	}
	if (values.data) {
		free(values.data);
	}

    return(status);
}

/**
 * @brief Terminate enumeration
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFFinishEnumeration(
	struct FDF_thread_state *fdf_thread_state,
	struct FDF_iterator     *iterator
	)
{
	return(_FDFGetRangeFinish(fdf_thread_state, (struct FDF_cursor *) iterator));
}

/**
 *  @brief Force modifications of an object to primary storage.
 *
 *  Flush any modified contents of an object to its backing store
 *  (as determined by its container type). For coherent containers,
 *  this is a global operation that applies to any cache or buffer
 *  in the SDF cluster. For non-coherent containers, this only applies
 *  to the local cache.
 *
 *  @param fdf_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return FDF_SUCCESS: operation completed successfully.
 *          FDF_BAD_CONTEXT: the provided context is invalid.
 *          FDF_CONTAINER_UNKNOWN: the container ID is invalid.
 *          FDF_OBJECT_UNKNOWN: the object does not exist.
 *          FDF_IN_TRANS: this operation cannot be done inside a transaction.
 *          FDF_FAILURE: operation failed.
 */
FDF_status_t _FDFFlushObject(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	)
{
    FDF_status_t      ret = FDF_FAILURE;
    btree_status_t    btree_ret = BTREE_FAILURE;
    struct btree     *bt;
	int				  index;


	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
    my_thd_state = fdf_thread_state;;

    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
    }
    if (!bt) 
		assert(0);

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
		bt_rel_entry(index);
        return (FDF_KEY_TOO_LONG);
    }

    trxenter( cguid);
    btree_ret = btree_flush(bt, key, keylen);
    trxleave( cguid);

    switch(btree_ret) {
        case BTREE_SUCCESS:
            ret = FDF_SUCCESS;
            break;
        case BTREE_FAILURE:
            ret = FDF_FAILURE_STORAGE_WRITE;
            break;
        case BTREE_KEY_NOT_FOUND:
	        ret = FDF_OBJECT_UNKNOWN;
            break;
        default:
	        msg("btree_flush failed for key '%s' with ret=%s!\n", key, FDFStrError(ret));
	        ret = FDF_FAILURE; // xxxzzz fix this!
            break;
    }
	bt_rel_entry(index);
    return(ret);
}

/**
 * @brief Flush container
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies.
 * @param cguid  <IN> container global identifier
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFFlushContainer(
	struct FDF_thread_state  *fdf_thread_state,
	FDF_cguid_t               cguid
	)
{
	FDF_status_t	ret;
    my_thd_state = fdf_thread_state;;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
    return(FDFFlushContainer(fdf_thread_state, cguid));
}

/**
 * @brief Flush the cache
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies.
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFFlushCache(
	struct FDF_thread_state  *fdf_thread_state
	)
{
    my_thd_state = fdf_thread_state;;

    return(FDFFlushCache(fdf_thread_state));
}

/**
 * @brief Get FDF statistics
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @param stats <OUT> pointer to statistics return structure
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFGetStats(
	struct FDF_thread_state *fdf_thread_state,
	FDF_stats_t             *stats
	)
{

    my_thd_state = fdf_thread_state;;

    return(FDFGetStats(fdf_thread_state, stats));
}

/**
 * @brief Get per container statistics
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param stats <OUT> pointer to statistics return structure
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFGetContainerStats(
	struct FDF_thread_state	*fdf_thread_state,
	FDF_cguid_t		 cguid,
	FDF_stats_t     	*stats
	)
{
    my_thd_state = fdf_thread_state;;
	FDF_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}

    return(FDFGetContainerStats(fdf_thread_state, cguid, stats));
}

/**
 * @brief Get error string for given error code
 *
 * @param errno FDF error number
 * @return  error string
 */
char *_FDFStrError(FDF_status_t fdf_errno)
{
    return(FDFStrError(fdf_errno));
}

/*
 * FDFTransactionStart
 */
FDF_status_t 
_FDFTransactionStart(struct FDF_thread_state *fdf_thread_state)
{

    return (trxstart( fdf_thread_state));
}


/*
 * FDFTransactionCommit
 */
FDF_status_t 
_FDFTransactionCommit(struct FDF_thread_state *fdf_thread_state)
{

    return (trxcommit( fdf_thread_state));
}

/*
 * FDFTransactionRollback
 */
FDF_status_t 
_FDFTransactionRollback(struct FDF_thread_state *fdf_thread_state)
{

    return (trxrollback( fdf_thread_state));
}

/*
 * FDFTransactionQuit
 */
FDF_status_t 
_FDFTransactionQuit(struct FDF_thread_state *fdf_thread_state)
{

    return (trxquit( fdf_thread_state));
}

/*
 * FDFTransactionID
 */
uint64_t
_FDFTransactionID(struct FDF_thread_state *fdf_thread_state)
{

    return (trxid( fdf_thread_state));
}

/*
 * FDFGetVersion
 */
FDF_status_t 
_FDFGetVersion(char **str)
{
    return FDFGetVersion(str);
}

/*
 * FDFGetRange
 */
FDF_status_t 
_FDFGetRange(struct FDF_thread_state *fdf_thread_state,
             FDF_cguid_t              cguid, 
             FDF_indexid_t            indexid,
             struct FDF_cursor      **cursor,
             FDF_range_meta_t        *rmeta)
{
	FDF_status_t      ret = FDF_SUCCESS;
	btree_status_t    status;
	struct btree     *bt;
	int				  index;

	my_thd_state = fdf_thread_state;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
	}

	status = btree_range_query_start(bt, 
	                                (btree_indexid_t) indexid, 
	                                (btree_range_cursor_t **)cursor,
	                                (btree_range_meta_t *)rmeta);

	if (status == BTREE_SUCCESS) {
		ret = FDF_SUCCESS;
	} else {
		ret = FDF_FAILURE;
	}
	bt_rel_entry(index);
	return(ret);
}

FDF_status_t
_FDFGetNextRange(struct FDF_thread_state *fdf_thread_state,
                 struct FDF_cursor       *cursor,
                 int                      n_in,
                 int                     *n_out,
                 FDF_range_data_t        *values)
{
	FDF_status_t      ret = FDF_SUCCESS;
	btree_status_t    status;
	int				  index;
	btree_t			  *bt;


	my_thd_state = fdf_thread_state;;

	/* TODO: Need to add additional callback to start_range_query
	 * cursor, which generically maps status of each values to
	 * whatever the user of btree needs (in this case fdf_wrapper).
	 * At present, the status numbers are matched, but thats not
	 * good for maintanability */
	const FDF_cguid_t cguid = ((read_node_t *)(((btree_range_cursor_t *)cursor)->btree->read_node_cb_data))->cguid;
	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return (ret);
	}
	trxenter( cguid);
	status = btree_range_get_next((btree_range_cursor_t *)cursor,
	                              n_in,
	                              n_out,
	                              (btree_range_data_t *)values
	                              );
	trxleave( cguid);

	if (status == BTREE_SUCCESS) {
		ret = FDF_SUCCESS;
	} else if (status == BTREE_QUERY_DONE) {
		ret = FDF_QUERY_DONE;
	} else if (status == BTREE_QUERY_PAUSED) {
		ret = FDF_QUERY_PAUSED;
	} else if (status == BTREE_WARNING) {
		ret = FDF_WARNING;
	} else {
		ret = FDF_FAILURE;
	}
	bt_rel_entry(index);
	return(ret);
}

FDF_status_t 
_FDFGetRangeFinish(struct FDF_thread_state *fdf_thread_state, 
                   struct FDF_cursor *cursor)
{
	FDF_status_t      ret = FDF_SUCCESS;
	btree_status_t    status;

	my_thd_state = fdf_thread_state;;

	status = btree_range_query_end((btree_range_cursor_t *)cursor);
	if (status == BTREE_SUCCESS) {
		ret = FDF_SUCCESS;
	} else if (status == BTREE_QUERY_DONE) {
		ret = FDF_QUERY_DONE;
	} else {
		ret = FDF_FAILURE;
	}

	return(ret);
}

/*****************************************************************
 *
 *  B-Tree Callback Functions
 *
 *****************************************************************/

static void* read_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid)
{
    read_node_t            *prn = (read_node_t *) data;
    struct btree_raw_node  *n;
    uint64_t                datalen;
    FDF_status_t            status = FDF_FAILURE;

    N_read_node++;

    status = FDFReadObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &n, &datalen);
    trxtrackread( prn->cguid, lnodeid);

    if (status == FDF_SUCCESS) {
	    assert(datalen == prn->nodesize);
        *ret = BTREE_SUCCESS;
	    // xxxzzz SEGFAULT
	    // fprintf(stderr, "SEGFAULT read_node_cb: %p [tid=%d]\n", n, tid);
	    return(n);
    } else {
	    fprintf(stderr, "ZZZZZZZZ   red_node_cb %lu - %lu failed with ret=%s   ZZZZZZZZZ\n", lnodeid, datalen, FDFStrError(*ret));
        *ret = BTREE_FAILURE;
	    return(NULL);
    }
}

static void write_node_cb(btree_status_t *ret_out, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen)
{
    FDF_status_t    ret;
    read_node_t     *prn = (read_node_t *) cb_data;

    N_write_node++;

    ret = FDFWriteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), data, datalen, 0 /* flags */);
    trxtrackwrite( prn->cguid, lnodeid);
    assert(prn->nodesize == datalen);

    switch(ret) {
        case FDF_SUCCESS:
            *ret_out = BTREE_SUCCESS;
            break;
        case FDF_FAILURE:
            *ret_out = BTREE_FAILURE;
            break;
        case FDF_FAILURE_OPERATION_DISALLOWED:
            *ret_out = BTREE_OPERATION_DISALLOWED;
            break;
        default:
            *ret_out = BTREE_FAILURE;
            break;
    }
}

static void flush_node_cb(btree_status_t *ret_out, void *cb_data, uint64_t lnodeid)
{
    FDF_status_t            ret;
    read_node_t            *prn = (read_node_t *) cb_data;

    N_flush_node++;

    ret = FDFFlushObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t));

    switch(ret) {
        case FDF_SUCCESS:
            *ret_out = BTREE_SUCCESS;
            break;
        case FDF_FAILURE:
            *ret_out = BTREE_FAILURE;
            break;
        case FDF_FAILURE_OPERATION_DISALLOWED:
            *ret_out = BTREE_OPERATION_DISALLOWED;
            break;
        default:
            *ret_out = BTREE_FAILURE;
            break;
    }
}

static int freebuf_cb(void *data, char *buf)
{
    // pid_t  tid = syscall(SYS_gettid);

    N_freebuf++;
    // xxxzzz SEGFAULT
    // fprintf(stderr, "SEGFAULT freebuf_cb: %p [tid=%d]\n", buf, tid);
    free(buf);
    return(0);
}

static void* create_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid)
{
	struct btree_raw_node	*n;
	FDF_status_t		status = FDF_FAILURE;
	read_node_t		*prn = (read_node_t *) data;
	uint64_t		datalen;

	N_create_node++;
	status = FDFWriteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), Create_Data, prn->nodesize, FDF_WRITE_MUST_NOT_EXIST  /* flags */);
	trxtrackwrite( prn->cguid, lnodeid);
	if (status == FDF_SUCCESS) {
		status = FDFReadObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &n, &datalen);
		if (status == FDF_SUCCESS) {
			assert(datalen == prn->nodesize);
			*ret = BTREE_SUCCESS;
			return(n);
		}
		*ret = BTREE_FAILURE;
		fprintf(stderr, "ZZZZZZZZ   create_node_cb failed with ret=%d   ZZZZZZZZZ\n", *ret);
	}
	return(NULL);
}

static btree_status_t delete_node_cb(void *data, uint64_t lnodeid)
{
	read_node_t	*prn	= (read_node_t *) data;

	N_delete_node++;
	FDF_status_t status = FDFDeleteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t));
	trxtrackwrite( prn->cguid, lnodeid);
	if (status == FDF_SUCCESS)
		return (BTREE_SUCCESS);
	return (BTREE_SUCCESS);		// return success until btree logic is fixed
}

// ???
static void log_cb(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree)
{
	N_log++;
	*ret = BTREE_SUCCESS;
}

#if 0
static int byte_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
    N_cmp++;

    if (keylen1 < keylen2) {
        return(-1);
    } else if (keylen1 > keylen2) {
        return(1);
    } else if (keylen1 == keylen2) {
        return(memcmp(key1, key2, keylen1));
    }
    assert(0);
    return(0);
}
#endif

static int lex_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
    int x;
    int cmp_len;

    N_cmp++;

    /* Handle open-left-end of a query */
    if(!key1) return -1;

    cmp_len = keylen1 < keylen2 ? keylen1: keylen2;

    x = memcmp(key1, key2, cmp_len);
    if (x != 0) {
        return x;
    }

    /* Equal so far, use len to decide */
    if (keylen1 < keylen2) {
        return -1;
    } else if (keylen1 > keylen2) {
        return 1;
    } else {
        return 0;
    }
}

/****************************************************
 *
 * Message Levels:
 *   0: error
 *   1: warning
 *   2: info
 *   3: debug
 *
 ****************************************************/
static void msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...)
{
    char     stmp[512];
    va_list  args;
    char    *prefix;
    int      quit = 0;

    va_start(args, msg);

    vsprintf(stmp, msg, args);
    strcat(stmp, "\n");

    va_end(args);

    switch (level) {
	    case 0:  prefix = "ERROR";                quit = 1; break;
	    case 1:  prefix = "WARNING";              quit = 0; break;
	    case 2:  prefix = "INFO";                 quit = 0; break;
	    case 3:  prefix = "DEBUG";                quit = 0; break;
	    case 4:  prefix = "NOTICE";		      quit = 0; break;
	    default: prefix = "PROBLEM WITH MSG_CB!"; quit = 1; break;
		     break;
    } 

    (void) fprintf(stderr, "%s: %s", prefix, stmp);
    if (quit) {
        assert(0);
        exit(1);
    }
}

FDF_ext_stat_t btree_to_fdf_stats_map[] = {
    /*{Btree stat, corresponding fdf stat, fdf stat type, NOP}*/
    {BTSTAT_L1ENTRIES,        FDF_CACHE_STAT_L1_ENTRIES,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_L1OBJECTS,        FDF_CACHE_STAT_L1_OBJECTS,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},

    {BTSTAT_LEAF_L1HITS,      FDF_CACHE_STAT_LEAF_L1_HITS,       FDF_STATS_TYPE_CACHE_TO_FLASH,0}, 
    {BTSTAT_NONLEAF_L1HITS,   FDF_CACHE_STAT_NONLEAF_L1_HITS,    FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_OVERFLOW_L1HITS,  FDF_CACHE_STAT_OVERFLOW_L1_HITS,   FDF_STATS_TYPE_CACHE_TO_FLASH,0},

    {BTSTAT_LEAF_L1MISSES,    FDF_CACHE_STAT_LEAF_L1_MISSES,     FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_NONLEAF_L1MISSES, FDF_CACHE_STAT_NONLEAF_L1_MISSES,  FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_OVERFLOW_L1MISSES,FDF_CACHE_STAT_OVERFLOW_L1_MISSES, FDF_STATS_TYPE_CACHE_TO_FLASH,0},

    {BTSTAT_LEAF_L1WRITES,    FDF_CACHE_STAT_LEAF_L1_WRITES,     FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_NONLEAF_L1WRITES, FDF_CACHE_STAT_NONLEAF_L1_WRITES,  FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_OVERFLOW_L1WRITES,FDF_CACHE_STAT_OVERFLOW_L1_WRITES, FDF_STATS_TYPE_CACHE_TO_FLASH,0},

    {BTSTAT_LEAF_NODES,       FDF_CACHE_STAT_BT_LEAF_NODES,      FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_NONLEAF_NODES,    FDF_CACHE_STAT_BT_NONLEAF_NODES,   FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_OVERFLOW_NODES,   FDF_CACHE_STAT_BT_OVERFLOW_NODES,  FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_LEAF_BYTES,       FDF_CACHE_STAT_BT_LEAF_BYTES,      FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_NONLEAF_BYTES,    FDF_CACHE_STAT_BT_NONLEAF_BYTES,   FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_OVERFLOW_BYTES,   FDF_CACHE_STAT_BT_OVERFLOW_BYTES,  FDF_STATS_TYPE_CACHE_TO_FLASH,0},

    {BTSTAT_NUM_OBJS,         FDF_CACHE_STAT_BT_NUM_OBJS,        FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_EVICT_BYTES,      FDF_CACHE_STAT_BT_EVICT_BYTES,     FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_SPLITS,           FDF_CACHE_STAT_BT_SPLITS,          FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_LMERGES,          FDF_CACHE_STAT_BT_LMERGES,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_RMERGES,          FDF_CACHE_STAT_BT_RMERGES,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_LSHIFTS,          FDF_CACHE_STAT_BT_LSHIFTS,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_RSHIFTS,          FDF_CACHE_STAT_BT_RSHIFTS,         FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_EX_TREE_LOCKS,    FDF_CACHE_STAT_BT_EX_TREE_LOCKS,   FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_NON_EX_TREE_LOCKS,FDF_CACHE_STAT_BT_NON_EX_TREE_LOCKS,FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_GET_CNT,          FDF_ACCESS_TYPES_APGRD,            FDF_STATS_TYPE_APP_REQ,0},
    {BTSTAT_GET_PATH,         FDF_CACHE_STAT_BT_GET_PATH_LEN,    FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_CREATE_CNT,       FDF_ACCESS_TYPES_APCOP,            FDF_STATS_TYPE_APP_REQ,0},
    {BTSTAT_CREATE_PATH,      FDF_CACHE_STAT_BT_CREATE_PATH_LEN, FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_SET_CNT,          FDF_ACCESS_TYPES_APSOB,            FDF_STATS_TYPE_APP_REQ,0},
    {BTSTAT_SET_PATH,         FDF_CACHE_STAT_BT_SET_PATH_LEN,    FDF_STATS_TYPE_CACHE_TO_FLASH,0},
    {BTSTAT_UPDATE_CNT,       FDF_ACCESS_TYPES_APPTA,            FDF_STATS_TYPE_APP_REQ,0},
    {BTSTAT_UPDATE_PATH,      FDF_CACHE_STAT_BT_UPDATE_PATH_LEN, FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
    {BTSTAT_DELETE_CNT,       FDF_ACCESS_TYPES_APDOB,            FDF_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_DELETE_PATH,      FDF_CACHE_STAT_BT_DELETE_PATH_LEN, FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
    {BTSTAT_FLUSH_CNT,        FDF_CACHE_STAT_BT_FLUSH_CNT,       FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
    {BTSTAT_DELETE_OPT_CNT,   FDF_CACHE_STAT_BT_DELETE_OPT_COUNT,FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
    {BTSTAT_MPUT_IO_SAVED,    FDF_CACHE_STAT_BT_MPUT_IO_SAVED,   FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
    {BTSTAT_PUT_RESTART_CNT,  FDF_CACHE_STAT_BT_PUT_RESTART_CNT, FDF_STATS_TYPE_CACHE_TO_FLASH, 0},
};

FDF_status_t btree_get_all_stats(FDF_cguid_t cguid, 
                                FDF_ext_stat_t **estat, uint32_t *n_stats) {
    int i, j;
    struct btree     *bt;
    btree_stats_t     bt_stats;
    uint64_t         *stats;
	FDF_status_t	  ret;
	int				  index;	

    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return ret;
    }
    stats = malloc( N_BTSTATS * sizeof(uint64_t));
    if( stats == NULL ) {
		bt_rel_entry(index);
        return FDF_FAILURE;
    }
    memset(stats, 0, N_BTSTATS * sizeof(uint64_t));
    for (i=0; i<bt->n_partitions; i++) {
        btree_raw_get_stats(bt->partitions[i], &bt_stats);

        for (j=0; j<N_BTSTATS; j++) {
            stats[j] +=  bt_stats.stat[j];
        }
    }
/*
    for (j=0; j<N_BTSTATS; j++) {
        if (stats[j] != 0) {
            stats[j] /= bt->n_partitions;
        }
    } */

    FDF_ext_stat_t *pstat_arr;
    pstat_arr = malloc(N_BTSTATS * sizeof(FDF_ext_stat_t));
    if( pstat_arr == NULL ) {
        free(stats);
		bt_rel_entry(index);
        return FDF_FAILURE;
    }
    for (i=0; i<N_BTSTATS; i++) {
        pstat_arr[i].estat = i;
        pstat_arr[i].fstat = btree_to_fdf_stats_map[i].fstat;     
        pstat_arr[i].ftype = btree_to_fdf_stats_map[i].ftype;     
        pstat_arr[i].value = stats[i];
    }
    free(stats);
    *estat = pstat_arr;
    *n_stats = N_BTSTATS;
	bt_rel_entry(index);
    return FDF_SUCCESS;
}


static void dump_btree_stats(FILE *f, FDF_cguid_t cguid)
{
    struct btree     *bt;
    btree_stats_t     bt_stats;
	int				  index;
	FDF_status_t	  ret;

    bt = bt_get_btree_from_cguid(cguid, &index, &ret);
    if (bt == NULL) {
        return;
    }

    btree_get_stats(bt, &bt_stats);
    btree_dump_stats(f, &bt_stats);
}

FDF_status_t
_FDFMPut(struct FDF_thread_state *fdf_ts,
        FDF_cguid_t cguid,
        uint32_t num_objs,
        FDF_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_done)
{
	int i;
	FDF_status_t ret = FDF_SUCCESS;
	btree_status_t btree_ret = BTREE_FAILURE;
	btree_metadata_t meta;
	struct btree *bt = NULL;
	uint32_t objs_written = 0;
	uint32_t objs_to_write = num_objs;
	int		index;


	my_thd_state = fdf_ts;;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
	if (num_objs == 0) {
		return FDF_SUCCESS;
	}

	bt = bt_get_btree_from_cguid(cguid, &index, &ret );
	if (bt == NULL) {
		return ret;
	}

	for (i = 0; i < num_objs; i++) {
		if (objs[i].flags != 0) {
			bt_rel_entry(index);
			return FDF_INVALID_PARAMETER;
		}

		if (objs[i].key_len > bt->max_key_size) {
			bt_rel_entry(index);
			return FDF_INVALID_PARAMETER;
		}
	}

	meta.flags = 0;
	meta.seqno = seqnoalloc(fdf_ts);
	if (meta.seqno == -1) {
		bt_rel_entry(index);
		return (FDF_FAILURE);
	}

	objs_written = 0;
	objs_to_write = num_objs;
	do {
		objs_to_write -= objs_written;
		objs_written = 0;
		btree_ret = btree_mput(bt, (btree_mput_obj_t *)&objs[num_objs - objs_to_write],
				       objs_to_write, flags, &meta, &objs_written);
	} while ((btree_ret == BTREE_SUCCESS) && objs_written < objs_to_write);

	*objs_done = num_objs - (objs_to_write - objs_written);

	switch(btree_ret) {
		case BTREE_SUCCESS:
			ret = FDF_SUCCESS;
			break;
		case BTREE_FAILURE:
			ret = FDF_FAILURE_STORAGE_WRITE;
			break;
		case BTREE_KEY_NOT_FOUND:
			ret = FDF_OBJECT_UNKNOWN;
			break;
		default:
			ret = FDF_FAILURE;
			break;
	}

	bt_rel_entry(index);
	return ret;
}

#if 0

#define NUM_IN_CHUNK 500
#define MAX_KEYLEN 256 //Must change to btree property.

static bool
key_in_range(struct btree *bt, char *range_key, uint32_t range_key_len,
	     char *key, uint32_t keylen)
{
	int x = 0;
	
	if (keylen < range_key_len) {
		return false;
	}

	x = bt->cmp_cb(bt->cmp_cb_data, range_key, range_key_len,
		       key, range_key_len);

	// TBD: we should have a range check function passed from user for
	// Btree

	if (x != 0) {
		return false;
	}	
	return true;
}

typedef struct {
	char *key;
	char *data;
} btree_rupdate_data_t;

FDF_status_t
_FDFRangeUpdate(struct FDF_thread_state *fdf_ts, 
	       FDF_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       FDF_range_update_cb_t callback_func,
	       void * callback_args,	
	       uint32_t *objs_updated)
{
	FDF_status_t ret = FDF_SUCCESS;
	struct btree *bt = NULL;
	btree_rupdate_cb_t cb_func = 
			(btree_rupdate_cb_t) callback_func;
	FDF_range_meta_t rmeta;
	FDF_range_data_t *values = NULL;
	struct FDF_cursor *cursor = NULL;
	btree_rupdate_data_t *tmp_data = NULL;
	FDF_obj_t *objs = NULL;
	uint32_t objs_written = 0;
	uint32_t objs_to_update = 0;
	uint32_t objs_in_range = 0;
	char *new_data = NULL;
	uint64_t new_data_len;
	int n_out = 0;
	int i = 0;
	int	index;

	*objs_updated = 0;

	if ((ret= bt_is_valid_cguid(cguid)) != FDF_SUCCESS) {
		return ret;
	}
	bt = bt_get_btree_from_cguid(cguid, &index, &ret);
	if (bt == NULL) {
		return ret;
	}

	objs = (FDF_obj_t *) malloc(sizeof(FDF_obj_t) * NUM_IN_CHUNK);
	if (objs == NULL) {
		bt_rel_entry(index);
		return FDF_FAILURE_MEMORY_ALLOC;
	}

	tmp_data = (btree_rupdate_data_t *) malloc(sizeof(*tmp_data) * NUM_IN_CHUNK);
	if (tmp_data == NULL) {
		ret = FDF_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	values = (FDF_range_data_t *)
		   malloc(sizeof(FDF_range_data_t) * NUM_IN_CHUNK);
	if (values == NULL) {
		ret = FDF_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	rmeta.key_start = (char *) malloc(MAX_KEYLEN);
	if (rmeta.key_start == NULL) {
		ret = FDF_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	memcpy(rmeta.key_start, range_key, range_key_len);
	rmeta.keylen_start = range_key_len;

	rmeta.flags = FDF_RANGE_START_GE;
	rmeta.key_end = NULL;
	rmeta.keylen_end = 0;
	rmeta.cb_data = NULL;
	rmeta.keybuf_size = 0;
	rmeta.databuf_size = 0;

	rmeta.allowed_fn = NULL;

	ret = _FDFGetRange(fdf_ts, cguid, FDF_RANGE_PRIMARY_INDEX,
			  &cursor, &rmeta);
	if (ret != FDF_SUCCESS) {
		goto exit;
	}

	/*
	 * Start TXN
	 */
#if RANGE_UPDATE_TXN
	ret = _FDFTransactionStart(fdf_ts);
	if (ret != FDF_SUCCESS) {
		assert(0);
		goto exit;	
	}
#endif 

	do {
		ret = _FDFGetNextRange(fdf_ts, cursor, NUM_IN_CHUNK, &n_out, values);
		if (ret != FDF_SUCCESS && ret != FDF_QUERY_DONE) {
			/*
			 * Return error.
			 */
			assert(0);
			goto rollback;
		}

		objs_to_update = 0;
		for (i = 0; i < n_out; i++) {

			assert(values[i].status == FDF_SUCCESS);

			if (key_in_range(bt, range_key, range_key_len,
					 values[i].key, values[i].keylen) != true) {
				/*
				 * Found end of range. We must update only
				 * objects that are in range.
				 */
				break;
			}

			objs[i].key = values[i].key;	
			objs[i].key_len = values[i].keylen;	
			objs[i].flags = 0;

			/*
			 * Call the user provided callback.
			 */
			new_data_len = 0;
			new_data = NULL;

			tmp_data[i].key = values[i].key;
			tmp_data[i].data = values[i].data;

			if (cb_func != NULL) {
				if ((*cb_func) (values[i].key, values[i].keylen,
						values[i].data, values[i].datalen,
						callback_args, &new_data, &new_data_len) == false) {
					/*
					 * object data not changed, no need to update.
					 */
					continue;
				}
			}

			assert(values[i].datalen != 0);
			if (new_data_len != 0) {
				/*
				 * Callback has changed the data size.
				 */
				objs[i].data = new_data;
				objs[i].data_len = new_data_len;

				/*
				 * Free older data and take new one in account.
				 */
				free(tmp_data[i].data);
				tmp_data[i].data = new_data;
			} else {
				/*
				 * Data size is not changed.
				 */
				objs[i].data = values[i].data;
				objs[i].data_len = values[i].datalen;
			}
			objs_to_update++;

		}

		objs_in_range = i;
	
		objs_written = 0;
		ret = _FDFMPut(fdf_ts, cguid, objs_to_update, objs, 0, &objs_written);

		/*
		 * Free up the used memory.
		 */
		for (i = 0; i < objs_in_range; i++) {
			free(tmp_data[i].key);
			free(tmp_data[i].data);
		}

		for (i = objs_in_range; i < n_out; i++) {
			free(values[i].key);
			free(values[i].data);
		}
		
		(*objs_updated) += objs_written;

		if ((ret != FDF_SUCCESS) || (objs_written != objs_to_update)) {
			assert(0);
			goto rollback;
		}	

	} while (n_out > 0);

	_FDFGetRangeFinish(fdf_ts, cursor);
	
	/*
	 *  commit txn
	 */
#if RANGE_UPDATE_TXN
	ret = _FDFTransactionCommit(fdf_ts);
	if (ret == FDF_SUCCESS) {
		/*
		 * Successfully committed the txn.
		 */
		goto exit;
	}

#endif 
	
rollback:
	/*
	 * Rollback txn
	 */
#if RANGE_UPDATE_TXN
	assert(0);
	FDFTransactionRollback(fdf_ts);
	assert(ret != FDF_SUCCESS);
	(*objs_updated) = 0;
#endif 

exit:

	if (tmp_data) {
		free(tmp_data);
	}

	if (rmeta.key_start) {
		free(rmeta.key_start);
	}

	if (values) {
		free(values);
	}

	if (objs) {
		free(objs);
	}
	bt_rel_entry(index);

	return ret;
}

#else 
FDF_status_t
_FDFRangeUpdate(struct FDF_thread_state *fdf_ts, 
	       FDF_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       FDF_range_update_cb_t callback_func,
	       void * callback_args,	
	       FDF_range_cmp_cb_t range_cmp_cb,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated)
{
	btree_rupdate_marker_t *markerp = NULL;
	FDF_status_t ret = FDF_SUCCESS;
	btree_status_t btree_ret = BTREE_FAILURE;
	btree_rupdate_cb_t cb_func = 
			(btree_rupdate_cb_t) callback_func;
	btree_metadata_t meta;
	struct btree *bt = NULL;
	uint32_t objs_done = 0;
	int index = -1;
	FDF_status_t error = FDF_SUCCESS;
	btree_range_cmp_cb_t bt_range_cmp_cb = range_cmp_cb;

	(*objs_updated) = 0;

	my_thd_state = fdf_ts;;

	bt = bt_get_btree_from_cguid(cguid, &index, &error);
	if (bt == NULL) {
		return FDF_FAILURE;
	}

	if (Container_Map[index].read_only == true) {
		ret = FDF_FAILURE;
		goto out;
	}

	markerp = btree_alloc_rupdate_marker(bt);
	if (markerp == NULL) {
		ret = FDF_OUT_OF_MEM;
		goto out;
	}

	meta.flags = 0;
	meta.seqno = seqnoalloc(fdf_ts);
	if (meta.seqno == -1) {
		ret = FDF_FAILURE;
		goto out;
	}

	do {
		objs_done = 0;
		btree_ret = btree_range_update(bt, &meta, range_key, range_key_len,
					       cb_func, callback_args, bt_range_cmp_cb,
					       range_cmp_cb_args, &objs_done, &markerp); 

		(*objs_updated) += objs_done;
		if (btree_ret == BTREE_RANGE_UPDATE_NEEDS_SPACE) {
			/*
			 * In place update failed due to node being full.
			 * Insert this individually so that the node
			 * splits and it makes space for insert.
			 */	
			assert( markerp->retry_keylen && markerp->retry_datalen);
			btree_ret = btree_update(bt, markerp->retry_key, markerp->retry_keylen,
						     markerp->retry_data, markerp->retry_datalen, &meta);	

			if (btree_ret == BTREE_SUCCESS) {
				/*
				 * Maker will not be updated in btree_update call, we have to set
				 * it here.
				 */
				memcpy(markerp->last_key, markerp->retry_key, markerp->retry_keylen);
				markerp->last_key_len = markerp->retry_keylen;
				markerp->set = true;
				(*objs_updated)++;
			}

			free(markerp->retry_key);
			free(markerp->retry_data);
		}
	} while ((btree_ret == BTREE_SUCCESS) && (markerp->set == true));
	
	switch(btree_ret) {
		case BTREE_SUCCESS:
			ret = FDF_SUCCESS;
			break;
		case BTREE_FAILURE:
			ret = FDF_FAILURE_STORAGE_WRITE;
			break;
		case BTREE_KEY_NOT_FOUND:
			ret = FDF_OBJECT_UNKNOWN;
			break;
		default:
			ret = FDF_FAILURE;
			break;
	}

out:
	if (markerp) {
		btree_free_rupdate_marker(bt, markerp);
	}

	bt_rel_entry(index);
	return ret;
}
#endif 

/*
 * persistent seqno facility
 *
 * This sequence number generator is shared by all btrees.  It offers
 * crashproof persistence, high performance, and thread safety.  The counter
 * is only reset by an FDF reformat and, with 64 bits of resolution, will
 * never wrap.
 *
 * Counter is maintained in its own high-durability container (non btree),
 * and is synchronized to flash after SEQNO_SYNC_INTERVAL increments.
 * It is advanced by up to that interval on FDF restart.
 */

#include	"utils/rico.h"

#define	SEQNO_SYNC_INTERVAL	10000000000


/*
 * return next seqno, or -1 on error
 */
static uint64_t
seqnoalloc( struct FDF_thread_state *t)
{
	static bool		initialized;
	static pthread_mutex_t	seqnolock		= PTHREAD_MUTEX_INITIALIZER;
	static FDF_cguid_t	cguid;
	static uint64_t		seqnolimit,
				seqno;
	static char		SEQNO_CONTAINER[]	= "__SanDisk_seqno_container";
	static char		SEQNO_KEY[]		= "__SanDisk_seqno_key";
	FDF_container_props_t	p;
	FDF_status_t		s;
	char			*data;
	uint64_t		dlen;

	pthread_mutex_lock( &seqnolock);
	unless (initialized) {
		memset( &p, 0, sizeof p);
		switch (s = FDFOpenContainer( t, SEQNO_CONTAINER, &p, 0, &cguid)) {
		case FDF_SUCCESS:
			if ((FDFReadObject( t, cguid, SEQNO_KEY, sizeof SEQNO_KEY, &data, &dlen) == FDF_SUCCESS)
			and (dlen == sizeof seqnolimit)) {
				seqnolimit = *(uint64_t *)data;
				seqno = seqnolimit;
				FDFFreeBuffer( data);
				break;
			}
			/* flash corruption likely, but consider recovery by tree search */
			FDFCloseContainer( t, cguid);
		default:
			fprintf( stderr, "seqnoalloc: cannot initialize seqnolimit - %s\n", FDFStrError(s));
			pthread_mutex_unlock( &seqnolock);
			return (-1);
		case FDF_INVALID_PARAMETER:	       /* schizo FDF/SDF return */
			p.size_kb = 1 * 1024 * 1024;
			p.fifo_mode = FDF_FALSE;
			p.persistent = FDF_TRUE;
			p.evicting = FDF_FALSE;
			p.writethru = FDF_TRUE;
			p.durability_level = FDF_DURABILITY_HW_CRASH_SAFE;
			s = FDFOpenContainer( t, SEQNO_CONTAINER, &p, FDF_CTNR_CREATE, &cguid);
			unless (s == FDF_SUCCESS) {
				fprintf( stderr, "seqnoalloc: cannot create %s (%s)\n", SEQNO_CONTAINER, FDFStrError( s));
				pthread_mutex_unlock( &seqnolock);
				return (-1);
			}
		}
		initialized = TRUE;
	}
	unless (seqno < seqnolimit) {
		seqnolimit = seqno + SEQNO_SYNC_INTERVAL;
		s = FDFWriteObject( t, cguid, SEQNO_KEY, sizeof SEQNO_KEY, (char *)&seqnolimit, sizeof seqnolimit, 0);
		unless (s == FDF_SUCCESS) {
			seqnolimit = 0;
			fprintf( stderr, "seqnoalloc: cannot update %s (%s)\n", SEQNO_KEY, FDFStrError( s));
			pthread_mutex_unlock( &seqnolock);
			return (-1);
		}
		fprintf( stderr, "seqnoalloc (info): new seqnolimit = %ld\n", seqnolimit);
	}
	uint64_t z = seqno++;
	pthread_mutex_unlock( &seqnolock);
	return (z);
}
