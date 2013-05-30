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
#include "btree.h"
#include "selftest.h"
#include "fdf_range.h"
#include "btree_range.h"

#define MAX_NODE_SIZE   128*1024
static char Create_Data[MAX_NODE_SIZE];

// xxxzzz temporary: used for dumping btree stats
static uint64_t  n_reads = 0;

__thread struct FDF_thread_state *my_thd_state;

typedef struct read_node {
    FDF_cguid_t              cguid;
    uint64_t                 nodesize;
} read_node_t;

static struct btree_raw_node *read_node_cb(int *ret, void *data, uint64_t lnodeid);
static void write_node_cb(int *ret, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen);
static int freebuf_cb(void *data, char *buf);
static struct btree_raw_node *create_node_cb(int *ret, void *data, uint64_t lnodeid);
static int                    delete_node_cb(struct btree_raw_node *node, void *data, uint64_t lnodeid);
static void                   log_cb(int *ret, void *data, uint32_t event_type, struct btree_raw *btree, struct btree_raw_node *n);
static int                    cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2);
static void                   msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...);
static void                   txn_cmd_cb(int *ret_out, void *cb_data, int cmd_type);

#define Error(msg, args...) \
    msg_cb(0, NULL, __FILE__, __LINE__, msg, ##args);

#define msg(msg, args...) \
    msg_cb(2, NULL, __FILE__, __LINE__, msg, ##args);

#ifdef notdef
    #define DEFAULT_N_PARTITIONS      100
    #define DEFAULT_MAX_KEY_SIZE      100
    #define DEFAULT_NODE_SIZE         8192
    #define DEFAULT_N_L1CACHE_BUCKETS 1000
    #define DEFAULT_MIN_KEYS_PER_NODE 4
#endif

#define DEFAULT_N_PARTITIONS      1
// #define DEFAULT_N_PARTITIONS      128
// #define DEFAULT_N_PARTITIONS      4096
// #define DEFAULT_N_PARTITIONS      512
// #define DEFAULT_MAX_KEY_SIZE      10
#define DEFAULT_MAX_KEY_SIZE      100
// #define DEFAULT_NODE_SIZE         4000
// #define DEFAULT_NODE_SIZE         1900
#define DEFAULT_NODE_SIZE         8100
// #define DEFAULT_NODE_SIZE         1990
// #define DEFAULT_NODE_SIZE         2100
// #define DEFAULT_N_L1CACHE_BUCKETS 1000
// #define DEFAULT_N_L1CACHE_BUCKETS 1000
// #define DEFAULT_N_L1CACHE_BUCKETS 9600
#define DEFAULT_N_L1CACHE_BUCKETS 18000
#define DEFAULT_MIN_KEYS_PER_NODE 4

    // Counts of number of times callbacks are invoked:
static uint64_t N_read_node   = 0;
static uint64_t N_write_node  = 0;
static uint64_t N_freebuf     = 0;
static uint64_t N_create_node = 0;
static uint64_t N_delete_node = 0;
static uint64_t N_log         = 0;
static uint64_t N_cmp         = 0;
static uint64_t N_txn_cmd_1   = 0;
static uint64_t N_txn_cmd_2   = 0;
static uint64_t N_txn_cmd_3   = 0;

//  xxxzzz these are temporary:

typedef struct cmap {
    char           cname[CONTAINER_NAME_MAXLEN];
    uint64_t       cguid;
    struct btree  *btree;
    read_node_t    node_data;
} ctrmap_t;

#define MAX_OPEN_CONTAINERS   UINT16_MAX - 1 - 9
static ctrmap_t Container_Map[MAX_OPEN_CONTAINERS];
static int N_Open_Containers = 0;

static int bt_get_ctnr_from_cguid( 
    FDF_cguid_t cguid
    )
{
    int i;
    int i_ctnr = -1;

    for ( i = 0; i < MAX_OPEN_CONTAINERS; i++ ) {
        if ( Container_Map[i].cguid == cguid ) { 
            i_ctnr = i; 
            break; 
        }
    }

    return i_ctnr;
}

static btree_t *bt_get_btree_from_cguid(FDF_cguid_t cguid)
{
	int i;

	i = bt_get_ctnr_from_cguid(cguid);
	if (i >= N_Open_Containers) {
		return(NULL); // xxxzzz fix this!
	}

	return (Container_Map[i].btree);
}

#if 0
static int bt_get_ctnr_from_cname( 
     char *cname
     )
{
    int i;
    int i_ctnr = -1;

    for ( i = 0; i < MAX_OPEN_CONTAINERS; i++ ) {
        if ( (NULL != Container_Map[i].cname) && (0 == strcmp( Container_Map[i].cname, cname )) ) {
            i_ctnr = i;
            break;
        }
    }

    return i_ctnr;
}
#endif

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

    // Initialize the map
    for (i=0; i<N_Open_Containers; i++) {
        Container_Map[i].cguid = FDF_NULL_CGUID;
        Container_Map[i].btree = NULL;
    }

    stest = getenv("FDF_RUN_BTREE_SELFTEST");
    if (stest != NULL) {
	(void) btree_selftest(0, NULL);
        exit(0);
    }

    return(FDFInit(fdf_state));
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
    return(FDFLoadCntrPropDefaults(props));
}

 /**
 * @brief Create and open a virtual container.
 *
 * @param fdf_thread_state <IN> The FDF context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cguid <OUT> container GUID
 * @return FDF_SUCCESS on success
 */
FDF_status_t _FDFOpenContainer(
	struct FDF_thread_state	*fdf_thread_state, 
	char					*cname, 
	FDF_container_props_t 	*properties, 
	uint32_t			 	 flags_in,
	FDF_cguid_t				*cguid
	)
{
    FDF_status_t  ret;
    struct btree *bt;
    uint32_t      flags;
    uint32_t      n_partitions;
    uint32_t      max_key_size;
    uint32_t      min_keys_per_node;
    uint32_t      nodesize;
    uint32_t      n_l1cache_buckets;
    void         *create_node_cb_data;
    void         *read_node_cb_data;
    void         *write_node_cb_data;
    void         *freebuf_cb_data;
    void         *delete_node_cb_data;
    void         *log_cb_data;
    void         *msg_cb_data;
    void         *cmp_cb_data;
    void         *txn_cmd_cb_data;
    read_node_t  *prn;
    int           index = -1;

    my_thd_state = fdf_thread_state;;

    if (!cname)
        return(FDF_INVALID_PARAMETER);

    if (N_Open_Containers >= MAX_OPEN_CONTAINERS) {
        msg("Exceeded size of Container_Map array in FDFOpenContainer!");
        return(FDF_FAILURE);
    }

    ret = FDFOpenContainer(fdf_thread_state, cname, properties, flags_in, cguid);
    if (ret != FDF_SUCCESS)
        return(ret);

    // See if metadata exists (recovered container or opened already)
    index = bt_get_ctnr_from_cguid(*cguid);
    if (index >= 0) {
        // Metadata exists, just return if btree is not empty
        if (Container_Map[index].btree)
            return(FDF_SUCCESS);
    } else {
        // Need to fill in metadata map
        index = N_Open_Containers;
        Container_Map[index].cguid = *cguid;
    } 
    
    prn = &(Container_Map[index].node_data);

    create_node_cb_data = (void *) prn;
    read_node_cb_data   = (void *) prn;
    write_node_cb_data  = (void *) prn;
    freebuf_cb_data     = (void *) prn;
    delete_node_cb_data = (void *) prn;
    log_cb_data         = (void *) prn;
    msg_cb_data         = (void *) prn;
    cmp_cb_data         = (void *) prn;
    txn_cmd_cb_data     = (void *) prn;

    //flags = SYNDROME_INDEX;
    // flags |= IN_MEMORY; // use in-memory b-tree for this test
    flags = SECONDARY_INDEX;
    if ((flags_in&FDF_CTNR_CREATE) == 0)
        flags |= RELOAD;

    n_partitions        = DEFAULT_N_PARTITIONS;
    max_key_size        = DEFAULT_MAX_KEY_SIZE;
    min_keys_per_node   = DEFAULT_MIN_KEYS_PER_NODE;
    nodesize            = DEFAULT_NODE_SIZE;

    char* b =  getenv("N_L1CACHE_BUCKETS");
    n_l1cache_buckets = b ? atoi(b) : 0;

    if(!n_l1cache_buckets)
	n_l1cache_buckets = DEFAULT_N_L1CACHE_BUCKETS;

    prn->cguid            = *cguid;
    prn->nodesize         = nodesize;

    msg("Creating a b-tree in FDFOpenContainer...\n");
    msg("n_partitions = %d\n",      n_partitions);
    msg("flags = %d\n",             flags);
    msg("max_key_size = %d\n",      max_key_size);
    msg("min_keys_per_node = %d\n", min_keys_per_node);
    msg("nodesize = %d\n",          nodesize);
    msg("n_l1cache_buckets = %d\n", n_l1cache_buckets);

    if (nodesize > MAX_NODE_SIZE) {
	msg("nodesize must <= %d\n", MAX_NODE_SIZE);
	return(FDF_FAILURE); // xxxzzz fix me
    }

    bt = btree_init(n_partitions, flags, max_key_size, min_keys_per_node, 
                    nodesize, n_l1cache_buckets,
                    create_node_cb, create_node_cb_data, 
                    read_node_cb, read_node_cb_data, 
                    write_node_cb, write_node_cb_data, 
                    freebuf_cb, freebuf_cb_data, 
                    delete_node_cb, delete_node_cb_data, 
                    log_cb, log_cb_data, 
                    msg_cb, msg_cb_data, 
                    cmp_cb, cmp_cb_data,
                    txn_cmd_cb, txn_cmd_cb_data
		    );

    if (bt == NULL) {
        msg("Could not create btree in FDFOpenContainer!");
        return(FDF_FAILURE);
    }

    // xxxzzz we should remember the btree info in a persistent place
    // xxxzzz for now, for performance testing, just use a hank

    Container_Map[index].btree = bt;
    N_Open_Containers++;

    return(ret);
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

    //msg("FDFCloseContainer is not currently supported!"); // xxxzzz
    return(FDFCloseContainer(fdf_thread_state, cguid));
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
    FDF_status_t status;
    int index = -1;
    my_thd_state = fdf_thread_state;;

    status = FDFDeleteContainer(fdf_thread_state, cguid);
    index = bt_get_ctnr_from_cguid(cguid);
    if (index >= 0) {
        btree_destroy(Container_Map[index].btree);
        Container_Map[index].cguid = FDF_NULL_CGUID;
        Container_Map[index].btree = NULL;
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
    btree_metadata_t  meta;
    struct btree     *bt;

    {
        // xxxzzz this is temporary!
	__sync_fetch_and_add(&(n_reads), 1);
	if ((n_reads % 50000) == 0) {
	    dump_btree_stats(stderr, cguid);
	}
    }

    my_thd_state = fdf_thread_state;;

    bt = bt_get_btree_from_cguid(cguid);
    if (bt == NULL) {
        return (FDF_FAILURE);
    }

    meta.flags = 0;

    ret = btree_get(bt, key, keylen, data, datalen, &meta);
    if (ret != 0) {
	msg("btree_get failed for key '%s' with ret=%d!\n", key, ret);
	if (ret == 2) {
	    ret = FDF_OBJECT_UNKNOWN;
	} else {
	    ret = FDF_FAILURE; // xxxzzz fix this!
	}
    } else {
	ret = FDF_SUCCESS;
    }

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

    //msg("FDFReadObjectExpiry is not currently supported!");
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
    FDF_status_t      ret = FDF_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;

    my_thd_state = fdf_thread_state;;

    bt = bt_get_btree_from_cguid(cguid);
    if (bt == NULL) {
        return (FDF_FAILURE);
    }

    meta.flags = 0;

    if (flags & FDF_WRITE_MUST_NOT_EXIST) {
	ret = btree_insert(bt, key, keylen, data, datalen, &meta);
    } else if (flags & FDF_WRITE_MUST_EXIST) {
	ret = btree_update(bt, key, keylen, data, datalen, &meta);
    } else {
	ret = btree_set(bt, key, keylen, data, datalen, &meta);
    }

    switch(ret) {
        case 0:
            ret = FDF_SUCCESS;
            break;
        case FDF_SUCCESS:
            break;
        case 2:
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
    my_thd_state = fdf_thread_state;;

    //msg("FDFDeleteObject is not currently supported!");
    return(FDFDeleteObject(fdf_thread_state, cguid, key, keylen));
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
    my_thd_state = fdf_thread_state;;

    //msg("FDFEnumerateContainerObjects is not currently supported!");
    return(FDFEnumerateContainerObjects(fdf_thread_state, cguid, iterator));
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
    my_thd_state = fdf_thread_state;;

    //msg("FDFNextEnumeratedObject is not currently supported!");
    return(FDFNextEnumeratedObject(fdf_thread_state, iterator, key, keylen, data, datalen));
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
    my_thd_state = fdf_thread_state;;

    //msg("FDFFinishEnumeration is not currently supported!");
    return(FDFFinishEnumeration(fdf_thread_state, iterator));
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
    my_thd_state = fdf_thread_state;;

    //msg("FDFFlushObject is not currently supported!");
    return(FDFFlushObject(fdf_thread_state, cguid, key, keylen));
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
    my_thd_state = fdf_thread_state;;

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

/**
 * @brief Start mini transaction
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_ALREADY_IN_TRANS if thread has active transaction already
 */
FDF_status_t _FDFMiniTransactionStart(
	struct FDF_thread_state	*fdf_thread_state
	)
{
    my_thd_state = fdf_thread_state;;

    return(FDFMiniTransactionStart(fdf_thread_state));
}

/**
 * @brief Commit mini transaction
 *
 * @param fdf_thread_state <IN> The SDF context for which this operation applies
 * @return FDF_SUCCESS on success
 *         FDF_FAILURE_NO_TRANS if there is no active transaction in the current thread
 */
FDF_status_t _FDFMiniTransactionCommit(
	struct FDF_thread_state	*fdf_thread_state
	)
{
    my_thd_state = fdf_thread_state;;

    return(FDFMiniTransactionCommit(fdf_thread_state));
}

/*
 * FDFTransactionStart
 */
FDF_status_t 
_FDFTransactionStart(struct FDF_thread_state *fdf_thread_state)
{
    my_thd_state = fdf_thread_state;

    return(FDFTransactionStart(fdf_thread_state));
}


/*
 * FDFMiniTransactionCommit
 */
FDF_status_t 
_FDFTransactionCommit(struct FDF_thread_state *fdf_thread_state)
{
    my_thd_state = fdf_thread_state;

    return(FDFTransactionCommit(fdf_thread_state));
}

/*
 * FDFTransactionRollback
 */
FDF_status_t 
_FDFTransactionRollback(struct FDF_thread_state *fdf_thread_state)
{
    my_thd_state = fdf_thread_state;

    return(FDFTransactionRollback(fdf_thread_state));
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

	my_thd_state = fdf_thread_state;

	bt = bt_get_btree_from_cguid(cguid);
	if (bt == NULL) {
		return (FDF_FAILURE);
	}

	status = btree_start_range_query(bt, 
	                                (btree_indexid_t) indexid, 
	                                (btree_range_cursor_t **)cursor,
	                                (btree_range_meta_t *)rmeta);

	if (status == BTREE_SUCCESS) {
		ret = FDF_SUCCESS;
	} else {
		ret = FDF_FAILURE;
	}

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

	my_thd_state = fdf_thread_state;;

	/* TODO: Need to add additional callback to start_range_query
	 * cursor, which generically maps status of each values to
	 * whatever the user of btree needs (in this case fdf_wrapper).
	 * At present, the status numbers are matched, but thats not
	 * good for maintanability */
	status = btree_get_next_range((btree_range_cursor_t *)cursor,
	                              n_in,
	                              n_out,
	                              (btree_range_data_t *)values);

	if (status == BTREE_SUCCESS) {
		ret = FDF_SUCCESS;
	} else if (status == BTREE_QUERY_DONE) {
		ret = FDF_QUERY_DONE;
	} else {
		ret = FDF_FAILURE;
	}

	return(ret);
}

FDF_status_t 
_FDFGetRangeFinish(struct FDF_thread_state *fdf_thread_state, 
                   struct FDF_cursor *cursor)
{
	FDF_status_t      ret = FDF_SUCCESS;
	btree_status_t    status;

	my_thd_state = fdf_thread_state;;

	status = btree_end_range_query((btree_range_cursor_t *)cursor);
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

static struct btree_raw_node *read_node_cb(int *ret, void *data, uint64_t lnodeid)
{
    read_node_t            *prn = (read_node_t *) data;
    struct btree_raw_node  *n;
    uint64_t                datalen;
    // pid_t  tid = syscall(SYS_gettid);

    N_read_node++;

    *ret = FDFReadObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &n, &datalen);

    if (*ret == FDF_SUCCESS) {
	assert(datalen == prn->nodesize);
        *ret = 0;
	// xxxzzz SEGFAULT
	// fprintf(stderr, "SEGFAULT read_node_cb: %p [tid=%d]\n", n, tid);
	return(n);
    } else {
	fprintf(stderr, "ZZZZZZZZ   read_node_cb failed with ret=%d   ZZZZZZZZZ\n", *ret);
        *ret = 1;
	return(NULL);
    }
}

static void write_node_cb(int *ret_out, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen)
{
    int                     ret;
    read_node_t            *prn = (read_node_t *) cb_data;

    N_write_node++;

    ret = FDFWriteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), data, datalen, 0 /* flags */);
    assert(prn->nodesize == datalen);

    if (ret == FDF_SUCCESS) {
	*ret_out = 0;
    } else {
	fprintf(stderr, "ZZZZZZZZ   write_node_cb failed with ret=%s   ZZZZZZZZZ\n", FDFStrError(ret));
	*ret_out = 1;
    }
}

static void txn_cmd_cb(int *ret_out, void *cb_data, int cmd_type)
{
    int ret = FDF_FAILURE;

    switch (cmd_type) {
        case 1: // start txn
	    N_txn_cmd_1++;
	    ret = FDFTransactionStart(my_thd_state);
	    break;
        case 2: // commit txn
	    ret = FDFTransactionCommit(my_thd_state);
	    N_txn_cmd_2++;
	    break;
        case 3: // abort txn
	    ret = FDFTransactionRollback(my_thd_state);
	    N_txn_cmd_3++;
	    break;
	default:
	    assert(0);
	    break;
    }

    if (ret == FDF_SUCCESS) {
	*ret_out = 0;
    } else {
	*ret_out = 1;
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

static struct btree_raw_node *create_node_cb(int *ret, void *data, uint64_t lnodeid)
{
    read_node_t            *prn = (read_node_t *) data;
    struct btree_raw_node  *n;
    uint64_t                datalen;
    // pid_t  tid = syscall(SYS_gettid);

    N_create_node++;

    *ret = FDFWriteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), Create_Data, prn->nodesize, FDF_WRITE_MUST_NOT_EXIST  /* flags */);

    if (*ret == FDF_SUCCESS) {
	*ret = FDFReadObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &n, &datalen);
	if (*ret == FDF_SUCCESS) {
	    assert(datalen == prn->nodesize);
	    *ret = 0;
	    // xxxzzz SEGFAULT
	    // fprintf(stderr, "SEGFAULT create_node_cb: %p [tid=%d]\n", n, tid);
	    return(n);
	} else {
	    *ret = 1;
	    fprintf(stderr, "ZZZZZZZZ   create_node_cb failed with ret=%d   ZZZZZZZZZ\n", *ret);
	    return(NULL);
	}
    } else {
	return(NULL);
    }
}

static int delete_node_cb(struct btree_raw_node *node, void *data, uint64_t lnodeid)
{
    N_delete_node++;
    // xxxzzz finish me!
    return(0);
}

static void log_cb(int *ret, void *data, uint32_t event_type, struct btree_raw *btree, struct btree_raw_node *n)
{
    N_log++;
}

static int cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
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
        default: prefix = "PROBLEM WITH MSG_CB!"; quit = 1; break;
	    break;
    } 

    (void) fprintf(stderr, "%s: %s", prefix, stmp);
    if (quit) {
        assert(0);
        exit(1);
    }
}


static void dump_btree_stats(FILE *f, FDF_cguid_t cguid)
{
    struct btree     *bt;
    btree_stats_t     bt_stats;

    bt = bt_get_btree_from_cguid(cguid);
    if (bt == NULL) {
        return;
    }

    btree_get_stats(bt, &bt_stats);
    btree_dump_stats(f, &bt_stats);
}

