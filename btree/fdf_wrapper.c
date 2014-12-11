/************************************************************************
 * 
 *  fdf_wrapper.c  Mar. 31, 2013   Brian O'Krafka
 * 
 *  ZS wrapper functions for btree layer.
 * 
 * NOTES: xxxzzz
 *     - xxxzzz create_node_cb has no datalen arg?
 * 
 ************************************************************************/

#include <sys/time.h>
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
#include <sched.h>
#include <api/zs.h>
#include "zs.h"
#include "fdf_internal_cb.h"
#include "btree.h"
#include "btree_range.h"
#include "trx.h"
#include "btree_raw_internal.h"
#include <sys/time.h>
#include "btree_malloc.h"
#include "flip/flip.h"
#include "btree_scavenger.h"
#include "btree_sync_th.h"
#include "btree_var_leaf.h"
#include <lz4.h>
#include <pthread.h>

#define MAX_NODE_SIZE   128*1024

#include "fdf_internal.h"

#ifdef _OPTIMIZE
#undef assert
#define assert(a)
#endif

#define BTREE_DELETE_CONTAINER_NAME "B#^++$(h@@n+^\0"

#define PERSISTENT_STATS_FLUSH_INTERVAL 100000
#define ZS_ASYNC_STATS_THREADS 8
#define ASYNC_STATS_SUSPEND_NODE_COUNT 10
#define ASYNC_STATS_SUSPEND_DURATION 5

#define READ 0
#define WRITE 1
#define NOLOCK 2

struct cmap;
extern int astats_done;
extern int __zs_check_mode_on;
//extern int bt_storm_mode;

static char Create_Data[MAX_NODE_SIZE];

// For ZSCheck workers
static uint32_t cguid_idx = 0;
static pthread_mutex_t cguid_idx_lock;
static uint32_t ncguids = 0;
static ZS_cguid_t *cguids = NULL;
void * zscheck_worker(void *arg);

uint64_t n_global_l1cache_buckets = 0;
uint64_t l1reg_buckets, l1raw_buckets;
uint64_t l1cache_size = 0;
uint64_t l1reg_size, l1raw_size;
uint64_t l1cache_partitions = 0;
uint32_t node_size = 8192; /* Btree node size, Default 8K */
uint32_t btree_partitions = 1;

struct PMap *global_l1cache;
struct PMap *global_raw_l1cache;
extern int init_l1cache();
extern void destroy_l1cache();

int btree_parallel_flush_disabled = 1;
int btree_parallel_flush_minbufs = 3;

// xxxzzz temporary: used for dumping btree stats
static uint64_t  n_reads = 0;

__thread struct ZS_thread_state *my_thd_state;
struct ZS_state *my_global_zs_state;
__thread bool bad_container = 0;
uint64_t invoke_scavenger_per_n_obj_del = 10000;
__thread ZS_cguid_t my_thrd_cguid;

struct ZS_state *ZSState;

uint64_t *flash_blks_allocated;
uint64_t *flash_segs_free;
uint64_t *flash_blks_consumed;
uint64_t *flash_hash_size;
uint64_t *flash_hash_alloc;
uint64_t flash_segment_size;
uint64_t flash_block_size;
uint64_t flash_space;
uint64_t flash_space_soft_limit;
bool flash_space_soft_limit_check = true;
uint64_t flash_space_limit_failure_count;
int btree_ld_valid = true;

bool bt_shutdown = false;

typedef enum __fdf_txn_mode {
	FDF_TXN_NONE_MODE = 0,
	FDF_TXN_BTREE_MODE,
	FDF_TXN_CORE_MODE
} __fdf_txn_mode_t;

static __thread int  __fdf_txn_mode_state = FDF_TXN_NONE_MODE;
static int  __fdf_txn_mode_state_global = FDF_TXN_NONE_MODE;

/*
 * Variables to manage persistent stats
 */
char stats_ctnr_name[] = "__SanDisk_pstats_container";
uint64_t stats_ctnr_cguid = 0;
uint64_t zs_flush_pstats_frequency;


typedef struct __zs_cont_iterator {
	void * iterator;
	ZS_cguid_t cguid;	
} __zs_cont_iterator_t;


ZS_status_t 
zs_fix_objs_cnt_stats(struct ZS_thread_state *thd_state, uint64_t obj_count, char *cont_name);


bool
seqnoread(struct ZS_thread_state *t);
static bool storage_space_exhausted( const char *);
ZS_status_t BtreeErr_to_ZSErr(btree_status_t b_status);
btree_status_t ZSErr_to_BtreeErr(ZS_status_t f_status);
int mput_default_cmp_cb(void *data, char *key, uint32_t keylen,
			    char *old_data, uint64_t old_datalen,
			    char *new_data, uint64_t new_datalen);

static void read_node_cb(btree_status_t *ret, void *data, void *pnode, uint64_t lnodeid, int rawobj);
static void write_node_cb(struct ZS_thread_state *thd_state, btree_status_t *ret, void *cb_data, uint64_t** lnodeid, char **data, uint64_t datalen, int count, uint32_t flags);
static void flush_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid);
static int freebuf_cb(void *data, char *buf);
static void* create_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid);
static btree_status_t delete_node_cb(void *data, uint64_t lnodeid, int rawobj);
static void                   log_cb(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree);
static int                    lex_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2);
static void                   msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...);
static uint64_t               seqno_alloc_cb(void);
uint64_t                      seqnoalloc( struct ZS_thread_state *);
ZS_status_t btree_get_all_stats(ZS_cguid_t cguid,
                                ZS_ext_stat_t **estat, uint32_t *n_stats) ;
static ZS_status_t zs_commit_stats_int(struct ZS_thread_state *zs_thread_state, zs_pstats_t *s, char *cname);
static void* pstats_fn(void *parm);
static void* bt_restart_delcont(void *parm);
void ZSInitPstats(struct ZS_thread_state *my_thd_state, char *key, zs_pstats_t *pstats);
void ZSLoadPstats(struct ZS_state *zs_state);
static bool pstats_prepare_to_flush(struct ZS_thread_state *thd_state);
static void pstats_prepare_to_flush_single(struct ZS_thread_state *thd_state, struct cmap *cmap_entry);
ZS_status_t set_flash_stats_buffer(uint64_t *alloc_blks, uint64_t *free_segs, uint64_t *consumed_blks, uint64_t *, uint64_t *, uint64_t blk_size, uint64_t seg_size);
ZS_status_t set_zs_function_ptrs( void *log_func);
ZS_status_t btree_check_license_ptr(int lic_state);
ZS_status_t btree_get_rawobj_mode(int storm_mode, uint64_t rawobjsz, int ratio);
ZS_status_t btree_get_node_info(ZS_cguid_t cguid, char *data, uint64_t datalen, uint32_t *node_type, bool *is_root, uint64_t *logical_id);

ZS_status_t btree_process_admin_cmd(struct ZS_thread_state *thd_state, 
                                     FILE *fp, cmd_token_t *tokens, size_t ntokens);
int bt_get_cguid(ZS_cguid_t cguid);

ZS_status_t
_ZSGetRange(struct ZS_thread_state *zs_thread_state,
             ZS_cguid_t              cguid,
             ZS_indexid_t            indexid,
             struct ZS_cursor      **cursor,
             ZS_range_meta_t        *rmeta);

ZS_status_t
_ZSGetNextRange(struct ZS_thread_state *zs_thread_state,
                 struct ZS_cursor       *cursor,
                 int                      n_in,
                 int                     *n_out,
                 ZS_range_data_t        *values);

ZS_status_t
_ZSGetRangeFinish(struct ZS_thread_state *zs_thread_state,
                   struct ZS_cursor *cursor);


ZS_status_t
_ZSMPut(struct ZS_thread_state *zs_ts,
        ZS_cguid_t cguid,
        uint32_t num_objs,
        ZS_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_done);

ZS_status_t
_ZSRangeUpdate(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       ZS_range_update_cb_t callback_func,
	       void * callback_args,	
	       ZS_range_cmp_cb_t range_cmp_cb,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated);

ZS_status_t
_ZSCheckBtree(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid, uint64_t flags);

ZS_status_t
_ZSCheck(struct ZS_thread_state *zs_thread_state, uint64_t flags);

ZS_status_t
_ZSCheckInit(char *logfile); 

ZS_status_t
_ZSCheckClose();

void
_ZSCheckMsg(ZS_check_entity_t entity, 
            uint64_t id, 
            ZS_check_error_t error, 
            char *msg
            );

void
_ZSCheckSetLevel(int level);

int
_ZSCheckGetLevel();

ZS_status_t
_ZSCheckMeta(); 

ZS_status_t
_ZSCheckFlog(); 

ZS_status_t
_ZSCheckPOT(); 

ZS_status_t _ZSScavengeContainer(struct ZS_state *zs_state, ZS_cguid_t cguid);
ZS_status_t ZSStartAstats(struct ZS_state *zs_state, ZS_cguid_t cguid);
int getZSVersion();

zs_log_func zs_log_func_ptr = NULL; 

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


#define FIRST_VALID_CGUID              3
#define LAST_VALID_CGUID               UINT16_MAX
#define MAX_OPEN_CONTAINERS            LAST_VALID_CGUID + 1

ctrmap_t 	Container_Map[MAX_OPEN_CONTAINERS];
int 			N_Open_Containers = 0;
pthread_rwlock_t	ctnrmap_rwlock = PTHREAD_RWLOCK_INITIALIZER;
uint32_t            g_api_version;


static int
bt_add_cguid(ZS_cguid_t cguid)
{
    int i_ctnr = -1;

    /*
     * TODO: This lock is replaceable with container entry lock
     */
    pthread_rwlock_wrlock(&ctnrmap_rwlock);
    if (bt_shutdown == true) {
        pthread_rwlock_unlock(&ctnrmap_rwlock);
        fprintf(stderr, "Shutdown in progress, OpenContainer failed\n");
        return -2;
    }

    if (N_Open_Containers < MAX_OPEN_CONTAINERS) {
        if (0 == Container_Map[cguid].cguid) {
            Container_Map[cguid].cguid = cguid;
            Container_Map[cguid].bt_state = BT_CNTR_INIT;
            (void) __sync_add_and_fetch(&N_Open_Containers, 1);
            i_ctnr = cguid;
        } else {
            /*
             * If Container_Map has non zero value for cguid, the container is already open
             */
            i_ctnr = cguid;
        }
    } else {
        i_ctnr = -1;
    }
    pthread_rwlock_unlock(&ctnrmap_rwlock);

    return i_ctnr;
}

static int
bt_get_ctnr_from_cguid( 
    ZS_cguid_t cguid
    )
{
    int i_ctnr = -1;
    pthread_rwlock_rdlock(&ctnrmap_rwlock);
    if (bt_shutdown == true) {
        pthread_rwlock_unlock(&ctnrmap_rwlock);
        return -2;
    }

    if (cguid < MAX_OPEN_CONTAINERS) {
        if (Container_Map[cguid].cguid > 0) {
            i_ctnr = cguid;
        }
    }
    pthread_rwlock_unlock(&ctnrmap_rwlock);
    return i_ctnr;
}

static inline void cm_lock(int idx, int type)
{
	if (type == READ) {
		pthread_rwlock_rdlock(&(Container_Map[idx].bt_cm_rwlock));
	} else {
		pthread_rwlock_wrlock(&(Container_Map[idx].bt_cm_rwlock));
	}
}

static inline void cm_unlock(int idx)
{
	pthread_rwlock_unlock(&(Container_Map[idx].bt_cm_rwlock));
}

/*
 * I M P O R T A N T:
 * This routine returns the index and btree with READ LOCK of the entry
 * held. Caller need to release the lock using bt_rel_entry() routine.
 */
btree_t *
bt_get_btree_from_cguid(ZS_cguid_t cguid, int *index, ZS_status_t *error,
						bool write)
{
	int i;
	ZS_status_t	err = ZS_SUCCESS;
	btree_t			*bt = NULL;

	assert(index);
	assert(error);

	i = bt_get_ctnr_from_cguid(cguid);
	if (i == -1) {
		*error = ZS_FAILURE_CONTAINER_NOT_FOUND;
		return NULL;
	} else if (i == -2) {
		*error = ZS_FAILURE_OPERATION_DISALLOWED;
		return NULL;
	}

	cm_lock(i, READ);
	if (bt_shutdown == true) {
		cm_unlock(i);
		*error = ZS_FAILURE_OPERATION_DISALLOWED;
		return NULL;
	}

    if (Container_Map[i].flags & (1 << 0)) {
		cm_unlock(i);
		*error = ZS_FAILURE_INVALID_CONTAINER_TYPE;
		return NULL;
    }

	/* There could be a delete when we were trying to acquire lock */
	if ((Container_Map[i].cguid == cguid) &&
			(Container_Map[i].bt_state == BT_CNTR_OPEN)) {
		*index = i;
		bt = Container_Map[i].btree;
		if (write) {
			pthread_mutex_lock(&(Container_Map[i].bt_snap_mutex));
			while (Container_Map[i].snap_initiated) {
				fprintf(stderr, "Snap in progress, writer waiting\n");
				pthread_cond_wait(&(Container_Map[i].bt_snap_wr_cv),
									&(Container_Map[i].bt_snap_mutex));
				fprintf(stderr, "writer wokenup\n");
			}
			assert(Container_Map[i].snap_initiated == 0);
			(void) __sync_add_and_fetch(&(Container_Map[i].bt_wr_count), 1);
			pthread_mutex_unlock(&(Container_Map[i].bt_snap_mutex));
		} else {
			(void) __sync_add_and_fetch(&(Container_Map[i].bt_rd_count), 1);
		}
	} else {
		/* The container has been deleted while we were acquiring the lock */
		if (Container_Map[i].cguid != cguid) {
			err = ZS_FAILURE_CONTAINER_NOT_FOUND;
		} else {
			err = ZS_FAILURE_CONTAINER_NOT_OPEN;
		}
		cm_unlock(i);
	}
	*error = err;
	return bt;
}

static void
bt_rel_entry(int i, bool write)
{
	int cnt;
	if (write) {
		cnt = __sync_sub_and_fetch(&(Container_Map[i].bt_wr_count), 1);
		assert(Container_Map[i].bt_wr_count >= 0);
		if (cnt == 0) {
			pthread_mutex_lock(&(Container_Map[i].bt_snap_mutex));
			if (Container_Map[i].snap_initiated) {
				if (Container_Map[i].bt_wr_count == 0) {
					fprintf(stderr, "Writer wakingup snap thread\n");
					pthread_cond_signal(&(Container_Map[i].bt_snap_cv));
				}
			}
			pthread_mutex_unlock(&(Container_Map[i].bt_snap_mutex));
		}
	} else {
		(void) __sync_sub_and_fetch(&(Container_Map[i].bt_rd_count), 1);
		assert(Container_Map[i].bt_rd_count >= 0);
	}

	cm_unlock(i);
	return;
}

static ZS_status_t
bt_is_valid_cguid(ZS_cguid_t cguid)
{
	if (cguid <= FIRST_VALID_CGUID ) {
		return ZS_FAILURE_ILLEGAL_CONTAINER_ID;
	} else if (cguid >= MAX_OPEN_CONTAINERS) {
		return ZS_FAILURE_CONTAINER_NOT_FOUND;
	} else {
		return ZS_SUCCESS;
	}
}

static bool
bt_is_license_valid()
{
	int state;
	if (btree_ld_valid == false) {
		ZSLicenseCheck(&state);
		return ((state == 1) ? true : false);
	}

	assert(btree_ld_valid == true);
	return (true);
}


static void dump_btree_stats(FILE *f, ZS_cguid_t cguid);

//  xxxzzz end of temporary stuff!


/*
 * Get a ZS property.
 */
const char *_ZSGetProperty(const char *key, const char *def)
{
    return(ZSGetProperty(key, def));
}


/**
 * @brief set ZS property
 *
 * @param propery <IN> property name
 * @param value <IN> pointer to value
 * 
 */
ZS_status_t _ZSSetProperty(
	const char* property,
	const char* value
	)
{
    return(ZSSetProperty(property, value));
}

/**
 * @brief Load properties from specified file
 *
 * @param proper_file <IN> properties file
 * @return ZS_SUCCESS on success
 * 
 */
ZS_status_t _ZSLoadProperties(
	const char *prop_file
	)
{
    return(ZSLoadProperties(prop_file));
}


uint32_t get_btree_node_size() {
    char *zs_prop, *env;
    uint32_t nodesize;

    zs_prop = (char *)ZSGetProperty("ZS_BTREE_NODE_SIZE",NULL);
    if( zs_prop != NULL ) {
        nodesize = (atoi(zs_prop) < 0)?0:atoi(zs_prop);
    }
    else {
        env = getenv("BTREE_NODE_SIZE");
        nodesize = env ? atoi(env) : 0;
    }

    if ( !nodesize ) {
        nodesize = DEFAULT_NODE_SIZE;
    }    

    return nodesize;
}

uint32_t get_btree_min_keys_per_node() {
    return DEFAULT_MIN_KEYS_PER_NODE;
}

uint32_t get_btree_max_key_size() {
    char *zs_prop, *env;
    uint32_t max_key_size, node_meta, nodesize, min_keys_per_node;
    
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_MAX_KEY_SIZE",NULL);
    if( zs_prop != NULL ) {
        max_key_size = (atoi(zs_prop) < 0)?0:atoi(zs_prop);
    }
    else {
        env = getenv("BTREE_MAX_KEY_SIZE");
        max_key_size = env ? atoi(env) : 0;
    }    
 

    if (!max_key_size) {
        nodesize = get_btree_node_size();
        min_keys_per_node = get_btree_min_keys_per_node();
        node_meta =  sizeof(node_vkey_t);
        if (node_meta < sizeof(node_vlkey_t)) {
                node_meta =  sizeof(node_vlkey_t);
        }
        max_key_size = ((nodesize - sizeof(btree_raw_node_t))/min_keys_per_node) - node_meta;
    }
    return max_key_size;
}

uint32_t get_btree_num_partitions() {
    char * zs_prop, *env;
    uint32_t n_partitions;

    zs_prop = (char *)ZSGetProperty("ZS_BTREE_NUM_PARTITIONS",NULL);
    n_partitions = 0; 
    if( zs_prop != NULL ) {
        n_partitions = (atoi(zs_prop) < 0)?0:atoi(zs_prop);
    }    
    else {
        env = getenv("N_PARTITIONS");
        n_partitions = env ? atoi(env) : 0; 
    }    

    if(!n_partitions)
        n_partitions = DEFAULT_N_PARTITIONS;

    return n_partitions;
}

/** @brief Print Btree configuration 
 *@return None
 */
void print_zs_btree_configuration() {
    char * zs_prop, *env;
    int zs_cache_enabled = 0, read_by_rquery = 0;

    zs_prop = (char *)ZSGetProperty("ZS_CACHE_FORCE_ENABLE",NULL);
    if( zs_prop != NULL ) {
        if( atoi(zs_prop) == 1 ) {
            zs_cache_enabled = ZS_TRUE;
        }     
    }    
    else {
       if(getenv("ZS_CACHE_FORCE_ENABLE")) {
            zs_cache_enabled = ZS_TRUE;
       }    
    }  

    env = getenv("BTREE_READ_BY_RQUERY");
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_READ_BY_RQUERY",NULL);
    if( zs_prop != NULL ) {
        read_by_rquery = atoi(zs_prop);
    }
    else if(env && (atoi(env) == 1)) {
        read_by_rquery = 1;
    }

    fprintf(stderr,"Btree configuration:: Partitions:%d, L1 Cache Size:%ld bytes,"
                   " L1 cache buckets:%ld, L1 Cache Partitions:%ld,"
                   " ZS Cache enabled:%d, Node size:%d bytes,"
                   " Max key size:%d bytes, Min keys per node:%d"
                   " Max data size: %"PRIu64" bytes"
                   " Parallel flush enabled:%d Parallel flush Min Nodes:%d"
                   " Reads by query:%d Total flash space:%lu Flash space softlimit:%lu"
                   " Flash space softlimit check:%d Storm_mode:%d\n",
                   get_btree_num_partitions(), l1cache_size, n_global_l1cache_buckets, l1cache_partitions,
                   zs_cache_enabled, get_btree_node_size(), get_btree_max_key_size(), 
                   get_btree_min_keys_per_node(),
                   (uint64_t)BTREE_MAX_DATA_SIZE_SUPPORTED,
                   !btree_parallel_flush_disabled, btree_parallel_flush_minbufs,
                   read_by_rquery, flash_space, flash_space_soft_limit, flash_space_soft_limit_check, bt_storm_mode
                  );
}

#define IS_ZS_HASH_CONTAINER(FLAGS) (FLAGS & (1 << 0))

int getZSVersion()
{
    return g_api_version;
}


/**
 * @brief ZS initialization
 *
 * @param zs_state <OUT> ZS state variable
 * @param prop_file <IN> ZS property file or NULL
 * @return ZS_SUCCESS on success
 */
static uint64_t delete_prefix;
ZS_status_t _ZSInitVersioned(
	struct ZS_state	**zs_state,
	uint32_t                api_version
	)
{
    char         *stest, *zs_prop;
    int           i = 0;
    ZS_status_t  ret;
    ZS_ext_cb_t  *cbs;

    pthread_t thr1;
    int iret;

    if (api_version != ZS_API_VERSION) {
        fprintf(stderr, "Error: Incompatibile ZS API Version. ZSInit called "
                        "with version '%u', ZS API verion is '%u'\n", 
                        api_version, ZS_API_VERSION);
        return ZS_VERSION_CHECK_FAILED;       
    }
    g_api_version = api_version;

    const char *ZS_SCAVENGER_THREADS = "60";
    const char *ZS_SCAVENGE_PER_OBJECTS = "10000";
    int NThreads = 10;

    // Initialize the map
    for (i=0; i<MAX_OPEN_CONTAINERS; i++) {
        Container_Map[i].cguid = ZS_NULL_CGUID;
        Container_Map[i].btree = NULL;
        Container_Map[i].snap_initiated= 0;
        pthread_mutex_init(&(Container_Map[i].bt_snap_mutex), NULL);
        pthread_cond_init(&(Container_Map[i].bt_snap_cv), NULL);
        pthread_rwlock_init(&(Container_Map[i].bt_cm_rwlock), NULL);
    }


    char buf[32];
    sprintf(buf, "%u", ZS_ASYNC_STATS_THREADS);
    int num_astats_threads = atoi(_ZSGetProperty("ZS_ASYNC_STATS_THREADS", buf));

    astats_init(num_astats_threads);

    cbs = malloc(sizeof(ZS_ext_cb_t));
    if( cbs == NULL ) {
        return ZS_FAILURE;
    }

    memset(cbs, 0, sizeof(ZS_ext_cb_t));
    cbs->stats_cb = btree_get_all_stats;
    cbs->flash_stats_buf_cb = set_flash_stats_buffer;
    cbs->zs_funcs_cb = set_zs_function_ptrs;
    cbs->zs_lic_cb = btree_check_license_ptr;
    cbs->zs_raw_cb = btree_get_rawobj_mode;
#ifdef FLIP_ENABLED
    cbs->zs_node_cb = btree_get_node_info;
#endif
    ret = ZSRegisterCallbacks(*zs_state, cbs);
    assert(ZS_SUCCESS == ret);

    ZSSetProperty ("ZS_KEY_CACHE", ZSGetProperty("ZS_KEY_CACHE", "1"));
    ZSSetProperty ("ZS_CACHE_CHUNK_SIZE", ZSGetProperty("ZS_CACHE_CHUNK_SIZE", "0"));
    ZSSetProperty ("ZS_COMPRESSION", ZSGetProperty("ZS_COMPRESSION", "1"));

    ZSSetProperty ("ZS_TRX", ZSGetProperty("ZS_TRX", "1"));

    ret = ZSInit(zs_state);
    if ( ret != ZS_SUCCESS) {
        return ret;
    }
    ZSState = *zs_state;

    NThreads = atoi(_ZSGetProperty("ZS_SCAVENGER_THREADS", ZS_SCAVENGER_THREADS));

	scavenger_init(NThreads);
    invoke_scavenger_per_n_obj_del  = atoi(_ZSGetProperty("ZS_SCAVENGE_PER_OBJECTS",ZS_SCAVENGE_PER_OBJECTS));
    my_global_zs_state = *zs_state;
    fprintf(stderr,"Flash Space consumed:%lu flash_blocks_alloc:%lu free_segs:%lu blk_size:%lu seg_size:%lu\n",
             *flash_blks_consumed * flash_block_size, *flash_blks_allocated, *flash_segs_free, flash_block_size, flash_segment_size);

    fprintf(stderr,"Number of cache buckets:%lu\n",n_global_l1cache_buckets);
    if( init_l1cache() ){
        fprintf(stderr, "Coundn't init global l1 cache.\n");
        return ZS_FAILURE;
    }


    /*
     * Opens or creates persistent stats container
     */
    ZSLoadPstats(*zs_state);

    /*
     * Create the flusher thread
     */
    iret = pthread_create(&thr1, NULL, pstats_fn, (void *)*zs_state);
    if (iret < 0) {
        fprintf(stderr,"_ZSInit: failed to spawn persistent stats flusher\n");
		ZSState = NULL;	
        return ZS_FAILURE;
    }

	/*
	 * Restart delete of containers
	 */
	if (!__zs_check_mode_on && 
	    bt_storm_mode && 
	   (atoi(ZSGetProperty("ZS_REFORMAT", "0")) == 0)) {
		iret = pthread_create(&thr1, NULL, bt_restart_delcont, (void *)*zs_state);
		if (iret < 0) {
			fprintf(stderr,"_FDFInit: failed to spawn persistent stats flusher\n");
			ZSState = NULL;
			return ZS_FAILURE;
		}
	}


    sprintf(buf, "%u",ZS_MIN_FLASH_SIZE);
    flash_space = atoi(ZSGetProperty("ZS_FLASH_SIZE", buf));
    flash_space = flash_space * 1024 * 1024 * 1024;
    sprintf(buf,"%u", 0 );
    flash_space_soft_limit = atoi(ZSGetProperty("ZS_FLASH_SIZE_SOFT_LIMIT", buf));
    if ( flash_space_soft_limit == 0 ) {
        /* soft limit not configured. use default */
        /* Discount the cmc and vnc size for calculating the 15% reserved space */
        flash_space_soft_limit = (flash_space - ((uint64_t)(ZS_DEFAULT_CONTAINER_SIZE_KB * 1024) * 2))  * 0.85 +
                                 ((uint64_t)(ZS_DEFAULT_CONTAINER_SIZE_KB * 1024) * 2);
    }
    else {
        flash_space_soft_limit = flash_space_soft_limit * 1024 * 1024 * 1024;
    }

    sprintf(buf,"%u", 0 );
    flash_space_soft_limit_check = !atoi(ZSGetProperty("ZS_DISABLE_SOFT_LIMIT_CHECK", buf));

    sprintf(buf, "%d", PERSISTENT_STATS_FLUSH_INTERVAL);
    zs_flush_pstats_frequency = atoi(ZSGetProperty("ZS_FLUSH_PSTATS_FREQUENCY", buf));
    fprintf(stderr, "ZSInit: zs_flush_pstats_frequency = %ld\n", zs_flush_pstats_frequency);

    cbs->admin_cb = btree_process_admin_cmd;
    trxinit( );


    zs_prop = (char *)ZSGetProperty("ZS_BTREE_PARALLEL_FLUSH",NULL);
    if((zs_prop != NULL) ) {
        if (atoi(zs_prop) == 1) {
            btree_parallel_flush_disabled = 0;
        }
    }
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_PARALLEL_MINBUFS",NULL);
    if((zs_prop != NULL) ) {
        btree_parallel_flush_minbufs = atoi(zs_prop);
    } else {
        btree_parallel_flush_minbufs = 3;
    }
    print_zs_btree_configuration();
	delete_prefix = time((time_t *)&delete_prefix);
    return(ret);
}



static void
pstats_prepare_to_flush_single(struct ZS_thread_state *thd_state, struct cmap *cmap_entry)
{
    ZS_status_t ret = ZS_SUCCESS;
    uint64_t idx = 0;
    int i = 0;
    bool skip_flush = true;

    if ( !cmap_entry->btree || (cmap_entry->cguid == stats_ctnr_cguid) ) {
        return;
    }

    (void) pthread_mutex_lock( &pstats_mutex );
    zs_pstats_t pstats = { 0, 0 };
    pstats.seq_num   = seqnoalloc(thd_state);

    for ( i = 0; i < cmap_entry->btree->n_partitions; i++ ) {
        if ( true == cmap_entry->btree->partitions[i]->pstats_modified ) {
            pstats.obj_count += cmap_entry->btree->partitions[i]->stats.stat[BTSTAT_NUM_OBJS];
			pstats.num_overflw_nodes += cmap_entry->btree->partitions[i]->stats.stat[BTSTAT_OVERFLOW_NODES];

            /*
             * All Btree partitions have same sequence number
             */
            cmap_entry->btree->partitions[i]->last_flushed_seq_num = pstats.seq_num;
            cmap_entry->btree->partitions[i]->pstats_modified = false;
            skip_flush = false;
#ifdef PSTATS_1
            fprintf(stderr, "pstats_prepare_to_flush_single: last_flushed_seq_num= %ld\n", pstats.seq_num);
#endif
        }
    }
    (void) pthread_mutex_unlock( &pstats_mutex );

    if (true == skip_flush) {
#ifdef PSTATS_1
        fprintf(stderr, "pstats_prepare_to_flush_single: successfully skipped flushing pstats\n");
#endif
        return;
    }

    char *cname = cmap_entry->cname;

    ret = zs_commit_stats_int(thd_state, &pstats, cname);

#ifdef PSTATS_1
    fprintf(stderr, "pstats_prepare_to_flush_single:cguid=%ld, obj_count=%ld seq_num=%ld ret=%s\n",
            cmap_entry->cguid, pstats.obj_count, pstats.seq_num, ZSStrError(ret));
#endif

    return;
}


/*
 * Prepare stats to flush
 */
static bool
pstats_prepare_to_flush(struct ZS_thread_state *thd_state)
{
    ZS_status_t ret = ZS_SUCCESS;
    uint64_t idx = 0;
    int i = 0;
    bool skip_flush = true;

    /* 
     * For each container in container map
     */
    for (idx = 0; idx < MAX_OPEN_CONTAINERS; idx++) {
        (void) cm_lock(idx, READ);

        if ( !Container_Map[idx].btree || (Container_Map[idx].cguid == stats_ctnr_cguid)
              || (IS_ZS_HASH_CONTAINER(Container_Map[idx].flags)) ) {
            (void) cm_unlock(idx);
            continue;
        }

        (void) pthread_mutex_lock( &pstats_mutex );
        zs_pstats_t pstats = { 0, 0 };
        pstats.seq_num   = seqnoalloc(thd_state);

		skip_flush = true;

        for ( i = 0; i < Container_Map[idx].btree->n_partitions; i++ ) {
            if ( true == Container_Map[idx].btree->partitions[i]->pstats_modified ) {
                pstats.obj_count += Container_Map[idx].btree->partitions[i]->stats.stat[BTSTAT_NUM_OBJS];
				pstats.num_snap_objs += Container_Map[idx].btree->partitions[i]->stats.stat[BTSTAT_NUM_SNAP_OBJS];
				pstats.snap_data_size += Container_Map[idx].btree->partitions[i]->stats.stat[BTSTAT_SNAP_DATA_SIZE];
				pstats.num_overflw_nodes += Container_Map[idx].btree->partitions[i]->stats.stat[BTSTAT_OVERFLOW_NODES];

                /*
                 * All Btree partitions have same sequence number
                 */
                Container_Map[idx].btree->partitions[i]->last_flushed_seq_num = pstats.seq_num;
                Container_Map[idx].btree->partitions[i]->pstats_modified = false;
                skip_flush = false;
#ifdef PSTATS_1
                fprintf(stderr, "pstats_prepare_to_flush: last_flushed_seq_num= %ld\n", pstats.seq_num);
#endif
            }
        }
        (void) pthread_mutex_unlock( &pstats_mutex );

        if (true == skip_flush) {
#ifdef PSTATS_1
            fprintf(stderr, "zs_commit_stats:cguid: successfully skipped flushing pstats\n");
#endif
            cm_unlock(idx);
            continue;
        }

        char *cname = Container_Map[idx].cname;
        cm_unlock(idx);

        ret = zs_commit_stats_int(thd_state, &pstats, cname);

#ifdef PSTATS_1
        if (true == bt_shutdown) {
            fprintf(stderr, "Shutdown calls zs_commit_stats:cguid=%ld, obj_count=%ld seq_num=%ld ret=%s\n",
                    Container_Map[idx].cguid, pstats.obj_count, pstats.seq_num, ZSStrError(ret));
        }
#endif
    }
    return true;
}


/*
 * Thread to periodically flush stats to a logical container.
 * It runs until shutdown happens.
 */
static void*
pstats_fn(void *parm)
{
    assert(parm);

    ZS_status_t ret = ZS_SUCCESS;

    struct ZS_thread_state *thd_state = NULL;
    struct ZS_state *zs_state = (struct ZS_state*)parm;
    struct timespec time_to_wait;
    struct timeval now;

    bool r = true;

    ret = ZSInitPerThreadState(zs_state, &thd_state);
    assert (ZS_SUCCESS == ret);

    while (true == r) {
        if ( true == bt_shutdown ) {
            break;
        }

        gettimeofday( &now, NULL );

        time_to_wait.tv_sec  = now.tv_sec + 1;
        time_to_wait.tv_nsec = 0;

        // Lock mutex and wait for signal to release mutex
        (void) pthread_mutex_lock( &pstats_mutex );

        // Wait till system wide writes count reaches 100K or a second passes
        (void) pthread_cond_timedwait( &pstats_cond_var, &pstats_mutex, &time_to_wait );

        pthread_mutex_unlock( &pstats_mutex );

        r = pstats_prepare_to_flush(thd_state);
    }
    ret = ZSReleasePerThreadState(&thd_state);
    assert (ZS_SUCCESS == ret);

    return NULL;
}


/**
 * @brief ZS per thread state initialization
 *
 * @param zs_state <IN> ZS state variable
 * @param thd_state <OUT> ZS per thread state variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSInitPerThreadState(
	struct ZS_state		 *zs_state,
	struct ZS_thread_state	**thd_state
	)
{
    ZS_status_t ret = ZS_SUCCESS;

    /*
     * Make per thread txn mode state as per global one.
     */
    __fdf_txn_mode_state = __fdf_txn_mode_state_global;
    ret = ZSInitPerThreadState(zs_state, thd_state);
    btree_raw_alloc_thread_bufs();

    my_thd_state = *thd_state;
    return ret;
}


/**
 * @brief ZS release per thread state initialization
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSReleasePerThreadState(
	struct ZS_thread_state	**thd_state
	)
{
    ZS_status_t status = ZS_SUCCESS;
	release_per_thread_keybuf();
    status = ZSReleasePerThreadState(thd_state);
    btree_raw_free_thread_bufs();

    my_thd_state = NULL;
    return status;
}


/**
 * @brief ZS shutdown
 *
 * @param zs_state <IN> ZS state variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSShutdown(
	struct ZS_state	*zs_state
	)
{
    struct ZS_thread_state *thd_state = NULL;
    ZS_status_t ret = ZS_SUCCESS;
    zs_pstats_t pstats = { 0, 0 };
    uint64_t idx;
	int			i; 
	struct btree *btree;

	if ( !zs_state ) {
		fprintf(stderr, "ZS state is NULL");
		return ZS_INVALID_PARAMETER;
	}
    /*
     * If shutdown already happened, just return!
     */
    if (true == bt_shutdown) {
        fprintf(stderr, "_ZSShutdown: shutdown already done!\n"); 
        return ZS_INVALID_PARAMETER;
    }

	pthread_rwlock_wrlock(&ctnrmap_rwlock);

    (void)__sync_fetch_and_or(&bt_shutdown, true);
	pthread_rwlock_unlock(&ctnrmap_rwlock);


    /*
     * Try to release global per thread state
     */
    if (NULL != my_thd_state) { 
        ret = ZSReleasePerThreadState(&my_thd_state);
#ifdef PSTATS
        fprintf(stderr, "_ZSShutdown: Releasing my_thd_state ret=%s\n", ZSStrError(ret)); 
#endif
    }

    ret = ZSInitPerThreadState(zs_state, &thd_state);
    assert (ZS_SUCCESS == ret);

	for (i=0; i < MAX_OPEN_CONTAINERS; i++) {
restart:
		cm_lock(i, WRITE);
		if (Container_Map[i].bt_state == BT_CNTR_OPEN) {
            if ( ZSCHECK_NO_CHECK == ZSCheckGetLevel() ) 
			    pstats_prepare_to_flush_single(thd_state, &Container_Map[i]);
			//No need to close container here, as shutdown in core will do the same.
			//(void) ZSCloseContainer(thd_state, Container_Map[i].cguid);
			Container_Map[i].bt_state = BT_CNTR_UNUSED;
			Container_Map[i].cguid = ZS_NULL_CGUID;
			btree = Container_Map[i].btree;
			Container_Map[i].btree = NULL;
			cm_unlock(i);
			if (btree) {
				btree_destroy(btree, false);
			}
			(void) __sync_sub_and_fetch(&N_Open_Containers, 1);
		} else if (Container_Map[i].bt_state == BT_CNTR_CLOSED) {
			Container_Map[i].bt_state = BT_CNTR_UNUSED;
			Container_Map[i].cguid = ZS_NULL_CGUID;
			btree = Container_Map[i].btree;
			Container_Map[i].btree = NULL;
			cm_unlock(i);
			if (btree) {
				btree_destroy(btree, false);
			}
			(void) __sync_sub_and_fetch(&N_Open_Containers, 1);
		} else if (bt_storm_mode && (Container_Map[i].bt_state == BT_CNTR_DELETING)) {
			if (Container_Map[i].bt_rd_count || Container_Map[i].bt_wr_count) {
				cm_unlock(i);
				sched_yield();
				goto restart;
			}
			Container_Map[i].bt_state = BT_CNTR_UNUSED;
			Container_Map[i].cguid = ZS_NULL_CGUID;
			btree = Container_Map[i].btree;
			Container_Map[i].btree = NULL;
			cm_unlock(i);
			if (btree) {
				btree_destroy(btree, false);
			}
			(void) __sync_sub_and_fetch(&N_Open_Containers, 1);

		} else {
			assert(Container_Map[i].bt_state == BT_CNTR_UNUSED);
			cm_unlock(i);
		}
	}

	/*
	 * Async Delete of container wants scavenger to be running. Thus, lets kill scavenger 
	 * after the requests are handled.
	 */
	scavenger_stop();
	astats_stop();

	sched_yield();

    if ( ZSCHECK_NO_CHECK == ZSCheckGetLevel() ) {
 
        pstats_prepare_to_flush(thd_state);
        assert (ZS_SUCCESS == ZSFlushContainer(thd_state, stats_ctnr_cguid));

    }

    ret = ZSReleasePerThreadState(&thd_state);
    assert (ZS_SUCCESS == ret);

    return(ZSShutdown(zs_state));
}


/**
 * @brief ZS load default container properties
 *
 * @param props <IN> ZS container properties pointer
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSLoadCntrPropDefaults(
	ZS_container_props_t *props
	)
{
	ZS_status_t status = ZSLoadCntrPropDefaults(props);

	props->flash_only = ZS_TRUE;

	return status;
}


 /**
 * @brief Create and open a virtual container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cname <IN> container name
 * @param properties <IN> container properties
 * @param flags <IN> container open options
 * @param cmeta <IN> container open meta
 * @param cguid <OUT> container GUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSOpenContainerSpecial(
	struct ZS_thread_state	  *zs_thread_state, 
	char                      *cname, 
	ZS_container_props_t 	  *properties, 
	uint32_t                  flags_in,
	ZS_container_meta_t      *cmeta,
	ZS_cguid_t               *cguid
	)
{
    ZS_status_t  ret;
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
    bt_mput_cmp_cb_t mput_cmp_cb;
    void *mput_cmp_cb_data;
    read_node_t  *prn;
    int           index = -1;
    char         *env = NULL, *zs_prop;
    ZS_cguid_t cg1 = 0;

    my_thd_state = zs_thread_state;;

    if (!cname) {
        return(ZS_INVALID_PARAMETER);
    }


     if (getZSVersion() < 2 && (1 == IS_ZS_HASH_CONTAINER(properties->flags))) {
         msg("Hash containers are supported on Version 2 and higher\n");
         return ZS_FAILURE;
     }


     /*
      * If this is first cont open by the app, the txn type is not set and we
      * we should set it according to type of cont being opened.
      * This is workaround solution till we have unified txn support.
      */
     if (__fdf_txn_mode_state_global == FDF_TXN_NONE_MODE) {
	    if (0 == (IS_ZS_HASH_CONTAINER(properties->flags)) ) {
			__fdf_txn_mode_state_global = FDF_TXN_BTREE_MODE;
	    } else {
			__fdf_txn_mode_state_global = FDF_TXN_CORE_MODE;
	    }
     }

restart:
	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

    if ( 0 == (IS_ZS_HASH_CONTAINER(properties->flags)) ) {
        zs_prop = (char *)ZSGetProperty("ZS_CACHE_FORCE_ENABLE",NULL);
        if( zs_prop != NULL ) {
            if( atoi(zs_prop) == 1 ) {
                properties->flash_only = ZS_FALSE;
            } else {
                properties->flash_only = ZS_TRUE;
			}
        } else {
            if(getenv("ZS_CACHE_FORCE_ENABLE")) {
                properties->flash_only = ZS_FALSE;
            }
        }
        //fprintf(stderr, "ZS cache %s for container: %s\n", properties->flash_only ? "disabled" : "enabled", cname);

        if (properties->flash_only == ZS_FALSE) {
            fprintf(stderr, "WARNING: Container '%s' is enabled with ZS cache "
                    "(not flash_only) with libbtree. This could impact "
                    "the performance\n", cname);
		}
	} else {
		/*
		 * Flash only cont with fdf core is not supported.
		 */
		properties->flash_only = false;
	}

    ret = ZSOpenContainer(zs_thread_state, cname, properties, flags_in, cguid);
    if (ret != ZS_SUCCESS)
        return(ret);

    // See if metadata exists (recovered container or opened already)
    cg1 = *cguid;
    index = bt_add_cguid(*cguid);

	/* This shouldnt happen, how ZS could create container but we exhausted map */
	if (index == -1) {
		ZSCloseContainer(zs_thread_state, *cguid);
		return ZS_TOO_MANY_CONTAINERS;
	} else if (index == -2) {
		ZSCloseContainer(zs_thread_state, *cguid);
		return ZS_FAILURE_OPERATION_DISALLOWED;
	}

	assert(index == cg1);

	cm_lock(index, WRITE);

	if (bt_shutdown == true) {
		cm_unlock(index);
		ZSCloseContainer(zs_thread_state, *cguid);
		return ZS_FAILURE_OPERATION_DISALLOWED;
	}

	/* Some one deleted the container which we were re-opening */
	if (Container_Map[index].cguid != *cguid) {
		cm_unlock(index);
		goto restart;
	}

	if (flags_in & ZS_CTNR_RO_MODE) {
		Container_Map[index].read_only = false;
	} else if (flags_in & ZS_CTNR_RW_MODE) {
		Container_Map[index].read_only = false;
	} else {
		Container_Map[index].read_only = false;
	}

    if (IS_ZS_HASH_CONTAINER(properties->flags)) {
        Container_Map[index].flags |= (1 << 0);
        Container_Map[index].btree = NULL;
        Container_Map[index].bt_state = BT_CNTR_OPEN;
        cm_unlock(index);
        fprintf(stderr, "Creating/opening a HASH container\n");
        return ZS_SUCCESS;
    } else {
        Container_Map[index].flags &= ~(1 << 0);
    }

    // Metadata exists, just return if btree is not empty
    if (Container_Map[index].btree) {	
		Container_Map[index].bt_state = BT_CNTR_OPEN;
		cm_unlock(index);
    	return(ZS_SUCCESS);
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
    mput_cmp_cb_data	= (void *) prn;

    //flags = SYNDROME_INDEX;
    flags = SECONDARY_INDEX;
    if ((flags_in&ZS_CTNR_CREATE) == 0)
        flags |= RELOAD;

    /* Check first the zs.prop */
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_NUM_PARTITIONS",NULL);
    n_partitions = 0;
    if( zs_prop != NULL ) {
        n_partitions = (atoi(zs_prop) < 0)?0:atoi(zs_prop);
    } 
    else {
        env = getenv("N_PARTITIONS");
        n_partitions = env ? atoi(env) : 0;
    }

    if(!n_partitions)
        n_partitions = DEFAULT_N_PARTITIONS;

    /* Check first the zs.prop */
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_NODE_SIZE",NULL);
    if( zs_prop != NULL ) {
        nodesize = (atoi(zs_prop) < 0)?0:atoi(zs_prop);
    }
    else {
        env = getenv("BTREE_NODE_SIZE");
        nodesize = env ? atoi(env) : 0;
    }

    if (!nodesize) {
	nodesize            = DEFAULT_NODE_SIZE;
    }

    min_keys_per_node   = DEFAULT_MIN_KEYS_PER_NODE;

    /* Check first the zs.prop */
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_MAX_KEY_SIZE",NULL);
    if( zs_prop != NULL ) {
        max_key_size = (atoi(zs_prop) < 0)?0:atoi(zs_prop); 
    }
    else {
        env = getenv("BTREE_MAX_KEY_SIZE");
        max_key_size = env ? atoi(env) : 0;
    }

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
    msg("Creating a b-tree in ZSOpenContainer...\n");
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

    mput_cmp_cb = mput_default_cmp_cb;
    if (cmeta) {
        if (cmeta->mput_cmp_fn) mput_cmp_cb = (bt_mput_cmp_cb_t) cmeta->mput_cmp_fn;
        if (cmeta->mput_cmp_cb_data) mput_cmp_cb_data = cmeta->mput_cmp_cb_data;
    }

    /*
     * Load persistent stats for the container
     */
    zs_pstats_t pstats = { 0 };
    strcpy(Container_Map[index].cname, cname);
    ZSInitPstats(zs_thread_state, cname, &pstats);

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
		    mput_cmp_cb,
		    mput_cmp_cb_data,
                    trx_cmd_cb,
		    *cguid,
                    &pstats,
                    (seqno_alloc_cb_t *)seqno_alloc_cb
		    );

    if (bt == NULL) {
	if (bad_container == 1) {
		msg("ZSOpenContainer failed with Error: ZS_CONTAINER_UNKNOWN");
		ret = ZS_CONTAINER_UNKNOWN;
		bad_container = 0;
		ZSCloseContainer(zs_thread_state, *cguid);
		ZSDeleteContainer(zs_thread_state, *cguid);
                goto fail;
	}
        msg("Could not create btree in ZSOpenContainer!");
        //ZSDeleteContainer(zs_thread_state, *cguid);
		ZSCloseContainer(zs_thread_state, *cguid);
		goto fail;
    }

    pthread_rwlock_init(&(bt->snapop_rwlock), NULL);
    env = getenv("BTREE_READ_BY_RQUERY");
    Container_Map[index].read_by_rquery = 0;
    zs_prop = (char *)ZSGetProperty("ZS_BTREE_READ_BY_RQUERY",NULL);
    if( zs_prop != NULL ) {
        Container_Map[index].read_by_rquery = atoi(zs_prop); 
    }
    else if(env && (atoi(env) == 1)) {
        Container_Map[index].read_by_rquery = 1;
    }
    if( Container_Map[index].read_by_rquery != 0 ) {
        fprintf(stderr,"Reads will be done through range query\n");
        Container_Map[index].read_by_rquery = 1;
    }

    // xxxzzz we should remember the btree info in a persistent place
    // xxxzzz for now, for performance testing, just use a hank

	Container_Map[index].btree = bt;
	if ((bt->partitions[0])->snap_meta->sc_status & SC_OVERFLW_DELCONT && !__zs_check_mode_on) {
		Scavenge_Arg_t s;

		__sync_add_and_fetch(&(Container_Map[index].bt_wr_count), 1);
		Container_Map[index].bt_state = BT_CNTR_DELETING;

		s.type = SC_OVERFLW_DELCONT;
		s.cguid = *cguid;
		s.zs_state = my_global_zs_state;
		s.btree_index = index;
		s.bt = Container_Map[index].btree;
		s.btree = Container_Map[index].btree->partitions[0];
		s.throttle_value = 0;
		btree_scavenge(my_global_zs_state, s);
		//btSyncMboxPost(&mbox_scavenger, (uint64_t)s);

		Container_Map[index].scavenger_state = 1;
		cm_unlock(index);

		fprintf(stderr,"Before the restart delete container was terminated ungracefully,  restarting the deletion of this container\n");
		return(ZS_FAILURE_CONTAINER_DELETED);
	} else {
		Container_Map[index].bt_state = BT_CNTR_OPEN;
		cm_unlock(index);
	}

	//Flush root node, so that it exists irrespective of durability level
	if (flags_in&ZS_CTNR_CREATE) {
		ZSFlushContainer(zs_thread_state, *cguid);
	}

	if ((bt->partitions[0])->snap_meta->sc_status & SC_STALE_ENT) {
		fprintf(stderr,"Before the restart scavenger was terminated ungracefully,  restarting the scavenger on this container\n");
		_ZSScavengeContainer(my_global_zs_state, *cguid);
	}

#if 0
    /*   
     * Check if a session is restarted
     */
    char *reformat = (char*)ZSGetProperty("ZS_REFORMAT", NULL);
    char *astats_enable = (char*)ZSGetProperty("ZS_ASYNC_STATS_ENABLE", NULL);
    if (reformat && reformat[0] != '0') {
        fprintf(stderr, "ZSOpenContainer: Disabling async stats workers\n");
        astats_done = 1;
    } else {
        if (astats_enable && astats_enable[0] == '1') {
            fprintf(stderr, "ZSOpenContainer: Starting async thread\n");
            ZSStartAstats(ZSState, *cguid);
        } else {
			astats_done = 1;
		}
    }
#endif
    return(ZS_SUCCESS);

fail:
	if (Container_Map[index].bt_state == BT_CNTR_INIT) {
		Container_Map[index].bt_state = BT_CNTR_CLOSED;
	}
	cm_unlock(index);
	return(ret);
}

ZS_status_t _ZSOpenContainer(
	struct ZS_thread_state	*zs_thread_state, 
	char                    *cname, 
	ZS_container_props_t 	*properties, 
	uint32_t                flags_in,
	ZS_cguid_t             *cguid
	)
{
	if (!cname) {
		return(ZS_INVALID_PARAMETER);
	}
	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	if (properties) {
		if (properties->writethru == ZS_FALSE) {
			properties->writethru = ZS_TRUE;
			Notice("WriteBack Mode is not supported, writethru is set to true for Container %s\n",cname);
		}
		if (properties->evicting == ZS_TRUE) {
			Notice("Eviction is not supported, evicting is reset to false for container %s\n", cname);
			properties->evicting = ZS_FALSE;
		}
		if (properties->durability_level == ZS_DURABILITY_PERIODIC) {
			Notice("PERIODIC durability is not supported, set to SW_CRASH_SAFE for %s\n", cname);
			properties->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
        }
        if (properties->async_writes == 1) {
            if (1 == (IS_ZS_HASH_CONTAINER(properties->flags))) {
                properties->async_writes = 1;
                Notice("Async Writes feature is supported on only hash ctnr. So enabling it for the container %s\n", cname);
            } else {

                Notice("Async Writes feature is not supported. So disabling it for the container %s\n", cname);
                properties->async_writes = 0; 
            }
        }
    }

	if (flags_in & ZS_CTNR_RO_MODE) {
		Notice("Read-only is not supported, set to read-write mode for container %s\n", cname);
		flags_in &= ~ZS_CTNR_RO_MODE;
		flags_in |= ZS_CTNR_RW_MODE;
	}

	return (_ZSOpenContainerSpecial(zs_thread_state,
				cname,
				properties,
				flags_in,
				NULL,
				cguid));
}

/**
 * @brief Close a virtual container.
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSCloseContainer(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t				 cguid
	)
{
    my_thd_state = zs_thread_state;;
    ZS_status_t	status = ZS_FAILURE;
    int				index;

#ifdef ASTATS_DEBUG
    while (astats_done == 0) sleep(1);
    dump_btree_stats(stderr, cguid);
#endif


restart:
    if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return status;
    }

    if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
    } else if (index == -2) {
		fprintf(stderr, "Shutdown in progress, CloseContainer failed\n");
		return ZS_FAILURE_OPERATION_DISALLOWED;
    }

    cm_lock(index, WRITE);

    if (bt_shutdown == true) {
        cm_unlock(index);
        fprintf(stderr, "Shutdown in progress, CloseContainer failed\n");
        return ZS_FAILURE_OPERATION_DISALLOWED;
    }

    /* Some one might have deleted it, while we were trying to acquire lock */
    if (Container_Map[index].cguid != cguid) {
        cm_unlock(index);
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
    } else if (Container_Map[index].bt_state == BT_CNTR_DELETING) {
        cm_unlock(index);
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
	}


    if (Container_Map[index].bt_state != BT_CNTR_OPEN) {
        cm_unlock(index);
        return ZS_FAILURE_CONTAINER_NOT_OPEN;
    }

    /* IO must not exist on this entry since we have write lock now */
    assert(Container_Map[index].bt_rd_count == 0);
    assert(Container_Map[index].bt_wr_count == 0);
    if (Container_Map[index].bt_rd_count ||
            Container_Map[index].bt_wr_count) {
        cm_unlock(index);
        while (Container_Map[index].bt_rd_count ||
                Container_Map[index].bt_wr_count) {
            sched_yield();
        }
        goto restart;
    }

    /* Lets block further IOs */
    Container_Map[index].bt_state = BT_CNTR_CLOSING;

    /*
     * Handle hash container case
     */
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[index].flags)) {
        Container_Map[index].iter = NULL;
#ifdef UNIFIED_CNTR_DEBUG
        fprintf(stderr, "Closing HASH container\n");
#endif
    } else {
        /*
         * Flush persistent stats
         */
        pstats_prepare_to_flush_single(zs_thread_state, &Container_Map[index]);
    }

    status = ZSCloseContainer(zs_thread_state, cguid);

    Container_Map[index].bt_state = BT_CNTR_CLOSED;
    cm_unlock(index);
    return status;
}

/**
 * @brief Delete a virtual container
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container CGUID
 * @return ZS_SUCCESS on success
 */

ZS_status_t _ZSDeleteContainer(
        struct ZS_thread_state *zs_thread_state,
        ZS_cguid_t				 cguid
        )
{
    int				index;
    ZS_status_t 	status = ZS_FAILURE;
    my_thd_state 	= zs_thread_state;;
    struct btree	*btree = NULL;

restart:
	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

	if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
		return ZS_FAILURE_CONTAINER_NOT_FOUND;
	} else if (index == -2) {
		fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
		return ZS_FAILURE_OPERATION_DISALLOWED;
	}

	cm_lock(index, WRITE);

	if (bt_shutdown == true) {
		cm_unlock(index);
		fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
		return ZS_FAILURE_OPERATION_DISALLOWED;
	}

	/* Some one might have deleted it, while we were trying to acquire lock */
	if (Container_Map[index].cguid != cguid) {
		cm_unlock(index);
		return ZS_FAILURE_CONTAINER_NOT_FOUND;
	} else if (Container_Map[index].bt_state == BT_CNTR_DELETING) {
		cm_unlock(index);
		return ZS_FAILURE_CONTAINER_NOT_FOUND;
	}


	/* IO might have started on this container. Lets wait and retry */
	assert(Container_Map[index].bt_rd_count == 0);
	assert(Container_Map[index].bt_wr_count == 0);
	if (Container_Map[index].bt_rd_count ||
	    Container_Map[index].bt_wr_count) {
		cm_unlock(index);
		while (Container_Map[index].bt_rd_count ||
		        Container_Map[index].bt_wr_count) {
			pthread_yield();
		}
		goto restart;
	}

	if (bt_storm_mode && (IS_ZS_HASH_CONTAINER(Container_Map[index].flags) == 0)) {
		Scavenge_Arg_t			s;
		ZS_container_props_t	pprops;

		ZSLoadCntrPropDefaults(&pprops);
		status = ZSGetContainerProps(zs_thread_state, Container_Map[index].cguid,
				&pprops);
		if (status != ZS_SUCCESS) {
			fprintf(stderr,"ZSGetContainerProps failed with error %s\n",
					ZSStrError(status));
			cm_unlock(index);
			return status;
		}

		/* If the container is closed, reopen it to flush metadata */
		if (Container_Map[index].bt_state == BT_CNTR_CLOSED) {
			status = ZSOpenContainer(zs_thread_state, pprops.name, &pprops, 
					ZS_CTNR_RW_MODE, &cguid);
			if (status != ZS_SUCCESS) {
				fprintf(stderr,"ZSOpenContainer failed with error %s\n",
						ZSStrError(status));
				cm_unlock(index);
				return status;
			}
		}

		char        cname[CONTAINER_NAME_MAXLEN] = {0};
		(void)__sync_fetch_and_add(&delete_prefix, 1);
		snprintf(cname,CONTAINER_NAME_MAXLEN,"%s_%lx%lx_%s",BTREE_DELETE_CONTAINER_NAME, delete_prefix, random(), pprops.name);
		if ((status = ZSRenameContainer(zs_thread_state, Container_Map[index].cguid, cname)) != ZS_SUCCESS) {
			fprintf(stderr,"ZSRenameContainer failed with error %s %d\n",  ZSStrError(status), (int)Container_Map[index].cguid);
			cm_unlock(index);
			return status;
		}

		strncpy(Container_Map[index].cname, cname, CONTAINER_NAME_MAXLEN);
		btree = Container_Map[index].btree;
		btree->partitions[0]->snap_meta->sc_status |= SC_OVERFLW_DELCONT;
		if ((status = savepersistent(btree->partitions[0], FLUSH_SNAPSHOT, true)) == BTREE_SUCCESS) {
			/*
			 * Increment IO count so that shutdown doesnt go through. This thread
			 * sends message to scavenger and releases the lock. By the time scavenger
			 * starts and checks shutdown might have gone through. So, need to stop
			 * shutdown in this entry so that scavenger gets consistent entry when
			 * it starts.
			 */
			__sync_add_and_fetch(&(Container_Map[index].bt_wr_count), 1);
			Container_Map[index].scavenger_state = 1;
			/* Lets block further IOs */
			Container_Map[index].bt_state = BT_CNTR_DELETING;
			status = ZS_SUCCESS;

			/*
			 * Send a msg to scavenger to delete overflow nodes n container.
			 * Container map lock will be released by scavenger thread.
			 */
			s.type = SC_OVERFLW_DELCONT;
			s.cguid = cguid;
			s.zs_state = my_global_zs_state;
			s.btree_index = index;
			s.bt = Container_Map[index].btree;
			s.btree = Container_Map[index].btree->partitions[0];
			s.throttle_value = 0;
			btree_scavenge(my_global_zs_state, s);
			//btSyncMboxPost(&mbox_scavenger, (uint64_t)s);
			cm_unlock(index);
			ZSDeleteObject(zs_thread_state, stats_ctnr_cguid, pprops.name, strlen(pprops.name)+1);
		} else {
			fprintf(stderr, "savepersistent SC_OVERFLW_DELCONT failed %s\n", ZSStrError(status));
			btree->partitions[0]->snap_meta->sc_status &= ~SC_OVERFLW_DELCONT;
			cm_unlock(index);
			/* Remove this on handling deleting closed container */
			status = ZS_FAILURE;
		}
	} else {
		ZS_container_props_t	pprops;

		/* Lets block further IOs */
		Container_Map[index].bt_state = BT_CNTR_DELETING;


		/* Let us mark the entry NULL and then delete, so that we dont get the same
		   cguid if there is a create container happening */

		if (0 == IS_ZS_HASH_CONTAINER(Container_Map[index].flags)) {
			btree = Container_Map[index].btree;

			ZSLoadCntrPropDefaults(&pprops);
			status = ZSGetContainerProps(zs_thread_state, Container_Map[index].cguid,
					&pprops);
			if (status != ZS_SUCCESS) {
				fprintf(stderr,"ZSGetContainerProps failed with error %s\n",
						ZSStrError(status));
				cm_unlock(index);
				return status;
			}
			ZSDeleteObject(zs_thread_state, stats_ctnr_cguid, pprops.name, strlen(pprops.name)+1);
		} else {
			btree = NULL;
		}

		Container_Map[index].cguid = ZS_NULL_CGUID;
		Container_Map[index].btree = NULL;
		Container_Map[index].bt_state = BT_CNTR_UNUSED;
		//Container_Map[index].flags |= ~0;
		(void) __sync_sub_and_fetch(&N_Open_Containers, 1);
		cm_unlock(index);

		if (btree) {
			trxdeletecontainer( zs_thread_state, cguid);
			btree_destroy(btree, true);
		}

		status = ZSDeleteContainer(zs_thread_state, cguid);

	}
	return(status);
}

/**
 * @brief Get container list
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies.
 * @param cguids  <OUT> pointer to container GUID array
 * @param n_cguids <OUT> pointer to container GUID count
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSGetContainers(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t             *cguids,
	uint32_t                *n_cguids
	)
{
    my_thd_state = zs_thread_state;;
	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

    return (ZSGetContainers(zs_thread_state, cguids, n_cguids)); 

}

/**
 * @brief Get container properties
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSGetContainerProps(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t            	 cguid,
	ZS_container_props_t	*pprops
	)
{
    my_thd_state = zs_thread_state;;
	ZS_status_t	ret;
	int index;
	ZS_container_props_t contprop;

	assert(pprops);

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

	if (bt_storm_mode) {
		if ((index = bt_get_ctnr_from_cguid(cguid)) == -2) {
			fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
			return ZS_FAILURE_OPERATION_DISALLOWED;
		} else if (index == -1) {
			ret = ZSGetContainerProps(zs_thread_state, cguid, &contprop);
			if (ret == ZS_SUCCESS) {
				if (!strncmp(contprop.name, BTREE_DELETE_CONTAINER_NAME,
								 strlen(BTREE_DELETE_CONTAINER_NAME))) {
					ret = ZS_FAILURE_CONTAINER_NOT_FOUND;
				} else {
					memcpy(pprops, &contprop, sizeof(ZS_container_props_t));
				}
			} 
			return ret;
		} else {
			cm_lock(index, WRITE);
			if (bt_shutdown == true) {
				cm_unlock(index);
				fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
				return ZS_FAILURE_OPERATION_DISALLOWED;
			}

			/* Some one might have deleted it, while we were trying to acquire lock */
			if (Container_Map[index].cguid != cguid) {
				cm_unlock(index);
				return ZS_FAILURE_CONTAINER_NOT_FOUND;
			} else if (Container_Map[index].bt_state == BT_CNTR_DELETING) {
				cm_unlock(index);
				return ZS_FAILURE_CONTAINER_NOT_FOUND;
			}
			cm_unlock(index);
		}
	}
	return(ZSGetContainerProps(zs_thread_state, cguid, pprops));
}

/**
 * @brief Set container properties
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies
 * @param cguid <IN> container global identifier
 * @param pprops <IN> pointer to structure into which to copy properties
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSSetContainerProps(
	struct ZS_thread_state 	*zs_thread_state,
	ZS_cguid_t              	 cguid,
	ZS_container_props_t   	*pprops
	)
{
    my_thd_state = zs_thread_state;;
	ZS_status_t	ret;
	int             index;
	ZS_container_props_t    contprop;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

	if (bt_storm_mode) {
		if ((index = bt_get_ctnr_from_cguid(cguid)) == -2) {
			fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
			return ZS_FAILURE_OPERATION_DISALLOWED;
		} else if (index == -1) {
			ret = ZSGetContainerProps(zs_thread_state, cguid, &contprop);
			if (ret == ZS_SUCCESS) {
				if (!strncmp(contprop.name, BTREE_DELETE_CONTAINER_NAME,
					strlen(BTREE_DELETE_CONTAINER_NAME))) {
					return ZS_FAILURE_CONTAINER_NOT_FOUND;
				}
			} else {
				return ret;
			}
		} else {
			cm_lock(index, WRITE);
			if (bt_shutdown == true) {
				cm_unlock(index);
				fprintf(stderr, "Shutdown in progress, DeleteContainer failed\n");
				return ZS_FAILURE_OPERATION_DISALLOWED;
			}
			/* Some one might have deleted it, while we were trying to acquire lock */
			if (Container_Map[index].cguid != cguid) {
				cm_unlock(index);
				return ZS_FAILURE_CONTAINER_NOT_FOUND;
			} else if (Container_Map[index].bt_state == BT_CNTR_DELETING) {
				cm_unlock(index);
				return ZS_FAILURE_CONTAINER_NOT_FOUND;
			}
			
			cm_unlock(index);
		}
	}

	if (pprops && (pprops->durability_level == ZS_DURABILITY_PERIODIC)) {
		pprops->durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
	}

    return(ZSSetContainerProps(zs_thread_state, cguid, pprops));
}

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param data <IN> Variable in which to return a pointer to the object (in a buffer
 *  allocated by SDF; it must be freed by the application with a call
 *  to SDFFreeObjectBuffer).
 *  @param datalen <OUT> Pointer to the variable in which to return the actual size of
 *  an object.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSReadObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen,
	char                     **data,
	uint64_t                 *datalen
	)
{
    ZS_status_t      ret = ZS_SUCCESS;
    btree_status_t    btree_ret = BTREE_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;
    int index;
    btree_range_meta_t   rmeta;
    btree_range_cursor_t *cursor;
    btree_range_data_t   values[1];
    int n_out;


	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    {
        // xxxzzz this is temporary!
        __sync_fetch_and_add(&(n_reads), 1);
        if (0 && (n_reads % 200000) == 0) {
            dump_btree_stats(stderr, cguid);
        }
    }

    my_thd_state = zs_thread_state;

    if (ZS_SUCCESS != (ret = ZSOperationAllowed())) {
        msg("Shutdown in Progress. Read object not allowed\n");
        return (ret);
    }

    if ((index = bt_get_ctnr_from_cguid(cguid)) == -1) {
        return ZS_FAILURE_CONTAINER_NOT_FOUND;
    }

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        ret = ZSReadObject(zs_thread_state, cguid, key, keylen, data, datalen);
        cm_unlock(cguid);
#ifdef UNIFIED_CNTR_DEBUG
        fprintf(stderr, "Reading objectfrom a HASH container\n");
#endif
        return ret;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return (ret);
    }

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
        bt_rel_entry(index, false);
        return (ZS_KEY_TOO_LONG);
    }

    meta.flags = 0;
    __sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_READ_CNT]),1);
    if (Container_Map[index].read_by_rquery) {
        rmeta.key_start    = key;
        rmeta.keylen_start = keylen;
        rmeta.key_end      = key;
        rmeta.keylen_end   = keylen;
        rmeta.flags        = RANGE_START_GE | RANGE_END_LE;

	trxenter( cguid);
        btree_ret = btree_raw_range_query_start(bt->partitions[0], 
                                            BTREE_RANGE_PRIMARY_INDEX,
                                            &cursor, &rmeta);
		if (btree_ret != BTREE_SUCCESS) {
				msg("Could not create start range query in ZSReadObject!");
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
    ret = BtreeErr_to_ZSErr(btree_ret);
    bt_rel_entry(index, false);
    return(ret);
}

/**
 *  @brief Get a copy of an object for read-only  access. Return its current expiry time.
 *
 *  Get an object and copy it into an SDF-allocated buffer. The application
 *  only intends to read the object. The current expiry time is returned.
 *
 *  @param zs_thread_state <IN> The ZS context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param robj <IN> Identity of a read object structure
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSReadObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_readobject_t         *robj
    )
{
	my_thd_state = zs_thread_state;;
	ZS_status_t	ret;
	int				index;
	struct btree	*bt;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
	if (bt == NULL) {
		return ret;
	}

	ret = ZSReadObjectExpiry(zs_thread_state, cguid, robj);
	bt_rel_entry(index, false);

	return(ret);
}


/**
 * @brief Free an object buffer
 *
 * @param buf <IN> object buffer
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSFreeBuffer(
	char *buf
	)
{
    // pid_t  tid = syscall(SYS_gettid);

    // xxxzzz SEGFAULT
    // fprintf(stderr, "SEGFAULT ZSFreeBuffer: %p [tid=%d]\n", buf, tid);
	btree_free(buf);

	return ZS_SUCCESS;
}


/**
 *  @brief Write entire object, creating it if necessary.  
 *
 *  Put an entire object, with contents copied from an application-provided
 *  buffer. This may change the size of the object. The expiry time
 *  is set. If the object does not exist, create it and assign its
 *  value.
 *
 *  @param zs_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *  @param datalen <IN> Size of object.
 *  @param data <IN> Pointer to application buffer from which to copy data.
 *  @param flags <IN> create/update flags
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OUT_OF_MEM: there is insufficient memory/flash.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSWriteObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t          cguid,
	char                *key,
	uint32_t             keylen,
	char                *data,
	uint64_t             datalen,
	uint32_t	     flags
	)
{
    ZS_status_t      ret = ZS_FAILURE;
    btree_status_t    btree_ret = BTREE_FAILURE;
    btree_metadata_t  meta;
    struct btree     *bt;
	int				  index;

    if (__zs_check_mode_on) {
	/*
	 * We are in checker mode so do not allow any new
	 * write or update in objects.
	 */
	return ZS_FAILURE_OPERATION_DISALLOWED;	
    }   

    my_thd_state = zs_thread_state;;

    if (bt_is_license_valid() == false) {
        return (ZS_LICENSE_CHK_FAILED);
    }

    if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return ret;
    }

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        ret = ZSWriteObject(my_thd_state, cguid, key, keylen, data, datalen, flags);
#ifdef UNIFIED_CONTAINER_DEBUG
        fprintf(stderr, "Writing object to a HASH container\n");
#endif
        cm_unlock(cguid);
        return ret;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
    if (bt == NULL) {
        return (ret);
    }

    if (Container_Map[index].read_only == true) {
        bt_rel_entry(index, true);
        return ZS_FAILURE;
    }

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
		bt_rel_entry(index, true);
        return (ZS_KEY_TOO_LONG);
    }

    if (datalen > BTREE_MAX_DATA_SIZE_SUPPORTED) {
        msg("btree_insert/update datalen(%"PRIu64") more than max supported "
            "datalen(%"PRIu64")\n", datalen, BTREE_MAX_DATA_SIZE_SUPPORTED);

        bt_rel_entry(index, true);
        return (ZS_OBJECT_TOO_BIG);
    }

    if (storage_space_exhausted( "ZSWriteObject")) {
	bt_rel_entry( index, true);
	return (ZS_OUT_OF_STORAGE_SPACE);
    }

    meta.flags = 0;
    __sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_WRITE_CNT]),1);
    trxenter( cguid);
    if (flags & ZS_WRITE_MUST_NOT_EXIST) {
		btree_ret = btree_insert(bt, key, keylen, data, datalen, &meta);
    } else if (flags & ZS_WRITE_MUST_EXIST) {
		btree_ret = btree_update(bt, key, keylen, data, datalen, &meta);
    } else {
		btree_ret = btree_set(bt, key, keylen, data, datalen, &meta);
    }
    trxleave( cguid);

    ret = BtreeErr_to_ZSErr(btree_ret);
    if ((ret == ZS_OBJECT_UNKNOWN) && (flags & ZS_WRITE_MUST_NOT_EXIST))
    {
        ret = ZS_OBJECT_EXISTS;
    }
    if (ret != ZS_SUCCESS)
    {
        char key1[256];
        strncpy(key1,key,255);
        key1[255] = '\0';
        msg("btree_insert/update failed for key '%s' with ret=%s!\n", key1, ZSStrError(ret));
    }
    bt_rel_entry(index, true);
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
 *  @param zs_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param wobj <IN> Identity of a write object structure
 *  @param flags <IN> create/update flags
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OUT_OF_MEM: there is insufficient memory/flash.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSWriteObjectExpiry(
    struct ZS_thread_state  *zs_thread_state,
    ZS_cguid_t               cguid,
    ZS_writeobject_t        *wobj,
    uint32_t                  flags
    )
{
    my_thd_state = zs_thread_state;;
    ZS_status_t	ret;
    int				index;
    struct btree	*bt;

    if (bt_is_license_valid() == false) {
        return (ZS_LICENSE_CHK_FAILED);
    }

    if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return ret;
    }

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
    if (bt == NULL) {
        return ret;
    }

    //msg("ZSWriteObjectExpiry is not currently supported!");
    ret = ZSWriteObjectExpiry(zs_thread_state, cguid, wobj, flags);

    bt_rel_entry(index, true);
    return(ret);
}

/**
 *  @brief Delete an object, but check for expiry first.
 *
 *  Delete an object, but check for expiry first.
 *
 *  @param zs_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSDeleteObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	)
{
    ZS_status_t      ret = ZS_SUCCESS;
    btree_status_t    btree_ret = BTREE_SUCCESS;
    btree_metadata_t  meta;
    struct btree     *bt;
	int				index;

    my_thd_state = zs_thread_state;;
	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags)) {
        ret = ZSDeleteObject(my_thd_state, cguid, key, keylen);
        // fprintf(stderr, "Deleting object from a HASH container\n");
        cm_unlock(cguid);
        return ret;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
    if (bt == NULL) {
        return (ret);
    }

    if (Container_Map[index].read_only == true) {
       bt_rel_entry(index, true);
       return ZS_FAILURE;
    }

    bt = Container_Map[index].btree;

    my_thrd_cguid = cguid;
    meta.flags = 0;

    trxenter( cguid);
    btree_ret = btree_delete(bt, key, keylen, &meta);
    trxleave( cguid);
    ret = BtreeErr_to_ZSErr(btree_ret);
    bt_rel_entry(index, true);
    return(ret);
}

/**
 * @brief Enumerate container objects
 *
 * @param zs_thread_state <IN> The SDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param iterator <IN> enumeration iterator
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSEnumerateContainerObjects(
	struct ZS_thread_state *zs_thread_state,
	ZS_cguid_t              cguid,
	struct ZS_iterator    **iterator
	)
{
	ZS_range_meta_t rmeta;
	ZS_status_t ret;
	struct btree *bt;
	int index;
	__zs_cont_iterator_t *itr = NULL;
	struct ZS_iterator *itr_int = NULL;

	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
			return ret;
	}

	itr = btree_malloc(sizeof(*itr));
	itr->cguid = cguid;

	/*
	 * Handle hashed container case
	 */
	cm_lock(cguid, READ);
	if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags)) {
		ret = ZSEnumerateContainerObjects(zs_thread_state, cguid, &itr_int);
		itr->iterator = itr_int;

		*iterator = (void *) itr;
		cm_unlock(cguid);
		return ret;
	}
	cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
	if (bt == NULL) {
		return (ret);
	}
	bzero(&rmeta, sizeof(ZS_range_meta_t));
	ret = _ZSGetRange(zs_thread_state, 
			    cguid, 
			ZS_RANGE_PRIMARY_INDEX, 
                        (struct ZS_cursor **) &itr_int, 
                        &rmeta);
	bt_rel_entry(index, false);

	itr->iterator = itr_int;

	*iterator = (void *) itr;
	return(ret);
}

/**
 * @brief Container object enumration iterator
 *
 * @param zs_thread_state <IN> The SDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @param cguid  <IN> container global identifier
 * @param key <OUT> pointer to key variable
 * @param keylen <OUT> pointer to key length variable
 * @param data <OUT> pointer to data variable
 * @param datalen <OUT> pointer to data length variable
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSNextEnumeratedObject(
	struct ZS_thread_state *zs_thread_state,
	struct ZS_iterator  *iterator,
	char                    **key,
	uint32_t                *keylen,
	char                    **data,
	uint64_t                *datalen
	)
{
	ZS_status_t     status = ZS_FAILURE;
	int              count = 0;
	ZS_range_data_t values;
	__zs_cont_iterator_t *itr = (__zs_cont_iterator_t *) iterator;

	ZS_cguid_t cguid = itr->cguid;
	uint64_t i;

	/*
	 * Handle hashed container case
	 */
	cm_lock(cguid, READ);
	if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags)) {
		status = ZSNextEnumeratedObject(zs_thread_state, itr->iterator, key, keylen, data, datalen);
		cm_unlock(cguid);
		return status;
	}
	cm_unlock(cguid);

	values.key = NULL;
	values.data = NULL;
	status = _ZSGetNextRange(zs_thread_state,
                              (struct ZS_cursor *) itr->iterator,
                              1,
                              &count,
                              &values);

	if (ZS_SUCCESS == status &&
	    ZS_RANGE_DATA_SUCCESS == values.status && count) {
		assert(count); // Hack
		*key = (char *) malloc(values.keylen);
		assert(*key);
		strncpy(*key, values.key, values.keylen);
		*keylen = values.keylen;
		*data = (char *) malloc(values.datalen);
		assert(*data);
		strncpy(*data, values.data, values.datalen);
		*datalen = values.datalen;
	} else {
		*key = NULL;
		*keylen = 0;
		*data = NULL;
		*datalen = 0;
		status = ZS_OBJECT_UNKNOWN;
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
 * @param zs_thread_state <IN> The SDF context for which this operation applies
 * @param iterator <IN> enumeration iterator
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSFinishEnumeration(
	struct ZS_thread_state *zs_thread_state,
	struct ZS_iterator     *iterator
	)
{
	ZS_status_t ret = ZS_SUCCESS;
	uint64_t i;
	__zs_cont_iterator_t *itr = (__zs_cont_iterator_t *) iterator;
	ZS_cguid_t cguid = itr->cguid;

    /*
	* Handle hashed container case
	*/
	cm_lock(cguid, READ);
	if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags)) {
		ret = (ZSFinishEnumeration(zs_thread_state, itr->iterator));
	} else {
		ret = (_ZSGetRangeFinish(zs_thread_state, (struct ZS_cursor *) itr->iterator));
	}
	cm_unlock(cguid);

	btree_free(itr);

	return ret;
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
 *  @param zs_thread_state <IN> The SDF context for which this operation applies.
 *  @param cguid <IN> Identity of an open container with appropriate permissions.
 *  @param key <IN> Key of object to be operated on.
 *  @param keylen <IN> Length of key in bytes.
 *
 *  @return ZS_SUCCESS: operation completed successfully.
 *          ZS_BAD_CONTEXT: the provided context is invalid.
 *          ZS_CONTAINER_UNKNOWN: the container ID is invalid.
 *          ZS_OBJECT_UNKNOWN: the object does not exist.
 *          ZS_IN_TRANS: this operation cannot be done inside a transaction.
 *          ZS_FAILURE: operation failed.
 */
ZS_status_t _ZSFlushObject(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid,
	char                     *key,
	uint32_t                  keylen
	)
{
    ZS_status_t      ret = ZS_FAILURE;
    btree_status_t    btree_ret = BTREE_FAILURE;
    struct btree     *bt;
	int				  index;


	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}
    my_thd_state = zs_thread_state;

    /*
     * Handle hashed container case
     */
    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags)) {
        ret = ZSFlushObject(my_thd_state, cguid, key, keylen);
        //fprintf(stderr, "Flushing object to a HASH container\n");
        cm_unlock(cguid);
        return ret;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return (ret);
    }
    if (!bt) 
		assert(0);

    if (keylen > bt->max_key_size) {
        msg("btree_insert/update keylen(%d) more than max_key_size(%d)\n",
                 keylen, bt->max_key_size);
		bt_rel_entry(index, false);
        return (ZS_KEY_TOO_LONG);
    }

    trxenter( cguid);
    btree_ret = btree_flush(bt, key, keylen);
    trxleave( cguid);

	ret = BtreeErr_to_ZSErr(btree_ret);
	if (ret != ZS_SUCCESS)
	{
		msg("btree_flush failed for key '%s' with ret=%s!\n", key, ZSStrError(ret));
	}
	bt_rel_entry(index, false);
    return(ret);
}

/**
 * @brief Flush container
 *
 * @param zs_thread_state <IN> The ZS context for which this operation applies.
 * @param cguid  <IN> container global identifier
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSFlushContainer(
	struct ZS_thread_state  *zs_thread_state,
	ZS_cguid_t               cguid
	)
{
	ZS_status_t	ret;
    my_thd_state = zs_thread_state;;

	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}
    return(ZSFlushContainer(zs_thread_state, cguid));
}

/**
 * @brief Flush the cache
 *
 * @param zs_thread_state <IN> The SDF context for which this operation applies.
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSFlushCache(
	struct ZS_thread_state  *zs_thread_state
	)
{
    my_thd_state = zs_thread_state;;

    return(ZSFlushCache(zs_thread_state));
}

/**
 * @brief Get ZS statistics
 *
 * @param zs_thread_state <IN> The SDF context for which this operation applies
 * @param stats <OUT> pointer to statistics return structure
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSGetStats(
	struct ZS_thread_state *zs_thread_state,
	ZS_stats_t             *stats
	)
{

    my_thd_state = zs_thread_state;;

    return(ZSGetStats(zs_thread_state, stats));
}

/**
 * @brief Get per container statistics
 *
 * @param zs_thread_state <IN> The SDF context for which this operation applies
 * @param cguid  <IN> container global identifier
 * @param stats <OUT> pointer to statistics return structure
 * @return ZS_SUCCESS on success
 */
ZS_status_t _ZSGetContainerStats(
	struct ZS_thread_state	*zs_thread_state,
	ZS_cguid_t		 cguid,
	ZS_stats_t     	*stats
	)
{
    my_thd_state = zs_thread_state;;
	ZS_status_t	ret;
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    return(ZSGetContainerStats(zs_thread_state, cguid, stats));
}

/**
 * @brief Get error string for given error code
 *
 * @param errno ZS error number
 * @return  error string
 */
char *_ZSStrError(ZS_status_t zs_errno)
{
    return(ZSStrError(zs_errno));
}

/*
 * ZSTransactionStart
 */
ZS_status_t 
_ZSTransactionStart(struct ZS_thread_state *zs_thread_state)
{

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	__fdf_txn_mode_state = (__fdf_txn_mode_state == FDF_TXN_NONE_MODE)? __fdf_txn_mode_state_global: __fdf_txn_mode_state;

	if (__fdf_txn_mode_state == FDF_TXN_BTREE_MODE) {
		return (ZSTransactionService(zs_thread_state, 0, 0));
	} else {
		return ZSTransactionStart(zs_thread_state);
	}
}


/*
 * ZSTransactionCommit
 */
ZS_status_t 
_ZSTransactionCommit(struct ZS_thread_state *zs_thread_state)
{

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	__fdf_txn_mode_state = (__fdf_txn_mode_state == FDF_TXN_NONE_MODE)? __fdf_txn_mode_state_global: __fdf_txn_mode_state;
	if (__fdf_txn_mode_state == FDF_TXN_BTREE_MODE) {
		return (ZSTransactionService( zs_thread_state, 1, 0));
	} else {
		return  ZSTransactionCommit(zs_thread_state);
	}
}

/*
 * ZSTransactionRollback
 */
ZS_status_t 
_ZSTransactionRollback(struct ZS_thread_state *zs_thread_state)
{

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	__fdf_txn_mode_state = (__fdf_txn_mode_state == FDF_TXN_NONE_MODE)? __fdf_txn_mode_state_global: __fdf_txn_mode_state;
	if (__fdf_txn_mode_state == FDF_TXN_BTREE_MODE) {
		return (ZS_UNSUPPORTED_REQUEST);
	} else {
		return ZSTransactionRollback(zs_thread_state);
	}
}

/*
 * ZSTransactionQuit
 */
ZS_status_t 
_ZSTransactionQuit(struct ZS_thread_state *zs_thread_state)
{

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	__fdf_txn_mode_state = (__fdf_txn_mode_state == FDF_TXN_NONE_MODE)? __fdf_txn_mode_state_global: __fdf_txn_mode_state;
	if (__fdf_txn_mode_state == FDF_TXN_BTREE_MODE) {
		return (ZS_UNSUPPORTED_REQUEST);
	} else {
		return ZSTransactionQuit(zs_thread_state);
	}
}

	

/*
 * ZSTransactionID
 */
uint64_t
_ZSTransactionID(struct ZS_thread_state *zs_thread_state)
{

    return (ZSTransactionID( zs_thread_state));
}

/*
 * ZSTransactionService
 */
ZS_status_t
_ZSTransactionService(struct ZS_thread_state *zs_thread_state, int cmd, void *arg)
{

    return (ZSTransactionService( zs_thread_state, cmd, arg));
}

/*
 * ZSTransactionGetMode
 */
ZS_status_t
_ZSTransactionGetMode(struct ZS_thread_state *zs_thread_state, int *mode)
{
	__fdf_txn_mode_state = (__fdf_txn_mode_state == FDF_TXN_NONE_MODE)? __fdf_txn_mode_state_global: __fdf_txn_mode_state;
	*mode = __fdf_txn_mode_state;
	return ZS_SUCCESS;
}

/*
 * ZSTransactionSetMode
 */
ZS_status_t
_ZSTransactionSetMode(struct ZS_thread_state *zs_thread_state, int mode)
{
	if (mode == FDF_TXN_NONE_MODE ||
	    (mode != FDF_TXN_BTREE_MODE &&
	    mode != FDF_TXN_CORE_MODE)) {
		return ZS_INVALID_PARAMETER;
	}

	__fdf_txn_mode_state = mode;	
	return ZS_SUCCESS;
}

/*
 * ZSGetVersion
 */
ZS_status_t 
_ZSGetVersion(char **str)
{
    return ZSGetVersion(str);
}

/*
 * ZSGetRange
 */
ZS_status_t 
_ZSGetRange(struct ZS_thread_state *zs_thread_state,
             ZS_cguid_t              cguid, 
             ZS_indexid_t            indexid,
             struct ZS_cursor      **cursor,
             ZS_range_meta_t        *rmeta)
{
	ZS_status_t      ret = ZS_SUCCESS;
	btree_status_t    status;
	struct btree     *bt;
	int				  index;

	my_thd_state = zs_thread_state;

	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSGetRange is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return (ret);
	}

	status = btree_raw_range_query_start(bt->partitions[0],
	                                (btree_indexid_t) indexid, 
	                                (btree_range_cursor_t **)cursor,
	                                (btree_range_meta_t *)rmeta);
        if (status == BTREE_SUCCESS) {
            __sync_add_and_fetch(&(((btree_range_cursor_t *)*cursor)->btree->stats.stat[BTSTAT_RANGE_CNT]), 1);
        }
	ret = BtreeErr_to_ZSErr(status);
	bt_rel_entry(index, false);
	return(ret);
}

ZS_status_t
_ZSGetNextRange(struct ZS_thread_state *zs_thread_state,
                 struct ZS_cursor       *cursor,
                 int                      n_in,
                 int                     *n_out,
                 ZS_range_data_t        *values)
{
	ZS_status_t      ret = ZS_SUCCESS;
	btree_status_t    status;
	int				  index;
	btree_t			  *bt;


	my_thd_state = zs_thread_state;;

	/* TODO: Need to add additional callback to start_range_query
	 * cursor, which generically maps status of each values to
	 * whatever the user of btree needs (in this case zs_wrapper).
	 * At present, the status numbers are matched, but thats not
	 * good for maintanability */
	const ZS_cguid_t cguid = ((btree_range_cursor_t *)cursor)->cguid;
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSGetNextRange is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
	if (bt == NULL) {
		return (ret);
	}

	/*
	 * If either container was deleted and cguid is resued or
	 * ZS was restarted. So, information maintained is not valid.
	 * Then application has to do RangeStart() again.
	 */
	assert(bt->n_partitions == 1);
	if (bt->partitions[0] != ((btree_range_cursor_t *)cursor)->btree) {
		ret = ZS_STATE_CHANGED;
		goto out;
	}

	__sync_add_and_fetch(&(((btree_range_cursor_t *)cursor)->btree->stats.stat[BTSTAT_RANGE_NEXT_CNT]), 1);
	trxenter( cguid);
	status = btree_range_get_next((btree_range_cursor_t *)cursor,
	                              n_in,
	                              n_out,
	                              (btree_range_data_t *)values
	                              );
	trxleave( cguid);
	if (status == BTREE_SUCCESS) {
		__sync_add_and_fetch(&(((btree_range_cursor_t *)cursor)->btree->stats.stat[BTSTAT_NUM_RANGE_NEXT_OBJS]), *n_out);
	} else if (status == BTREE_QUERY_DONE) {
		__sync_add_and_fetch(&(((btree_range_cursor_t *)cursor)->btree->stats.stat[BTSTAT_NUM_RANGE_NEXT_OBJS]), *n_out);
   	}

	ret = BtreeErr_to_ZSErr(status);
out:
	bt_rel_entry(index, false);
	return(ret);
}

ZS_status_t 
_ZSGetRangeFinish(struct ZS_thread_state *zs_thread_state, 
                   struct ZS_cursor *cursor)
{
	ZS_status_t      ret = ZS_SUCCESS;
	btree_status_t    status;

	const ZS_cguid_t cguid = ((btree_range_cursor_t *)cursor)->cguid;
    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSGetRangeFinish is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	my_thd_state = zs_thread_state;
        __sync_add_and_fetch(&(((btree_range_cursor_t *)cursor)->btree->stats.stat[BTSTAT_RANGE_FINISH_CNT]), 1);
	status = btree_range_query_end((btree_range_cursor_t *)cursor);
	ret = BtreeErr_to_ZSErr(status);
	return(ret);
}


/*****************************************************************
 *
 *  B-Tree Callback Functions
 *
 *****************************************************************/

static void
read_node_cb(btree_status_t *ret, void *data, void *pnode, uint64_t lnodeid, int rawobj)
{
    read_node_t            *prn = (read_node_t *) data;
    uint64_t                datalen;
    ZS_status_t            status = ZS_FAILURE;

    N_read_node++;

    assert(ret);
    assert(my_thd_state);
    assert(data);
    assert(pnode);

    if (rawobj) {
        datalen = overflow_node_sz;
        status = ZSReadRawObject(my_thd_state, prn->cguid, lnodeid, (char **) &pnode, &datalen, true);
    } else {
        datalen = prn->nodesize;
        status = ZSReadObject2(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &pnode, &datalen);
    }

    trxtrackread( prn->cguid, lnodeid);

    if (status == ZS_SUCCESS) {
        assert((rawobj && (datalen == overflow_node_sz)) || 
               (!rawobj && (datalen == prn->nodesize)));
        *ret = BTREE_SUCCESS;
        // xxxzzz SEGFAULT
        // fprintf(stderr, "SEGFAULT read_node_cb: %p [tid=%d]\n", n, tid);
    } else {
        fprintf(stderr, "ZZZZZZZZ   read_node_cb %lu - %lu failed with ret=%s   ZZZZZZZZZ\n", lnodeid, datalen, ZSStrError(status));
        //*ret = BTREE_FAILURE;
        *ret = ZSErr_to_BtreeErr(status);
    }
}

static void write_node_cb(struct ZS_thread_state *thd_state, btree_status_t *ret_out,
		void *cb_data, uint64_t** lnodeid, char **data, uint64_t datalen,
		int count, uint32_t rawobj)
{
    ZS_status_t    ret;
    read_node_t     *prn = (read_node_t *) cb_data;

    N_write_node++;

	if (rawobj) {
		assert(count == 1);
		ret = ZSWriteRawObject(thd_state, prn->cguid, (char *) *lnodeid, sizeof(uint64_t), (char *)*data, datalen, 0);
	} else {
		ret = ZSWriteObjects(thd_state, prn->cguid, (char **) lnodeid, sizeof(uint64_t), data, datalen, count, 0);
	}
    trxtrackwrite( prn->cguid, lnodeid);
    assert((rawobj && (datalen == overflow_node_sz)) ||
		   (!rawobj && (prn->nodesize == datalen)));
    *ret_out = ZSErr_to_BtreeErr(ret);
}

static void flush_node_cb(btree_status_t *ret_out, void *cb_data, uint64_t lnodeid)
{
    ZS_status_t            ret;
    read_node_t            *prn = (read_node_t *) cb_data;

    N_flush_node++;

    assert(ret_out);
    assert(cb_data);
    assert(my_thd_state);

    ret = ZSFlushObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t));
	*ret_out = ZSErr_to_BtreeErr(ret);
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
	ZS_status_t		status = ZS_FAILURE;
	read_node_t		*prn = (read_node_t *) data;
	uint64_t		datalen;

	N_create_node++;
	status = ZSWriteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), Create_Data, prn->nodesize, ZS_WRITE_MUST_NOT_EXIST  /* flags */);
	*ret = ZSErr_to_BtreeErr(status);
	trxtrackwrite( prn->cguid, lnodeid);
	if (status == ZS_SUCCESS) {
		status = ZSReadObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t), (char **) &n, &datalen);
		if (status == ZS_SUCCESS) {
			assert(datalen == prn->nodesize);
			*ret = BTREE_SUCCESS;
			return(n);
		}
		//*ret = BTREE_FAILURE;
		*ret = ZSErr_to_BtreeErr(status);
		fprintf(stderr, "ZZZZZZZZ   create_node_cb failed with ret=%d   ZZZZZZZZZ\n", *ret);
	}
	return(NULL);
}

static btree_status_t delete_node_cb(void *data, uint64_t lnodeid, int rawobj)
{
	read_node_t	*prn	= (read_node_t *) data;
	ZS_status_t status;

	N_delete_node++;
	if (rawobj) {
		status = ZSDeleteRawObject(my_thd_state, prn->cguid, lnodeid, sizeof(uint64_t), 0);
	} else {
		status = ZSDeleteObject(my_thd_state, prn->cguid, (char *) &lnodeid, sizeof(uint64_t));
	}
	trxtrackwrite( prn->cguid, lnodeid);
	if (status == ZS_SUCCESS)
		return (BTREE_SUCCESS);
	//return (BTREE_SUCCESS);		// return success until btree logic is fixed
	return (ZSErr_to_BtreeErr(status));
}

static uint64_t seqno_alloc_cb(void)
{
	return (seqnoalloc(my_thd_state));
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

/*
 * Default comparator to check if an update is required.
 * It returns 1 always and that means all update are required.
 */
int
mput_default_cmp_cb(void *data, char *key, uint32_t keylen,
		    char *old_data, uint64_t old_datalen,
		    char *new_data, uint64_t new_datalen)
{
	return 1;
//	return 0;
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

    fprintf(stderr, "%s: %s", prefix, stmp);
    if (quit) {
        assert(0);
        exit(1);
    }
}

ZS_ext_stat_t btree_to_zs_stats_map[] = {
    /*{Btree stat, corresponding zs stat, zs stat type, NOP}*/
    /*{Btree stat, corresponding zs stat, zs stat type, NOP}*/
    {BTSTAT_L1ENTRIES,        ZS_BTREE_L1_ENTRIES,         ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_L1OBJECTS,        ZS_BTREE_L1_OBJECTS,         ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_LEAF_L1HITS,      ZS_BTREE_LEAF_L1_HITS,       ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NONLEAF_L1HITS,   ZS_BTREE_NONLEAF_L1_HITS,    ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_OVERFLOW_L1HITS,  ZS_BTREE_OVERFLOW_L1_HITS,   ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_LEAF_L1MISSES,    ZS_BTREE_LEAF_L1_MISSES,     ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NONLEAF_L1MISSES, ZS_BTREE_NONLEAF_L1_MISSES,  ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_OVERFLOW_L1MISSES,ZS_BTREE_OVERFLOW_L1_MISSES, ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_BACKUP_L1MISSES,  ZS_BTREE_BACKUP_L1_MISSES,   ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_BACKUP_L1HITS,    ZS_BTREE_BACKUP_L1_HITS,     ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_LEAF_L1WRITES,    ZS_BTREE_LEAF_L1_WRITES,     ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NONLEAF_L1WRITES, ZS_BTREE_NONLEAF_L1_WRITES,  ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_OVERFLOW_L1WRITES,ZS_BTREE_OVERFLOW_L1_WRITES, ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_LEAF_NODES,       ZS_BTREE_LEAF_NODES,      ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NONLEAF_NODES,    ZS_BTREE_NONLEAF_NODES,   ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_OVERFLOW_NODES,   ZS_BTREE_OVERFLOW_NODES,  ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_LEAF_BYTES,       ZS_BTREE_LEAF_BYTES,      ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NONLEAF_BYTES,    ZS_BTREE_NONLEAF_BYTES,   ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_OVERFLOW_BYTES,   ZS_BTREE_OVERFLOW_BYTES,  ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NUM_OBJS,         ZS_BTREE_NUM_OBJS,        ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_TOTAL_BYTES,      ZS_BTREE_TOTAL_BYTES,     ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_EVICT_BYTES,      ZS_BTREE_EVICT_BYTES,     ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_SPLITS,           ZS_BTREE_SPLITS,          ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_LMERGES,          ZS_BTREE_LMERGES,         ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_RMERGES,          ZS_BTREE_RMERGES,         ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_LSHIFTS,          ZS_BTREE_LSHIFTS,         ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_RSHIFTS,          ZS_BTREE_RSHIFTS,         ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_EX_TREE_LOCKS,    ZS_BTREE_EX_TREE_LOCKS,   ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_NON_EX_TREE_LOCKS,ZS_BTREE_NON_EX_TREE_LOCKS,ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_GET_CNT,          ZS_BTREE_GET,            ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_GET_PATH,         ZS_BTREE_GET_PATH_LEN,    ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_CREATE_CNT,       ZS_BTREE_CREATE,            ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_CREATE_PATH,      ZS_BTREE_CREATE_PATH_LEN, ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_SET_CNT,          ZS_BTREE_SET,            ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_SET_PATH,         ZS_BTREE_SET_PATH_LEN,    ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_UPDATE_CNT,       ZS_BTREE_UPDATE,            ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_UPDATE_PATH,      ZS_BTREE_UPDATE_PATH_LEN, ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_DELETE_CNT,       ZS_ACCESS_TYPES_DELETE,            ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_DELETE_PATH,      ZS_BTREE_DELETE_PATH_LEN, ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_FLUSH_CNT,        ZS_BTREE_FLUSH_CNT,       ZS_STATS_TYPE_BTREE, 0},

    {BTSTAT_DELETE_OPT_CNT,   ZS_BTREE_DELETE_OPT_COUNT,ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_MPUT_IO_SAVED,    ZS_BTREE_MPUT_IO_SAVED,   ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_PUT_RESTART_CNT,  ZS_BTREE_PUT_RESTART_CNT, ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_SPCOPT_BYTES_SAVED, ZS_BTREE_SPCOPT_BYTES_SAVED,   ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_MPUT_CNT,         ZS_ACCESS_TYPES_MPUT,            ZS_STATS_TYPE_APP_REQ, 0},

    {BTSTAT_MSET_CNT,         ZS_ACCESS_TYPES_MSET,            ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_RANGE_CNT,        ZS_ACCESS_TYPES_RANGE,            ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_RANGE_NEXT_CNT,   ZS_ACCESS_TYPES_RANGE_NEXT,            ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_RANGE_FINISH_CNT, ZS_ACCESS_TYPES_RANGE_FINISH,            ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_RANGE_UPDATE_CNT, ZS_ACCESS_TYPES_RANGE_UPDATE,            ZS_STATS_TYPE_APP_REQ, 0},

    {BTSTAT_CREATE_SNAPSHOT_CNT, ZS_ACCESS_TYPES_CREATE_SNAPSHOT,         ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_DELETE_SNAPSHOT_CNT, ZS_ACCESS_TYPES_DELETE_SNAPSHOT,         ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_LIST_SNAPSHOT_CNT,   ZS_ACCESS_TYPES_LIST_SNAPSHOT,         ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_TRX_START_CNT,       ZS_ACCESS_TYPES_TRX_START,         ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_TRX_COMMIT_CNT,      ZS_ACCESS_TYPES_TRX_START,         ZS_STATS_TYPE_APP_REQ, 0},

    {BTSTAT_NUM_MPUT_OBJS,       ZS_BTREE_NUM_MPUT_OBJS,         ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_NUM_RANGE_NEXT_OBJS,      ZS_BTREE_NUM_RANGE_NEXT_OBJS,         ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_NUM_RANGE_UPDATE_OBJS,      ZS_BTREE_NUM_RANGE_UPDATE_OBJS,     ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_NUM_SNAP_OBJS,    ZS_BTREE_NUM_SNAP_OBJS,   ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_SNAP_DATA_SIZE,    ZS_BTREE_SNAP_DATA_SIZE,   ZS_STATS_TYPE_BTREE,0},

    {BTSTAT_NUM_SNAPS,    ZS_BTREE_NUM_SNAPS,   ZS_STATS_TYPE_BTREE,0},
    {BTSTAT_BULK_INSERT_CNT,  ZS_BTREE_NUM_BULK_INSERT_CNT, ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_BULK_INSERT_FULL_NODES_CNT,  ZS_BTREE_NUM_BULK_INSERT_FULL_NODES_CNT, ZS_STATS_TYPE_BTREE, 0},
    {BTSTAT_READ_CNT, ZS_ACCESS_TYPES_READ,         ZS_STATS_TYPE_APP_REQ, 0},
    {BTSTAT_WRITE_CNT, ZS_ACCESS_TYPES_WRITE,         ZS_STATS_TYPE_APP_REQ, 0},
};

ZS_status_t set_flash_stats_buffer(uint64_t *alloc_blks, uint64_t *free_segs, uint64_t *consumed_blks, uint64_t *hash_size, uint64_t *hash_alloc, uint64_t blk_size, uint64_t seg_size) {
    flash_blks_allocated = alloc_blks;
    flash_segs_free      = free_segs;
    flash_blks_consumed  = consumed_blks;
    flash_hash_size      = hash_size;
    flash_hash_alloc     = hash_alloc;
    flash_block_size     = blk_size;
    flash_segment_size   = seg_size;

    return ZS_SUCCESS;
}

ZS_status_t set_zs_function_ptrs( void *log_func) {
    zs_log_func_ptr = (zs_log_func) log_func;
    return ZS_SUCCESS;
}

ZS_status_t btree_check_license_ptr(int lic_state) {
	btree_ld_valid = lic_state;
	return ZS_SUCCESS;
}

ZS_status_t btree_get_all_stats(ZS_cguid_t cguid, 
                                ZS_ext_stat_t **estat, uint32_t *n_stats) {
    int i, j;
    struct btree     *bt;
    btree_stats_t     bt_stats;
    uint64_t         *stats;
    ZS_status_t	  ret;
    int				  index;	

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return ret;
    }
    stats = malloc( N_BTSTATS * sizeof(uint64_t));
    if( stats == NULL ) {
		bt_rel_entry(index, false);
        return ZS_FAILURE;
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

    ZS_ext_stat_t *pstat_arr;
    pstat_arr = malloc(N_BTSTATS * sizeof(ZS_ext_stat_t));
    if( pstat_arr == NULL ) {
        free(stats);
		bt_rel_entry(index, false);
        return ZS_FAILURE;
    }
    for (i=0; i<N_BTSTATS; i++) {
        pstat_arr[i].estat = i;
        pstat_arr[i].fstat = btree_to_zs_stats_map[i].fstat;     
        pstat_arr[i].ftype = btree_to_zs_stats_map[i].ftype;     
        pstat_arr[i].value = stats[i];
    }
    free(stats);
    *estat = pstat_arr;
    *n_stats = N_BTSTATS;
	bt_rel_entry(index, false);
    return ZS_SUCCESS;
}


ZS_status_t btree_get_rawobj_mode(int storm_mode, uint64_t rawobjsz, int ratio) {
	bt_storm_mode = storm_mode;
	overflow_node_sz = rawobjsz;

	if (bt_storm_mode) {
		datasz_in_overflow = overflow_node_sz - sizeof(btree_raw_node_t);
		overflow_node_ratio = ratio;
	}
	return ZS_SUCCESS;
}

#ifdef FLIP_ENABLED
/* NOTE: This is used for debugging purpose and hence is_root is not guaranteed under
 * all conditions be correct, since it does not any lock */
ZS_status_t btree_get_node_info(ZS_cguid_t cguid, char *data, uint64_t datalen, 
                                uint32_t *node_type, bool *is_root, uint64_t *logical_id)
{
	btree_raw_node_t *node;
	btree_raw_t *btree;
	btree_t *pbtree;
	int i;

	*node_type = 0;
	*is_root = false;

	if (data == NULL) {
		return ZS_SUCCESS;
	}

	/* Get index from cguid */
	i = bt_get_ctnr_from_cguid(cguid);
	if (i < 0) {
		return ZS_SUCCESS;
	}

	if ((Container_Map[i].cguid != cguid) ||
	    (Container_Map[i].bt_state != BT_CNTR_OPEN)) {
		return ZS_SUCCESS;
	}

	pbtree = Container_Map[i].btree;
	if (pbtree == NULL) {
		return ZS_SUCCESS;
	}
	btree = pbtree->partitions[0];

	if ((datalen != DEFAULT_NODE_SIZE) &&
	    (bt_storm_mode && (datalen != overflow_node_sz))) {
		/* Not a btree node */
		return ZS_SUCCESS;
	}

	node = (btree_raw_node_t *)data;
	*logical_id = node->logical_id;
	*node_type  = (node->logical_id & META_LOGICAL_ID_MASK) ? 0 : node->flags;
	*is_root    = (btree->rootid == node->logical_id);

	return ZS_SUCCESS;
}
#endif

static void dump_btree_stats(FILE *f, ZS_cguid_t cguid)
{
    struct btree     *bt;
    btree_stats_t     bt_stats;
	int				  index;
	ZS_status_t	  ret;

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return;
    }

    btree_get_stats(bt, &bt_stats);
    btree_dump_stats(f, &bt_stats);
	bt_rel_entry(index, false);
}

ZS_status_t
_ZSMPut(struct ZS_thread_state *zs_ts,
        ZS_cguid_t cguid,
        uint32_t num_objs,
        ZS_obj_t *objs,
	uint32_t flags,
	uint32_t *objs_done)
{
	int i;
	ZS_status_t ret = ZS_SUCCESS;
	btree_status_t btree_ret = BTREE_FAILURE;
	btree_metadata_t meta;
	struct btree *bt = NULL;
	int		index;

	if (__zs_check_mode_on) {
		/*
		 * We are in checker mode so do not allow any new
		 * write or update in objects.
		 */
		return ZS_FAILURE_OPERATION_DISALLOWED;	
	}   

	my_thd_state = zs_ts;;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}
	if (num_objs == 0) {
		return ZS_SUCCESS;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("_ZSMPut is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
	if (bt == NULL) {
		return ret;
	}
	if (storage_space_exhausted( "ZSMPut")) {
		bt_rel_entry( index, true);
		return (ZS_OUT_OF_STORAGE_SPACE);
	}

	for (i = 0; i < num_objs; i++) {
#if 0
		if (objs[i].flags != 0) {
			bt_rel_entry(index, true);
			return ZS_INVALID_PARAMETER;
		}
#endif

		if (objs[i].key_len > bt->max_key_size) {
			bt_rel_entry(index, true);
			return ZS_INVALID_PARAMETER;
		}

		if (objs[i].data_len > BTREE_MAX_DATA_SIZE_SUPPORTED) {
			msg("btree_insert/update datalen(%"PRIu64") more than "
			    "max supported datalen(%"PRIu64")\n",
			    objs[i].data_len, BTREE_MAX_DATA_SIZE_SUPPORTED);
			bt_rel_entry(index, true);
			return ZS_OBJECT_TOO_BIG;
		}

		assert((i == 0) || (bt->cmp_cb(bt->cmp_cb_data, objs[i-1].key, objs[i-1].key_len, 
		                               objs[i].key, objs[i].key_len) <= 0));
	}

	meta.flags = 0;

	__sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_MPUT_CNT]),1);
	ZSTransactionService( zs_ts, 0, 0);
	btree_ret = btree_mput(bt, (btree_mput_obj_t *)objs,
			num_objs, flags, &meta, objs_done);
	ZSTransactionService( zs_ts, 1, 0);
	assert((ret != BTREE_SUCCESS) || (*objs_done == num_objs));

	if( btree_ret == BTREE_SUCCESS ) {
		__sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_NUM_MPUT_OBJS]),num_objs);
	}
	ret = BtreeErr_to_ZSErr(btree_ret);
	bt_rel_entry(index, true);
	return ret;
}


/*
 * ZSCheckBtree: internal api for testing purpose.
 */
ZS_status_t
zs_check_btree(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid, uint64_t flags, uint64_t *num_objs)
{
	ZS_status_t ret = ZS_SUCCESS;
	my_thd_state = zs_thread_state;;
	struct btree *bt = NULL;
	int index;

	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
	if (bt == NULL) {
		return ret;
	}

	if (btree_check(bt, num_objs) == false) {
		ret = ZS_FAILURE;
	} else {
		ret = ZS_SUCCESS;
	}


	msg("Got %"PRId64" objects in cont id = %d.\n", *num_objs, cguid);

	bt_rel_entry(index, false);
	return ret;
}

ZS_status_t
_ZSCheckBtree(struct ZS_thread_state *zs_thread_state, 
	       ZS_cguid_t cguid, uint64_t flags)
{
	uint64_t num_objs = 0;
	return zs_check_btree(zs_thread_state, cguid, flags, &num_objs);
}

static ZS_status_t 
check_hash_cont(struct ZS_thread_state *thd_state, ZS_cguid_t cguid)
{
		
	ZS_status_t status = ZS_SUCCESS;
	struct ZS_iterator *iterator = NULL;
	char *key = NULL;
	uint32_t keylen = 0;
	uint64_t datalen = 0;
	char *data = NULL;
	uint64_t count = 0;

	status = ZSEnumerateContainerObjects(thd_state, cguid, &iterator);

	while ((status = ZSNextEnumeratedObject(thd_state, iterator, 
				      &key, &keylen, &data, &datalen)) == ZS_SUCCESS) {

		ZSFreeBuffer(key);
		ZSFreeBuffer(data);
		count++;
	}
	msg("Got %"PRId64" objects in cont id = %d.\n", count, cguid);
	
	return ZS_SUCCESS;
}

#define MCD_MAX_NUM_CNTRS UINT16_MAX - 1 - 9

/*
 * _ZSCheck: internal api for testing purpose.
 */
ZS_status_t
_ZSCheck(struct ZS_thread_state *zs_thread_state, uint64_t flags)
{
	ZS_cguid_t cguid = 0;
	ZS_container_props_t props = {0};
	ZS_status_t status = ZS_SUCCESS;
	int i = 0;
	uint64_t *num_objs = NULL;
	char err_msg[1024];
	uint32_t ndelcguids = 0;

    int threads = atoi(_ZSGetProperty("ZSCHECK_THREADS", "1"));
    pthread_t thread_id[threads];
    pthread_attr_t attr;


	if (!__zs_check_mode_on) {
		return ZS_FAILURE_OPERATION_DISALLOWED;	
	}

	if (0 && seqnoread(zs_thread_state) != true) {
		return ZS_FAILURE;
	}

	cguids = btree_malloc(sizeof(ZS_cguid_t) * MCD_MAX_NUM_CNTRS);
	assert(cguids);
	
    /*
     * Check containers
     */
    status = ZSGetContainers(zs_thread_state, cguids, &ncguids);
    if (ZS_SUCCESS != status)
        return status;

    msg("Running Data consistency checker on %d normal containers.\n", ncguids);

	if ((status = ZSGetBtDelContainers(zs_thread_state, &cguids[ncguids], &ndelcguids)) != ZS_SUCCESS) {
		goto out;
	}

    ncguids += ndelcguids;
    msg("Running Data consistency checker on %d deleted containers.\n", ndelcguids);

    msg("Running Data consistency checker on %d total containers.\n", ncguids);

    num_objs = (uint64_t *) btree_malloc(ncguids * sizeof(uint64_t));

    // Init thread sync
    pthread_mutex_init(&cguid_idx_lock, NULL);
    cguid_idx = 0;

    // Call the workers
    pthread_attr_init( &attr );
    pthread_attr_setdetachstate( &attr, PTHREAD_CREATE_JOINABLE );

    for (i = 0; i < threads; i++)
        pthread_create(&thread_id[i], &attr, zscheck_worker, (void*)(long)i);

    for (i = 0; i < threads; i++)
        pthread_join(thread_id[i], NULL);

    // Call space check
    status = ZSCheckSpace(zs_thread_state);

    if (!(flags & ZS_CHECK_FIX_BOJ_CNT)) {
        goto out;
    }
    /*
     * If required, fix the number of objects.
     */
    for (i = 0; i < ncguids; i++) {

        status = ZSGetContainerProps(zs_thread_state, cguids[i], &props);
        if (status != ZS_SUCCESS) {
            goto out;
        }


        /*
         * Fix the number of objects in pstats for btree containers.
         */
        if (IS_ZS_HASH_CONTAINER(props.flags) == false) {
            status = zs_fix_objs_cnt_stats(zs_thread_state, num_objs[i], props.name);
            if (status == ZS_SUCCESS) {
                msg("Fixed object count for cont name %s.\n", props.name);
            } else {
                msg("Failed fixing object count for cont name %s.\n", props.name);
            }
        }

    }

out:
    if (cguids) {
        btree_free(cguids);
    }

    if (num_objs) {
        btree_free(num_objs);
    }
    return status;
}

ZS_status_t
_ZSCheckClose()            
{
    return ZSCheckClose();
}

void
_ZSCheckMsg(ZS_check_entity_t entity, 
            uint64_t id, 
            ZS_check_error_t error, 
            char *msg
            )
{
    return ZSCheckMsg(entity,
                      id,
                      error,
                      msg
                      );
}

void
_ZSCheckSetLevel(int level)            
{
    ZSCheckSetLevel(level);
}

int
_ZSCheckGetLevel()            
{
    return ZSCheckGetLevel();
}

ZS_status_t
_ZSCheckInit(char *logfile)
{
    return ZSCheckInit(logfile);
}

ZS_status_t
_ZSCheckMeta()
{
    return ZSCheckMeta();
}

ZS_status_t
_ZSCheckFlog()
{
    return ZSCheckFlog();
}

ZS_status_t
_ZSCheckPOT()
{
    return ZSCheckPOT();
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

ZS_status_t
_ZSRangeUpdate(struct ZS_thread_state *zs_ts, 
	       ZS_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       ZS_range_update_cb_t callback_func,
	       void * callback_args,	
	       uint32_t *objs_updated)
{
	ZS_status_t ret = ZS_SUCCESS;
	struct btree *bt = NULL;
	btree_rupdate_cb_t cb_func = 
			(btree_rupdate_cb_t) callback_func;
	ZS_range_meta_t rmeta;
	ZS_range_data_t *values = NULL;
	struct ZS_cursor *cursor = NULL;
	btree_rupdate_data_t *tmp_data = NULL;
	ZS_obj_t *objs = NULL;
	uint32_t objs_written = 0;
	uint32_t objs_to_update = 0;
	uint32_t objs_in_range = 0;
	char *new_data = NULL;
	uint64_t new_data_len;
	int n_out = 0;
	int i = 0;
	int	index;

	*objs_updated = 0;

	if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return ret;
	}
	bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
	if (bt == NULL) {
		return ret;
	}

	objs = (ZS_obj_t *) malloc(sizeof(ZS_obj_t) * NUM_IN_CHUNK);
	if (objs == NULL) {
		bt_rel_entry(index);
		return ZS_FAILURE_MEMORY_ALLOC;
	}

	tmp_data = (btree_rupdate_data_t *) malloc(sizeof(*tmp_data) * NUM_IN_CHUNK);
	if (tmp_data == NULL) {
		ret = ZS_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	values = (ZS_range_data_t *)
		   malloc(sizeof(ZS_range_data_t) * NUM_IN_CHUNK);
	if (values == NULL) {
		ret = ZS_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	rmeta.key_start = (char *) malloc(MAX_KEYLEN);
	if (rmeta.key_start == NULL) {
		ret = ZS_FAILURE_MEMORY_ALLOC;
		goto exit;
	}

	memcpy(rmeta.key_start, range_key, range_key_len);
	rmeta.keylen_start = range_key_len;

	rmeta.flags = ZS_RANGE_START_GE;
	rmeta.key_end = NULL;
	rmeta.keylen_end = 0;
	rmeta.cb_data = NULL;
	rmeta.keybuf_size = 0;
	rmeta.databuf_size = 0;

	rmeta.allowed_fn = NULL;

	ret = _ZSGetRange(zs_ts, cguid, ZS_RANGE_PRIMARY_INDEX,
			  &cursor, &rmeta);
	if (ret != ZS_SUCCESS) {
		goto exit;
	}

	/*
	 * Start TXN
	 */
#if RANGE_UPDATE_TXN
	ret = _ZSTransactionStart(zs_ts);
	if (ret != ZS_SUCCESS) {
		assert(0);
		goto exit;	
	}
#endif 

	do {
		ret = _ZSGetNextRange(zs_ts, cursor, NUM_IN_CHUNK, &n_out, values);
		if (ret != ZS_SUCCESS && ret != ZS_QUERY_DONE) {
			/*
			 * Return error.
			 */
			assert(0);
			goto rollback;
		}

		objs_to_update = 0;
		for (i = 0; i < n_out; i++) {

			assert(values[i].status == ZS_SUCCESS);

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
		ret = _ZSMPut(zs_ts, cguid, objs_to_update, objs, 0, &objs_written);

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

		if ((ret != ZS_SUCCESS) || (objs_written != objs_to_update)) {
			assert(0);
			goto rollback;
		}	

	} while (n_out > 0);

	_ZSGetRangeFinish(zs_ts, cursor);
	
	/*
	 *  commit txn
	 */
#if RANGE_UPDATE_TXN
	ret = _ZSTransactionCommit(zs_ts);
	if (ret == ZS_SUCCESS) {
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
	ZSTransactionRollback(zs_ts);
	assert(ret != ZS_SUCCESS);
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
	bt_rel_entry(index, true);

	return ret;
}

#else 
ZS_status_t
_ZSRangeUpdate(struct ZS_thread_state *zs_ts, 
	       ZS_cguid_t cguid,
	       char *range_key,
	       uint32_t range_key_len,
	       ZS_range_update_cb_t callback_func,
	       void * callback_args,	
	       ZS_range_cmp_cb_t range_cmp_cb,
	       void *range_cmp_cb_args,
	       uint32_t *objs_updated)
{
	btree_rupdate_marker_t *markerp = NULL;
	ZS_status_t ret = ZS_SUCCESS;
	btree_status_t btree_ret = BTREE_FAILURE;
	btree_rupdate_cb_t cb_func = 
			(btree_rupdate_cb_t) callback_func;
	btree_metadata_t meta;
	struct btree *bt = NULL;
	uint32_t objs_done = 0;
	int index = -1;
	ZS_status_t error = ZS_SUCCESS;
	btree_range_cmp_cb_t bt_range_cmp_cb = range_cmp_cb;

	(*objs_updated) = 0;

	my_thd_state = zs_ts;;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
    }

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSRangeUpdate is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &error, true);
	if (bt == NULL) {
		return ZS_FAILURE;
	}
	if (storage_space_exhausted( "ZSRangeUpdate")) {
		bt_rel_entry( index, true);
		return (ZS_OUT_OF_STORAGE_SPACE);
	}
	if (Container_Map[index].read_only == true) {
		ret = ZS_FAILURE;
		goto out;
	}

	markerp = btree_alloc_rupdate_marker(bt);
	if (markerp == NULL) {
		ret = ZS_OUT_OF_MEM;
		goto out;
	}

	meta.flags = 0;
	meta.seqno = seqnoalloc(zs_ts);
	if (meta.seqno == -1) {
		ret = ZS_FAILURE;
		goto out;
	}
        __sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_RANGE_UPDATE_CNT]),1);
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
				 * Marker will not be updated in btree_update call, we have to set
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

	if( btree_ret == BTREE_SUCCESS ) {
		__sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_NUM_RANGE_UPDATE_OBJS]),
																				 *objs_updated);
	} else {
		//Unset/invalidate the marker
		markerp->set = false;
	}

	ret = BtreeErr_to_ZSErr(btree_ret);	

out:
	if (markerp) {
		btree_free_rupdate_marker(bt, markerp);
	}

	bt_rel_entry(index, true);
	return ret;
}
#endif 

/*
 * Create a snapshot for a container.  
 * 
 * Returns: ZS_SUCCESS if successful
 *          ZS_TOO_MANY_SNAPSHOTS if snapshot limit is reached
 */
ZS_status_t
_ZSCreateContainerSnapshot(struct ZS_thread_state *zs_thread_state, //  client thread ZS context
                           ZS_cguid_t cguid,           //  container
                           uint64_t *snap_seq)         //  returns sequence number of snapshot
{
	my_thd_state = zs_thread_state;
	ZS_status_t    status = ZS_FAILURE;
	int				index, i;
	struct btree	*bt;
	btree_status_t  btree_ret = BTREE_FAILURE;

	assert(snap_seq);

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSCreateContainerSnapshot is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &status, false);
	if (bt == NULL) {
		return status;
	}
	if (storage_space_exhausted( "ZSCreateContainerSnapshot")) {
		bt_rel_entry( index, false);
		return (ZS_OUT_OF_STORAGE_SPACE);
	}

	pthread_rwlock_wrlock(&(bt->snapop_rwlock));
	pthread_mutex_lock(&(Container_Map[index].bt_snap_mutex));
	Container_Map[index].snap_initiated = 1;

	while (Container_Map[index].bt_wr_count) {
		fprintf(stderr, "Snapshot thread waiting for writers to finish: count=%d\n", Container_Map[index].bt_wr_count);
		pthread_cond_wait(&(Container_Map[index].bt_snap_cv),
								&(Container_Map[index].bt_snap_mutex));
	}	
	pthread_mutex_unlock(&(Container_Map[index].bt_snap_mutex));

	ZSFlushContainer(zs_thread_state, cguid);

	fprintf(stderr, "Snapshot initiated\n");
	assert(bt->n_partitions == 1);
	for (i = 0; i < bt->n_partitions; i++) {
		*snap_seq = seqnoalloc(zs_thread_state);
		btree_ret = btree_snap_create_meta(bt->partitions[i], *snap_seq);
		if (btree_ret != BTREE_SUCCESS) {
			break;
		}
		deref_l1cache(bt->partitions[i]);
	}

	pthread_mutex_lock(&(Container_Map[index].bt_snap_mutex));
	Container_Map[index].snap_initiated = 0;
	fprintf(stderr, "Waking writer threads\n");
	pthread_cond_broadcast(&(Container_Map[index].bt_snap_wr_cv));
	pthread_mutex_unlock(&(Container_Map[index].bt_snap_mutex));
	fprintf(stderr, "Snapshot Done\n");
	pthread_rwlock_unlock(&(bt->snapop_rwlock));

	bt_rel_entry(index, false);

	assert(!dbg_referenced);
	switch(btree_ret) {
		case BTREE_SUCCESS:
			status = ZS_SUCCESS;
			break;
		case BTREE_TOO_MANY_SNAPSHOTS:
			status = ZS_TOO_MANY_SNAPSHOTS;
			break;
		case BTREE_FAILURE:
		default:
			status = ZS_FAILURE;
			break;
	}
	return(status);
}

/*
 * Delete a snapshot.
 * 
 * Returns: ZS_SUCCESS if successful
 *          ZS_SNAPSHOT_NOT_FOUND if no snapshot for snap_seq is found
 */
ZS_status_t
_ZSDeleteContainerSnapshot(struct ZS_thread_state *zs_thread_state, //  client thread ZS context
                           ZS_cguid_t cguid,           //  container
                           uint64_t snap_seq)          //  sequence number of snapshot to delete
{
	my_thd_state = zs_thread_state;
	btree_status_t	btree_ret = BTREE_FAILURE;
	ZS_status_t    status = ZS_FAILURE;
	int				index, i;
	struct btree	*bt;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        cm_unlock(cguid);
        msg("ZSDeleteContainerSnapshot is not supported on a HASH container\n");
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &status, false);
	if (bt == NULL) {
		return status;
	}

	assert(bt->n_partitions == 1);
	for (i = 0; i < bt->n_partitions; i++) {
		btree_ret = btree_snap_delete_meta(bt->partitions[i], snap_seq);
		if (btree_ret != BTREE_SUCCESS) {
			break;
		}
		deref_l1cache(bt->partitions[i]);
	}

	if (btree_ret != BTREE_SUCCESS) {
		ZSFlushContainer(zs_thread_state, cguid); 
	}

	bt_rel_entry(index, false);

	assert(!dbg_referenced);
	switch(btree_ret) {
		case BTREE_SUCCESS:
			status = ZS_SUCCESS;
			_ZSScavengeContainer(my_global_zs_state, cguid);
			break;
		case BTREE_FAILURE:
		default:
			status = ZS_SNAPSHOT_NOT_FOUND;
			break;
	}
	return status;
}

/*
 * Get a list of all current snapshots.
 * Array returned in snap_seqs is allocated by ZS and
 * must be freed by application.
 * 
 * Returns: ZS_SUCCESS if successful
 *          ZS_xxxzzz if snap_seqs cannot be allocated
 */
ZS_status_t
_ZSGetContainerSnapshots(struct ZS_thread_state *ts, //  client thread ZS context
                         ZS_cguid_t cguid,           //  container
                         uint32_t *n_snapshots,       //  returns number of snapshots
                         ZS_container_snapshots_t **snap_seqs)        //  returns array of snapshot sequence numbers
{
	my_thd_state = ts;
	btree_status_t	btree_ret = BTREE_FAILURE;
	ZS_status_t    status = ZS_FAILURE;
	int				index, i;
	struct btree	*bt;

	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        msg("ZSGetContainerSnapshots is not supported on a hash container\n");
        cm_unlock(cguid);
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);
	bt = bt_get_btree_from_cguid(cguid, &index, &status, false);
	if (bt == NULL) {
		return status;
	}

	assert(bt->n_partitions == 1);
	for (i = 0; i < bt->n_partitions; i++) {
		btree_ret = btree_snap_get_meta_list(bt->partitions[i], n_snapshots, snap_seqs);
		if (btree_ret != BTREE_SUCCESS) {
			break;
		}
		deref_l1cache(bt->partitions[i]);
	}
	bt_rel_entry(index, false);

	switch(btree_ret) {
		case BTREE_SUCCESS:
			status = ZS_SUCCESS;
			break;
		case BTREE_FAILURE:
			status = ZS_FAILURE_STORAGE_WRITE;
			break;
		case BTREE_KEY_NOT_FOUND:
			status = ZS_OBJECT_UNKNOWN;
			break;
		default:
			status = ZS_FAILURE;
			break;
	}
	return(status);
}

ZS_status_t
_ZSIoctl(struct ZS_thread_state *zs_ts, 
         ZS_cguid_t cguid, uint32_t ioctl_type, void *data)
{
	struct btree *bt = NULL;
	ZS_status_t ret = ZS_SUCCESS;
	btree_status_t btree_ret = ZS_SUCCESS;
	int index = -1;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

    my_thd_state = zs_ts;

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        ret = ZSIoctl(zs_ts, cguid, ioctl_type, data);
        cm_unlock(cguid);
        return ret;
    }
    cm_unlock(cguid);

	bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
	if (bt == NULL) {
		bt_rel_entry(index, false);
		return ret;
	}

	btree_ret = btree_ioctl(bt, ioctl_type, data);
	switch(btree_ret) {
	case BTREE_SUCCESS:
		ret = ZS_SUCCESS;
		break;
	default:
		ret = ZS_FAILURE;
		break;
	}

	bt_rel_entry(index, false);
	return (ret);
}

ZS_status_t
_ZSGetLastError(ZS_cguid_t cguid, void **pp_context, uint32_t *p_err_size)
{
	ZS_status_t    status = ZS_FAILURE;
	btree_status_t btree_status;
	btree_t        *btree;
	int            index;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

	cm_lock(cguid, READ);
	if (IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) == true) {
		msg("ZSGetLastError is not supported on a hash container\n");
		cm_unlock(cguid);
		return ZS_FAILURE_INVALID_CONTAINER_TYPE;
	}
	cm_unlock(cguid);

	btree = bt_get_btree_from_cguid(cguid, &index, &status, false);
	if (btree == NULL) {
		bt_rel_entry(index, false);
		return status;
	}

	btree_status = btree_move_lasterror(btree, pp_context, p_err_size);

	bt_rel_entry(index, false);
	return (BtreeErr_to_ZSErr(btree_status));
}

ZS_status_t
_ZSRescueContainer(struct ZS_thread_state *zs_ts, ZS_cguid_t cguid, void *pcontext)
{
	ZS_status_t    status = ZS_FAILURE;
	btree_status_t btree_status;
	btree_t        *btree;
	int            index;

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}

	if ((status = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
		return status;
	}

	my_thd_state = zs_ts;

	cm_lock(cguid, READ);
	if (IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) == true) {
		msg("ZSRescueContainer is not supported on a hash container\n");
		cm_unlock(cguid);
		return ZS_FAILURE_INVALID_CONTAINER_TYPE;
	}
	cm_unlock(cguid);

	btree = bt_get_btree_from_cguid(cguid, &index, &status, true);
	if (btree == NULL) {
		bt_rel_entry(index, false);
		return status;
	}

	btree_status = btree_rescue(btree, pcontext);

	bt_rel_entry(index, true);
	return (BtreeErr_to_ZSErr(btree_status));
}

/*
 * check for flash capacity exceeded
 *
 * Protect btree from hard-error conditions returned from the core
 * when writing objects.  The two resources checked are free slabs and
 * available hash table entries.  Either can change instantaneously, so
 * a slop factor is built into the calculations.  Return TRUE if a core
 * resource is exceeded.
 */
static bool
storage_space_exhausted( const char *op)
{

	if (flash_space_soft_limit_check) {
		uint64_t flash_space_used = ZS_DEFAULT_CONTAINER_SIZE_KB*1024L*2 + *flash_blks_consumed*flash_block_size;
		if (flash_space_used > flash_space_soft_limit) {
			if (__sync_fetch_and_add( &flash_space_limit_failure_count, 1) == 0)
				fprintf( stderr, "%s failed: out of storage (flash space used:%lu flash space limit:%lu)",
				op, flash_space_used, flash_space_soft_limit);
			return (true);
		}
		if (*flash_hash_alloc*105/100 > *flash_hash_size) {
			if (__sync_fetch_and_add( &flash_space_limit_failure_count, 1) == 0)
				fprintf( stderr, "%s failed: nearly out of hash entries (%lu/%lu)", op, *flash_hash_alloc, *flash_hash_size);
			return (true);
		}
	}
	return (false);
}


/*
 * persistent seqno facility
 *
 * This sequence number generator is shared by all btrees.  It offers
 * crashproof persistence, high performance, and thread safety.  The counter
 * has 64 bits of resolution, and is only reset by a flash reformat.
 *
 * Counter is maintained in its own high-durability container (non btree),
 * and is synchronized to flash after SEQNO_SYNC_INTERVAL increments.
 * It is advanced by up to that interval on ZS restart.  ZS warm restart
 * is supported.
 *
 * For maximum performance, seqnos are allocated in blocks of SEQNO_INCR to a
 * thread-local reservoir.  On a 2GHz dual-socket Xeon box, performance peaks
 * at 32 HT (16 cores) with an aggregate allocation frequency of 3.463 GHz.
 * The lifetime supply of 2^64 seqnos at this rate will last 170 years.
 * Reservoir seqnos are wasted when a client thread exits, but the allocation
 * rate remains well within bounds.
 *
 * Value of SEQNO_SYNC_INTERVAL is balanced between flash usage
 * and seqno wastage.  At the peak allocation rate mentioned above,
 * variable 'seqnolimit' is persisted to flash at 29-second intervals.
 * While SEQNO_SYNC_INTERVAL seqnos may go unused on each ZS restart,
 * supply will last at least 30 years given a restart every 5 seconds.
 *
 * To prevent propagation of corruption flash-wide, all errors are fatal.
 */

#include	"utils/rico.h"

#define	SEQNO_SYNC_INTERVAL	100000000000
#define	SEQNO_INCR		10000

typedef uint64_t	seq_t;

/*
 * return next seqno, or -1 on error
 */
static char		SEQNO_CONTAINER[]	= "__SanDisk_seqno_container";
static char		SEQNO_KEY[]		= "__SanDisk_seqno_key";
bool
seqnoread(struct ZS_thread_state *t)
{
	ZS_container_props_t	p;
	ZS_status_t		s;
	ZS_cguid_t		c;
	char *data = NULL;
	uint64_t dlen = 0;


	memset(&p, 0, sizeof p);
	p.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
	p.flags = 1; //hash cont
	s = ZSOpenContainer( t, SEQNO_CONTAINER, &p, 0, &c);
	if (s != ZS_SUCCESS) {
		return false;
	}
	if (ZSReadObject(t, c, SEQNO_KEY, sizeof SEQNO_KEY, &data, &dlen) != ZS_SUCCESS) {
		//assert(0);
		return false;
	}

	ZSFreeBuffer( data);
//	ZSCloseContainer( t, c);

	return true;
}

seq_t
seqnoalloc( struct ZS_thread_state *t)
{
	static bool		initialized;
	static pthread_mutex_t	seqnolock		= PTHREAD_MUTEX_INITIALIZER;
	static seq_t		seqnolimit,
				seqno;
	static __thread seq_t	iseqno,
				iseqnolimit;
	ZS_container_props_t	p;
	ZS_status_t		s;
	ZS_cguid_t		c;
	char			*data;
	uint64_t		dlen;

	unless (iseqno < iseqnolimit) {
		pthread_mutex_lock( &seqnolock);
		unless (initialized) {
			memset( &p, 0, sizeof p);
			p.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
			p.flags = 1; //hash cont
			switch (s = ZSOpenContainer( t, SEQNO_CONTAINER, &p, 0, &c)) {
			case ZS_SUCCESS:
				if ((ZSReadObject( t, c, SEQNO_KEY, sizeof SEQNO_KEY, &data, &dlen) == ZS_SUCCESS)
				and (dlen == sizeof seqnolimit)) {
					seqnolimit = *(seq_t *)data;
					seqno = seqnolimit;
					ZSFreeBuffer( data);
					break;
				}
				ZSCloseContainer( t, c);
				fprintf( stderr, "missing SEQNO_KEY");
				abort( );
			default:
				fprintf( stderr, "cannot open %s (%s)", SEQNO_CONTAINER, ZSStrError( s));
				abort( );
			case ZS_INVALID_PARAMETER:		/* schizo ZS/SDF return */
				p.size_kb = 1 * 1024 * 1024;
				p.fifo_mode = ZS_FALSE;
				p.persistent = ZS_TRUE;
				p.evicting = ZS_FALSE;
				p.writethru = ZS_TRUE;
				s = ZSOpenContainer( t, SEQNO_CONTAINER, &p, ZS_CTNR_CREATE, &c);
				unless (s == ZS_SUCCESS) {
					fprintf( stderr, "cannot create %s (%s)", SEQNO_CONTAINER, ZSStrError( s));
					abort( );
				}
			}
			ZSCloseContainer( t, c);
			initialized = TRUE;
		}
		if (seqnolimit < seqno+SEQNO_INCR) {
			seq_t n = seqno + SEQNO_SYNC_INTERVAL;
			memset( &p, 0, sizeof p);
			p.durability_level = ZS_DURABILITY_HW_CRASH_SAFE;
			unless ((ZSOpenContainer( t, SEQNO_CONTAINER, &p, 0, &c) == ZS_SUCCESS)
			and (ZSWriteObject( t, c, SEQNO_KEY, sizeof SEQNO_KEY, (char *)&n, sizeof n, 0) == ZS_SUCCESS)) {
				fprintf( stderr, "cannot update %s", SEQNO_KEY);
				abort( );
			}
			ZSCloseContainer( t, c);
			seqnolimit = n;
		}
		iseqno = seqno;
		iseqnolimit = iseqno + SEQNO_INCR;
		seqno += SEQNO_INCR;
		pthread_mutex_unlock( &seqnolock);
	}
	return (iseqno++);
}


ZS_status_t BtreeErr_to_ZSErr(btree_status_t b_status)
{
	ZS_status_t    f_status = ZS_SUCCESS;
	switch (b_status) {
		case BTREE_SUCCESS:
			f_status = ZS_SUCCESS;
			break;
		case BTREE_INVALID_QUERY:
		case BTREE_FAILURE:
			f_status = ZS_FAILURE;
			break;
		case BTREE_FAILURE_GENERIC:
			f_status = ZS_FAILURE_GENERIC;
			break;
		case BTREE_FAILURE_CONTAINER_GENERIC:
			f_status = ZS_FAILURE_CONTAINER_GENERIC;
			break;
		case BTREE_FAILURE_CONTAINER_NOT_OPEN:
			f_status = ZS_FAILURE_CONTAINER_NOT_OPEN;
			break;
		case BTREE_FAILURE_INVALID_CONTAINER_TYPE:
			f_status = ZS_FAILURE_INVALID_CONTAINER_TYPE;
			break;
		case BTREE_INVALID_PARAMETER:
			f_status = ZS_INVALID_PARAMETER;
			break;
		case BTREE_CONTAINER_UNKNOWN:
			f_status = ZS_CONTAINER_UNKNOWN;
			break;
		case BTREE_UNPRELOAD_CONTAINER_FAILED:
			f_status = ZS_UNPRELOAD_CONTAINER_FAILED;
			break;
		case BTREE_CONTAINER_EXISTS:
			f_status = ZS_CONTAINER_EXISTS;
			break;
		case BTREE_SHARD_NOT_FOUND:
			f_status = ZS_SHARD_NOT_FOUND;
			break;
		case BTREE_KEY_NOT_FOUND:
		case BTREE_OBJECT_UNKNOWN:
			f_status = ZS_OBJECT_UNKNOWN;
			break;
		case BTREE_OBJECT_EXISTS:
			f_status = ZS_OBJECT_EXISTS;
			break;
		case BTREE_OBJECT_TOO_BIG:
			f_status = ZS_OBJECT_TOO_BIG;
			break;
		case BTREE_FAILURE_STORAGE_READ:
			f_status = ZS_FAILURE_STORAGE_READ;
			break;
		case BTREE_FAILURE_STORAGE_WRITE:
			f_status = ZS_FAILURE_STORAGE_WRITE;
			break;
		case BTREE_FAILURE_MEMORY_ALLOC:
			f_status = ZS_FAILURE_MEMORY_ALLOC;
			break;
		case BTREE_LOCK_INVALID_OP:
			f_status = ZS_LOCK_INVALID_OP;
			break;
		case BTREE_ALREADY_UNLOCKED:
			f_status = ZS_ALREADY_UNLOCKED;
			break;
		case BTREE_ALREADY_READ_LOCKED:
			f_status = ZS_ALREADY_READ_LOCKED;
			break;
		case BTREE_ALREADY_WRITE_LOCKED:
			f_status = ZS_ALREADY_WRITE_LOCKED;
			break;
		case BTREE_OBJECT_NOT_CACHED:
			f_status = ZS_OBJECT_NOT_CACHED;
			break;
		case BTREE_SM_WAITING:
			f_status = ZS_SM_WAITING;
			break;
		case BTREE_TOO_MANY_OPIDS:
			f_status = ZS_TOO_MANY_OPIDS;
			break;
		case BTREE_TRANS_CONFLICT:
			f_status = ZS_TRANS_CONFLICT;
			break;
		case BTREE_PIN_CONFLICT:
			f_status = ZS_PIN_CONFLICT;
			break;
		case BTREE_OBJECT_DELETED:
			f_status = ZS_OBJECT_DELETED;
			break;
		case BTREE_TRANS_NONTRANS_CONFLICT:
			f_status = ZS_TRANS_NONTRANS_CONFLICT;
			break;
		case BTREE_ALREADY_READ_PINNED:
			f_status = ZS_ALREADY_READ_PINNED;
			break;
		case BTREE_ALREADY_WRITE_PINNED:
			f_status = ZS_ALREADY_WRITE_PINNED;
			break;
		case BTREE_TRANS_PIN_CONFLICT:
			f_status = ZS_TRANS_PIN_CONFLICT;
			break;
		case BTREE_PIN_NONPINNED_CONFLICT:
			f_status = ZS_PIN_NONPINNED_CONFLICT;
			break;
		case BTREE_TRANS_FLUSH:
			f_status = ZS_TRANS_FLUSH;
			break;
		case BTREE_TRANS_LOCK:
			f_status = ZS_TRANS_LOCK;
			break;
		case BTREE_TRANS_UNLOCK:
			f_status = ZS_TRANS_UNLOCK;
			break;
		case BTREE_UNSUPPORTED_REQUEST:
			f_status = ZS_UNSUPPORTED_REQUEST;
			break;
		case BTREE_UNKNOWN_REQUEST:
			f_status = ZS_UNKNOWN_REQUEST;
			break;
		case BTREE_BAD_PBUF_POINTER:
			f_status = ZS_BAD_PBUF_POINTER;
			break;
		case BTREE_BAD_PDATA_POINTER:
			f_status = ZS_BAD_PDATA_POINTER;
			break;
		case BTREE_BAD_SUCCESS_POINTER:
			f_status = ZS_BAD_SUCCESS_POINTER;
			break;
		case BTREE_NOT_PINNED:
			f_status = ZS_NOT_PINNED;
			break;
		case BTREE_NOT_READ_LOCKED:
			f_status = ZS_NOT_READ_LOCKED;
			break;
		case BTREE_NOT_WRITE_LOCKED:
			f_status = ZS_NOT_WRITE_LOCKED;
			break;
		case BTREE_PIN_FLUSH:
			f_status = ZS_PIN_FLUSH;
			break;
		case BTREE_BAD_CONTEXT:
			f_status = ZS_BAD_CONTEXT;
			break;
		case BTREE_IN_TRANS:
			f_status = ZS_IN_TRANS;
			break;
		case BTREE_NONCACHEABLE_CONTAINER:
			f_status = ZS_NONCACHEABLE_CONTAINER;
			break;
		case BTREE_OUT_OF_CONTEXTS:
			f_status = ZS_OUT_OF_CONTEXTS;
			break;
		case BTREE_INVALID_RANGE:
			f_status = ZS_INVALID_RANGE;
			break;
		case BTREE_OUT_OF_MEM:
			f_status = ZS_OUT_OF_MEM;
			break;
		case BTREE_NOT_IN_TRANS:
			f_status = ZS_NOT_IN_TRANS;
			break;
		case BTREE_TRANS_ABORTED:
			f_status = ZS_TRANS_ABORTED;
			break;
		case BTREE_FAILURE_MBOX:
			f_status = ZS_FAILURE_MBOX;
			break;
		case BTREE_FAILURE_MSG_ALLOC:
			f_status = ZS_FAILURE_MSG_ALLOC;
			break;
		case BTREE_FAILURE_MSG_SEND:
			f_status = ZS_FAILURE_MSG_SEND;
			break;
		case BTREE_FAILURE_MSG_RECEIVE:
			f_status = ZS_FAILURE_MSG_RECEIVE;
			break;
		case BTREE_ENUMERATION_END:
			f_status = ZS_ENUMERATION_END;
			break;
		case BTREE_BAD_KEY:
			f_status = ZS_BAD_KEY;
			break;
		case BTREE_FAILURE_CONTAINER_OPEN:
			f_status = ZS_FAILURE_CONTAINER_OPEN;
			break;
		case BTREE_BAD_PEXPTIME_POINTER:
			f_status = ZS_BAD_PEXPTIME_POINTER;
			break;
		case BTREE_BAD_PINVTIME_POINTER:
			f_status = ZS_BAD_PINVTIME_POINTER;
			break;
		case BTREE_BAD_PSTAT_POINTER:
			f_status = ZS_BAD_PSTAT_POINTER;
			break;
		case BTREE_BAD_PPCBUF_POINTER:
			f_status = ZS_BAD_PPCBUF_POINTER;
			break;
		case BTREE_BAD_SIZE_POINTER:
			f_status = ZS_BAD_SIZE_POINTER;
			break;
		case BTREE_EXPIRED:
			f_status = ZS_EXPIRED;
			break;
		case BTREE_EXPIRED_FAIL:
			f_status = ZS_EXPIRED_FAIL;
			break;
		case BTREE_PROTOCOL_ERROR:
			f_status = ZS_PROTOCOL_ERROR;
			break;
		case BTREE_TOO_MANY_CONTAINERS:
			f_status = ZS_TOO_MANY_CONTAINERS;
			break;
		case BTREE_STOPPED_CONTAINER:
			f_status = ZS_STOPPED_CONTAINER;
			break;
		case BTREE_GET_METADATA_FAILED:
			f_status = ZS_GET_METADATA_FAILED;
			break;
		case BTREE_PUT_METADATA_FAILED:
			f_status = ZS_PUT_METADATA_FAILED;
			break;
		case BTREE_GET_DIRENTRY_FAILED:
			f_status = ZS_GET_DIRENTRY_FAILED;
			break;
		case BTREE_EXPIRY_GET_FAILED:
			f_status = ZS_EXPIRY_GET_FAILED;
			break;
		case BTREE_EXPIRY_DELETE_FAILED:
			f_status = ZS_EXPIRY_DELETE_FAILED;
			break;
		case BTREE_EXIST_FAILED:
			f_status = ZS_EXIST_FAILED;
			break;
		case BTREE_NO_PSHARD:
			f_status = ZS_NO_PSHARD;
			break;
		case BTREE_SHARD_DELETE_SERVICE_FAILED:
			f_status = ZS_SHARD_DELETE_SERVICE_FAILED;
			break;
		case BTREE_START_SHARD_MAP_ENTRY_FAILED:
			f_status = ZS_START_SHARD_MAP_ENTRY_FAILED;
			break;
		case BTREE_STOP_SHARD_MAP_ENTRY_FAILED:
			f_status = ZS_STOP_SHARD_MAP_ENTRY_FAILED;
			break;
		case BTREE_DELETE_SHARD_MAP_ENTRY_FAILED:
			f_status = ZS_DELETE_SHARD_MAP_ENTRY_FAILED;
			break;
		case BTREE_CREATE_SHARD_MAP_ENTRY_FAILED:
			f_status = ZS_CREATE_SHARD_MAP_ENTRY_FAILED;
			break;
		case BTREE_FLASH_DELETE_FAILED:
			f_status = ZS_FLASH_DELETE_FAILED;
			break;
		case BTREE_FLASH_EPERM:
			f_status = ZS_FLASH_EPERM;
			break;
		case BTREE_FLASH_ENOENT:
			f_status = ZS_FLASH_ENOENT;
			break;
		case BTREE_FLASH_EIO:
			f_status = ZS_FLASH_EIO;
			break;
		case BTREE_FLASH_EAGAIN:
			f_status = ZS_FLASH_EAGAIN;
			break;
		case BTREE_FLASH_ENOMEM:
			f_status = ZS_FLASH_ENOMEM;
			break;
		case BTREE_FLASH_EDATASIZE:
			f_status = ZS_FLASH_EDATASIZE;
			break;
		case BTREE_FLASH_EBUSY:
			f_status = ZS_FLASH_EBUSY;
			break;
		case BTREE_FLASH_EEXIST:
			f_status = ZS_FLASH_EEXIST;
			break;
		case BTREE_FLASH_EACCES:
			f_status = ZS_FLASH_EACCES;
			break;
		case BTREE_FLASH_EINVAL:
			f_status = ZS_FLASH_EINVAL;
			break;
		case BTREE_FLASH_EMFILE:
			f_status = ZS_FLASH_EMFILE;
			break;
		case BTREE_FLASH_ENOSPC:
			f_status = ZS_FLASH_ENOSPC;
			break;
		case BTREE_FLASH_ENOBUFS:
			f_status = ZS_FLASH_ENOBUFS;
			break;
		case BTREE_FLASH_EDQUOT:
			f_status = ZS_FLASH_EDQUOT;
			break;
		case BTREE_FLASH_STALE_CURSOR:
			f_status = ZS_FLASH_STALE_CURSOR;
			break;
		case BTREE_FLASH_EDELFAIL:
			f_status = ZS_FLASH_EDELFAIL;
			break;
		case BTREE_FLASH_EINCONS:
			f_status = ZS_FLASH_EINCONS;
			break;
		case BTREE_STALE_LTIME:
			f_status = ZS_STALE_LTIME;
			break;
		case BTREE_WRONG_NODE:
			f_status = ZS_WRONG_NODE;
			break;
		case BTREE_UNAVAILABLE:
			f_status = ZS_UNAVAILABLE;
			break;
		case BTREE_TEST_FAIL:
			f_status = ZS_TEST_FAIL;
			break;
		case BTREE_TEST_CRASH:
			f_status = ZS_TEST_CRASH;
			break;
		case BTREE_VERSION_CHECK_NO_PEER:
			f_status = ZS_VERSION_CHECK_NO_PEER;
			break;
		case BTREE_VERSION_CHECK_BAD_VERSION:
			f_status = ZS_VERSION_CHECK_BAD_VERSION;
			break;
		case BTREE_VERSION_CHECK_FAILED:
			f_status = ZS_VERSION_CHECK_FAILED;
			break;
		case BTREE_META_DATA_VERSION_TOO_NEW:
			f_status = ZS_META_DATA_VERSION_TOO_NEW;
			break;
		case BTREE_META_DATA_INVALID:
			f_status = ZS_META_DATA_INVALID;
			break;
		case BTREE_BAD_META_SEQNO:
			f_status = ZS_BAD_META_SEQNO;
			break;
		case BTREE_BAD_LTIME:
			f_status = ZS_BAD_LTIME;
			break;
		case BTREE_LEASE_EXISTS:
			f_status = ZS_LEASE_EXISTS;
			break;
		case BTREE_BUSY:
			f_status = ZS_BUSY;
			break;
		case BTREE_SHUTDOWN:
			f_status = ZS_SHUTDOWN;
			break;
		case BTREE_TIMEOUT:
			f_status = ZS_TIMEOUT;
			break;
		case BTREE_NODE_DEAD:
			f_status = ZS_NODE_DEAD;
			break;
		case BTREE_SHARD_DOES_NOT_EXIST:
			f_status = ZS_SHARD_DOES_NOT_EXIST;
			break;
		case BTREE_STATE_CHANGED:
			f_status = ZS_STATE_CHANGED;
			break;
		case BTREE_NO_META:
			f_status = ZS_NO_META;
			break;
		case BTREE_TEST_MODEL_VIOLATION:
			f_status = ZS_TEST_MODEL_VIOLATION;
			break;
		case BTREE_REPLICATION_NOT_READY:
			f_status = ZS_REPLICATION_NOT_READY;
			break;
		case BTREE_REPLICATION_BAD_TYPE:
			f_status = ZS_REPLICATION_BAD_TYPE;
			break;
		case BTREE_REPLICATION_BAD_STATE:
			f_status = ZS_REPLICATION_BAD_STATE;
			break;
		case BTREE_NODE_INVALID:
			f_status = ZS_NODE_INVALID;
			break;
		case BTREE_CORRUPT_MSG:
			f_status = ZS_CORRUPT_MSG;
			break;
		case BTREE_QUEUE_FULL:
			f_status = ZS_QUEUE_FULL;
			break;
		case BTREE_RMT_CONTAINER_UNKNOWN:
			f_status = ZS_RMT_CONTAINER_UNKNOWN;
			break;
		case BTREE_FLASH_RMT_EDELFAIL:
			f_status = ZS_FLASH_RMT_EDELFAIL;
			break;
		case BTREE_LOCK_RESERVED:
			f_status = ZS_LOCK_RESERVED;
			break;
		case BTREE_KEY_TOO_LONG:
			f_status = ZS_KEY_TOO_LONG;
			break;
		case BTREE_NO_WRITEBACK_IN_STORE_MODE:
			f_status = ZS_NO_WRITEBACK_IN_STORE_MODE;
			break;
		case BTREE_WRITEBACK_CACHING_DISABLED:
			f_status = ZS_WRITEBACK_CACHING_DISABLED;
			break;
		case BTREE_UPDATE_DUPLICATE:
			f_status = ZS_UPDATE_DUPLICATE;
			break;
		case BTREE_FAILURE_CONTAINER_TOO_SMALL:
			f_status = ZS_FAILURE_CONTAINER_TOO_SMALL;
			break;
		case BTREE_CONTAINER_FULL:
			f_status = ZS_CONTAINER_FULL;
			break;
		case BTREE_CANNOT_REDUCE_CONTAINER_SIZE:
			f_status = ZS_CANNOT_REDUCE_CONTAINER_SIZE;
			break;
		case BTREE_CANNOT_CHANGE_CONTAINER_SIZE:
			f_status = ZS_CANNOT_CHANGE_CONTAINER_SIZE;
			break;
		case BTREE_OUT_OF_STORAGE_SPACE:
			f_status = ZS_OUT_OF_STORAGE_SPACE;
			break;
		case BTREE_TRANS_LEVEL_EXCEEDED:
			f_status = ZS_TRANS_LEVEL_EXCEEDED;
			break;
		case BTREE_FAILURE_NO_TRANS:
			f_status = ZS_FAILURE_NO_TRANS;
			break;
		case BTREE_CANNOT_DELETE_OPEN_CONTAINER:
			f_status = ZS_CANNOT_DELETE_OPEN_CONTAINER;
			break;
		case BTREE_FAILURE_INVALID_KEY_SIZE:
			f_status = ZS_FAILURE_INVALID_KEY_SIZE;
			break;
		case BTREE_FAILURE_OPERATION_DISALLOWED:
			f_status = ZS_FAILURE_OPERATION_DISALLOWED;
			break;
		case BTREE_FAILURE_ILLEGAL_CONTAINER_ID:
			f_status = ZS_FAILURE_ILLEGAL_CONTAINER_ID;
			break;
		case BTREE_FAILURE_CONTAINER_NOT_FOUND:
			f_status = ZS_FAILURE_CONTAINER_NOT_FOUND;
			break;
		case BTREE_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING:
			f_status = ZS_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING;
			break;
		case BTREE_THREAD_CONTEXT_BUSY:
			f_status = ZS_THREAD_CONTEXT_BUSY;
			break;
		case BTREE_LICENSE_CHK_FAILED:
			f_status = ZS_LICENSE_CHK_FAILED;
			break;
		case BTREE_CONTAINER_OPEN:
			f_status = ZS_CONTAINER_OPEN;
			break;
		case BTREE_FAILURE_INVALID_CONTAINER_SIZE:
			f_status = ZS_FAILURE_INVALID_CONTAINER_SIZE;
			break;
		case BTREE_FAILURE_INVALID_CONTAINER_STATE:
			f_status = ZS_FAILURE_INVALID_CONTAINER_STATE;
			break;
		case BTREE_FAILURE_CONTAINER_DELETED:
			f_status = ZS_FAILURE_CONTAINER_DELETED;
			break;
		case BTREE_QUERY_DONE:
			f_status = ZS_QUERY_DONE;
			break;
		case BTREE_FAILURE_CANNOT_CREATE_METADATA_CACHE:
			f_status = ZS_FAILURE_CANNOT_CREATE_METADATA_CACHE;
			break;
		case BTREE_BUFFER_TOO_SMALL:
		case BTREE_WARNING:
			f_status = ZS_WARNING;
			break;
		case BTREE_QUERY_PAUSED:
			f_status = ZS_QUERY_PAUSED;
			break;
		case BTREE_RESCUE_INVALID_REQUEST:
			f_status = ZS_RESCUE_INVALID_REQUEST;
			break;
		case BTREE_RESCUE_NOT_NEEDED:
			f_status = ZS_RESCUE_NOT_NEEDED;
			break;
		case BTREE_RESCUE_IO_ERROR:
			f_status = ZS_RESCUE_IO_ERROR;
			break;
		case BTREE_FAIL_TXN_START:
		case BTREE_FAIL_TXN_COMMIT:
		case BTREE_FAIL_TXN_ROLLBACK:
		case BTREE_OPERATION_DISALLOWED:
		case BTREE_RANGE_UPDATE_NEEDS_SPACE:
		case BTREE_SKIPPED:
		case BTREE_TOO_MANY_SNAPSHOTS:
				assert(0);
			break;
		case BTREE_UNKNOWN_STATUS:
			f_status = N_ZS_STATUS_STRINGS;
			break;
		default:
			assert(0);
	}
	return f_status;
}
btree_status_t ZSErr_to_BtreeErr(ZS_status_t f_status)
{
	btree_status_t b_status;
	switch (f_status) {
		case ZS_SUCCESS:
			b_status = BTREE_SUCCESS;
			break;
		case ZS_FAILURE:
			b_status = BTREE_FAILURE;
			break;
		case ZS_FAILURE_GENERIC:
			b_status = BTREE_FAILURE_GENERIC;
			break;
		case ZS_FAILURE_CONTAINER_GENERIC:
			b_status = BTREE_FAILURE_CONTAINER_GENERIC;
			break;
		case ZS_FAILURE_CONTAINER_NOT_OPEN:
			b_status = BTREE_FAILURE_CONTAINER_NOT_OPEN;
			break;
		case ZS_FAILURE_INVALID_CONTAINER_TYPE:
			b_status = BTREE_FAILURE_INVALID_CONTAINER_TYPE;
			break;
		case ZS_INVALID_PARAMETER:
			b_status = BTREE_INVALID_PARAMETER;
			break;
		case ZS_CONTAINER_UNKNOWN:
			b_status = BTREE_CONTAINER_UNKNOWN;
			break;
		case ZS_UNPRELOAD_CONTAINER_FAILED:
			b_status = BTREE_UNPRELOAD_CONTAINER_FAILED;
			break;
		case ZS_CONTAINER_EXISTS:
			b_status = BTREE_CONTAINER_EXISTS;
			break;
		case ZS_SHARD_NOT_FOUND:
			b_status = BTREE_SHARD_NOT_FOUND;
			break;
		case ZS_OBJECT_UNKNOWN:
			b_status = BTREE_OBJECT_UNKNOWN;
			break;
		case ZS_OBJECT_EXISTS:
			b_status = BTREE_OBJECT_EXISTS;
			break;
		case ZS_OBJECT_TOO_BIG:
			b_status = BTREE_OBJECT_TOO_BIG;
			break;
		case ZS_FAILURE_STORAGE_READ:
			b_status = BTREE_FAILURE_STORAGE_READ;
			break;
		case ZS_FAILURE_STORAGE_WRITE:
			b_status = BTREE_FAILURE_STORAGE_WRITE;
			break;
		case ZS_FAILURE_MEMORY_ALLOC:
			b_status = BTREE_FAILURE_MEMORY_ALLOC;
			break;
		case ZS_LOCK_INVALID_OP:
			b_status = BTREE_LOCK_INVALID_OP;
			break;
		case ZS_ALREADY_UNLOCKED:
			b_status = BTREE_ALREADY_UNLOCKED;
			break;
		case ZS_ALREADY_READ_LOCKED:
			b_status = BTREE_ALREADY_READ_LOCKED;
			break;
		case ZS_ALREADY_WRITE_LOCKED:
			b_status = BTREE_ALREADY_WRITE_LOCKED;
			break;
		case ZS_OBJECT_NOT_CACHED:
			b_status = BTREE_OBJECT_NOT_CACHED;
			break;
		case ZS_SM_WAITING:
			b_status = BTREE_SM_WAITING;
			break;
		case ZS_TOO_MANY_OPIDS:
			b_status = BTREE_TOO_MANY_OPIDS;
			break;
		case ZS_TRANS_CONFLICT:
			b_status = BTREE_TRANS_CONFLICT;
			break;
		case ZS_PIN_CONFLICT:
			b_status = BTREE_PIN_CONFLICT;
			break;
		case ZS_OBJECT_DELETED:
			b_status = BTREE_OBJECT_DELETED;
			break;
		case ZS_TRANS_NONTRANS_CONFLICT:
			b_status = BTREE_TRANS_NONTRANS_CONFLICT;
			break;
		case ZS_ALREADY_READ_PINNED:
			b_status = BTREE_ALREADY_READ_PINNED;
			break;
		case ZS_ALREADY_WRITE_PINNED:
			b_status = BTREE_ALREADY_WRITE_PINNED;
			break;
		case ZS_TRANS_PIN_CONFLICT:
			b_status = BTREE_TRANS_PIN_CONFLICT;
			break;
		case ZS_PIN_NONPINNED_CONFLICT:
			b_status = BTREE_PIN_NONPINNED_CONFLICT;
			break;
		case ZS_TRANS_FLUSH:
			b_status = BTREE_TRANS_FLUSH;
			break;
		case ZS_TRANS_LOCK:
			b_status = BTREE_TRANS_LOCK;
			break;
		case ZS_TRANS_UNLOCK:
			b_status = BTREE_TRANS_UNLOCK;
			break;
		case ZS_UNSUPPORTED_REQUEST:
			b_status = BTREE_UNSUPPORTED_REQUEST;
			break;
		case ZS_UNKNOWN_REQUEST:
			b_status = BTREE_UNKNOWN_REQUEST;
			break;
		case ZS_BAD_PBUF_POINTER:
			b_status = BTREE_BAD_PBUF_POINTER;
			break;
		case ZS_BAD_PDATA_POINTER:
			b_status = BTREE_BAD_PDATA_POINTER;
			break;
		case ZS_BAD_SUCCESS_POINTER:
			b_status = BTREE_BAD_SUCCESS_POINTER;
			break;
		case ZS_NOT_PINNED:
			b_status = BTREE_NOT_PINNED;
			break;
		case ZS_NOT_READ_LOCKED:
			b_status = BTREE_NOT_READ_LOCKED;
			break;
		case ZS_NOT_WRITE_LOCKED:
			b_status = BTREE_NOT_WRITE_LOCKED;
			break;
		case ZS_PIN_FLUSH:
			b_status = BTREE_PIN_FLUSH;
			break;
		case ZS_BAD_CONTEXT:
			b_status = BTREE_BAD_CONTEXT;
			break;
		case ZS_IN_TRANS:
			b_status = BTREE_IN_TRANS;
			break;
		case ZS_NONCACHEABLE_CONTAINER:
			b_status = BTREE_NONCACHEABLE_CONTAINER;
			break;
		case ZS_OUT_OF_CONTEXTS:
			b_status = BTREE_OUT_OF_CONTEXTS;
			break;
		case ZS_INVALID_RANGE:
			b_status = BTREE_INVALID_RANGE;
			break;
		case ZS_OUT_OF_MEM:
			b_status = BTREE_OUT_OF_MEM;
			break;
		case ZS_NOT_IN_TRANS:
			b_status = BTREE_NOT_IN_TRANS;
			break;
		case ZS_TRANS_ABORTED:
			b_status = BTREE_TRANS_ABORTED;
			break;
		case ZS_FAILURE_MBOX:
			b_status = BTREE_FAILURE_MBOX;
			break;
		case ZS_FAILURE_MSG_ALLOC:
			b_status = BTREE_FAILURE_MSG_ALLOC;
			break;
		case ZS_FAILURE_MSG_SEND:
			b_status = BTREE_FAILURE_MSG_SEND;
			break;
		case ZS_FAILURE_MSG_RECEIVE:
			b_status = BTREE_FAILURE_MSG_RECEIVE;
			break;
		case ZS_ENUMERATION_END:
			b_status = BTREE_ENUMERATION_END;
			break;
		case ZS_BAD_KEY:
			b_status = BTREE_BAD_KEY;
			break;
		case ZS_FAILURE_CONTAINER_OPEN:
			b_status = BTREE_FAILURE_CONTAINER_OPEN;
			break;
		case ZS_BAD_PEXPTIME_POINTER:
			b_status = BTREE_BAD_PEXPTIME_POINTER;
			break;
		case ZS_BAD_PINVTIME_POINTER:
			b_status = BTREE_BAD_PINVTIME_POINTER;
			break;
		case ZS_BAD_PSTAT_POINTER:
			b_status = BTREE_BAD_PSTAT_POINTER;
			break;
		case ZS_BAD_PPCBUF_POINTER:
			b_status = BTREE_BAD_PPCBUF_POINTER;
			break;
		case ZS_BAD_SIZE_POINTER:
			b_status = BTREE_BAD_SIZE_POINTER;
			break;
		case ZS_EXPIRED:
			b_status = BTREE_EXPIRED;
			break;
		case ZS_EXPIRED_FAIL:
			b_status = BTREE_EXPIRED_FAIL;
			break;
		case ZS_PROTOCOL_ERROR:
			b_status = BTREE_PROTOCOL_ERROR;
			break;
		case ZS_TOO_MANY_CONTAINERS:
			b_status = BTREE_TOO_MANY_CONTAINERS;
			break;
		case ZS_STOPPED_CONTAINER:
			b_status = BTREE_STOPPED_CONTAINER;
			break;
		case ZS_GET_METADATA_FAILED:
			b_status = BTREE_GET_METADATA_FAILED;
			break;
		case ZS_PUT_METADATA_FAILED:
			b_status = BTREE_PUT_METADATA_FAILED;
			break;
		case ZS_GET_DIRENTRY_FAILED:
			b_status = BTREE_GET_DIRENTRY_FAILED;
			break;
		case ZS_EXPIRY_GET_FAILED:
			b_status = BTREE_EXPIRY_GET_FAILED;
			break;
		case ZS_EXPIRY_DELETE_FAILED:
			b_status = BTREE_EXPIRY_DELETE_FAILED;
			break;
		case ZS_EXIST_FAILED:
			b_status = BTREE_EXIST_FAILED;
			break;
		case ZS_NO_PSHARD:
			b_status = BTREE_NO_PSHARD;
			break;
		case ZS_SHARD_DELETE_SERVICE_FAILED:
			b_status = BTREE_SHARD_DELETE_SERVICE_FAILED;
			break;
		case ZS_START_SHARD_MAP_ENTRY_FAILED:
			b_status = BTREE_START_SHARD_MAP_ENTRY_FAILED;
			break;
		case ZS_STOP_SHARD_MAP_ENTRY_FAILED:
			b_status = BTREE_STOP_SHARD_MAP_ENTRY_FAILED;
			break;
		case ZS_DELETE_SHARD_MAP_ENTRY_FAILED:
			b_status = BTREE_DELETE_SHARD_MAP_ENTRY_FAILED;
			break;
		case ZS_CREATE_SHARD_MAP_ENTRY_FAILED:
			b_status = BTREE_CREATE_SHARD_MAP_ENTRY_FAILED;
			break;
		case ZS_FLASH_DELETE_FAILED:
			b_status = BTREE_FLASH_DELETE_FAILED;
			break;
		case ZS_FLASH_EPERM:
			b_status = BTREE_FLASH_EPERM;
			break;
		case ZS_FLASH_ENOENT:
			b_status = BTREE_FLASH_ENOENT;
			break;
		case ZS_FLASH_EIO:
			b_status = BTREE_FLASH_EIO;
			break;
		case ZS_FLASH_EAGAIN:
			b_status = BTREE_FLASH_EAGAIN;
			break;
		case ZS_FLASH_ENOMEM:
			b_status = BTREE_FLASH_ENOMEM;
			break;
		case ZS_FLASH_EDATASIZE:
			b_status = BTREE_FLASH_EDATASIZE;
			break;
		case ZS_FLASH_EBUSY:
			b_status = BTREE_FLASH_EBUSY;
			break;
		case ZS_FLASH_EEXIST:
			b_status = BTREE_FLASH_EEXIST;
			break;
		case ZS_FLASH_EACCES:
			b_status = BTREE_FLASH_EACCES;
			break;
		case ZS_FLASH_EINVAL:
			b_status = BTREE_FLASH_EINVAL;
			break;
		case ZS_FLASH_EMFILE:
			b_status = BTREE_FLASH_EMFILE;
			break;
		case ZS_FLASH_ENOSPC:
			b_status = BTREE_FLASH_ENOSPC;
			break;
		case ZS_FLASH_ENOBUFS:
			b_status = BTREE_FLASH_ENOBUFS;
			break;
		case ZS_FLASH_EDQUOT:
			b_status = BTREE_FLASH_EDQUOT;
			break;
		case ZS_FLASH_STALE_CURSOR:
			b_status = BTREE_FLASH_STALE_CURSOR;
			break;
		case ZS_FLASH_EDELFAIL:
			b_status = BTREE_FLASH_EDELFAIL;
			break;
		case ZS_FLASH_EINCONS:
			b_status = BTREE_FLASH_EINCONS;
			break;
		case ZS_STALE_LTIME:
			b_status = BTREE_STALE_LTIME;
			break;
		case ZS_WRONG_NODE:
			b_status = BTREE_WRONG_NODE;
			break;
		case ZS_UNAVAILABLE:
			b_status = BTREE_UNAVAILABLE;
			break;
		case ZS_TEST_FAIL:
			b_status = BTREE_TEST_FAIL;
			break;
		case ZS_TEST_CRASH:
			b_status = BTREE_TEST_CRASH;
			break;
		case ZS_VERSION_CHECK_NO_PEER:
			b_status = BTREE_VERSION_CHECK_NO_PEER;
			break;
		case ZS_VERSION_CHECK_BAD_VERSION:
			b_status = BTREE_VERSION_CHECK_BAD_VERSION;
			break;
		case ZS_VERSION_CHECK_FAILED:
			b_status = BTREE_VERSION_CHECK_FAILED;
			break;
		case ZS_META_DATA_VERSION_TOO_NEW:
			b_status = BTREE_META_DATA_VERSION_TOO_NEW;
			break;
		case ZS_META_DATA_INVALID:
			b_status = BTREE_META_DATA_INVALID;
			break;
		case ZS_BAD_META_SEQNO:
			b_status = BTREE_BAD_META_SEQNO;
			break;
		case ZS_BAD_LTIME:
			b_status = BTREE_BAD_LTIME;
			break;
		case ZS_LEASE_EXISTS:
			b_status = BTREE_LEASE_EXISTS;
			break;
		case ZS_BUSY:
			b_status = BTREE_BUSY;
			break;
		case ZS_SHUTDOWN:
			b_status = BTREE_SHUTDOWN;
			break;
		case ZS_TIMEOUT:
			b_status = BTREE_TIMEOUT;
			break;
		case ZS_NODE_DEAD:
			b_status = BTREE_NODE_DEAD;
			break;
		case ZS_SHARD_DOES_NOT_EXIST:
			b_status = BTREE_SHARD_DOES_NOT_EXIST;
			break;
		case ZS_STATE_CHANGED:
			b_status = BTREE_STATE_CHANGED;
			break;
		case ZS_NO_META:
			b_status = BTREE_NO_META;
			break;
		case ZS_TEST_MODEL_VIOLATION:
			b_status = BTREE_TEST_MODEL_VIOLATION;
			break;
		case ZS_REPLICATION_NOT_READY:
			b_status = BTREE_REPLICATION_NOT_READY;
			break;
		case ZS_REPLICATION_BAD_TYPE:
			b_status = BTREE_REPLICATION_BAD_TYPE;
			break;
		case ZS_REPLICATION_BAD_STATE:
			b_status = BTREE_REPLICATION_BAD_STATE;
			break;
		case ZS_NODE_INVALID:
			b_status = BTREE_NODE_INVALID;
			break;
		case ZS_CORRUPT_MSG:
			b_status = BTREE_CORRUPT_MSG;
			break;
		case ZS_QUEUE_FULL:
			b_status = BTREE_QUEUE_FULL;
			break;
		case ZS_RMT_CONTAINER_UNKNOWN:
			b_status = BTREE_RMT_CONTAINER_UNKNOWN;
			break;
		case ZS_FLASH_RMT_EDELFAIL:
			b_status = BTREE_FLASH_RMT_EDELFAIL;
			break;
		case ZS_LOCK_RESERVED:
			b_status = BTREE_LOCK_RESERVED;
			break;
		case ZS_KEY_TOO_LONG:
			b_status = BTREE_KEY_TOO_LONG;
			break;
		case ZS_NO_WRITEBACK_IN_STORE_MODE:
			b_status = BTREE_NO_WRITEBACK_IN_STORE_MODE;
			break;
		case ZS_WRITEBACK_CACHING_DISABLED:
			b_status = BTREE_WRITEBACK_CACHING_DISABLED;
			break;
		case ZS_UPDATE_DUPLICATE:
			b_status = BTREE_UPDATE_DUPLICATE;
			break;
		case ZS_FAILURE_CONTAINER_TOO_SMALL:
			b_status = BTREE_FAILURE_CONTAINER_TOO_SMALL;
			break;
		case ZS_CONTAINER_FULL:
			b_status = BTREE_CONTAINER_FULL;
			break;
		case ZS_CANNOT_REDUCE_CONTAINER_SIZE:
			b_status = BTREE_CANNOT_REDUCE_CONTAINER_SIZE;
			break;
		case ZS_CANNOT_CHANGE_CONTAINER_SIZE:
			b_status = BTREE_CANNOT_CHANGE_CONTAINER_SIZE;
			break;
		case ZS_OUT_OF_STORAGE_SPACE:
			b_status = BTREE_OUT_OF_STORAGE_SPACE;
			break;
		case ZS_TRANS_LEVEL_EXCEEDED:
			b_status = BTREE_TRANS_LEVEL_EXCEEDED;
			break;
		case ZS_FAILURE_NO_TRANS:
			b_status = BTREE_FAILURE_NO_TRANS;
			break;
		case ZS_CANNOT_DELETE_OPEN_CONTAINER:
			b_status = BTREE_CANNOT_DELETE_OPEN_CONTAINER;
			break;
		case ZS_FAILURE_INVALID_KEY_SIZE:
			b_status = BTREE_FAILURE_INVALID_KEY_SIZE;
			break;
		case ZS_FAILURE_OPERATION_DISALLOWED:
			b_status = BTREE_FAILURE_OPERATION_DISALLOWED;
			break;
		case ZS_FAILURE_ILLEGAL_CONTAINER_ID:
			b_status = BTREE_FAILURE_ILLEGAL_CONTAINER_ID;
			break;
		case ZS_FAILURE_CONTAINER_NOT_FOUND:
			b_status = BTREE_FAILURE_CONTAINER_NOT_FOUND;
			break;
		case ZS_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING:
			b_status = BTREE_UNLIMITED_CONTAINER_MUST_BE_NON_EVICTING;
			break;
		case ZS_THREAD_CONTEXT_BUSY:
			b_status = BTREE_THREAD_CONTEXT_BUSY;
			break;
		case ZS_LICENSE_CHK_FAILED:
			b_status = BTREE_LICENSE_CHK_FAILED;
			break;
		case ZS_CONTAINER_OPEN:
			b_status = BTREE_CONTAINER_OPEN;
			break;
		case ZS_FAILURE_INVALID_CONTAINER_SIZE:
			b_status = BTREE_FAILURE_INVALID_CONTAINER_SIZE;
			break;
		case ZS_FAILURE_INVALID_CONTAINER_STATE:
			b_status = BTREE_FAILURE_INVALID_CONTAINER_STATE;
			break;
		case ZS_FAILURE_CONTAINER_DELETED:
			b_status = BTREE_FAILURE_CONTAINER_DELETED;
			break;
		case ZS_QUERY_DONE:
			b_status = BTREE_QUERY_DONE;
			break;
		case ZS_FAILURE_CANNOT_CREATE_METADATA_CACHE:
			b_status = BTREE_FAILURE_CANNOT_CREATE_METADATA_CACHE;
			break;
		case ZS_WARNING:
			b_status = BTREE_WARNING;
			break;
		case ZS_QUERY_PAUSED:
			b_status = BTREE_QUERY_PAUSED;
			break;
		case ZS_RESCUE_INVALID_REQUEST:
			b_status = BTREE_RESCUE_INVALID_REQUEST;
			break;
		case ZS_RESCUE_NOT_NEEDED:
			b_status = BTREE_RESCUE_NOT_NEEDED;
			break;
		case ZS_RESCUE_IO_ERROR:
			b_status = BTREE_RESCUE_IO_ERROR;
			break;
		case N_ZS_STATUS_STRINGS:
		default:
			b_status = BTREE_UNKNOWN_STATUS;
			break;
	}
	return b_status;
}


/**
 * @brief: Create the stats container, open if exists!
 */
void
ZSLoadPstats(struct ZS_state *zs_state)
{
    ZS_container_props_t p;
    ZS_status_t ret = ZS_SUCCESS;
    struct ZS_thread_state *thd_state = NULL;

    ret = ZSInitPerThreadState(zs_state, &thd_state);
    assert (ZS_SUCCESS == ret);

    ret = ZSOpenContainer(thd_state, stats_ctnr_name, &p, 0, &stats_ctnr_cguid);
    fprintf(stderr, "ZSLoadPstats:ZSOpenContainer ret=%s\n", ZSStrError(ret));

    switch(ret) {
        case ZS_SUCCESS:
            break;
        default:
        case ZS_INVALID_PARAMETER:
            p.size_kb = 0;
            p.fifo_mode = ZS_FALSE;
            p.persistent = ZS_TRUE;
            p.evicting = ZS_FALSE;
            p.writethru = ZS_TRUE;
            p.durability_level = ZS_DURABILITY_SW_CRASH_SAFE;
            /*
             * We will use a hash container
             */
            p.flags |= (1 << 0);
            ret = ZSOpenContainer( thd_state, stats_ctnr_name, &p, ZS_CTNR_CREATE, &stats_ctnr_cguid);
            assert(ZS_SUCCESS == ret);        
    }

    ret = ZSReleasePerThreadState(&thd_state);
    assert (ZS_SUCCESS == ret);
}


/**
 * @brief: Read persistent stats for a given Btree 
 *         and init stats in Btree stats
 */
void
ZSInitPstats(struct ZS_thread_state *my_thd_state, char *key, zs_pstats_t *pstats)
{
    ZS_status_t ret = ZS_SUCCESS;

    char *data = NULL;
    uint64_t len = 0;

    ret = ZSReadObject(my_thd_state, stats_ctnr_cguid, key, strlen(key)+1, &data, &len);
    if (ZS_SUCCESS == ret) {
        pstats->seq_num   = ((zs_pstats_t*)data)->seq_num;
        pstats->obj_count = ((zs_pstats_t*)data)->obj_count;
		pstats->num_snap_objs = ((zs_pstats_t*)data)->num_snap_objs;
		pstats->snap_data_size = ((zs_pstats_t*)data)->snap_data_size;
		pstats->num_overflw_nodes = ((zs_pstats_t*)data)->num_overflw_nodes;
        //pstats->cntr_sz   = ((zs_pstats_t*)data)->cntr_sz;
        fprintf(stderr, "ZSInitPstats: seq = %ld obcount=%ld\n", pstats->seq_num, pstats->obj_count);
    } else {
        pstats->seq_num = 0;
        pstats->obj_count = 0;
		pstats->num_snap_objs = 0;
		pstats->snap_data_size = 0;
		pstats->num_overflw_nodes = 0;
        //fprintf(stderr, "Error: ZSInitPstats failed to read stats for cname %s\n", key);
        //pstats->cntr_sz = 0;
    }
    ZSFreeBuffer(data);
}


ZS_status_t 
zs_fix_objs_cnt_stats(struct ZS_thread_state *thd_state, uint64_t obj_count, char *cont_name)
{

    ZS_status_t ret = ZS_SUCCESS;
    zs_pstats_t *pstats = NULL;
    uint64_t datalen = 0;


    /*
     * Read the cont stats
     */
    ret = ZSReadObject(thd_state, stats_ctnr_cguid, cont_name, 
			strlen(cont_name) + 1, (char **) &pstats, &datalen);

    if (ret != ZS_SUCCESS) {
	return ret;
    }
    assert(datalen == sizeof(zs_pstats_t));


    pstats->obj_count = obj_count;
    
    /*   
     * Write stats to ZS physical container
     */
    ret = ZSWriteObject(thd_state, stats_ctnr_cguid, cont_name, 
			strlen(cont_name) + 1, (char *)pstats, sizeof(zs_pstats_t), 0);

    ZSFreeBuffer((char *) pstats);
    return ret;
}

static ZS_status_t 
zs_commit_stats_int(struct ZS_thread_state *thd_state, zs_pstats_t *s, char *key)
{

    ZS_status_t ret = ZS_SUCCESS;

    /*   
     * Write stats to ZS physical container
     */
    ret = ZSWriteObject(thd_state, stats_ctnr_cguid, key, strlen(key) + 1, (char*)s, sizeof(zs_pstats_t), 0);
    return ret;
}

#define SIMBACKUP_DEFAULT_CHUNK_SIZE     50
#define SIMBACKUP_DEFAULT_SLEEP_USECS    1000
#define PROGRESS_PRINT_CHUNKS            1000

static void
simulate_backup(struct ZS_thread_state *thd_state, ZS_cguid_t cguid, 
                uint64_t start_seqno, uint64_t end_seqno, int chunk_size, 
                int sleep_usecs, FILE *fp)
{
	ZS_status_t ret;
	ZS_range_meta_t *rmeta;
	ZS_range_data_t *rvalues;
	struct ZS_cursor *cursor;       // opaque cursor handle
	uint64_t overall_cnt = 0;
	int n_out;
	int i;

	/* Initialize rmeta */
	rmeta = (ZS_range_meta_t *)malloc(sizeof(ZS_range_meta_t));
	rmeta->key_start = NULL;
	rmeta->keylen_start = 0;
	rmeta->key_end   = NULL;
	rmeta->keylen_end = 0;
	rmeta->class_cmp_fn = NULL;
	rmeta->allowed_fn = NULL;
	rmeta->cb_data = NULL;

	if (start_seqno == -1) {
		rmeta->flags = ZS_RANGE_SEQNO_LE;
		rmeta->end_seq = end_seqno;
	} else {
		rmeta->flags = ZS_RANGE_SEQNO_GT_LE;
		rmeta->start_seq = start_seqno;
		rmeta->end_seq   = end_seqno;
	}

	ret = _ZSGetRange(thd_state,
	                  cguid,
	                  ZS_RANGE_PRIMARY_INDEX,
	                  &cursor, 
	                  rmeta);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ZSStartRangeQuery failed with status=%d\n", ret);
		return;
	}
	free(rmeta);

	if (chunk_size == 0) chunk_size = SIMBACKUP_DEFAULT_CHUNK_SIZE;
	if (sleep_usecs == 0) sleep_usecs = SIMBACKUP_DEFAULT_SLEEP_USECS;
	fprintf(fp, "Starting backup simulation with each read of "
	       "chunk_size: %d every %d usecs", chunk_size, sleep_usecs);
	fflush(fp);

	do {
		rvalues = (ZS_range_data_t *)
		           malloc(sizeof(ZS_range_data_t) * chunk_size);
		assert(rvalues);

		ret = _ZSGetNextRange(thd_state,
		                      cursor,
		                      chunk_size,
		                      &n_out,
		                      rvalues);

		if ((ret != ZS_SUCCESS) && (ret != ZS_QUERY_DONE)) {
			fprintf(fp, "Error: Snapshot read returned %d\n", ret);
			free(rvalues);
			break;
		}

		for (i = 0; i < n_out; i++) {
			free(rvalues[i].key);
			free(rvalues[i].data);
		}
	
		overall_cnt += n_out;
		free(rvalues);

		/* Sleep for every 10 chunks */
		if (overall_cnt % (chunk_size * PROGRESS_PRINT_CHUNKS) == 0) {
			fprintf(fp, ".");
			fflush(fp);
		}

		usleep(sleep_usecs);
	} while (ret != ZS_QUERY_DONE);
	fprintf(fp, "\n");

	ret = _ZSGetRangeFinish(thd_state, cursor);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ERROR: ZSGetRangeFinish failed ret=%d\n", ret);
	}
	fprintf(fp, "Backup of %"PRIu64" objects completed\n", overall_cnt);
	fflush(fp);
}

static ZS_status_t btree_process_snapshot_cmd(
                       struct ZS_thread_state *thd_state, 
                       FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	ZS_container_snapshots_t *snaps;
	ZS_cguid_t cguid;
	ZS_status_t status;
	uint32_t n_snaps = 0;
	uint32_t i;
	uint64_t snap_seqno;

	if ( ntokens < 3 ) {
		fprintf(fp, "Invalid argument! Type help for more info\n");
		return ZS_FAILURE;
	}

	status = ZS_FAILURE;
	if (strcmp(tokens[1].value, "list") == 0) {
		cguid = atol(tokens[2].value);
		status = _ZSGetContainerSnapshots(thd_state, cguid, &n_snaps, &snaps);
		if ((status != ZS_SUCCESS) || (n_snaps == 0)) {
			fprintf(fp, "No snapshots for cguid: %"PRIu64"\n", cguid);
		} else {
			for (i = 0; i < n_snaps; i++) {
				fprintf(fp, "\t%d: %"PRIu64"\n", i, snaps[i].seqno);
			}
		}
	} else if (strcmp(tokens[1].value, "create") == 0) {
		cguid = atol(tokens[2].value);
		status = _ZSCreateContainerSnapshot(thd_state, cguid, &snap_seqno);
		if (status == ZS_SUCCESS) {
			fprintf(fp, "Snapshot created seqno=%"PRIu64
			            " for cguid=%"PRIu64"\n", snap_seqno, cguid);
		} else {
			fprintf(fp, "Snapshot create failed for cguid=%"PRIu64
			            " Error=%s\n", cguid, ZSStrError(status));
		}
	} else if (strcmp(tokens[1].value, "delete") == 0) {
		if (ntokens < 4) {
			fprintf(fp, "Invalid argument! Type help for more info\n");
			return ZS_FAILURE;
		}
			
		cguid = atol(tokens[2].value);
		snap_seqno = atol(tokens[3].value);

		status = _ZSDeleteContainerSnapshot(thd_state, cguid, snap_seqno);
		if (status == ZS_SUCCESS) {
			fprintf(fp, "Snapshot deleted seqno=%"PRIu64
			            " for cguid=%"PRIu64"\n", snap_seqno, cguid);
		} else {
			fprintf(fp, "Snapshot delete failed for cguid=%"PRIu64
			            " Error=%s\n", cguid, ZSStrError(status));
		}
	} else if (strcmp(tokens[1].value, "sim_backup") == 0) {
		if (ntokens < 5) {
			fprintf(fp, "Invalid argument! Type help for more info\n");
			return ZS_FAILURE;
		}

		cguid = atol(tokens[2].value);
		uint64_t start_seqno = atol(tokens[3].value);
		uint64_t end_seqno = atol(tokens[4].value);
		int chunk_size = (ntokens >= 6) ? atoi(tokens[5].value): 0;
		int sleep_usecs = (ntokens >= 7) ? atoi(tokens[6].value): 0;

		simulate_backup(thd_state, cguid, start_seqno, end_seqno, chunk_size, sleep_usecs, fp);
	}

	return status;
}

ZS_status_t btree_process_admin_cmd(
                       struct ZS_thread_state *thd_state, 
                       FILE *fp, cmd_token_t *tokens, size_t ntokens)
{
	if (strcmp(tokens[0].value, "snapshot") == 0) {
		return (btree_process_snapshot_cmd(thd_state, fp, tokens, ntokens));
	} else if (strcmp(tokens[0].value, "help") == 0) {
		fprintf(fp, "snapshot create <cguid>\n");
		fprintf(fp, "snapshot delete <cguid>\n");
		fprintf(fp, "snapshot list <cguid>\n\n");
	}

	return (ZS_FAILURE);
}

ZS_status_t _ZSScavenger(struct ZS_state  *zs_state) 
{
    assert(0);
        ZS_status_t                                    ret = ZS_FAILURE;
        Scavenge_Arg_t s;
		if (bt_is_license_valid() == false) {
			return (ZS_LICENSE_CHK_FAILED);
		}
		//Check for shutdown is not handled
        s.type = SC_STALE_ENT;
        ret = btree_scavenge(zs_state, s);
        return ret;
       // invoke and return;
}

ZS_status_t _ZSScavengeContainer(struct ZS_state *zs_state, ZS_cguid_t cguid) 
{
    ZS_status_t    ret = ZS_FAILURE;
    int             index;
    struct btree    *bt;
    Scavenge_Arg_t s;
    const char *ZS_SCAVENGER_THROTTLE_VALUE = "1";  // 1 milli second
    s.throttle_value = 1;

#if 0
    char *p = _ZSGetProperty("ZS_SCAVENGER_ENABLE", NULL);
    if (!p) {
        fprintf(stderr, "Disbaling scavenger\n");
        return ZS_SUCCESS;
    }
#endif

	if (bt_is_license_valid() == false) {
		return (ZS_LICENSE_CHK_FAILED);
	}
    if ((ret = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return ret;
    }

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        fprintf(stderr, "Scavenger is unavailable on a hash container\n");
        cm_unlock(cguid);
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return ret;
    }

    s.type = SC_STALE_ENT;
    s.cguid = cguid;
    s.btree_index = index;
    s.btree = (struct btree_raw *)(bt->partitions[0]);
    s.bt = bt;
    s.throttle_value = atoi(_ZSGetProperty("ZS_SCAVENGER_THROTTLE_VALUE",ZS_SCAVENGER_THROTTLE_VALUE));

    index = bt_get_cguid(s.cguid);
    pthread_rwlock_wrlock(&ctnrmap_rwlock);

    if (Container_Map[index].scavenger_state == 1) {
        fprintf(stderr,"scavenge operation on this container is already in progress\n");
        bt_rel_entry(index, false);
        pthread_rwlock_unlock(&ctnrmap_rwlock);
        return ZS_FAILURE;
    } else {
        (s.btree)->snap_meta->sc_status |= SC_STALE_ENT;
        Container_Map[index].scavenger_state = 1;
    }
    pthread_rwlock_unlock(&ctnrmap_rwlock);

    savepersistent(s.btree, FLUSH_SNAPSHOT, true);
    if (deref_l1cache(s.btree) != BTREE_SUCCESS) {
        fprintf(stderr," deref_l1_cache failed\n");
    }
    ret = btree_scavenge(zs_state, s);
    return ret;
}

ZS_status_t _ZSScavengeSnapshot(struct ZS_state *zs_state, ZS_cguid_t cguid, uint64_t snap_seq) {
    ZS_status_t    ret = ZS_FAILURE;
    Scavenge_Arg_t s;

    cm_lock(cguid, READ);
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        fprintf(stderr, "Scavenger is unavailable on a hash container\n");
        cm_unlock(cguid);
        return ZS_FAILURE_INVALID_CONTAINER_TYPE;
    }
    cm_unlock(cguid);

    //Check for shutdown is not handled
    s.type = SC_STALE_ENT;
    s.cguid = cguid;
    return ret;
    ret = btree_scavenge(zs_state, s);
}

ZS_status_t
ZSStartAstats(struct ZS_state *zs_state, ZS_cguid_t cguid)
{
    assert(zs_state);

    ZS_status_t    ret = ZS_FAILURE;
    int             index;
    struct btree    *bt;
    astats_arg_t    s;
    char buf[64]; 

    if ((ret = bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return ret;
    }

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        return ret;
    }

    s.cguid = cguid;
    s.btree = (struct btree_raw *)(bt->partitions[0]);
    s.bt = bt;
    s.btree_index = index;

    sprintf(buf, "%u", ASYNC_STATS_SUSPEND_NODE_COUNT);
    s.suspend_duration= atoi(ZSGetProperty("ZS_ASYNC_STATS_SUSPEND_NODE_COUNT", buf));

    sprintf(buf, "%u", ASYNC_STATS_SUSPEND_DURATION);
    s.suspend_after_node_count = atoi(ZSGetProperty("ZS_ASYNC_STATS_SUSPEND_DURATION", buf));

    ret = btree_start_astats(zs_state, s);

    index = bt_get_cguid(s.cguid);
    bt_rel_entry(index, false);

    return ret;
}


ZS_status_t
open_container(struct ZS_thread_state *zs_thread_state, ZS_cguid_t cguid)
{
    int           index = -1;
    ZS_status_t       ret = ZS_SUCCESS;
    ZS_container_props_t   pprops;
    uint32_t                  flags_in = ZS_CTNR_RW_MODE;
    ZSLoadCntrPropDefaults(&pprops);
    ret = ZSGetContainerProps(zs_thread_state,cguid, &pprops);

    if (ret != ZS_SUCCESS)
    {
        fprintf(stderr,"ZSGetContainerProps failed with error %s\n",  ZSStrError(ret));
        return ret;
    }

    ret = ZSOpenContainer(zs_thread_state, pprops.name, &pprops, flags_in, &cguid);
    if (ret != ZS_SUCCESS)
    {
        fprintf(stderr,"ZSOpenContainer failed with error %s\n",  ZSStrError(ret));
        return ret;
    }

    index = bt_get_cguid(cguid);
    // Need to add the code (btreeinit) if cguid is added but not retrived

    /* This shouldnt happen, how ZS could create container but we exhausted map */
    if (index == -1) {
        ZSCloseContainer(zs_thread_state, cguid);
        fprintf(stderr,"ZS Scavenger failed to open the containte with error ZS_TOO_MANY_CONTAINERS\n");
    }

    pthread_rwlock_rdlock(&ctnrmap_rwlock);

    if (Container_Map[index].cguid != cguid) {
        pthread_rwlock_unlock(&ctnrmap_rwlock);
        fprintf(stderr,"ZS Scavenger failed, error: Container got deleted\n");
        return ZS_FAILURE;
    }

    pthread_rwlock_unlock(&ctnrmap_rwlock);
    return ret;
}


ZS_status_t
astats_open_container(struct ZS_thread_state *zs_thread_state, ZS_cguid_t cguid, astats_arg_t *s)
{
    int           index = -1;
    ZS_status_t       ret = ZS_SUCCESS;
    struct btree    *bt;

    ZS_container_props_t   pprops;
    uint32_t                  flags_in = ZS_CTNR_RW_MODE;

    ZSLoadCntrPropDefaults(&pprops);

    ret = ZSGetContainerProps(zs_thread_state,cguid, &pprops);
    if (ret != ZS_SUCCESS) {
        fprintf(stderr,"ZSGetContainerProps failed with error %s\n",  ZSStrError(ret));
        return ret;
    }

    ret = ZSOpenContainer(zs_thread_state, pprops.name, &pprops, flags_in, &cguid);
    if (ret != ZS_SUCCESS) {
        fprintf(stderr,"ZSOpenContainer failed with error %s\n",  ZSStrError(ret));
        return ret;
    }

    index = bt_get_cguid(cguid);
    // Need to add the code (btreeinit) if cguid is added but not retrived

    /* This shouldnt happen, how ZS could create container but we exhausted map */
    if (index == -1) {
        ZSCloseContainer(zs_thread_state, cguid);
        fprintf(stderr,"ZS Scavenger failed to open the containte with error ZS_TOO_MANY_CONTAINERS\n");
    }

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, false);
    if (bt == NULL) {
        ret = ZS_FAILURE;
        return ret;
    }

    index = bt_get_cguid(cguid);
    s->cguid = cguid;
    s->btree = (struct btree_raw *)(bt->partitions[0]);
    s->bt = bt;
    s->btree_index = index;

    pthread_rwlock_rdlock(&ctnrmap_rwlock);

    if (Container_Map[index].cguid != cguid) {
        pthread_rwlock_unlock(&ctnrmap_rwlock);
        fprintf(stderr,"ZS Async stats failed, error: Container got deleted\n");
        return ZS_FAILURE;
    }
    pthread_rwlock_unlock(&ctnrmap_rwlock);

    return ret;
}


void
close_container(struct ZS_thread_state *zs_thread_state, Scavenge_Arg_t *S)
{
    ZS_status_t       ret = ZS_SUCCESS;
    int           index = -1;

    assert(S);
    (S->bt->partitions[0])->snap_meta->sc_status &= ~(S->type);
    savepersistent(S->btree, FLUSH_SNAPSHOT, true);
    if (deref_l1cache(S->btree) != BTREE_SUCCESS) {
        fprintf(stderr,"deref_l1_cache failed\n");
    }
    bt_rel_entry(S->btree_index, false);
    index = bt_get_cguid(S->cguid);
    pthread_rwlock_wrlock(&ctnrmap_rwlock);
    Container_Map[index].scavenger_state = 0;
    pthread_rwlock_unlock(&ctnrmap_rwlock);
}


void
astats_deref_container(astats_arg_t *S)
{
    bt_rel_entry(S->btree_index, false);
}


int
bt_get_cguid(ZS_cguid_t cguid)
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

void bt_cntr_unlock_scavenger(Scavenge_Arg_t *s, bool write)
{
	 bt_rel_entry(s->btree_index, write);
}

ZS_status_t bt_cntr_lock_scavenger(Scavenge_Arg_t *s)
{
	ZS_status_t       zs_ret = ZS_SUCCESS;
	struct btree    *bt;
	bt = bt_get_btree_from_cguid(s->cguid, &(s->btree_index), &zs_ret, false);
	if (bt == NULL) {
		s->bt = NULL;
		s->btree = NULL;
		return zs_ret;
	}
	s->bt = bt;
	s->btree = (struct btree_raw *)(bt->partitions[0]);
	return zs_ret;
}

static void* 
bt_restart_delcont(void *parm)
{
    assert(parm);

    ZS_status_t ret = ZS_SUCCESS;

    struct ZS_thread_state		*thd_state = NULL;
    struct ZS_state			*zs_state = (struct ZS_state*)parm;
	ZS_container_props_t		props;
	uint32_t					ncg, i, j, flag;
	ZS_cguid_t					*cguids = NULL, cguid;
	char 						*node;
	uint64_t					node_size;
	uint64_t 					nodeid = META_SNAPSHOT_LOGICAL_ID;
	btree_snap_meta_t                       *snap_meta;
	ZS_status_t				status;

    if (_ZSInitPerThreadState(zs_state, &thd_state) != ZS_SUCCESS) {
		return NULL;
	}
	cguids = (ZS_cguid_t *) malloc(sizeof(*cguids) * MAX_OPEN_CONTAINERS);
	if (cguids == NULL) {
		goto out;
	}

	//Get the number of containers in delete state on the device.
	if ((status = ZSGetBtDelContainers(thd_state, cguids, &ncg)) != ZS_SUCCESS) {
		goto out;
	}

	for (i = 0; i < ncg; i++) {
		
		if ((status = ZSGetContainerProps(thd_state, cguids[i], &props)) != ZS_SUCCESS) {
			continue;
		}

		if (ZSOpenContainer(thd_state, props.name, &props, 
					ZS_CTNR_RW_MODE, &cguid) == ZS_SUCCESS) {

			if (cguids[i] == cguid) {
restart:
				status = ZSReadObject(thd_state, cguid, (char *)&nodeid,
						sizeof(uint64_t), &node, &node_size);
				if (status == ZS_SUCCESS) {
					snap_meta = (btree_snap_meta_t *)node;
					if (snap_meta->sc_status == SC_OVERFLW_DELCONT) {
						ZSFreeBuffer(node);
						ZSCloseContainer(thd_state, cguid);
						fprintf(stderr, "Restarting deletion of container: %s\n", props.name);
						status = _ZSOpenContainer(thd_state, props.name,
								&props, ZS_CTNR_RW_MODE, &cguid);
						fprintf(stderr, "Restarting deletion of container: %s %s\n", props.name, ZSStrError(status));
						assert(status == ZS_FAILURE_CONTAINER_DELETED);
					} else {
						snap_meta->sc_status = SC_OVERFLW_DELCONT;
						status = ZSWriteObject(thd_state, cguid, (char *)&nodeid,
									sizeof(uint64_t), (char *)node, node_size, 0);
						ZSFreeBuffer(node);
						if (status == ZS_SUCCESS) {
							goto restart;
						}
					}

				}
			}
		}

	}

out:
	if (cguids) {
		free(cguids);
	}
	_ZSReleasePerThreadState(&thd_state);
	return NULL;
}

//========================== btree defragmenter =======================
// TO BE USED WITH STORM in TESSELLATION and COMPRESSION mode

#define DATA_SEPARATOR "\1\1" // change this whenever data key format changes.
                  // currently <key><DATA_SEPARATOR><start offset><end offset>
static int maxosz = 16; //max offset size
static uint64_t mcsz = 64000; //max overflow node usage
static uint64_t ofminusage = 32000; // min overflow node usage
static __thread struct ZS_thread_state *defrag_ts;
static pthread_t defrag_thd = (pthread_t)NULL;
static bool stop_defrag = false;

pthread_cond_t defrag_cv = PTHREAD_COND_INITIALIZER;
pthread_mutex_t defrag_mutex = PTHREAD_MUTEX_INITIALIZER;

extern unsigned long int getProperty_uLongInt(const char *key,
                            const unsigned long int defaultVal);
extern btree_status_t get_leaf_data_nth_key(btree_raw_t *bt, btree_raw_node_t *n, int index,
                        btree_metadata_t *meta, uint64_t syndrome,
                        char **data, uint64_t *datalen, int ref);

/* _ZSMerge - merges two keys and associated data. */
static ZS_status_t  
_ZSMerge(struct ZS_thread_state *zs_thread_state,
         ZS_cguid_t cguid,
         char *skey, // merged key
         uint32_t skeylen, //merged key length
         char *sdata, //merged data
         uint64_t sdatalen, //merged data length
         char *dkey1, //first key
         char *dkey2) //second key
{
    ZS_status_t ret = ZS_FAILURE;
    btree_status_t btree_ret = BTREE_FAILURE;
    btree_metadata_t meta;
    uint64_t flash_space_used;
    struct btree *bt;
    int index;

    if (__zs_check_mode_on) {
        /*
         * We are in checker mode so do not allow any new
         * write or update in objects.
         */
        return ZS_FAILURE_OPERATION_DISALLOWED;
    }

    my_thd_state = zs_thread_state;;

    if (bt_is_license_valid() == false) {
        return (ZS_LICENSE_CHK_FAILED);
    }
    if ((ret= bt_is_valid_cguid(cguid)) != ZS_SUCCESS) {
        return ret;
    }

    pthread_rwlock_rdlock(&(Container_Map[cguid].bt_cm_rwlock));
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cguid].flags) ) {
        pthread_rwlock_unlock(&(Container_Map[cguid].bt_cm_rwlock));
        return ret;
    }
    pthread_rwlock_unlock(&(Container_Map[cguid].bt_cm_rwlock));

    bt = bt_get_btree_from_cguid(cguid, &index, &ret, true);
    if (bt == NULL) {
        return (ret);
    }

    if (Container_Map[index].read_only == true) {
        bt_rel_entry(index, true);
        return ZS_FAILURE;
    }
    if (skeylen > bt->max_key_size) {
        bt_rel_entry(index, true);
        return (ZS_KEY_TOO_LONG);
    }

    if (sdatalen > BTREE_MAX_DATA_SIZE_SUPPORTED) {
        bt_rel_entry(index, true);
        return (ZS_OBJECT_TOO_BIG);
    }

    if (storage_space_exhausted( "ZSMerge")) {
        bt_rel_entry(index, true);
        return ZS_OUT_OF_STORAGE_SPACE;
    }

    meta.flags = 0;
    __sync_add_and_fetch(&(bt->partitions[0]->stats.stat[BTSTAT_WRITE_CNT]),1);
    trxenter( cguid);
    btree_ret = btree_set(bt, skey, skeylen, sdata, sdatalen, &meta);
    ret = BtreeErr_to_ZSErr(btree_ret);
    if (ZS_SUCCESS != ret){
        trxleave( cguid);
        bt_rel_entry(index, true);
        return(ret);
    }
    meta.flags = FORCE_DELETE;
    btree_ret = btree_delete(bt, dkey1, skeylen, &meta);
    ret = BtreeErr_to_ZSErr(btree_ret);
    if (ZS_SUCCESS != ret){
        trxleave( cguid);
        bt_rel_entry(index, true);
        return(ret);
    }

    meta.flags = FORCE_DELETE;
    btree_ret = btree_delete(bt, dkey2, skeylen, &meta);
    trxleave( cguid);

    ret = BtreeErr_to_ZSErr(btree_ret);
    bt_rel_entry(index, true);
    return(ret);
}

static ZS_status_t
_ZSDefragContainer(struct ZS_thread_state *ts,
                   ZS_cguid_t cid )
{
#if 0
    ZS_status_t ret = ZS_FAILURE;
    struct btree *bt;
    int index;
    uint64_t i;
    int write_lock = 0;
    struct btree_raw_mem_node *node;
    btree_raw_t *btree;
    uint64_t child_id;
    btree_raw_mem_node_t *parent;
    key_stuff_t ks_current_1;
    btree_status_t bret = BTREE_SUCCESS;
    key_meta_t key_meta;
    key_info_t key_info1, key_info2;
    char *tmp=NULL, orig_key[256];

    if ((ret= bt_is_valid_cguid(cid)) != ZS_SUCCESS) {
        return ret;
    }

    pthread_rwlock_rdlock(&(Container_Map[cid].bt_cm_rwlock));
    if (true == IS_ZS_HASH_CONTAINER(Container_Map[cid].flags)) {
        pthread_rwlock_unlock(&(Container_Map[cid].bt_cm_rwlock));
        return ZS_SUCCESS;
    }
    pthread_rwlock_unlock(&(Container_Map[cid].bt_cm_rwlock));

    bt = bt_get_btree_from_cguid(cid, &index, &ret, false);
    if (bt == NULL) {
        return ret;
    }

    assert(bt->n_partitions == 1); //assuming only one partition per container

    btree = bt->partitions[0];
    assert(btree);
    open_container(ts, cid);
    node = root_get_and_lock(btree, write_lock, &bret);

    while(!is_leaf(btree, (node)->pnode)) {      
        (void) get_key_stuff(btree, (node)->pnode, 0, &ks_current_1);
        child_id = ks_current_1.ptr;  
        parent = node;
        node = get_existing_node_low(&bret, btree, child_id, NODE_CACHE_DEREF_DELETE); 
        assert(node);
        if (is_leaf(btree, (node)->pnode)) {
            node_lock(node, true);  
        } else {
            node_lock(node, false);   
        }
        node_unlock(parent);
        deref_l1cache_node(btree, parent);
    }

    while (1) { //traverse all leaf node
        for (i = 0; i <( node->pnode->nkeys ? (node->pnode->nkeys-1) : 0); i++) {
            key_info1.keylen = 0;
            key_info1.datalen = 0;
            key_info1.key = NULL;
            assert(true == btree_leaf_get_nth_key_info(btree, node->pnode, i, &key_info1));

            if((key_info1.keylen + key_info1.datalen) < btree->big_object_size) {
                if(key_info1.key) free(key_info1.key);
                continue;
            }
            if(key_info1.datalen > ofminusage) {
                if(key_info1.key) free(key_info1.key);
                continue;
            }
            memset(orig_key, 0, 256);
            assert(key_info1.keylen < 256);
            strncpy(orig_key, key_info1.key, key_info1.keylen);
            if(strstr(orig_key, DATA_SEPARATOR) == NULL) {
                if(key_info1.key) free(key_info1.key);
                continue;
            }
            
            key_info2.keylen = 0;
            key_info2.datalen = 0;
            key_info2.key = NULL;
            assert(true == btree_leaf_get_nth_key_info(btree, node->pnode, i+1, &key_info2));
            if((key_info2.keylen + key_info2.datalen) < btree->big_object_size) {
                if(key_info1.key) free(key_info1.key);
                if(key_info2.key) free(key_info2.key);
                i++;
                continue;
            }
            if(key_info2.datalen > ofminusage) {
                if(key_info1.key) free(key_info1.key);
                if(key_info2.key) free(key_info2.key);
                i++;
                continue;
            }
            memset(orig_key, 0, 256);
            assert(key_info2.keylen < 256);
            strncpy(orig_key, key_info2.key, key_info2.keylen);
            if(strstr(orig_key, DATA_SEPARATOR) == NULL) {
                if(key_info1.key) free(key_info1.key);
                if(key_info2.key) free(key_info2.key);
                i++;
                continue;
            }

            if (memcmp(key_info1.key, key_info2.key, (key_info1.keylen - (2*maxosz))) != 0) {
                if(key_info1.key) free(key_info1.key);
                if(key_info2.key) free(key_info2.key);
                i++;
                continue;
            }

            uint64_t so1,so2,eo1,eo2;
            char *chk = (char *) malloc((maxosz+1)*sizeof(char));  
            assert(chk != NULL);
            memset(chk, 0, maxosz+1);
            memcpy(chk, key_info1.key + (key_info1.keylen-(2*maxosz)), maxosz);
            so1 = atoll(chk);
            memset(chk, 0, maxosz+1);
            memcpy(chk, (key_info1.key + (key_info1.keylen-maxosz)), maxosz);
            eo1 = atoll(chk);
            memset(chk, 0, maxosz+1);
            memcpy(chk, (key_info2.key + (key_info2.keylen-(2*maxosz))), maxosz);
            so2 = atoll(chk);
            memset(chk, 0, maxosz+1);
            memcpy(chk, (key_info2.key + (key_info2.keylen-maxosz)), maxosz);
            eo2 = atoll(chk);
            if(chk) free(chk); 

            char *mdata = (char *) malloc ((eo2 - so1 + 1) * sizeof(char));
            memset(mdata, 0, (eo2 - so1 + 1));

            char *tmp_data1 = NULL;
            uint64_t tmp_datalen1 = 0;
            btree_metadata_t meta;
            meta.flags = 0;
            assert(BTREE_SUCCESS == get_leaf_data_nth_key(btree, node->pnode, i, &meta, 0,
                        &tmp_data1, &tmp_datalen1, 0));
            int dclen = eo1 - so1 + 1;
            char *uc = (char *) calloc(dclen, sizeof(char));
            assert(uc != NULL);
            int ucsz = LZ4_decompress_safe(tmp_data1, uc, tmp_datalen1, dclen);
            assert(0 != ucsz);
            assert(ucsz == dclen);
            memcpy(mdata, uc, dclen);  
            if(uc) free(uc);
            if(tmp_data1) free(tmp_data1);

            char *tmp_data2 = NULL;
            uint64_t tmp_datalen2 = 0;
            assert(BTREE_SUCCESS == get_leaf_data_nth_key(btree, node->pnode, i+1, &meta, 0,
                        &tmp_data2, &tmp_datalen2, 0));
            dclen = eo2 - so2 + 1;  
            uc = (char *) calloc(dclen, sizeof(char));
            assert(uc != NULL);
            ucsz = LZ4_decompress_safe(tmp_data2, uc, tmp_datalen2, dclen); 
            assert(0 != ucsz);
            assert(ucsz == dclen);
            memcpy(mdata + so2 - so1,uc, dclen);  
            if(uc) free(uc);
            if(tmp_data2) free(tmp_data2);

            //try write
            uint64_t bound = LZ4_compressBound(mcsz);    
            char *out = (char *) calloc(bound, sizeof(char));
            assert(out != NULL);
            uint64_t mdatalen = eo2 - so1 + 1;     
            uint64_t olen = LZ4_compress( mdata, out, mdatalen);
            if(olen == 0) {
                if(key_info1.key) free(key_info1.key);
                if(key_info2.key) free(key_info2.key);
                if(mdata) free(mdata);
                if(out) free(out); 
                continue;
            }
            char *ton = (char *) malloc((key_info1.keylen + 1) * sizeof(char));
            memset(ton, 0, key_info1.keylen + 1);
            memcpy(ton, key_info1.key, key_info1.keylen);
            sprintf(ton+key_info1.keylen-(2*maxosz), "%016lu%016lu", so1, eo2);

            node_unlock(node);
            deref_l1cache_node(btree, node);
            bt_rel_entry(index, false);
            assert(ZS_SUCCESS == _ZSMerge(ts, cid, ton, key_info1.keylen, 
                        out, olen, key_info1.key, key_info2.key));
            if(key_info1.key) free(key_info1.key);
            if(key_info2.key) free(key_info2.key);
            if(mdata) free(mdata);
            if(out) free(out);
            if(ton) free(ton);
            return ZS_SUCCESS;
        }

        child_id = node->pnode->next;
        parent = node;

        if (child_id == 0) {
            node_unlock(parent);
            deref_l1cache_node(btree, parent);
            break;
        } else {
            node =  get_existing_node_low(&bret, btree, child_id, NODE_CACHE_DEREF_DELETE);
            if (node == NULL) {
                node_unlock(parent);
                deref_l1cache_node(btree, parent);
                break;
            } else {
                node_lock(node, true);
                node_unlock(parent);
                deref_l1cache_node(btree, parent);
            }
        }
    }
    bt_rel_entry(index, false);
#endif
    return ZS_SUCCESS;
}

static void *defrag_thd_hdlr(void *arg)
{
    struct timespec abstime;
    void *retval = NULL;

    uint64_t defrag_sleep_sec = getProperty_uLongInt(
            "ZS_DEFRAG_INTERVAL", 60); // 1 minute
    ofminusage = getProperty_uLongInt("ZS_DEFRAG_OFMINSZ", 32000);

    if(ZS_SUCCESS != _ZSInitPerThreadState(( struct ZS_state * ) arg,
                ( struct ZS_thread_state ** )&defrag_ts)) {
        pthread_exit(retval);
    }

    ZS_cguid_t cguids[MAX_OPEN_CONTAINERS];
    uint32_t ncguids = 0;
    int i;
    while(1){
        if ( stop_defrag == true){
            pthread_mutex_lock(&defrag_mutex);
            clock_gettime(CLOCK_REALTIME, &abstime);
            abstime.tv_sec += 31536000; //yield for an year
            pthread_cond_timedwait(&defrag_cv, &defrag_mutex, &abstime);
            pthread_mutex_unlock(&defrag_mutex);
        }

        if (ZS_SUCCESS == _ZSGetContainers(defrag_ts, cguids, &ncguids)){
            for(i = 0; i < ncguids;i++){
                _ZSDefragContainer(defrag_ts, cguids[i]);
                pthread_mutex_lock(&defrag_mutex);
                clock_gettime(CLOCK_REALTIME, &abstime);
                abstime.tv_sec += defrag_sleep_sec;
                pthread_cond_timedwait(&defrag_cv, &defrag_mutex, &abstime);
                pthread_mutex_unlock(&defrag_mutex);

            }
        }
        pthread_mutex_lock(&defrag_mutex);
        clock_gettime(CLOCK_REALTIME, &abstime);
        abstime.tv_sec += defrag_sleep_sec;
        pthread_cond_timedwait(&defrag_cv, &defrag_mutex, &abstime);
        pthread_mutex_unlock(&defrag_mutex);
    }
}

ZS_status_t 
zs_start_defrag_thread(struct ZS_state *zs_state )
{
    stop_defrag = false;

    if( zs_state == NULL ) {
        return ZS_FAILURE;
    }

    if((pthread_t)NULL == defrag_thd) {
        if( 0 == pthread_create(&defrag_thd,NULL,
                    defrag_thd_hdlr,(void *)zs_state)){
            return ZS_SUCCESS;
        }
        return ZS_FAILURE;
    } else {
        if(0 == pthread_cond_broadcast(&defrag_cv)) {
            return ZS_SUCCESS;
        } else {
            return ZS_FAILURE;
        }
    }
}

ZS_status_t 
zs_stop_defrag_thread()
{
    stop_defrag = true;
    return ZS_SUCCESS;
}

void *
zscheck_worker(void *arg)
{
    ZS_cguid_t cguid = 0;
    ZS_container_props_t props = {0};
    ZS_status_t status = ZS_FAILURE;
    int i = 0;
    char err_msg[1024];
    struct ZS_thread_state *my_thread_state = NULL;

    status = _ZSInitPerThreadState(ZSState, &my_thread_state);
    if (ZS_SUCCESS != status) {
        ZSCheckMsg(ZSCHECK_BTREE_NODE, 0, ZSCHECK_FAILURE, "Failed to init ZS btree check worker");
        return 0;
    }

    if (seqnoread(my_thread_state) != true) {
        ZSCheckMsg(ZSCHECK_BTREE_NODE, 0, ZSCHECK_FAILURE, "Failed to init seqno for ZS btree check worker");
        //return 0;
    }

    while (cguid_idx < ncguids) {

        pthread_mutex_lock(&cguid_idx_lock);
        i = cguid_idx;
        ++cguid_idx;
        pthread_mutex_unlock(&cguid_idx_lock);

        status = ZSGetContainerProps(my_thread_state, cguids[i], &props);
        if (status != ZS_SUCCESS) {
            sprintf(err_msg, "Btree check failed to get properties for container %lu: %s",
                    cguids[i], ZSStrError(status));
            ZSCheckMsg(ZSCHECK_BTREE_NODE, cguids[i], ZSCHECK_FAILURE, err_msg);
            continue;
        }

        // Open the container
        status = _ZSOpenContainer(my_thread_state, props.name, &props,
                     ZS_CTNR_RW_MODE, &cguid);
        if (status != ZS_SUCCESS) {
            sprintf(err_msg, "Btree check failed to open container %s: %s",
                    props.name, ZSStrError(status));
            ZSCheckMsg(ZSCHECK_BTREE_NODE, cguids[i], ZSCHECK_FAILURE, err_msg);
            continue;
        }

        if (IS_ZS_HASH_CONTAINER(props.flags) == true) {
            /*
             * It is hash containers, so check all objs by enumeration.
             */
            status = check_hash_cont(my_thread_state, cguids[i]);
            if (status != ZS_SUCCESS) {
                sprintf(err_msg, "Btree check failed for container %s: %s",
                        props.name, ZSStrError(status));
                ZSCheckMsg(ZSCHECK_BTREE_NODE, cguids[i], ZSCHECK_FAILURE, err_msg);
                continue;
            }
        } else {
            /*
             * If btree container, then check btre
             */
            status = _ZSCheckBtree(my_thread_state, cguids[i], 0);
            if (status != ZS_SUCCESS) {
                sprintf(err_msg, "Btree check failed for container %s: %s", props.name, ZSStrError(status));
                ZSCheckMsg(ZSCHECK_BTREE_NODE, cguids[i], ZSCHECK_FAILURE, err_msg);
                continue;
            }
            sprintf(err_msg, "Btree check successful for container %s", props.name);
            ZSCheckMsg(ZSCHECK_BTREE_NODE, cguids[i], ZSCHECK_SUCCESS, err_msg);
        }

        msg("Data consistency check passed for cguid %s.\n", props.name);
    }

    ZSReleasePerThreadState(&my_thread_state);

    return 0;
}



