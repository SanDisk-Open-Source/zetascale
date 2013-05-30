/************************************************************************
 * 
 *  btree_raw.c  Jan. 21, 2013   Brian O'Krafka
 * 
 * xxxzzz NOTES:
 *     - check all uses of "ret"
 *     - make sure that all btree updates are logged!
 *     - add doxygen comments for all functions
 *     - make asserts a compile time option
 *     - make sure that left/right node pointers are properly maintained
 *     - check insert_ptr arithmetic
 *     - optimize key search within a node?
 *     - how is logical_id_counter persisted?
 *     - where is rightmost updated?
 *     - what if multiple matches for a particular syndrome?  must be
 *       be able to return multiple matches--NO--MUST JUST CHECK MULTIPLE
 *       MATCHES INTERNALLY TO FIND EXACT MATCH
 *     - is there a need to support non-uniqueue keys?
 *       if not, must enforce uniqueness somehow
 *     - add upsert flag and support
 *     - add check that keylen/datalen are correct when using 
 *       size fixed keys/data
 *     - if btree_raw provides returned data/key buffer, a special btree_raw_free_buffer() call
 *       should be used to free the buffer(s).  This will allow optimized buffer management
 *       allocation/deallocation methods (that avoid malloc) in the future.
 *     - modularize l1cache stuff
 *     - add free buffer callback
 *     - add get buffer callback
 *     - optimize updates to manipulate btree node in a single operation
 *     - if updates decrease node size below a threshold, must coalesce nodes!!!
 *     - stash overflow key in overflow node(s)!
 *     - use "right-sized" FDF objects for overflow objects, without chaining fixed
 *       sized nodes!
 *     - add 'leftmost' pointers for use with leaf nodes for reverse scans and
 *       simplified update of 'rightmost' pointers
 *     - where is max key length enforced?
 *     - add stats
 *     - add upsert (set) function to btree_raw_write()
 *     - change chunk size in DRAM cache code to match btree node size!
 *     - improve object packing in b-tree nodes
 * 
 * Flavors of b-tree:
 *     - Syndrome search + variable sized keys with variable sized data (primary index)
 *       ((btree->flags & SYNDROME_INDEX) == 0)
 *         - non-leaf nodes: fixed length syndrom, no data
 *         - leaf nodes: fixed length syndrom + variable length key + variable length data
 *     - Variable sized keys with variable sized data (secondary indices)
 *       ((btree->flags & SECONDARY_INDEX) == 0)
 *         - non-leaf nodes: fixed length key, no data
 *         - leaf nodes: fixed length syndrom + variable length key + variable length data
 * 
 ************************************************************************/

//  This instantiates the stats string array
#define _INSTANTIATE_BTSTATS_STRINGS

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <inttypes.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include "btree_hash.h"
#include "btree_raw.h"
#include "btree_map.h"
#include "btree_raw_internal.h"

//  Define this to include detailed debugging code
//#define DEBUG_STUFF
//#define BTREE_RAW_CHECK

#define W_UPDATE  1
#define W_CREATE  2
#define W_SET     3

#define MODIFY_TREE 1

//  used to count depth of btree traversal for writes/deletes
__thread int _pathcnt;

//  used to hold key values during delete or write operations
__thread char      *_keybuf      = NULL;
__thread uint32_t   _keybuf_size = 0;

static int Verbose = 0;

#define bt_err(msg, args...) \
    (bt->msg_cb)(0, 0, __FILE__, __LINE__, msg, ##args)

#define zmemcpy(to_in, from_in, n_in)  \
{\
    uint64_t zi;\
    uint64_t zn = (n_in);\
    char  *zto = ((char *) to_in);\
    char  *zfrom = ((char *) from_in);\
    for (zi=0; zi<zn; zi++) {\
        *zto++ = *zfrom++;\
    }\
}

#define zmemmove(to_in, from_in, n_in)  \
{\
    uint64_t zi;\
    uint64_t zn = (n_in);\
    char  *zto = ((char *) to_in);\
    char  *zfrom = ((char *) from_in);\
    if (zto < zfrom) {\
	for (zi=0; zi<zn; zi++) {\
	    *zto++ = *zfrom++;\
	}\
    } else {\
        zto   += zn;\
        zfrom += zn;\
	for (zi=0; zi<zn; zi++) {\
	    *(--zto) = *(--zfrom);\
	}\
    }\
}

//static void print_key(FILE *f, char* key, int keylen, char *msg, ...);

static uint64_t get_syndrome(btree_raw_t *bt, char *key, uint32_t keylen);
static char *dump_key(char *key, uint32_t keylen);

static int savepersistent( btree_raw_t *);
static int loadpersistent( btree_raw_t *);
static char *get_buffer(btree_raw_t *btree, uint64_t nbytes);
static void free_buffer(btree_raw_t *btree, char *buf);
static void free_node(int *ret, btree_raw_t *btree, btree_raw_node_t *n);

static btree_raw_node_t *get_new_node(int *ret, btree_raw_t *btree, uint32_t leaf_flags);
static btree_raw_node_t *get_new_node_low(int *ret, btree_raw_t *btree, uint32_t leaf_flags, int ref);
btree_raw_node_t *get_existing_node_low(int *ret, btree_raw_t *btree, uint64_t logical_id, plat_rwlock_t** lock, int ref);

static int init_l1cache(btree_raw_t *btree, uint32_t n_l1cache_buckets);
static int deref_l1cache(btree_raw_t *btree);
static void deref_l1cache_node(btree_raw_t* btree, btree_raw_node_t *node);
static int add_l1cache(btree_raw_t *btree, btree_raw_node_t *n, plat_rwlock_t** lock);
static void ref_l1cache(btree_raw_t *btree, btree_raw_node_t *n);
static btree_raw_node_t *get_l1cache(btree_raw_t *btree, uint64_t logical_id, plat_rwlock_t** lock);
static void delete_l1cache(btree_raw_t *btree, btree_raw_node_t *n);

static void dump_node(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key, uint32_t keylen);
static void update_keypos(btree_raw_t *btree, btree_raw_node_t *n, uint32_t n_key_start);

typedef struct deldata {
    btree_raw_node_t   *balance_node;
} deldata_t;

static void delete_key(int *ret, btree_raw_t *btree, btree_raw_node_t *x, char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome);
static void delete_key_by_pkrec(int* ret, btree_raw_t *btree, btree_raw_node_t *x, node_key_t *pk_delete, int modify_tree);
static int btree_raw_write(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta, int write_type);

static int find_rebalance(int *ret, btree_raw_t *btree, uint64_t this_id, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent_in, int r_this_parent_in, char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome);
static void collapse_root(int *ret, btree_raw_t *btree, btree_raw_node_t *old_root_node);
static int rebalance(int *ret, btree_raw_t *btree, btree_raw_node_t *this_node, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent, int r_this_parent, btree_metadata_t *meta);
static void merge_right(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno);
static void merge_left(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno);
static void shift_right(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno, char **r_key_out, uint32_t *r_keylen_out, uint64_t *r_syndrome_out, uint64_t *r_seqno_out);
static void shift_left(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno, char **r_key_out, uint32_t *r_keylen_out, uint64_t *r_syndrome_out, uint64_t *r_seqno_out);

static int check_per_thread_keybuf(btree_raw_t *btree);
static void btree_raw_init_stats(struct btree_raw *btree, btree_stats_t *stats);
#ifdef DEBUG_STUFF
static void btree_raw_dump(FILE *f, struct btree_raw *btree);
#endif

#ifdef BTREE_RAW_CHECK
static void btree_raw_check(struct btree_raw *btree, char* func, char* key);
#endif

static void default_msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...)
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
        exit(1);
    }
}

static int default_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
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

//======================   INIT  =========================================

static void l1cache_replace(void *callback_data, char *key, uint32_t keylen, char *pdata, uint64_t datalen)
{
    btree_raw_mem_node_t *n = (btree_raw_mem_node_t*)pdata;

    free_buffer((btree_raw_t *) callback_data, (void*)n->node);

    plat_rwlock_destroy(&n->lock);
    free(n);
}

btree_raw_t *btree_raw_init(uint32_t flags, uint32_t n_partition, uint32_t n_partitions, uint32_t max_key_size, uint32_t min_keys_per_node, uint32_t nodesize, uint32_t n_l1cache_buckets, create_node_cb_t *create_node_cb, void *create_node_data, read_node_cb_t *read_node_cb, void *read_node_cb_data, write_node_cb_t *write_node_cb, void *write_node_cb_data, freebuf_cb_t *freebuf_cb, void *freebuf_cb_data, delete_node_cb_t *delete_node_cb, void *delete_node_data, log_cb_t *log_cb, void *log_cb_data, msg_cb_t *msg_cb, void *msg_cb_data, cmp_cb_t *cmp_cb, void * cmp_cb_data)
{
    btree_raw_t      *bt;
    uint32_t          nbytes_meta;
    int               ret = 0;

    bt = (btree_raw_t *) malloc(sizeof(btree_raw_t));
    if (bt == NULL) {
        return(NULL);
    }

    if (init_l1cache(bt, n_l1cache_buckets)) {
        return(NULL);
    }

    if (flags & VERBOSE_DEBUG) {
        Verbose = 1;
    }

    btree_raw_init_stats(bt, &(bt->stats));

    bt->n_partition          = n_partition;
    bt->n_partitions         = n_partitions;
    bt->flags                = flags;
    bt->max_key_size         = max_key_size;
    bt->min_keys_per_node    = min_keys_per_node;;
    bt->nodesize             = nodesize;
    bt->nodesize_less_hdr    = nodesize - sizeof(btree_raw_node_t);
    // bt->big_object_size      = (nodesize - sizeof(btree_raw_node_t))/2; // xxxzzz check this
    bt->big_object_size      = (nodesize - sizeof(btree_raw_node_t))/4 - sizeof(node_vlkey_t); // xxxzzz check this
    bt->logical_id_counter   = 1;
    bt->create_node_cb       = create_node_cb;
    bt->create_node_cb_data  = create_node_data;
    bt->read_node_cb         = read_node_cb;
    bt->read_node_cb_data    = read_node_cb_data;
    bt->write_node_cb        = write_node_cb;
    bt->write_node_cb_data   = write_node_cb_data;
    bt->freebuf_cb           = freebuf_cb;
    bt->freebuf_cb_data      = freebuf_cb_data;
    bt->delete_node_cb       = delete_node_cb;
    bt->delete_node_cb_data  = delete_node_data;
    bt->log_cb               = log_cb;
    bt->log_cb_data          = log_cb_data;
    bt->msg_cb               = msg_cb;
    bt->msg_cb_data          = msg_cb_data;
    if (msg_cb == NULL) {
	bt->msg_cb           = default_msg_cb;
	bt->msg_cb_data      = NULL;
    }
    bt->cmp_cb               = cmp_cb;
    bt->cmp_cb_data          = cmp_cb_data;
    if (cmp_cb == NULL) {
	bt->cmp_cb           = default_cmp_cb;
	bt->cmp_cb_data      = NULL;
    }

    if (min_keys_per_node < 4) {
	bt_err("min_keys_per_node must be >= 4");
        free(bt);
	return(NULL);
    }

    bt->fkeys_per_node = (nodesize - sizeof(btree_raw_node_t))/sizeof(node_fkey_t);

    nbytes_meta = sizeof(node_vkey_t);
    if (nbytes_meta < sizeof(node_vlkey_t)) {
        nbytes_meta = sizeof(node_vlkey_t);
    }
    nbytes_meta += max_key_size;
    nbytes_meta *= min_keys_per_node;
    nbytes_meta += sizeof(btree_raw_node_t);

    if (nodesize < nbytes_meta) {
	bt_err("Node size (%d bytes) must be large enough to hold at least %d max sized keys (%d bytes each).", nodesize, min_keys_per_node, max_key_size);
        free(bt);
	return(NULL);
    }

    if (flags & RELOAD) {
        if (! loadpersistent( bt)) {
            bt_err( "Could not identify root node!");
            free( bt);
            return (NULL);
        }
    }
    else {
        btree_raw_node_t *root_node = get_new_node( &ret, bt, LEAF_NODE);
        if (ret) {
            bt_err( "Could not allocate root node!");
            free( bt);
            return (NULL);
        }
        bt->rootid = root_node->logical_id;
        if (! savepersistent( bt)) {
            free( bt);
            return (NULL);
        }
    }
    if (deref_l1cache(bt)) {
        ret = 1;
    }

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    btree_raw_dump(stderr, bt);
	}
    #endif

    plat_rwlock_init(&bt->lock);

#ifdef DEBUG_STUFF
    if(Verbose)
	fprintf(stderr, "%x %s bt %p lock %p n_part %d\n", (int)pthread_self(), __FUNCTION__, bt, &bt->lock, n_partition);
#endif

    return(bt);
}

/*
 * save persistent btree metadata
 *
 * Info is stored as a btree node with special logical ID.  Return 1 on
 * success, otherwise 0.
 */
static int
savepersistent( btree_raw_t *bt)
{
	int	ret;
	char	buf[bt->nodesize];

	if (bt->flags & IN_MEMORY) {
		return (1);
	}

	btree_raw_persist_t *r = (void *) buf;
	r->logical_id_counter = bt->logical_id_counter;
    	r->rootid = bt->rootid;
	(*bt->write_node_cb)( &ret, bt->write_node_cb_data, META_LOGICAL_ID+bt->n_partition, buf, sizeof buf);
	if (ret) {
		bt_err( "Could not persist btree!");
		return (0);
	}
	return (1);
}

/*
 * load persistent btree metadata
 *
 * Info is stored as a btree node with special logical ID.  Return 1 on
 * success, otherwise 0.
 */
static int
loadpersistent( btree_raw_t *bt)
{
	int	ret;

	btree_raw_node_t *n = (*bt->read_node_cb)( &ret, bt->read_node_cb_data, META_LOGICAL_ID+bt->n_partition);
	if (ret)
		return (0);
	btree_raw_persist_t *r = (void *) n;
	bt->logical_id_counter = r->logical_id_counter;
    	bt->rootid = r->rootid;
	free( n);
	return (1);
}

int btree_raw_free_buffer(btree_raw_t *btree, char *buf)
{
    free_buffer(btree, buf);
    return(0);
}

//======================   GET  =========================================

static int is_root(btree_raw_t *btree, btree_raw_node_t *node) { return btree->rootid == node->logical_id; }

int is_leaf(btree_raw_t *btree, btree_raw_node_t *node)
{
    if (node->flags & LEAF_NODE) {
        return(1);
    } else {
        return(0);
    }
}

int get_key_stuff(btree_raw_t *bt, btree_raw_node_t *n, uint32_t nkey, key_stuff_t *pks)
{
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    node_fkey_t   *pfk;
    int            leaf = 0;

    pks->nkey = nkey;
    if (bt->flags & SECONDARY_INDEX) {
        pks->fixed = 0;
        //  used for secondary indices
	if (n->flags & LEAF_NODE) {
	    leaf               = 1;
	    pvlk               = ((node_vlkey_t *) n->keys) + nkey;
	    pks->ptr           = pvlk->ptr;
	    pks->offset        = sizeof(node_vlkey_t);
	    pks->pkey_struct   = (void *) pvlk;
	    pks->pkey_val      = (char *) n + pvlk->keypos;
	    pks->keylen        = pvlk->keylen;
	    pks->datalen       = pvlk->datalen;
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvlk->seqno;
	    pks->syndrome      = pvlk->syndrome;
	} else {
	    pvk                = ((node_vkey_t *) n->keys) + nkey;
	    pks->ptr           = pvk->ptr;
	    pks->offset        = sizeof(node_vkey_t);
	    pks->pkey_struct   = (void *) pvk;
	    pks->pkey_val      = (char *) n + pvk->keypos;
	    pks->keylen        = pvk->keylen;
	    pks->datalen       = sizeof(uint64_t);
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvk->seqno;
	    pks->syndrome      = 0;
	}
    } else if (bt->flags & SYNDROME_INDEX) {
        //  used for primary indices
	if (n->flags & LEAF_NODE) {
	    leaf               = 1;
	    pvlk               = ((node_vlkey_t *) n->keys) + nkey;
	    pks->fixed         = 0;
	    pks->ptr           = pvlk->ptr;
	    pks->offset        = sizeof(node_vlkey_t);
	    pks->pkey_struct   = (void *) pvlk;
	    pks->pkey_val      = (char *) n + pvlk->keypos;
	    pks->keylen        = pvlk->keylen;
	    pks->datalen       = pvlk->datalen;
	    pks->fkeys_per_node = 0;
	    pks->seqno         = pvlk->seqno;
	    pks->syndrome      = pvlk->syndrome;
	} else {
	    pfk                = ((node_fkey_t *) n->keys) + nkey;
	    pks->fixed         = 1;
	    pks->ptr           = pfk->ptr;
	    pks->offset        = sizeof(node_fkey_t);
	    pks->pkey_struct   = (void *) pfk;
	    pks->pkey_val      = (char *) (pfk->key);
	    pks->keylen        = sizeof(uint64_t);
	    pks->datalen       = sizeof(uint64_t);
	    pks->fkeys_per_node = bt->fkeys_per_node;
	    pks->seqno         = pfk->seqno;
	    pks->syndrome      = pfk->key;
	}
    } else {
        assert(0);
    }
    pks->leaf = leaf;
    return(leaf);
}

/*
 *  Returns: key structure which matches 'key', if one is found; NULL otherwise
 *           'pk_insert' returns a pointer to the key struct that would FOLLOW 'key' on 
 *           an insertion into this node. If 'pk_insert' is NULL, 'key' must be
 *           inserted at end of node, or key is already in node.
 *
 *  If node is NOT a leaf node, these 3 values are returned:
 *    child_id:        id of child node that may contain this key
 *    child_id_before: id of child sibling before 'child_id'
 *    child_id_after:  id of child sibling after 'child_id'
 *
 *    If the before or after siblings don't exist, BAD_CHILD is returned.
 *
 *    nkey_child returns the index into the key array of the key entry corresponding
 *    to 'child_id' (for a non-leaf), or for the matching record (for a leaf).  
 *    If 'child_id' corresponds to the rightmost pointer, nkey_child is
 *    set to n->nkeys.  If there is no valid child_id (nonleaf) or matching record (leaf),
 *    nkey_child is set to -1.
 */

static node_key_t *find_key(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in, uint64_t *child_id, uint64_t *child_id_before, uint64_t *child_id_after, node_key_t **pk_insert, btree_metadata_t *meta, uint64_t syndrome, int32_t *nkey_child)
{
    int            i_start, i_end, i_check, x;
    int            i_check_old;
    node_key_t    *pk = NULL;
    uint64_t       id_child;
    key_stuff_t    ks;
    int            key_found = 0;

    if (n->nkeys == 0) {
        if (n->rightmost == 0) {
	    *child_id        = BAD_CHILD;
	    *nkey_child      = -1;
	    *child_id_before = BAD_CHILD;
	    *child_id_after  = BAD_CHILD;
	    *pk_insert       = NULL;
	} else {
	    // YES, this is really possible!
	    // For example, when the root is a leaf and overflows on an insert.
	    *child_id        = n->rightmost;
	    *nkey_child      = 0;
	    *child_id_before = BAD_CHILD;
	    *child_id_after  = BAD_CHILD;
	    *pk_insert       = NULL;
	}
        return(NULL);
    }

    i_start     = 0;
    i_end       = n->nkeys - 1;
    i_check     = (i_start + i_end)/2;
    i_check_old = i_check;
    while (1) {

        (void) get_key_stuff(bt, n, i_check, &ks);
	pk       = ks.pkey_struct;
	id_child = ks.ptr;

        if (ks.fixed) {
	    if (syndrome < ks.syndrome) {
		x = -1;
	    } else if (syndrome > ks.syndrome) {
		x = 1;
	    } else {
		x = 0;
	    }
	} else {
	    if (bt->flags & SYNDROME_INDEX) {
		if (syndrome < ks.syndrome) {
		    x = -1;
		} else if (syndrome > ks.syndrome) {
		    x = 1;
		} else {
		    x = 0;
		}
	    } else {
		x = bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in, ks.pkey_val, ks.keylen);
	    }
	}

        if ((x == 0) &&
            ((meta->flags & READ_SEQNO_LE) || 
             (meta->flags & READ_SEQNO_GT_LE)))
        {
            //  Must take sequence numbers into account

            if (meta->flags & READ_SEQNO_LE) {
                if (ks.seqno > meta->seqno_le) {
                    x = -1; // higher sequence numbers go BEFORE lower ones!
                }
            } else if (meta->flags & READ_SEQNO_LE) {
                if (ks.seqno > meta->seqno_le) {
                    x = -1; // higher sequence numbers go BEFORE lower ones!
                } else if (ks.seqno <= meta->seqno_gt) {
                    x = 1; // lower sequence numbers go AFTER lower ones!
                }
            } else {
                assert(0);
            }
        }

        if (x > 0) {
            //  key > pvk->key
            if (i_check == (n->nkeys-1)) {
                // key might be in rightmost child
                *child_id        = n->rightmost;
		*nkey_child      = n->nkeys;
		*child_id_before = id_child;
                *child_id_after  = BAD_CHILD;
                *pk_insert       = NULL;
                return(NULL);
            }
            i_start     = i_check + 1;
        } else if (x < 0) {
            //  key < pvk->key
            if (i_check == 0) {
                // key might be in leftmost child
                *child_id        = id_child;
		*nkey_child      = i_check;
                *child_id_before = BAD_CHILD;
		if (i_check == (n->nkeys-1)) {
		    *child_id_after = n->rightmost;
		} else {
		    (void) get_key_stuff(bt, n, i_check+1, &ks);
		    *child_id_after = ks.ptr;
		}
                *pk_insert      = (node_key_t *) n->keys;
                return(NULL);
            }
            i_end       = i_check;
        } else {
            //  key == pvk->key
	    key_found = 1;
        }

	i_check_old = i_check;
	i_check     = (i_start + i_end)/2;

	if (key_found || 
	    (i_check_old == i_check)) 
	{
	    //  this is the end of the search

	    *child_id   = id_child;
	    *nkey_child = i_check;
	    *pk_insert  = pk;

	    if (i_check == 0) {
		*child_id_before = BAD_CHILD;
	    } else {
		get_key_stuff(bt, n, i_check-1, &ks);
		*child_id_before = ks.ptr;
	    }

	    if (i_check >= (n->nkeys-1)) {
		if (x > 0) {
		    *child_id        = n->rightmost;
		    *child_id_after  = BAD_CHILD;
		    *nkey_child      = n->nkeys;
		    *pk_insert       = NULL;
		} else {
		    *child_id_after  = n->rightmost;
		}
	    } else {
		get_key_stuff(bt, n, i_check+1, &ks);
		*child_id_after = ks.ptr;
	    }

            if (n->flags & LEAF_NODE) {
                *child_id_before = BAD_CHILD;
                *child_id_after  = BAD_CHILD;
                *child_id  = BAD_CHILD;
	    }

            if (!key_found) {
		pk = NULL;
	    }

	    return(pk);
	}
    }
    assert(0);  // we should never get here!
}

static node_key_t *bsearch_key(btree_raw_t *bt, btree_raw_node_t *n, char *key_in, uint32_t keylen_in, uint64_t *child_id, btree_metadata_t *meta, uint64_t syndrome)
{
    node_key_t       *pk_insert;
    uint64_t          child_id_before, child_id_after;
    int32_t           nkey_child;

    return find_key(bt, n, key_in, keylen_in, child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);
}

static char *get_buffer(btree_raw_t *btree, uint64_t nbytes)
{
    char  *p;
    // pid_t  tid = syscall(SYS_gettid);

    p = malloc(nbytes);
    // xxxzzz SEGFAULT
    // fprintf(stderr, "SEGFAULT get_buffer: %p [tid=%d]\n", p, tid);
    return(p);
}

static void free_buffer(btree_raw_t *btree, char *buf)
{
    // pid_t  tid = syscall(SYS_gettid);

    if (btree->freebuf_cb != NULL) {
        if (btree->freebuf_cb(btree->freebuf_cb_data, buf)) {
	    assert(0); // xxxzzz remove this!
	}
    } else {
	// xxxzzz SEGFAULT
	// fprintf(stderr, "SEGFAULT free_buffer: %p [tid=%d]\n", buf, tid);
	free(buf);
    }
}

btree_status_t get_leaf_data(btree_raw_t *bt, btree_raw_node_t *n, void *pkey, char **data, uint64_t *datalen, uint32_t meta_flags, int ref)
{
    node_vlkey_t       *pvlk;
    btree_raw_node_t   *z;
    int                 ret=0;
    char               *buf;
    char               *p;
    int                 buf_alloced=0;
    uint64_t            nbytes;
    uint64_t            copybytes;
    uint64_t            z_next;

    pvlk = (node_vlkey_t *) pkey;

    if (meta_flags & BUFFER_PROVIDED) {
        if (*datalen < pvlk->datalen) {
	    ret = BTREE_BUFFER_TOO_SMALL;
	    if (meta_flags & ALLOC_IF_TOO_SMALL) {
		buf = get_buffer(bt, pvlk->datalen);
		if (buf == NULL) {
		    bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", pvlk->datalen);
		    return(1);
		}
		buf_alloced = 1;
	    } else {
	        return(BTREE_BUFFER_TOO_SMALL);
	    }
	} else {
	    buf = *data;
	}
    } else {
        buf = get_buffer(bt, pvlk->datalen);
	if (buf == NULL) {
	    bt_err("Failed to allocate a buffer of size %lld in get_leaf_data!", pvlk->datalen);
	    return(1);
	}
	buf_alloced = 1;
    }

    if ((pvlk->keylen + pvlk->datalen) < bt->big_object_size) {
        //  key and data are in this btree node
	memcpy(buf, (char *) n + pvlk->keypos + pvlk->keylen, pvlk->datalen);
    } else {
        //  data is in overflow btree nodes

        if (pvlk->datalen > 0) {
	    nbytes = pvlk->datalen;
	    p      = buf;
	    z_next = pvlk->ptr;
	    while(nbytes > 0 && z_next)
	    {
		z = get_existing_node_low(&ret, bt, z_next, NULL, ref);
		if(!z)
		    break;
		if (nbytes >= bt->nodesize_less_hdr) {
		    copybytes = bt->nodesize_less_hdr;
		} else {
		    copybytes = nbytes;
		}
		memcpy(p, ((char *) z + sizeof(btree_raw_node_t)), copybytes);
		nbytes -= copybytes;
		p      += copybytes;
		z_next  = z->next;
		if(!ref)
		    deref_l1cache_node(bt, z);
	    }
	    if (nbytes) {
		bt_err("Failed to get overflow node (logical_id=%lld) in get_leaf_data!", z_next);
		if (buf_alloced) {
		    free_buffer(bt, buf);
		}
		return(1);
	    }
	    assert(z_next == 0);
	}
    }
    *datalen = pvlk->datalen;
    *data    = buf;
    return(ret);
}

btree_status_t 
get_leaf_key(btree_raw_t *bt, btree_raw_node_t *n, void *pkey, char **key, 
             uint32_t *keylen, uint32_t meta_flags)
{
	node_vlkey_t       *pvlk;
	btree_status_t     ret = BTREE_SUCCESS;
	char               *buf;

	pvlk = (node_vlkey_t *) pkey;

	if (meta_flags & BUFFER_PROVIDED) {
		if (*keylen < pvlk->keylen) {
			ret = BTREE_BUFFER_TOO_SMALL;
			if (!(meta_flags & ALLOC_IF_TOO_SMALL)) {
				return ret;
			}

			buf = get_buffer(bt, pvlk->keylen);
			if (buf == NULL) {
				bt_err("Failed to allocate a buffer of size %lld "
				       "in get_leaf_key!", pvlk->keylen);
				return(BTREE_FAILURE);
			}
		} else {
			buf = *key;
		}
	} else {
		buf = get_buffer(bt, pvlk->keylen);
		if (buf == NULL) {
			bt_err("Failed to allocate a buffer of size %lld "
			       "in get_leaf_key!", pvlk->keylen);
			return(BTREE_FAILURE);
		}
	}

	memcpy(buf, (char *) n + pvlk->keypos, pvlk->keylen);
	*keylen = pvlk->keylen;
	*key    = buf;

	return(ret);
}

static void delete_overflow_data(int *ret, btree_raw_t *bt, uint64_t ptr_in, uint64_t datalen, int modify_tree)
{
    uint64_t            ptr;
    uint64_t            ptr_next;
    btree_raw_node_t   *n;

    if (*ret) { return; }

    for (ptr = ptr_in; ptr != 0; ptr = ptr_next) {
        int ret2 = 0;
	n = get_existing_node_low(&ret2, bt, ptr, NULL, modify_tree);
	assert(n != NULL);
	if (ret2) {
	    bt_err("Failed to find an existing overflow node in delete_overflow_data!");
	    *ret = 1;
	    return;
	}

	ptr_next = n->next;
	ret2 = 0;
	if(!modify_tree)
	    deref_l1cache_node(bt, n);
	free_node(&ret2, bt, n);
	if (ret2) {
	    bt_err("Failed to free an existing overflow node in delete_overflow_data!");
	    *ret = 1;
	}
    }
}

static uint64_t allocate_overflow_data(btree_raw_t *bt, uint64_t datalen, char *data, btree_metadata_t *meta, int modify_tree)
{
    uint64_t            n_nodes;
    btree_raw_node_t   *n;
    btree_raw_node_t   *n_first = NULL;
    btree_raw_node_t   *n_last = NULL;
    int                 ret = 0;
    char               *p = data;;
    uint64_t            nbytes = datalen;

    if (!datalen)
        return(0);

    n_nodes = (datalen + bt->nodesize_less_hdr - 1) / bt->nodesize_less_hdr;

    n_first = n = get_new_node_low(&ret, bt, OVERFLOW_NODE, modify_tree);
    while(nbytes > 0 && !ret) {
	n->next = 0;

	if (n_last != NULL)
	    n_last->next = n->logical_id;

	int b = nbytes < bt->nodesize_less_hdr ? nbytes : bt->nodesize_less_hdr;

	memcpy(((char *) n + sizeof(btree_raw_node_t)), p, b);

	if(!modify_tree) {
	    bt->write_node_cb(&ret, bt->write_node_cb_data, n->logical_id, (char*) n, bt->nodesize);
	    bt->stats.stat[BTSTAT_L1WRITES]++;
	}

	p += b;
	nbytes -= b;
	n_last = n;

	if(nbytes)
	    n = get_new_node_low(&ret, bt, OVERFLOW_NODE, modify_tree);
    }

    if(!ret) /* Success */
	return n_first->logical_id;

    ret = 0;
    delete_overflow_data(&ret, bt, n_first->logical_id, datalen, modify_tree); 
    return(0);
}

static uint64_t get_syndrome(btree_raw_t *bt, char *key, uint32_t keylen)
{
    uint64_t   syndrome;

    syndrome = btree_hash((const unsigned char *) key, keylen, 0);
    return(syndrome);
}

/* Caller is responsible for leaf_lock unlock and node dereferencing */
node_key_t* btree_raw_find(struct btree_raw *btree, char *key, uint32_t keylen, uint64_t syndrome, btree_metadata_t *meta, btree_raw_node_t** node, plat_rwlock_t** leaf_lock, int* pathcnt)
{
    int               ret = 0;
    uint64_t          child_id;

    *node = get_existing_node_low(&ret, btree, btree->rootid, leaf_lock, 0);
    assert(*node);

    while(!is_leaf(btree, *node)) {
        (void)bsearch_key(btree, *node, key, keylen, &child_id, meta, syndrome);
        assert(child_id != BAD_CHILD);

        deref_l1cache_node(btree, *node);

        *node = get_existing_node_low(&ret, btree, child_id, leaf_lock, 0);
        assert(!ret && *node);

        (*pathcnt)++;
    }

    plat_rwlock_rdlock(*leaf_lock);

    return bsearch_key(btree, *node, key, keylen, &child_id, meta, syndrome);
}

int btree_raw_get(struct btree_raw *btree, char *key, uint32_t keylen, char **data, uint64_t *datalen, btree_metadata_t *meta)
{
    int               ret = 0, pathcnt = 1;
    btree_raw_node_t *node;
    node_key_t       *keyrec;
    plat_rwlock_t    *leaf_lock;
    uint64_t          syndrome = get_syndrome(btree, key, keylen);

    plat_rwlock_rdlock(&btree->lock);

    keyrec = btree_raw_find(btree, key, keylen, syndrome, meta, &node, &leaf_lock, &pathcnt);

    ret = 2; // object not found;
    if(keyrec) {
	ret = get_leaf_data(btree, node, keyrec, data, datalen, meta->flags, 0);
	assert(!ret);
    }

    deref_l1cache_node(btree, node);
    plat_rwlock_unlock(leaf_lock);

    plat_rwlock_unlock(&btree->lock);

    btree->stats.stat[BTSTAT_GET_CNT]++;
    btree->stats.stat[BTSTAT_GET_PATH] += pathcnt;

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    fprintf(stderr, "********  After btree_raw_get for key '%s' [syn=%lu]: ret=%d  *******\n", dump_key(key, keylen), syndrome, ret);
	}
    #endif

    return(ret);
}

//======================   INSERT/UPDATE/UPSERT  =========================================

//  return 0 if success, 1 otherwise
static int init_l1cache(btree_raw_t *bt, uint32_t n_l1cache_buckets)
{
    bt->n_l1cache_buckets = n_l1cache_buckets;
    bt->l1cache = MapInit(n_l1cache_buckets, 16 * n_l1cache_buckets, 1, l1cache_replace, (void *) bt);
    if (bt->l1cache == NULL) {
        return(1);
    }
    bt->l1cache_refs= MapInit(n_l1cache_buckets, 0, 0, NULL, NULL);
    if (bt->l1cache_refs == NULL) {
        return(1);
    }

    bt->l1cache_mods = MapInit(n_l1cache_buckets, 0, 0, NULL, NULL);
    if (bt->l1cache_mods == NULL) {
        return(1);
    }

    return(0);
}

static void deref_l1cache_node(btree_raw_t* btree, btree_raw_node_t *node)
{
    if (btree->flags & IN_MEMORY)
        return;

    if (!MapRelease(btree->l1cache, (char *) &node->logical_id, sizeof(node->logical_id)))
        assert(0);
#ifdef DEBUG_STUFF
    if(Verbose)
	fprintf(stderr, "%x %s node %p root: %d leaf: %d refcnt %d\n", (int)pthread_self(), __FUNCTION__, node, is_root(btree, node), is_leaf(btree, node), MapGetRefcnt(btree->l1cache, (char *) &node->logical_id, sizeof(uint64_t)));
#endif
}

static int deref_l1cache(btree_raw_t *btree)
{
    struct Iterator *it;
    char                 *key;
    uint32_t              keylen;
    char                 *data;
    uint64_t              datalen;
    int                   ret = 0;

    //  first write out modifications
    for (it = MapEnum(btree->l1cache_mods);
         MapNextEnum(btree->l1cache_mods, it, &key, &keylen, &data, &datalen);
	 )
    {
        // xxxzzz this is temporary!
	// btree_raw_node_t *n = (btree_raw_node_t *) data;
	// if (n->flags & LEAF_NODE) {
#ifdef DEBUG_STUFF
	if(Verbose)
	    fprintf(stderr, "%x %s write_node_cb key=%ld data=%p datalen=%ld\n", (int)pthread_self(), __FUNCTION__, (uint64_t)key, data, datalen);
#endif
	    btree->write_node_cb(&ret, btree->write_node_cb_data, (uint64_t) key, data, datalen);
	// }
	btree->stats.stat[BTSTAT_L1WRITES]++;
	// break; // xxxzzz limited to 1!
    }
    FinishEnum(btree->l1cache_mods, it);

    //  clear reference bits
    for (it = MapEnum(btree->l1cache_refs);
         MapNextEnum(btree->l1cache_refs, it, &key, &keylen, &data, &datalen);
	 )
    {
        // xxxzzz this is temporary!
	// btree_raw_node_t *n = (btree_raw_node_t *) data;
	// if (n->flags & LEAF_NODE) {
	    if (!MapRelease(btree->l1cache, (char *) &key, keylen)) {
		ret = 1;
	    }
#ifdef DEBUG_STUFF
	fprintf(stderr, "%x %s map_release key=%ld data=%p datalen=%ld refcnt %d\n", (int)pthread_self(), __FUNCTION__, (uint64_t)key, data, datalen, MapGetRefcnt(btree->l1cache, (char*)&key, sizeof(uint64_t)));
#endif
	// }
    }
    FinishEnum(btree->l1cache_refs, it);

    //  clear the hashtables
    MapClear(btree->l1cache_refs);
    MapClear(btree->l1cache_mods);

    // xxxzzz
    if (MapNEntries(btree->l1cache) > btree->n_l1cache_buckets) {
        assert(0);
    }

    return(ret);
}

static int add_l1cache(btree_raw_t *btree, btree_raw_node_t *n, plat_rwlock_t** lock)
{
    btree_raw_mem_node_t *node;

    node = malloc(sizeof(btree_raw_mem_node_t));
    assert(node);
    assert(n);

    node->node = n;
    plat_rwlock_init(&node->lock);
#ifdef DEBUG_STUFF
    fprintf(stderr, "%x %s %p %p root: %d leaf: %d\n", (int)pthread_self(), __FUNCTION__, n, &node->lock, is_root(btree, n), is_leaf(btree, n));
#endif

    if(!MapCreate(btree->l1cache, (char *) &(n->logical_id), sizeof(uint64_t), (char *) node, sizeof(uint64_t)))
    {
        plat_rwlock_destroy(&node->lock);
        free(n);
        free(node);
        return 0;
    }

    if(lock) *lock = &node->lock;

    btree->stats.stat[BTSTAT_L1ENTRIES] = MapNEntries(btree->l1cache);
    return 1;
}

static void ref_l1cache(btree_raw_t *btree, btree_raw_node_t *n)
{
    char                *old_data;
    uint64_t             old_datalen;
    uint64_t             datalen;
    btree_raw_node_t    *n2;

    if (MapGet(btree->l1cache_refs, (char *) &(n->logical_id), sizeof(uint64_t), (char **) &n2, &datalen) != NULL) {
        // already referenced
        return;
    }
    assert(MapSet(btree->l1cache_refs, (char *) &(n->logical_id), sizeof(uint64_t), (char *) n, sizeof(uint64_t), &old_data, &old_datalen) != NULL);
}

static btree_raw_node_t *get_l1cache(btree_raw_t *btree, uint64_t logical_id, plat_rwlock_t** lock)
{
    uint64_t             datalen;
    btree_raw_mem_node_t    *n;

    if (MapGet(btree->l1cache, (char *) &logical_id, sizeof(uint64_t), (char **) &n, &datalen) == NULL) {
        return(NULL);
    }

    if(lock) *lock = &n->lock;

#ifdef DEBUG_STUFF
    fprintf(stderr, "%x %s n %p node %p lock %p root: %d leaf: %d refcnt %d\n", (int)pthread_self(), __FUNCTION__, n, n->node, &n->lock, is_root(btree, n->node), is_leaf(btree, n->node), MapGetRefcnt(btree->l1cache, (char *) &logical_id, sizeof(uint64_t)));
#endif
    return(n->node);
}

static void delete_l1cache(btree_raw_t *btree, btree_raw_node_t *n)
{
#ifdef DEBUG_STUFF
    fprintf(stderr, "%x %s node %p root: %d leaf: %d refcnt %d\n", (int)pthread_self(), __FUNCTION__, n, is_root(btree, n), is_leaf(btree, n), MapGetRefcnt(btree->l1cache, (char *) &n->logical_id, sizeof(uint64_t)));
#endif
    (void) MapDelete(btree->l1cache, (char *) &(n->logical_id), sizeof(uint64_t));
    (void) MapDelete(btree->l1cache_refs, (char *) &(n->logical_id), sizeof(uint64_t));
    (void) MapDelete(btree->l1cache_mods, (char *) &(n->logical_id), sizeof(uint64_t));
    btree->stats.stat[BTSTAT_L1ENTRIES] = MapNEntries(btree->l1cache);
}

static void modify_l1cache_node(btree_raw_t *btree, btree_raw_node_t *n)
{
    uint64_t  old_datalen;
    char     *old_data;

    (void) MapSet(btree->l1cache_mods, (char *) &(n->logical_id), sizeof(uint64_t), (char *) n, btree->nodesize, &old_data, &old_datalen);
}

btree_raw_node_t *get_existing_node_low(int *ret, btree_raw_t *btree, uint64_t logical_id, plat_rwlock_t** lock, int ref)
{
    btree_raw_node_t  *n;

    if (*ret) { return(NULL); }

    if (btree->flags & IN_MEMORY) {
        btree_raw_mem_node_t* node = (btree_raw_mem_node_t*)logical_id;
        n = node->node;
        if(lock) *lock = &node->lock;
//#ifdef DEBUG_STUFF
//        fprintf(stderr, "%x %s n=%p node=%p flags=%d\n", (int)pthread_self(), __FUNCTION__, n, node, n->flags);
//#endif
    } else {
retry:
        //  check l1cache first
	n = get_l1cache(btree, logical_id, lock);
	if (n != NULL) {
	    btree->stats.stat[BTSTAT_L1HITS]++;
	} else {
	    btree->stats.stat[BTSTAT_L1MISSES]++;
	    //  look for the node the hard way
	    n = btree->read_node_cb(ret, btree->read_node_cb_data, logical_id);
	    if (n == NULL) {
		*ret = 1;
		return(NULL);
	    }
            // already in the cache retry get
	    if(!add_l1cache(btree, n, lock))
                goto retry;
	}
        if(ref)
            ref_l1cache(btree, n);
    }
    if (n == NULL) {
        *ret = 1;
	return(NULL);
    }
    
    return(n);
}

btree_raw_node_t *get_existing_node(int *ret, btree_raw_t *btree, uint64_t logical_id)
{
    return get_existing_node_low(ret, btree, logical_id, NULL, 1);
}

static btree_raw_node_t *get_new_node_low(int *ret, btree_raw_t *btree, uint32_t leaf_flags, int ref)
{
    btree_raw_node_t  *n;
    uint64_t           logical_id;
    // pid_t  tid = syscall(SYS_gettid);

    if (*ret) { return(NULL); }

    if (btree->flags & IN_MEMORY) {
        btree_raw_mem_node_t* node = malloc(sizeof(btree_raw_mem_node_t) + btree->nodesize);
        node->node = (btree_raw_node_t*) ((void*)node + sizeof(btree_raw_mem_node_t));
        n = node->node;
        plat_rwlock_init(&node->lock);
	logical_id = (uint64_t) node;
	if (n != NULL) {
	    n->logical_id = logical_id;
	}
#ifdef DEBUG_STUFF
	if(Verbose)
        fprintf(stderr, "%x %s n=%p node=%p flags=%d\n", (int)pthread_self(), __FUNCTION__, n, node, leaf_flags);
#endif
    } else {
        logical_id = btree->logical_id_counter++*btree->n_partitions + btree->n_partition;
        if (! savepersistent( btree)) {
            *ret = 1;
            return (NULL);
        }
	// n = btree->create_node_cb(ret, btree->create_node_cb_data, logical_id);
	//  Just malloc the node here.  It will be written
	//  out at the end of the request by deref_l1cache().
        n = (btree_raw_node_t *) malloc(btree->nodesize);
	if (n != NULL) {
	    n->logical_id = logical_id;
	    int not_exists = add_l1cache(btree, n, NULL);
            assert(not_exists); /* the tree is exclusively locked */
	    if(ref)
	    {
	    	ref_l1cache(btree, n);
            	modify_l1cache_node(btree, n);
	    }
	    // xxxzzz SEGFAULT
	    // fprintf(stderr, "SEGFAULT get_new_node: %p [tid=%d]\n", n, tid);
	}
    }
    if (n == NULL) {
        *ret = 1;
	return(NULL);
    }

    n->flags      = leaf_flags;
    n->lsn        = 0;
    n->checksum   = 0;
    n->insert_ptr = btree->nodesize;
    n->nkeys      = 0;
    n->prev       = 0; // used for chaining nodes for large objects
    n->next       = 0; // used for chaining nodes for large objects
    n->rightmost  = BAD_CHILD;

    if (leaf_flags & LEAF_NODE) {
	btree->stats.stat[BTSTAT_LEAVES]++;
    } else if (leaf_flags & OVERFLOW_NODE) {
	btree->stats.stat[BTSTAT_OVERFLOW_NODES]++;
    } else {
	btree->stats.stat[BTSTAT_NONLEAVES]++;
    }

    return(n);
}

static btree_raw_node_t *get_new_node(int *ret, btree_raw_t *btree, uint32_t leaf_flags)
{
    return get_new_node_low(ret, btree, leaf_flags, 1);
}

static void free_node(int *ret, btree_raw_t *btree, btree_raw_node_t *n)
{
    // pid_t  tid = syscall(SYS_gettid);

    if (*ret) { return; }

    if (n->flags & LEAF_NODE) {
	btree->stats.stat[BTSTAT_LEAVES]--;
    } else if (n->flags & OVERFLOW_NODE) {
	btree->stats.stat[BTSTAT_OVERFLOW_NODES]--;
    } else {
	btree->stats.stat[BTSTAT_NONLEAVES]--;
    }

    if (btree->flags & IN_MEMORY) {
	// xxxzzz SEGFAULT
	// fprintf(stderr, "SEGFAULT free_node: %p [tid=%d]\n", n, tid);
       // fprintf(stderr, "%x %s n=%p node=%p flags=%d", (int)pthread_self(), __FUNCTION__, n, (void*)n - sizeof(btree_raw_mem_node_t), n->flags);
	free((void*)n - sizeof(btree_raw_mem_node_t));
    } else {
	delete_l1cache(btree, n);
	*ret = btree->delete_node_cb(n, btree->create_node_cb_data, n->logical_id);
    }
}

/*   Split the 'from' node across 'from' and 'to'.
 *
 *   Returns: pointer to the key at which the split was done
 *            (all keys < key must go in node 'to')
 */
static void split_copy(int *ret, btree_raw_t *btree, btree_raw_node_t *from, btree_raw_node_t *to, char **key_out, uint32_t *keylen_out, uint64_t *split_syndrome_out)
{
    node_fkey_t   *pfk;
    uint32_t       i, threshold, nbytes_to, nbytes_from, nkeys_to, nkeys_from;
    uint32_t       nbytes = 0;
    uint32_t       nbytes_split_nonleaf = 0;
    uint32_t       nbytes_fixed;
    key_stuff_t    ks;
    uint64_t       n_right     = 0;
    uint64_t       old_n_right = 0;
    uint64_t       split_syndrome = 0;
    char          *key = NULL;
    uint32_t       keylen = 0;

    if (*ret) { return; }

    (void) get_key_stuff(btree, from, 0, &ks);

    if (ks.fixed) {
        nkeys_to     = (ks.fkeys_per_node/2);
        nbytes_to    = nkeys_to*ks.offset;

        nkeys_from   = (from->nkeys - nkeys_to);
        nbytes_from  = nkeys_from*ks.offset;
	nbytes_fixed = ks.offset;

	//  last key in 'to' node gets inserted into parent
	//  For lack of a better place, we stash the split key
	//  in an unused key slot in the 'to' node.
	//  This temporary copy is only used by the caller to
	//  split_copy to insert the split key into the parent.
 
 	pfk            = (node_fkey_t *) to->keys + nkeys_to;
	pfk->ptr       = ((node_fkey_t *) from->keys + nkeys_to)->ptr;
	key            = (char *) &(pfk->ptr);
	keylen         = sizeof(uint64_t);
	split_syndrome = ((node_fkey_t *) from->keys + nkeys_to - 1)->key;
	n_right        = ((node_fkey_t *) from->keys + nkeys_to - 1)->ptr;

    } else {
        threshold = (btree->nodesize - sizeof(btree_raw_node_t))/2;
	nbytes_to = 0;
	for (i=0; i<from->nkeys; i++) {
	    (void) get_key_stuff(btree, from, i, &ks);
	    nbytes     = ks.keylen;
	    if (ks.leaf) {
		if ((ks.keylen + ks.datalen) < btree->big_object_size) { // xxxzzz check this!
		    nbytes += ks.datalen;
		}
	    }
	    nbytes_to += nbytes;
	    old_n_right = n_right;
	    n_right     = ks.ptr;
	    if ((nbytes_to + (i+1)*ks.offset) > threshold) {
	        break;
	    }
	    //  last key in 'to' node gets inserted into parent
	    key            = ks.pkey_val; // This should be unchanged in the from node, 
		   		          // even though it is no longer used!
	    keylen               = ks.keylen;
	    split_syndrome       = ks.syndrome;
	    nbytes_split_nonleaf = ks.keylen;
	}
	assert(i < from->nkeys); // xxxzzz remove me!
	assert(i != 0);          // xxxzzz remove me!

	nkeys_to   = i; // xxxzzz check this
	nbytes_to -= nbytes; // xxxzzz check this
	n_right    = old_n_right;

	nkeys_from   = from->nkeys - nkeys_to;
	nbytes_from  = btree->nodesize - from->insert_ptr - nbytes_to;
	nbytes_fixed = ks.offset;
    }
    *key_out            = key;
    *keylen_out         = keylen;
    *split_syndrome_out = split_syndrome;

    // copy the fixed size portion of the keys

    memcpy(to->keys, from->keys, nkeys_to*nbytes_fixed);
    to->nkeys = ks.leaf ? nkeys_to : nkeys_to - 1;

    memmove(from->keys, (char *) from->keys + nkeys_to*nbytes_fixed, nkeys_from*nbytes_fixed);
    from->nkeys = nkeys_from;

    if (ks.fixed) {
	to->insert_ptr   = 0;
	from->insert_ptr = 0;
    } else {
	// for variable sized keys, copy the variable sized portion
	//  For leaf nodes, copy the data too

        if (ks.leaf) {
	    memcpy(((char *) to) + btree->nodesize - nbytes_to, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_to);

	    to->insert_ptr   = btree->nodesize - nbytes_to;
	    from->insert_ptr = btree->nodesize - nbytes_from;
	} else {
	    //  for non-leaves, skip split key
	    memcpy(((char *) to) + btree->nodesize - nbytes_to + nbytes_split_nonleaf, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_to - nbytes_split_nonleaf);

	    to->insert_ptr   = btree->nodesize - nbytes_to + nbytes_split_nonleaf;
	    from->insert_ptr = btree->nodesize - nbytes_from;
	}
	
	//  update the keypos pointers
        update_keypos(btree, to, 0);
        update_keypos(btree, from, 0);
    }

    // update the rightmost pointers of the 'to' node
    if (ks.leaf) {
	to->rightmost = from->logical_id;
	// xxxzzz from->leftmost = to->logical_id;
	// xxxzzz continue from here:fix rightmost pointer of left sibling!!
	// xxxzzz continue from here:fix leftmost pointer of right sibling!!
    } else {
	to->rightmost = n_right;
    }

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], rightmost %lx B-Tree BEGIN:  *******\n", dump_key(key, keylen), split_syndrome, to->rightmost);
	    btree_raw_dump(stderr, btree);
	    fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], To-Node:  *******\n", dump_key(key, keylen), split_syndrome);
            dump_node(btree, stderr, to, key, keylen);
	    fprintf(stderr, "********  After split_copy for key '%s' [syn=%lu], B-Tree END:  *******\n", dump_key(key, keylen), split_syndrome);
	}
    #endif

    return;
}

static int has_fixed_keys(btree_raw_t *btree, btree_raw_node_t *n)
{
    return((btree->flags & SYNDROME_INDEX) && !(n->flags & LEAF_NODE));
}

static void update_keypos(btree_raw_t *btree, btree_raw_node_t *n, uint32_t n_key_start)
{
    int            i;
    uint32_t       keypos;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;

    if (has_fixed_keys(btree, n)) {
        return;
    }

    keypos = n->insert_ptr;
    if (n->flags & LEAF_NODE) {
	for (i=n_key_start; i<n->nkeys; i++) {
	    pvlk = (node_vlkey_t *) (((char *) n->keys) + i*sizeof(node_vlkey_t));
	    pvlk->keypos = keypos;
	    keypos += pvlk->keylen;
	    if ((pvlk->keylen + pvlk->datalen) < btree->big_object_size) {
	        //  data is NOT overflowed!
		keypos += pvlk->datalen;
	    }
	}
    } else {
	for (i=n_key_start; i<n->nkeys; i++) {
	    pvk = (node_vkey_t *) (((char *) n->keys) + i*sizeof(node_vkey_t));
	    pvk->keypos = keypos;
	    keypos += pvk->keylen;
	}
    }
}

/*   Insert a new key into a node (and possibly its data if this is a leaf)
 *   This assumes that we have enough space!
 *   It is the responsibility of the caller to check!
 */
static void insert_key_low(int *ret, btree_raw_t *btree, btree_raw_node_t *x, char *key, uint32_t keylen, uint64_t seqno, uint64_t datalen, char *data, btree_metadata_t *meta, uint64_t syndrome, node_key_t* pkrec, node_key_t* pk_insert, int modify_tree)
{
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    node_fkey_t   *pfk;
    uint32_t       nkeys_to = 0, nkeys_from = 0;
    uint32_t       fixed_bytes;
    uint64_t       child_id, child_id_before, child_id_after;
    node_vkey_t   *pvk_insert;
    node_vlkey_t  *pvlk_insert;
    uint32_t       pos_new_key = 0;
    key_stuff_t    ks;
    uint32_t       vbytes_this_node = 0;
    uint64_t       ptr_overflow = 0;
    uint32_t       pos_split = 0;
    uint32_t       nbytes_split = 0;
    uint32_t       nbytes_free;
    int32_t        nkey_child;

    if (*ret) { return; }

    if (x->flags & LEAF_NODE) {
	btree->stats.stat[BTSTAT_LEAVE_BYTES] += (keylen + datalen);
    } else {
	btree->stats.stat[BTSTAT_NONLEAVE_BYTES] += (keylen + datalen);
    }

    if (pkrec != NULL) {
        // delete existing key first
	delete_key_by_pkrec(ret, btree, x, pkrec, modify_tree);
	assert((*ret) == 0);
	pkrec = find_key(btree, x, key, keylen, &child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);
	assert(pkrec == NULL);
    }

    (void) get_key_stuff(btree, x, 0, &ks);

    if(modify_tree)
        modify_l1cache_node(btree, x);

    if (pk_insert == NULL) {
	nkeys_to     = x->nkeys;
	pos_split    = btree->nodesize;
	nbytes_split = btree->nodesize - x->insert_ptr;
    }

    if (!ks.fixed) {
        if (x->flags & LEAF_NODE) {
	    pvlk_insert = (node_vlkey_t *) pk_insert;
	    if (pvlk_insert != NULL) {
		nkeys_to     = (((char *) pk_insert) - ((char *) x->keys))/ks.offset;
		pos_split    = pvlk_insert->keypos;
		nbytes_split = pvlk_insert->keypos - x->insert_ptr;
	    }
	} else {
	    pvk_insert  = (node_vkey_t *) pk_insert;
	    if (pvk_insert != NULL) {
		nkeys_to     = (((char *) pk_insert) - ((char *) x->keys))/ks.offset;
		pos_split    = pvk_insert->keypos;
		nbytes_split = pvk_insert->keypos - x->insert_ptr;
	    }
	}
        fixed_bytes = ks.offset;
    } else {
        fixed_bytes = ks.offset;
	if (pk_insert != NULL) {
	    nkeys_to = (((char *) pk_insert) - ((char *) x->keys))/ks.offset;
	}
    }
    nkeys_from = x->nkeys - nkeys_to;

    if ((!ks.fixed) && 
        (x->flags & LEAF_NODE) && 
	((keylen + datalen) >= btree->big_object_size)) // xxxzzz check this!
    { 
	//  Allocate nodes for overflowed objects first, in case
	//  something goes wrong.
        
	ptr_overflow = allocate_overflow_data(btree, datalen, data, meta, modify_tree);
	if ((ptr_overflow == 0) && (datalen != 0)) {
	    // something went wrong with the allocation
	    *ret = 1;
	    return;
	}
    }

    if (ks.fixed) {
	// check that there is enough space!
	assert(x->nkeys < (btree->fkeys_per_node));
    } else {
        if (x->flags & LEAF_NODE) {

	    //  insert variable portion of new key (and possibly data) in
	    //  sorted key order at end of variable data stack in node

	    if ((keylen + datalen) >= btree->big_object_size) { // xxxzzz check this!
	        //  put key in this node, data in overflow nodes
		vbytes_this_node = keylen;
	    } else {
	        //  put key and data in this node
		vbytes_this_node = keylen + datalen;
	    }
	    // check that there is enough space!
	    nbytes_free = x->insert_ptr - sizeof(btree_raw_node_t) - x->nkeys*sizeof(node_vlkey_t);
	    assert(nbytes_free >= (sizeof(node_vlkey_t) + vbytes_this_node));

	    //  make space for variable portion of new key/data

	    memmove((char *) x + pos_split - nbytes_split - vbytes_this_node,
		    (char *) x + pos_split - nbytes_split, 
		    nbytes_split);

	    pos_new_key = pos_split - vbytes_this_node;

	    //  insert variable portion of new key

	    memcpy((char *) x + pos_new_key, key, keylen);
	    if (vbytes_this_node > keylen) {
		//  insert data
		memcpy((char *) x + pos_new_key + keylen, data, datalen);
	    }
	} else {
	    vbytes_this_node = keylen;

	    // check that there is enough space!
	    nbytes_free = x->insert_ptr - sizeof(btree_raw_node_t) - x->nkeys*sizeof(node_vkey_t);
	    assert(nbytes_free >= (sizeof(node_vkey_t) + vbytes_this_node));

	    //  make space for variable portion of new key/data

	    memmove((char *) x + pos_split - nbytes_split - vbytes_this_node,
		    (char *) x + pos_split - nbytes_split, 
		    nbytes_split);

	    pos_new_key = pos_split - vbytes_this_node;

	    //  insert variable portion of new key

	    memcpy((char *) x + pos_new_key, key, keylen);
	}
    }

    //  Make space for fixed portion of new key.
    // 
    //  NOTE: This MUST be done after updating the variable part
    //        because the variable part uses key data in its old location!
    //

    if (nkeys_from != 0) {
	memmove((char *) (x->keys) + (nkeys_to + 1)*fixed_bytes, (char *) (x->keys) + nkeys_to*fixed_bytes, nkeys_from*fixed_bytes);
    }

    if (!ks.fixed) {
	x->insert_ptr -= vbytes_this_node;
    } else {
	x->insert_ptr = 0; // xxxzzz this should be redundant!
    }

    //  Do this here because update_keypos() requires it!
    x->nkeys += 1;

    //  insert fixed portion of new key
    if (!ks.fixed) {
        if (x->flags & LEAF_NODE) {
	    pvlk           = (node_vlkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
	    pvlk->keylen   = keylen;
	    pvlk->keypos   = pos_new_key;
	    pvlk->datalen  = datalen;
	    pvlk->seqno    = seqno;
	    pvlk->syndrome = syndrome;
	    if ((keylen + datalen) >= btree->big_object_size) { // xxxzzz check this!
	        //  data is in overflow nodes
		pvlk->ptr = ptr_overflow;
	    } else {
	        //  data is in this node
		pvlk->ptr = 0;
	    }
	} else {
	    pvk          = (node_vkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
	    pvk->keylen  = keylen;
	    pvk->keypos  = pos_new_key;
	    pvk->seqno   = seqno;
	    assert(datalen == sizeof(uint64_t));
	    pvk->ptr     = *((uint64_t *) data);
	}

	//  update all of the 'keypos' fields in the fixed portion
	update_keypos(btree, x, 0);

    } else {
        pfk            = (node_fkey_t *) ((char *) (x->keys) + nkeys_to*fixed_bytes);
	pfk->key       = syndrome;
	pfk->seqno     = seqno;
	assert(datalen == sizeof(uint64_t));
	pfk->ptr       = *((uint64_t *) data);
    }

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    char  stmp[10000];
	    int   len;
	    if ((btree->flags & SYNDROME_INDEX) && !(x->flags & LEAF_NODE)) {
	        sprintf(stmp, "%p", key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, key, keylen);
		len = keylen;
	    }
#
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, modify_tree:%d node %p BEGIN:  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen, modify_tree, x);
	    btree_raw_dump(stderr, btree);
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, NODE:  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen);
	    (void) get_key_stuff(btree, x, 0, &ks);
	    if ((btree->flags & SYNDROME_INDEX) && !(x->flags & LEAF_NODE)) {
	        sprintf(stmp, "%p", ks.pkey_val);
		dump_node(btree, stderr, x, stmp, strlen(stmp));
	    } else {
		dump_node(btree, stderr, x, ks.pkey_val, ks.keylen);
	    }
	    fprintf(stderr, "%x ********  After insert_key '%s' [syn %lu], datalen=%ld, node %p END  *******\n", (int)pthread_self(), dump_key(stmp, len), syndrome, datalen, x);
	}
    #endif
}

static void insert_key(int *ret, btree_raw_t *btree, btree_raw_node_t *x, char *key, uint32_t keylen, uint64_t seqno, uint64_t datalen, char *data, btree_metadata_t *meta, uint64_t syndrome)
{
    uint64_t       child_id, child_id_before, child_id_after;
    int32_t        nkey_child;
    node_key_t    *pk_insert;
    node_key_t* pkrec;

    pkrec = find_key(btree, x, key, keylen, &child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);

    return insert_key_low(ret, btree, x, key, keylen, seqno, datalen, data, meta, syndrome, pkrec, pk_insert, MODIFY_TREE);
}

static void delete_key_by_pkrec(int* ret, btree_raw_t *btree, btree_raw_node_t *x, node_key_t *pk_delete, int modify_tree)
{
    uint32_t       nkeys_to, nkeys_from;
    uint32_t       fixed_bytes;
    uint64_t       datalen = 0;
    uint64_t       keylen = 0;
    uint64_t       datalen_stats = 0;
    node_vkey_t   *pvk_delete = NULL;
    node_vlkey_t  *pvlk_delete = NULL;
    key_stuff_t    ks;

    assert(pk_delete);

    if(*ret) return;

    (void) get_key_stuff(btree, x, 0, &ks);

    if(modify_tree)
        modify_l1cache_node(btree, x);

    if (!ks.fixed) {
        if (x->flags & LEAF_NODE) {
	    pvlk_delete = (node_vlkey_t *) pk_delete;
	    datalen_stats = pvlk_delete->datalen;
	    keylen = pvlk_delete->keylen;
	    if ((pvlk_delete->keylen + pvlk_delete->datalen) >= btree->big_object_size) {
	        // data NOT stored in the node
		datalen = 0;
                delete_overflow_data(ret, btree, pvlk_delete->ptr, pvlk_delete->datalen, modify_tree);
	    } else {
	        // data IS stored in the node
		datalen = pvlk_delete->datalen;
	    }
	} else {
	    pvk_delete = (node_vkey_t *) pk_delete;
	    keylen = pvk_delete->keylen;
	    datalen_stats = 0;
	}
	fixed_bytes = ks.offset;
	nkeys_to = (((char *) pk_delete) - ((char *) x->keys))/ks.offset;
    } else {
        fixed_bytes = sizeof(node_fkey_t);
	nkeys_to = (((char *) pk_delete) - ((char *) x->keys))/sizeof(node_fkey_t);
	datalen_stats = 0;
    }

    if (x->flags & LEAF_NODE) {
	btree->stats.stat[BTSTAT_LEAVE_BYTES] -= (keylen + datalen_stats);
    } else {
	btree->stats.stat[BTSTAT_NONLEAVE_BYTES] -= (keylen + datalen_stats);
    }

    nkeys_from = x->nkeys - nkeys_to - 1;

    if (!ks.fixed) {
	assert(keylen);
	//  remove variable portion of key
        if (x->flags & LEAF_NODE) {
	    memmove((char *) x + x->insert_ptr + keylen + datalen, 
		    (char *) x + x->insert_ptr, 
		    pvlk_delete->keypos - x->insert_ptr);
	    x->insert_ptr += (keylen + datalen);
	} else {
	    memmove((char *) x + x->insert_ptr + keylen, 
		    (char *) x + x->insert_ptr, 
		    pvk_delete->keypos - x->insert_ptr);
	    x->insert_ptr += keylen;
	}
    }

    //  Remove fixed portion of deleted key.
    // 
    //  NOTE: This MUST be done after deleting the variable part
    //        because the variable part uses key data in its old location!
    //

    memmove((char *) (x->keys) + nkeys_to*fixed_bytes, (char *) (x->keys) + (nkeys_to+1)*fixed_bytes, nkeys_from*fixed_bytes);

    //  Do this here because update_keypos() requires it!
    x->nkeys -= 1;

    //  delete fixed portion of new key
    if (!ks.fixed) {
	//  update all of the 'keypos' fields in the fixed portion
	update_keypos(btree, x, 0);
    } else {
	x->insert_ptr = 0; // xxxzzz this should be redundant!
    }

    #if 0 //def DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", pvlk_delete->key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, pvlk_delete->key, pvlk_delete->keylen);
		len = pvlk_delete->keylen;
	    }
	    fprintf(stderr, "********  After delete_key '%s' [syn %lu]:  *******\n", dump_key(stmp, len), syndrome);
	    btree_raw_dump(stderr, btree);
	}
    #endif
}

static void delete_key(int *ret, btree_raw_t *btree, btree_raw_node_t *node, char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome)
{
    uint64_t       child_id;
    node_key_t    *pk_delete;

    if (*ret) { return; }

    pk_delete = bsearch_key(btree, node, key, keylen, &child_id, meta, syndrome);

    if (pk_delete == NULL) {
	*ret = 1; // key not found!
	return;
    }

    delete_key_by_pkrec(ret, btree, node, pk_delete, MODIFY_TREE);
}

static void btree_split_child(int *ret, btree_raw_t *btree, btree_raw_node_t *n_parent, btree_raw_node_t *n_child, uint64_t seqno, btree_metadata_t *meta, uint64_t syndrome)
{
    btree_raw_node_t     *n_new;
    uint32_t              keylen = 0;
    char                 *key = NULL;
    uint64_t              split_syndrome = 0;

    if (*ret) { return; }

    btree->stats.stat[BTSTAT_SPLITS]++;

    n_new = get_new_node(ret, btree, is_leaf(btree, n_child) ? LEAF_NODE : 0);

    // n_parent will be marked modified by insert_key()
    // n_new was marked in get_new_node()
    modify_l1cache_node(btree, n_child);

    split_copy(ret, btree, n_child, n_new, &key, &keylen, &split_syndrome);
    
    if (!(*ret)) {
	//  Add the split key in the parent
	insert_key(ret, btree, n_parent, key, keylen, seqno, sizeof(uint64_t), (char *) &(n_new->logical_id), meta, split_syndrome);

	btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, n_parent);
	btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, n_child);
	btree->log_cb(ret, btree->log_cb_data, BTREE_CREATE_NODE, btree, n_new);
    }

	//fprintf(stderr, "%x %s ret=%d\n", (int)pthread_self(), __FUNCTION__, *ret);

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    fprintf(stderr, "********  After btree_split_child (id_child='%ld'):  *******\n", n_child->logical_id);
	    btree_raw_dump(stderr, btree);
	}
    #endif
}

//  Check if a node has enough space for insertion
//  of a totally new item.
static int is_full_insert(btree_raw_t *btree, btree_raw_node_t *n, uint32_t keylen, uint64_t datalen)
{
    int        ret = 0;
    uint32_t   nbytes_free;

    if (n->flags & LEAF_NODE) {
        // vlkey
	nbytes_free = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*sizeof(node_vlkey_t);
	if ((keylen + datalen) >= btree->big_object_size) { // xxxzzz check this!
	    //  don't include datalen because object data is kept
	    //  in overflow btree nodes
	    if (nbytes_free < (sizeof(node_vlkey_t) + keylen)) {
		ret = 1;
	    }
	} else {
	    if (nbytes_free < (sizeof(node_vlkey_t) + keylen + datalen)) {
		ret = 1;
	    }
	}
    } else if (btree->flags & SECONDARY_INDEX) {
        // vkey
	nbytes_free = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*sizeof(node_vkey_t);
	// if (nbytes_free < (sizeof(node_vkey_t) + keylen)) {
	if (nbytes_free < (sizeof(node_vkey_t) + btree->max_key_size)) {
	    ret = 1;
	}
    } else {
        // fkey
	if (n->nkeys > (btree->fkeys_per_node-1)) {
	    ret = 1;
	}
    }

    return(ret);
}

//  Check if a leaf node has enough space for an update of
//  an existing item.
static int is_full_update(btree_raw_t *btree, btree_raw_node_t *n, node_vlkey_t *pvlk, uint32_t keylen, uint64_t datalen)
{
    int        ret = 0;
    uint32_t   nbytes_free;
    uint64_t   update_bytes;

    assert(n->flags & LEAF_NODE);  //  xxxzzz remove this

    if ((keylen + datalen) >= btree->big_object_size) { // xxxzzz check this!
        //  updated data will be put in overflow node(s)
        update_bytes = keylen;
    } else {
        //  updated data fits in a node
        update_bytes = keylen + datalen;
    }

    // must be vlkey!
    nbytes_free = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*sizeof(node_vlkey_t);
    if ((pvlk->keylen + pvlk->datalen) >= btree->big_object_size) { // xxxzzz check this!
        //  Data to be overwritten is in overflow node(s).
	if ((nbytes_free + pvlk->keylen) < update_bytes) {
	    ret = 1;
	}
    } else {
        //  Data to be overwritten is in this node.
	if ((nbytes_free + pvlk->keylen + pvlk->datalen) < update_bytes) {
	    ret = 1;
	}
    }

    return(ret);
}

static int is_node_full(btree_raw_t *bt, btree_raw_node_t *r, char *key, uint32_t keylen, uint64_t datalen, btree_metadata_t *meta, uint64_t syndrome, int write_type, node_key_t* pkrec)
{
    node_vlkey_t  *pvlk;
    int            full;

    if (r->flags & LEAF_NODE) {
        pvlk = (node_vlkey_t *) pkrec;
	if (pvlk == NULL) {
	    full = is_full_insert(bt, r, keylen, datalen);
	} else {
	  // key found for update or set
	  full = is_full_update(bt, r, pvlk, keylen, datalen);
	}
    } else {
        //  non-leaf nodes
	if (pkrec == NULL) {
	    full = is_full_insert(bt, r, keylen, datalen);
	} else if (bt->flags & SECONDARY_INDEX) {
	    // must be enough room for max sized key in case child is split!
	    full = is_full_insert(bt, r, keylen, datalen);
	} else {
	    // SYNDROME_INDEX
	    // must be enough room for max sized key in case child is split!
	    full = is_full_insert(bt, r, keylen, datalen);
	}
    }
    return full;
}

static int btree_raw_write_low(btree_raw_t *btree, char *key, uint32_t keylen, uint64_t seqno, uint64_t datalen, char *data, btree_metadata_t *meta, uint64_t syndrome, int write_type, int* pathcnt)
{
    int               ret = 0, modify_tree = 0;
    int32_t           nkey_child;
    uint64_t          child_id, child_id_before, child_id_after;
    node_key_t       *pk_insert, *pkrec = NULL;
    plat_rwlock_t    *leaf_lock;
    btree_raw_node_t *node = NULL, *parent = NULL;

    plat_rwlock_rdlock(&btree->lock);

restart:
    child_id = btree->rootid;

    while(child_id != BAD_CHILD) {
        if(!(node = get_existing_node_low(&ret, btree, child_id, &leaf_lock, modify_tree)))
            goto err_exit;

        (*pathcnt)++;

        if(!modify_tree && is_leaf(btree, node))
            plat_rwlock_wrlock(leaf_lock);

        pkrec = find_key(btree, node, key, keylen, &child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);

        if (!is_node_full(btree, node, key, keylen, datalen, meta, syndrome, write_type, pkrec)) {
            if(!modify_tree && !is_root(btree, node))
                deref_l1cache_node(btree, parent);
            parent = node;
            continue;
        }

        /* Handle tree restructure case */
        if(!modify_tree) {
            if(is_leaf(btree, node))
                plat_rwlock_unlock(leaf_lock);

            if(!is_root(btree, node))
                deref_l1cache_node(btree, parent);

            deref_l1cache_node(btree, node);

            plat_rwlock_unlock(&btree->lock);
            plat_rwlock_wrlock(&btree->lock);

            modify_tree = 1;

            goto restart;
        }

        if(is_root(btree, node)) {
            parent = get_new_node(&ret, btree, 0 /* flags */);
            if(!parent)
                goto err_exit;

            parent->rightmost  = btree->rootid;
            btree->rootid = parent->logical_id;

            if (! savepersistent( btree))
                assert( 0);
        }

        btree_split_child(&ret, btree, parent, node, seqno, meta, syndrome);
        if(ret)
            goto err_exit;

        pkrec = find_key(btree, parent, key, keylen, &child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);
    }

    assert(is_leaf(btree, node));
    assert(leaf_lock);

    if ((write_type != W_UPDATE || pkrec) && (write_type != W_CREATE || !pkrec)) {
        insert_key_low(&ret, btree, node, key, keylen, seqno, datalen, data, meta, syndrome, pkrec, pk_insert, modify_tree);

        if(!modify_tree) {
            btree->write_node_cb(&ret, btree->write_node_cb_data, (uint64_t) key, (char*) node, btree->nodesize);
            btree->stats.stat[BTSTAT_L1WRITES]++;
        }

        btree->log_cb(&ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, node);
    }
    else //((write_type == W_UPDATE && !pkrec) || (write_type == W_CREATE && pkrec))
	ret = 2; // key not found for an update! or key was found for an insert!

    if(!modify_tree)
    {
        plat_rwlock_unlock(leaf_lock);
        deref_l1cache_node(btree, node);

        assert(!MapNEntries(btree->l1cache_refs));
        assert(!MapNEntries(btree->l1cache_mods));
    }
    else if (deref_l1cache(btree))
        ret = 1;

err_exit:
    plat_rwlock_unlock(&btree->lock);

    return ret;
}


static int btree_raw_write(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta, int write_type)
{
    int                 ret = 0;
    int                 pathcnt = 0;
    uint64_t            syndrome = get_syndrome(btree, key, keylen);

    #ifdef notdef
	if (syndrome == 13133508245666864056ULL) {
	    fprintf(stderr, "ZZZZZZZZ: AHA write!\n");
	}
    #endif

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    fprintf(stderr, "%x ********  Before btree_raw_write for key '%s' [syn=%lu]: ret=%d  *******\n", (int)pthread_self(), dump_key(key, keylen), syndrome, ret);
	}
    #endif

    ret = btree_raw_write_low(btree, key, keylen, meta->seqno, datalen, data, meta, syndrome, write_type, &pathcnt);

    //TODO change to atomics
    if (!ret) {
        switch (write_type) {
	    case W_CREATE:
		btree->stats.stat[BTSTAT_CREATE_CNT]++;
		btree->stats.stat[BTSTAT_CREATE_PATH] += pathcnt;
		break;
	    case W_SET:
		btree->stats.stat[BTSTAT_SET_CNT]++;
		btree->stats.stat[BTSTAT_SET_PATH] += pathcnt;
		break;
	    case W_UPDATE:
		btree->stats.stat[BTSTAT_UPDATE_CNT]++;
		btree->stats.stat[BTSTAT_UPDATE_PATH] += pathcnt;
		break;
	    default:
	        assert(0);
		break;
	}
    }

#ifdef BTREE_RAW_CHECK
    btree_raw_check(btree, __FUNCTION__, dump_key(key, keylen));
#endif

    return(ret);
}
//======================   INSERT  =========================================

int btree_raw_insert(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta)
{
    return(btree_raw_write(btree, key, keylen, data, datalen, meta, W_CREATE));
}

//======================   UPDATE  =========================================

int btree_raw_update(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta)
{
    return(btree_raw_write(btree, key, keylen, data, datalen, meta, W_UPDATE));
}

//======================   UPSERT (SET)  =========================================

int btree_raw_set(struct btree_raw *btree, char *key, uint32_t keylen, char *data, uint64_t datalen, btree_metadata_t *meta)
{
    return(btree_raw_write(btree, key, keylen, data, datalen, meta, W_SET));
}

//======================   DELETE   =========================================

static int is_leaf_minimal_after_delete(btree_raw_t *btree, btree_raw_node_t *n, node_vlkey_t* pk)
{
    assert(n->flags & LEAF_NODE);
    uint32_t datalen = ((pk->keylen + pk->datalen) < btree->big_object_size) ? pk->datalen : 0;
    uint32_t nbytes_used = (btree->nodesize - n->insert_ptr - pk->keylen - datalen) + (n->nkeys - 1) * sizeof(node_vlkey_t);
    return 2 * nbytes_used < btree->nodesize - sizeof(btree_raw_node_t);
}

static int is_minimal(btree_raw_t *btree, btree_raw_node_t *n, uint32_t l_balance_keylen, uint32_t r_balance_keylen)
{
    uint32_t   nbytes_used;
    int        ret = 0;
    uint32_t   max_balance_keylen;

    if (n->logical_id == btree->rootid) {
        // root
	if (!(n->flags & LEAF_NODE) && (n->nkeys == 0)) {
	    ret = 1;
	} else {
	    ret = 0;
	}
    } else {
        // non-root
	if (n->flags & LEAF_NODE) {
	    nbytes_used = (btree->nodesize - n->insert_ptr) + n->nkeys*sizeof(node_vlkey_t);
	} else if (btree->flags & SYNDROME_INDEX) {
	    //  The '+1' here is to allow for conversion of a rightmost pointer to
	    //  a key value during a merge!
	    nbytes_used = (n->nkeys + 1)*sizeof(node_fkey_t);
	} else {
	    max_balance_keylen = (l_balance_keylen > r_balance_keylen) ? l_balance_keylen : r_balance_keylen;
	    nbytes_used  = (btree->nodesize - n->insert_ptr) + n->nkeys*sizeof(node_vkey_t);
	    //  This allows for conversion of the rightmost 
	    //  pointer to a normal key, using the anchor key value.
	    nbytes_used  += max_balance_keylen + sizeof(node_vkey_t);
	}
	if ((2*nbytes_used) < (btree->nodesize - sizeof(btree_raw_node_t))) {
	    ret = 1;
	} else {
	    ret = 0;
	}
    }
    return(ret);
}

/*   delete a key
 *
 *   returns 0: success
 *   returns 1: error
 *   returns 2: key not found
 *
 *   Reference: "Implementing Deletion in B+-trees", Jan Jannink, SIGMOD RECORD,
 *              Vol. 24, No. 1, March 1995
 */
int btree_raw_delete(struct btree_raw *btree, char *key, uint32_t keylen, btree_metadata_t *meta)
{
    int                   ret=0, pathcnt = 0, opt;
    btree_raw_node_t     *node;
    plat_rwlock_t        *leaf_lock;
    node_key_t           *keyrec;
    uint64_t              syndrome = get_syndrome(btree, key, keylen);

    plat_rwlock_rdlock(&btree->lock);

    keyrec = btree_raw_find(btree, key, keylen, syndrome, meta, &node, &leaf_lock, &pathcnt);

    /* Check if delete without restructure is possible */
    opt = keyrec && _keybuf && !is_leaf_minimal_after_delete(btree, node, (node_vlkey_t*)keyrec);

    if(opt) {
	delete_key_by_pkrec(&ret, btree, node, keyrec, 0);
	btree->stats.stat[BTSTAT_DELETE_OPT_CNT]++;
    }

    deref_l1cache_node(btree, node);
    plat_rwlock_unlock(leaf_lock);

    plat_rwlock_unlock(&btree->lock);

    if(!keyrec || ret) return 1; // key not found

    if(opt) {
#ifdef BTREE_RAW_CHECK
	btree_raw_check(btree, __FUNCTION__, dump_key(key, keylen));
#endif
	return 0; // optimistic delete succeeded
    }

    /* Need tree restructure. Write lock whole tree and retry */
    plat_rwlock_wrlock(&btree->lock);

    // make sure that the temporary key buffer has been allocated
    if (check_per_thread_keybuf(btree)) {
        plat_rwlock_unlock(&btree->lock);

	return(1); // xxxzzz is this the best I can do?
    }

    (void) find_rebalance(&ret, btree, btree->rootid, BAD_CHILD, BAD_CHILD, BAD_CHILD, NULL, BAD_CHILD, NULL, 0, 0, key, keylen, meta, syndrome);

    if (deref_l1cache(btree)) {
        ret = 1;
    }

    plat_rwlock_unlock(&btree->lock);

    btree->stats.stat[BTSTAT_DELETE_CNT]++;
    btree->stats.stat[BTSTAT_DELETE_PATH] += pathcnt;

#ifdef BTREE_RAW_CHECK
    btree_raw_check(btree, __FUNCTION__, dump_key(key, keylen));
#endif

    return(ret);
}

/*   recursive deletion/rebalancing routine
 *
 *   ret = 0: don't rebalance this level
 *   ret = 1: rebalance this level if necessary
 *
 */
static int find_rebalance(int *ret, btree_raw_t *btree, uint64_t this_id, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent_in, int r_this_parent_in, char *key, uint32_t keylen, btree_metadata_t *meta, uint64_t syndrome)
{
    node_key_t         *keyrec;
    node_key_t         *pk_insert;
    btree_raw_node_t   *this_node, *left_node, *right_node;
    uint64_t            next_node, next_left, next_right, next_l_anchor, next_r_anchor;
    uint64_t            child_id, child_id_before, child_id_after;
    int                 l_this_parent, r_this_parent;
    key_stuff_t         ks, ks_l, ks_r;
    key_stuff_t        *next_l_anchor_stuff;
    key_stuff_t        *next_r_anchor_stuff;
    int32_t             nkey_child;
    int                 do_rebalance = 1;
    uint32_t            l_balance_keylen = 0;
    uint32_t            r_balance_keylen = 0;

    if (*ret) { return(0); }

    this_node = get_existing_node(ret, btree, this_id);
    assert(this_node != NULL); // xxxzzz remove this
    _pathcnt++;

    //  PART 1: recursive descent from root to leaf node

        //  find path in this node for key
    keyrec = find_key(btree, this_node, key, keylen, &child_id, &child_id_before, &child_id_after, &pk_insert, meta, syndrome, &nkey_child);

    next_node = child_id;

    if (is_leaf(btree, this_node)) {
        if (keyrec) {
	    // key found at leaf
            // remove entry from a leaf node
            delete_key(ret, btree, this_node, key, keylen, meta, syndrome);
            btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, this_node);
	} else {
	    // key NOT found at leaf
	    *ret = 1;
	}
    } else {
        //   this node is internal

	    // calculate neighbor and anchor nodes
	if (child_id_before == BAD_CHILD) {
	    // next_node is least entry in this_node
	    if (left_id != BAD_CHILD) {
		left_node = get_existing_node(ret, btree, left_id);
		next_left = left_node->rightmost;
	    } else {
		next_left = BAD_CHILD;
	    }
	    next_l_anchor       = l_anchor_id;
	    next_l_anchor_stuff = l_anchor_stuff;
	    l_this_parent       = 0;
	    if (l_anchor_stuff == NULL) {
	        l_balance_keylen = 0;
	    } else {
	        l_balance_keylen = l_anchor_stuff->keylen;
	    }
	} else {
	    next_left           = child_id_before;
	    next_l_anchor       = this_node->logical_id;
	    (void) get_key_stuff(btree, this_node, nkey_child - 1, &ks_l);
	    next_l_anchor_stuff = &ks_l;
	    l_this_parent       = 1;
	    l_balance_keylen    = ks_l.keylen;
	}

	if (child_id_after == BAD_CHILD) {
	    // next_node is greatest entry in this_node
	    if (right_id != BAD_CHILD) {
		right_node = get_existing_node(ret, btree, right_id);
		assert(right_node); // xxxzzz fix this!
		(void) get_key_stuff(btree, right_node, 0, &ks);
		next_right = ks.ptr;
	    } else {
		next_right = BAD_CHILD;
	    }
	    next_r_anchor       = r_anchor_id;
	    next_r_anchor_stuff = r_anchor_stuff;
	    r_this_parent       = 0;
	    if (r_anchor_stuff == NULL) {
	        r_balance_keylen = 0;
	    } else {
	        r_balance_keylen = r_anchor_stuff->keylen;
	    }
	} else {
	    next_right          = child_id_after;
	    next_r_anchor       = this_node->logical_id;
	    (void) get_key_stuff(btree, this_node, nkey_child, &ks_r);
	    next_r_anchor_stuff = &ks_r;
	    r_this_parent       = 1;
	    r_balance_keylen    = ks_r.keylen;
	}

	    // recursive call
	do_rebalance = find_rebalance(ret, btree, next_node, next_left, next_right, next_l_anchor, next_l_anchor_stuff, next_r_anchor, next_r_anchor_stuff, l_this_parent, r_this_parent, key, keylen, meta, syndrome);
    }

	//  does this node need to be rebalanced?
    if ((!do_rebalance) || (!is_minimal(btree, this_node, l_balance_keylen, r_balance_keylen)))
	return 0;

    if (this_id == btree->rootid) {
        collapse_root(ret, btree, this_node);
	return 0;
    }

    return rebalance(ret, btree, this_node, left_id, right_id, l_anchor_id, l_anchor_stuff, r_anchor_id, r_anchor_stuff, l_this_parent_in, r_this_parent_in, meta);
}

static void collapse_root(int *ret, btree_raw_t *btree, btree_raw_node_t *old_root_node)
{
    if (*ret) { return; }

    if (is_leaf(btree, old_root_node)) {
	//  just keep old empty root node
        if (old_root_node->nkeys != 0) {
	    *ret = 1; // this should never happen!
	}
    } else {
	assert(old_root_node->nkeys == 0);
	assert(old_root_node->rightmost != BAD_CHILD);
	btree->rootid = old_root_node->rightmost;
        if (! savepersistent( btree))
                assert( 0);
	free_node(ret, btree, old_root_node);
    }
    return;
}

static void update_ptr(btree_raw_t *btree, btree_raw_node_t *n, uint32_t nkey, uint64_t ptr)
{
    node_vlkey_t  *pvlk;
    node_vkey_t   *pvk;
    node_fkey_t   *pfk;

    if (n->flags & LEAF_NODE) {
	pvlk = ((node_vlkey_t *) n->keys) + nkey;
	pvlk->ptr = ptr;
    } else if (btree->flags & SECONDARY_INDEX) {
	pvk      = ((node_vkey_t *) n->keys) + nkey;
	pvk->ptr = ptr;
    } else {
	pfk      = ((node_fkey_t *) n->keys) + nkey;
	pfk->ptr = ptr;
    }
}

static int rebalance(int *ret, btree_raw_t *btree, btree_raw_node_t *this_node, uint64_t left_id, uint64_t right_id, uint64_t l_anchor_id, key_stuff_t *l_anchor_stuff, uint64_t r_anchor_id, key_stuff_t *r_anchor_stuff, int l_this_parent, int r_this_parent, btree_metadata_t *meta)
{
    btree_raw_node_t     *left_node, *right_node;
    btree_raw_node_t     *balance_node;
    btree_raw_node_t     *anchor_node;
    btree_raw_node_t     *merge_node;
    char                 *s_key;
    uint32_t              s_keylen;
    uint64_t              s_syndrome;
    uint64_t              s_seqno;
    uint64_t              s_ptr;
    char                 *r_key = NULL;
    uint32_t              r_keylen = 0;
    uint64_t              r_syndrome = 0;
    uint64_t              r_seqno = 0;
    uint32_t              balance_keylen;
    key_stuff_t           ks;
    int                   balance_node_is_sibling;
    int                   next_do_rebalance = 0;

    if (*ret) { return(0); }

    if (left_id == BAD_CHILD) {
        left_node = NULL;
    } else {
	left_node = get_existing_node(ret, btree, left_id);
    }

    if (right_id == BAD_CHILD) {
        right_node = NULL;
    } else {
	right_node = get_existing_node(ret, btree, right_id);
    }

    if (left_node == NULL) {
        balance_node   = right_node;
	balance_keylen = r_anchor_stuff->keylen;
    } else if (right_node == NULL) {
        balance_node   = left_node;
	balance_keylen = l_anchor_stuff->keylen;
    } else {
        // give siblings preference
	if (l_this_parent && (!r_this_parent)) {
	    balance_node   = left_node;
	    balance_keylen = l_anchor_stuff->keylen;
	} else if (r_this_parent && (!l_this_parent)) {
	    balance_node   = right_node;
	    balance_keylen = r_anchor_stuff->keylen;
        } else if (left_node->insert_ptr > right_node->insert_ptr) {
	    balance_node   = right_node;
	    balance_keylen = r_anchor_stuff->keylen;
	} else {
	    balance_node   = left_node;
	    balance_keylen = l_anchor_stuff->keylen;
	}
    }
    if (balance_node == left_node) {
        if (l_this_parent) {
	    balance_node_is_sibling = 1;
	} else {
	    balance_node_is_sibling = 0;
	}
    } else {
        if (r_this_parent) {
	    balance_node_is_sibling = 1;
	} else {
	    balance_node_is_sibling = 0;
	}
    }

    assert(balance_node != NULL);

    if ((!is_minimal(btree, balance_node, balance_keylen, 0)) ||
        (!balance_node_is_sibling))
    {
        next_do_rebalance = 0;
        if (balance_node == left_node) {
	    anchor_node    = get_existing_node(ret, btree, l_anchor_id);

	    s_key      = l_anchor_stuff->pkey_val;
	    s_keylen   = l_anchor_stuff->keylen;
	    s_syndrome = l_anchor_stuff->syndrome;
	    s_seqno    = l_anchor_stuff->seqno;
	    s_ptr      = l_anchor_stuff->ptr;

	    shift_right(ret, btree, anchor_node, balance_node, this_node, s_key, s_keylen, s_syndrome, s_seqno, &r_key, &r_keylen, &r_syndrome, &r_seqno);

            if (r_key != NULL) {
		// update keyrec in anchor
		delete_key(ret, btree, anchor_node, s_key, s_keylen, meta, s_syndrome);
		insert_key(ret, btree, anchor_node, r_key, r_keylen, r_seqno, sizeof(uint64_t), (char *) &s_ptr, meta, r_syndrome);
	    }
	} else {
	    anchor_node    = get_existing_node(ret, btree, r_anchor_id);

	    s_key      = r_anchor_stuff->pkey_val;
	    s_keylen   = r_anchor_stuff->keylen;
	    s_syndrome = r_anchor_stuff->syndrome;
	    s_seqno    = r_anchor_stuff->seqno;
	    s_ptr      = r_anchor_stuff->ptr;

	    shift_left(ret, btree, anchor_node, balance_node, this_node, s_key, s_keylen, s_syndrome, s_seqno, &r_key, &r_keylen, &r_syndrome, &r_seqno);

            if (r_key != NULL) {
		// update keyrec in anchor
		delete_key(ret, btree, anchor_node, s_key, s_keylen, meta, s_syndrome);
		insert_key(ret, btree, anchor_node, r_key, r_keylen, r_seqno, sizeof(uint64_t), (char *) &s_ptr, meta, r_syndrome);
	    }
	}

    } else {
        next_do_rebalance = 1;
        if (balance_node == left_node) {
	    //  left anchor is parent of this_node
	    anchor_node    = get_existing_node(ret, btree, l_anchor_id);
	    merge_node     = left_node;

	    s_key      = l_anchor_stuff->pkey_val;
	    s_keylen   = l_anchor_stuff->keylen;
	    s_syndrome = l_anchor_stuff->syndrome;
	    s_seqno    = l_anchor_stuff->seqno;

	    merge_left(ret, btree, anchor_node, this_node, merge_node, s_key, s_keylen, s_syndrome, s_seqno);

	    //  update the anchor
	    //  cases:
	    //       1) this_node is the rightmost pointer
	    //       2) this_node is NOT a rightmost pointer
	    //

            if (this_node->logical_id == anchor_node->rightmost) {
		//  child is the rightmost pointer
		// 
	        //  Make the 'rightmost' point to the merge_node,
		//  then delete the key for the merge_node.
		anchor_node->rightmost = l_anchor_stuff->ptr;
	    } else {
	        //  Make the key for 'this_node' point to the merge_node,
		//  then delete the key for the merge_node.
		//  Note that this_node corresponds to l_anchor_stuff->nkey+1!
		// 
	        update_ptr(btree, anchor_node, l_anchor_stuff->nkey+1, l_anchor_stuff->ptr);
	    }
	    delete_key(ret, btree, anchor_node, l_anchor_stuff->pkey_val, l_anchor_stuff->keylen, meta, l_anchor_stuff->syndrome);
	    btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, anchor_node);

	    // free this_node
	    if (!(*ret)) {
		free_node(ret, btree, this_node);
	    }

	} else {
	    //  Since the left anchor is not the parent of this_node,
	    //  the right anchor MUST be parent of this_node.
	    //  Also, this_node must be key number 0.

	    assert(r_this_parent);
	    anchor_node    = get_existing_node(ret, btree, r_anchor_id);
	    merge_node     = right_node;

	    s_key      = r_anchor_stuff->pkey_val;
	    s_keylen   = r_anchor_stuff->keylen;
	    s_syndrome = r_anchor_stuff->syndrome;
	    s_seqno    = r_anchor_stuff->seqno;

	    merge_right(ret, btree, anchor_node, this_node, merge_node, s_key, s_keylen, s_syndrome, s_seqno);

	    //  update the anchor
	    // 
	    //  Just delete this_node.  
	    //  Whether or not the merge_node is the rightmost pointer,
	    //  the separator for the merge key is still valid after the
	    //  'this_key' is deleted.
	    //  

	    //  If anchor is 'rightmost', r_anchor_stuff holds data for 'this_node'.
	    //  Otherwise, r_anchor_stuff holds data for the node to the
	    //  immediate right of 'this_node'.

            //  Get data for 'this_node'.
            if (r_anchor_stuff->ptr == this_node->logical_id) {
	        //  Anchor is 'rightmost' node.
		//  Delete key for 'this_node'.
		delete_key(ret, btree, anchor_node, r_anchor_stuff->pkey_val, r_anchor_stuff->keylen, meta, r_anchor_stuff->syndrome);
	    } else {
	        //  Anchor is NOT 'rightmost' node.
		//  Delete key for 'this_node'.
		(void) get_key_stuff(btree, anchor_node, r_anchor_stuff->nkey-1, &ks);
		delete_key(ret, btree, anchor_node, ks.pkey_val, ks.keylen, meta, ks.syndrome);
	    }
	    btree->log_cb(ret, btree->log_cb_data, BTREE_UPDATE_NODE, btree, anchor_node);

	    // free this_node
	    if (!(*ret)) {
		free_node(ret, btree, this_node);
	    }
	}
    }

    return(next_do_rebalance);
}

static int check_per_thread_keybuf(btree_raw_t *btree)
{
    //  Make sure that the per-thread key buffer has been allocated,
    //  and that it is big enough!
    if (_keybuf_size < btree->nodesize) {
	if (_keybuf != NULL) {
	    free(_keybuf);
	    _keybuf_size = 0;
	}
	_keybuf = malloc(btree->nodesize);
	if (_keybuf == NULL) {
	    return(1);
	}
	_keybuf_size = btree->nodesize;
    }
    return(0);
}

/*   Equalize keys between 'from' node and 'to' node, given that 'to' is to right of 'from'.
 */
static void shift_right(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno, char **r_key_out, uint32_t *r_keylen_out, uint64_t *r_syndrome_out, uint64_t *r_seqno_out)
{
    int            i;
    uint32_t       threshold;
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    uint32_t       nbytes_fixed;
    uint32_t       nbytes_free;
    uint32_t       nbytes_needed;
    uint32_t       nkeys_shift;
    uint32_t       nbytes_shift;
    uint32_t       nbytes_shift_old;
    key_stuff_t    ks;
    uint32_t       nbytes_f;
    uint32_t       nbytes_t;
    uint32_t       nbytes;

    char          *r_key;
    uint32_t       r_keylen;
    uint64_t       r_syndrome;
    uint64_t       r_seqno;
    uint64_t       r_ptr;

    if (*ret) { return; }

    btree->stats.stat[BTSTAT_RSHIFTS]++;

    (void) get_key_stuff(btree, from, 0, &ks);
    nbytes_fixed = ks.offset;

    if (ks.fixed) {
        if (from->nkeys <= to->nkeys) {
	    *r_key_out = NULL;
	    return;
	}
	// xxxzzz should the following takes into account the inclusion of the anchor separator key?
        nkeys_shift = (from->nkeys - to->nkeys)/2;
	if (nkeys_shift == 0) {
	    nkeys_shift = 1;
	}
        nbytes_shift = nkeys_shift*ks.offset;
	pfk = (node_fkey_t *) ((char *) from->keys + (from->nkeys - nkeys_shift)*nbytes_fixed);
	r_key      = (char *) pfk->key;
	r_keylen   = sizeof(uint64_t);
	r_syndrome = pfk->key;
	r_seqno    = pfk->seqno;
	r_ptr      = pfk->ptr;

    } else {

        nkeys_shift  = 0;
	nbytes_shift = 0;
	nbytes_f     = (btree->nodesize - from->insert_ptr) + from->nkeys*nbytes_fixed;
	nbytes_t     = (btree->nodesize - to->insert_ptr)   + to->nkeys*nbytes_fixed;
	if ((nbytes_f <= nbytes_t) || (from->nkeys <= 1)) {
	    *r_key_out = NULL;
	    return;
	}
        threshold    = (nbytes_f - nbytes_t)/2;

        nbytes_shift_old = 0;
	for (i=0; i<from->nkeys; i++) {
	    (void) get_key_stuff(btree, from, from->nkeys - 1 - i, &ks);
	    nbytes = ks.keylen;
	    if (ks.leaf) {
		if ((ks.keylen + ks.datalen) < btree->big_object_size) { // xxxzzz check this!
		    nbytes += ks.datalen;
		}
	    }
	    nbytes_shift_old = nbytes_shift;
	    nbytes_shift += nbytes;
	    nkeys_shift++;
	    if (ks.leaf) {
		if ((nbytes_shift + nkeys_shift*nbytes_fixed) >= threshold) {
		    break;
		}
	    } else {
		// the following takes into account the inclusion of the anchor separator key!
		if ((nbytes_shift + nkeys_shift*nbytes_fixed + (s_keylen - ks.keylen)) >= threshold) {
		    break;
		}
	    }
	}
	assert(i < from->nkeys); // xxxzzz remove this!

	if (nkeys_shift >= from->nkeys) {
	    nkeys_shift--;
	    nbytes_shift = nbytes_shift_old;
	}

	if (ks.leaf) {
	    pvlk = (node_vlkey_t *) ((char *) from->keys + (from->nkeys - nkeys_shift - 1)*nbytes_fixed);
	    // copy the key into the non-volatile per-thread buffer
	    assert(_keybuf);
	    memcpy(_keybuf, (char *) from + pvlk->keypos, pvlk->keylen);
	    r_key      = _keybuf;
	    r_keylen   = pvlk->keylen;
	    r_syndrome = pvlk->syndrome;
	    r_seqno    = pvlk->seqno;
	    r_ptr      = pvlk->ptr;
	} else {
	    pvk = (node_vkey_t *) ((char *) from->keys + (from->nkeys - nkeys_shift)*nbytes_fixed);
	    // copy the key into the non-volatile per-thread buffer
	    assert(_keybuf);
	    memcpy(_keybuf, (char *) from + pvk->keypos, pvk->keylen);
	    r_key      = _keybuf;
	    r_keylen   = pvk->keylen;
	    r_syndrome = 0;
	    r_seqno    = pvk->seqno;
	    r_ptr      = pvk->ptr;
	}
    }

    if (ks.leaf) {
	nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vlkey_t);
	nbytes_needed  = nbytes_shift + nkeys_shift*sizeof(node_vlkey_t);
	assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	// make room for the lower fixed keys
	memmove((char *) to->keys + nkeys_shift*nbytes_fixed, (char *) to->keys, to->nkeys*nbytes_fixed);

	// copy the fixed size portion of the keys
	memcpy(to->keys, (char *) from->keys + (from->nkeys - nkeys_shift)*nbytes_fixed, nkeys_shift*nbytes_fixed);
	to->nkeys   = to->nkeys + nkeys_shift;
	from->nkeys = from->nkeys - nkeys_shift;

    } else {
	if (ks.fixed) {
	    assert((to->nkeys + nkeys_shift) <= btree->fkeys_per_node); // xxxzzz remove this!
	} else {
	    nbytes_free    = to->insert_ptr   - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vkey_t);
	    nbytes_needed  = (nbytes_shift - r_keylen + s_keylen) + nkeys_shift*sizeof(node_vkey_t);
	    assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	}

	// make room for the lower fixed keys, plus the separator from the anchor
	memmove((char *) to->keys + nkeys_shift*nbytes_fixed, (char *) to->keys, to->nkeys*nbytes_fixed);
	// copy the fixed size portion of the keys
	memcpy(to->keys, (char *) from->keys + (from->nkeys - nkeys_shift + 1)*nbytes_fixed, (nkeys_shift - 1)*nbytes_fixed);
	to->nkeys   = to->nkeys + nkeys_shift;
	from->nkeys = from->nkeys - nkeys_shift; // convert last key to rightmost pointer

	// copy 'from' rightmost pointer
	if (ks.fixed) {
	    pfk = (node_fkey_t *) ((char *) to->keys + (nkeys_shift - 1)*nbytes_fixed);
	    pfk->key   = (uint64_t) s_key;
	    pfk->ptr   = from->rightmost;
	    pfk->seqno = s_seqno;
	} else {
	    pvk = (node_vkey_t *) ((char *) to->keys + (nkeys_shift - 1)*nbytes_fixed);
	    pvk->keylen = s_keylen;
	    pvk->keypos = 0; // will be set in update_keypos below
	    pvk->ptr    = from->rightmost;
	    pvk->seqno  = s_seqno;
	}
    }

    // copy variable sized stuff

    if (ks.fixed) {
	to->insert_ptr = 0;
    } else {
	// for variable sized keys, copy the variable sized portion
	//  For leaf nodes, copy the data too

        if (ks.leaf) {
	    memcpy(((char *) to) + to->insert_ptr - nbytes_shift, 
		   ((char *) from) + btree->nodesize - nbytes_shift,
		   nbytes_shift);
	    // clean up 'from' variable stuff
	    memmove(((char *) from) + from->insert_ptr + nbytes_shift, 
		    ((char *) from) + from->insert_ptr,
		    (btree->nodesize - from->insert_ptr) - nbytes_shift);

	    to->insert_ptr   = to->insert_ptr   - nbytes_shift;
	    from->insert_ptr = from->insert_ptr + nbytes_shift;
	} else {
	    //  for non-leaves, include the 'right' pointer from the 'from' node

	    memcpy(((char *) to) + to->insert_ptr - (nbytes_shift - r_keylen) - s_keylen, 
		   ((char *) from) + btree->nodesize - (nbytes_shift - r_keylen),
		   nbytes_shift - r_keylen);
	    memcpy(((char *) to) + to->insert_ptr - s_keylen, s_key, s_keylen);

	    // clean up 'from' variable stuff
	    memmove(((char *) from) + from->insert_ptr + nbytes_shift, 
		    ((char *) from) + from->insert_ptr,
		    btree->nodesize - from->insert_ptr - nbytes_shift);

	    to->insert_ptr   = to->insert_ptr   - (nbytes_shift - r_keylen) - s_keylen;
	    from->insert_ptr = from->insert_ptr + (nbytes_shift - r_keylen) + r_keylen;
	}
	
	//  update the keypos pointers
        update_keypos(btree, to,   0);
        update_keypos(btree, from, 0);
    }

    // update the rightmost pointer of the 'from' node
    from->rightmost = r_ptr;

    *r_key_out      = r_key;
    *r_keylen_out   = r_keylen;
    *r_syndrome_out = r_syndrome;
    *r_seqno_out    = r_seqno;

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", s_key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, s_key, s_keylen);
		len = s_keylen;
	    }
	    fprintf(stderr, "********  After shift_right for key '%s' [syn=%lu] (from=%p, to=%p) B-Tree:  *******\n", dump_key(stmp, len), s_syndrome, from, to);
	    btree_raw_dump(stderr, btree);
	}
    #endif

    return;
}

/*   Equalize keys between 'from' node and 'to' node, given that 'to' is to the left of 'from'.
 */
static void shift_left(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno, char **r_key_out, uint32_t *r_keylen_out, uint64_t *r_syndrome_out, uint64_t *r_seqno_out)
{
    int            i;
    uint32_t       threshold;
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    node_vlkey_t  *pvlk;
    uint32_t       nbytes_fixed;
    uint32_t       nbytes_free;
    uint32_t       nbytes_needed;
    uint32_t       nkeys_shift;
    uint32_t       nbytes_shift;
    uint32_t       nbytes_shift_old;
    key_stuff_t    ks;
    uint32_t       nbytes_f;
    uint32_t       nbytes_t;
    uint32_t       nbytes_to;
    uint32_t       nbytes;

    char          *r_key;
    uint32_t       r_keylen;
    uint64_t       r_syndrome;
    uint64_t       r_seqno;
    uint64_t       r_ptr;

    if (*ret) { return; }
    btree->stats.stat[BTSTAT_LSHIFTS]++;

    (void) get_key_stuff(btree, from, 0, &ks);
    nbytes_fixed = ks.offset;

    if (ks.fixed) {
        if (from->nkeys <= to->nkeys) {
	    *r_key_out = NULL;
	    return;
	}
	// xxxzzz should the following takes into account the inclusion of the anchor separator key?
        nkeys_shift = (from->nkeys - to->nkeys)/2;
	if (nkeys_shift == 0) {
	    nkeys_shift = 1; // always shift at least one key!
	}
        nbytes_shift = nkeys_shift*ks.offset;
	pfk = (node_fkey_t *) ((char *) from->keys + (nkeys_shift - 1)*nbytes_fixed);
	r_key      = (char *) pfk->key;
	r_keylen   = sizeof(uint64_t);
	r_syndrome = pfk->key;
	r_seqno    = pfk->seqno;
	r_ptr      = pfk->ptr;

    } else {

        nkeys_shift  = 0;
	nbytes_shift = 0;
	nbytes_f     = (btree->nodesize - from->insert_ptr) + from->nkeys*nbytes_fixed;
	nbytes_t     = (btree->nodesize - to->insert_ptr)   + to->nkeys*nbytes_fixed;
	if ((nbytes_f <= nbytes_t) || (from->nkeys <= 1)) {
	    *r_key_out = NULL;
	    return;
	}
        threshold    = (nbytes_f - nbytes_t)/2;

        nbytes_shift_old = 0;
	for (i=0; i<from->nkeys; i++) {
	    (void) get_key_stuff(btree, from, i, &ks);
	    nbytes = ks.keylen;
	    if (ks.leaf) {
		if ((ks.keylen + ks.datalen) < btree->big_object_size) { // xxxzzz check this!
		    nbytes += ks.datalen;
		}
	    }
	    nbytes_shift_old = nbytes_shift;
	    nbytes_shift    += nbytes;
	    nkeys_shift++;
	    if (ks.leaf) {
		if ((nbytes_shift + nkeys_shift*nbytes_fixed) >= threshold) {
		    break;
		}
	    } else {
		// the following takes into account the inclusion of the anchor separator key!
		if ((nbytes_shift + nkeys_shift*nbytes_fixed + (s_keylen - ks.keylen)) >= threshold) {
		    break;
		}
	    }
	}
	assert(i < from->nkeys); // xxxzzz remove this!
	if (nkeys_shift >= from->nkeys) {
	    nkeys_shift--;
	    nbytes_shift = nbytes_shift_old;
	}

	if (ks.leaf) {
	    pvlk = (node_vlkey_t *) ((char *) from->keys + (nkeys_shift - 1)*nbytes_fixed);
	    // copy the key into the non-volatile per-thread buffer
	    assert(_keybuf);
	    memcpy(_keybuf, (char *) from + pvlk->keypos, pvlk->keylen);
	    r_key      = _keybuf;
	    r_keylen   = pvlk->keylen;
	    r_syndrome = pvlk->syndrome;
	    r_seqno    = pvlk->seqno;
	    r_ptr      = pvlk->ptr;
	} else {
	    pvk = (node_vkey_t *) ((char *) from->keys + (nkeys_shift - 1)*nbytes_fixed);
	    // copy the key into the non-volatile per-thread buffer
	    assert(_keybuf);
	    memcpy(_keybuf, (char *) from + pvk->keypos, pvk->keylen);
	    r_key      = _keybuf;
	    r_keylen   = pvk->keylen;
	    r_syndrome = 0;
	    r_seqno    = pvk->seqno;
	    r_ptr      = pvk->ptr;
	}
    }

    if (ks.leaf) {
	nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vlkey_t);
	nbytes_needed  = nbytes_shift + nkeys_shift*sizeof(node_vlkey_t);
	assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!

	// copy the fixed size portion of the keys
	memcpy((char *) to->keys + to->nkeys*nbytes_fixed, from->keys, nkeys_shift*nbytes_fixed);

	// remove keys from 'from' node
	memmove(from->keys, (char *) from->keys + nkeys_shift*nbytes_fixed, (from->nkeys - nkeys_shift)*nbytes_fixed);

	to->nkeys   = to->nkeys + nkeys_shift;
	from->nkeys = from->nkeys - nkeys_shift;

    } else {
	if (ks.fixed) {
	    //  this allows for the conversion of the 'to' right ptr to a regular key:
	    assert((to->nkeys + nkeys_shift) <= btree->fkeys_per_node); // xxxzzz remove this!
	} else {
	    nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vkey_t);
	    nbytes_needed  = (nbytes_shift - r_keylen + s_keylen) + nkeys_shift*sizeof(node_vkey_t);
	    assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	}

	// copy the fixed size portion of the keys
	    //  this allows for the conversion of the 'to' right ptr to a regular key:
	memcpy((char *) to->keys + (to->nkeys + 1)*nbytes_fixed, from->keys, (nkeys_shift - 1)*nbytes_fixed);

	// remove keys from 'from' node
	memmove(from->keys, (char *) from->keys + nkeys_shift*nbytes_fixed, (from->nkeys - nkeys_shift)*nbytes_fixed);

	// convert 'to' rightmost pointer into a regular key
	if (ks.fixed) {
	    pfk = (node_fkey_t *) ((char *) to->keys + to->nkeys*nbytes_fixed);
	    pfk->key   = (uint64_t) s_key;
	    pfk->ptr   = to->rightmost;
	    pfk->seqno = s_seqno;
	} else {
	    pvk = (node_vkey_t *) ((char *) to->keys + to->nkeys*nbytes_fixed);
	    pvk->keylen = s_keylen;
	    pvk->keypos = 0; // will be set in update_keypos below
	    pvk->ptr    = to->rightmost;
	    pvk->seqno  = s_seqno;
	}
	
	//  Update nkeys AFTER converting rightmost pointer so that nkeys math
	//  above is correct!

	to->nkeys   = to->nkeys + nkeys_shift;
	from->nkeys = from->nkeys - nkeys_shift; // convert last key to rightmost pointer
    }

    // copy variable sized stuff

    if (ks.fixed) {
	to->insert_ptr = 0;
    } else {
	// for variable sized keys, copy the variable sized portion
	//  For leaf nodes, copy the data too

        if (ks.leaf) {
	    //  Move existing 'to' stuff to make room for 'from' stuff.
	    nbytes_to = btree->nodesize - to->insert_ptr;
	    memmove(((char *) to) + to->insert_ptr - nbytes_shift, 
		   ((char *) to) + to->insert_ptr,
		   nbytes_to);

            //  Copy over the 'from' stuff.
	    memcpy(((char *) to) + to->insert_ptr - nbytes_shift + nbytes_to, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_shift);

	    to->insert_ptr   = to->insert_ptr   - nbytes_shift;
	    from->insert_ptr = from->insert_ptr + nbytes_shift;

	} else {

	    //  Move existing 'to' stuff to make room for 'from' stuff.
	    //  For non-leaves, include the 'right' pointer from the 'to' node.
	    nbytes_to = btree->nodesize - to->insert_ptr;
	    memmove(((char *) to) + to->insert_ptr - (nbytes_shift - r_keylen + s_keylen), 
		   ((char *) to) + to->insert_ptr,
		   nbytes_to);

            //  Copy key that converts 'right' pointer of 'to' node to a regular key.
	    memcpy(((char *) to) + to->insert_ptr - (nbytes_shift - r_keylen + s_keylen) + nbytes_to, s_key, s_keylen);

            //  Copy over the 'from' stuff.
	    memcpy(((char *) to) + to->insert_ptr - (nbytes_shift - r_keylen + s_keylen) + nbytes_to + s_keylen, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_shift - r_keylen);

	    to->insert_ptr   = to->insert_ptr   - (nbytes_shift - r_keylen + s_keylen);
	    from->insert_ptr = from->insert_ptr + nbytes_shift;
	}
	
	//  update the keypos pointers
        update_keypos(btree, to,   0);
        update_keypos(btree, from, 0);
    }

    // update the rightmost pointer of the 'to' node
    to->rightmost = r_ptr;

    *r_key_out      = r_key;
    *r_keylen_out   = r_keylen;
    *r_syndrome_out = r_syndrome;
    *r_seqno_out    = r_seqno;

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", s_key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, s_key, s_keylen);
		len = s_keylen;
	    }
	    fprintf(stderr, "********  After shift_left for key '%s' [syn=%lu], B-Tree:  *******\n", dump_key(stmp, len), s_syndrome);
	    btree_raw_dump(stderr, btree);
	}
    #endif

    return;
}

/*   Copy keys from 'from' node to 'to' node, given that 'to' is to right of 'from'.
 */
static void merge_right(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno)
{
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    uint32_t       nbytes_from;
    uint32_t       nbytes_fixed;
    uint32_t       nbytes_free;
    uint32_t       nbytes_needed;
    key_stuff_t    ks;

    if (*ret) { return; }
    btree->stats.stat[BTSTAT_RMERGES]++;

    (void) get_key_stuff(btree, from, 0, &ks);

    nbytes_fixed = ks.offset;
    if (ks.fixed) {
        nbytes_from = from->nkeys*ks.offset;
    } else {
	nbytes_from = btree->nodesize - from->insert_ptr;;
    }

    if (ks.leaf) {
	nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vlkey_t);
	nbytes_needed  = btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vlkey_t);
	assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	// make room for the lower fixed keys
	memmove((char *) to->keys + from->nkeys*nbytes_fixed, (char *) to->keys, to->nkeys*nbytes_fixed);

	// copy the fixed size portion of the keys
	memcpy(to->keys, from->keys, from->nkeys*nbytes_fixed);
	to->nkeys = to->nkeys + from->nkeys;

    } else {
	if (ks.fixed) {
	    assert((to->nkeys + from->nkeys + 1) <= btree->fkeys_per_node); // xxxzzz remove this!
	} else {
	    nbytes_free    = to->insert_ptr   - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vkey_t);
	    nbytes_needed  = btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vkey_t);
	    nbytes_needed += (s_keylen + sizeof(node_vkey_t));
	    assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	}

	// make room for the lower fixed keys, plus the separator from the anchor
	memmove((char *) to->keys + (from->nkeys + 1)*nbytes_fixed, (char *) to->keys, to->nkeys*nbytes_fixed);
	// copy the fixed size portion of the keys
	memcpy(to->keys, from->keys, from->nkeys*nbytes_fixed);
	to->nkeys = to->nkeys + from->nkeys + 1;

	// copy 'from' rightmost pointer
	if (ks.fixed) {
	    pfk = (node_fkey_t *) ((char *) to->keys + from->nkeys*nbytes_fixed);
	    pfk->key   = (uint64_t) s_key;
	    pfk->ptr   = from->rightmost;
	    pfk->seqno = s_seqno;
	} else {
	    pvk = (node_vkey_t *) ((char *) to->keys + from->nkeys*nbytes_fixed);
	    pvk->keylen = s_keylen;
	    pvk->keypos = 0; // will be set in update_keypos below
	    pvk->ptr    = from->rightmost;
	    pvk->seqno  = s_seqno;
	}
    }

    // copy variable sized stuff

    if (ks.fixed) {
	to->insert_ptr = 0;
    } else {
	// for variable sized keys, copy the variable sized portion
	//  For leaf nodes, copy the data too

        if (ks.leaf) {
	    memcpy(((char *) to) + to->insert_ptr - nbytes_from, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_from);

	    to->insert_ptr = to->insert_ptr - nbytes_from;
	} else {
	    //  for non-leaves, include the 'right' pointer from the 'from' node
	    memcpy(((char *) to) + to->insert_ptr - nbytes_from - s_keylen, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_from);

	    memcpy(((char *) to) + to->insert_ptr - s_keylen, s_key, s_keylen);

	    to->insert_ptr   = to->insert_ptr- nbytes_from - s_keylen;
	}
	
	//  update the keypos pointers
        update_keypos(btree, to, 0);
    }

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", s_key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, s_key, s_keylen);
		len = s_keylen;
	    }
	    fprintf(stderr, "********  After merge_right for key '%s' [syn=%lu], B-Tree:  *******\n", dump_key(stmp, len), s_syndrome);
	    btree_raw_dump(stderr, btree);
	}
    #endif

    return;
}

/*   Copy keys from 'from' node to 'to' node, given that 'to' is to left of 'from'.
 */
static void merge_left(int *ret, btree_raw_t *btree, btree_raw_node_t *anchor, btree_raw_node_t *from, btree_raw_node_t *to, char *s_key, uint32_t s_keylen, uint64_t s_syndrome, uint64_t s_seqno)
{
    node_fkey_t   *pfk;
    node_vkey_t   *pvk;
    uint32_t       nbytes_from;
    uint32_t       nbytes_to;
    uint32_t       nbytes_fixed;
    uint32_t       nbytes_free;
    uint32_t       nbytes_needed;
    key_stuff_t    ks;

    if (*ret) { return; }
    btree->stats.stat[BTSTAT_LMERGES]++;

    (void) get_key_stuff(btree, from, 0, &ks);

    nbytes_fixed = ks.offset;
    if (ks.fixed) {
        nbytes_from = from->nkeys*ks.offset;
    } else {
	nbytes_from = btree->nodesize - from->insert_ptr;;
    }

    if (ks.leaf) {
	nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vlkey_t);
	nbytes_needed  = btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vlkey_t);
	assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!

	// copy the fixed size portion of the keys
	memcpy((char *) to->keys + to->nkeys*nbytes_fixed, from->keys, from->nkeys*nbytes_fixed);
	to->nkeys = to->nkeys + from->nkeys;

    } else {
	if (ks.fixed) {
	    assert((to->nkeys + from->nkeys + 1) <= btree->fkeys_per_node); // xxxzzz remove this!
	} else {
	    nbytes_free    = to->insert_ptr - sizeof(btree_raw_node_t) - to->nkeys*sizeof(node_vkey_t);
	    nbytes_needed  = btree->nodesize - from->insert_ptr + from->nkeys*sizeof(node_vkey_t);
	    nbytes_needed += (s_keylen + sizeof(node_vkey_t));
	    assert(nbytes_free >= nbytes_needed); // xxxzzz remove this!
	}

	//  Copy the fixed size portion of the keys, leaving space for the
	//  converting the 'to' right pointer to a regular key.
	memcpy((char *) to->keys + (to->nkeys + 1)*nbytes_fixed, from->keys, from->nkeys*nbytes_fixed);

	// convert 'to' rightmost pointer to a regular key
	if (ks.fixed) {
	    pfk = (node_fkey_t *) ((char *) to->keys + to->nkeys*nbytes_fixed);
	    pfk->key   = (uint64_t) s_key;
	    pfk->ptr   = to->rightmost;
	    pfk->seqno = s_seqno;
	} else {
	    pvk = (node_vkey_t *) ((char *) to->keys + to->nkeys*nbytes_fixed);
	    pvk->keylen = s_keylen;
	    pvk->keypos = 0; // will be set in update_keypos below
	    pvk->ptr    = to->rightmost;
	    pvk->seqno  = s_seqno;
	}
	to->nkeys = to->nkeys + from->nkeys + 1;
    }

    // copy variable sized stuff

    if (ks.fixed) {
	to->insert_ptr = 0;
    } else {
	// for variable sized keys, copy the variable sized portion
	//  For leaf nodes, copy the data too

        if (ks.leaf) {
	    //  Move existing 'to' stuff to make room for 'from' stuff.
	    nbytes_to = btree->nodesize - to->insert_ptr;
	    memmove(((char *) to) + to->insert_ptr - nbytes_from, 
		   ((char *) to) + to->insert_ptr,
		   nbytes_to);

            //  Copy over the 'from' stuff.
	    memcpy(((char *) to) + to->insert_ptr - nbytes_from + nbytes_to, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_from);

	    to->insert_ptr = to->insert_ptr - nbytes_from;
	} else {
	    //  Move existing 'to' stuff to make room for 'from' stuff.
	    //  For non-leaves, include the 'right' pointer from the 'to' node.
	    nbytes_to = btree->nodesize - to->insert_ptr;
	    memmove(((char *) to) + to->insert_ptr - nbytes_from - s_keylen, 
		   ((char *) to) + to->insert_ptr,
		   nbytes_to);

            //  Copy over the 'from' stuff.
	    memcpy(((char *) to) + to->insert_ptr - nbytes_from - s_keylen + nbytes_to + s_keylen, 
		   ((char *) from) + from->insert_ptr,
		   nbytes_from);

            //  Copy key that converts 'right' pointer of 'to' node to a regular key.
	    memcpy(((char *) to) + to->insert_ptr - nbytes_from - s_keylen + nbytes_to, s_key, s_keylen);

	    to->insert_ptr   = to->insert_ptr- nbytes_from - s_keylen;
	}
	
	//  update the keypos pointers
        update_keypos(btree, to, 0);
    }

    // adjust the 'right' pointer of the merged node
    to->rightmost = from->rightmost;

    #ifdef DEBUG_STUFF
	if (Verbose) {
	    char stmp[10000];
	    int  len;
	    if (btree->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", s_key);
		len = strlen(stmp);
	    } else {
		strncpy(stmp, s_key, s_keylen);
		len = s_keylen;
	    }
	    fprintf(stderr, "********  After merge_left for key '%s' [syn=%lu], B-Tree:  *******\n", dump_key(stmp, len), s_syndrome);
	    btree_raw_dump(stderr, btree);
	}
    #endif

    return;
}

//======================   RANGE_QUERY   =========================================
#if 0
/* Like btree_get, but gets next n_in keys after a specified key.
 * Use key=NULL and keylen=0 for first call in enumeration.
 */
int btree_raw_get_next_n(uint32_t n_in, uint32_t *n_out, struct btree_raw *btree, char *key_in, uint32_t keylen_in, char **keys_out, uint32_t *keylens_out, char **data_out, uint64_t datalens_out, btree_metadata_t *meta)
{

#ifdef notdef
    xxxzzz

    btree_raw_node_t *n;

    // find node with next key after key_in

    xxxzzz

    for (n = get_existing_node(ret, btree, btree->root);
         n != NULL;
	 )
    {
        keyrec = find_key_greater(n, key, keylen, &child_id);
        if (keyrec) {
	    if (is_leaf(btree, n) {
	        *ptr    = keyrec->ptr;
		break;
	    } else {
	        n = btree->read_node_cb(btree->read_node_cb_data, keyrec->child_id);
		continue;
	    }
	}

	//  key not found
	if (child_id != BAD_CHILD) {
	    n = btree->read_node_cb(btree->read_node_cb_data, child_id);
	    continue;
	} else {
	    *ptr = NULL;
	    break;
	}
    }

    //  get next n_in keys
    n_actual = 0;
    for (i=0; i<n_in; i++) {
        xxxzzz
	key_out[n_actual]    = xxxzzz;
	keylen_out[n_actual] = xxxzzz;
	ptrs_out[n_actual]   = xxxzzz;
	n_actual++;
    }
    *n_out = n_actual;

#endif

    return(1);
}
#endif

//======================   FAST_BUILD  =========================================

int btree_raw_fast_build(btree_raw_t *btree)
{
    // TBD xxxzzz
    return(0);
}

//======================   DUMP  =========================================

static char *dump_key(char *key, uint32_t keylen)
{
    static char  stmp[100];

    stmp[0] = '\0';
    if (keylen > 10) {
	strncat(stmp, key, 10);
	strcat(stmp, "...");
    } else {
	strncat(stmp, key, keylen);
    }
    return(stmp);
}

static void dump_line(FILE *f, char *key, uint32_t keylen)
{
    if (key != NULL) {
	fprintf(f, "----------- Key='%s' -----------\n", dump_key(key, keylen));
    } else {
	fprintf(f, "-----------------------------------------------------------------------------------\n");
    }
}

static void dump_node(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key, uint32_t keylen)
{
    int             i;
    char           *sflags;
    int             nfreebytes;
    int             nkey_bytes;
    node_fkey_t    *pfk;
    node_vkey_t    *pvk;
    node_vlkey_t   *pvlk;
    key_stuff_t     ks;
    btree_raw_node_t   *n_child;

    dump_line(f, key, keylen);

    if (n == NULL) {
        fprintf(f, "***********   BAD NODE!!!!   **********\n");
        abort();
	return;
    }

    if (is_leaf(bt, n)) {
        sflags = "LEAF";
	nkey_bytes = sizeof(node_vlkey_t);
    } else {
        sflags = "";
	if (bt->flags & SYNDROME_INDEX) {
	    nkey_bytes = sizeof(node_fkey_t);
	} else {
	    nkey_bytes = sizeof(node_vkey_t);
	}
	assert(n->rightmost != 0);
    }

    if ((bt->flags & SYNDROME_INDEX) && !(n->flags & LEAF_NODE)) {
	nfreebytes = bt->nodesize - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    } else {
	nfreebytes = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    }
    assert(nfreebytes >= 0);

    fprintf(f, "Node [%ld][%p]: %d keys, ins_ptr=%d, %d free bytes, flags:%s%s, right=[%ld]\n", n->logical_id, n, n->nkeys, n->insert_ptr, nfreebytes, sflags, is_root(bt, n) ? ":ROOT" : "", n->rightmost);

    for (i=0; i<n->nkeys; i++) {

	if (n->flags & LEAF_NODE) {
	    pvlk = ((node_vlkey_t *) n->keys) + i;
	    fprintf(f, "   syn=%lu, Key='%s': ", pvlk->syndrome, dump_key((char *) n + pvlk->keypos, pvlk->keylen));
	    fprintf(f, "keylen=%d, keypos=%d, datalen=%ld, ptr=%ld, seqno=%ld", pvlk->keylen, pvlk->keypos, pvlk->datalen, pvlk->ptr, pvlk->seqno);
	    if ((pvlk->keylen + pvlk->datalen) >= bt->big_object_size) {
		//  overflow object
		fprintf(f, " [OVERFLOW!]\n");
	    } else {
		fprintf(f, "\n");
	    }
	} else if (bt->flags & SECONDARY_INDEX) {
	    pvk  = ((node_vkey_t *) n->keys) + i;
	    fprintf(f, "   Key='%s': ", dump_key((char *) n + pvk->keypos, pvk->keylen));
	    fprintf(f, "keylen=%d, keypos=%d, ptr=%ld, seqno=%ld\n", pvk->keylen, pvk->keypos, pvk->ptr, pvk->seqno);
	} else if (bt->flags & SYNDROME_INDEX) {
	    pfk  = ((node_fkey_t *) n->keys) + i;
	    fprintf(f, "   syn=%lu: ", pfk->key);
	    fprintf(f, "ptr=%ld, seqno=%ld\n", pfk->ptr, pfk->seqno);
	} else {
	    assert(0);
	}
    }

    if (!(n->flags & LEAF_NODE)) {
	int   ret=0;
	char  stmp[100];

	// non-leaf
	for (i=0; i<n->nkeys; i++) {
	    assert(!get_key_stuff(bt, n, i, &ks));
	    n_child = get_existing_node(&ret, bt, ks.ptr); 
	    if (bt->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", ks.pkey_val);
		dump_node(bt, f, n_child, stmp, strlen(stmp));
	    } else {
		dump_node(bt, f, n_child, ks.pkey_val, ks.keylen);
	    }
	}
	if (n->rightmost != 0) {
	    n_child = get_existing_node(&ret, bt, n->rightmost); 
	    dump_node(bt, f, n_child, "==RIGHT==", 9);
	}
    }
}

#ifdef DEBUG_STUFF
static
void btree_raw_dump(FILE *f, btree_raw_t *bt)
{
    int                ret = 0;
    btree_raw_node_t  *n;
    char               sflags[1000];

    sflags[0] = '\0';
    if (bt->flags & SYNDROME_INDEX) {
        strcat(sflags, "SYN ");
    }
    if (bt->flags & SECONDARY_INDEX) {
        strcat(sflags, "SEC ");
    }
    if (bt->flags & IN_MEMORY) {
        strcat(sflags, "MEM");
    }

    dump_line(f, NULL, 0);

    fprintf(f, "B-Tree: flags:(%s), node:%dB, maxkey:%dB, minkeys:%d, bigobj:%dB\n", sflags, bt->nodesize, bt->max_key_size, bt->min_keys_per_node, bt->big_object_size);

    n = get_existing_node(&ret, bt, bt->rootid); 
    if (ret || (n == NULL)) {
	fprintf(f, "*********************************************\n");
	fprintf(f, "    *****  Could not get root node!!!!  *****\n");
	fprintf(f, "*********************************************\n");
    }
    
    dump_node(bt, f, n, "===ROOT===", 10);

    dump_line(f, NULL, 0);
}
#endif

//======================   CHECK   =========================================
#if 0
static void print_key(FILE *f, char* key, int keylen, char *msg, ...)
{
	int i;
    char     stmp[1024];
    char     stmp1[1024];
    va_list  args;
return;
    va_start(args, msg);

    vsprintf(stmp, msg, args);

    va_end(args);

	assert(keylen + 1 < sizeof(stmp1));
	for(i=0;i<keylen;i++)
		stmp1[i] = key[i] < 32 ? '^' : key[i];
	stmp1[i] = 0;
    (void) fprintf(stderr, "%s key=[%s]\n", stmp, stmp1);
}
#endif
static void check_err(FILE *f, char *msg, ...)
{
    char     stmp[1024];
    va_list  args;

    va_start(args, msg);

    vsprintf(stmp, msg, args);
    strcat(stmp, "\n");

    va_end(args);

    (void) fprintf(stderr, "%x %s", (int)pthread_self(), stmp);
    abort();
}

static void check_node(btree_raw_t *bt, FILE *f, btree_raw_node_t *n, char *key_in_left, uint32_t keylen_in_left, char *key_in, uint32_t keylen_in, char *key_in_right, uint32_t keylen_in_right, int rightmost_flag)
{
    int                 i;
    int                 nfreebytes;
    int                 nkey_bytes;
    node_fkey_t        *pfk;
    node_vkey_t        *pvk;
    node_vlkey_t       *pvlk;
    key_stuff_t         ks;
    key_stuff_t         ks_left;
    key_stuff_t         ks_right;
    btree_raw_node_t   *n_child;
    int                 x;

    if (n == NULL) {
        fprintf(f, "***********   ERROR: check_node: BAD NODE!!!!   **********\n");
	return;
    }
#ifdef DEBUG_STUFF
#if 0
    fprintf(stderr, "%x %s node=%p\n", (int)pthread_self(), __FUNCTION__, n);
    if(key_in_left)
    fprintf(stderr, "%x %s left [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in_left, key_in_left);
    if(key_in_right)
    fprintf(stderr, "%x %s right [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in_right, key_in_right);
    if(key_in)
    fprintf(stderr, "%x %s in [%.*s]\n", (int)pthread_self(), __FUNCTION__, keylen_in, key_in);
#endif
#endif
    if (n->flags & LEAF_NODE) {
	nkey_bytes = sizeof(node_vlkey_t);
    } else {
	if (bt->flags & SYNDROME_INDEX) {
	    nkey_bytes = sizeof(node_fkey_t);
	} else {
	    nkey_bytes = sizeof(node_vkey_t);
	}
	assert(n->rightmost != 0);
    }
    if ((bt->flags & SYNDROME_INDEX) && !(n->flags & LEAF_NODE)) {
	nfreebytes = bt->nodesize - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    } else {
	nfreebytes = n->insert_ptr - sizeof(btree_raw_node_t) - n->nkeys*nkey_bytes;
    }
    assert(nfreebytes >= 0);

    for (i=0; i<n->nkeys; i++) {
        if (n->flags & LEAF_NODE) {
	    assert(get_key_stuff(bt, n, i, &ks));
	} else {
	    assert(!get_key_stuff(bt, n, i, &ks));
	}
	if (key_in_left != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in_left, keylen_in_left, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x != -1) {
		    check_err(f, "***********   ERROR: check_node left (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x != -1) {
		    check_err(f, "***********   ERROR: check_node left: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (key_in != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in, keylen_in, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (key_in_right != NULL) {
	    x = bt->cmp_cb(bt->cmp_cb_data, key_in_right, keylen_in_right, ks.pkey_val, ks.keylen);
	    if (rightmost_flag) {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node right (right): node %p key %d out of order!!!!   **********\n", n, i);
		}
	    } else {
		if (x == -1) {
		    check_err(f, "***********   ERROR: check_node right: node %p key %d out of order!!!!   **********\n", n, i);
		}
	    }
	}

	if (i > 0) {
	    // make sure the keys within this node are sorted! 
	    (void) get_key_stuff(bt, n, i, &ks_left);
	    x = bt->cmp_cb(bt->cmp_cb_data, ks_left.pkey_val, ks_left.keylen, ks.pkey_val, ks.keylen);
	    if (x == -1) {
		check_err(f, "***********   ERROR: check_node internal: node %p key %d out of order!!!!   **********\n", n, i);
	    }
	}

	if (n->flags & LEAF_NODE) {
	    pvlk = ((node_vlkey_t *) n->keys) + i;
	    // purposefully empty 
	} else if (bt->flags & SECONDARY_INDEX) {
	    pvk  = ((node_vkey_t *) n->keys) + i;
	    // purposefully empty 
	} else if (bt->flags & SYNDROME_INDEX) {
	    pfk  = ((node_fkey_t *) n->keys) + i;
	    // purposefully empty 
	} else {
	    assert(0);
	}
    }

    if (!(n->flags & LEAF_NODE)) {
	int   ret=0;
	char  stmp[100];
	char  stmp_left[100];
	char  stmp_right[100];

	// non-leaf
	for (i=0; i<n->nkeys; i++) {
	    (void) get_key_stuff(bt, n, i, &ks);
	    n_child = get_existing_node(&ret, bt, ks.ptr); 
	    if (bt->flags & SYNDROME_INDEX) {
	        sprintf(stmp, "%p", ks.pkey_val);
		if (i == 0) {
		    if (key_in_left == NULL) {
			strcpy(stmp_left, "");
		    } else {
			strcpy(stmp_left, key_in_left);
		    }
		} else {
		    (void) get_key_stuff(bt, n, i-1, &ks_left);
		    sprintf(stmp_left, "%p", ks_left.pkey_val);
		}
		if (i == (n->nkeys-1)) {
		    if (key_in_right == NULL) {
			strcpy(stmp_right, "");
		    } else {
			strcpy(stmp_right, key_in_right);
		    }
		} else {
		    (void) get_key_stuff(bt, n, i+1, &ks_right);
		    sprintf(stmp_right, "%p", ks_right.pkey_val);
		}
		check_node(bt, f, n_child, stmp_left, strlen(stmp_left), stmp, strlen(stmp), stmp_right, strlen(stmp_right), 0 /* right */);
	    } else {
		if (i == 0) {
		    if (key_in_left != NULL) {
			ks_left.pkey_val = key_in_left;
			ks_left.keylen   = keylen_in_left;
		    } else {
			ks_left.pkey_val = NULL;
			ks_left.keylen   = 0;
		    }
		} else {
		    (void) get_key_stuff(bt, n, i-1, &ks_left);
		}
		if (i == (n->nkeys-1)) {
		    ks_right.pkey_val = key_in_right;
		    ks_right.keylen   = keylen_in_right;
		} else {
		    (void) get_key_stuff(bt, n, i+1, &ks_right);
		}
		check_node(bt, f, n_child, ks_left.pkey_val, ks_left.keylen, ks.pkey_val, ks.keylen, ks_right.pkey_val, ks_right.keylen, 0 /* right */);
	    }
	}
	if (n->rightmost != 0) {
	    n_child = get_existing_node(&ret, bt, n->rightmost); 
	    if (n->nkeys == 0) {
	        //  this can only happen for the root!
	        assert(n->logical_id == bt->rootid);
		check_node(bt, f, n_child, NULL, 0, NULL, 0, NULL, 0, 1 /* right */);
	    } else {
		(void) get_key_stuff(bt, n, n->nkeys-1, &ks_left);
		check_node(bt, f, n_child, ks_left.pkey_val, ks_left.keylen, key_in_right, keylen_in_right, NULL, 0, 1 /* right */);
	    }
	}
    }
}

#ifdef BTREE_RAW_CHECK
static
void btree_raw_check(btree_raw_t *bt, char* func, char* key)
{
    int                ret = 0;
    btree_raw_node_t  *n;

    plat_rwlock_wrlock(&bt->lock);

#ifdef DEBUG_STUFF
	fprintf(stderr, "BTREE_CHECK %x %s btree %p key %s lock %p BEGIN\n", (int)pthread_self(), func, bt, key, &bt->lock);
#endif
    n = get_existing_node(&ret, bt, bt->rootid); 
    if (ret || (n == NULL)) {
	check_err(stderr, "*****  ERROR: btree_raw_check: Could not get root node!!!!  *****\n");
    }
    
    check_node(bt, stderr, n, NULL, 0, NULL, 0, NULL, 0, 0);

    (void)deref_l1cache(bt);

#ifdef DEBUG_STUFF
    fprintf(stderr, "BTREE_CHECK %x %s btree %p key %s lock %p END\n", (int)pthread_self(), func, bt, key, &bt->lock);
#endif

    plat_rwlock_unlock(&bt->lock);
}
#endif

//======================   TEST  =========================================

void btree_raw_test(btree_raw_t *btree)
{
    // TBD xxxzzz
}

//======================   SNAPSHOTS   =========================================

extern int btree_raw_snapshot(struct btree_raw *btree, uint64_t *seqno)
{
    // TBD xxxzzz
    return(0);
}

extern int btree_raw_delete_snapshot(struct btree_raw *btree, uint64_t seqno)
{
    // TBD xxxzzz
    return(0);
}

extern int btree_raw_get_snapshots(struct btree_raw *btree, uint32_t *n_snapshots, uint64_t *seqnos)
{
    // TBD xxxzzz
    return(0);
}

//======================   STATS   =========================================

static void btree_raw_init_stats(struct btree_raw *btree, btree_stats_t *stats)
{
    memset(stats, 0, sizeof(btree_stats_t));
}

void btree_raw_get_stats(struct btree_raw *btree, btree_stats_t *stats)
{
    memcpy(stats, &(btree->stats), sizeof(btree_stats_t));
}

char *btree_stat_name(btree_stat_t stat_type)
{
    return(btree_stats_strings[stat_type]);
}

void btree_dump_stats(FILE *f, btree_stats_t *stats)
{
    int j;

    fprintf(stderr, "==============================================================\n");
    for (j=0; j<N_BTSTATS; j++) {
        fprintf(stderr, "%-23s = %"PRIu64"\n", btree_stat_name(j), stats->stat[j]);
    }
    fprintf(stderr, "==============================================================\n");
}



