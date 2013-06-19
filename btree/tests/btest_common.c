/************************************************************************
 * 
 *  btest_common.c  Jan. 21, 2013   Brian O'Krafka
 * 
 *  Built-in self-Test program for btree package.
 * 
 * NOTES: xxxzzz
 * 
 ************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include "../btree_map.h"
#include "../btree.h"
#include "btest_common.h"

static char *gendata(uint32_t max_datalen, uint64_t *pdatalen);

static struct btree_raw_node *read_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid);
static void write_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen);
static void flush_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid);
static int freebuf_cb(void *data, char *buf);
static struct btree_raw_node *create_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid);
static btree_status_t delete_node_cb(struct btree_raw_node *node, void *data, uint64_t lnodeid);
static void log_cb(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree, struct btree_raw_node *n);
static int cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2);
static void msg_cb(int level, void *msg_data, char *filename, int lineno, char *msg, ...);
static void txn_cmd_cb(btree_status_t *ret_out, void *cb_data, int cmd_type);

#define Error(msg, args...) \
    msg_cb(0, NULL, __FILE__, __LINE__, msg, ##args);

#define msg(msg, args...) \
    msg_cb(2, NULL, __FILE__, __LINE__, msg, ##args);

static char      *char_array = "abcdefghijklmnopqrstuvwxyz0123456789_-.";
static uint32_t   n_char_array;

// Must allow for a 64-bit integer in decimal form plus a trailing NULL
#define MIN_KEY_SIZE      21

#ifdef notdef
    #define DEFAULT_N_PARTITIONS      100
    #define DEFAULT_MIN_KEY_SIZE      17
    #define DEFAULT_MAX_KEY_SIZE      100
    #define DEFAULT_MAX_DATA_SIZE     100000
    #define DEFAULT_NODE_SIZE         8192
    #define DEFAULT_N_L1CACHE_BUCKETS 1000
    #define DEFAULT_MIN_KEYS_PER_NODE 4
    #define DEFAULT_N_TEST_KEYS       1000
    #define DEFAULT_N_TEST_ITERATIONS 1000000
#endif

#define DEFAULT_N_PARTITIONS      1
// #define DEFAULT_MAX_KEY_SIZE      10
// #define DEFAULT_MAX_KEY_SIZE      100
#define DEFAULT_MAX_KEY_SIZE      160
// #define DEFAULT_MAX_DATA_SIZE     1000
#define DEFAULT_MAX_DATA_SIZE     100
// #define DEFAULT_MAX_DATA_SIZE     20000
#define DEFAULT_NODE_SIZE         8100
// #define DEFAULT_NODE_SIZE         2100
// #define DEFAULT_NODE_SIZE         900
#define DEFAULT_N_L1CACHE_BUCKETS 1000
#define DEFAULT_MIN_KEYS_PER_NODE 4
// #define DEFAULT_N_TEST_KEYS       6000
// #define DEFAULT_N_TEST_KEYS       20
#define DEFAULT_N_TEST_KEYS       4000000
// #define DEFAULT_N_TEST_KEYS       1000000
// #define DEFAULT_N_TEST_ITERATIONS 1000000
// #define DEFAULT_N_TEST_ITERATIONS 10000
#define DEFAULT_N_TEST_ITERATIONS 10000000

    // Counts of number of times callbacks are invoked:
static uint64_t N_read_node   = 0;
static uint64_t N_write_node  = 0;
static uint64_t N_flush_node  = 0;
static uint64_t N_freebuf     = 0;
static uint64_t N_create_node = 0;
static uint64_t N_delete_node = 0;
static uint64_t N_log         = 0;
static uint64_t N_cmp         = 0;
static uint64_t N_txn_cmd_1   = 0;
static uint64_t N_txn_cmd_2   = 0;
static uint64_t N_txn_cmd_3   = 0;

void btest_common_usage(char *program)
{
	fprintf(stderr, "=============================================================================\n\n");
	fprintf(stderr, "usage: %s [-p <n_partitions]\n", program);
	fprintf(stderr, "             [-k <max_key_size_bytes>]\n");
	fprintf(stderr, "             [-d <max_data_size_kbytes]\n");
	fprintf(stderr, "             [-n <node_size_bytes>]\n");
	fprintf(stderr, "             [-l <n_l1cache_buckets>]\n");
	fprintf(stderr, "             [-z <min_keys_per_node>]\n");
	fprintf(stderr, "             [-t <n_test_keys>]\n");
	fprintf(stderr, "             [-i <n_test_iterations>]\n");
	fprintf(stderr, "             [-s] [-v]\n\n");
	fprintf(stderr, "       -s: use secondary index\n");
	fprintf(stderr, "       -v: dump verbose output\n\n");
	fprintf(stderr, "       Defaults: \n");
	fprintf(stderr, "             <n_partitions>        = %d\n", DEFAULT_N_PARTITIONS);
	fprintf(stderr, "             <max_key_size_bytes>  = %d\n", DEFAULT_MAX_KEY_SIZE);
	fprintf(stderr, "             <max_data_size_bytes  = %d\n", DEFAULT_MAX_DATA_SIZE);
	fprintf(stderr, "             <node_size_bytes>     = %d\n", DEFAULT_NODE_SIZE);
	fprintf(stderr, "             <n_l1cache_buckets>   = %d\n", DEFAULT_N_L1CACHE_BUCKETS);
	fprintf(stderr, "             <min_keys_per_node>   = %d\n", DEFAULT_MIN_KEYS_PER_NODE);
	fprintf(stderr, "             <n_test_keys>         = %d\n", DEFAULT_N_TEST_KEYS);
	fprintf(stderr, "             <n_test_iterations>   = %d\n", DEFAULT_N_TEST_ITERATIONS);
	fprintf(stderr, "             Key size NOT fixed\n");
	fprintf(stderr, "             Non-verbose output\n");
	fprintf(stderr, "\n=============================================================================\n\n");
}

int
btest_basic_parse(btest_cfg_t *cfg, int argc, char **argv)
{
	int i;

	for (i=1; i<argc; i++) {
		if (argv[i][0] != '-') {
			btest_common_usage(cfg->program);
			return 1;
		}

		/* switches without second arguments */

		switch (argv[i][1]) {
		case 'h': // help
			btest_common_usage(cfg->program);
			return 1;
			break;
		case 'v': // verbose output
			cfg->flags |= VERBOSE_DEBUG;
			continue;
			break;
		case 's': // use secondary index
			cfg->flags |= SECONDARY_INDEX;
			continue;
			break;
		default:
			/* purposefully empty */
			break;
		}

		/* switches with second arguments */
		if (i == (argc - 1)) {
			// second argument is missing!
			btest_common_usage(cfg->program);
			return 1;
		}

		switch (argv[i][1]) {
		case 'p':
			cfg->n_partitions = atoi(argv[i+1]);
			if (cfg->n_partitions <= 0) {
				Error("<n_partitions> must be positive");
			}
			break;
		case 'k':
			cfg->max_key_size = atoi(argv[i+1]);
			if (cfg->max_key_size <= MIN_KEY_SIZE) {
				Error("<max_key_size_bytes> must be >= %d", MIN_KEY_SIZE);
			}
			break;
		case 'd':
			cfg->max_datalen = atoi(argv[i+1]);
			if (cfg->max_datalen <= 0) {
				Error("<max_data_size_bytes> must be positive");
			}
			break;
		case 'n':
			cfg->nodesize = atoi(argv[i+1]);
			if (cfg->nodesize <= 0) {
				Error("<node_size_bytes> must be positive");
			}
			break;
		case 'l':
			cfg->n_l1cache_buckets = atoi(argv[i+1]);
			if (cfg->n_l1cache_buckets <= 0) {
				Error("<n_l1cache_buckets> must be positive");
			}
			break;
		case 'z':
			cfg->min_keys_per_node = atoi(argv[i+1]);
			if (cfg->min_keys_per_node <= 0) {
				Error("<min_keys_per_node> must be positive");
			}
			break;
		case 't':
			cfg->n_test_keys = atoi(argv[i+1]);
			if (cfg->n_test_keys <= 0) {
				Error("<n_test_keys> must be positive");
			}
			break;
		case 'i':
			cfg->n_test_iters = atoi(argv[i+1]);
			if (cfg->n_test_iters <= 0) {
				Error("<n_test_iters> must be positive");
			}
			break;
		default:   /* Left it so that caller can parse */
			break;
		}
		i++; // skip the second argument
	}

	return (0);
}

btest_cfg_t *
btest_init(int argc, char **argv, char *program, btest_parse_fn parse_fn)
{
	void         *create_node_cb_data;
	void         *read_node_cb_data;
	void         *write_node_cb_data;
	void         *flush_node_cb_data;
	void         *freebuf_cb_data;
	void         *delete_node_cb_data;
	void         *log_cb_data;
	void         *msg_cb_data;
	void         *cmp_cb_data;
	void         *txn_cmd_cb_data;
	int          error;
	btest_cfg_t *cfg;

	create_node_cb_data = NULL;
	read_node_cb_data   = NULL;
	write_node_cb_data  = NULL;
	flush_node_cb_data  = NULL;
	freebuf_cb_data     = NULL;
	delete_node_cb_data = NULL;
	log_cb_data         = NULL;
	msg_cb_data         = NULL;
	cmp_cb_data         = NULL;
	txn_cmd_cb_data     = NULL;

	cfg = (btest_cfg_t *)malloc(sizeof(btest_cfg_t));
	if (cfg == NULL) {
		assert(cfg);
		return NULL;
	}

	/* All default values */
	cfg->program = program;
	cfg->n_partitions        = DEFAULT_N_PARTITIONS;
	cfg->flags               = 0;
	cfg->max_key_size        = DEFAULT_MAX_KEY_SIZE;
	cfg->max_datalen         = DEFAULT_MAX_DATA_SIZE;
	cfg->min_keys_per_node   = DEFAULT_MIN_KEYS_PER_NODE;
	cfg->nodesize            = DEFAULT_NODE_SIZE;
	cfg->n_l1cache_buckets   = DEFAULT_N_L1CACHE_BUCKETS;

	cfg->n_test_keys         = DEFAULT_N_TEST_KEYS;
	cfg->n_test_iters        = DEFAULT_N_TEST_ITERATIONS;

	if (parse_fn) {
		error = (*parse_fn)(cfg, argc, argv);
	} else {
		error = btest_basic_parse(cfg, argc, argv);
	}

	if (error) {
		free(cfg);
		return NULL;
	}

	/* initialization */
	if (cfg->max_key_size <= MIN_KEY_SIZE) {
		Error("<max_key_size_bytes> must be >= %d", MIN_KEY_SIZE);
	}

	cfg->flags |= SECONDARY_INDEX;

	if (!(cfg->flags & SECONDARY_INDEX)) {
		cfg->flags |= SYNDROME_INDEX;
	}

	cfg->flags |= IN_MEMORY; // use in-memory b-tree for this test

	msg("Starting btree test %s...", cfg->program);
	msg("n_partitions = %d",         cfg->n_partitions);
	msg("flags = %d",                cfg->flags);
	msg("max_key_size = %d",         cfg->max_key_size);
	msg("min_keys_per_node = %d",    cfg->min_keys_per_node);
	msg("nodesize = %d",             cfg->nodesize);
	msg("n_l1cache_buckets = %d",    cfg->n_l1cache_buckets);
	msg("n_test_keys = %d",          cfg->n_test_keys);
	msg("max_datalen = %lld",        cfg->max_datalen);
	msg("iterations = %lld",         cfg->n_test_iters);

	cfg->bt = btree_init(cfg->n_partitions, cfg->flags, cfg->max_key_size, 
	                cfg->min_keys_per_node, cfg->nodesize, cfg->n_l1cache_buckets,
	                (create_node_cb_t *)create_node_cb, create_node_cb_data, 
	                (read_node_cb_t *)read_node_cb, read_node_cb_data, 
			(write_node_cb_t *)write_node_cb, write_node_cb_data, 
			(flush_node_cb_t *)flush_node_cb, flush_node_cb_data, 
			freebuf_cb, freebuf_cb_data, 
			(delete_node_cb_t *)delete_node_cb, delete_node_cb_data, 
			(log_cb_t *)log_cb, log_cb_data, 
			msg_cb, msg_cb_data, 
			cmp_cb, cmp_cb_data,
			(txn_cmd_cb_t *)txn_cmd_cb, txn_cmd_cb_data
	                );

	if (cfg->bt == NULL) {
		Error("Could not create btree!");
	}

	/* generate some random keys and data and stash in a hashtable and array */
	cfg->kmap = MapInit(cfg->n_test_keys, 0, 0, NULL, NULL);
	if (cfg->kmap == NULL) {
		Error("Could not create key map!");
	}

	return (cfg);
}

/* 
 * This function generates random pattern of data and insert into the btree 
 */
void
btest_serial_data_gen(btest_cfg_t *cfg)
{
	int              i;
	char             *keytmp;
	uint32_t         nsuccess;
	uint32_t         nconflict;
	uint64_t         key;
	uint32_t         keylen;
	uint64_t         datalen;
	char             *pdata;
	btree_metadata_t meta;
	int              ret;
	btree_stats_t    bt_stats;

	//  Initialize array of characters used 
	//  to generate random keys and data
	n_char_array = strlen(char_array);

	cfg->keys = (uint64_t *) malloc(cfg->n_test_keys*sizeof(uint64_t));
	assert(cfg->keys);
	cfg->keylens = (uint32_t *) malloc(cfg->n_test_keys*sizeof(uint32_t));
	assert(cfg->keylens);
	cfg->datas = (char **) malloc(cfg->n_test_keys*sizeof(char *));
	assert(cfg->datas);
	cfg->datalens = (uint32_t *) malloc(cfg->n_test_keys*sizeof(uint32_t));
	assert(cfg->datalens);

	/* create a random string of data for key suffixes */
	keytmp = (char *) malloc(cfg->max_key_size + 1);
	assert(keytmp);

	cfg->keysuffix = (char *) malloc(cfg->max_key_size);
	assert(cfg->keysuffix);
	for (i=0; i<cfg->max_key_size-1; i++) {
		cfg->keysuffix[i] = char_array[random()%n_char_array];
	}
	cfg->keysuffix[cfg->max_key_size-1] = '\0';

	nsuccess  = 0;
	nconflict = 0;

	for (i = 0; i < cfg->n_test_keys; i++) {
		key = i;
		keylen = cfg->max_key_size;
		pdata = gendata(cfg->max_datalen, &datalen);

		cfg->keys[nsuccess]  = key;
		cfg->keylens[nsuccess]  = keylen;
		cfg->datas[nsuccess] = pdata;
		cfg->datalens[nsuccess] = datalen;

		if (!MapCreate(cfg->kmap, (char *) &key, sizeof(uint64_t), pdata, datalen)) {
			// Error("MapCreate failed for data item %d", nsuccess);
			nconflict++;
		} else {
			nsuccess++;
		}
	}
	msg("%d out of %d data items created (%d conflicts)", nsuccess, cfg->n_test_keys, nconflict);

	/* load the data into the btree */
	for (i=0; i<cfg->n_test_keys; i++) {
		ret = 0;

		(void) sprintf(keytmp, "%08"PRIu64"", cfg->keys[i]);

		ret = btree_insert(cfg->bt, keytmp, strlen(keytmp) + 1, 
		                   cfg->datas[i], cfg->datalens[i], &meta);

		if (ret != 0) {
			Error("btree_insert failed for key '%s' "
			      "with ret=%d!", keytmp, ret);
		}
	}

	msg("%d data items loaded into b-tree", cfg->n_test_keys);

	btree_get_stats(cfg->bt, &bt_stats);
	btree_dump_stats(stderr, &bt_stats);
}

void
btest_rand_data_gen(btest_cfg_t *cfg)
{
	int              i;
	char             *keytmp;
	uint32_t         nsuccess;
	uint32_t         nconflict;
	uint64_t         key;
	uint32_t         keylen;
	uint64_t         datalen;
	char             *pdata;
	btree_metadata_t meta;
	int              ret;
	btree_stats_t    bt_stats;

	//  Initialize array of characters used 
	//  to generate random keys and data
	n_char_array = strlen(char_array);

	cfg->keys = (uint64_t *) malloc(cfg->n_test_keys*sizeof(uint64_t));
	assert(cfg->keys);
	cfg->keylens = (uint32_t *) malloc(cfg->n_test_keys*sizeof(uint32_t));
	assert(cfg->keylens);
	cfg->datas = (char **) malloc(cfg->n_test_keys*sizeof(char *));
	assert(cfg->datas);
	cfg->datalens = (uint32_t *) malloc(cfg->n_test_keys*sizeof(uint32_t));
	assert(cfg->datalens);

	/* create a random string of data for key suffixes */
	keytmp = (char *) malloc(cfg->max_key_size + 1);
	assert(keytmp);
	cfg->keysuffix = (char *) malloc(cfg->max_key_size);
	assert(cfg->keysuffix);
	for (i=0; i<cfg->max_key_size-1; i++) {
		cfg->keysuffix[i] = char_array[random()%n_char_array];
	}
	cfg->keysuffix[cfg->max_key_size-1] = '\0';

	nsuccess  = 0;
	nconflict = 0;

	while (1) {
		key = random();
		keylen = random() % (cfg->max_key_size - MIN_KEY_SIZE);
		pdata = gendata(cfg->max_datalen, &datalen);

		cfg->keys[nsuccess]  = key;
		cfg->keylens[nsuccess]  = keylen;
		cfg->datas[nsuccess] = pdata;
		cfg->datalens[nsuccess] = datalen;

		if (!MapCreate(cfg->kmap, (char *) &key, sizeof(uint64_t), pdata, datalen)) {
			// Error("MapCreate failed for data item %d", nsuccess);
			nconflict++;
		} else {
			nsuccess++;
			if (nsuccess == cfg->n_test_keys) {
				break;
			}
		}
	}
	msg("%d out of %d data items created (%d conflicts)", nsuccess, cfg->n_test_keys, nconflict);

	/* load the data into the btree */
	for (i=0; i<cfg->n_test_keys; i++) {
		ret = 0;

		while (1) {
			(void) sprintf(keytmp, "%"PRIu64"", cfg->keys[i]);
			strncat(keytmp, cfg->keysuffix, cfg->keylens[i]);
			meta.flags = 0;

			ret = btree_insert(cfg->bt, keytmp, strlen(keytmp) + 1, 
			                   cfg->datas[i], cfg->datalens[i], &meta);

			if (ret == 0) {
				break;
			}

			if (ret == 2) {
				// msg("key conflict for '%s', i=%"PRIu64"", keytmp, i);
				cfg->keylens[i]++;
			} else {
				Error("btree_insert failed for key '%s' "
				      "with ret=%d!", keytmp, ret);
			}
		}
	}

	msg("%d data items loaded into b-tree)", cfg->n_test_keys);

	btree_get_stats(cfg->bt, &bt_stats);
	btree_dump_stats(stderr, &bt_stats);
}

void
btest_life_cycle(btest_cfg_t *cfg)
{
	int i;
	uint32_t          p;
	char              *keytmp;
	btree_metadata_t  meta;
	char              *pdata, *old_pdata;
	uint64_t          datalen, old_datalen;
	btree_stats_t     bt_stats;
	int               ret;
	uint32_t          nkey;

	/* do a bunch of gets and updates, checking data as we go */
	keytmp = (char *) malloc(cfg->max_key_size + 1);
	assert(keytmp);

	for (i=0; i<cfg->n_test_iters; i++) {

        	// xxxzzz
		// check for corruption of nkey=11
		// xxxzzz

		if ((i % 10000) == 0) {
			// msg("%lld iters...", i);
			fprintf(stderr, ".");
		}

		//  randomly decide to get or update
		p = random() % 3 ;

		//  pick a random key to get or update
		nkey = random() % cfg->n_test_keys;

		(void) sprintf(keytmp, "%"PRIu64"", cfg->keys[nkey]);
		strncat(keytmp, cfg->keysuffix, cfg->keylens[nkey]);

		//  do the operations
		if (p == 1) {
			// get
			meta.flags = 0;
			ret = btree_get(cfg->bt, keytmp, strlen(keytmp) + 1, &pdata, &datalen, &meta);
			if (ret != 0) {
				Error("btree_get failed for key '%s' with ret=%d (i=%"PRIu64", nkey=%d)!", keytmp, ret, i, nkey);
			}

			// check the data
			if (cfg->datalens[nkey] != datalen) {
				Error("Data length mismatch on btree_get for key '%s' [%d] (%d expected, %lld found) (i=%lld)!", keytmp, nkey, cfg->datalens[nkey], datalen, i);
			}

			if (memcmp(cfg->datas[nkey], pdata, datalen)) {
				Error("Data mismatch on btree_get for key '%s' (i=%lld)!", keytmp, i);
			}
			free(pdata);
		} else if (p == 2) {

			// do a set operation

			// create new data
			free(cfg->datas[nkey]);
			pdata = gendata(cfg->max_datalen, &datalen);
			cfg->datas[nkey] = pdata;
			cfg->datalens[nkey] = datalen;

			meta.flags = 0;
			(void) MapSet(cfg->kmap, (char *) &(cfg->keys[nkey]), 
			              sizeof(uint64_t), 
			              cfg->datas[nkey], cfg->datalens[nkey],
			              &old_pdata, &old_datalen);

		    	ret = btree_set(cfg->bt, 
			                keytmp, strlen(keytmp) + 1, 
			                cfg->datas[nkey], cfg->datalens[nkey],
			                &meta);
			if (ret != 0) {
				Error("btree_set failed for key '%s' with ret=%d "
				       "(i=%lld)!", keytmp, ret, i);
			}
		} else {

			// update the data for an existing key
			// create new data
			free(cfg->datas[nkey]);
			pdata = gendata(cfg->max_datalen, &datalen);
			cfg->datas[nkey] = pdata;
			cfg->datalens[nkey] = datalen;

			meta.flags = 0;
			if (!MapUpdate(cfg->kmap, 
			               (char *) &(cfg->keys[nkey]), sizeof(uint64_t),
			               cfg->datas[nkey], cfg->datalens[nkey])) {
				Error("Inconsistency with MapUpdate for key %lld "
				      "(i=%lld)", cfg->keys[nkey], i);
	    		}

			ret = btree_update(cfg->bt, keytmp, strlen(keytmp) + 1, 
			                   cfg->datas[nkey], cfg->datalens[nkey], &meta);
			if (ret != 0) {
				Error("btree_update failed for key '%s' with "
				      "ret=%d (i=%lld)!", keytmp, ret, i);
			}
		}
	}
	msg(""); // for carriage return


	/* do a bunch of scans, checking data as we go */

	// xxxzzz TBD

	/* do a bunch of deletes */

	msg("Successfully finished get/insert test with %"PRIu64" iterations...", i);

	btree_get_stats(cfg->bt, &bt_stats);
	btree_dump_stats(stderr, &bt_stats);

	for (i=0; i<cfg->n_test_keys; i++) {
	// delete

		(void) sprintf(keytmp, "%"PRIu64"", cfg->keys[i]);
		strncat(keytmp, cfg->keysuffix, cfg->keylens[i]);

		meta.flags = 0;
		ret = btree_delete(cfg->bt, keytmp, strlen(keytmp) + 1, &meta);
		if (ret != 0) {
			Error("btree_delete failed for key '%s' with ret=%d!", keytmp, ret);
		}
	}
	msg("Successfully deleted all %d test objects...", cfg->n_test_keys);

	btree_get_stats(cfg->bt, &bt_stats);
	btree_dump_stats(stderr, &bt_stats);

	/* delete the btree */
	btree_destroy(cfg->bt);

	msg("Ending btree test...  TEST PASSED!");

	exit(0);
}

static char *gendata(uint32_t max_datalen, uint64_t *pdatalen)
{
	uint32_t   datalen;
	char      *pdata;
	uint32_t   i;

	datalen = random() % max_datalen;
	pdata = (char *) malloc(datalen);
	assert(pdata);
	if (datalen > 0) {
		for (i=0; i<datalen-1; i++) {
			pdata[i] = char_array[random()%n_char_array];
		}
		pdata[datalen-1] = '\0';
	}
	*pdatalen = datalen;
	return(pdata);
}

/**********************
 *
 *  Callback Functions
 *
 **********************/

static struct btree_raw_node *read_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid)
{
    N_read_node++;
    return(NULL);
}

static void write_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid, char *data, uint64_t datalen)
{
    N_write_node++;
}

static void flush_node_cb(btree_status_t *ret, void *cb_data, uint64_t lnodeid)
{
    N_flush_node++;
}

static int freebuf_cb(void *data, char *buf)
{
    N_freebuf++;
    return(0);
}

static struct btree_raw_node *create_node_cb(btree_status_t *ret, void *data, uint64_t lnodeid)
{
    N_create_node++;
    return(NULL);
}

static btree_status_t delete_node_cb(struct btree_raw_node *node, void *data, uint64_t lnodeid)
{
    N_delete_node++;
    return(0);
}

static void log_cb(btree_status_t *ret, void *data, uint32_t event_type, struct btree_raw *btree, struct btree_raw_node *n)
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
    return(0);
}

static void txn_cmd_cb(btree_status_t *ret_out, void *cb_data, int cmd_type)
{
    switch (cmd_type) {
        case 1: // start txn
            N_txn_cmd_1++;
            break;
        case 2: // commit txn
            N_txn_cmd_2++;
            break;
        case 3: // abort txn
            N_txn_cmd_3++;
            break;
        default:
            assert(0);
            break;
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
        default: prefix = "PROBLEM WITH MSG_CB!"; quit = 1; break;
	    break;
    } 

    (void) fprintf(stderr, "%s: %s", prefix, stmp);
    if (quit) {
        assert(0);
        exit(1);
    }
}
