#include <zs.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>
#include <stdbool.h>
#include <pthread.h>
#include <sched.h>

#define ZS_MAX_KEY_LEN       100
#define SMALL_OBJECT_SIZE     500
#define LARGE_OBJECT_SIZE     (1 * 1024 * 1024)

#define MAX_THREADS           128
#define ADMIN_PORT            4000
#define MAX_ERR_CTXT          10000
#define MAX_ELEMS             100000000   /* 100M objects */

#define MIN_SLEEP_USECS       2000       /* 2 msecs */
#define MAX_SLEEP_USECS       1000000    /* 1 secs */
//#define MAX_SLEEP_USECS       40000000    /* 40 secs */
//#define MAX_SLEEP_USECS       15000000    /* 15 secs */
//#define MAX_SLEEP_USECS       100000000    /* 100 secs */

#define MAX_ERR_INJ_PER_TEST  10

typedef enum {
	THR_LAUNCHED,
	THR_RUNNING,
	THR_DONE,
	THR_JOINED
} thr_state_t;

typedef enum {
	TEST_TYPE_READ     = 1<<0,
	TEST_TYPE_WRITE    = 1<<1,
	TEST_TYPE_RQUERY   = 1<<2,
	TEST_TYPE_MPUT     = 1<<3,
	TEST_TYPE_RESCUE   = 1<<4,
	MAX_TEST_TYPES
} test_type_t;

typedef struct {
	bool     write_fail;
	uint32_t node_type;
	char     is_root;
	uint64_t logical_id;
	bool     io_error;
	uint32_t count;
} err_inj_t;

typedef struct {
	uint32_t    nelems;
	uint32_t    start_elem;
	uint32_t    end_elem;
	uint32_t    batch_size;
	uint32_t    repeats;
	uint32_t    *key_arr;
	test_type_t test_type;
	thr_state_t thr_state;
	ZS_status_t status;
} thread_param_t;

typedef void *(pthread_func_t) (void *);
pthread_t thread_id[MAX_THREADS];
thread_param_t glob_p[MAX_THREADS];

#ifdef WITH_MUTEX
pthread_cond_t cond_complete;
pthread_mutex_t cond_mutex;
#endif

void *err_ctxt[MAX_ERR_CTXT];
uint32_t err_ctxt_cnt = 0;
uint32_t missing_keys[MAX_ELEMS];
uint32_t missing_cnt = 0;

uint32_t step = 0;

ZS_cguid_t cguid;
struct ZS_state *zs_state;

static char *char_array = "abcdefghijklmnopqrstuvwxyz0123456789_-.";

static void
gen_data(char *data, uint64_t datalen, int seed)
{
	int i, j;
	uint32_t n_char_array = strlen(char_array) - 1;
	char strN[9];

	if (seed == -1) {
		for (i=0; i< datalen - 1; i++) {
			data[i] = char_array[random() % n_char_array];
		}
	} else {
		sprintf(strN, "%d", seed);
		i = 0; j = 0;
		while (i < datalen-1) {
			data[i++] = strN[j++];
			if (j == strlen(strN)) 
				j = 0;
		}
	}
	data[datalen - 1] = '\0';
}

#if 0
void *do_read_verify(void *arg)
{
	blk_write_param_t *p = (blk_write_param_t *)arg;
	ZS_status_t status;
	struct ZS_thread_state *thd_state;
	int i, c;
	char key[ZS_MAX_KEY_LEN];
	char *data;
	uint64_t datalen;
	uint32_t keylen;
	uint32_t errors = 0;

	ZSInitPerThreadState(zs_state, &thd_state);	

	for (i = p->start_row; i < p->start_row + p->nrows; i++) {
		for (c = 0; c < p->ncols; c++) { 
			if (key == NULL) {
				printf("Cannot allocate memory.\n");
				fflush(stdout);
				exit(0);
			}

			memset(key, 0, 50);
			sprintf(key, "%04d:%04d", i, c);
		//	keylen = strlen(key) + 1;
			keylen = 50;

			status = ZSReadObject(thd_state, cguid, key, keylen,
			                       &data, &datalen);
			if (status != ZS_SUCCESS) {
				fprintf(stderr, "Error: Read object for key '%s'"
				        "returned error '%d'\n", key, status);
				errors++;
				continue;
			}
			
			if ((datalen != p->data_size) && 
			    (datalen != SMALL_OBJECT_SIZE)) {
				fprintf(stderr, "Error: Object for key '%s' returned"
				        " wrong datalen '%"PRIu64"'\n", key, datalen);
				errors++;
				continue;
			}
		}
	}

	if (errors == 0) {
		fprintf(stderr, "Read from %d to %d rows completed successfully\n", p->start_row, 
		                p->start_row + p->nrows);
	} else {
		fprintf(stderr, "ERROR: Read from %d to %d rows failed\n", p->start_row,
		                p->start_row + p->nrows);
	}

	ZSReleasePerThreadState(&thd_state);
	return NULL;
}
#endif

static void 
open_stuff(struct ZS_thread_state *thd_state, char *cntr_name)
{
	ZS_container_props_t props;
	ZS_status_t status;

	ZSLoadCntrPropDefaults(&props);
	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	props.size_kb = 0;
//	props.size_kb = (1024 * 1024 * 10);;

	status = ZSOpenContainer(thd_state, cntr_name, &props, ZS_CTNR_CREATE, &cguid);
	if (status != ZS_SUCCESS) {
		printf("Open Cont failed with error=%s.\n", ZSStrError(status));
		fflush(stdout);
		exit(-1);
	}
}

static void 
free_stuff(struct ZS_thread_state *thd_state)
{
	ZS_status_t status;

	status = ZSCloseContainer(thd_state, cguid);
	status = ZSDeleteContainer(thd_state, cguid);
	if (status != ZS_SUCCESS) {
		printf("Delete Cont failed with error=%s.\n", ZSStrError(status));
		fflush(stdout);
		exit(-1);
	}
}

static void get_last_error(void)
{
	ZS_status_t status;
	uint32_t idx;
	uint32_t ctxt_size;

	idx = __sync_fetch_and_add(&err_ctxt_cnt, 1);
	status = ZSGetLastError(cguid, &err_ctxt[idx], &ctxt_size);

	if (status != ZS_SUCCESS) {
		fprintf(stderr, "ZSGetLastError failed, returned=%s\n", ZSStrError(status));
	} else {
		fprintf(stderr, "ZSGetLastError returned context for size=%u\n", ctxt_size);
	}
}

void *do_read_thr(void *arg)
{
	thread_param_t *p = (thread_param_t *)arg;
	ZS_status_t status;
	struct ZS_thread_state *thd_state;
	int i;
	char key[ZS_MAX_KEY_LEN];
	char *data;
	uint64_t datalen;
	uint32_t keylen;
	uint32_t iter = 0;

	ZSInitPerThreadState(zs_state, &thd_state);	

	while (iter++ < p->repeats) {
	for (i = p->start_elem; i < p->end_elem; i++) {

		/* TODO: Put range query in here itself */
		memset(key, 0, ZS_MAX_KEY_LEN);
		sprintf(key, "%08d", i);
		keylen = strlen(key)+1;

		status = ZSReadObject(thd_state, cguid, key, keylen,
		                       &data, &datalen);
		if (status != ZS_SUCCESS) {
			fprintf(stderr, "Error: Read object for key '%s'"
			        "returned error '%d'\n", key, status);
			p->status = status;

#if 0
			if ((status == ZS_FLASH_EINCONS) || (status == ZS_FLASH_EIO)) {
				do_fix_read_error();
			}
#endif
		} else {
			free(data);
		}

		if (((i - p->start_elem) % (p->nelems/10)) == 0) {
			fprintf(stderr, "Read from (%u-%u) completed %d%%\n", p->start_elem, p->end_elem,
				                 (i-p->start_elem)*100/p->nelems);
		}
	}
	}

//exit:
	ZSReleasePerThreadState(&thd_state);
#ifdef WITH_MUTEX
	pthread_mutex_lock(&cond_mutex);
	p->thr_state = THR_DONE;
	pthread_cond_signal(&cond_complete);
	pthread_mutex_unlock(&cond_mutex);
#else
	p->thr_state = THR_DONE;
#endif
	return NULL;
}

void *do_misskeys_thr(void *arg)
{
	thread_param_t *p = (thread_param_t *)arg;
	ZS_status_t status;
	struct ZS_thread_state *thd_state;
	int i;
	char key[ZS_MAX_KEY_LEN];
	char *data;
	uint64_t datalen;
	uint32_t keylen;

	ZSInitPerThreadState(zs_state, &thd_state);	

	for (i = p->start_elem; i < p->end_elem; i++) {

		/* TODO: Put range query in here itself */
		memset(key, 0, ZS_MAX_KEY_LEN);
		sprintf(key, "%08d", i);
		keylen = strlen(key)+1;

		status = ZSReadObject(thd_state, cguid, key, keylen,
		                       &data, &datalen);
		if (status != ZS_SUCCESS) {
			fprintf(stderr, "Error: Read object for key '%s'"
			        "returned error '%s'\n", key, ZSStrError(status));
			p->status = status;
			if ((status == ZS_OBJECT_UNKNOWN) || (status == ZS_FLASH_EIO) || 
			    (status == ZS_FLASH_EINCONS)) {
				uint32_t idx = __sync_fetch_and_add(&missing_cnt, 1);
				missing_keys[idx] = i;
			}
		} else {
			free(data);
		}

#if 0
		if (((i - p->start_elem) % (p->nelems/10)) == 0) {
			fprintf(stderr, "Read from (%u-%u) completed %d%%\n", p->start_elem, p->end_elem,
				                 (i-p->start_elem)*100/p->nelems);
		}
#endif
	}

	ZSReleasePerThreadState(&thd_state);
#ifdef WITH_MUTEX
	pthread_mutex_lock(&cond_mutex);
	p->thr_state = THR_DONE;
	pthread_cond_signal(&cond_complete);
	pthread_mutex_unlock(&cond_mutex);
#else
	p->thr_state = THR_DONE;
#endif
	return NULL;
}

void *do_write_thr(void *arg)
{
	thread_param_t *p = (thread_param_t *)arg;

	uint32_t *large_cols;
	uint32_t large_col_cnt;
	ZS_status_t status = ZS_SUCCESS;
	ZS_obj_t *objs = NULL; 
	struct ZS_thread_state *thd_state;
	int i, l, c;
	uint32_t objs_written = 0;
	uint32_t iter = 0;
	uint32_t writes = 0;
//	uint32_t pct_complete = 0;

	ZSInitPerThreadState(zs_state, &thd_state);	

	p->status = ZS_SUCCESS;
	p->thr_state = THR_RUNNING;

	while (iter++ < p->repeats) {
		for (i = p->start_elem; i < p->end_elem; i += p->batch_size) {
			objs = (ZS_obj_t *) malloc(sizeof(ZS_obj_t) * p->batch_size);
			if (objs == NULL) {
				printf("Cannot allocate memory.\n");
				fflush(stdout);
				goto exit;
			}
	
			/* Random number of cols in a row will be large */
			large_col_cnt = rand() % p->batch_size;
	
			/* Determine randomly which of the cols among the count to be larger */
			large_cols = malloc(sizeof(uint32_t) * large_col_cnt);
			for (l = 0; l < large_col_cnt; l++) {
				large_cols[l] = rand() % p->batch_size;
			}
	
			l = 0;
			memset(objs, 0, sizeof(ZS_obj_t) * p->batch_size);
	
			for (c = 0; c < p->batch_size; c++) { 
				if (writes == p->nelems) {
					break;
				}
				objs[c].key = malloc(ZS_MAX_KEY_LEN);
				if (objs[c].key == NULL) {
					printf("Cannot allocate memory.\n");
					fflush(stdout);
					goto exit;
				}
	
				memset(objs[c].key, 0, ZS_MAX_KEY_LEN);
				sprintf(objs[c].key, "%08d", p->key_arr ? p->key_arr[i+c] : i+c);
				objs[c].key_len = strlen(objs[c].key) + 1;
//				objs[c].key_len = 50;
	
				if (c == large_cols[l]) {
					l++;
					objs[c].data_len = LARGE_OBJECT_SIZE;
				} else {
					objs[c].data_len = SMALL_OBJECT_SIZE;
				}
	
				objs[c].data = malloc(objs[c].data_len + 1);
				gen_data(objs[c].data, objs[c].data_len, 1);
				objs[c].flags = 0;
				writes++;
			}
	
			uint32_t flags = 0;
			if (p->batch_size == 1) {
				status = ZSWriteObject(thd_state, cguid, objs[0].key, objs[0].key_len,
				                       objs[0].data, objs[0].data_len, flags);
			} else {
				status = ZSMPut(thd_state, cguid, c, &objs[0], flags, &objs_written);
			}
	
			if (status != ZS_SUCCESS) {
				fprintf(stderr, "Failed to write objects using ZSMPut, status = %d.\n",
				        status);
				fflush(stdout);
				if ((status == ZS_FLASH_EIO) || (status == ZS_FLASH_EINCONS)) {
					get_last_error();
				}
				p->status = status;
				goto exit;
			}
	
			for (c = 0; c < p->batch_size; c++) {
				free(objs[c].key);
				free(objs[c].data);
			}
	
			free(objs);
			free(large_cols);
	
			if (((i - p->start_elem) % (p->nelems/10)) == 0) {
				fprintf(stderr, "Write from (%u-%u) completed %d%%\n", p->start_elem, p->end_elem,
				                 (i-p->start_elem)*100/p->nelems);
			}
		}
		fprintf(stderr, "Write from (%u-%u) completed 100%%\n", p->start_elem, p->end_elem);
	}
	
exit:
	ZSReleasePerThreadState(&thd_state);
#ifdef WITH_MUTEX
	pthread_mutex_lock(&cond_mutex);
	p->thr_state = THR_DONE;
	pthread_cond_signal(&cond_complete);
	pthread_mutex_unlock(&cond_mutex);
#else
	p->thr_state = THR_DONE;
#endif
	return NULL;
}

void *do_rescue_thr(void *arg)
{
	thread_param_t *p = (thread_param_t *)arg;

	ZS_status_t status = ZS_SUCCESS;
	struct ZS_thread_state *thd_state;
	int i;
	uint32_t iter = 0;

	ZSInitPerThreadState(zs_state, &thd_state);	

	p->status = ZS_SUCCESS;
	p->thr_state = THR_RUNNING;

	while (iter++ < p->repeats) {
		for (i = p->start_elem; i < p->end_elem; i++) {
			status = ZSRescueContainer(thd_state, cguid, err_ctxt[i]);
			fprintf(stderr, "Rescue Container for err_ctxt[%d] responded "
			        "with status '%s'\n", i, ZSStrError(status));
		}
	}

	ZSReleasePerThreadState(&thd_state);
#ifdef WITH_MUTEX
	pthread_mutex_lock(&cond_mutex);
	p->thr_state = THR_DONE;
	pthread_cond_signal(&cond_complete);
	pthread_mutex_unlock(&cond_mutex);
#else
	p->thr_state = THR_DONE;
#endif
	return NULL;
}

void *do_multiple_thr(void *arg)
{
	uint32_t t;
	uint32_t tests[MAX_TEST_TYPES];
	uint32_t test_cnt = 0;
	uint32_t i;

	thread_param_t *p = (thread_param_t *)arg;

	for (i = 1; i < MAX_TEST_TYPES; i<<=1) {
		if (p->test_type & i) {
			tests[test_cnt++] = i;
		}
	}

	/* Randomly pick from one of the test and call approp. threads */
	t = rand() % (test_cnt-1);

	if (tests[t] == TEST_TYPE_READ) {
		p->batch_size = 1;
		do_read_thr(arg);
	} else if (tests[t] == TEST_TYPE_RQUERY) {
		p->batch_size = 10;
		do_read_thr(arg);
	} else if (tests[t] == TEST_TYPE_WRITE) {
		p->batch_size = 1;
		p->repeats = 1;
		do_write_thr(arg);
	} else if (tests[t] == TEST_TYPE_MPUT) {
		p->batch_size = 10;
		p->repeats = 1;
		do_write_thr(arg);
	}

	return NULL;
}

void inject_flip(err_inj_t *e) 
{
	char cmd[1024];
	char lstr[16];

	if (e->logical_id == 0) {
		strcpy(lstr, "*");
	} else {
		sprintf(lstr, "%lu", e->logical_id);
	}

	sprintf(cmd, "echo \"flip set %s node_type=%u is_root=%c logical_id=%s "
	             "return=%d --count=%u\" | nc localhost %d",
	             e->write_fail ? "set_zs_write_node_error multi_write_type=*" :
	                          "set_zs_read_node_error",
	             e->node_type, e->is_root, lstr,
	             e->io_error ? 5 : 14, e->count, ADMIN_PORT);

	fprintf(stderr, "Command to execute: %s\n", cmd);
	system(cmd);
}

void reset_flip()
{
	char cmd[1024];

	sprintf(cmd, "echo \"flip reset\" | nc localhost %d", ADMIN_PORT);
	fprintf(stderr, "Command to execute: %s\n", cmd);
	system(cmd);
}

void launch_threads(uint32_t nelems, uint32_t batch_size, uint32_t nthreads, 
                    uint32_t repeats, uint32_t *key_arr, test_type_t test_type, pthread_func_t *func)
{
	int i;

	for(i = 0; i < nthreads; i++) {
		fprintf(stderr,"Creating thread %i\n",i );

		glob_p[i].batch_size = batch_size;
		glob_p[i].start_elem = i  * (nelems/nthreads);
		glob_p[i].nelems     = nelems/nthreads;
		if (i == (nthreads-1)) {
			glob_p[i].nelems += nelems % nthreads; // Remaining to last thread
		}
		glob_p[i].end_elem   = glob_p[i].start_elem + glob_p[i].nelems;
		glob_p[i].repeats    = repeats;
		glob_p[i].thr_state  = THR_LAUNCHED;
		glob_p[i].key_arr    = key_arr;
		glob_p[i].test_type  = test_type;

		if(pthread_create(&thread_id[i], NULL, func, (void *)&glob_p[i])) {
			perror("pthread_create: ");
			exit(1);
		}
	}
}

#ifdef WITH_MUTEX
ZS_status_t wait_for_one_thread(uint32_t nthreads, uint32_t *pindex)
{
	int i;

	pthread_mutex_lock(&cond_mutex);
	pthread_cond_wait(&cond_complete, &cond_mutex);

	for (i = 0; i < nthreads; i++) {
		if (glob_p[i].thr_state == THR_DONE) {
			pthread_join(thread_id[i], NULL);
			glob_p[i].thr_state = THR_JOINED;
			*pindex = i;
			pthread_mutex_unlock(&cond_mutex);
			return glob_p[i].status;
		}
	}
	assert(0);
	pthread_mutex_unlock(&cond_mutex);

	return ZS_FAILURE;
}
#endif

ZS_status_t wait_for_threads(uint32_t nthreads)
{
	uint32_t i;
	uint32_t tidx;
	ZS_status_t overall_status = ZS_SUCCESS;

	for (i = 0; i < nthreads; i++) {
#ifdef WITH_MUTEX
		wait_for_one_thread(nthreads, &tidx);
#else
		tidx = i;
		pthread_join(thread_id[tidx], NULL);
#endif

		assert(glob_p[tidx].thr_state == THR_DONE);
		glob_p[tidx].thr_state = THR_JOINED;

		fprintf(stderr, "Thread[%u] returned with status %s\n",
		           tidx, ZSStrError(glob_p[tidx].status));
		if (glob_p[tidx].status != ZS_SUCCESS) {
			overall_status = ZS_FAILURE;
		}
	}

	return overall_status;
}

#if 0
void do_read_test(uint32_t nelems, uint32_t node_type, char is_root,
                  uint64_t logical_id, bool io_error, uint32_t count)
{
	struct ZS_thread_state *thd_state;
	int nthreads;
	ZS_status_t status;

	ZSInitPerThreadState(zs_state, &thd_state);	

	open_stuff(thd_state, "cntr_1");

#ifdef WITH_MUTEX
	pthread_cond_init(&cond_complete, NULL);
	pthread_mutex_init(&cond_mutex, NULL);
#endif

	step = 0;

	/* Insert data in a single thread and wait for its completion */
	fprintf(stderr, "============ Read Test ==========\n");

	fprintf(stderr, "#### Step %d.0: Insert multiple objects\n", ++step);
	nthreads = 10;
	launch_threads(nelems, 100, nthreads, 1, NULL, do_write_thr);
	status = wait_for_threads(nthreads);

	if (status != ZS_SUCCESS) {
		fprintf(stderr, "Write threads reported error: write_status=%d\n", 
		            status);
		return;
	}

	/* Read individual data in 10 threads for 15 times */
	fprintf(stderr, "#### Step %d.0: Issue read for 15 times\n", ++step);
	nthreads = 10;
	launch_threads(nelems, 1, nthreads, 15, NULL, do_read_thr);

#ifdef FLIP_ENABLED
	uint64_t rand_usecs = (rand() % MAX_SLEEP_USECS) + MIN_SLEEP_USECS;
	fprintf(stderr, "#### Step %d.0: Sleep for random (%lu usecs) before inserting error\n", ++step, rand_usecs);
	usleep(rand_usecs);

	fprintf(stderr, "#### Step %d.0: Inserting the error\n", ++step);
	inject_flip(false, node_type, is_root, logical_id, io_error, count);
#else
	sleep(10);
#endif

	fprintf(stderr, "#### Step %d.0: Waiting for all reads to complete\n", ++step);
	status = wait_for_threads(nthreads);

#ifdef WITH_MUTEX
	pthread_cond_destroy(&cond_complete);
	pthread_mutex_destroy(&cond_mutex);
#endif

	free_stuff(thd_state);
	ZSReleasePerThreadState(&thd_state);
}
#endif

static inline char *node_type_str(uint32_t node_type)
{
	switch (node_type) {
	case 0: return "Meta Logical";
	case 1: return "Interior";
	case 2: return "Leaf";
	case 3: return "Overflow";
	case '*': return "Any";
	default: return "Unknown";
	}
}

void find_missing_keys(uint32_t nelems)
{
	ZS_status_t status;

	uint32_t nthreads      = 10;
	uint32_t batch_size    = 100;
	missing_cnt = 0;

	launch_threads(nelems, batch_size, nthreads, 1, NULL, TEST_TYPE_READ, do_misskeys_thr);
	sleep(10);

	status = wait_for_threads(nthreads);

	fprintf(stderr, "==== There are total %u missing keys identified =====\n", missing_cnt);
}

void rescue_container(void)
{
	ZS_status_t status;
	uint32_t nthreads;

	if (err_ctxt_cnt == 0) {
		fprintf(stderr, "No errors reported to rescue the container\n");
		return;
	}

	nthreads = 10;
	if (nthreads < err_ctxt_cnt) {
		nthreads = err_ctxt_cnt;
	}

	/* Split the error context evenly to multiple threads and let it fix
	 * rescue the containers. */
	launch_threads(err_ctxt_cnt, 1, nthreads, 1, NULL, TEST_TYPE_RESCUE, do_rescue_thr);
	sleep(10);

	status = wait_for_threads(nthreads);
}

static int my_cmp_cb(const void *p1, const void *p2)
{
	uint32_t n1 = *((uint32_t *)p1);
	uint32_t n2 = *((uint32_t *)p2);

	if (n1 == n2) return 0;
	if (n1 < n2) return -1;
	return 1;
}

void insert_objects(uint32_t *key_arr, uint32_t nelems)
{
	ZS_status_t status;

	uint32_t nthreads      = 10;
	uint32_t batch_size    = 100;

	if (nelems == 0) {
		fprintf(stderr, "Nothing to insert\n");
		return;
	}

	if (nelems < nthreads) {
		nthreads = nelems;
	}

	if (nelems/nthreads < batch_size) {
		batch_size = nelems/nthreads;
	}

	/* Sort the key arr */
	qsort(key_arr, nelems, sizeof(uint32_t), my_cmp_cb);

	launch_threads(nelems, batch_size, nthreads, 1, key_arr, TEST_TYPE_MPUT, do_write_thr);
	sleep(10);

	status = wait_for_threads(nthreads);
}

void perform_rw_test(uint32_t nelems, err_inj_t *einj, uint32_t einj_count)
{
	struct ZS_thread_state *thd_state;
	int nthreads;
	ZS_status_t status;
#ifdef FLIP_ENABLED
	uint32_t i;
#endif

	step = 0;
	ZSInitPerThreadState(zs_state, &thd_state);	

	open_stuff(thd_state, "cntr_1");

#ifdef WITH_MUTEX
	pthread_cond_init(&cond_complete, NULL);
	pthread_mutex_init(&cond_mutex, NULL);
#endif

#if 0
	/* Insert data in a single thread and wait for its completion */
	fprintf(stderr, "============ RW Test, fail a %s of a %s node (is_root = %s, logical_id = %lu) ==========\n",
	        einj->write_fail ? "write" : "read", node_type_str(einj->node_type), 
	        (einj->is_root == '1') ? "true" : "false", einj->logical_id);
#endif
	fprintf(stderr, "============= RW Test start ==============\n");

	reset_flip();
	err_ctxt_cnt = 0;
	missing_cnt = 0;

	fprintf(stderr, "#### Step %d.0: Insert multiple objects\n", ++step);
	nthreads = 10;
	launch_threads(nelems, 100, nthreads, 1, NULL, TEST_TYPE_MPUT, do_write_thr);

	fprintf(stderr, "#### Step %d.0: Waiting for all inserts to complete\n", ++step);
	status = wait_for_threads(nthreads);

	fprintf(stderr, "#### Step %d.0: Do rangequery, reads, writes, mputs in parallel\n", ++step);
	nthreads = 10;
	launch_threads(nelems, 100, nthreads, 1, NULL, 
	               TEST_TYPE_READ|TEST_TYPE_RQUERY|TEST_TYPE_MPUT, do_multiple_thr);

#ifdef FLIP_ENABLED
	uint64_t rand_usecs = (rand() % MAX_SLEEP_USECS) + MIN_SLEEP_USECS;
	fprintf(stderr, "#### Step %d.0: Sleep for random (%lu usecs) before inserting error\n", ++step, rand_usecs);
	usleep(rand_usecs);

	fprintf(stderr, "#### Step %d.0: Inserting the errors\n", ++step);
	for (i = 0; i < einj_count; i++) {
		err_inj_t *e = &einj[i];
		fprintf(stderr, "\t#### Step %d:%d: Injecting %s failure of %s node "
		                 "(is_root=%s, logical_id = %lu\n", 
		                 step, (i+1),
		                 e->write_fail ? "write" : "read",
		                 node_type_str(e->node_type), 
		                 (e->is_root == '1') ? "true" : "false",
		                 e->logical_id);
		inject_flip(e);
	}
#else
	sleep(10);
#endif

	fprintf(stderr, "#### Step %d.0: Waiting for all operations to complete\n", ++step);
	status = wait_for_threads(nthreads);

	fprintf(stderr, "#### Step %d.0: Identify all the missing objects\n", ++step);
	find_missing_keys(nelems);

	fprintf(stderr, "#### Step %d.0: Rescue Btree\n", ++step);
	rescue_container();

	fprintf(stderr, "#### Step %d.0: Inserting missing objects\n", ++step);
	reset_flip();
	insert_objects(missing_keys, missing_cnt);

	fprintf(stderr, "#### Step %d.0: Identify all the missing objects\n", ++step);
	missing_cnt = 0;
	find_missing_keys(nelems);

	if (missing_cnt == 0) {
		fprintf(stderr, "SUCCESS: All Keys are inserted succesfully\n");
	} else {
		fprintf(stderr, "ERROR: Test failed. There are still %u keys missing\n", missing_cnt);
	}

#ifdef WITH_MUTEX
	pthread_cond_destroy(&cond_complete);
	pthread_mutex_destroy(&cond_mutex);
#endif

	free_stuff(thd_state);
	ZSReleasePerThreadState(&thd_state);
}


int 
main(int argc, char *argv[])
{
	uint32_t nelems = 10000;
	err_inj_t einj[MAX_ERR_INJ_PER_TEST];

	if (argc >= 2) {
		nelems = atoi(argv[1]);
	}

	fprintf(stderr, "Starting test with %u objects\n", nelems);

	ZSInit(&zs_state);

	/* Test 1: Two error injected at same time */
	/* Error 1. Fail two interior non-root nodes writes with media error */
	einj[0].write_fail = true;
	einj[0].node_type  = 1;
	einj[0].is_root    = '0';
	einj[0].logical_id = 0;
	einj[0].io_error   = true;
	einj[0].count      = 2;
	/* Error 2. Fail two leaf root nodes reads with checksum error */
	einj[1].write_fail = false;
	einj[1].node_type  = 2;
	einj[1].is_root    = '0';
	einj[1].logical_id = 0;
	einj[1].io_error   = false;
	einj[1].count      = 2;
	perform_rw_test(nelems, einj, 2);

	/* Test 2: Write failure of a non-leaf root node */
	einj[0].write_fail = true;
	einj[0].node_type  = 1;
	einj[0].is_root    = '1';
	einj[0].logical_id = 0;
	einj[0].io_error   = true;
	einj[0].count      = 1;
//	perform_rw_test(nelems, einj, 1);

	/* Test 3: Three errors injected at same time */
	/* Error 1. Media error on read of overflow nodes */
	einj[0].write_fail = false;
	einj[0].node_type  = 3;
	einj[0].is_root    = '0';
	einj[0].logical_id = 0;
	einj[0].io_error   = true;
	einj[0].count      = 2;
	/* Error 2. Checksum errors of read of interior nodes */
	einj[1].write_fail = false;
	einj[1].node_type  = 1;
	einj[1].is_root    = '0';
	einj[1].logical_id = 0;
	einj[1].io_error   = false;
	einj[1].count      = 3;
	/* Error 3. Checksum errors of read of leaf nodes */
	einj[2].write_fail = false;
	einj[2].node_type  = 2;
	einj[2].is_root    = '0';
	einj[2].logical_id = 0;
	einj[2].io_error   = false;
	einj[2].count      = 3;
	perform_rw_test(nelems, einj, 3);

	ZSShutdown(zs_state);

	return 0;
}
