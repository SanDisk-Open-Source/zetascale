#include <fdf.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>


#define FDF_MAX_KEY_LEN 256
#define NUM_OBJS 10000 //max mput in single operation
#define NUM_MPUTS 1000000 

static int cur_thd_id = 0;
static __thread int my_thdid = 0;
FDF_cguid_t cguid;
struct FDF_state *fdf_state;
int num_mputs =  NUM_MPUTS;
int num_objs = NUM_OBJS;
int use_mput = 1;
int test_run = 1;
uint64_t total_ops = 0;
uint64_t total_up_ops = 0;
uint32_t flags = 0;


inline uint64_t
get_time_usecs(void)
{
        struct timeval tv = { 0, 0};
        gettimeofday(&tv, NULL);
        return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}

#define MPUT_SIZE 1000
void
do_mput(struct FDF_thread_state *thd_state, FDF_cguid_t cguid)
{
	int i, k;
	FDF_status_t status;
	FDF_obj_t *objs = NULL; 
	uint64_t num_fdf_mputs = 0;
	uint64_t key_num = 0;
	uint32_t objs_written = 0;
	int iterations = 0;

	objs = (FDF_obj_t *) malloc(sizeof(FDF_obj_t) * MPUT_SIZE);
	if (objs == NULL) {
		printf("Cannot allocate memory.\n");
		exit(0);
	}
	memset(objs, 0, sizeof(FDF_obj_t) * MPUT_SIZE);
	for (i = 0; i < MPUT_SIZE; i++) {
		objs[i].key = malloc(FDF_MAX_KEY_LEN);
		if (objs[i].key == NULL) {
			printf("Cannot allocate memory.\n");
			exit(0);
		}
		objs[i].data = malloc(1024);
		if (objs[i].data == NULL) {
			printf("Cannot allocate memory.\n");
			exit(0);
		}
	}
	k = 0;

	iterations = num_objs / MPUT_SIZE;
	while (iterations-- > 0) {
		for (i = 0; i < MPUT_SIZE; i++) {
			memset(objs[i].key, 0, FDF_MAX_KEY_LEN);
			sprintf(objs[i].key, "key_%d_%08"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "data_%d_%08"PRId64"", my_thdid, key_num);
			key_num++;

			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].data_len = strlen(objs[i].data) + 1;
			objs[i].flags = 0;
		}

		status = FDFMPut(thd_state, cguid, MPUT_SIZE, &objs[0], flags, &objs_written);
		if (status != FDF_SUCCESS) {
			printf("Failed to write objects using FDFMPut, status = %d.\n",
				status);
			assert(0);
			return ;
			}
		num_fdf_mputs++;
		k += MPUT_SIZE;
	}

	__sync_fetch_and_add(&total_ops, k);
}

void *
write_stress(void *t)
{
	struct FDF_thread_state *thd_state;


	my_thdid = __sync_fetch_and_add(&cur_thd_id, 1);
	FDFInitPerThreadState(fdf_state, &thd_state);	

	do_mput(thd_state, cguid);

	return NULL;
}

int length_incr = 0;
bool range_update_cb(char *key, uint32_t keylen, char *data, uint32_t datalen,
		     void *callback_args, char **new_data, uint32_t *new_datalen)
{
	
	(*new_data) = (char *) malloc(datalen + 2);
	if (*new_data == NULL) {
		return false;
	}	

	memcpy(*new_data, data, datalen);
	(*new_data)[0] = 'J';	//Change in data
	*new_datalen = datalen + length_incr; //Change in data size
	return true;
}

void
do_range_update(struct FDF_thread_state *thd_state, FDF_cguid_t cguid)
{
	FDF_status_t status;
	uint32_t objs_updated = 0;
	char range_key[FDF_MAX_KEY_LEN] = {0};
	uint32_t range_key_len = 0;
	FDF_range_update_cb_t callback_func = range_update_cb;
	void *callback_args = NULL;
	uint64_t total_updates = 0;

	while (test_run) {
		objs_updated = 0;
		sprintf(range_key, "key_%d_", my_thdid);
		range_key_len = strlen(range_key);

		status = FDFRangeUpdate(thd_state, cguid, range_key, range_key_len,
					callback_func, callback_args, &objs_updated);
		if (status != FDF_SUCCESS) {
			assert(objs_updated == 0);
			printf("Failed to do range update.\n");
			assert(0);
			exit(-1);
		}
		total_updates += objs_updated;
	}

	__sync_fetch_and_add(&total_up_ops, total_updates);
}

void *
rupdate_stress(void *t)
{
	struct FDF_thread_state *thd_state;


	my_thdid = __sync_fetch_and_add(&cur_thd_id, 1);
	FDFInitPerThreadState(fdf_state, &thd_state);	

	do_range_update(thd_state, cguid);

	return NULL;
}

int 
main(int argc, char *argv[])
{
	FDF_container_props_t props;
	struct FDF_thread_state *thd_state;
	FDF_status_t	status;
	int num_thds = 1;
	pthread_t thread_id[128];
	int n, m;
	int i;
	int time = 0;

	if (argc < 4) {
		printf("Usage: ./run time(secs) num_objs(multiple of 1000) num_threads.\n");
		exit(0);
	}

	m = atoi(argv[1]);
	if (m > 0) {
		time = m;	
	}
	n = atoi(argv[2]);
	if (n > 1000) {
		num_objs = n;
	}

	n = atoi(argv[3]);
	if (n > 0 && n < 128) {
		num_thds = n;
	}

	printf("Running with time = %dsecs, num objs = %d, threads = %d.\n", time, num_objs, num_thds);


	FDFInit(&fdf_state);
	FDFInitPerThreadState(fdf_state, &thd_state);	

	FDFLoadCntrPropDefaults(&props);

	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	
	status = FDFOpenContainer(thd_state, "cntr", &props, FDF_CTNR_CREATE, &cguid);
	if (status != FDF_SUCCESS) {
		printf("Open Cont failed with error=%x.\n", status);
		return -1;	
	}

	/*
	 * Populate data.
	 */
	sleep(10);
	for(i = 0; i < num_thds; i++) {
		fprintf(stderr,"Creating thread %i\n",i );
		if( pthread_create(&thread_id[i], NULL, write_stress, NULL)!= 0 ) {
		    perror("pthread_create: ");
		    exit(1);
		}
	}

	for(i = 0; i < num_thds; i++) {
		if( pthread_join(thread_id[i], NULL) != 0 ) {
			perror("pthread_join: ");
			exit(1);
		} 
	}

	printf("Total ops = %"PRIu64", in %d secs.\n", total_ops, time);

	/*
	 * update data.
	 */
	cur_thd_id = 0;
	sleep(10);
	for(i = 0; i < num_thds; i++) {
		fprintf(stderr,"Creating thread %i\n",i );
		if( pthread_create(&thread_id[i], NULL, rupdate_stress, NULL)!= 0 ) {
		    perror("pthread_create: ");
		    exit(1);
		}
	}

	sleep(time);
	test_run = 0;   //Stop tests

	for(i = 0; i < num_thds; i++) {
		if( pthread_join(thread_id[i], NULL) != 0 ) {
			perror("pthread_join: ");
			exit(1);
		} 
	}
	printf("Total update ops = %"PRIu64", in %d secs.\n", total_up_ops, time);

	FDFCloseContainer(thd_state, cguid);
	FDFReleasePerThreadState(&thd_state);

	FDFShutdown(fdf_state);
	return 0;
}

