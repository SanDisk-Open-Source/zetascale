#include <fdf.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>


#define FDF_MAX_KEY_LEN 1979
#define NUM_OBJS 10000 //max mput in single operation
#define NUM_MPUTS 1000000 

static int cur_thd_id = 0;
static __thread int my_thdid = 0;
FDF_cguid_t cguid;
struct FDF_state *fdf_state;
int num_mputs =  NUM_MPUTS;
int num_objs = NUM_OBJS;
int use_mput = 1;
uint32_t flags_global = 0;
int num_thds = 1;

int cnt_id = 0;


inline uint64_t
get_time_usecs(void)
{
        struct timeval tv = { 0, 0};
        gettimeofday(&tv, NULL);
        return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}


void
do_mput(struct FDF_thread_state *thd_state, FDF_cguid_t cguid,
	uint32_t flags, int key_seed)
{
	int i, k;
	FDF_status_t status;
	FDF_obj_t *objs = NULL; 
	uint64_t num_fdf_writes = 0;
	uint64_t num_fdf_reads = 0;
	uint64_t num_fdf_mputs = 0;
	uint32_t objs_written = 0;
	char *data;
	uint64_t data_len;
	uint64_t key_num = 0;
	uint64_t mismatch = 0;

	objs = (FDF_obj_t *) malloc(sizeof(FDF_obj_t) * num_objs);
	if (objs == NULL) {
		printf("Cannot allocate memory.\n");
		exit(0);
	}
	memset(objs, 0, sizeof(FDF_obj_t) * num_objs);
	for (i = 0; i < num_objs; i++) {
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

	printf("Doing Mput in threads %d.\n", my_thdid);
	get_time_usecs();
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, FDF_MAX_KEY_LEN);
			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "key_%d_%06"PRId64"", my_thdid, key_num);

			key_num += key_seed;
			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].data_len = strlen(objs[i].data) + 1;
			objs[i].flags = 0;
			if (!use_mput) {
				status = FDFWriteObject(thd_state, cguid,
						        objs[i].key, objs[i].key_len,
							objs[i].data, objs[i].data_len, flags);
				if (status != FDF_SUCCESS) {
					printf("Write failed with %d errror.\n", status);
					assert(0);
				}
				num_fdf_writes++;
			}
		}

		if (use_mput) {
			status = FDFMPut(thd_state, cguid, num_objs,
					 &objs[0], flags, &objs_written);
			if (status != FDF_SUCCESS) {
				printf("Failed to write objects using FDFMPut, status = %d.\n",
					status);
				assert(0);
				return ;
			}
			num_fdf_mputs++;
		}


	}



	num_fdf_reads = 0;
	
	printf("Reading all objects put in thread = %d.\n", my_thdid);
	key_num = 0;
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, FDF_MAX_KEY_LEN);

			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "key_%d_%06"PRId64"", my_thdid, key_num);

			key_num += key_seed;

			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].data_len = strlen(objs[i].data) + 1;
			objs[i].flags = 0;

			status = FDFReadObject(thd_state, cguid,
					       objs[i].key, objs[i].key_len,
						&data, &data_len);
			if (status != FDF_SUCCESS) {
					printf("Read failed with %d errror.\n", status);
					assert(0);
					exit(0);
			}

			if (data_len != objs[i].data_len) {
				printf("Object length of read object mismatch.\n");	
				assert(0);
				mismatch++;
			}
			num_fdf_reads++;
		}
	}

	printf("Verified the mput objects using reads, mismatch = %"PRId64".\n", mismatch);



#if 0
	key_num = 0;
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, FDF_MAX_KEY_LEN);

			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);
			key_num++;

			objs[i].key_len = strlen(objs[i].key) + 1;

			status = FDFDeleteObject(thd_state, cguid,
					       objs[i].key, objs[i].key_len);
			if (status != FDF_SUCCESS) {
				printf("Delete failed with %d errror.\n", status);
				assert(0);
				exit(0);
			}

		}

	}

	printf("Deleted objects successfully for thread = %d.\n", my_thdid);
#endif 
}

void *
write_stress(void *t)
{
	struct FDF_thread_state *thd_state;


	my_thdid = __sync_fetch_and_add(&cur_thd_id, 1);
	FDFInitPerThreadState(fdf_state, &thd_state);	

	if (flags_global & FDF_WRITE_MUST_EXIST) {
		do_mput(thd_state, cguid, FDF_WRITE_MUST_NOT_EXIST, 1); //populate data if it is update case.
	}

	if (flags_global == 0) {
		do_mput(thd_state, cguid, FDF_WRITE_MUST_NOT_EXIST, 2); //sparsely populate for set case
	}

	do_mput(thd_state, cguid, flags_global, 1);

	return NULL;
}


void launch_thds()
{
	int i;
	pthread_t thread_id[128];

	sleep(1);
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

}
void
do_op(uint32_t flags_in) 
{
	FDF_container_props_t props;
	struct FDF_thread_state *thd_state;
	FDF_status_t status;

	char cnt_name[100] = {0};

	sprintf(cnt_name, "cntr_%d", cnt_id++);


	FDFInitPerThreadState(fdf_state, &thd_state);	

	FDFLoadCntrPropDefaults(&props);

	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	props.size_kb = (1024 * 1024 * 10);;

	status = FDFOpenContainer(thd_state, cnt_name, &props, FDF_CTNR_CREATE, &cguid);
	if (status != FDF_SUCCESS) {
		printf("Open Cont failed with error=%x.\n", status);
		exit(-1);
	}

	flags_global = flags_in;
	launch_thds(); //actual operations

	FDFCloseContainer(thd_state, cguid);
	FDFDeleteContainer(thd_state, cguid);

	FDFReleasePerThreadState(&thd_state);
}

int 
main(int argc, char *argv[])
{
	int n, m;

	if (argc < 5) {
		printf("Usage: ./run 0/1(use mput)  num_mputs num_objs_each_mput num_thds.\n");
		exit(0);
	}

	use_mput = atoi(argv[1]);
	m = atoi(argv[2]);
	if (m > 0) {
		num_mputs = m;	
	}
	n = atoi(argv[3]);
	if (n > 0) {
		num_objs = n;
	}

	n = atoi(argv[4]);
	if (n > 0) {
		num_thds = n;
	}

	printf("Running with mput (y/n) = %d, mputs = %d, num objs each mput = %d, num threads = %d.\n",
		use_mput, num_mputs, num_objs, num_thds);

	FDFInit(&fdf_state);

	printf(" ======================== Doing test for set case. ===================\n");
	do_op(0);// set
	printf(" ******************  Done test for set case.***********************\n");

	printf(" ======================== Doing test for create case. ===================\n");
	do_op(FDF_WRITE_MUST_NOT_EXIST); //create
	printf(" ******************  Done test for create  case.***********************\n");

	printf(" ======================== Doing test for update case. ===================\n");
	do_op(FDF_WRITE_MUST_EXIST); //update
	printf(" ******************  Done test for update  case.***********************\n");

	FDFShutdown(fdf_state);
	return 0;
}

