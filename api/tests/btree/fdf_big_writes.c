#include <fdf.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>
#include <stdbool.h>
#include <pthread.h>
#include <sched.h>

#define FDF_MAX_KEY_LEN       100
#define LARGE_OBJECT_SIZE     (25 * 1024 * 1024)
//#define LARGE_OBJECT_SIZE     (800 * 1024)
#define SMALL_OBJECT_SIZE     100

#define MAX_THREADS           128

FDF_cguid_t cguid;
struct FDF_state *fdf_state;

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

typedef struct {
	uint32_t ncols;
	uint32_t nrows;
	uint32_t start_row;
	unsigned long data_size;
} blk_write_param_t;

void *do_bulk_write(void *arg)
{
	blk_write_param_t *p = (blk_write_param_t *)arg;
	uint32_t *large_cols;
	uint32_t large_col_cnt;
	FDF_status_t status;
	FDF_obj_t *objs = NULL; 
	struct FDF_thread_state *thd_state;
	int i, l, c;
	uint32_t objs_written = 0;

	FDFInitPerThreadState(fdf_state, &thd_state);	

	for (i = p->start_row; i < p->start_row + p->nrows; i++) {
		objs = (FDF_obj_t *) malloc(sizeof(FDF_obj_t) * p->ncols);
		if (objs == NULL) {
			printf("Cannot allocate memory.\n");
			fflush(stdout);
			exit(0);
		}

		/* Random number of cols in a row will be large */
		large_col_cnt = rand() % p->ncols;

		/* Determine randomly which of the cols among the count to be larger */
		large_cols = malloc(sizeof(uint32_t) * large_col_cnt);
		for (l = 0; l < large_col_cnt; l++) {
			large_cols[l] = rand() % p->ncols;
		}

		l = 0;
		memset(objs, 0, sizeof(FDF_obj_t) * p->ncols);

		for (c = 0; c < p->ncols; c++) { 
			objs[c].key = malloc(FDF_MAX_KEY_LEN);
			if (objs[c].key == NULL) {
				printf("Cannot allocate memory.\n");
				fflush(stdout);
				exit(0);
			}

			memset(objs[c].key, 0, 50);
			sprintf(objs[c].key, "%04d:%04d", i, c);
//			objs[c].key_len = strlen(objs[c].key) + 1;
			objs[c].key_len = 50;

			if (c == large_cols[l]) {
				l++;
				objs[c].data_len = (uint64_t)p->data_size;
			} else {
				objs[c].data_len = SMALL_OBJECT_SIZE;
			}

			objs[c].data = malloc(objs[c].data_len + 1);
			gen_data(objs[c].data, objs[c].data_len, 1);
			objs[c].flags = 0;
		}

		uint32_t flags = 0;
		status = FDFMPut(thd_state, cguid, p->ncols,
		                 &objs[0], flags, &objs_written);
		if (status != FDF_SUCCESS) {
			fprintf(stderr, "Failed to write objects using FDFMPut, status = %d.\n",
			        status);
			fflush(stdout);
			return NULL;
		}

		status = FDFMPut(thd_state, cguid, p->ncols,
		                 &objs[0], flags, &objs_written);
		if (status != FDF_SUCCESS) {
			fprintf(stderr, "Failed to update objects using FDFMPut, status = %d.\n",
			        status);
			fflush(stdout);
			return NULL;
		}

		for (c = 0; c < p->ncols; c++) {
			free(objs[c].key);
			free(objs[c].data);
		}

		free(objs);
		free(large_cols);
	}
	fprintf(stderr, "MPut from %d to %d rows completed successfully\n", p->start_row, 
	                p->start_row + p->nrows);

	FDFReleasePerThreadState(&thd_state);
	return NULL;
}

void *do_read_verify(void *arg)
{
	blk_write_param_t *p = (blk_write_param_t *)arg;
	FDF_status_t status;
	struct FDF_thread_state *thd_state;
	int i, c;
	char key[FDF_MAX_KEY_LEN];
	char *data;
	uint64_t datalen;
	uint32_t keylen;
	uint32_t errors = 0;

	FDFInitPerThreadState(fdf_state, &thd_state);	

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

			status = FDFReadObject(thd_state, cguid, key, keylen,
			                       &data, &datalen);
			if (status != FDF_SUCCESS) {
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

	FDFReleasePerThreadState(&thd_state);
	return NULL;
}

void launch_blk_write_threads(uint32_t nrows, uint32_t ncols, uint32_t nthreads, unsigned long data_size)
{
	pthread_t thread_id[MAX_THREADS];
	blk_write_param_t p[MAX_THREADS];
	int i;

	for(i = 0; i < nthreads; i++) {
		fprintf(stderr,"Creating Writer thread %i\n",i );

		p[i].ncols = ncols;
		p[i].start_row = i  * (nrows/nthreads);
		p[i].nrows = nrows/nthreads;
		p[i].data_size = data_size;
		if(pthread_create(&thread_id[i], NULL, do_bulk_write, (void *)&p[i])) {
			perror("pthread_create: ");
			exit(1);
		}
	}

	for(i = 0; i < nthreads; i++) {
		pthread_join(thread_id[i], NULL);
	}

	for (i = 0; i < nthreads; i++) {
		fprintf(stderr,"Creating Reader thread %i\n",i );

		p[i].ncols = ncols;
		p[i].start_row = i  * (nrows/nthreads);
		p[i].nrows = nrows/nthreads;
		p[i].data_size = data_size;
		if(pthread_create(&thread_id[i], NULL, do_read_verify, (void *)&p[i])) {
			perror("pthread_create: ");
			exit(1);
		}
	}

	for(i = 0; i < nthreads; i++) {
		pthread_join(thread_id[i], NULL);
	}

}

static void 
open_stuff(struct FDF_thread_state *thd_state, char *cntr_name)
{
	FDF_container_props_t props;
	FDF_status_t status;

	FDFLoadCntrPropDefaults(&props);
	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	props.size_kb = 0;
//	props.size_kb = (1024 * 1024 * 10);;

	status = FDFOpenContainer(thd_state, cntr_name, &props, FDF_CTNR_CREATE, &cguid);
	if (status != FDF_SUCCESS) {
		printf("Open Cont failed with error=%s.\n", FDFStrError(status));
		fflush(stdout);
		exit(-1);
	}
}

static void 
free_stuff(struct FDF_thread_state *thd_state)
{
	FDF_status_t status;

	status = FDFCloseContainer(thd_state, cguid);
	status = FDFDeleteContainer(thd_state, cguid);
	if (status != FDF_SUCCESS) {
		printf("Delete Cont failed with error=%s.\n", FDFStrError(status));
		fflush(stdout);
		exit(-1);
	}
}

void do_bulk_test(uint32_t nrows, uint32_t ncols, uint32_t nthreads, unsigned long data_size)
{
	struct FDF_thread_state *thd_state;

	FDFInitPerThreadState(fdf_state, &thd_state);	

	open_stuff(thd_state, "cntr_1");
	launch_blk_write_threads(nrows, ncols, nthreads, data_size);	
	free_stuff(thd_state);
	FDFReleasePerThreadState(&thd_state);
}

int 
main(int argc, char *argv[])
{
	uint32_t nrows = 1000;
	uint32_t ncols = 100;
	uint32_t nthreads = 10;
	unsigned long data_size = 1 * 1024 * 1024; // default 1 MB

	printf("Usage: <prog> <num_rows> <num_cols> <num_threads> <data_size>\n");

	if (argc >= 2) {
		nrows = atoi(argv[1]);
	}

	if (argc >= 3) {
		ncols = atoi(argv[2]);
	}

	if (argc >= 4) {
		nthreads = atoi(argv[3]);
	}

	if (argc >= 5) {
		data_size = atol(argv[4]);
	}

	if (nthreads > MAX_THREADS) {
		nthreads = MAX_THREADS;
	}
	fprintf(stderr, "Starting test with %u rows %u cols in %u threads %lu datasize\n",
	        nrows, ncols, nthreads, data_size);

	FDFInit(&fdf_state);
	do_bulk_test(nrows, ncols, nthreads, data_size);
	FDFShutdown(fdf_state);

	return 0;
}
