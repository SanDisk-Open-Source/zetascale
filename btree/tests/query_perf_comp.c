/****************************
#function : query_perf_comp.c
#author   : Harihara Kadayam
#date     : Jun 03 2013
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <fdf.h>
#include <sys/time.h>

static FILE                    *fp;
static struct FDF_state        *fdf_state;
static struct FDF_thread_state *fdf_thrd_state;

static char *char_array = "abcdefghijklmnopqrstuvwxyz0123456789_-.";
static char **data_arr;     
static uint32_t *datalen_arr;

static char *gen_data(uint32_t max_datalen, uint32_t *pdatalen);
static void discard_data(uint32_t n_objects);
//static long get_point_query_perf(int parallel, uint32_t n_objects, int query_cnt);
//static long get_range_query_perf(int parallel, uint32_t n_objects, uint32_t query_cnt, int chunks);

#define MAX_KEYLEN            20
#define MAX_DATALEN           500
#define DEFAULT_WRITE_THREADS 32
#define DEFAULT_READ_THREADS  32
#define DEFAULT_N_OBJECTS     64000
#define DEFAULT_TEST_ITER     100

int test_iter = DEFAULT_TEST_ITER;

FDF_status_t PreEnvironment()
{
	FDF_status_t ret;
    
//	FDFSetProperty("FDF_CACHE_SIZE", "100000000");

	ret = FDFInit(&fdf_state);
	if (ret != FDF_SUCCESS) {
		fprintf(fp, "FDF initialization failed. Status = %d\n", ret);
		return (ret);
	}

	fprintf(fp, "FDF initialization succeed!\n");
	ret = FDFInitPerThreadState(fdf_state, &fdf_thrd_state);
        if( ret != FDF_SUCCESS) {
		fprintf(fp, "FDF thread initialization failed!\n");
		return (ret);
	}

	fprintf(fp, "FDF thread initialization succeed!\n");
	return ret;
}

void ClearEnvironment()
{
	(void)FDFReleasePerThreadState(&fdf_thrd_state);
	(void)FDFShutdown(fdf_state);

	fprintf(fp, "clear env completed.\n");
}

static FDF_status_t OpenContainer(char *cname, FDF_cguid_t *pcguid)
{
	FDF_status_t          ret;
	FDF_container_props_t p;
	uint32_t flag;

	ret = FDF_FAILURE;        

	(void)FDFLoadCntrPropDefaults(&p);
	p.async_writes = 0;
	p.durability_level = 0;
	p.fifo_mode = 0;
	p.persistent = 1;
	p.writethru = 1;
	p.size_kb = 20*1024*1024; // 20GB Container
	p.num_shards = 1;
	p.evicting = 0;
 
	flag = FDF_CTNR_CREATE;
	fprintf(fp, "FDFOpenContainer: Attempting to create with "
	                         "Writeback mode of size 2GB\n");

	ret = FDFOpenContainer(fdf_thrd_state,
	                       cname,
	                       &p,
	                       flag,
	                       pcguid);

	fprintf(fp, "FDFOpenContainer : %s\n",FDFStrError(ret));
	return ret;
}

FDF_status_t CloseContainer(FDF_cguid_t cguid)
{
	FDF_status_t ret;
	ret = FDFCloseContainer(fdf_thrd_state, cguid);

	fprintf(fp,"FDFCloseContainer : ");
	fprintf(fp,"%s\n",FDFStrError(ret));

	return ret;
}

FDF_status_t DeleteContainer(FDF_cguid_t cguid)
{
	FDF_status_t ret;
	ret = FDFDeleteContainer(fdf_thrd_state, cguid);

	fprintf(fp,"FDFDeleteContainer : ");
	fprintf(fp,"%s\n",FDFStrError(ret));

	return ret;
}

FDF_status_t DeleteObject(FDF_cguid_t cguid, char *key, uint32_t keylen)
{
	FDF_status_t ret;
	ret = FDFDeleteObject(fdf_thrd_state, cguid, key, keylen);

	fprintf(fp,"FDFDeleteObject : ");
	fprintf(fp,"%s\n",FDFStrError(ret));
	return ret;
}

int GenerateKeyDataSeries(struct FDF_thread_state *thrd_state, FDF_cguid_t cguid, 
                          uint32_t start, uint32_t end, uint32_t flags)
{
	FDF_status_t ret = FDF_SUCCESS;
	char *keytmp;
	uint32_t i;
	struct timeval start_tv, end_tv;
	long wdiff = 0;

	keytmp = malloc(MAX_KEYLEN);
	assert(keytmp);

	for (i = start; i < end; i++) {
		/* generate serial key and data */
		(void) sprintf(keytmp, "%08d", i);
		data_arr[i] = gen_data(MAX_DATALEN, &datalen_arr[i]);

		gettimeofday(&start_tv, NULL);
		ret = FDFWriteObject(thrd_state, cguid, 
		                     keytmp, strlen(keytmp) + 1, 
		                     data_arr[i], datalen_arr[i], flags);
		if (ret != FDF_SUCCESS) {
			fprintf(fp, "WriteObject failed with status=%s\n", 
			            FDFStrError(ret));
			return -1;
		}
		gettimeofday(&end_tv, NULL);

		wdiff += ((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
		         (end_tv.tv_usec - start_tv.tv_usec);
	}

	free(keytmp);
	return (wdiff);
}

/******************** Helper Functions *******************/
static char *gen_data(uint32_t max_datalen, uint32_t *pdatalen)
{
	uint32_t   datalen;
	char      *pdata;
	uint32_t   i;
	uint32_t   n_char_array;

	n_char_array = strlen(char_array) - 1;

	datalen = random() % max_datalen;
//	datalen = 100;
	if (datalen == 0) datalen = 1;
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

static void discard_data(uint32_t n_objects)
{
	int i;
	for (i = 0; i < n_objects; i++) {
//		free(data_arr[i]);
	}
//	free(datalen_arr);
}

typedef struct {
	FDF_cguid_t cguid;
	uint32_t start;
	uint32_t end;
	uint32_t flags;
	uint32_t  chunks;
	pthread_t thread_id;
	long      write_time;
	long      read_time;
} thr_obj_range_t;

static void *
thr_write(void *arg)
{
	thr_obj_range_t *r = (thr_obj_range_t *)arg;
	struct FDF_thread_state *my_state;

	(void)FDFInitPerThreadState(fdf_state, &my_state);
	r->write_time = GenerateKeyDataSeries(my_state, r->cguid, r->start, r->end, r->flags);
	(void)FDFReleasePerThreadState(&my_state);
	return (NULL);
}

static void *
thr_point_query(void *arg)
{
	thr_obj_range_t *r = (thr_obj_range_t *)arg;
	struct FDF_thread_state *my_state;
	struct timeval start_tv, end_tv;
	char keytmp[MAX_KEYLEN];
	FDF_status_t status;
	int i;
	int iter = 0;

	(void)FDFInitPerThreadState(fdf_state, &my_state);

	while (iter < test_iter) {
		r->read_time = 0;
		for (i = r->start; i < r->end; i++) {
			(void) sprintf(keytmp, "%08d", i);
			gettimeofday(&start_tv, NULL);
			status = FDFReadObject(my_state, r->cguid, 
			                       keytmp, strlen(keytmp)+1,
			                       &data_arr[i], (uint64_t *)&datalen_arr[i]);

			if (status != FDF_SUCCESS) {
				fprintf(fp, "Error in FDFReadObject. Status "
				            " = %d\n", status);
				r->read_time = 0;
				break;
			}
			gettimeofday(&end_tv, NULL);

			r->read_time += ((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
			                  (end_tv.tv_usec - start_tv.tv_usec);
		}
		printf("Point Query Iter=%d read time = %"PRIu64" usecs\n", iter, r->read_time);
		iter++;
	//	usleep(50000);
		sleep(1);
	}

	(void)FDFReleasePerThreadState(&my_state);
	return (NULL);
}

static void *
thr_range_query(void *arg)
{
	thr_obj_range_t *r = (thr_obj_range_t *)arg;

	struct FDF_thread_state *my_state;
	FDF_status_t status;
	FDF_range_meta_t  rmeta;
	FDF_range_data_t *values = NULL;
	struct FDF_cursor *cursor;       // opaque cursor handle

	struct timeval start_tv, end_tv;
	int n_out;
	int n_in_chunk;
	int i, j;
	int iter = 0;

	(void)FDFInitPerThreadState(fdf_state, &my_state);

	fprintf(fp, "####Doing Range query test in %d chunks\n", r->chunks);
	if (r->chunks == -1) {
		n_in_chunk = 1;
		r->chunks = r->end - r->start;
	} else if (r->chunks == 1) {
		n_in_chunk = r->end - r->start;
	} else {
		n_in_chunk = (r->end - r->start)/ r->chunks + 1;
	}

	for (i = 0; i < r->chunks; i++) {
		values = (FDF_range_data_t *)malloc(sizeof(FDF_range_data_t) * n_in_chunk);
		assert(values);
	}

	bzero(&rmeta, sizeof(FDF_range_meta_t));
	rmeta.flags = FDF_RANGE_START_GE | FDF_RANGE_END_LT | 
	              FDF_RANGE_BUFFER_PROVIDED | FDF_RANGE_ALLOC_IF_TOO_SMALL;

	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", r->start);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", r->end);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1; 

/*	rmeta.flags = FDF_RANGE_START_LE | FDF_RANGE_END_GT | 
	              FDF_RANGE_BUFFER_PROVIDED | FDF_RANGE_ALLOC_IF_TOO_SMALL; 

	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", r->end);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", r->start);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1; */

	while (iter < test_iter) {
		status = FDFGetRange(my_state, 
		                     r->cguid,
		                     FDF_RANGE_PRIMARY_INDEX,
		                     &cursor, 
		                     &rmeta);

		if (status != FDF_SUCCESS) {
			fprintf(fp, "FDFStartRangeQuery failed with status=%d\n", status);
			return (NULL);
		}

		r->read_time = 0;
		for (i = 0; i < r->chunks; i++) {
			for (j = 0; j < n_in_chunk; j++) {
/*		for (i = r->chunks - 1; i >= 0; i--) {
			for (j = n_in_chunk - 1; j >= 0; j--) { */
				values[j].key = malloc(MAX_KEYLEN);
				assert(values[j].key);
				values[j].keylen = MAX_KEYLEN;

				int offset = (i * n_in_chunk) + j + r->start;
				values[j].data = data_arr[offset];
				values[j].datalen = datalen_arr[offset];
			}

			gettimeofday(&start_tv, NULL);
			status = FDFGetNextRange(my_state, 
			                         cursor,
			                         n_in_chunk,
			                         &n_out,
			                         values);
			if ((status != FDF_SUCCESS) &&
			    (status != FDF_WARNING) &&
			    (status != FDF_QUERY_DONE)) {
				fprintf(fp, "FDFGetNextRange failed with status=%s\n", FDFStrError(status));
				r->read_time = 0;
				break;
			} 
			gettimeofday(&end_tv, NULL);

/*			if ((i % 50) == 0) {
				printf("Chunk %d read took: %"PRIu64" usecs\n", i,
				        (((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
			                 (end_tv.tv_usec - start_tv.tv_usec)));
			} */

			/*if (n_out != query_cnt) {
				fprintf(fp, "FDFGetNextRange returned inconsistent result, "
				            "input n=%d, output cnt=%d does not match\n",
				             query_cnt, n_out);
				return -1;
			} */

			r->read_time += ((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
			                 (end_tv.tv_usec - start_tv.tv_usec);
		}
		(void)FDFGetRangeFinish(my_state, cursor);

		printf("Range Query Iter=%d read time = %"PRIu64" usecs\n", iter, r->read_time);
		iter++;

		sleep(1);
	}

	(void)FDFReleasePerThreadState(&my_state);
	return (NULL);
}

static void do_insert_parallel(FDF_cguid_t cguid, int n_threads, 
                               uint32_t start, uint32_t end, thr_obj_range_t **range)
{
	uint32_t start_chunk, end_chunk, slice;
	thr_obj_range_t *r;
	int ret;
	int i;

	slice = (end - start)/n_threads;
	start_chunk = start;
	end_chunk = start + slice;
	r = malloc(sizeof(thr_obj_range_t) * n_threads);
	
	assert(r);
	for (i = 0; i < n_threads; i++) {
		r[i].start = start_chunk;
		r[i].end = end_chunk;
		r[i].cguid = cguid;
		r[i].flags = 0;

		ret = pthread_create(&r[i].thread_id, NULL, &thr_write, (void *)&r[i]);
		if (ret != 0) {
			fprintf(fp, "####Error in creating thread: %d\n", i);
		}

		start_chunk += slice;
		end_chunk += slice;
	}
	*range = r;
}

static void do_point_query_parallel(FDF_cguid_t cguid, int n_threads, 
                               uint32_t start, uint32_t end, thr_obj_range_t **range)
{
	uint32_t start_chunk, end_chunk, slice;
	thr_obj_range_t *r;
	int ret;
	int i;

	slice = (end - start)/n_threads;
	start_chunk = start;
	end_chunk = start + slice;
	r = malloc(sizeof(thr_obj_range_t) * n_threads);
	
	assert(r);
	for (i = 0; i < n_threads; i++) {
		r[i].start = start_chunk;
		r[i].end = end_chunk;
		r[i].cguid = cguid;
		r[i].flags = 0;

		ret = pthread_create(&r[i].thread_id, NULL, &thr_point_query, (void *)&r[i]);
		if (ret != 0) {
			fprintf(fp, "####Error in creating thread: %d\n", i);
		}

		start_chunk += slice;
		end_chunk += slice;
	}
	*range = r;
} 

static void do_range_query_parallel(FDF_cguid_t cguid, int n_threads, int chunks,
                                    uint32_t start, uint32_t end, thr_obj_range_t **range)
{
	uint32_t start_chunk, end_chunk, slice;
	thr_obj_range_t *r;
	int ret;
	int i;

	slice = (end - start)/n_threads;
	start_chunk = start;
	end_chunk = start + slice;
	r = malloc(sizeof(thr_obj_range_t) * n_threads);
	
	assert(r);
	for (i = 0; i < n_threads; i++) {
		r[i].start = start_chunk;
		r[i].end = end_chunk;
		r[i].cguid = cguid;
		r[i].flags = 0;
		r[i].chunks = chunks;

		ret = pthread_create(&r[i].thread_id, NULL, &thr_range_query, (void *)&r[i]);
		if (ret != 0) {
			fprintf(fp, "####Error in creating thread: %d\n", i);
		}

		start_chunk += slice;
		end_chunk += slice;
	}
	*range = r;
}

static long get_query_perf(int write_n_threads, int read_n_threads, uint32_t n_objects, int chunks, int is_range)
{
	uint32_t i;
	FDF_status_t status;
	FDF_cguid_t cguid;
	uint32_t initial_objects;
	long rtime = 0;
	long wtime = 0;
	long initial_wtime = 0;
	thr_obj_range_t *wr = NULL;
	thr_obj_range_t *rd = NULL;

	fprintf(fp, "\n####Starting to measure query perf with write_n_threads=%d, read_n_threads=%d\n",
	               write_n_threads, read_n_threads);

	status = OpenContainer("pcheck1", &cguid);
	if (status != FDF_SUCCESS) {
		fprintf(fp, "####get_point_query_perf failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	data_arr    = (char **)malloc(sizeof(char *) * n_objects); 
	datalen_arr = (uint32_t *)malloc(sizeof(uint32_t) * n_objects); 

	initial_objects = write_n_threads > 0 ? n_objects/2 : n_objects;

	fprintf(fp, "####Generating %u objects out of total(%u) into container\n", initial_objects, n_objects);
	initial_wtime = GenerateKeyDataSeries(fdf_thrd_state, cguid, 0, initial_objects, 0);
	if (initial_wtime == -1) {
		fprintf(fp, "####get_query_perf failed. Writing serial key "
		        "and data has failed 1st time. ret=%d\n", -1);
		return -1;
	}
	fprintf(fp, "####Generating %u objects took %ld usecs\n", initial_objects, initial_wtime);

	if (write_n_threads > 0) {
		fprintf(fp, "#####Writing %u objects in %d threads\n", n_objects-initial_objects, write_n_threads);
		do_insert_parallel(cguid, write_n_threads, initial_objects, n_objects, &wr);
		usleep(500);
	}

	if (read_n_threads > 0) {
		if (is_range) {
			fprintf(fp, "####Doing range query test for %u objects in %d threads in %d chunks\n",
			             initial_objects, read_n_threads, chunks);
			do_range_query_parallel(cguid, read_n_threads, chunks, 0, initial_objects, &rd);
		} else {
			fprintf(fp, "####Doing point query test for %u objects in %d threads\n",
			             initial_objects, read_n_threads);
			do_point_query_parallel(cguid, read_n_threads, 0, initial_objects, &rd);
		}
	}

	if (write_n_threads > 0) {
		fprintf(fp, "####Waiting for write parallel threads to complete\n");
		for (i = 0; i < write_n_threads; i++) {
			if (wr[i].thread_id != 0) {
				pthread_join(wr[i].thread_id, NULL);
			}
			wtime += wr[i].write_time;
		}
	}

	if (read_n_threads > 0) {
		fprintf(fp, "####Waiting for read parallel threads to complete\n");
		for (i = 0; i < read_n_threads; i++) {
			if (rd[i].thread_id != 0) {
				pthread_join(rd[i].thread_id, NULL);
			}
			rtime += rd[i].read_time;
		}
	}

	fprintf(fp, "####Cleaning up the data and container\n");
	CloseContainer(cguid);
        (void)DeleteContainer(cguid);
	discard_data(n_objects);

	if (is_range) {
		fprintf(fp, "********* For %u objects, range query chunks=%d time = %ld usecs, "
		            "write time = %ld usecs *******\n\n", initial_objects, chunks,
		            rtime, wtime);
	} else {
		fprintf(fp, "********* For %u objects, point query time = %ld usecs, "
		            "write time = %ld usecs *******\n\n", initial_objects,
		            rtime, wtime);
	}
	return (rtime);
}

#if 0
static long get_range_query_perf(int parallel, uint32_t n_objects, int chunks)
{
	FDF_status_t status;
	FDF_cguid_t cguid;
	FDF_range_meta_t  rmeta;
	FDF_range_data_t *values;
	struct FDF_cursor *cursor;       // opaque cursor handle
	uint32_t initial_objects;
	int n_out;
	int n_in_chunk;
	struct timeval start_tv, end_tv;
	long qdiff = 0;
	long wtime = 0;
	long initial_wtime = 0;
	int i;
	thr_obj_range_t *r = NULL;

	fprintf(fp, "####Starting to measure range query perf with parallel IO = %s\n",
	             parallel ? "on" : "off");

	status = OpenContainer("pcheck2", &cguid);
	if (status != FDF_SUCCESS) {
		fprintf(fp, "####get_range_query_perf failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	data_arr    = (char **)malloc(sizeof(char *) * n_objects); 
	datalen_arr = (uint32_t *)malloc(sizeof(uint32_t) * n_objects); 

	initial_objects = n_objects/2;

	fprintf(fp, "####Generating %u objects out of total(%u) into container\n", initial_objects, n_objects);
	initial_wtime = GenerateKeyDataSeries(fdf_thrd_state, cguid, 0, initial_objects, 0);
	if (initial_wtime == -1) {
		fprintf(fp, "####get_range_query_perf failed. Writing serial key "
		        "and data has failed 1st time. ret=%d\n", -1);
		return -1;
	}
	fprintf(fp, "####Generating %u objects took %ld usecs\n", initial_objects, initial_wtime);

	if (parallel) {
		fprintf(fp, "#####Generating Parallel %u objects\n", n_objects-initial_objects);
		generate_parallel_data(cguid, N_THREADS, initial_objects, n_objects, &r);
		usleep(500);
	}

	rmeta.flags = FDF_RANGE_START_GE | FDF_RANGE_END_LT;
	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", 0);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", initial_objects);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1;

	status = FDFGetRange(fdf_thrd_state, 
	                     cguid,
	                     FDF_RANGE_PRIMARY_INDEX,
	                     &cursor, 
	                     &rmeta);

	if (status != FDF_SUCCESS) {
		fprintf(fp, "FDFStartRangeQuery failed with status=%d\n", status);
		return -1;
	}

	if (chunks == -1) {
		n_in_chunk = 1;
		chunks = initial_objects;
	} else if (chunks == 1) {
		n_in_chunk = initial_objects;
	} else {
		n_in_chunk = initial_objects / chunks + 1;
	}

	fprintf(fp, "####Doing Range query test in %d chunks\n", chunks);
	for (i = 0; i < chunks; i++) {
		values = (FDF_range_data_t *)malloc(sizeof(FDF_range_data_t) * n_in_chunk);
		assert(values);

		gettimeofday(&start_tv, NULL);
		status = FDFGetNextRange(fdf_thrd_state, 
		                         cursor,
		                         n_in_chunk,
		                         &n_out,
		                         values);
		if (status != FDF_SUCCESS) {
			fprintf(fp, "FDFGetNextRange failed with status=%d\n", status);
			return -1;
		}
		gettimeofday(&end_tv, NULL);

		/*if (n_out != query_cnt) {
			fprintf(fp, "FDFGetNextRange returned inconsistent result, "
			            "input n=%d, output cnt=%d does not match\n",
			             query_cnt, n_out);
			return -1;
		} */

		qdiff += ((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
		          (end_tv.tv_usec - start_tv.tv_usec);
	}
	fprintf(fp, "####Done Range Query for %d chunks\n", chunks);

	if (parallel) {
		fprintf(fp, "####Waiting for parallel threads to complete\n");
		for (i = 0; i < N_THREADS; i++) {
			if (r[i].thread_id != 0) {
				pthread_join(r[i].thread_id, NULL);
			}
			wtime += r[i].write_time;
		}
	}

	fprintf(fp, "####Cleaning up the data and container\n");
	CloseContainer(cguid);
        (void)DeleteContainer(cguid);
	discard_data(n_objects);

	if (parallel) {
		fprintf(fp, "********* For %u objects, range query in %d chunks = %ld usecs, "
		            "write time = %ld usecs *******\n\n", initial_objects, chunks, qdiff, wtime);
	} else {
		fprintf(fp, "********* For %u objects, range query in %d chunks = %ld usecs\n\n ", 
		           initial_objects, chunks, qdiff);
	}

	return (qdiff);
}
#endif

/****** main function ******/

int main(int argc, char *argv[])
{
	int ret;
	uint32_t n_objects = 0;
	int i;
	int write_n_threads = -1;
	int read_n_threads = -1;

/*	if((fp = fopen("query_perf_comp.log", "w+"))== 0) {
		fprintf(stderr, "Open query_perf_comp.log failed!\n");
		return (1);
	}   */
	fp = stderr;

	i = 1;
	while (i < argc) {
		if (strcmp(argv[i], "-o") == 0) {
			n_objects = strtoul(argv[++i], NULL, 10);
		} else if (strcmp(argv[i], "-wt") == 0) {
			write_n_threads = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-rt") == 0) {
			read_n_threads = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-i") == 0) {
			test_iter = atoi(argv[++i]);
		} else {
			fprintf(stderr, "Usage: %s [-o <n_objects> ] [-wt <write threads> ] [-rt <read_threads>]\n", argv[0]);
			return 1;
		}
		i++;
	}

	if (n_objects == 0) {
		n_objects = DEFAULT_N_OBJECTS;
	}

	if (write_n_threads < 0) {
		write_n_threads = DEFAULT_WRITE_THREADS;
	}

	if (read_n_threads < 0) {
		read_n_threads = DEFAULT_READ_THREADS;
	}

	printf("Test Settings:\n");
	printf("Total objects = %u\n", n_objects);
	printf("Write Threads = %d\n", write_n_threads);
	printf("Read  Threads = %d\n", read_n_threads);

	fprintf(fp, "####Setting up environment for test\n");
	if (PreEnvironment() != FDF_SUCCESS) {
		fprintf(fp, "####ERROR: PreEnvironment failed\n");
		return (1);
	}

	ret = get_query_perf(write_n_threads, read_n_threads, n_objects, 0, 0);
	if (ret == -1) {
		fprintf(fp, "####Point Query perf failed\n");
	}

	ret = get_query_perf(write_n_threads, read_n_threads, n_objects, 1, 1);
	if (ret == -1) {
		fprintf(fp, "####Range Query perf failed\n");
	}

	ret = get_query_perf(write_n_threads, read_n_threads, n_objects, n_objects/10, 1);
	if (ret == -1) {
		fprintf(fp, "####Range Query perf failed\n");
	}

	ret = get_query_perf(write_n_threads, read_n_threads, n_objects, -1, 1);
	if (ret == -1) {
		fprintf(fp, "####Individual Range Query perf failed\n");
	}

	fclose(fp);
	ClearEnvironment();

	return (0);
}
