/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
#include <zs.h>
#include <sys/time.h>

static FILE                    *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;

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

ZS_status_t PreEnvironment()
{
	ZS_status_t ret;
    
//	ZSSetProperty("ZS_CACHE_SIZE", "100000000");

	ret = ZSInit(&zs_state);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ZS initialization failed. Status = %d\n", ret);
		return (ret);
	}

	fprintf(fp, "ZS initialization succeed!\n");
	ret = ZSInitPerThreadState(zs_state, &zs_thrd_state);
        if( ret != ZS_SUCCESS) {
		fprintf(fp, "ZS thread initialization failed!\n");
		return (ret);
	}

	fprintf(fp, "ZS thread initialization succeed!\n");
	return ret;
}

void ClearEnvironment()
{
	(void)ZSReleasePerThreadState(&zs_thrd_state);
	(void)ZSShutdown(zs_state);

	fprintf(fp, "clear env completed.\n");
}

static ZS_status_t OpenContainer(char *cname, ZS_cguid_t *pcguid)
{
	ZS_status_t          ret;
	ZS_container_props_t p;
	uint32_t flag;

	ret = ZS_FAILURE;        

	(void)ZSLoadCntrPropDefaults(&p);
	p.async_writes = 0;
	p.durability_level = 0;
	p.fifo_mode = 0;
	p.persistent = 1;
	p.writethru = 1;
	p.size_kb = 20*1024*1024; // 20GB Container
	p.num_shards = 1;
	p.evicting = 0;
 
	flag = ZS_CTNR_CREATE;
	fprintf(fp, "ZSOpenContainer: Attempting to create with "
	                         "Writeback mode of size 2GB\n");

	ret = ZSOpenContainer(zs_thrd_state,
	                       cname,
	                       &p,
	                       flag,
	                       pcguid);

	fprintf(fp, "ZSOpenContainer : %s\n",ZSStrError(ret));
	return ret;
}

ZS_status_t CloseContainer(ZS_cguid_t cguid)
{
	ZS_status_t ret;
	ret = ZSCloseContainer(zs_thrd_state, cguid);

	fprintf(fp,"ZSCloseContainer : ");
	fprintf(fp,"%s\n",ZSStrError(ret));

	return ret;
}

ZS_status_t DeleteContainer(ZS_cguid_t cguid)
{
	ZS_status_t ret;
	ret = ZSDeleteContainer(zs_thrd_state, cguid);

	fprintf(fp,"ZSDeleteContainer : ");
	fprintf(fp,"%s\n",ZSStrError(ret));

	return ret;
}

ZS_status_t DeleteObject(ZS_cguid_t cguid, char *key, uint32_t keylen)
{
	ZS_status_t ret;
	ret = ZSDeleteObject(zs_thrd_state, cguid, key, keylen);

	fprintf(fp,"ZSDeleteObject : ");
	fprintf(fp,"%s\n",ZSStrError(ret));
	return ret;
}

int GenerateKeyDataSeries(struct ZS_thread_state *thrd_state, ZS_cguid_t cguid, 
                          uint32_t start, uint32_t end, uint32_t flags)
{
	ZS_status_t ret = ZS_SUCCESS;
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
		ret = ZSWriteObject(thrd_state, cguid, 
		                     keytmp, strlen(keytmp) + 1, 
		                     data_arr[i], datalen_arr[i], flags);
		if (ret != ZS_SUCCESS) {
			fprintf(fp, "WriteObject failed with status=%s\n", 
			            ZSStrError(ret));
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
	ZS_cguid_t cguid;
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
	struct ZS_thread_state *my_state;

	(void)ZSInitPerThreadState(zs_state, &my_state);
	r->write_time = GenerateKeyDataSeries(my_state, r->cguid, r->start, r->end, r->flags);
	(void)ZSReleasePerThreadState(&my_state);
	return (NULL);
}

static void *
thr_point_query(void *arg)
{
	thr_obj_range_t *r = (thr_obj_range_t *)arg;
	struct ZS_thread_state *my_state;
	struct timeval start_tv, end_tv;
	char keytmp[MAX_KEYLEN];
	ZS_status_t status;
	int i;
	int iter = 0;

	(void)ZSInitPerThreadState(zs_state, &my_state);

	while (iter < test_iter) {
		r->read_time = 0;
		for (i = r->start; i < r->end; i++) {
			(void) sprintf(keytmp, "%08d", i);
			gettimeofday(&start_tv, NULL);
			status = ZSReadObject(my_state, r->cguid, 
			                       keytmp, strlen(keytmp)+1,
			                       &data_arr[i], (uint64_t *)&datalen_arr[i]);

			if (status != ZS_SUCCESS) {
				fprintf(fp, "Error in ZSReadObject. Status "
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

	(void)ZSReleasePerThreadState(&my_state);
	return (NULL);
}

static void *
thr_range_query(void *arg)
{
	thr_obj_range_t *r = (thr_obj_range_t *)arg;

	struct ZS_thread_state *my_state;
	ZS_status_t status;
	ZS_range_meta_t  rmeta;
	ZS_range_data_t *values = NULL;
	struct ZS_cursor *cursor;       // opaque cursor handle

	struct timeval start_tv, end_tv;
	int n_out;
	int n_in_chunk;
	int i, j;
	int iter = 0;

	(void)ZSInitPerThreadState(zs_state, &my_state);

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
		values = (ZS_range_data_t *)malloc(sizeof(ZS_range_data_t) * n_in_chunk);
		assert(values);
	}

	bzero(&rmeta, sizeof(ZS_range_meta_t));
	rmeta.flags = ZS_RANGE_START_GE | ZS_RANGE_END_LT | 
	              ZS_RANGE_BUFFER_PROVIDED | ZS_RANGE_ALLOC_IF_TOO_SMALL;

	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", r->start);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", r->end);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1; 

/*	rmeta.flags = ZS_RANGE_START_LE | ZS_RANGE_END_GT | 
	              ZS_RANGE_BUFFER_PROVIDED | ZS_RANGE_ALLOC_IF_TOO_SMALL; 

	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", r->end);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", r->start);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1; */

	while (iter < test_iter) {
		status = ZSGetRange(my_state, 
		                     r->cguid,
		                     ZS_RANGE_PRIMARY_INDEX,
		                     &cursor, 
		                     &rmeta);

		if (status != ZS_SUCCESS) {
			fprintf(fp, "ZSStartRangeQuery failed with status=%d\n", status);
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
			status = ZSGetNextRange(my_state, 
			                         cursor,
			                         n_in_chunk,
			                         &n_out,
			                         values);
			if ((status != ZS_SUCCESS) &&
			    (status != ZS_WARNING) &&
			    (status != ZS_QUERY_DONE)) {
				fprintf(fp, "ZSGetNextRange failed with status=%s\n", ZSStrError(status));
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
				fprintf(fp, "ZSGetNextRange returned inconsistent result, "
				            "input n=%d, output cnt=%d does not match\n",
				             query_cnt, n_out);
				return -1;
			} */

			r->read_time += ((end_tv.tv_sec - start_tv.tv_sec) * 1000000) + 
			                 (end_tv.tv_usec - start_tv.tv_usec);
		}
		(void)ZSGetRangeFinish(my_state, cursor);

		printf("Range Query Iter=%d read time = %"PRIu64" usecs\n", iter, r->read_time);
		iter++;

		sleep(1);
	}

	(void)ZSReleasePerThreadState(&my_state);
	return (NULL);
}

static void do_insert_parallel(ZS_cguid_t cguid, int n_threads, 
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

static void do_point_query_parallel(ZS_cguid_t cguid, int n_threads, 
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

static void do_range_query_parallel(ZS_cguid_t cguid, int n_threads, int chunks,
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
	ZS_status_t status;
	ZS_cguid_t cguid;
	uint32_t initial_objects;
	long rtime = 0;
	long wtime = 0;
	long initial_wtime = 0;
	thr_obj_range_t *wr = NULL;
	thr_obj_range_t *rd = NULL;

	fprintf(fp, "\n####Starting to measure query perf with write_n_threads=%d, read_n_threads=%d\n",
	               write_n_threads, read_n_threads);

	status = OpenContainer("pcheck1", &cguid);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "####get_point_query_perf failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	data_arr    = (char **)malloc(sizeof(char *) * n_objects); 
	datalen_arr = (uint32_t *)malloc(sizeof(uint32_t) * n_objects); 

	initial_objects = write_n_threads > 0 ? n_objects/2 : n_objects;

	fprintf(fp, "####Generating %u objects out of total(%u) into container\n", initial_objects, n_objects);
	initial_wtime = GenerateKeyDataSeries(zs_thrd_state, cguid, 0, initial_objects, 0);
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
	ZS_status_t status;
	ZS_cguid_t cguid;
	ZS_range_meta_t  rmeta;
	ZS_range_data_t *values;
	struct ZS_cursor *cursor;       // opaque cursor handle
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
	if (status != ZS_SUCCESS) {
		fprintf(fp, "####get_range_query_perf failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	data_arr    = (char **)malloc(sizeof(char *) * n_objects); 
	datalen_arr = (uint32_t *)malloc(sizeof(uint32_t) * n_objects); 

	initial_objects = n_objects/2;

	fprintf(fp, "####Generating %u objects out of total(%u) into container\n", initial_objects, n_objects);
	initial_wtime = GenerateKeyDataSeries(zs_thrd_state, cguid, 0, initial_objects, 0);
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

	rmeta.flags = ZS_RANGE_START_GE | ZS_RANGE_END_LT;
	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", 0);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", initial_objects);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1;

	status = ZSGetRange(zs_thrd_state, 
	                     cguid,
	                     ZS_RANGE_PRIMARY_INDEX,
	                     &cursor, 
	                     &rmeta);

	if (status != ZS_SUCCESS) {
		fprintf(fp, "ZSStartRangeQuery failed with status=%d\n", status);
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
		values = (ZS_range_data_t *)malloc(sizeof(ZS_range_data_t) * n_in_chunk);
		assert(values);

		gettimeofday(&start_tv, NULL);
		status = ZSGetNextRange(zs_thrd_state, 
		                         cursor,
		                         n_in_chunk,
		                         &n_out,
		                         values);
		if (status != ZS_SUCCESS) {
			fprintf(fp, "ZSGetNextRange failed with status=%d\n", status);
			return -1;
		}
		gettimeofday(&end_tv, NULL);

		/*if (n_out != query_cnt) {
			fprintf(fp, "ZSGetNextRange returned inconsistent result, "
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
	if (PreEnvironment() != ZS_SUCCESS) {
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
