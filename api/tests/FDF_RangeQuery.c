//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/****************************
#function : ZS_RangeQuery.c
#author   : Harihara Kadayam
#date     : May 24 2013
*****************************/

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include "zs.h"

static FILE                    *fp;
static struct ZS_state        *zs_state;
static struct ZS_thread_state *zs_thrd_state;

static char *char_array = "abcdefghijklmnopqrstuvwxyz0123456789_-.";
static char **data_arr;     
static uint32_t *datalen_arr;

static char *gen_data(uint32_t max_datalen, uint32_t *pdatalen);
static void discard_data(uint32_t n_objects);
static int verify_range_query_data(ZS_range_data_t *values, 
                                   int exp_start,
                                   int exp_end,
                                   int n_in_chunk, 
                                   int n_out);
static int is_range_query_allowed(void *data, char *key, uint32_t len);

#define  MAX_KEYLEN             20
#define  MAX_DATALEN           500

typedef struct eq_class_info_s {
	uint32_t cur_class;
	uint32_t max_keys_per_class;
} eq_class_info_t;

ZS_status_t PreEnvironment()
{
	ZS_status_t ret;
    
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
	p.writethru = 0;
	p.size_kb = 2*1024*1024; // 2GB Container
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


static int is_range_query_allowed(void *data, char *key, uint32_t len)
{
	eq_class_info_t *eq = (eq_class_info_t *)data;
	char fmt[100];
	uint32_t int_key;

	sprintf(fmt, "%%0%dd", len);
	sscanf(key, fmt, &int_key);

	if (int_key == 0) {
		return 0;
	}

	uint32_t cur_class = (int_key-1)/eq->max_keys_per_class + 1;
	if (eq->cur_class == cur_class) {
		/* No change in class */
		return 1;
	}

	/* Start of a new class */
	eq->cur_class = cur_class;
	return 0;
}

static ZS_range_meta_t *
get_start_end_params(uint32_t n_objects, uint32_t start, uint32_t end,
                     int start_incl, int end_incl, int flags, int lo_to_hi,
                     int *n_in, uint32_t *exp_start, uint32_t *exp_end)
{
	ZS_range_meta_t *rmeta;

	/* Initialize rmeta */
	rmeta = (ZS_range_meta_t *)malloc(sizeof(ZS_range_meta_t));
	assert(rmeta);

	*exp_start = start;
	*exp_end   = end;

	if (start == 0) {
		*exp_start = lo_to_hi ? 1 : n_objects;
		start_incl = -1; /* Don't add anything to start flags */
	} else if (start == (n_objects + 1)) {
		*exp_start = n_objects;
		start_incl = -1; /* Don't add anything to start */
	}

	if (end == 0) {
		*exp_end = lo_to_hi ? n_objects : 1;
		end_incl = -1;  /* Don't add anything to end */
	} else if (end == (n_objects + 1)) {
		*exp_end = n_objects;
		end_incl = -1;  /* Don't add anything to end */
	}


	if (start > end) {
		*n_in = *exp_start - *exp_end + 1; /* All inclusive */

		if (start_incl == 1) {
			flags |= ZS_RANGE_START_LE;
		} else if (start_incl == 0) {
			flags |= ZS_RANGE_START_LT;
			*exp_start = start - 1;
			(*n_in)--;
		}

		if (end_incl >= 1) {
			flags |= ZS_RANGE_END_GE;
		} else if (end_incl == 0) {
			flags |= ZS_RANGE_END_GT;
			*exp_end = end + 1;
			(*n_in)--;
		}
	} else {
		*n_in = *exp_end - *exp_start + 1;

		if (start_incl == 1) {
			flags |= ZS_RANGE_START_GE;
		} else if (start_incl == 0) {
			flags |= ZS_RANGE_START_GT;
			*exp_start = start + 1;
			(*n_in)--;
		}
		if (end_incl >= 1) {
			flags |= ZS_RANGE_END_LE;
		} else if (end_incl == 0) {
			flags |= ZS_RANGE_END_LT;
			*exp_end = end - 1;
			(*n_in)--;
		}
	}

	rmeta->flags = flags;

	if ((start == 0) || (start == (n_objects+1))) {
		rmeta->key_start = NULL;
		rmeta->keylen_start = 0;
	} else {
		rmeta->key_start = (char *)malloc(MAX_KEYLEN);
		assert(rmeta->key_start);
		sprintf(rmeta->key_start, "%08d", start);
		rmeta->keylen_start = strlen(rmeta->key_start) + 1;
	}

	if ((end == 0) || (end == (n_objects+1))) {
		rmeta->key_end = NULL;
		rmeta->keylen_end = 0;
	} else {
		rmeta->key_end = (char *)malloc(MAX_KEYLEN);
		assert(rmeta->key_end);
		sprintf(rmeta->key_end, "%08d", end);
		rmeta->keylen_end = strlen(rmeta->key_end) + 1;
	}

	return (rmeta);
}

/* Does a range query for start to end keys which will get encoded to string. 
 * The query will be split into "chunks" of equal chunk */
ZS_status_t RangeQuery(ZS_cguid_t cguid, 
                        ZS_range_enums_t flags,
                        uint32_t n_objects,
                        uint32_t start, int start_incl, 
                        uint32_t end, int end_incl, 
                        int lo_to_hi, int chunks,
                        int keys_per_class)
{
	int n_in, n_in_chunk, n_in_max;
	int n_out;
	uint32_t exp_start;
	uint32_t exp_end;
	ZS_range_meta_t  *rmeta;
	ZS_range_data_t *values;
	struct ZS_cursor *cursor;       // opaque cursor handle
	ZS_status_t ret;
	int i;
	int failures = 0;
	int success = 0;
	eq_class_info_t eq;
	uint32_t exp_keys_in_class;

	rmeta = get_start_end_params(n_objects, start, end, start_incl,
	                             end_incl, flags, lo_to_hi, 
	                             &n_in, &exp_start, &exp_end);

	if (keys_per_class != -1) {
		eq.cur_class = 0;
		eq.max_keys_per_class = 10;

		rmeta->class_cmp_fn = NULL;
		rmeta->allowed_fn = is_range_query_allowed;
		rmeta->cb_data = &eq;
	} else {
		rmeta->class_cmp_fn = NULL;
		rmeta->allowed_fn = NULL;
		rmeta->cb_data = NULL;
	}

	fprintf(fp, "RangeQuery params: (%d-%d) split into %d chunks:\n",
	            start, end, chunks);

	ret = ZSGetRange(zs_thrd_state, 
	                  cguid,
	                  ZS_RANGE_PRIMARY_INDEX,
	                  &cursor, 
	                  rmeta);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ZSStartRangeQuery failed with status=%d\n", ret);
		return ret;
	}

	/* Following 3 blocks of code is to test cases where things are freed 
	 * after start range is clalled */
	if (rmeta->key_start) {
		memset(rmeta->key_start, 0, MAX_KEYLEN);
		free(rmeta->key_start);
	}

	if (rmeta->key_end) {
		memset(rmeta->key_end, 0, MAX_KEYLEN);
		free(rmeta->key_end);
	}

	memset(rmeta, 0, sizeof(ZS_range_meta_t)); 
	free(rmeta);

	/* Divide into near equal chunks */
	n_in_max = n_in / chunks + 1;
	fprintf(fp, "RangeQuery (%d-%d) n_in=%d split into %d chunks, each "
	            "chunk expecting %d objects\n",
	            exp_start, exp_end, n_in, chunks, n_in_max);

	while (n_in > 0) {
		n_in_chunk = n_in < n_in_max? n_in: n_in_max;

		/* Allocate for sufficient values */
		values = (ZS_range_data_t *)
		           malloc(sizeof(ZS_range_data_t) * n_in_chunk);
		assert(values);

		/* Do the query now */
		ret = ZSGetNextRange(zs_thrd_state, 
		                      cursor,
		                      n_in_chunk,
		                      &n_out,
		                      values);

		if ((ret == ZS_SUCCESS) || (ret == ZS_QUERY_PAUSED)) {
			if (ret == ZS_QUERY_PAUSED) {
				if (values[n_out-1].status != ZS_RANGE_PAUSED) {
					fprintf(fp, "ERROR: Expected last (%d) status as "
					            "%d (ZS_RANGE_PAUSED), instead it is %d",
					            n_out-1, ZS_RANGE_PAUSED, 
					            values[n_out-1].status); 
					failures++;
					break;
				}

				/* This is just a switch to new class. No key/val to verify */
				if (n_out == 1)
					continue;

				n_out--;

				if (exp_start < exp_end) {
					exp_keys_in_class = (eq.max_keys_per_class - (exp_start % eq.max_keys_per_class) + 1);
				} else {
					exp_keys_in_class = ((exp_start-1) % eq.max_keys_per_class) + 1;
				}
//				fprintf(fp, "exp_start=%d exp_end=%d exp_keys_in_class=%d n_in_chunk=%d, n_out=%d\n", exp_start, exp_end, exp_keys_in_class, n_in_chunk, n_out);
				if (exp_keys_in_class < n_in_chunk)
					n_in_chunk = exp_keys_in_class;
			}

			if (verify_range_query_data(values,
			                            exp_start,
			                            exp_end,
			                            n_in_chunk,
			                            n_out) != 0) {
				fprintf(fp, "ERROR: VerifyRangeQuery failed "
				        "for start key %d, n_in=%d n_out=%d\n",
				        exp_start, n_in_chunk, n_out);
				failures++;
				break;
			}

/*			if (chunks <= 10) {
				fprintf(fp, "VerifyRangeQuery for a chunk start key "
					        "%d success\n", exp_start);
			} */
			success++;
		} else if (ret != ZS_QUERY_DONE) {
			fprintf(fp, "ERROR: ZSGetNextRange failed with "
			            "error %d\n", ret);
			failures++;
		}

		n_in -= n_in_chunk;
		exp_start = (start > end) ? exp_start - n_in_chunk : 
		                            exp_start + n_in_chunk;

		if (ret == ZS_QUERY_DONE) {
			if (n_in == 0) {
				fprintf(fp, "ZS Reports QueryDone\n");
			} else {
				fprintf(fp, "ERROR: ZS Reports QueryDone, "
				   "prematurely.%d yet to be returned\n", n_in);
			}
		}

		for (i = 0; i < n_out; i++) {
			/* ZS_WARNING and BUFFER_TOO_SMALL should be checked here */
			if(!(flags & ZS_RANGE_INPLACE_POINTERS)) {
				free(values[i].key);
				free(values[i].data);
			}
		}
		free(values);
	}

	ret = ZSGetRangeFinish(zs_thrd_state, cursor);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ERROR: ZSGetRangeFinish failed ret=%d\n", ret);
		failures++;
	}

	if (failures) {
		fprintf(fp, "ERROR: RangeQuery completed with %d failures and %d success\n", failures, success);
		return (ZS_FAILURE);
	} else {
		fprintf(fp, "SUCCESS: RangeQuery completed successfully in %d chunks\n", success);
		return (ZS_SUCCESS);
	}
}

ZS_status_t ReadSeqno(ZS_cguid_t cguid, uint32_t key_no, uint64_t *seq_no)
{
	ZS_range_meta_t  rmeta;
	ZS_range_data_t *values;
	struct ZS_cursor *cursor;       // opaque cursor handle
	ZS_status_t ret;
	int n_in = 1;
	int n_out;

	rmeta.flags = ZS_RANGE_START_GE | ZS_RANGE_END_LE;

	rmeta.key_start = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_start);
	sprintf(rmeta.key_start, "%08d", key_no);
	rmeta.keylen_start = strlen(rmeta.key_start) + 1;

	rmeta.key_end = (char *)malloc(MAX_KEYLEN);
	assert(rmeta.key_end);
	sprintf(rmeta.key_end, "%08d", key_no);
	rmeta.keylen_end = strlen(rmeta.key_end) + 1;

	ret = ZSGetRange(zs_thrd_state, 
	                  cguid,
	                  ZS_RANGE_PRIMARY_INDEX,
	                  &cursor, 
	                  &rmeta);
	if (ret != ZS_SUCCESS) {
		fprintf(fp, "ZSStartRangeQuery failed with status=%d\n", ret);
		return ret;
	}

	/* Allocate for sufficient values */
	values = (ZS_range_data_t *) malloc(sizeof(ZS_range_data_t) * n_in);
	assert(values);

	/* Do the query now */
	ret = ZSGetNextRange(zs_thrd_state, 
	                      cursor,
	                      n_in,
	                      &n_out,
	                      values);

	if ((ret == ZS_SUCCESS) && 
	    (n_out == n_in) &&
	    (values[0].status == ZS_RANGE_DATA_SUCCESS)) {
		*seq_no = values[0].seqno;
	} else {
		ret = ZS_FAILURE;
	}

	ZSGetRangeFinish(zs_thrd_state, cursor);

	return (ret);
}

ZS_status_t GenerateSerialKeyData(ZS_cguid_t cguid, uint32_t n_objects, uint32_t flags)
{
	ZS_status_t ret = ZS_SUCCESS;
	char *keytmp;
	int i;

	keytmp = malloc(MAX_KEYLEN);
	assert(keytmp);

	data_arr    = (char **)malloc(sizeof(char *) * (n_objects + 1)); 
	datalen_arr = (uint32_t *)malloc(sizeof(uint32_t) * (n_objects + 1)); 

	for (i = 1; i <= n_objects; i++) {

		/* generate serial key and data */
		(void) sprintf(keytmp, "%08d", i);
		data_arr[i] = gen_data(MAX_DATALEN, &datalen_arr[i]);

		ret = ZSWriteObject(zs_thrd_state, cguid, 
		                     keytmp, strlen(keytmp) + 1, 
		                     data_arr[i], datalen_arr[i], flags);
		if (ret != ZS_SUCCESS) {
			fprintf(fp, "WriteObject failed with status=%s\n", 
			            ZSStrError(ret));
			return ret;
		}
	}

	return (ret);
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
	for (i = 1; i <= n_objects; i++) {
		free(data_arr[i]);
	}
	free(datalen_arr);
}

static int verify_range_query_data(ZS_range_data_t *values, 
                                   int exp_start,
                                   int exp_end,
                                   int n_in_chunk, 
                                   int n_out)
{
	int i;
	char keytmp[MAX_KEYLEN];

	if (n_in_chunk != n_out) {
		fprintf(fp, "Error: Input count (%d) and Output count(%d) "
		            "does not match\n", n_in_chunk, n_out);
		return -1;
	}

	for (i = 0; i < n_out; i++) {
		if (values[i].status != ZS_RANGE_DATA_SUCCESS) {
			fprintf(fp, "Error: Values[%d] status=%d\n", 
			            i, values[i].status);
			return -1;
		} 

		(void) sprintf(keytmp, "%08d", exp_start);
		if (strcmp(keytmp, values[i].key) != 0) {
			fprintf(fp, "Error: Key mismatch Expected Key %s, "
			            "Read Key %s for index(%d)\n", 
			            keytmp, values[i].key, i);
			return -1;
		}

		if (datalen_arr[exp_start] != values[i].datalen) {
			fprintf(fp, "Error: Datalen mismatch for key '%s', "
			            "Expected len %d Actual len %d\n", keytmp,
			       datalen_arr[exp_start], (int)values[i].datalen);
			return -1;
		}

		if (strncmp(data_arr[exp_start],
		            values[i].data, 
		            values[i].datalen) != 0) {
			fprintf(fp, "Error: Data mismatch for key %s, Expected "
			            "data: '%s' Read data '%s' for index(%d)\n", keytmp, 
			            data_arr[exp_start], values[i].data, i);
			return -1;
		}

		(exp_start > exp_end) ? exp_start-- : exp_start++;
	}

	return 0;
}

static void do_range_query_in_chunks(ZS_cguid_t cguid, 
                                     ZS_range_enums_t flags,
                                     uint32_t n_objects,
                                     uint32_t start, int start_incl, 
                                     uint32_t end, int end_incl, 
                                     int lo_to_hi, int test_id)
{
	int max_chunks = 10;

	fprintf(fp, "-----------Test:%d.1: No_of_chunks=1, 1 class--------------\n",test_id);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, 1, -1);

	fprintf(fp, "-----------Test:%d.2: No_of_chunks=%u, 1 class --------------\n",
	            test_id, n_objects);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, n_objects, -1);

	int rand_chunks = (random() % (max_chunks - 1)) + 1;
	fprintf(fp, "-----------Test:%d.3: No_of_chunks=%u, 1 class--------------\n",
	            test_id, rand_chunks);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, rand_chunks, -1);

	fprintf(fp, "-----------Test:%d.4: No_of_chunks=1, multi-class with keys_per_class=%d--------------\n",test_id, 10);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, 1, 10);

	fprintf(fp, "-----------Test:%d.5: No_of_chunks=%u, multi-class with keys_per_class=%d--------------\n",
	            test_id, n_objects, 10);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, n_objects, 10);

	fprintf(fp, "-----------Test:%d.6: No_of_chunks=%u, multi-class with keys_per_class=%d--------------\n",
	            test_id, rand_chunks, 10);
	RangeQuery(cguid, flags, n_objects, start, start_incl, end, end_incl, lo_to_hi, rand_chunks, 10);
}

#define MAX  n_objects+1

/***************** test ******************/
int test_basic_check(uint32_t n_objects)
{
	ZS_range_enums_t flags = 0;
//	int n_test_iter = 10;
//	int i;
//	int start, end, n_chunks;
	int test_id = 1;
	int rand_n1;
	int rand_n2;

	ZS_status_t status;
	ZS_cguid_t cguid;

	fprintf(fp, "test_basic_check starting\n");
	status = OpenContainer("rcheck1", &cguid);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	status = GenerateSerialKeyData(cguid, n_objects, 0);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. Writing serial key "
		        "and data has failed. ret=%d\n", status);
		return -1;
	}

/*	for (i = 1; i <= n_test_iter; i++) {
		start = random() % (n_objects+1);
		end   = random() % (n_objects+1);
		n_chunks = (random() % (max_chunks - 1)) + 1;
	} */

	flags |= ZS_RANGE_INPLACE_POINTERS;
	flags =0;

	for(int i = 0; i < 1; i++)
	{
		if(flags)
			fprintf(fp, "================ Inplace range queries ===================\n");
		else
			fprintf(fp, "================ Fast range queries ===================\n");

		test_id = 1;

	fprintf(fp, "================ Ascending Order Tests ===================\n");
	fprintf(fp, "\n######### Test %d: Start and End NULL #########\n\n", test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, MAX, 0, 1, test_id++);

	/* start is higher than 1 - edge condition */
	fprintf(fp, "\n######### Test %d: Start >1 and end NULL #########\n\n", test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 1, 0, MAX, 0, 1, test_id++);

	fprintf(fp, "\n######### Test %d: Start >=1 and end NULL #########\n\n", test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 1, 1, MAX, 0, 1, test_id++);

	/* start higher than random number */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start > %d(rand) and end NULL #########\n\n",
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 0, MAX, 0, 1, test_id++);

	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start >= %d(rand) and end NULL #########\n\n",
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, MAX, 0, 1, test_id++);

	/* start higher than last but one number */
	fprintf(fp, "\n######### Test %d: Start >%d and end NULL #########\n\n", 
	         test_id, n_objects-1);
	do_range_query_in_chunks(cguid, flags, n_objects, n_objects-1, 0, MAX, 0, 1, test_id++);

	fprintf(fp, "\n######### Test %d: Start >=%d and end NULL #########\n\n", 
	         test_id, n_objects-1);
	do_range_query_in_chunks(cguid, flags, n_objects, n_objects-1, 1, MAX, 0, 1, test_id++);

	/* start higher than random number and end less than bigger random number */
	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start > %d(rand) and end < %d(rand)#########\n", 
	         test_id, rand_n1, rand_n2);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 0, rand_n2, 0, 1, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start >= %d(rand) and end < %d(rand)#########\n", 
	         test_id, rand_n1, rand_n2);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, rand_n2, 0, 1, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start > %d(rand) and end <= %d(rand)#########\n", 
	         test_id, rand_n1, rand_n2);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 0, rand_n2, 1, 1, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start >= %d(rand) and end <= %d(rand)#########\n", 
	         test_id, rand_n1, rand_n2);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, rand_n2, 1, 1, test_id++);

	/* start and end <= & >= on same number */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start >= %d(rand) and end <= %d(rand)#########\n", 
	         test_id, rand_n1, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, rand_n1, 1, 1, test_id++);

	/* Range: start NULL till end last but one */
	fprintf(fp, "\n######### Test %d: Start = NULL and end < (last-1) #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, n_objects-1, 0, 1, test_id++);

	fprintf(fp, "\n######### Test %d: Start = NULL and end <= (last-1) #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, n_objects-1, 1, 1, test_id++);

	/* Range: start NULL till end random number */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start = NULL and end < %d(rand)#########\n", 
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, rand_n1, 0, 1, test_id++);

	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start = NULL and end <= %d(rand)#########\n", 
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, rand_n1, 1, 1, test_id++);

	/* Range: start NULL till end second number */
	fprintf(fp, "\n######### Test %d: Start = NULL and end < 2 #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, 2, 0, 1, test_id++);

	fprintf(fp, "\n######### Test %d: Start = NULL and end <= 2 #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, 2, 1, 1, test_id++);

	flags = 0;
	}

	/**************** Descending Order ********************/
	fprintf(fp, "================ Descending Order Tests ===================\n");

	/* range last-1 till NULL - edge condition */
	fprintf(fp, "\n######### Test %d: Start <(last-1) and end NULL #########\n\n", test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, n_objects-1, 0, 0, 0, 0, test_id++);

	fprintf(fp, "\n######### Test %d: Start <=(last-1) and end NULL #########\n\n", test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, n_objects-1, 1, 0, 0, 0, test_id++);

	/* start lesser than random number */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start < %d(rand) and end NULL #########\n\n",
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 0, 0, 0, 0, test_id++);

	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start <= %d(rand) and end NULL #########\n\n",
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, 0, 0, 0, test_id++);

	/* start lesser than second number */
	fprintf(fp, "\n######### Test %d: Start <2 and end NULL #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 2, 0, 0, 0, 0, test_id++);

	fprintf(fp, "\n######### Test %d: Start <=%d and end NULL #########\n\n", 
	         test_id, n_objects-1);
	do_range_query_in_chunks(cguid, flags, n_objects, 2, 1, 0, 0, 0, test_id++);

	/* start lesser than random number and end greater than lower random number */
	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start < %d(rand) and end > %d(rand)#########\n", 
	         test_id, rand_n2, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n2, 0, rand_n1, 0, 0, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start <= %d(rand) and end > %d(rand)#########\n", 
	         test_id, rand_n2, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n2, 1, rand_n1, 0, 0, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start < %d(rand) and end >= %d(rand)#########\n", 
	         test_id, rand_n2, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n2, 0, rand_n1, 1, 0, test_id++);

	rand_n1 = random() % n_objects/2;
	rand_n2 = rand_n1 + random() % n_objects/2;
	fprintf(fp, "\n######### Test %d: Start <= %d(rand) and end >= %d(rand)#########\n", 
	         test_id, rand_n2, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n2, 1, rand_n1, 1, 0, test_id++);

	/* start and end <= & >= on same number */
	/* TODO: This does not check accurately with ZSGetRange. Find a way */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start <= %d(rand) and end >= %d(rand)#########\n\n", 
	         test_id, rand_n1, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, rand_n1, 1, rand_n1, 1, 0, test_id++);

	/* Range: start NULL till end last but one */
	fprintf(fp, "\n######### Test %d: Start = NULL and end > (last-1) #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, n_objects-1, 0, 0, test_id++);

	fprintf(fp, "\n######### Test %d: Start = NULL and end >= (last-1) #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, n_objects-1, 1, 0, test_id++);

	/* Range: start NULL till end random number */
	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start = NULL and end > %d(rand)#########\n\n", 
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, rand_n1, 0, 0, test_id++);

	rand_n1 = random() % n_objects;
	fprintf(fp, "\n######### Test %d: Start = NULL and end >= %d(rand)#########\n\n", 
	         test_id, rand_n1);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, rand_n1, 1, 0, test_id++);

	/* Range: start NULL till end second number */
	fprintf(fp, "\n######### Test %d: Start = NULL and end > 2 #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, 2, 0, 0, test_id++);

	fprintf(fp, "\n######### Test %d: Start = NULL and end >= 2 #########\n\n", 
	         test_id);
	do_range_query_in_chunks(cguid, flags, n_objects, 0, 0, 2, 1, 0, test_id++);

	CloseContainer(cguid);
        (void)DeleteContainer(cguid);
	discard_data(n_objects);
	return 0;
}

int test_seqno_check(void)
{
	int n_objects = 4000;
//	ZS_range_enums_t flags = 0;
//	int n_test_iter = 10;
//	int max_chunks = 10;
//	int i;
//	int n_chunks;
//	int start, end;
	uint64_t seqno_offset;

	ZS_status_t status;
	ZS_cguid_t cguid;

	fprintf(fp, "==========================\n");
	fprintf(fp, "test_seqno_check starting\n");
	fprintf(fp, "==========================\n");
	status = OpenContainer("rcheck2", &cguid);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_seqno_check failed. OpenContainer failed "
		        "ret=%d\n", status);
		return -1;
	}

	status = GenerateSerialKeyData(cguid, n_objects, 0);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. Writing serial key "
		        "and data has failed 1st time. ret=%d\n", status);
		return -1;
	}

	/* Get the first range seqno */
	status = ReadSeqno(cguid, 0, &seqno_offset);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. Unable to read"
		        "seqno for first elemenet. ret=%d\n", status);
		return -1;
	}
	fprintf(fp, "Sequence no of first item (offset) = %"PRIu64"\n", seqno_offset);

	status = GenerateSerialKeyData(cguid, n_objects, ZS_WRITE_MUST_EXIST);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. Writing serial key "
		        "and data has failed 2nd time. ret=%d\n", status);
		return -1;
	}

	status = ReadSeqno(cguid, 5, &seqno_offset);
	if (status != ZS_SUCCESS) {
		fprintf(fp, "test_basic_check failed. Unable to read"
		        "seqno for first elemenet. ret=%d\n", status);
		return -1;
	}
	fprintf(fp, "Sequence no of first item (offset) = %"PRIu64"\n", seqno_offset);

/*	for (i = 1; i <= n_test_iter; i++) {
		first_half  = random() % n_objects;
		second_half = n_objects + random() % n_objects;
		n_chunks = (random() % (max_chunks - 1)) + 1;

		status = RangeQuerySeqno(cguid, flags,
		                         first_half, second_half,
		                         n_chunks);

		if (status != ZS_SUCCESS) {
			fprintf(fp, "test_basic_check failed. Range Query "
			            "failed with ret=%d\n", status);
			return -1;
		}
	} */

	CloseContainer(cguid);
        (void)DeleteContainer(cguid);
	discard_data(n_objects);
	return 0;
}

/****** main function ******/

int main(int argc, char *argv[])
{
	int ret;
/*	if((fp = fopen("ZS_RangeQuery.log", "w+"))== 0) {
		fprintf(stderr, "Open ZS_RangeQuery.log failed!\n");
		return (1);
	}   */
	fp = stderr;

	if (PreEnvironment() != ZS_SUCCESS) {
		fprintf(fp, "ERROR: PreEnvironment failed\n");
		return (1);
	}

	ret = test_basic_check(50000);
//	ret = test_seqno_check();

	fclose(fp);
	ClearEnvironment();

	fprintf(stderr, "Test Result:\n");
	if (ret == 0) {
		fprintf(stderr, "test_basic_check range query passed\n");
	} else {
		fprintf(stderr, "test_basic_check range query failed\n");
	}

	return (0);
}

#if 0
fprintf(stderr,"count is %d\n", count);
    if(3*2 == count)
    {
        fprintf(stderr, "#Test of ZSWriteObject pass!\n");
	fprintf(stderr, "#The related test script is ZS_WriteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_WriteObject.log\n");
    }else{
        fprintf(stderr, "#Test of ZSWriteObject fail!\n");
	fprintf(stderr, "#The related test script is ZS_WriteObject.c\n");
	fprintf(stderr, "#If you want, you can check test details in ZS_WriteObject.log\n");
    }
    return (!(3*2 == count));
#endif
