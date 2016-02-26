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
#define OVERFLOW_DATA_SIZE    4096
#define INNODE_DATA_SIZE      1000

#define MAX_OBJECTS           10000
#define MAX_SNAPSHOTS         300

ZS_cguid_t cguid;
struct ZS_state *zs_state;

static char *char_array = "abcdefghijklmnopqrstuvwxyz0123456789_-.";

typedef struct vals {
	char *value;
	uint64_t vallen;
} vals_t;

vals_t values[MAX_OBJECTS];
uint64_t snap_seqnos[MAX_SNAPSHOTS];
int snapshot_cnt = 0;

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

static void
do_mput(struct ZS_thread_state *thd_state, uint32_t start, uint32_t cnt, int iter, uint32_t overflow_freq)
{
	int i;
	ZS_status_t status;
	ZS_obj_t *objs = NULL; 
	uint32_t objs_written;
	uint32_t keyind = start;

	objs = (ZS_obj_t *) malloc(sizeof(ZS_obj_t) * cnt);
	if (objs == NULL) {
		printf("Cannot allocate memory.\n");
		fflush(stdout);
		exit(0);
	}

	memset(objs, 0, sizeof(ZS_obj_t) * cnt);
	for (i = 0, keyind = start; i < cnt; i++, keyind++) {
		objs[i].key = malloc(ZS_MAX_KEY_LEN);
		if (objs[i].key == NULL) {
			printf("Cannot allocate memory.\n");
			fflush(stdout);
			exit(0);
		}

		sprintf(objs[i].key, "key_%04d", start + i);
		objs[i].key_len = strlen(objs[i].key) + 1;

		if ((overflow_freq == 0) || (i % overflow_freq)) {
			values[keyind].vallen = INNODE_DATA_SIZE;
		} else {
			values[keyind].vallen = OVERFLOW_DATA_SIZE;
		}

		gen_data(values[keyind].value, values[keyind].vallen, iter);

		objs[i].data = values[keyind].value;
		objs[i].data_len = values[keyind].vallen;
		objs[i].flags = 0;
	}

	uint32_t flags = 0;
	status = ZSMPut(thd_state, cguid, cnt,
			 &objs[0], flags, &objs_written);
	if (status != ZS_SUCCESS) {
		printf("Failed to write objects using ZSMPut, status = %d.\n",
			status);
		fflush(stdout);
		assert(0);
		return ;
	}

	for (i = 0; i < cnt; i++) {
		free(objs[i].key);
	}
}

static void
do_write(struct ZS_thread_state *thd_state, uint32_t start, uint32_t cnt, int iter, bool overflow_freq)
{
	char *key;
	uint32_t keylen;
	ZS_status_t status;
	uint32_t i;

	for (i = start; i < start + cnt; i++) {
		key = malloc(ZS_MAX_KEY_LEN);
		sprintf(key, "key_%04d", i);
		keylen = strlen(key) + 1;

		if (overflow_freq && (i % overflow_freq)) {
			values[i].vallen = OVERFLOW_DATA_SIZE;
		} else {
			values[i].vallen = INNODE_DATA_SIZE;
		}

		gen_data(values[i].value, values[i].vallen, iter);

		status = ZSWriteObject(thd_state, cguid, key, keylen,
		                        values[i].value, values[i].vallen, 0);
		if (status != ZS_SUCCESS) {
			printf("ZSWrite failed for key %d\n", i);
			fflush(stdout);
		}

		free(key);
	}
}

static void
do_read_verify(struct ZS_thread_state *thd_state, uint32_t start, uint32_t cnt,
               ZS_status_t exp_status)
{
	char *key;
	uint32_t keylen;
	char *act_value;
	uint64_t act_vallen;
	ZS_status_t status;
	uint32_t i;
	uint32_t errors = 0;

	for (i = start; i < start + cnt; i++) {
		key = malloc(ZS_MAX_KEY_LEN);
		sprintf(key, "key_%04d", i);
		keylen = strlen(key) + 1;

		status = ZSReadObject(thd_state, cguid, key, keylen,
		                       &act_value, &act_vallen);
		if (status != exp_status) {
			printf("ZSRead returned status=%s, expected status=%s for "
			        "key %d\n", ZSStrError(status), ZSStrError(exp_status), i);
			fflush(stdout);
			errors++;
			free(key);
			continue;
		}

		if (status != ZS_SUCCESS) {
			free(key);
			continue;
		}

		if (values[i].vallen != act_vallen) {
			fprintf(stdout, "Error: Mismatch: Actual datalen=%u, "
			         "Expected datalen=%u for key %u\n",
			         (uint32_t)act_vallen, (uint32_t)values[i].vallen, i);
			fflush(stdout);
			errors++;
			free(key);
			continue;
		}

		if (memcmp(values[i].value, act_value, act_vallen) != 0) {
			fprintf(stdout, "Error: Mismatch: Actual data differs "
			         "with expected data for key %u\n", i);
			fflush(stdout);
			errors++;
		}

		free(key);
	}

	if (errors == 0) {
		printf("SUCCESS: Read and Verified the data/status successfully\n");
	} else {
		printf("ERROR: Read and Verification of data/status failed\n");
	}
	fflush(stdout);
}

static void
do_delete(struct ZS_thread_state *thd_state, uint32_t start, uint32_t cnt,
          ZS_status_t exp_status)
{
	char *key;
	uint32_t keylen;
	ZS_status_t status;
	uint32_t errors = 0;
	int i;

	for (i = start; i < start + cnt; i++) {
		key = malloc(ZS_MAX_KEY_LEN);
		sprintf(key, "key_%04d", i);
		keylen = strlen(key) + 1;

		status = ZSDeleteObject(thd_state, cguid, key, keylen);
		if (status != exp_status) {
			printf("ZSDelete returned status=%s, expected status=%s for "
			        "key %d\n", ZSStrError(status), ZSStrError(exp_status), i);
			fflush(stdout);
			free(key);
			errors++;
			continue;
		}

		free(key);
	}

	if (errors == 0) {
		printf("SUCCESS: ZSDeleted keys successfully\n");
	} else {
		printf("ERROR: ZSDelete Failed\n");
	}
	fflush(stdout);
}

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
	props.size_kb = (1024 * 1024 * 10);;

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
static void
allocate_globals(uint32_t num_objs)
{
	int i;

	for (i = 0; i < num_objs; i++) {
		values[i].value = malloc(OVERFLOW_DATA_SIZE);
	}
}

static void
create_snapshot(struct ZS_thread_state *thd_state, uint32_t snap_ind)
{
	ZS_status_t status;
	status = ZSCreateContainerSnapshot(thd_state, cguid, &snap_seqnos[snap_ind]);

	if (status != ZS_SUCCESS) {
		if (status == ZS_TOO_MANY_SNAPSHOTS) {
			printf("Hitting too many snapshots\n");
			fflush(stdout);
		}
	
		printf("Error %s in creating snapshots for snap_ind=%u\n",
		        ZSStrError(status), snap_ind);
		fflush(stdout);
	} else {
		snapshot_cnt++;
	}
}

static void
delete_snapshot(struct ZS_thread_state *thd_state, uint32_t snap_ind)
{
	ZS_status_t status;
	status = ZSDeleteContainerSnapshot(thd_state, cguid, snap_seqnos[snap_ind]);

	if (status != ZS_SUCCESS) {
		printf("Error %s in deleting snapshots for snap_ind=%u\n",
		        ZSStrError(status), snap_ind);
		fflush(stdout);
	} else {
		snapshot_cnt--;
	}
}

static void
do_read_verify_snapshot(struct ZS_thread_state *thd_state, uint32_t start, uint32_t cnt, 
                        uint32_t snap_ind, int chunk_size)
{
	ZS_status_t ret;
	ZS_range_meta_t *rmeta;
	ZS_range_data_t *rvalues;
	struct ZS_cursor *cursor;       // opaque cursor handle
	int n_out;
	int errors = 0;
	char strN[10];
	int i, k, x, y;
	int overall_cnt = 0;
	int act_key;

	/* Initialize rmeta */
	rmeta = (ZS_range_meta_t *)malloc(sizeof(ZS_range_meta_t));
	rmeta->flags = 0;
	rmeta->key_start = NULL;
	rmeta->keylen_start = 0;
	rmeta->key_end   = NULL;
	rmeta->keylen_end = 0;
	rmeta->class_cmp_fn = NULL;
	rmeta->allowed_fn = NULL;
	rmeta->cb_data = NULL;

	if (snap_ind == 0) {
		rmeta->flags = ZS_RANGE_SEQNO_LE;
		rmeta->end_seq = snap_seqnos[snap_ind];
	} else if (snap_ind < snapshot_cnt) {
		rmeta->flags = ZS_RANGE_SEQNO_GT_LE;
		rmeta->start_seq = snap_seqnos[snap_ind-1];
		rmeta->end_seq   = snap_seqnos[snap_ind];
	}

	ret = ZSGetRange(thd_state,
	                  cguid,
	                  ZS_RANGE_PRIMARY_INDEX,
	                  &cursor, 
	                  rmeta);
	if (ret != ZS_SUCCESS) {
		fprintf(stdout, "ZSStartRangeQuery failed with status=%d\n", ret);
		fflush(stdout);
		return;
	}
	free(rmeta);

	do {
		rvalues = (ZS_range_data_t *)
		           malloc(sizeof(ZS_range_data_t) * chunk_size);
		assert(rvalues);

		ret = ZSGetNextRange(thd_state,
		                      cursor,
		                      chunk_size,
		                      &n_out,
		                      rvalues);

		if ((n_out != chunk_size) && (ret != ZS_QUERY_DONE)) {
			fprintf(stdout, "Error: Snapshot read chunk size "
			         "expected %d, actual %d\n", chunk_size, n_out);
			fflush(stdout);
			errors++;
		} else if ((ret != ZS_SUCCESS) && (ret != ZS_QUERY_DONE)) {
			fprintf(stdout, "Error: Snapshot read returned %d\n", ret);
			fflush(stdout);
			errors++;
			goto freeup;
		}

		for (i = 0; i < n_out; i++) {
			sscanf(rvalues[i].key, "key_%04d", &act_key);
			k = act_key - start; /* in case start is not 0 */

			if (values[k].vallen != rvalues[i].datalen) {
				fprintf(stdout, "Error: Mismatch: Expected datalen=%u, "
				         "Actual datalen=%u for key %u\n",
				         (uint32_t)values[k].vallen,
				         (uint32_t)rvalues[i].datalen, act_key);
				fflush(stdout);
				errors++;
			}

			sprintf(strN, "%d", snap_ind);
			x = 0; y = 0;
			while (x < rvalues[i].datalen - 1) {
				if (rvalues[i].data[x] != strN[y]) {
					fprintf(stdout, "Error: Mismatch: Expected data[%d] = "
					        "%c Actual data[%d] = %c for key %u\n",
					         x, strN[y], x, rvalues[i].data[x], act_key);
					fflush(stdout);
					errors++;
				}
				x++; y++;
				if (y == strlen(strN)) 
					y = 0;
			}

			overall_cnt++;
			free(rvalues[i].key);
			free(rvalues[i].data);
		}
freeup:
		free(rvalues);
	} while (ret != ZS_QUERY_DONE);

	if (overall_cnt != cnt) {
		fprintf(stdout, "ERROR: Expected snapshot obj count of objects to be %d, actual count=%d\n",
		                 cnt, overall_cnt);
		fflush(stdout);
		errors++;
	}

	ret = ZSGetRangeFinish(thd_state, cursor);
	if (ret != ZS_SUCCESS) {
		fprintf(stdout, "ERROR: ZSGetRangeFinish failed ret=%d\n", ret);
		fflush(stdout);
		errors++;
	}

	if (errors == 0) {
		fprintf(stdout, "SUCCESS: Verified the snapshot data successfully for snap_ind=%d\n", snap_ind);
	} else {
		fprintf(stdout, "ERROR: Snapshot read verify failed for snap_ind=%d\n", snap_ind);
	}
	fflush(stdout);
}

static void
do_rw_test(uint32_t num_objs, uint32_t iters, uint32_t num_snaps)
{
	struct ZS_thread_state *thd_state;
	int i;
	uint32_t iter_print_cnt = 0;

	printf("============================= Snapshot Read/Write Test =========================\n");
	fflush(stdout);
	ZSInitPerThreadState(zs_state, &thd_state);	
	open_stuff(thd_state, "cntr_1");

	snapshot_cnt = 0;

	/* Write the contents of snapshots */
	for (i = 0; i < num_snaps; i++) {
		printf("Iter: %u: %s %u objects\n", ++iter_print_cnt, 
		     (i%2) ? "Writing": "MPutting", num_objs);
		fflush(stdout);

		if ((i % 2) == 0) {
			do_mput(thd_state, 0, num_objs, i, 0);
		} else {
			do_write(thd_state, 0, num_objs, i, 0);
		}
		printf("Creating Snapshot %u\n", i);
		fflush(stdout);
		create_snapshot(thd_state, i);
	}

	/* Write the remaining iterations */
	for (;i < iters; i++) {
		printf("Iter: %u: %s %u objects\n", ++iter_print_cnt, 
		     (i%2) ? "Writing": "MPutting", num_objs);
		fflush(stdout);

		if ((i % 2) == 0) {
			do_mput(thd_state, 0, num_objs, i, 0);
		} else {
			do_write(thd_state, 0, num_objs, i, 0);
		}
	}

	do_read_verify(thd_state, 0, num_objs, ZS_SUCCESS);

	printf("Doing range query for latest update\n");
	do_read_verify_snapshot(thd_state, 0, num_objs, iters-1, 20);

	for (i = 0; i < num_snaps; i++) {
		do_read_verify_snapshot(thd_state, 0, num_objs, i, 20);
	}

	free_stuff(thd_state);
	ZSReleasePerThreadState(&thd_state);
}

static void
do_delete_test(uint32_t num_objs, uint32_t iters, uint32_t num_snaps)
{
	struct ZS_thread_state *thd_state;
	int i;
	uint32_t iter_print_cnt = 0;

	printf("============================= Snapshot object Delete Test =========================\n");
	fflush(stdout);
	ZSInitPerThreadState(zs_state, &thd_state);	
	open_stuff(thd_state, "cntr_2");

	snapshot_cnt = 0;

	/* Write the contents of snapshots */
	for (i = 0; i < num_snaps; i++) {
		printf("Iter: %u: Mputting %u objects\n", ++iter_print_cnt, 
		       num_objs);
		fflush(stdout);

		do_mput(thd_state, 0, num_objs, i, 0);

		printf("Creating Snapshot %u\n", i);
		fflush(stdout);
		create_snapshot(thd_state, i);
	}

#if 0
#include <string.h>
	printf("Waiting to get confirmation to proceed\n");
	getchar();
#endif

	/* After delete, validate if the latest data is reported
	 * not found and earlier snapshot still intact */
	do_delete(thd_state, 0, num_objs, ZS_SUCCESS);
	do_read_verify(thd_state, 0, num_objs, ZS_OBJECT_UNKNOWN);
	do_read_verify_snapshot(thd_state, 0, num_objs, num_snaps-1, 20);

	/* Do a range query (read/verify active snapshot) and 
	 * ensure that no objects are found */
	printf("Doing range query for latest update\n");
	do_read_verify_snapshot(thd_state, 0, 0, num_snaps, 20);

	/* Do delete on top of tombstone delete and ensure it
	 * says key not found */
	do_delete(thd_state, 0, num_objs, ZS_OBJECT_UNKNOWN);

	for (i = 0; i < num_snaps; i++) {
		printf("Deleting Snapshot %u\n", i+1);
		fflush(stdout);
		delete_snapshot(thd_state, i);
	}

	/* Do another round of insert on top of tombstoned one and
	 * delete them to see if we still see key not found */
	printf("Iter: %u: Mputting %u objects\n", ++iter_print_cnt, 
	       num_objs);
	fflush(stdout);
	do_mput(thd_state, 0, num_objs, iters++, 0);
	do_delete(thd_state, 0, num_objs, ZS_SUCCESS);
	do_read_verify(thd_state, 0, num_objs, ZS_OBJECT_UNKNOWN);
	free_stuff(thd_state);

	ZSReleasePerThreadState(&thd_state);
}

int change_data = 1;
int length_incr = 3;
int objs_processed = 0;
bool
range_update_cb(char *key, uint32_t keylen, char *data, uint32_t datalen,
		             void *callback_args, char **new_data, uint32_t *new_datalen)
{
	*new_datalen = 0;
	(*new_data) = NULL;

	objs_processed++;

	if (!change_data) {
		return false;
	}

	if (length_incr) {
		assert((datalen + length_incr) > 0);
		(*new_data) = (char *) malloc(datalen + length_incr);
		if (*new_data == NULL) {
			return false;
		}
		*new_datalen = datalen + length_incr; //Change in data size
		gen_data(*new_data, *new_datalen, 999 + objs_processed);
	} else {
		data[0] = 'O';
	}

	return true;
}

static void
do_rangeupdate_test(uint32_t num_objs, uint32_t iters, uint32_t num_snaps)
{
	struct ZS_thread_state *thd_state;
	int i = 0;
	uint32_t objs_updated = 0;
	ZS_status_t status;
	char *key;
	uint32_t keylen;
	char *act_value = NULL;
	uint64_t act_vallen;
	char *new_data = NULL;
	int error = 0;

//	uint32_t iter_print_cnt = 0;

	printf("============================= Snapshot RangeUpdate Test =========================\n");
	fflush(stdout);
	ZSInitPerThreadState(zs_state, &thd_state);	

	/* Test case 1: Let there be objects in snapshot, create new in active container and do
	   rangeupdate on all keys, should be able to read updated data for all the keys and read old data
	   for keys in snapshot */
	open_stuff(thd_state, "cntr_3");
	snapshot_cnt = 0;

	do_mput(thd_state, 0, (num_objs + (num_objs % 2))/2, 0, 0);
	create_snapshot(thd_state, i);
	do_mput(thd_state, (num_objs + (num_objs % 2))/2, (num_objs - (num_objs % 2))/2, 1, 0);
	ZSRangeUpdate(thd_state, cguid, "key_", strlen("key_"), range_update_cb, NULL, NULL, NULL, &objs_updated);
	printf("RangeUpdated objs_update: %d\n", objs_updated);
	if (objs_updated != num_objs) {
		printf("ERROR: All objects are not updated\n");
		return;
	}
	key = malloc(ZS_MAX_KEY_LEN);
	for (i = 0; i < num_objs; i++) {
		sprintf(key, "key_%04d", i);
		keylen = strlen(key) + 1;
		act_value = NULL;
		status = ZSReadObject(thd_state, cguid, key, keylen,
		                       &act_value, &act_vallen);
		if (status != ZS_SUCCESS) {
			printf("ERROR: ZSRead returned status=%s, expected status=%s for "
			        "key %d\n", ZSStrError(status), ZSStrError(ZS_SUCCESS), i);
			error++;
			fflush(stdout);
			free(key);
			break;
		}

		if (values[i].vallen + length_incr != act_vallen) {
			fprintf(stdout, "ERROR: Mismatch: Actual datalen=%u, "
			         "Expected datalen=%u for key %u\n",
			         (uint32_t)act_vallen, (uint32_t)values[i].vallen + length_incr, i);
			error++;
			fflush(stdout);
			free(act_value);
			free(key);
			break;
		}

		new_data = NULL;
		new_data = (char *) malloc(act_vallen);
		gen_data(new_data, act_vallen, 1000 + i);
		if (memcmp(new_data, act_value, act_vallen) != 0) {
			fprintf(stdout, "ERROR: Mismatch: Actual data differs "
			         "with expected data for key %u\n", i);
			error++;
			fflush(stdout);
			break;
		}

		//free(key);
		//free(new_data);
		//free(act_value);
	}
	do_read_verify_snapshot(thd_state, 0, (num_objs + (num_objs % 2))/2, 0, 20);
	free_stuff(thd_state);


	/* Test case 2: Create a subtree having a key with its snapshot data, and rangeupdate on all
	   keys, should be able to read updated data for key and data of the key of each snapshot */
	status = ZS_SUCCESS;

	open_stuff(thd_state, "cntr_3");
	do_mput(thd_state, 0, num_objs, 0, 0);
	create_snapshot(thd_state, 0);

	key = malloc(ZS_MAX_KEY_LEN);
	for (i = 0; i < num_snaps; i++) {
		sprintf(key, "key_%04d", num_objs/2);
		keylen = strlen(key) + 1;
		if (i%2 == 0) {
			act_vallen = OVERFLOW_DATA_SIZE;
		} else {
			act_vallen = INNODE_DATA_SIZE;
		}
		new_data = (char *) malloc(act_vallen);
		gen_data(new_data, act_vallen, 123 + i);
		status = ZSWriteObject(thd_state, cguid, key, keylen, new_data, act_vallen, 0);
		if (status != ZS_SUCCESS) {
			printf("WriteObject failed (%d) error: %s\n", i, ZSStrError(status));
			break;
		}
		create_snapshot(thd_state, i + 1);
		free(new_data);
	}
	objs_processed = 0;
	ZSRangeUpdate(thd_state, cguid, "key_", strlen("key_"), range_update_cb, NULL, NULL, NULL, &objs_updated);
	if (objs_updated != num_objs) {
		printf("ERROR: All objects are not updated\n");
		return;
	}
	for (i = 0; i < num_objs; i++) {
		key = malloc(ZS_MAX_KEY_LEN);
		sprintf(key, "key_%04d", i);
		keylen = strlen(key) + 1;
		act_value = NULL;
		status = ZSReadObject(thd_state, cguid, key, keylen,
		                       &act_value, &act_vallen);
		if (status != ZS_SUCCESS) {
			printf("ZSRead returned status=%s, expected status=%s for "
			        "key %d\n", ZSStrError(status), ZSStrError(ZS_SUCCESS), i);
			error++;
			fflush(stdout);
			free(key);
			break;
		}

		if (values[i].vallen + length_incr != act_vallen) {
			fprintf(stdout, "ERROR: Mismatch: Actual datalen=%u, "
			         "Expected datalen=%u for key %u\n",
			         (uint32_t)act_vallen, (uint32_t)values[i].vallen + length_incr, i);
			error++;
			fflush(stdout);
			free(act_value);
			free(key);
			break;
		}

		new_data = NULL;
		new_data = (char *) malloc(act_vallen);
		gen_data(new_data, act_vallen, 1000 + i);
		if (memcmp(new_data, act_value, act_vallen) != 0) {
			fprintf(stdout, "ERROR: Mismatch: Actual data differs "
			         "with expected data for key %u\n", i);
			error++;
			fflush(stdout);
			break;
		}

		free(key);
		free(new_data);
		free(act_value);
	}

	free_stuff(thd_state);
	ZSReleasePerThreadState(&thd_state);

}
	
void
write_data(void *arg)
{
	uint64_t iters = (uint64_t)arg;
	int i;
	struct ZS_thread_state *thd_state;

	ZSInitPerThreadState(zs_state, &thd_state);	
	for (i = 0; i < iters; i++) {
	   do_mput(thd_state, 0, objs_processed, i, 0);
	   sched_yield();
	}
	ZSReleasePerThreadState(&thd_state);
}

static void
do_quiesce_test(uint32_t num_objs, uint32_t iters, uint32_t num_snaps)
{
	struct ZS_thread_state *thd_state;
	int i = 0;
	pthread_t           thread_id;
//	uint32_t iter_print_cnt = 0;

	printf("============================= Snapshot Quiesce Test =========================\n");
	fflush(stdout);
	ZSInitPerThreadState(zs_state, &thd_state);	
	open_stuff(thd_state, "cntr_4");
	snapshot_cnt = 0;

	objs_processed = num_objs;
	pthread_create(&thread_id, NULL, (void *)write_data, (void *)(uint64_t)iters);

	for (i = 0; i < num_snaps; i++) {
		create_snapshot(thd_state, i);
		sched_yield();
	}
	pthread_join(thread_id, NULL);
	for (i = 0; i < num_snaps; i++) {
		if ((snap_seqnos[i] - snap_seqnos[i - 1]) != 1 ||
				(snap_seqnos[i] - snap_seqnos[i - 1]) != num_objs) {
			printf("IO is not quiesced when snapshot is in progress\n");
		}
	}

	free_stuff(thd_state);
	ZSReleasePerThreadState(&thd_state);

}

int 
main(int argc, char *argv[])
{
	uint32_t num_objs = 0;
	uint32_t num_snaps = 0;
	uint32_t iters = 0;

	printf("Usage: <prog> <num_objs> <num_snaps> <num_iters>\n");

	if (argc >= 2) {
		num_objs = atoi(argv[1]);
	}
	if ((num_objs == 0) || (num_objs > MAX_OBJECTS)) {
		printf("Setting num_objs to MAX_OBJECTS=%u\n", MAX_OBJECTS);
		fflush(stdout);
		num_objs = MAX_OBJECTS;
	}

	if (argc >= 3) {
		num_snaps = atoi(argv[2]);
	}

	if ((num_snaps == 0) || (num_snaps > MAX_SNAPSHOTS)) {
		printf("Setting num_snaps to %u\n", MAX_SNAPSHOTS);
		fflush(stdout);
		num_snaps = MAX_SNAPSHOTS;
	}

	if (argc >= 4) {
		iters = atoi(argv[3]);
	}
	if (iters == 0) {
		iters = num_snaps + 1;
	}

	allocate_globals(num_objs);

	printf("Starting test with %u objs %u snapshots and %u iterations\n",
	        num_objs, num_snaps, iters);
	fflush(stdout);

	ZSInit(&zs_state);

	do_rw_test(num_objs, iters, num_snaps);
	do_rangeupdate_test(num_objs, iters, num_snaps);
	do_delete_test(num_objs, iters, num_snaps);
	do_quiesce_test(num_objs, iters, num_snaps);

	ZSShutdown(zs_state);

	return 0;
}
