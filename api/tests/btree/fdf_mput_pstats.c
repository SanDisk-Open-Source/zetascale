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


#define ZS_MAX_KEY_LEN 1979
#define NUM_OBJS 10000 //max mput in single operation
#define NUM_MPUTS 1000000 

//static int cur_thd_id = 0;
static __thread int my_thdid = 0;
//ZS_cguid_t cguid;
struct ZS_state *zs_state;
int num_mputs =  NUM_MPUTS;
int num_objs = NUM_OBJS;
int obj_data_len = 0;
int use_mput = 1;
uint32_t flags_global = 0;
int num_thds = 1;

int cnt_id = 0;

#if 0
static int 
my_cmp_cb(void *data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
    int x;
    int cmp_len;

    cmp_len = keylen1 < keylen2 ? keylen1: keylen2;

    x = memcmp(key1, key2, cmp_len);
    if (x != 0) {
        return x;
    }

    /* Equal so far, use len to decide */
    if (keylen1 < keylen2) {
        return -1;
    } else if (keylen1 > keylen2) {
        return 1;
    } else {
        return 0;
    }
}
#endif 

inline uint64_t
get_time_usecs(void)
{
        struct timeval tv = { 0, 0};
        gettimeofday(&tv, NULL);
        return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}


void
do_mput(struct ZS_thread_state *thd_state, ZS_cguid_t cguid,
	uint32_t flags, int key_seed)
{
	int i, j, k;
	ZS_status_t status;
	ZS_obj_t *objs = NULL; 
	uint64_t start_time;
	uint64_t num_zs_writes = 0;
	uint64_t num_zs_reads = 0;
	uint64_t num_zs_mputs = 0;
	uint32_t objs_written = 0;
	char *data;
	uint64_t data_len;
	uint64_t key_num = 0;
	uint64_t mismatch = 0;

	objs = (ZS_obj_t *) malloc(sizeof(ZS_obj_t) * num_objs);
	if (objs == NULL) {
		printf("Cannot allocate memory.\n");
		exit(0);
	}
	memset(objs, 0, sizeof(ZS_obj_t) * num_objs);
	for (i = 0; i < num_objs; i++) {
		objs[i].key = malloc(ZS_MAX_KEY_LEN);
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

	//printf("Doing Mput in threads %d.\n", my_thdid);
	start_time = get_time_usecs();
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, ZS_MAX_KEY_LEN);
			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);

			sprintf(objs[i].data, "key_%d_%06"PRId64"_%x", my_thdid, key_num, flags);
			objs[i].data_len = strlen(objs[i].data) + 1;
		
			if (flags == ZS_WRITE_MUST_EXIST) { 
				strncat(&objs[i].data[objs[i].data_len - 1], objs[i].data, objs[i].data_len - 1);
			}
			objs[i].data_len = strlen(objs[i].data) + 1;



			key_num += key_seed;
			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].flags = 0;
			if (!use_mput) {
				status = ZSWriteObject(thd_state, cguid,
						        objs[i].key, objs[i].key_len,
							objs[i].data, objs[i].data_len, flags);
				if (status != ZS_SUCCESS) {
					printf("Write failed with %d errror.\n", status);
					assert(0);
				}
				num_zs_writes++;

				status = ZSReadObject(thd_state, cguid,
						       objs[i].key, objs[i].key_len,
						       &data, &data_len);
				if (status != ZS_SUCCESS) {
					printf("Rread failed after write.\n");
					assert(0);
				}

				assert(objs[i].data_len == data_len);

				assert(memcmp(objs[i].data, data, data_len) == 0);
				ZSFreeBuffer(data);
			}
		}

		if (use_mput) {
			status = ZSMPut(thd_state, cguid, num_objs,
					 &objs[0], flags, &objs_written);
			if (status != ZS_SUCCESS) {
				printf("Failed to write objects using ZSMPut, status = %d.\n",
					status);
				assert(0);
				return ;
			}
			num_zs_mputs++;
		}


	}



	num_zs_reads = 0;
	
	j = 0;
	printf("Reading all objects put in thread = %d.\n", my_thdid);
	key_num = 0;
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, ZS_MAX_KEY_LEN);

			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "key_%d_%06"PRId64"_%x", my_thdid, key_num, flags);
			objs[i].data_len = strlen(objs[i].data) + 1;
		
			if (flags == ZS_WRITE_MUST_EXIST) { 
				strncat(&objs[i].data[objs[i].data_len - 1], objs[i].data, objs[i].data_len - 1);
			}
			objs[i].data_len = strlen(objs[i].data) + 1;

			key_num += key_seed;

			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].data_len = strlen(objs[i].data) + 1;
			objs[i].flags = 0;

			status = ZSReadObject(thd_state, cguid,
					       objs[i].key, objs[i].key_len,
						&data, &data_len);
			if (status != ZS_SUCCESS) {
					printf("Read failed with %d errror. Key=%s.\n", status, objs[i].key);
					assert(0);
					exit(0);
			}

			if (data_len != objs[i].data_len) {
				printf("Object length of read object mismatch. Key=%s\n", objs[i].key);
				assert(0);
				mismatch++;
			}

			if (memcmp(data, objs[i].data, objs[i].data_len) != 0) {
				printf("Object data of read object mismatch.\n");	
				assert(0);
				mismatch++;
			}
			num_zs_reads++;
			ZSFreeBuffer(data);
		}
	}

	printf("Verified the mput objects using reads, mismatch = %"PRId64".\n", mismatch);



#if 0
	key_num = 0;
	for (k = 1; k <= num_mputs; k++) {

		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, ZS_MAX_KEY_LEN);

			sprintf(objs[i].key, "key_%d_%06"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "key_%d_%06"PRId64"", my_thdid, key_num);

			key_num += key_seed;

			objs[i].key_len = strlen(objs[i].key) + 1;

			status = ZSDeleteObject(thd_state, cguid,
					       objs[i].key, objs[i].key_len);
			if (status != ZS_SUCCESS) {
				printf("Delete failed with %d errror.\n", status);
				assert(0);
				exit(0);
			}

		}

	}

	printf("Deleted objects successfully for thread = %d.\n", my_thdid);
#endif

    struct ZS_iterator *iterator;
    uint64_t datalen;
    char	*key;
    data = NULL;
    int count = 0;
    uint32_t keylen;

    status =  ZSEnumerateContainerObjects(thd_state, cguid, &iterator);

    while (ZSNextEnumeratedObject(thd_state, iterator, &key, &keylen, &data, &datalen) == ZS_SUCCESS) {
        ZSFreeBuffer(data);
        ZSFreeBuffer(key);
        count++;
    }
    ZSFinishEnumeration(thd_state, iterator);

    ZS_stats_t stats;
    status = ZSGetContainerStats(thd_state, cguid, &stats);
    printf("Enum count=%d Pstats count=%ld size = %ld\n",
            count, stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS], stats.cntr_stats[ZS_CNTR_STATS_USED_SPACE]);
    assert(count == stats.cntr_stats[ZS_CNTR_STATS_NUM_OBJS]);
}

void *
write_stress(void *t)
{
	ZS_container_props_t props;
	struct ZS_thread_state *thd_state;
	ZS_status_t status;
        ZS_cguid_t cguid;

	char cnt_name[100] = {0};

	sprintf(cnt_name, "cntr_%d", __sync_fetch_and_add(&cnt_id, 1));

	ZSInitPerThreadState(zs_state, &thd_state);	

	ZSLoadCntrPropDefaults(&props);

	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 1;
	props.fifo_mode = 0;
	props.size_kb = (1024 * 1024 * 10);;

	status = ZSOpenContainer(thd_state, cnt_name, &props, ZS_CTNR_CREATE, &cguid);
	if (status != ZS_SUCCESS) {
		printf("Open Cont failed with error=%x.\n", status);
		exit(-1);
	}

	do_mput(thd_state, cguid, flags_global, 1);

	ZSCloseContainer(thd_state, cguid);
	ZSDeleteContainer(thd_state, cguid);

	ZSReleasePerThreadState(&thd_state);

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
	flags_global = flags_in;
	launch_thds(); //actual operations
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

	ZSInit(&zs_state);

	printf(" ======================== Doing test for set case. ===================\n");
	do_op(0);// set
	printf(" ******************  Done test for set case.***********************\n");

	printf(" ======================== Doing test for create case. ===================\n");
	do_op(ZS_WRITE_MUST_NOT_EXIST); //create
	printf(" ******************  Done test for create  case.***********************\n");

#if 0
	printf(" ======================== Doing test for update case. ===================\n");
	do_op(ZS_WRITE_MUST_EXIST); //update
	printf(" ******************  Done test for update  case.***********************\n");
#endif

	ZSShutdown(zs_state);
	return 0;
}
