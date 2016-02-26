/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <zs.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>


#define ZS_MAX_KEY_LEN 256
#define NUM_OBJS 10000 //max mput in single operation
#define NUM_MPUTS 1000000 

static int cur_thd_id = 0;
static __thread int my_thdid = 0;
ZS_cguid_t cguid;
struct ZS_state *zs_state;
int num_mputs =  NUM_MPUTS;
int num_objs = NUM_OBJS;
int use_mput = 1;
int test_run = 1;
uint64_t total_ops = 0;
uint32_t flags = 0;


inline uint64_t
get_time_usecs(void)
{
        struct timeval tv = { 0, 0};
        gettimeofday(&tv, NULL);
        return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}


void
do_mput(struct ZS_thread_state *thd_state, ZS_cguid_t cguid)
{
	int i, k;
	ZS_status_t status;
	ZS_obj_t *objs = NULL; 
	uint64_t num_zs_writes = 0;
	uint64_t num_zs_mputs = 0;
	uint64_t key_num = 0;
	uint32_t objs_written = 0;

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
    k = 0;
    while (test_run) {
		for (i = 0; i < num_objs; i++) {
			memset(objs[i].key, 0, ZS_MAX_KEY_LEN);
			sprintf(objs[i].key, "key_%d_%08"PRId64"", my_thdid, key_num);
			sprintf(objs[i].data, "data_%d_%08"PRId64"", my_thdid, key_num);
			key_num++;

			objs[i].key_len = strlen(objs[i].key) + 1;
			objs[i].data_len = strlen(objs[i].data) + 1;
			objs[i].flags = 0;
			if (!use_mput) {
				status = ZSWriteObject(thd_state, cguid,
						        objs[i].key, objs[i].key_len,
							objs[i].data, objs[i].data_len, 0);
				if (status != ZS_SUCCESS) {
					printf("Write failed with %d errror.\n", status);
					exit(0);
				}
				num_zs_writes++;
			}
		}

        k += num_objs;

		if (use_mput) {
			status = ZSMPut(thd_state, cguid, num_objs, &objs[0], flags, &objs_written);
			if (status != ZS_SUCCESS) {
				printf("Failed to write objects using ZSMPut, status = %d.\n",
					status);
				return ;
			}
			num_zs_mputs++;
		}

	}

    __sync_fetch_and_add(&total_ops, k);

}

void *
write_stress(void *t)
{
	struct ZS_thread_state *thd_state;


	my_thdid = __sync_fetch_and_add(&cur_thd_id, 1);
	ZSInitPerThreadState(zs_state, &thd_state);	

	do_mput(thd_state, cguid);

	return NULL;
}

int 
main(int argc, char *argv[])
{
	ZS_container_props_t props;
	struct ZS_thread_state *thd_state;
	ZS_status_t	status;
	int num_thds = 1;
	pthread_t thread_id[128];
	int n, m;
	int i;
    int time = 0;

	if (argc < 5) {
		printf("Usage: ./run 0/1(use mput)  time(secs) num_objs_each_mput num_threads.\n");
		exit(0);
	}

	use_mput = atoi(argv[1]);
	m = atoi(argv[2]);
	if (m > 0) {
		time = m;	
	}
	n = atoi(argv[3]);
	if (n > 0) {
		num_objs = n;
	}

    n = atoi(argv[4]);
    if (n > 0) {
            num_thds = n;
    }

	printf("Running with mput (y/n) = %d, time = %dsecs, num objs each mput = %d, threads = %d.\n",use_mput, time, num_objs, num_thds);


	ZSInit(&zs_state);
	ZSInitPerThreadState(zs_state, &thd_state);	

	ZSLoadCntrPropDefaults(&props);

	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	
	status = ZSOpenContainer(thd_state, "cntr", &props, ZS_CTNR_CREATE, &cguid);
	if (status != ZS_SUCCESS) {
		printf("Open Cont failed with error=%x.\n", status);
		return -1;	
	}

	sleep(10);
	for(i = 0; i < num_thds; i++) {
		fprintf(stderr,"Creating thread %i\n",i );
		if( pthread_create(&thread_id[i], NULL, write_stress, NULL)!= 0 ) {
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

	printf("Total ops = %"PRIu64", in %d secs.\n", total_ops, time);

	ZSCloseContainer(thd_state, cguid);
	ZSReleasePerThreadState(&thd_state);

	ZSShutdown(zs_state);
	return 0;
}

