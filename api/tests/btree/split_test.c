/*
 * Unit test case for testing splist and mulitiple splits.
 */

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <zs.h>

#define ZS_MAX_KEY_LEN 2000

int32_t
generate_keys(int prefix_len, int seed1, int seed2, char *key_buf)
{
	int i = 0;
	/*
	 * First generate the prefix.
	 */
	for (i = 0; i < prefix_len; ) {
		snprintf(&key_buf[i], 11, "%s", "AAAAAAAAAAA");
		i += 10;	
	}

	snprintf(&key_buf[i], 6, "%s", "START");
	i += 5;

	/*
	 * Append the seed values.
	 */
	snprintf(&key_buf[i], 6, "%05d", seed1);
	i += 5;
	snprintf(&key_buf[i], 4, "%03d", seed2);

	return (strlen(key_buf) + 1);
}

ZS_cguid_t cguid;
struct ZS_state *zs_state;
struct ZS_thread_state *thd_state;

int
my_cmp_cb(void *cmp_cb_data, char *key1, uint32_t keylen1, char *key2, uint32_t keylen2)
{
	char *skey1 = NULL;
	char *skey2 = NULL;
	uint32_t skeylen1 = 0;
	uint32_t skeylen2 = 0;

	uint32_t cmp_len = 0;
	int x = 0;

	/*
	 * Ignore the prefix part of the key.
	 */

	skey1 = strstr(key1, "START");
	skeylen1 = keylen1 - (skey1 - key1);

	skey2 = strstr(key2, "START");
	skeylen2 = keylen2 - (skey2 - key2);

	cmp_len = skeylen1 > skeylen2? skeylen2: skeylen1;
	x = memcmp(skey1, skey2, cmp_len);

	if (x == 0) {
		/*
		 * Contents are same so far, bugger length wins.
		 */
		if (skeylen1 > skeylen2) {
			return 1;
		} else  if (skeylen1 < skeylen2) {
			return -1;

		} else {
			return 0;
		}

	} else {
		return x;

	}
}

char tmp_data[8000] = {0};
uint64_t datalen = 0;
#define DATA_LEN 100
void
insert_keys(int num_keys, int prefix_len, int seed1_start, int seed2_start, bool use_mput)
{
	char *data = &tmp_data[0];
//	uint64_t datalen = DATA_LEN;
	int i = 0;
	ZS_obj_t *objs = NULL; 
	int num_objs = 100;
	int num_mputs;
	int k = 0;
	char *tmp_data = NULL;
	uint64_t tmp_datalen = 0;

	ZS_status_t status = ZS_SUCCESS;

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
#if 0
		objs[i].data = malloc(1024);
		if (objs[i].data == NULL) {
			printf("Cannot allocate memory.\n");
			exit(0);
		}
#endif 

	}

	if (!use_mput) {


		for (i = 0; i < num_keys; i++) {
#if 1
			memset(objs[0].key, 0, ZS_MAX_KEY_LEN);
			objs[0].key_len = generate_keys(prefix_len, seed1_start, seed2_start, objs[0].key);
			assert(objs[0].key_len < ZS_MAX_KEY_LEN);

#endif 
			objs[0].data = data;
			objs[0].data_len = datalen;

			/*
			 * Insert key 		
			 */
			status = ZSWriteObject(thd_state, cguid, objs[0].key, objs[0].key_len,
						objs[0].data, objs[0].data_len, 0);
			if (status != ZS_SUCCESS) {
				printf("Write failed with error = %d.\n", status);
				exit(-1);
			}

			status = ZSReadObject(thd_state, cguid, objs[0].key, 
					       objs[0].key_len, &tmp_data, &tmp_datalen);

			if (status != ZS_SUCCESS) {
				printf("Read failed with error %d.\n", status);
				exit(-1);
			}

			seed1_start++;

		}


	} else {

		num_mputs = num_keys / num_objs + 1;
		for (i = 0; i < num_mputs; i++) {

			for (k = 0; k < num_objs; k++) {
				memset(objs[k].key, 0, ZS_MAX_KEY_LEN);
				objs[k].key_len = generate_keys(prefix_len, seed1_start, seed2_start, objs[k].key);
				objs[k].data = data;
				objs[k].data_len = datalen;
			
				seed1_start++;
			}

			if (use_mput) {
				uint32_t written = 0;
				status = ZSMPut(thd_state, cguid, num_objs, &objs[0], 0, &written);
				if (status != ZS_SUCCESS) {
					printf("Write failed with error = %d.\n", status);
					exit(-1);
				}

				assert(written == num_objs);
				for (k = 0; k < num_objs; k++) {
					status = ZSReadObject(thd_state, cguid, objs[k].key, 
							       objs[k].key_len, &tmp_data, &tmp_datalen);

					if (status != ZS_SUCCESS) {
						printf("Read failed with error %d.\n", status);
						assert(0);
						exit(-1);
					}

					ZSFreeBuffer(tmp_data);
				}
			}
		}

	}
#if 1
	for (i = 0; i < num_objs; i++) {
		free(objs[i].key);
	}

	free(objs);
#endif 
}


void
read_keys(int num_keys, int prefix_len, int seed1_start, int seed2_start)
{

	int i = 0;
	char *tmp_data = NULL;
	uint64_t tmp_datalen = 0;
	char key[ZS_MAX_KEY_LEN];
	uint32_t keylen = 0;

	ZS_status_t status = ZS_SUCCESS;


	for (i = 0; i < num_keys; i++) {

		keylen = generate_keys(prefix_len, seed1_start, seed2_start, key);
		status = ZSReadObject(thd_state, cguid, key, 
				       keylen, &tmp_data, &tmp_datalen);

		if (status != ZS_SUCCESS) {
			printf("Read failed with error %d.\n", status);
			exit(-1);
		}

		seed1_start++;
	}

}

void
test_splits(bool use_mput, int seed_start1)
{
	/*
	 * Multiple split of leaf node
	 */
	insert_keys(200, 900, seed_start1, 0, use_mput); 

	read_keys(200, 900, seed_start1, 0);

	/*
	 * Insert smaller key at start of batch.
	 */
	insert_keys(1, 90, seed_start1 + 1, 1, false); 
	read_keys(1, 90, seed_start1 + 1, 1); 

#if 1
	/*
	 * Insert smaller key at end of batch.
	 */
	insert_keys(1, 90, seed_start1 + 202, 1, false); 
#endif 


#if 1

	/*
	 * Extend the batch.
   	 */
	insert_keys(1000, 900, seed_start1 + 210, 0, use_mput); 
	read_keys(1000, 900, seed_start1 + 210, 0); 

	/*
	 * Insert same len key at start of batch.
	 */
	insert_keys(1, 900, seed_start1 + 211, 1, false); 
#endif 


}

int 
main()
{
	char tmp_buf[1000];
	char tmp_buf1[1000];
	ZS_container_props_t props;
	ZS_status_t status;
	ZS_container_meta_t cmeta = {my_cmp_cb, NULL, NULL, NULL};
	uint32_t keylen1 = 0;
	uint32_t keylen2 = 0;
	bool use_mput = false;
	int num_keys = 0;
	int i;

	generate_keys(100, 0, 111, tmp_buf);
	printf("Key = %s.\n", tmp_buf);
	generate_keys(100, 1111, 22, tmp_buf);
	printf("Key = %s.\n", tmp_buf);
	generate_keys(100, 22222, 0, tmp_buf);
	printf("Key = %s.\n", tmp_buf);
	generate_keys(100, 11111, 333, tmp_buf);
	printf("Key = %s.\n", tmp_buf);

	
	keylen1 = generate_keys(100, 11111, 333, tmp_buf);
	keylen2 = generate_keys(50, 11111, 222, tmp_buf1);

	my_cmp_cb(NULL, tmp_buf, keylen1, tmp_buf1, keylen2);


	ZSInit(&zs_state);

	ZSInitPerThreadState(zs_state, &thd_state);	

	ZSLoadCntrPropDefaults(&props);

	props.persistent = 1;
	props.evicting = 0;
	props.writethru = 1;
	props.durability_level= 0;
	props.fifo_mode = 0;
	props.size_kb = 0; 

	status = ZSOpenContainerSpecial(thd_state, "cnt_01", &props,
					 ZS_CTNR_CREATE, &cmeta, &cguid);
	if (status != ZS_SUCCESS) {
		printf("Open Cont failed with error=%x.\n", status);
		exit(-1);
	}



	printf("Writing key with ZSWriteObjs with datalen = %ld.\n", datalen);
	test_splits(false, 0);
	printf("Done without any issues. Passed.\n");

	printf("Writing key with ZSMPut.\n");
	test_splits(true, 300);
	printf("Done without any issues. Passed.\n");

	datalen = 200;

	printf("Writing key with ZSWriteObjs with datalen = %ld.\n", datalen);
	test_splits(false, 0);
	printf("Done without any issues. Passed.\n");

	printf("Writing key with ZSMPut.\n");
	test_splits(true, 300);
	printf("Done without any issues. Passed.\n");

	datalen = 1500;

	printf("Writing key with ZSWriteObjs with datalen = %ld.\n", datalen);
	test_splits(false, 0);
	printf("Done without any issues. Passed.\n");

	printf("Writing key with ZSMPut.\n");
	test_splits(true, 300);
	printf("Done without any issues. Passed.\n");


	datalen = 150;
	printf("Writing key with ZSWriteObjs with datalen = %ld.\n", datalen);
	test_splits(false, 0);
	printf("Done without any issues. Passed.\n");

	printf("Writing key with ZSMPut.\n");
	test_splits(true, 300);
	printf("Done without any issues. Passed.\n");

	datalen = 100;

	printf("Starting Random tests.\n");
	for (i = 0; i < 10; i++) {
		use_mput = rand() % 2;
		num_keys = rand() % 2000;	
		datalen = rand() % 8000;
		test_splits(use_mput, num_keys);
	}
	printf("Done without any issues. Passed.\n");

	ZSCloseContainer(thd_state, cguid);
	ZSDeleteContainer(thd_state, cguid);

}
