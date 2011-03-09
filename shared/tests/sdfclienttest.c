/*
 * File:   sdfclient/sdfclienttest.c
 * Author: Darpan Dinker
 *
 * Created on February 4, 2008, 3:49 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdfclienttest.c 332 2008-02-22 23:30:24Z darryl $
 */

// #define DEBUG


#include "platform/string.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "shared/object.h"
#include "shared/container_props.h"
#include "block_container.h"
#include "shared/init_sdf.h"
#include "shared/cmc.h"


char cpath[] = "myDatabase1/maria/inventory.idx";

SDF_size_t blockSize = 8096;
int numBlocks = 1024;


int
testCreateBlockContainer(void)
{
    int failed = 0;
    SDF_status_t status;
    SDF_container_props_t properties;

    printf("// testCreateBlockContainer\n");
    properties.container_id.size = blockSize * numBlocks;

    if (SDF_SUCCESS != (status = SDFCreateBlockContainer(NULL, blockSize, properties))) {
        // NOTE: should fail as NULL path is not allowed
        plat_log_msg(21631, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testCreateBlockContainer() #1");
    } else {
        plat_log_msg(21632, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILED: testCreateBlockContainer() #1");
        failed = 1;
    }

    if (SDF_SUCCESS != (status = SDFCreateBlockContainer(cpath, 8095, properties))) {
        // NOTE: should fail as odd number for block size is not allowed
        plat_log_msg(21633, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testCreateBlockContainer() #2");
        failed = 1;
    } else {
        plat_log_msg(21634, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILED: testCreateBlockContainer() #2");
    }

    if (SDF_SUCCESS != (status = SDFCreateBlockContainer(cpath, blockSize, properties))) {
        plat_log_msg(21635, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILED: testCreateBlockContainer() #3");
        failed = 1;
    } else {
        failed = 0;
        plat_log_msg(21636, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testCreateBlockContainer() #3");
    }

    return (failed);
}

int
testDeleteBlockContainer() {

    int failed = 1;

    printf("// testDeleteBlockContainer\n");
    if (SDFDeleteContainer(cpath) == SDF_SUCCESS) {
        failed = 0;
        plat_log_msg(21637, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: delete block container");
    } else {
        plat_log_msg(21638, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: delete block container");
    }

    return (failed);
}

SDF_CONTAINER
testOpenBlockContainer()
{
    SDF_CONTAINER container = containerNull;

    printf("// testOpenBlockContainer\n");
    if (!isContainerNull(container = SDFOpenBlockContainer(cpath, SDF_READ_WRITE_MODE))) {
        plat_log_msg(21639, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILED: testOpenBlockContainer() #1");
    } else {
        plat_log_msg(21640, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testOpenBlockContainer() #1");
    }

    return (container);
}


int
testGetPutBlockContainer(SDF_CONTAINER c) {

    int failed = 0;
    SDF_key_t *key = plat_alloc(64);;
    SDF_tx_id txid;
    char *pdata = plat_alloc(blockSize);

    printf("// testGetPutBlockContainer\n");
    for (int i = 0; i < numBlocks; ++i) {

        char s[16];

        for (int j = 0; j < blockSize/16; j++) {
            memcpy(&pdata[j*16], "0123456789123456", 16);
        }
        sprintf(s, "BLOCK-%d-", i);
        memcpy(&pdata[0], s, strlen(s));

        key->block_id = i;
        if (SDFPutBlock(txid, c, i, (void *) pdata, 0, 0, 0, 0) != SDF_SUCCESS) {
            failed = 1;
            break;
        }
        plat_log_msg(21641, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testGetPutBlockContainer - put block %d", i);
    }

    for (int k = 0; k < numBlocks; k++) {

        key->block_id = k;
        if ((SDFGetBlockForRead(txid, c, k, 0, 0, (void **) &pdata)) != SDF_SUCCESS) {
            plat_log_msg(21642, PLAT_LOG_CAT_SDF_CLIENT,
                         PLAT_LOG_LEVEL_DEBUG, "FAILED: testGetPutBlockContainer - get block %d", (int)key->block_id);
        } else {
            plat_log_msg(21643, PLAT_LOG_CAT_SDF_CLIENT,
                         PLAT_LOG_LEVEL_DEBUG, "SUCCESS: testGetPutBlockContainer - get block %d", (int)key->block_id);
        }
    }

    return (failed);
}

SDF_status_t
test_create_container(char *cname) {

    SDF_status_t status = SDF_FAILURE;
    SDF_container_props_t p;
    
    printf("// test_create_container\n");
    // Create the container
    p.container_id.id = 1777;
    p.container_type.type = SDF_OBJECT_CONTAINER;
    status = SDFCreateContainer(cname, p);

    if (status == SDF_SUCCESS) {
        plat_log_msg(21644, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: test container create");

    } else {
        plat_log_msg(21645, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: test container create");
    }

    return (status);
}


SDF_status_t
test_delete_container(char *cname) {

    SDF_status_t status = SDF_FAILURE;

    printf("// test_delete_container\n");
    if ((status = SDFDeleteContainer(cname)) == SDF_SUCCESS) {
        plat_log_msg(21646, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: delete container");
    } else {
        plat_log_msg(21647, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: delete container");
    }

    return (status);
}

SDF_status_t
test_open_close_container(char *cname) {

    SDF_status_t status = SDF_FAILURE;
    SDF_CONTAINER c = containerNull;

    printf("// test_open_close_container\n");
    if (!isContainerNull(c = SDFOpenBlockContainer(cname, SDF_READ_WRITE_MODE))) {
        plat_log_msg(21648, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: Open Container - %s", cname);
    } else if ((SDFCloseBlockContainer(c)) != SDF_SUCCESS) {
        plat_log_msg(21649, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: Close Container - %s", cname);
    } else {
        status = SDF_SUCCESS;
        plat_log_msg(21650, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: Open/Close Container - %s", cname);
    }
    return (status);
}

// Execute object put and get against an existing container.
/*
void
test_container_object(char *cname) {


    SDF_CONTAINER c = containerNull;

    printf("// test_container_object\n");
    if (!isContainerNull(c = SDFOpenBlockContainer(cname, SDF_READ_WRITE_MODE))) {
        plat_log_msg(21648, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: Open Container - %s", cname);
    } else {

        int size = 5;
        // Don't that chars are at offset 2
        string_t *key = SDF_MALLOC(sizeof(string_t) + 3);
        key->len = 3;
        memcpy(&key->chars, "foo", 3);

        string_t *inData = SDF_MALLOC(8192);

        inData->len = size * 10;
        for (int i = 0; i < size; i++) {
            memcpy(&inData->chars[i*10], "0123456789", 10);
        }

        char *pdata = plat_strdup(inData->chars);
        char *gdata = (char *) plat_alloc(inData->len);

        if ((SDFCreate(c, (SDF_key_t *)key, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21651, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFCreate");
            return;
        } 

        memcpy(&key->chars, "goo", 3);

        if ((SDFCreate(c, (SDF_key_t *)key, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21651, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFCreate");
            return;
        } 

        if ((SDFPut(c, (SDF_key_t *)key, (void *) pdata, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21652, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFPut");
            return;
        } 

        memcpy(&key->chars, "foo", 3);
        if ((SDFGet(c, (SDF_key_t *)key, (void **) &gdata, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21653, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFGet");
            return;
        }
        printf("foo - %s\n", gdata);

        memcpy(&key->chars, "goo", 3);
        if ((SDFGet(c, (SDF_key_t *)key, (void **) &gdata, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21653, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFGet");
            return;
        }
        printf("goo - %s\n", gdata);

        memcpy(&key->chars, "boo", 3);
        if ((SDFGet(c, (SDF_key_t *)key, (void **) &gdata, (SDF_size_t) inData->len, 0)) != SDF_SUCCESS) {
            plat_log_msg(21653, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG, 
                         "FAILURE: SDFGet");
        } else {
            printf("foo - %s\n", gdata);
        }

        if ((SDFCloseContainer(cname, c)) != SDF_SUCCESS) {
            plat_log_msg(21649, PLAT_LOG_CAT_SDF_CLIENT,
                         PLAT_LOG_LEVEL_DEBUG, "FAILURE: Close Container - %s", cname);
        } else {
            plat_log_msg(21650, PLAT_LOG_CAT_SDF_CLIENT,
                         PLAT_LOG_LEVEL_DEBUG, "SUCCESS: Open/Close Container - %s", cname);
        }
    }
}
*/
void initialize(int num_objects) {

    printf("// initialize\n");
    if (init_sdf_initialize(num_objects) == SDF_SUCCESS) {
        plat_log_msg(21654, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "SUCCESS: SDF init");
    } else {
        plat_log_msg(21655, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILED: SDF init");
    }

    printf("*************************************\n");
}

void
test_flash() {

    // Use a fake flash file for testing
    flashDev_t *dev = (flashDev_t *) SDF_MALLOC(sizeof(flashDev_t));
    dev->fd = flashFileInit("/tmp/flash");

    // Initialize the block table.
    flashBlock_t *blockTable[16];
    flashBlock_t *prev = NULL;
    for (int i = 0; i < 16; i++) {
        blockTable[i] = (flashBlock_t *) SDF_MALLOC(sizeof(flashBlock_t));
        blockTable[i]->freeNext = prev;
        blockTable[i]->blockNum = i;
        blockTable[i]->freeAddr = 0;
        blockTable[i]->freeSpace = FLASH_BLOCK_SIZE;
        blockTable[i]->chunkHead = NULL;
        prev = blockTable[i];
    }

    dev->freeBlocks = blockTable[15];
    dev->badBlocks = NULL;

    // Initialize the shard header until we merge with Darryl's code
    shard_t *shard = (shard_t *) SDF_MALLOC(sizeof(shard_t));
    shard->shardType = 1;
    shard->dev = dev;
    shard->curBlock = NULL;
    shard->numBuckets = 16;
    shard->objTable = (objDesc_t **) SDF_MALLOC(sizeof(objDesc_t **) * 16);
    for (int i = 0; i < 16; i++) {
        shard->objTable[i] = NULL;
    }

    // Just set up some data to move in and out
    string_t *key = plat_alloc(2+3);
    key->len = 3;
    memcpy(&key->chars, "foo", 3);

    string_t *inData = plat_alloc(8192);
    string_t *outData = plat_alloc(8192);
    inData->len = 500;
    for (int i = 0; i < 50; i++) {
        memcpy(&inData->chars[i*10], "0123456789", 10);
    }
    //    memcpy(&inData->chars, "data", 4);
    // memcpy(&inData->chars, "data0123456789", 14);

    if (!flashPut(shard, key, inData, 0, 0, 0)) {
        plat_log_msg(21656, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashPut");
    }
    
    memcpy(&key->chars, "bar", 3);
    for (int i = 0; i < 50; i++) {
        memcpy(&inData->chars[i*10], "abcdefghij", 10);
    }

    if (!flashPut(shard, key, inData, 0, 0, 0)) {
        plat_log_msg(21656, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashPut");
    }

    if (!flashPut(shard, key, inData, 0, 0, 0)) {
        plat_log_msg(21656, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashPut");
    }

    outData = NULL;
    if (!flashGet(shard, key, &outData, 0, 0)) {
        plat_log_msg(21657, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashGet");
    }
    printf("bar - %s\n", outData->chars);

    memcpy(&key->chars, "foo", 3);

    outData = NULL;
    if (!flashGet(shard, key, &outData, 0, 0)) {
        plat_log_msg(21657, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashGet");
    }
    printf("foo - %s\n", outData->chars);

    if (!flashPut(shard, key, inData, 0, 0, 0)) {
        plat_log_msg(21656, PLAT_LOG_CAT_SDF_CLIENT,
                     PLAT_LOG_LEVEL_DEBUG, "FAILURE: flashPut");
    }

}


int
main(int argc, char **argv) {
    int failed = 0;
    SDF_CONTAINER c;
    char *cname = "/db/row91";
    int num_objects = 128;


    printf("// =================================\n");
    initialize(num_objects);

    plat_log_parse_arg("sdf/client=debug");

    if (1) {
        printf("// =================================\n");
        failed = testCreateBlockContainer();

        printf("// =================================\n");
        c = testOpenBlockContainer();
        if (isContainerNull(c)) {
            failed = 1;
        }

        printf("// =================================\n");
        c = testOpenBlockContainer();
        if (isContainerNull(c)) {
            failed = 1;
        }

        printf("// =================================\n");
        if (!failed) {
            failed = testGetPutBlockContainer(c);
        }

        printf("// =================================\n");
        failed = testDeleteBlockContainer();


        printf("// =================================\n");
        test_flash();

        printf("// =================================\n");
        test_create_container(cname);

        printf("// =================================\n");
        test_open_close_container(cname);

        printf("// =================================\n");
        // test_container_object(cname); TODO

        printf("// =================================\n");
        test_delete_container(cname);
    }

    return (failed);
}
