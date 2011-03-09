/*
 * File:   stockTradingApp.c
 * Author: Darpan Dinker
 *
 * Created on February 21, 2008, 10:22 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: stockTradingApp.c 332 2008-02-22 23:30:24Z darryl $
 */

#include <stdio.h> // for file IO
#include <inttypes.h> // for uint64_t
#include <string.h> // for strcpy, strdup
#include "platform/logging.h" // for logging
#include "shared/container.h"
#include "shared/init_sdf.h" // for initializing cmc
#include "shared/object.h" // for object container
#include "common/sdftypes.h"

// const char *symbols = {"SUNW", "IBM", "HPQ", "DELL", "ORCL", "CSCO", "MSFT", "GOOG", "YHOO", "SCNR"};

typedef struct {
    char symbol[8];

    float pe;
    float eps;
    uint64_t marketCap;
    long avgVolume;
    float openPrice;
    float closePrice;
    float yearRange_low;
    float yearRange_high;

    char coName[64];
} StockInfoS;

typedef struct {
    char symbol[8];

    float bid;
    float ask;
    long dayVolume;
    float gain;
    float dayValue;
    float percentChange;
    float dayRange_low;
    float dayRange_high;
} StockInfoD;

StockInfoS *
readStaticRecord()
{
    StockInfoS *record = NULL;

    return (record);
}

const char alphabet[26] = {'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
const char *pathSFile = "/tmp/STOCK_STATIC.dat";
const char *pathDFile = "/tmp/STOCK_DYNAMIC.dat";
// =====================================================================================================================
// {{ TODO
char crand() {
    return (alphabet[rand() % 26]);
}

void strrand(char *p, int len) {
    int i;

    for (i = 0; i < len-1; i++, p++) {
        *p = crand();
    }
    *p = '\0';
}

float frand(float from, float to) {
    return (0.0);
}

float fprand(float val, float percent) {
    return (0.0F);
}

long lrand(long from, long to) {
    return (0L);
}
// }} TODO
// =====================================================================================================================
void
createJunkStaticRecord(StockInfoS *rec)
{
    long prob = 0;

    strrand(rec->symbol, 8);
    // strrand(rec->coName, 64);
    rec->pe = frand(0.0F, 200.0F);
    rec->eps = frand(-4.0F, 20.0F);
    rec->marketCap = lrand(1000000L, 100000000000L);
    rec->avgVolume = lrand(1000000L, 100000000000L);

    rec->openPrice = frand(0.0F, 600.0F);
    prob = ((long)rec->openPrice) % 2;

    if (prob) { // odd
        rec->closePrice = fprand(rec->openPrice, 5.0F);
        rec->yearRange_low = fprand(rec->openPrice, 5.0F);
        rec->yearRange_high = fprand(rec->closePrice, 10.0F);
    } else { // even
        rec->closePrice = fprand(rec->openPrice, -5.0F);
        rec->yearRange_low = fprand(rec->closePrice, -5.0F);
        rec->yearRange_high = fprand(rec->yearRange_low, 5.0F);
    }
}

void
createJunkDynamicRecord(StockInfoD *rec, char symbol[], float closePrice)
{
    long prob = 0;

    prob = ((long)closePrice) % 2;
    if (prob) {
        rec->dayValue = fprand(closePrice, 5.0F);
    } else {
        rec->dayValue = fprand(closePrice, -5.0F);
    }
    strncpy(rec->symbol, symbol, 8);

    rec->ask = fprand(rec->dayValue, 5.0F);
    rec->bid = fprand(rec->dayValue, -5.0F);
    rec->dayRange_high = fprand(rec->dayValue, 5.0F);
    rec->dayRange_low = fprand(rec->bid, -2.0F);

    rec->dayVolume = lrand(1000000L, 1000000L);
    rec->gain = frand(-5.0F, 10.0F);
    rec->percentChange = frand(0.0F, 10.0F);

}

int
internal_createJunkFiles(unsigned numRecords)
{
    FILE *fp1 = NULL, *fp2 = NULL;
    StockInfoS rec1;
    StockInfoD rec2;
    unsigned count = 0;

    if (NULL == (fp1 = fopen(pathSFile, "wb+"))) {
        printf("Error openeing %s \n", pathSFile);
        return (-1);
    }

    if (NULL == (fp2 = fopen(pathDFile, "wb+"))) {
        printf("Error openeing %s \n", pathDFile);
        fclose(fp1);
        return (-2);
    }

    for (; count < numRecords; count++) {
        createJunkStaticRecord(&rec1);
        if (!fwrite(&rec1, sizeof(StockInfoS), 1, fp1)) {
            printf("Error writing to %s. %d records written. \n", pathSFile, count);
            break;
        }

        createJunkDynamicRecord(&rec2, rec1.symbol, rec1.closePrice);
        if (!fwrite(&rec2, sizeof(StockInfoD), 1, fp2)) {
            printf("Error writing to %s. %d records written. \n", pathDFile, count);
            break;
        }
    }

    if (count == numRecords) {
        printf("Created %d records. \n", numRecords);
    }

    fclose(fp1);
    fclose(fp2);

    return (1);
}

int
internal_junkFilesExist() {
    FILE *fp1 = NULL, *fp2 = NULL;
    int ret = 0;

    if (NULL != (fp1 = fopen(pathSFile, "rb"))) {
        fclose(fp1);
        if (NULL != (fp2 = fopen(pathDFile, "rb"))) {
            fclose(fp2);
            ret = 1;
        }
    }

    return (ret);
}

char *
mystrndup(char arr[], int len) {
    int i;
    char *ret = (char *) plat_alloc(sizeof (char) * len);

    for (i = 0; i < len; i++) {
        *(ret+i) = arr[i];
    }

    return (ret);
}

SDF_key_t *
setKey(const char *str, int len) {
    string_t *key = plat_alloc(sizeof(string_t) + len);
    key->len = len;
    memcpy(&key->chars, str, len);
    return ((SDF_key_t *)key);
}

int
internal_readFileAndPopulateObjectContainers(SDF_CONTAINER *ocs, SDF_CONTAINER *ocd)
{
    FILE *fp1 = NULL, *fp2 = NULL;
    StockInfoS rec1;
    StockInfoD rec2;
    unsigned count = 0;
    int ret = 1;

    printf("internal_readFileAndPopulateObjectContainers**************************\n");
    if (NULL == (fp1 = fopen(pathSFile, "rb")) || NULL == (fp2 = fopen(pathDFile, "rb"))) {
        ret = -1;
    } else {
        while (!feof(fp1) && 1 == ret) {
            if (!fread(&rec1, sizeof(StockInfoS), 1, fp1)) {
                if (!feof(fp1)) {
                    printf("Error reading %s\n", pathSFile);
                    ret = -1;
                }
            } else {
                SDF_key_t *okey = setKey((rec1.symbol), 8);

		if (SDF_SUCCESS != SDFCreatePut(ocs, okey, &rec1, sizeof(StockInfoS), NULL)) {
                    printf("ObjectContainer::CreatePut() failed\n");
                    ret = -1;
                } else {
                    count++;
                }
            }
        }
        fclose(fp1);
        printf("Read %d records from %s\n", count, pathSFile);
        count = 0;
        while (!feof(fp2) && 1 == ret) {
            if (!fread(&rec2, sizeof(StockInfoD), 1, fp2)) {
                if (!feof(fp2)) {
                    printf("Error reading %s\n", pathDFile);
                    ret = -1;
                }
            } else {
                SDF_key_t *okey = setKey((rec2.symbol), 8);

                if (SDF_SUCCESS != SDFCreatePut(ocd, okey, &rec2, sizeof(StockInfoD), NULL)) {
                    printf("ObjectContainer::CreatePut() failed\n");
                    ret = -1;
                } else {
                    count++;
                }
            }
        }
        fclose(fp2);
        printf("Read %d records from %s\n", count, pathDFile);
    }

    return (1);
}

int
internal_readFileAndPopulateBlockContainers(SDF_CONTAINER *bcs, SDF_CONTAINER *bcd)
{
    FILE *fp1 = NULL, *fp2 = NULL;
    StockInfoS rec1;
    StockInfoD rec2;
    unsigned count = 0;
    int ret = 1;

    printf("internal_readFileAndPopulateBlockContainers**************************\n");
    if (NULL == (fp1 = fopen(pathSFile, "rb")) || NULL == (fp2 = fopen(pathDFile, "rb"))) {
        ret = -1;
    } else {
        while (!feof(fp1) && 1 == ret) {
            if (!fread(&rec1, sizeof(StockInfoS), 1, fp1)) {
                if (!feof(fp1)) {
                    printf("Error reading %s\n", pathSFile);
                    ret = -1;
                }
            } else {
                SDF_key_t okey;
                okey.block_id = count;

                if (SDF_SUCCESS != SDFCreatePut(bcs, &okey, &rec1, sizeof(StockInfoS), NULL)) {
                    printf("ObjectContainer::CreatePut() failed\n");
                    ret = -1;
                } else {
                    count++;
                }
            }
        }
        fclose(fp1);
        printf("Read %d records from %s\n", count, pathSFile);
        count = 0;
        while (!feof(fp2) && 1 == ret) {
            if (!fread(&rec2, sizeof(StockInfoD), 1, fp2)) {
                if (!feof(fp2)) {
                    printf("Error reading %s\n", pathDFile);
                    ret = -1;
                }
            } else {
                SDF_key_t okey;
                okey.block_id = count;

                if (SDF_SUCCESS != SDFCreatePut(bcd, &okey, &rec2, sizeof(StockInfoD), NULL)) {
                    printf("ObjectContainer::CreatePut() failed\n");
                    ret = -1;
                } else {
                    count++;
                }
            }
        }
        fclose(fp2);
        printf("Read %d records from %s\n", count, pathDFile);
    }

    return (1);
}

// ====================================================================================================================

int
useBlockContainers()
{
    char *cnameS = "/tmp/db/stocks/staticblk";
    char *cnameD = "/tmp/db/stocks/dynamicblk";
    SDF_CONTAINER *bcs = NULL, *bcd = NULL;

    if (SDF_SUCCESS != (SDFOpenContainer(cnameS, SDF_READ_WRITE_MODE, &bcs))) {
        // maybe container does not exist, so create it
        SDF_container_props_t p;

        p.container_id.id = 13;
        p.container_type.type = SDF_BLOCK_CONTAINER;
        p.specific.block_props.blockSize = sizeof(StockInfoS);
        if (SDF_SUCCESS != SDFCreateContainer(cnameS, p)) {
            printf("Error creating container %s\n", cnameS);
            return (-1);
        }
        if (SDF_SUCCESS != (SDFOpenContainer(cnameS, SDF_READ_WRITE_MODE, &bcs))) {
            printf("Error opening container %s after successful creation.\n", cnameS);
            return (-1);
        }
    }

    if (SDF_SUCCESS != (SDFOpenContainer(cnameD, SDF_READ_WRITE_MODE, &bcd))) {
        // maybe container does not exist, so create it
        SDF_container_props_t p;

        p.container_id.id = 13;
        p.container_type.type = SDF_BLOCK_CONTAINER;
        p.specific.block_props.blockSize = sizeof(StockInfoD);
        if (SDF_SUCCESS != SDFCreateContainer(cnameD, p)) {
            printf("Error creating container %s\n", cnameD);
            return (-1);
        }
        if (SDF_SUCCESS != (SDFOpenContainer(cnameD, SDF_READ_WRITE_MODE, &bcd))) {
            printf("Error opening container %s after successful creation.\n", cnameD);
            return (-1);
        }
    }
    return (internal_readFileAndPopulateBlockContainers(bcs, bcd));
}

int
useObjectContainers()
{
    char *cnameS = "/tmp/db/stocks/staticobj";
    char *cnameD = "/tmp/db/stocks/dynamicobj";
    SDF_CONTAINER *ocs = NULL, *ocd = NULL;

    if (SDF_SUCCESS != (SDFOpenContainer(cnameS, SDF_READ_WRITE_MODE, &ocs))) {
        // maybe container does not exist, so create it
        SDF_container_props_t p;

        p.container_id.id = 11;
        p.container_type.type = SDF_OBJECT_CONTAINER;
        if (SDF_SUCCESS != SDFCreateContainer(cnameS, p)) {
            printf("Error creating container %s\n", cnameS);
            return (-1);
        }
        if (SDF_SUCCESS != (SDFOpenContainer(cnameS, SDF_READ_WRITE_MODE, &ocs))) {
            printf("Error opening container %s after successful creation.\n", cnameS);
            return (-1);
        }
    }

    if (SDF_SUCCESS != (SDFOpenContainer(cnameD, SDF_READ_WRITE_MODE, &ocd))) {
        // maybe container does not exist, so create it
        SDF_container_props_t p;

        p.container_id.id = 12;
        p.container_type.type = SDF_OBJECT_CONTAINER;
        if (SDF_SUCCESS != SDFCreateContainer(cnameD, p)) {
            printf("Error creating container %s\n", cnameD);
            return (-1);
        }
        if (SDF_SUCCESS != (SDFOpenContainer(cnameD, SDF_READ_WRITE_MODE, &ocd))) {
            printf("Error opening container %s after successful creation.\n", cnameD);
            return (-1);
        }
    }
    return (internal_readFileAndPopulateObjectContainers(ocs, ocd));
}
// ====================================================================================================================
int
stockTradingApp_start()
{
    int num_objects = 256;

    plat_log_parse_arg("sdf/client=debug");
    if (init_sdf_initialize(num_objects) != SDF_SUCCESS) {
        return (-1);
    }
    if (!internal_junkFilesExist()) {
        internal_createJunkFiles(1000);
    }

    return (useObjectContainers() && useBlockContainers());
}

int
main(int argc, char *argv[])
{
    return (stockTradingApp_start());
}
