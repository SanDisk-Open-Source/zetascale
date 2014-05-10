#include "fdf.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#define FDF_MAX_KEY_LEN 256
#define NWORKER 4
#define OBJ_COUNT 100000

static int count = 0;

typedef struct worker_arg_ {
    int id;
} worker_arg_t;

struct FDF_state *fdf_state;

void worker(void *ptr);


void worker(void *ptr)
{
    assert(ptr);
    FDF_container_props_t props;
    struct FDF_thread_state *thd_state;
    struct FDF_iterator *iterator;
    FDF_status_t	status;
    FDF_cguid_t cguid;
    uint64_t datalen;
    char	*key;
    char	*data = NULL;
    char	data1[256] = "data is just for testing pstats";
    int i;
    uint32_t keylen;
    char cname[FDF_MAX_KEY_LEN];
    char key_var[FDF_MAX_KEY_LEN] ={0};

    FDFInitPerThreadState(fdf_state, &thd_state);	

    FDFLoadCntrPropDefaults(&props);

    props.persistent = 1;
    props.evicting = 0;
    props.writethru = 1;
    //	props.async_writes = 0;
    props.durability_level= FDF_DURABILITY_SW_CRASH_SAFE;
    //	props.fifo_mode = 1;
    props.fifo_mode = 0;

    //int id = ((worker_arg_t*)ptr)->id;
    sprintf(cname,  "cntr_%d", __sync_fetch_and_add(&(count), 1));
    printf("worker:cname=%s\n", cname);

    status = FDFOpenContainer(thd_state, cname, &props, FDF_CTNR_CREATE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        exit(0);//return -1;	
    }

    status = FDFOpenContainer(thd_state, cname, &props, FDF_CTNR_RW_MODE, &cguid);
    if (status != FDF_SUCCESS) {
        printf("Open Cont failed with error=%x.\n", status);
        exit(0);//return -1;	
    }
    for (i = 1; i <= OBJ_COUNT; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%08d", i);
        sprintf(data1, "data_%08d", i);

        FDFWriteObject(thd_state, cguid, key_var, strlen(key_var) + 1, data1, strlen(data1), 0);
    }

    for (i = 1; i <= OBJ_COUNT/1000; i++) {
        memset(key_var, 0, FDF_MAX_KEY_LEN);
        sprintf(key_var, "key_%08d", i);
        status = FDFDeleteObject(thd_state, cguid, key_var, strlen(key_var) + 1);
        if (status != FDF_SUCCESS)
            fprintf(stderr, "FDFDELETE RETURNED %s\n", FDFStrError(status));
    }

# if 0
    //  _FDFScavengeContainer(struct FDF_state *fdf_state, FDF_cguid_t cguid, bool is_sync)
    status = FDFScavengeContainer(fdf_state, cguid);
    fprintf(stderr, "FDFScavengeContainer returns %s\n", FDFStrError(status));
#endif
    status =  FDFEnumerateContainerObjects(thd_state, cguid, &iterator);

    int ii = 0;
    while (FDFNextEnumeratedObject(thd_state, iterator, &key, &keylen, &data, &datalen) == FDF_SUCCESS) {
        if (0 == (ii % 12345)) {
            fprintf(stderr, "key = %s keylen=%d\n", key, keylen);
        }
        ii++;
        FDFFreeBuffer(data);
        FDFFreeBuffer(key);
    }
    FDFFinishEnumeration(thd_state, iterator);

    FDFCloseContainer(thd_state, cguid);
    FDFReleasePerThreadState(&thd_state);
}


int
main()
{
    FDF_status_t status;
    FDFSetProperty("FDF_REFORMAT", "1");
    status = FDFInit(&fdf_state);

    if (FDF_SUCCESS != status) {
      fprintf(stderr, "FDFInit: error=%s\n", FDFStrError(status));
      exit(0);
    }

    pthread_t thread[32];
    int  iret[32];
    /* Create independent threads each of which will execute function */

    int i;
    for (i = 0; i < NWORKER; i++) {
        worker_arg_t arg;
        arg.id = 0;
        //(void) __sync_fetch_and_add(&(arg.id), 1);
        arg.id += i;
        iret[i] = pthread_create( &thread[i], NULL, (void *)worker, (void*)&arg);
        fprintf(stderr,"Create worker Thread %d\n",arg.id);
    }

    for (i = 0; i < NWORKER; i++) {         
        pthread_join( thread[i], NULL);
    }
    FDFShutdown(fdf_state);
    exit(0);
}
