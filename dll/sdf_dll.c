/*
 * FDF linkage; this file was generated.
 * Copyright (c) 2012, SanDisk Corporation.  All rights reserved.
 */
#include <dlfcn.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include "sdf.h"


/*
 * Macros.
 */
#define nel(a)      (sizeof(a)/sizeof(*(a)))
#define unlikely(x) __builtin_expect((x), 0)


/*
 * FDF Library locations.
 */
static char *fdflibs[] ={
    "/usr/lib64/fdf/libfdf.so",
    "/usr/lib/fdf/libfdf.so",
    "/lib64/libfdf.so",
    "/lib/libfdf.so",
    "/usr/local/lib64/libfdf.so",
    "/usr/local/lib/libfdf.so",
    "libfdf.so",
};


/*
 * Function pointers.
 */
static SDF_status_t 
(*ptr_Init)(struct SDF_state **sdf_state, int argc, char **argv);

static struct SDF_thread_state *
(*ptr_InitPerThreadState)(struct SDF_state *sdf_state);

static SDF_status_t 
(*ptr_CreateContainer)(struct SDF_thread_state *sdf_thread_state,
                       char *cname,
                       SDF_container_props_t *props,
                       SDF_cguid_t *cguid);

static SDF_status_t 
(*ptr_OpenContainer)(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     SDF_container_mode_t mode);

static SDF_status_t 
(*ptr_CloseContainer)(struct SDF_thread_state *sdf_thread_state,
                      SDF_cguid_t cguid);

static SDF_status_t 
(*ptr_DeleteContainer)(struct SDF_thread_state *sdf_thread_state,
                       SDF_cguid_t cguid);

static SDF_status_t 
(*ptr_GetForReadBufferedObject)(struct SDF_thread_state *sdf_thread_state,
                                SDF_cguid_t cguid,
                                char *key,
                                uint32_t keylen,
                                char **data,
                                uint64_t *datalen,
                                SDF_time_t current_time,
                                SDF_time_t *expiry_time);

static SDF_status_t 
(*ptr_CreateBufferedObject)(struct SDF_thread_state *sdf_thread_state,
                            SDF_cguid_t cguid,
                            char *key,
                            uint32_t keylen,
                            char *data,
                            uint64_t datalen,
                            SDF_time_t current_time,
                            SDF_time_t expiry_time);

static SDF_status_t 
(*ptr_SetBufferedObject)(struct SDF_thread_state *sdf_thread_state,
                         SDF_cguid_t cguid,
                         char *key,
                         uint32_t keylen,
                         char *data,
                         uint64_t datalen,
                         SDF_time_t current_time,
                         SDF_time_t expiry_time);

static SDF_status_t 
(*ptr_PutBufferedObject)(struct SDF_thread_state *sdf_thread_state,
                         SDF_cguid_t cguid,
                         char *key,
                         uint32_t keylen,
                         char *data,
                         uint64_t datalen,
                         SDF_time_t current_time,
                         SDF_time_t expiry_time);

static SDF_status_t 
(*ptr_RemoveObjectWithExpiry)(struct SDF_thread_state *sdf_thread_state,
                              SDF_cguid_t cguid,
                              char *key,
                              uint32_t keylen,
                              SDF_time_t current_time);

static SDF_status_t 
(*ptr_FreeBuffer)(struct SDF_thread_state *sdf_thread_state, char *data);

static SDF_status_t 
(*ptr_GetBuffer)(struct SDF_thread_state *sdf_thread_state,
                 char **data,
                 uint64_t datalen);

static SDF_status_t 
(*ptr_FlushObject)(struct SDF_thread_state *sdf_thread_state,
                   SDF_cguid_t cguid,
                   char *key,
                   uint32_t keylen,
                   SDF_time_t current_time);

static SDF_status_t 
(*ptr_FlushContainer)(struct SDF_thread_state *sdf_thread_state,
                      SDF_cguid_t cguid,
                      SDF_time_t current_time);

static SDF_status_t 
(*ptr_FlushCache)(struct SDF_thread_state *sdf_thread_state);

static SDF_status_t 
(*ptr_GetContainers)(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t *cguids,
                     uint32_t *n_cguids);

static SDF_status_t 
(*ptr_GetContainerProps)(struct SDF_thread_state *sdf_thread_state,
                         SDF_cguid_t cguid,
                         SDF_container_props_t *ctnr_props);

static SDF_status_t 
(*ptr_SetContainerProps)(struct SDF_thread_state *sdf_thread_state,
                         SDF_cguid_t cguid,
                         SDF_container_props_t *ctnr_props);

static SDF_status_t 
(*ptr_EnumerateContainerObjects)(struct SDF_thread_state *sdf_thread_state,
                                 SDF_cguid_t cguid,
                                 struct SDF_iterator **iterator);

static SDF_status_t 
(*ptr_NextEnumeratedObject)(struct SDF_thread_state *sdf_thread_state,
                            struct SDF_iterator *iterator,
                            char **key,
                            uint32_t *keylen,
                            char **data,
                            uint64_t *datalen);

static SDF_status_t 
(*ptr_FinishEnumeration)(struct SDF_thread_state *sdf_thread_state,
                         struct SDF_iterator *iterator);

static SDF_status_t 
(*ptr_GetStats)(struct SDF_thread_state *sdf_thread_state,
                SDF_stats_t *stats);

static SDF_status_t 
(*ptr_GetContainerStats)(struct SDF_thread_state *sdf_thread_state,
                         SDF_cguid_t cguid,
                         SDF_container_stats_t *stats);

static SDF_status_t 
(*ptr_StartContainer)(struct SDF_thread_state *sdf_thread_state,
                      SDF_cguid_t cguid);

static SDF_status_t 
(*ptr_StopContainer)(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid);

static SDF_status_t 
(*ptr_BackupContainer)(struct SDF_thread_state *sdf_thread_state,
                       SDF_cguid_t cguid,
                       char *backup_directory);

static SDF_status_t 
(*ptr_RestoreContainer)(struct SDF_thread_state *sdf_thread_state,
                        SDF_cguid_t cguid,
                        char *backup_directory);


/*
 * Linkage table.
 */
static struct {
    const char *name;
    void       *func;
} table[] ={
    { "SDFInit",                       &ptr_Init                      },
    { "SDFInitPerThreadState",         &ptr_InitPerThreadState        },
    { "SDFCreateContainer",            &ptr_CreateContainer           },
    { "SDFOpenContainer",              &ptr_OpenContainer             },
    { "SDFCloseContainer",             &ptr_CloseContainer            },
    { "SDFDeleteContainer",            &ptr_DeleteContainer           },
    { "SDFGetForReadBufferedObject",   &ptr_GetForReadBufferedObject  },
    { "SDFCreateBufferedObject",       &ptr_CreateBufferedObject      },
    { "SDFSetBufferedObject",          &ptr_SetBufferedObject         },
    { "SDFPutBufferedObject",          &ptr_PutBufferedObject         },
    { "SDFRemoveObjectWithExpiry",     &ptr_RemoveObjectWithExpiry    },
    { "SDFFreeBuffer",                 &ptr_FreeBuffer                },
    { "SDFGetBuffer",                  &ptr_GetBuffer                 },
    { "SDFFlushObject",                &ptr_FlushObject               },
    { "SDFFlushContainer",             &ptr_FlushContainer            },
    { "SDFFlushCache",                 &ptr_FlushCache                },
    { "SDFGetContainers",              &ptr_GetContainers             },
    { "SDFGetContainerProps",          &ptr_GetContainerProps         },
    { "SDFSetContainerProps",          &ptr_SetContainerProps         },
    { "SDFEnumerateContainerObjects",  &ptr_EnumerateContainerObjects },
    { "SDFNextEnumeratedObject",       &ptr_NextEnumeratedObject      },
    { "SDFFinishEnumeration",          &ptr_FinishEnumeration         },
    { "SDFGetStats",                   &ptr_GetStats                  },
    { "SDFGetContainerStats",          &ptr_GetContainerStats         },
    { "SDFStartContainer",             &ptr_StartContainer            },
    { "SDFStopContainer",              &ptr_StopContainer             },
    { "SDFBackupContainer",            &ptr_BackupContainer           },
    { "SDFRestoreContainer",           &ptr_RestoreContainer          },
};


/*
 * Print out an error message and exit.
 */
static void
panic(char *fmt, ...)
{
    va_list alist;

    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
    sdf_exit(1);
}


/*
 * An undefined symbol was found.
 */
static void
undefined(char *sym)
{
    panic("FDF: undefined symbol: %s", sym);
}


/*
 * Determine if the string ends with "No such file or directory".
 */
static int
nsfod(char *str)
{
    char *err = "No such file or directory";
    int  elen = strlen(err);
    int  slen = strlen(str);

    if (slen < elen)
        return 0;
    return !strcmp(str+slen-elen, err);
}


/*
 * Load the FDF library.
 */
static int
load(char *path)
{
    int i;
    void  *dl = dlopen(path, RTLD_LAZY);
    char *err = dlerror();

    if (!dl) {
        if (nsfod(err))
            return 0;
        panic("%s", err);
    }
    
    int n = nel(table);
    for (i = 0; i < n; i++) {
        const char *name = table[i].name;
        void *func = dlsym(dl, name);
        if (func)
            *(void **)table[i].func = func;
        else
            fprintf(stderr, "warning: FDF: undefined symbol: %s\n", name);
    }
    return 1;
}


/*
 * Load the FDF library.
 */
static void
parse(int argc, char *argv[])
{
    int i;
    char *lib = getenv("FDF_LIB");

    if (lib) {
        if (load(lib))
            return;
        panic("cannot find FDF_LIB=%s", lib);
    }

    if (load("/usr/lib64/fdf/libfdf.so"))
        return;

    for (i = 0; i < nel(fdflibs); i++)
        if (load(fdflibs[i]))
            return;
    panic("cannot find libfdf.so");
}


/*
 * SDFInit
 */
SDF_status_t 
SDFInit(struct SDF_state **sdf_state, int argc, char **argv)
{
    parse(argc, argv);
    if (unlikely(!ptr_Init))
        undefined("SDFInit");

    return (*ptr_Init)(sdf_state, argc, argv);
}


/*
 * SDFInitPerThreadState
 */
struct SDF_thread_state *
SDFInitPerThreadState(struct SDF_state *sdf_state)
{
    if (unlikely(!ptr_InitPerThreadState))
        undefined("SDFInitPerThreadState");

    return (*ptr_InitPerThreadState)(sdf_state);
}


/*
 * SDFCreateContainer
 */
SDF_status_t 
SDFCreateContainer(struct SDF_thread_state *sdf_thread_state,
                   char *cname,
                   SDF_container_props_t *props,
                   SDF_cguid_t *cguid)
{
    if (unlikely(!ptr_CreateContainer))
        undefined("SDFCreateContainer");

    return (*ptr_CreateContainer)(sdf_thread_state, cname, props, cguid);
}


/*
 * SDFOpenContainer
 */
SDF_status_t 
SDFOpenContainer(struct SDF_thread_state *sdf_thread_state,
                 SDF_cguid_t cguid,
                 SDF_container_mode_t mode)
{
    if (unlikely(!ptr_OpenContainer))
        undefined("SDFOpenContainer");

    return (*ptr_OpenContainer)(sdf_thread_state, cguid, mode);
}


/*
 * SDFCloseContainer
 */
SDF_status_t 
SDFCloseContainer(struct SDF_thread_state *sdf_thread_state,
                  SDF_cguid_t cguid)
{
    if (unlikely(!ptr_CloseContainer))
        undefined("SDFCloseContainer");

    return (*ptr_CloseContainer)(sdf_thread_state, cguid);
}


/*
 * SDFDeleteContainer
 */
SDF_status_t 
SDFDeleteContainer(struct SDF_thread_state *sdf_thread_state,
                   SDF_cguid_t cguid)
{
    if (unlikely(!ptr_DeleteContainer))
        undefined("SDFDeleteContainer");

    return (*ptr_DeleteContainer)(sdf_thread_state, cguid);
}


/*
 * SDFGetForReadBufferedObject
 */
SDF_status_t 
SDFGetForReadBufferedObject(struct SDF_thread_state *sdf_thread_state,
                            SDF_cguid_t cguid,
                            char *key,
                            uint32_t keylen,
                            char **data,
                            uint64_t *datalen,
                            SDF_time_t current_time,
                            SDF_time_t *expiry_time)
{
    if (unlikely(!ptr_GetForReadBufferedObject))
        undefined("SDFGetForReadBufferedObject");

    return (*ptr_GetForReadBufferedObject)(sdf_thread_state,
                                           cguid,
                                           key,
                                           keylen,
                                           data,
                                           datalen,
                                           current_time,
                                           expiry_time);
}


/*
 * SDFCreateBufferedObject
 */
SDF_status_t 
SDFCreateBufferedObject(struct SDF_thread_state *sdf_thread_state,
                        SDF_cguid_t cguid,
                        char *key,
                        uint32_t keylen,
                        char *data,
                        uint64_t datalen,
                        SDF_time_t current_time,
                        SDF_time_t expiry_time)
{
    if (unlikely(!ptr_CreateBufferedObject))
        undefined("SDFCreateBufferedObject");

    return (*ptr_CreateBufferedObject)(sdf_thread_state,
                                       cguid,
                                       key,
                                       keylen,
                                       data,
                                       datalen,
                                       current_time,
                                       expiry_time);
}


/*
 * SDFSetBufferedObject
 */
SDF_status_t 
SDFSetBufferedObject(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     char *key,
                     uint32_t keylen,
                     char *data,
                     uint64_t datalen,
                     SDF_time_t current_time,
                     SDF_time_t expiry_time)
{
    if (unlikely(!ptr_SetBufferedObject))
        undefined("SDFSetBufferedObject");

    return (*ptr_SetBufferedObject)(sdf_thread_state,
                                    cguid,
                                    key,
                                    keylen,
                                    data,
                                    datalen,
                                    current_time,
                                    expiry_time);
}


/*
 * SDFPutBufferedObject
 */
SDF_status_t 
SDFPutBufferedObject(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     char *key,
                     uint32_t keylen,
                     char *data,
                     uint64_t datalen,
                     SDF_time_t current_time,
                     SDF_time_t expiry_time)
{
    if (unlikely(!ptr_PutBufferedObject))
        undefined("SDFPutBufferedObject");

    return (*ptr_PutBufferedObject)(sdf_thread_state,
                                    cguid,
                                    key,
                                    keylen,
                                    data,
                                    datalen,
                                    current_time,
                                    expiry_time);
}


/*
 * SDFRemoveObjectWithExpiry
 */
SDF_status_t 
SDFRemoveObjectWithExpiry(struct SDF_thread_state *sdf_thread_state,
                          SDF_cguid_t cguid,
                          char *key,
                          uint32_t keylen,
                          SDF_time_t current_time)
{
    if (unlikely(!ptr_RemoveObjectWithExpiry))
        undefined("SDFRemoveObjectWithExpiry");

    return (*ptr_RemoveObjectWithExpiry)(sdf_thread_state,
                                         cguid,
                                         key,
                                         keylen,
                                         current_time);
}


/*
 * SDFFreeBuffer
 */
SDF_status_t 
SDFFreeBuffer(struct SDF_thread_state *sdf_thread_state, char *data)
{
    if (unlikely(!ptr_FreeBuffer))
        undefined("SDFFreeBuffer");

    return (*ptr_FreeBuffer)(sdf_thread_state, data);
}


/*
 * SDFGetBuffer
 */
SDF_status_t 
SDFGetBuffer(struct SDF_thread_state *sdf_thread_state,
             char **data,
             uint64_t datalen)
{
    if (unlikely(!ptr_GetBuffer))
        undefined("SDFGetBuffer");

    return (*ptr_GetBuffer)(sdf_thread_state, data, datalen);
}


/*
 * SDFFlushObject
 */
SDF_status_t 
SDFFlushObject(struct SDF_thread_state *sdf_thread_state,
               SDF_cguid_t cguid,
               char *key,
               uint32_t keylen,
               SDF_time_t current_time)
{
    if (unlikely(!ptr_FlushObject))
        undefined("SDFFlushObject");

    return (*ptr_FlushObject)(sdf_thread_state,
                              cguid,
                              key,
                              keylen,
                              current_time);
}


/*
 * SDFFlushContainer
 */
SDF_status_t 
SDFFlushContainer(struct SDF_thread_state *sdf_thread_state,
                  SDF_cguid_t cguid,
                  SDF_time_t current_time)
{
    if (unlikely(!ptr_FlushContainer))
        undefined("SDFFlushContainer");

    return (*ptr_FlushContainer)(sdf_thread_state, cguid, current_time);
}


/*
 * SDFFlushCache
 */
SDF_status_t 
SDFFlushCache(struct SDF_thread_state *sdf_thread_state)
{
    if (unlikely(!ptr_FlushCache))
        undefined("SDFFlushCache");

    return (*ptr_FlushCache)(sdf_thread_state);
}


/*
 * SDFGetContainers
 */
SDF_status_t 
SDFGetContainers(struct SDF_thread_state *sdf_thread_state,
                 SDF_cguid_t *cguids,
                 uint32_t *n_cguids)
{
    if (unlikely(!ptr_GetContainers))
        undefined("SDFGetContainers");

    return (*ptr_GetContainers)(sdf_thread_state, cguids, n_cguids);
}


/*
 * SDFGetContainerProps
 */
SDF_status_t 
SDFGetContainerProps(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     SDF_container_props_t *ctnr_props)
{
    if (unlikely(!ptr_GetContainerProps))
        undefined("SDFGetContainerProps");

    return (*ptr_GetContainerProps)(sdf_thread_state, cguid, ctnr_props);
}


/*
 * SDFSetContainerProps
 */
SDF_status_t 
SDFSetContainerProps(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     SDF_container_props_t *ctnr_props)
{
    if (unlikely(!ptr_SetContainerProps))
        undefined("SDFSetContainerProps");

    return (*ptr_SetContainerProps)(sdf_thread_state, cguid, ctnr_props);
}


/*
 * SDFEnumerateContainerObjects
 */
SDF_status_t 
SDFEnumerateContainerObjects(struct SDF_thread_state *sdf_thread_state,
                             SDF_cguid_t cguid,
                             struct SDF_iterator **iterator)
{
    if (unlikely(!ptr_EnumerateContainerObjects))
        undefined("SDFEnumerateContainerObjects");

    return (*ptr_EnumerateContainerObjects)(sdf_thread_state,
                                            cguid,
                                            iterator);
}


/*
 * SDFNextEnumeratedObject
 */
SDF_status_t 
SDFNextEnumeratedObject(struct SDF_thread_state *sdf_thread_state,
                        struct SDF_iterator *iterator,
                        char **key,
                        uint32_t *keylen,
                        char **data,
                        uint64_t *datalen)
{
    if (unlikely(!ptr_NextEnumeratedObject))
        undefined("SDFNextEnumeratedObject");

    return (*ptr_NextEnumeratedObject)(sdf_thread_state,
                                       iterator,
                                       key,
                                       keylen,
                                       data,
                                       datalen);
}


/*
 * SDFFinishEnumeration
 */
SDF_status_t 
SDFFinishEnumeration(struct SDF_thread_state *sdf_thread_state,
                     struct SDF_iterator *iterator)
{
    if (unlikely(!ptr_FinishEnumeration))
        undefined("SDFFinishEnumeration");

    return (*ptr_FinishEnumeration)(sdf_thread_state, iterator);
}


/*
 * SDFGetStats
 */
SDF_status_t 
SDFGetStats(struct SDF_thread_state *sdf_thread_state, SDF_stats_t *stats)
{
    if (unlikely(!ptr_GetStats))
        undefined("SDFGetStats");

    return (*ptr_GetStats)(sdf_thread_state, stats);
}


/*
 * SDFGetContainerStats
 */
SDF_status_t 
SDFGetContainerStats(struct SDF_thread_state *sdf_thread_state,
                     SDF_cguid_t cguid,
                     SDF_container_stats_t *stats)
{
    if (unlikely(!ptr_GetContainerStats))
        undefined("SDFGetContainerStats");

    return (*ptr_GetContainerStats)(sdf_thread_state, cguid, stats);
}


/*
 * SDFStartContainer
 */
SDF_status_t 
SDFStartContainer(struct SDF_thread_state *sdf_thread_state,
                  SDF_cguid_t cguid)
{
    if (unlikely(!ptr_StartContainer))
        undefined("SDFStartContainer");

    return (*ptr_StartContainer)(sdf_thread_state, cguid);
}


/*
 * SDFStopContainer
 */
SDF_status_t 
SDFStopContainer(struct SDF_thread_state *sdf_thread_state, SDF_cguid_t cguid)
{
    if (unlikely(!ptr_StopContainer))
        undefined("SDFStopContainer");

    return (*ptr_StopContainer)(sdf_thread_state, cguid);
}


/*
 * SDFBackupContainer
 */
SDF_status_t 
SDFBackupContainer(struct SDF_thread_state *sdf_thread_state,
                   SDF_cguid_t cguid,
                   char *backup_directory)
{
    if (unlikely(!ptr_BackupContainer))
        undefined("SDFBackupContainer");

    return (*ptr_BackupContainer)(sdf_thread_state, cguid, backup_directory);
}


/*
 * SDFRestoreContainer
 */
SDF_status_t 
SDFRestoreContainer(struct SDF_thread_state *sdf_thread_state,
                    SDF_cguid_t cguid,
                    char *backup_directory)
{
    if (unlikely(!ptr_RestoreContainer))
        undefined("SDFRestoreContainer");

    return (*ptr_RestoreContainer)(sdf_thread_state, cguid, backup_directory);
}
