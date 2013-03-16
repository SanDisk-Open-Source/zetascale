
/*
 * File:   protocol_utils.h
 * Author: Brian O'Krafka
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: protocol_utils.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _PROTOCOL_UTILS_H
#define _PROTOCOL_UTILS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef char *PCHAR;
typedef PCHAR **PPCHAR;
typedef int BOOLEAN;

#define FALSE   0
#define TRUE    1

/*   If MALLOC_TRACE is defined, a detailed memory allocation/deallocation
 *   trace is generated for all of sdf/protocol.
 */
// #define MALLOC_TRACE

/*   If MALLOC_FAIL_TEST is defined, randomized malloc failures are
 *   generated to test for correct handling.
 */
// #define MALLOC_FAIL_TEST

#ifdef MALLOC_FAIL_TEST
   /* probability that malloc fails, in percent */
#define MALLOC_FAIL_PROB   5

#endif // MALLOC_FAIL_TEST

#ifdef MALLOC_TRACE

#define MALLOC_TRACE_DUMP_INTERVAL 10000

typedef enum {
    N_SHMEM_ALLOC = 0,
    N_SHMEM_FREE,
    N_LOCAL_ALLOC,
    N_LOCAL_FREE,
    SHMEM_ALLOC,
    SHMEM_FREE,
    LOCAL_ALLOC,
    LOCAL_FREE,
    N_UTMALLOC_STATS,
} UT_malloc_stats_t;

    /* these MUST be kept in sync with above enums! */
#ifndef _PROTOCOL_UTILS_C
    extern char *UTMallocTraceStrings[];
#else
    char *UTMallocTraceStrings[] = {
        "N_SHMEM_ALLOC",
        "N_SHMEM_FREE",
        "N_LOCAL_ALLOC",
        "N_LOCAL_FREE",
        "SHMEM_ALLOC",
        "SHMEM_FREE",
        "LOCAL_ALLOC",
        "LOCAL_FREE",
    };
#endif

typedef struct {
    BOOLEAN      init_done;
    long long    ncalls;
    fthWaitEl_t *wait;
    fthLock_t    lock;
    char        *fname;
    FILE        *ftrace;
    long long    stats[N_UTMALLOC_STATS];
} UT_malloc_data_t;

extern UT_malloc_data_t  UTMallocData;

extern void UTMallocClearStats();
extern void UTMallocTraceInit();
extern void UTMallocTrace(char *tag, int is_init, int is_free, int is_shmem, void *ptr, size_t size);
extern void UTMallocStats(FILE *f);

#endif // MALLOC_TRACE


#define MAKEARRAY(p, t, n) {\
   (p) = proto_plat_alloc_arena((n)*sizeof(t), NonCacheObjectArena);\
   if ((p) == NULL) {\
       plat_log_msg(21081, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, "plat_alloc failed!");\
       plat_abort();\
   }\
}

#define MYFREE(p) {\
   plat_free(p);\
}

char *flashRetCodeName(int ret);
void UTStartDebugger(const char *sprog);
char *UTCopyString(char *s);
void UTFreeString(char *s);
void UTError(char *fmt, ...) __attribute__((__noreturn__));
// void UTWarning(char *fmt, ...);
// void UTMessage(char *fmt, ...);

#define UTMessage(id, format, args...)                  \
   plat_log_msg(id, PLAT_LOG_CAT_SDF_PROT,              \
                PLAT_LOG_LEVEL_TRACE, format, ##args);

#define UTWarning(id, format, args...)                  \
   plat_log_msg(id, PLAT_LOG_CAT_SDF_PROT,              \
                PLAT_LOG_LEVEL_TRACE, "Warning: " format, ##args);

#ifdef	__cplusplus
}
#endif

#endif	/* _PROTOCOL_UTILS_H */

