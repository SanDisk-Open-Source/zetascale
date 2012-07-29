/*
 * File:   protocol_utils.c
 * Author: Brian O'Krafka
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: protocol_utils.c 802 2008-03-29 00:44:48Z darpan $
 */

#define _PROTOCOL_UTILS_C

#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdarg.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>

#include "fth/fth.h"
#include "platform/stdlib.h"
#include "platform/logging.h"
#include "platform/platform.h"
#include "flash/flash.h"
#include "protocol_utils.h"
#include "protocol_alloc.h"

#ifdef MALLOC_TRACE
UT_malloc_data_t  UTMallocData;
#endif // MALLOC_TRACE

  // Arena settings for protocol code memory allocations
enum plat_shmem_arena NonCacheObjectArena = PLAT_SHMEM_ARENA_CACHE;
enum plat_shmem_arena CacheObjectArena    = PLAT_SHMEM_ARENA_CACHE;

char *flashRetCodeName(int ret)
{
    switch (ret) {    
	case FLASH_EOK:           return("EOK");          break;
	case FLASH_EPERM:         return("EPERM");        break;
	case FLASH_ENOENT:        return("ENOENT");       break;
	case FLASH_EDATASIZE:     return("EDATASIZE");    break;
	case FLASH_EAGAIN:        return("EAGAIN");       break;
	case FLASH_ENOMEM:        return("ENOMEM");       break;
	case FLASH_EBUSY:         return("EBUSY");        break;
	case FLASH_EEXIST:        return("EEXIST");       break;
	case FLASH_EACCES:        return("EACCES");       break;
	case FLASH_EINVAL:        return("EINVAL");       break;
	case FLASH_EMFILE:        return("EMFILE");       break;
	case FLASH_ENOSPC:        return("ENOSPC");       break;
	case FLASH_ENOBUFS:       return("ENOBUFS");      break;
	case FLASH_EDQUOT:        return("EDQUOT");       break;
	case FLASH_ESTALE:        return("ESTALE");       break;
	case FLASH_EINCONS:       return("EINCONS");      break;
	case FLASH_ESTOPPED:      return("ESTOPPED");     break;
	case FLASH_EBADCTNR:      return("EBADCTNR");     break;
	case FLASH_RMT_EBADCTNR:  return("RMT_EBADCTNR"); break;
	case FLASH_EDELFAIL:      return("EDELFAIL");     break;
	case FLASH_RMT_EDELFAIL:  return("RMT_EDELFAIL"); break;
	default:                  return("????");         break;
    }
    return("????");
}


void UTStartDebugger(const char *sprog)
{
    char   stmp[1024];
    pid_t  pid;

    pid = getpid();
    /* sprintf(stmp, "xterm -geometry 100x65 -e bash -c 'gdb %s %d' &", sprog, pid); */
    snprintf(stmp, 1024, "xterm -geometry 100x65 -e bash -c '/usr/bin/gdb %s %d' &", sprog, pid);
    fprintf(stderr, "Invoking debugger: '%s'\n", stmp);
    system(stmp);
    system("sleep 2");
    // sleep(2);
}

char *UTCopyString(char *sin)
{
   char *s;
   int   len;

   len = strlen(sin);
   MAKEARRAY(s, char, (len+1));
   (void) strcpy(s, sin);
   return(s);
}

void UTFreeString(char *s)
{
   MYFREE(s);
}

void UTError(char *fmt, ...)
{
   char     stmp[512];
   va_list  args;

   va_start(args, fmt);

   vsprintf(stmp, fmt, args);
   strcat(stmp, "\n");

   va_end(args);

#ifdef notdef
   (void) fprintf(stderr, "%s", stmp);
#endif

   plat_log_msg(20819, PLAT_LOG_CAT_SDF_PROT, 
                PLAT_LOG_LEVEL_FATAL, "%s", stmp);

#if 0
   UTStartDebugger(plat_get_exe_path());
#endif

   plat_abort();
}

#ifdef notdef
/* these have been replaced by macros that call plat_log_msg */
void UTWarning(int id, char *fmt, ...)
{
   char     stmp[512];
   va_list  args;

   va_start(args, fmt);

   vsprintf(stmp, fmt, args);
   strcat(stmp, "\n");

   va_end(args);

#ifdef notdef
   (void) fprintf(stderr, "%s", stmp);
#endif

   plat_log_msg(id, PLAT_LOG_CAT_SDF_PROT,
                PLAT_LOG_LEVEL_INFO, "%s", stmp);
}

void UTMessage(int id, char *fmt, ...)
{
   char     stmp[512];
   va_list  args;

   va_start(args, fmt);

   vsprintf(stmp, fmt, args);
   strcat(stmp, "\n");

   va_end(args);

#ifdef notdef
   (void) fprintf(stderr, "%s", stmp);
#endif

   plat_log_msg(id, PLAT_LOG_CAT_SDF_PROT,
                PLAT_LOG_LEVEL_DEBUG, "%s", stmp);
}
#endif /* notdef */

#ifdef MALLOC_TRACE

void UTMallocTraceInit()
{
    int   i;

    UTMallocData.init_done = FALSE;
    UTMallocData.ncalls    = 0;
    fthLockInit(&(UTMallocData.lock));
    UTMallocData.fname  = "UTMALLOC_DUMP.txt";
    UTMallocData.ftrace = fopen(UTMallocData.fname, "w");
    if (UTMallocData.ftrace == NULL) {
        plat_log_msg(21342, PLAT_LOG_CAT_SDF_PROT, \
		     PLAT_LOG_LEVEL_FATAL, "Could not open UTMalloc trace file");
	plat_abort();
    }
    setbuf(UTMallocData.ftrace, NULL);
    plat_log_msg(21343, PLAT_LOG_CAT_SDF_PROT, \
		 PLAT_LOG_LEVEL_INFO, "Opened UTMalloc trace file: %s", UTMallocData.fname);
    for (i=0; i<N_UTMALLOC_STATS; i++) {
	UTMallocData.stats[i] = 0;
    }
}

void UTMallocClearStats()
{
    int   i;

    UTMallocData.wait = fthLock(&(UTMallocData.lock), TRUE, NULL);
    (void) fprintf(UTMallocData.ftrace, "clear\n");
    for (i=0; i<N_UTMALLOC_STATS; i++) {
	UTMallocData.stats[i] = 0;
    }
    fthUnlock(UTMallocData.wait);
}

void UTMallocTrace(char *tag, int is_init, int is_free, int is_shmem, void *ptr, size_t size)
{
    (UTMallocData.ncalls)++;
    UTMallocData.wait = fthLock(&(UTMallocData.lock), TRUE, NULL);

    if (!is_init) {
        if (!UTMallocData.init_done) {
	    UTMallocData.init_done = TRUE;
	    (void) fprintf(UTMallocData.ftrace, "Init done\n");
	    UTMallocStats(stderr);
	}
    }

    if ((UTMallocData.ncalls % MALLOC_TRACE_DUMP_INTERVAL) == 0) {
	    (void) fprintf(UTMallocData.ftrace, "-----  %lld Calls  -----\n", UTMallocData.ncalls);
	    UTMallocStats(stderr);
    }

    if (is_shmem) {
        if (is_free) {
	    (void) fprintf(UTMallocData.ftrace, "Sfree %p %6lld %s\n", ptr, (long long) size, tag);
	    UTMallocData.stats[N_SHMEM_FREE]++;
	    UTMallocData.stats[SHMEM_FREE] += size;
	} else {
	    (void) fprintf(UTMallocData.ftrace, "Salloc %p %6lld %s\n", ptr, (long long) size, tag);
	    UTMallocData.stats[N_SHMEM_ALLOC]++;
	    UTMallocData.stats[SHMEM_ALLOC] += size;
	}
    } else {
        if (is_free) {
	    (void) fprintf(UTMallocData.ftrace, "Lfree %p %6lld %s\n", ptr, (long long) size, tag);
	    UTMallocData.stats[N_LOCAL_FREE]++;
	    UTMallocData.stats[LOCAL_FREE] += size;
	} else {
	    (void) fprintf(UTMallocData.ftrace, "Lalloc %p %6lld %s\n", ptr, (long long) size, tag);
	    UTMallocData.stats[N_LOCAL_ALLOC]++;
	    UTMallocData.stats[LOCAL_ALLOC] += size;
	}
    }
    fthUnlock(UTMallocData.wait);
}

void UTMallocStats(FILE *f)
{
    int i;
    struct plat_shmem_alloc_stats  shmem_stats;

    for (i=0; i<N_UTMALLOC_STATS; i++) {
	(void) fprintf(f, "%-30s: %lld\n", UTMallocTraceStrings[i], UTMallocData.stats[i]);
	(void) fprintf(UTMallocData.ftrace, "%-30s: %lld\n", UTMallocTraceStrings[i], UTMallocData.stats[i]);
    }

    plat_shmem_alloc_get_stats(&shmem_stats);

    (void) fprintf(UTMallocData.ftrace, "%-30s: %"PRIi64"\n", "allocated_count", shmem_stats.allocated_count);
    (void) fprintf(UTMallocData.ftrace, "%-30s: %"PRIi64"\n", "allocated_bytes", shmem_stats.allocated_bytes);
    (void) fprintf(UTMallocData.ftrace, "%-30s: %"PRIi64"\n", "used_bytes", shmem_stats.used_bytes);
    (void) fprintf(UTMallocData.ftrace, "%-30s: %"PRIu64"\n", "unusable_bytes", shmem_stats.unusable_bytes);
    (void) fprintf(UTMallocData.ftrace, "%-30s: %"PRIu64"\n", "total_bytes", shmem_stats.total_bytes);
}

#endif // MALLOC_TRACE

