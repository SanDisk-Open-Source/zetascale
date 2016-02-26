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

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fth/fth.h>
#include <sys/time.h>

#undef  rt_enable_logging
#define rt_enable_logging
#include "rtlog.h"

#define null  ((void *) 0)

#if   defined(use_rt_clock)
#define clock_get(a)  clock_gettime(CLOCK_REALTIME, a)
#define clock_s       timespec
#define clock_sec     tv_sec
#define clock_subsec  tv_nsec

#elif defined(use_gettimeofday)
#define clock_get(a)  gettimeofday(a, null)
#define clock_s       timeval
#define clock_sec     tv_sec
#define clock_subsec  tv_usec

#else /* use rdtsc */
#define use_rdtsc

struct clock_s
{
   uint64_t tv_ticks;
};

#define clock_get(a)  do { asm volatile ("mfence"); (a)->tv_ticks = rt_rdtsc(); } while (0)
#endif /* use_rdtsc */

typedef struct
{
   struct clock_s time;
   const char *   format;
   uint64_t       arg;
   void *         fth;
} entry_t;

typedef struct log_s log_t;

struct log_s
{
   entry_t *  rt_buffer;
   int        rt_limit;
   int        rt_index;
   log_t *    rt_next;
   pthread_t  rt_thread;
};

static __thread log_t   rt_log_area;

static int                rt_size   = 8192;
static pthread_mutex_t    rt_mutex  = PTHREAD_MUTEX_INITIALIZER;
static log_t *            rt_list   = null;

/*
 *  Define the area for the time stamp.  Reserve space around it so it's on
 *  its own cache line (most likely).
 */

#ifdef use_rdtsc
static volatile uint64_t  rt_stamp_area[32] =  { 0 };
#define rt_stamp rt_stamp_area[16]
#endif

static __inline__ uint64_t rt_rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u));
    return (((uint64_t) u << 32) | l);
}

static __inline__ uint64_t rt_rdtsc_sync(void)
{
    uint32_t u, l;
    asm volatile("xorl %%eax, %%eax; cpuid; rdtsc" : "=a" (l), "=d" (u) : : "%ebx", "%ecx");
    return (((uint64_t) u << 32) | l);
}

#ifdef use_rdtsc

static void *
rt_clocker(void *arg)
{
   cpu_set_t   mask;

   CPU_ZERO(&mask);
   CPU_SET(2, &mask);
   sched_setaffinity(0, 1, &mask);

   for (;;)
   {
      rt_stamp = rt_rdtsc();
   }

   return null;
}

static void
rt_init_stamp(void)
{
   pthread_t        thread;
   struct timespec  sleep_time;

   if (rt_stamp != 0)
   {
      return;
   }

   pthread_mutex_lock(&rt_mutex);

   if (rt_stamp == 0)
   {
      pthread_create(&thread, null, rt_clocker, null);
   }

   sleep_time.tv_sec  = 0;
   sleep_time.tv_nsec = 1000 * 1000 * 1000;

   do
   {
       nanosleep(&sleep_time, null);
   } while (rt_stamp == 0);

   pthread_mutex_unlock(&rt_mutex);
}

#endif /* use_rdtsc */

void
rtlog(const char *format, uint64_t arg)
{
   entry_t *  entry;

   if (rt_log_area.rt_buffer == null)
   {
      rt_init();
#ifdef use_rdtsc
      rt_init_stamp();
#endif
   }

   entry = &rt_log_area.rt_buffer[rt_log_area.rt_index++];

   if (rt_log_area.rt_index >= rt_log_area.rt_limit)
   {
      rt_log_area.rt_index = 0;
   }

   clock_get(&entry->time);
   entry->format = format;
   entry->arg    = arg;
   entry->fth    = fthId();
   return;
}

void
rt_init(void)
{
   int buffer_size;

   buffer_size = rt_size * sizeof(*rt_log_area.rt_buffer);

   rt_log_area.rt_index  = 0;
   rt_log_area.rt_limit  = rt_size;
   // rt_log_area.rt_buffer = malloc(buffer_size);
   rt_log_area.rt_buffer = plat_alloc(buffer_size);
   rt_log_area.rt_thread = pthread_self();

   memset(rt_log_area.rt_buffer, 0, buffer_size);

   pthread_mutex_lock(&rt_mutex);
   rt_log_area.rt_next = rt_list;
   rt_list = &rt_log_area;
   pthread_mutex_unlock(&rt_mutex);
   return;
}

void
rt_dump(void)
{
   log_t *    log;
   int        index;
   entry_t *  entry;
   char       path[] = "/tmp/rtlog_XXXXXX";
   int        fd;
   FILE *     log_file;

   log = rt_list;
   fd = mkstemp(path);

   if (fd < 0)
   {
      fprintf(stderr, "rt_dump:  no log file (mkstemp)\n");
      plat_abort();
   }

   log_file = fdopen(fd, "w");

   if (log_file == null)
   {
      fprintf(stderr, "rt_dump:  no log file (fdopen)\n");
      plat_abort();
   }

   fprintf(stderr, "rtlog:  Using %s\n", path);

   while (log != null)
   {
      index = log->rt_index;

      do
      {
         entry = &log->rt_buffer[index];

#if defined(use_rt_clock) || defined(use_gettimeofday)
         if (entry->format != null)
         {
            fprintf(log_file, "%8ld%06ld 0x%08lx 0x%08lx 0x%llx %-20s\n",
               (long)               entry->time.clock_sec,
               (long)               entry->time.clock_subsec,
               (long)               log->rt_thread,
               (long)               entry->fth,
               (unsigned long long) entry->arg,
                                    entry->format);
         }
#else
         if (entry->format != null)
         {
            fprintf(log_file, "%8ld 0x%08lx 0x%lx 0x%llx %-20s\n",
               (long)               entry->time.tv_ticks,
               (long)               log->rt_thread,
               (long)               entry->fth,
               (unsigned long long) entry->arg,
                                    entry->format);
         }
#endif

         index++;

         if (index >= log->rt_limit)
         {
            index = 0;
         }
      } while (index != log->rt_index);

      log = log->rt_next;
   }

   fclose(log_file);
   return;
}

void
rt_set_size(int size)
{
   rt_size = size;
   return;
}
