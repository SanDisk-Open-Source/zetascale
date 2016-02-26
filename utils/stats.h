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

/*
 *  Sample Code:
 *
 *   statistics_t stats;
 *   init_stats(&stats, "Write Statistics");
 *   for (i = 0; i < 100; i++)
 *   {
 *      start_event(&stats);
 *      write(fd, &c, sizeof(c));
 *      record_event(&stats);
 *   }
 *   dump_stats(&stats);
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#define STATS_USE_TSC
#define CLOCK_RESOLUTION 10000

typedef struct {
   char *     name;
   uint64_t   total_time;
   uint64_t   min_time;
   uint64_t   max_time;
   uint64_t   dist[65];
   double     squares;
   double     log_product;
#if defined(STATS_USE_GETTIMEOFDAY)
   struct timeval start_time; 
#elif defined(STATS_USE_TSC)
   uint64_t start_time;
#else
#error
#endif
} statistics_t;

#ifndef RECORD_STATS
#define init_stats(...)
#define start_event(...)
#define record_event(...)
#define sum_stats(...)
#define dump_stats(...)
#else //RECORD_STATS
void init_stats(statistics_t *statistics, char *name);
void start_event(statistics_t *statistics);
void record_event(statistics_t *statistics);
void sum_stats(statistics_t *sum, statistics_t *addend);
void dump_stats(statistics_t *statistics);
#endif//RECORD_STATS

static __inline__ uint64_t rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u));
    return (((uint64_t) u << 32) | l);
}

