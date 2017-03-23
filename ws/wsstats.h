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


#ifndef __STATS2_H__
#define __STATS2_H__

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

/*
 *  Sample Code:
 *
 *   for (i = 0; i < 100; i++)
 *   {
 *      read_clock(&start_time);
 *      write(fd, &c, sizeof(c));
 *      record_op(&start_time, &write_stats);
 *   }
 */

// #define wsrt_use_gettimeofday
#define wsrt_use_rdtsc
#define wsrt_enable_stats

#if     defined(wsrt_enable_stats) && defined(wsrt_use_gettimeofday)
#define wsrt_clock_res        ((uint64_t)(1000 * 1000))
#define wsrt_read_clock(time) do { gettimeofday(time, NULL); } while (0)
#define wsrt_time_t           struct timeval

#elif   defined(wsrt_enable_stats) && defined(wsrt_use_rdtsc)
#define wsrt_clock_res        ((uint64_t) 3000 * 1000 * 1000)
#define wsrt_read_clock(time) do { *(time) = wsrt_rdtsc(); } while (0)
#define wsrt_time_t           uint64_t


static __inline__ uint64_t wsrt_rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u));
    return (((uint64_t) u << 32) | l);
}

#else
#define wsrt_time_t uint64_t
#define wsrt_read_clock(x)  { *(x) = 0; }

#endif

typedef struct {
   char *     name;
   int        log_zero;
   uint64_t   total_time;
   uint64_t   min_time;
   uint64_t   max_time;
   uint64_t   dist[65];
   double     squares;
   double     log_product;
} wsrt_statistics_t;

#ifdef wsrt_enable_stats
void wsrt_init_stats(wsrt_statistics_t *statistics, char *name);
void wsrt_record_op(wsrt_time_t *start_time, wsrt_statistics_t *statistics);
void wsrt_record_value(uint64_t total_time, wsrt_statistics_t *statistics);
void wsrt_sum_stats(wsrt_statistics_t *sum, wsrt_statistics_t *addend);
int  wsrt_dump_stats(FILE *f, wsrt_statistics_t *statistics, int dumpflag);
#else
#define wsrt_init_stats(statistics, name)
#define wsrt_record_op(start_time, statistics)
#define wsrt_record_value(total_time, statistics)
#define wsrt_sum_stats(sum, addend)
#define wsrt_dump_stats(FILE *f, statistics)
#endif

#endif  /* __STATS2_H__ */
