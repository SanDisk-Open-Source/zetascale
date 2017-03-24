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

#define rt_use_gettimeofday
/* #define rt_use_rdtsc */

#if     defined(rt_enable_stats) && defined(rt_use_gettimeofday)
#define rt_clock_res        ((uint64_t)(1000 * 1000))
#define rt_read_clock(time) do { gettimeofday(time, NULL); } while (0)
#define rt_time_t           struct timeval

#elif   defined(rt_enable_stats) && defined(rt_use_rdtsc)
#define rt_clock_res        ((uint64_t) 3000 * 1000 * 1000)
#define rt_read_clock(time) do { *(time) = rt_rdtsc(); } while (0)
#define rt_time_t           uint64_t


static __inline__ uint64_t rt_rdtsc(void)
{
    uint32_t u, l;
    asm volatile("rdtsc" : "=a" (l), "=d" (u));
    return (((uint64_t) u << 32) | l);
}

#else
#define rt_time_t uint64_t
#define rt_read_clock(x)  { *(x) = 0; }

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
} rt_statistics_t;

#ifdef rt_enable_stats
void rt_init_stats(rt_statistics_t *statistics, char *name);
void rt_record_op(rt_time_t *start_time, rt_statistics_t *statistics);
void rt_record_value(uint64_t total_time, rt_statistics_t *statistics);
void rt_sum_stats(rt_statistics_t *sum, rt_statistics_t *addend);
void rt_dump_stats(rt_statistics_t *statistics);
#else
#define rt_init_stats(statistics, name)
#define rt_record_op(start_time, statistics)
#define rt_record_value(total_time, statistics)
#define rt_sum_stats(sum, addend)
#define rt_dump_stats(statistics)
#endif

#endif  /* __STATS2_H__ */
