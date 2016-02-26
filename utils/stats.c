/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#define RECORD_STATS
#include "stats.h"

#define array_size(x) (sizeof(x) / sizeof((x)[0]))
#define min(a, b)     (((a) > (b)) ? (b) : (a))
#define max(a, b)     (((a) < (b)) ? (b) : (a))

static int
local_log2(uint64_t value)
{
   int  result;

   result = 0;

   while (result <= 63 && ((uint64_t) 1 << result) < value)
   {
       result++;
   }

   return result;
}

void
init_stats(statistics_t *statistics, char *name)
{
   memset(statistics, 0, sizeof(*statistics));

   statistics->name          = name;
   statistics->min_time      = (uint64_t) -1;
   statistics->max_time      = 0;
   statistics->log_product   = 0;
#if defined(STATS_USE_GETTIMEOFDAY)
   statistics->start_time.tv_sec = 0;
   statistics->start_time.tv_usec = 0;
#elif defined(STATS_USE_TSC)
   statistics->start_time = 0;
#endif
   return;
}

void
start_event(statistics_t *statistics)
{
#if defined(STATS_USE_GETTIMEOFDAY)
    gettimeofday(&statistics->start_time, NULL);
#elif defined(STATS_USE_TSC)
    statistics->start_time = rdtsc() / CLOCK_RESOLUTION;
#endif
}

void
record_event(statistics_t *statistics)
{
   uint64_t          total_time;
#if defined(STATS_USE_GETTIMEOFDAY)
   struct timeval    area;
   struct timeval *  end_time;

   end_time = &area;
   gettimeofday(end_time, NULL);

   if (end_time->tv_usec < statistics->start_time->tv_usec)
   {
      end_time->tv_sec--;
      end_time->tv_usec += CLOCK_RESOLUTION;
   }

   total_time  = end_time->tv_usec - statistics->start_time->tv_usec;
   total_time += (end_time->tv_sec - statistics->start_time->tv_sec) * CLOCK_RESOLUTION;

   statistics->start_time = *end_time;
#elif defined(STATS_USE_TSC)
   uint64_t end_time = rdtsc() / CLOCK_RESOLUTION;
   total_time = rdtsc() - statistics->start_time;
   statistics->start_time = end_time;
#endif

   statistics->total_time += total_time;

   if (total_time < statistics->min_time)
   {
      statistics->min_time = total_time;
   }

   if (total_time > statistics->max_time)
   {
      statistics->max_time = total_time;
   }

   statistics->dist[local_log2(total_time)]++;
   statistics->squares += (double) total_time * total_time;

   if (total_time != 0)
   {
      statistics->log_product += log((double) total_time);
   }

   return;
}

void
sum_stats(statistics_t *sum, statistics_t *addend)
{
   int  i;

   sum->total_time  += addend->total_time;
   sum->squares     += addend->squares;
   sum->log_product += addend->log_product;
   sum->min_time     = min(sum->min_time, addend->min_time);
   sum->max_time     = max(sum->max_time, addend->max_time);

   for (i = 0; i < array_size(sum->dist); i++)
   {
      sum->dist[i] += addend->dist[i];
   }

   return;
}

void
dump_stats(statistics_t *statistics)
{
   int      i;
   int      events;
   int      max_non_zero;
   int      min_non_zero;
   double   average;

   events = 0;
   max_non_zero = 0;

   for (i = 0; i < array_size(statistics->dist); i++)
   {
      events += statistics->dist[i];

      if (statistics->dist[i] != 0)
      {
         max_non_zero = i;
      }
   }

   average = (double) statistics->total_time / events;

   printf("%s Statistics (%d events)\n", statistics->name, (int) events);

   if (events == 0)
   {
      return;
   }

   printf("   Min:  %8lld\n", (long long) statistics->min_time);
   printf("   Max:  %8lld\n", (long long) statistics->max_time);
   printf("   Ave:  %8lld\n",
      (long long) statistics->total_time / events);
   printf("   Geo:  %8lf\n",
      exp(statistics->log_product / events));

   if (events > 1)
   {
      printf("   Std:  %8lf\n",
      sqrt((statistics->squares - events * average * average) / (events - 1)));
   }

   printf("   Log2 Dist:");

   min_non_zero = 0;

   for (i = 0; i <= max_non_zero - 4; i += 4)
   {
      if
      (
         statistics->dist[i + 0] != 0
      || statistics->dist[i + 1] != 0
      || statistics->dist[i + 2] != 0
      || statistics->dist[i + 3] != 0
      )
      {
         min_non_zero = i;
         break;
      }
   }

   for (i = min_non_zero; i <= max_non_zero; i++)
   {
      if ((i % 4) == 0)
      {
         printf("\n      %2d:", (int) i);
      }

      printf("   %6ld", statistics->dist[i]);
   }

   printf("\n");
   return;
}
