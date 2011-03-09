#undef  rt_enable_stats
#define rt_enable_stats
#include "stats2.h"

#ifdef rt_enable_stats
#define array_size(x) (sizeof(x) / sizeof((x)[0]))
#define min(a, b)     (((a) > (b)) ? (b) : (a))
#define max(a, b)     (((a) < (b)) ? (b) : (a))

static int
rt_local_log2(uint64_t value)
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
rt_init_stats(rt_statistics_t *statistics, char *name)
{
   memset(statistics, 0, sizeof(*statistics));

   statistics->name          = name;
   statistics->min_time      = (uint64_t) -1;
   statistics->max_time      = 0;
   statistics->log_product   = 0;
   statistics->log_zero      = 0;
   return;
}

void
rt_record_op(rt_time_t *start_time, rt_statistics_t *statistics)
{
   rt_time_t    area;
   rt_time_t *  end_time;
   uint64_t     total_time;

   end_time = &area;
   rt_read_clock(end_time);

#ifdef rt_use_gettimeofday
   if (end_time->tv_usec < start_time->tv_usec)
   {
      end_time->tv_sec--;
      end_time->tv_usec += rt_clock_res;
   }

   total_time  = end_time->tv_usec - start_time->tv_usec;
   total_time += (end_time->tv_sec - start_time->tv_sec) * rt_clock_res;
#else
   total_time  = *end_time - *start_time;
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

   statistics->dist[rt_local_log2(total_time)]++;
   statistics->squares += (double) total_time * total_time;

   if (statistics->log_zero == 0)
   {
      if (total_time != 0)
      {
         statistics->log_product += log((double) total_time);
      }
      else
      {
         statistics->log_zero++;
      }
   }

   return;
}

void
rt_sum_stats(rt_statistics_t *sum, rt_statistics_t *addend)
{
   int  i;

   sum->total_time  += addend->total_time;
   sum->squares     += addend->squares;
   sum->log_product += addend->log_product;
   sum->min_time     = min(sum->min_time, addend->min_time);
   sum->max_time     = max(sum->max_time, addend->max_time);
   sum->log_zero     = max(sum->log_zero, addend->log_zero);

   for (i = 0; i < array_size(sum->dist); i++)
   {
      sum->dist[i] += addend->dist[i];
   }

   return;
}

void
rt_dump_stats(rt_statistics_t *statistics)
{
   int      i;
   int      events;
   int      max_non_zero;
   int      min_non_zero;
   double   average;
   double   geometric_mean;

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

   printf("%s Statistics (%d events)\n", statistics->name, (int) events);

   if (events == 0)
   {
      return;
   }

   average = (double) statistics->total_time / events;

   printf("   Min:  %8lld us\n", (long long) statistics->min_time);
   printf("   Max:  %8lld us\n", (long long) statistics->max_time);
   printf("   Ave:  %8lld us\n", (long long) average);

   if (statistics->log_zero == 0)
   {
      geometric_mean = exp(statistics->log_product / events);
   }
   else
   {
      geometric_mean = 0;
   }

   printf("   Geo:  %8lf us\n", geometric_mean);

   if (events > 1)
   {
      printf("   Std:  %8lf us\n",
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

void
rt_record_value( uint64_t total_time, rt_statistics_t *statistics )
{
   statistics->total_time += total_time;

   if (total_time < statistics->min_time)
   {
      statistics->min_time = total_time;
   }

   if (total_time > statistics->max_time)
   {
      statistics->max_time = total_time;
   }
    
   statistics->dist[rt_local_log2(total_time)]++;
   statistics->squares += (double) total_time * total_time;

   if (statistics->log_zero == 0)
   {
      if (total_time != 0)
      {
         statistics->log_product += log((double) total_time);
      }
      else
      {
         statistics->log_zero++;
      }
   }

   return;
}
#endif
