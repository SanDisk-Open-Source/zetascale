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

#undef  wsrt_enable_stats
#define wsrt_enable_stats
#include "wsstats.h"

#ifdef wsrt_enable_stats
#define array_size(x) (sizeof(x) / sizeof((x)[0]))
#define min(a, b)     (((a) > (b)) ? (b) : (a))
#define max(a, b)     (((a) < (b)) ? (b) : (a))

static int
wsrt_local_log2(uint64_t value)
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
wsrt_init_stats(wsrt_statistics_t *statistics, char *name)
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
wsrt_record_op(wsrt_time_t *start_time, wsrt_statistics_t *statistics)
{
   wsrt_time_t    area;
   wsrt_time_t *  end_time;
   uint64_t     total_time;

   end_time = &area;
   wsrt_read_clock(end_time);

#ifdef wsrt_use_gettimeofday
   if (end_time->tv_usec < start_time->tv_usec)
   {
      end_time->tv_sec--;
      end_time->tv_usec += wsrt_clock_res;
   }

   total_time  = end_time->tv_usec - start_time->tv_usec;
   total_time += (end_time->tv_sec - start_time->tv_sec) * wsrt_clock_res;
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

   statistics->dist[wsrt_local_log2(total_time)]++;
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
wsrt_sum_stats(wsrt_statistics_t *sum, wsrt_statistics_t *addend)
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

int wsrt_dump_stats(FILE *f, wsrt_statistics_t *statistics, int dumpflag)
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

   if (dumpflag) {
       fprintf(f, "\n%s Statistics (%d events)\n", statistics->name, (int) events);
   }

   if (events == 0)
   {
      return(events);
   }


   if (dumpflag) {

       average = (double) statistics->total_time / events;

       fprintf(f, "   Min:  %8.4g\n", ((double) statistics->min_time));
       fprintf(f, "   Max:  %8.4g\n", ((double) statistics->max_time));
       fprintf(f, "   Ave:  %8.4g\n", (average));

       if (statistics->log_zero == 0)
       {
	  geometric_mean = exp(statistics->log_product / events);
       }
       else
       {
	  geometric_mean = 0;
       }

       fprintf(f, "   Geo:  %8.4g\n", geometric_mean);

       if (events > 1)
       {
	  fprintf(f, "   Std:  %8.4g\n",
	  sqrt((statistics->squares - events * average * average) / (events - 1)));
       }

       fprintf(f, "   Log2 Dist:");

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
	     fprintf(f, "\n      %2d:", (int) i);
	  }

	  fprintf(f, "   %6ld", statistics->dist[i]);
       }

       fprintf(f, "\n");
   }
   return(events);
}

void
wsrt_record_value( uint64_t total_time, wsrt_statistics_t *statistics )
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
    
   statistics->dist[wsrt_local_log2(total_time)]++;
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
