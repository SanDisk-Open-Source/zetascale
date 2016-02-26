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

#include "stats.h"

/* cc -o stats example.c ../../build/sdf/utils/stats.oo -lm */

statistics_t  read_stats;
statistics_t  write_stats;
statistics_t  total_stats;

int
main(void)
{
   int   i;
   char  c;
   int   fd;

   struct timeval  start_time;

   fd = open("/tmp/data", O_RDWR | O_CREAT, 0666);

   init_stats(&read_stats,  "Read");
   init_stats(&write_stats, "Write");
   init_stats(&total_stats, "Total");

   c = 'X';

   for (i = 0; i < 100; i++)
   {
      read_clock(&start_time);
      write(fd, &c, sizeof(c));
      record_op(&start_time, &write_stats);
   }

   lseek(fd, 0, SEEK_SET);

   for (i = 0; i < 100; i++)
   {
      read_clock(&start_time);
      read(fd, &c, sizeof(c));
      record_op(&start_time, &read_stats);
   }

   sum_stats(&total_stats, &read_stats);
   sum_stats(&total_stats, &write_stats);

   dump_stats(&read_stats);
   dump_stats(&write_stats);
   dump_stats(&total_stats);
   return 0;
}
