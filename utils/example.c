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
