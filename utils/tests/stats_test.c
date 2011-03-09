#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

//#define rt_enable_logging
#include "rtlog.h"
#define RECORD_STATS
#include "stats.h"

#define BLOCK_SIZE 32 * 1024

int main (int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "%s: missing file operand\n", argv[0]);
        return -EINVAL;
    }

    rt_init();

    statistics_t stats;
    init_stats(&stats, "Example Statistics");
    start_event(&stats);

    rt_log("start", 0);

    const char *path = argv[1];
    int fd = open(path, O_RDWR | O_CREAT | O_SYNC);
    if (fd == -1) { perror(argv[1]); return errno; }

    rt_log("opened", 0);

    char buf[BLOCK_SIZE];
    memset(buf, 'a', BLOCK_SIZE);
    ssize_t sz = write(fd, buf, BLOCK_SIZE);
    if (sz == -1) { perror(argv[1]); return errno; }

    rt_log("written", 0);

    int rc = ftruncate(fd, 4 * 1024);
    if (rc == -1) { perror(argv[1]); return errno; }

    rt_log("trucated to %d", 4 * 1024);

    rc = ftruncate(fd, 0);
    if (rc == -1) { perror(argv[1]); return errno; }

    rt_log("trucated to %d", 0);

    rc = close(fd);
    if (rc == -1) { perror(argv[1]); return errno; }

    rt_log("closed", 0);
    record_event(&stats);

    rt_dump();
    dump_stats(&stats);

    return 0;
}
