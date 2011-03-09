/*
 * File: sdf_lock.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Test out locks.
 */
#include <stdio.h>
#include <unistd.h>
#include "sdftcp/locks.h"
#include "sdftcp/stats.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_msg.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define CLINE_SIZE  64
#define STACK_SIZE  (128*1024)


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)


/*
 * Thread statistics.
 */
typedef struct {
    atom_t count;
    char   align[]
        __attribute__((aligned(CLINE_SIZE)));
} tstat_t;


/*
 * Configurable static variables.
 *
 *  NStats   - Statistics for null threads.
 *  NThreads - The number of null threads.
 *  RStats   - Statistics for read threads.
 *  RThreads - The number of read threads.
 *  WStats   - Statistics for write threads.
 *  WThreads - The number of write threads.
 *  TimeSecs - Length of time that we run.  If 0, we run forever.  But not yet
 *             used.
 *  TimeZero - Time that we started.
 */
static int      NThreads;
static int      RThreads;
static int      WThreads;
static tstat_t *NStats;
static tstat_t *RStats;
static tstat_t *WStats;
static ntime_t  TimeSecs;
static ntime_t  TimeZero;


/*
 * Static variables.
 */
rwlock_t *Lock;


/*
 * Function prototypes.
 */
static int      numarg(char *opt, char *arg, int min);
static void     test(void);
static void     usage(void);
static void     nthread(uint64_t arg);
static void     rthread(uint64_t arg);
static void     wthread(uint64_t arg);
static void     our_stats(stat_t *stat);
static void     parse(int *argcp, char ***argvp);
static void    *pth_main(void *arg);
static tstat_t *tstat_alloc(int n);


/*
 * Main.
 */
int
main(int argc, char *argv[])
{
    parse(&argc, &argv);
    agent_init(argc, argv);
    test();
    agent_exit();
    return 0;
}


/*
 * Parse arguments.
 */
static void
parse(int *argcp, char ***argvp)
{
    char **argr = (*argvp)+1;
    char **argw = argr;

    for (;;) {
        char *arg = *argr++;

        if (!arg)
            break;
        if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else if (streq(arg, "-nt") || streq(arg, "--nthreads"))
            NThreads = numarg(arg, *argr++, 0);
        else if (streq(arg, "-rt") || streq(arg, "--rthreads"))
            RThreads = numarg(arg, *argr++, 0);
        else if (streq(arg, "-wt") || streq(arg, "--wthreads"))
            WThreads = numarg(arg, *argr++, 0);
        else if (streq(arg, "-ts") || streq(arg, "--time_secs"))
            TimeSecs = NANO * numarg(arg, *argr++, 1);
        else
            *argw++ = arg;
    }
    *argw = NULL;
    *argcp = argw - *argvp;
}


/*
 * Return a numeric argument.
 */
static int
numarg(char *opt, char *arg, int min)
{
    int n;

    if (!arg)
        panic("%s requires an argument", opt);
    n = atoi(arg);
    if (n < min)
        panic("value to %s must be at least %d", opt, min);
    return n;
}


/*
 * Print out a usage message and exit.
 */
static void
usage(void)
{
    char *s =
        "Usage\n"
        "    sdf_lock Options\n"
        "Options\n"
        "    --help|-h\n"
        "        Print this message.\n"
        "    --nthreads N|-nt\n"
        "        Set number of null threads.\n"
        "    --rthreads N|-rt\n"
        "        Set number of read threads.\n"
        "    --wthreads N|-wt\n"
        "        Set number of write threads.\n"
        "    --time N|-ts\n"
        "        Make test last N seconds.\n";
    fputs(s, stderr);
    plat_exit(0);
}


/*
 * Start the test.
 */
static void
test(void)
{
    int i;
    pthread_t *pth;
    int n = NThreads + WThreads + RThreads;

    if (!n)
        panic("must set one of -nt, -rt or -wt");

    pth = malloc_q(n * sizeof(*pth));
    NStats = tstat_alloc(NThreads);
    RStats = tstat_alloc(RThreads);
    WStats = tstat_alloc(WThreads);
    Lock = rwl_init();
    fthInitMultiQ(1, n);
    sdf_msg_call_stat(our_stats);

    for (i = 0; i < NThreads; i++)
        XResume(fthSpawn(nthread, STACK_SIZE), i);
    for (i = 0; i < RThreads; i++)
        XResume(fthSpawn(rthread, STACK_SIZE), i);
    for (i = 0; i < WThreads; i++)
        XResume(fthSpawn(wthread, STACK_SIZE), i);

    TimeZero = msg_ntime();
    for (i = 0; i < n; i++)
        if (pthread_create(&pth[i], NULL, &pth_main, NULL) < 0)
            fatal("pthread_create failed");

    for (i = 0; i < n; i++)
        pthread_join(pth[i], NULL);
    rwl_free(Lock);
    free(RStats);
    free(WStats);
}


/*
 * Allocate space for thread statistics.
 */
static tstat_t *
tstat_alloc(int n)
{
    void *stats;
    int s = n * sizeof(tstat_t);

    if (posix_memalign((void **)&stats, CLINE_SIZE, s) != 0)
        panic("out of memory");
    memset(stats, 0, s);
    return stats;
}


/*
 * Start a fth scheduler.
 */
static void *
pth_main(void *arg)
{
    fthSchedulerPthread(0);
    return NULL;
}


/*
 * Do nothing.
 */
static void
nthread(uint64_t arg)
{
    int n = (uint64_t) arg;
    tstat_t *t = &NStats[n];

    use_all_cpus();
    for (;;) {
        t->count++;
    }
}


/*
 * Acquire the lock for reading.
 */
static void
rthread(uint64_t arg)
{
    int n = (uint64_t) arg;
    tstat_t *t = &RStats[n];

    use_all_cpus();
    for (;;) {
        rwl_lockr(Lock);
        t->count++;
        if (Lock->nr < 0)
            panic("r: %lx < 0", Lock->nr);
        rwl_unlockr(Lock);
    }
    fthKill(1);
}


/*
 * Acquire the lock for writing.
 */
static void
wthread(uint64_t arg)
{
    int n = (uint64_t) arg;
    tstat_t *t = &WStats[n];

    use_all_cpus();
    for (;;) {
        rwl_lockw(Lock);
        t->count++;
        if (Lock->nr >= 0)
            panic("w: %lx >= 0", Lock->nr);
        rwl_unlockw(Lock);
    }
}


/*
 * Show statistics.
 */
static void
our_stats(stat_t *stat)
{
    int i;
    atom_t l;
    char buf[16];
    double t = (msg_ntime() - TimeZero) / (double)NANO;

    for (i = 0; i < NThreads; i++) {
        l = NStats[i].count;
        snprintf(buf, sizeof(buf), "sdf_lock n%d", i);
        stat_labl(stat, buf);
        if (l)
            stat_time(stat, "", t/l);
        stat_long(stat, "n", l);
        stat_endl(stat);
    }
    for (i = 0; i < RThreads; i++) {
        l = RStats[i].count;
        snprintf(buf, sizeof(buf), "sdf_lock r%d", i);
        stat_labl(stat, buf);
        if (l)
            stat_time(stat, "", t/l);
        stat_long(stat, "n", l);
        stat_endl(stat);
    }
    for (i = 0; i < WThreads; i++) {
        l = WStats[i].count;
        snprintf(buf, sizeof(buf), "sdf_lock w%d", i);
        stat_labl(stat, buf);
        if (l)
            stat_time(stat, "", t/l);
        stat_long(stat, "n", l);
        stat_endl(stat);
    }
}
