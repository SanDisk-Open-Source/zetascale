/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: stats.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "stats.h"
#include "tools.h"
#include "msg_cat.h"
#include "sdfmsg/sdf_msg.h"
#include "platform/alloc.h"


/*
 * Configurable parameters.
 */
#define TSTOP 24
#define LSIZE 80
#define ROUND 0.5


/*
 * Cpu time indices.
 */
enum {
    T_USER,
    T_NICE,
    T_KERNEL,
    T_IDLE,
    T_IOWAIT,
    T_IRQ,
    T_SOFTIRQ,
    T_STEAL,
    T_GUEST,
    T_N
};


/*
 * Function prototypes.
 */
static void    end(stat_t *stat);
static void    fit(stat_t *stat);
static void    new(stat_t *stat);
static void    our_stats(stat_t *stat);
static void    show_cpus(stat_t *stat);
static void    p3(xstr_t *xstr, double d);
static void    tab(stat_t *stat, char *name);
static void    long_abbr(xstr_t *xstr, int64_t l);
static void    long_full(xstr_t *xstr, int64_t l);
static int     get_ticks_all(clock_t *ticks);
static int     get_ticks_one(FILE *fp, int cpu, clock_t ticks[T_N]);
static clock_t cpu_total(clock_t *new, clock_t *old);


/*
 * Static variables.
 */
static int      NoCpus;
static int      TicksPS;
static ntime_t  OrigTime;
static ntime_t  LastTime;
static int     *CpuUsed;
static clock_t *OrigTicks;
static clock_t *LastTicks;
static clock_t *CurrTicks;


/*
 * Global variables.
 */
uint64_t StatBits;


/*
 * Initialise the statistics system.
 */
void
stat_init(void)
{
    int n;

    NoCpus = sysconf(_SC_NPROCESSORS_ONLN);
    TicksPS = sysconf(_SC_CLK_TCK);
    CpuUsed = m_malloc(NoCpus * sizeof(int), "stats:N*int");
    n = NoCpus * T_N * sizeof(clock_t);
    OrigTicks = m_malloc(n, "stats:N*clock_t");
    LastTicks = m_malloc(n, "stats:N*clock_t");
    CurrTicks = m_malloc(n, "stats:N*clock_t");
    if (!get_ticks_all(OrigTicks))
        return;
    memcpy(LastTicks, OrigTicks, n);
    OrigTime = msg_ntime();
    LastTime = OrigTime;
    sdf_msg_call_stat(our_stats);
}


/*
 * Exit the statistics system.
 */
void
stat_exit(void)
{
    m_free(CpuUsed);
    m_free(OrigTicks);
    m_free(LastTicks);
    m_free(CurrTicks);
}


/*
 * Initialize a statistics structure.
 */
void
stat_make(stat_t *stat)
{
    clear(*stat);
    xsinit(&stat->xstr);
}


/*
 * Free a statistics structure.
 */
void
stat_free(stat_t *stat)
{
    xsfree(&stat->xstr);
}


/*
 * Show our statistics.
 */
static void
our_stats(stat_t *stat)
{
    show_cpus(stat);
}


/*
 * Show CPU usage.
 */
static void
show_cpus(stat_t *stat)
{
    int i;
    ntime_t now;
    double origmul;
    double lastmul;
    int sumorig = 0;
    int sumlast = 0;

    if (!OrigTime)
        return;
    if (!get_ticks_all(CurrTicks))
        return;

    now = msg_ntime();
    origmul = 1.0 / TicksPS / ((double)(now-OrigTime)/NANO) * 100;
    lastmul = 1.0 / TicksPS / ((double)(now-LastTime)/NANO) * 100;
    for (i = 0; i < NoCpus; i++) {
        int used = cpu_total(&CurrTicks[i*T_N], &LastTicks[i*T_N]) * lastmul;
        sumlast += used;
        CpuUsed[i] = used;
        sumorig += cpu_total(&CurrTicks[i*T_N], &OrigTicks[i*T_N]) * origmul;
    }

    stat_labl(stat, "time");
    stat_time(stat, NULL, (double)(now-LastTime)/NANO);
    stat_time(stat, "cum", (double)(now-OrigTime)/NANO);
    stat_endl(stat);

    stat_labl(stat, "cpu usage");
    stat_long(stat, NULL, sumlast);
    stat_long(stat, "cum", sumorig);
    stat_endl(stat);

    stat_labl(stat, "cpu breakdown");
    for (i = 0; i < NoCpus; i++) {
        char buf[16];

        snprintf(buf, sizeof(buf), "p%d", i);
        stat_long(stat, buf, CpuUsed[i]);
    }
    stat_endl(stat);

    LastTime = now;
    memcpy(LastTicks, CurrTicks, NoCpus * T_N * sizeof(clock_t));
}


/*
 * Get the breakdown of CPU times for all CPUs.
 */
static int
get_ticks_all(clock_t *ticks)
{
    int i;
    int s = 0;
    FILE *fp = fopen("/proc/stat", "r");

    if (!fp)
        goto err;
    if (!get_ticks_one(fp, 0, ticks))
        goto err;
    for (i = 0; i < NoCpus; i++)
        if (!get_ticks_one(fp, i, &ticks[i*T_N]))
            goto err;
    s = 1;

err:
    if (fp)
        fclose(fp);
    return s;
}


/*
 * Get the breakdown of CPU times for a single CPU.
 */
static int
get_ticks_one(FILE *fp, int cpu, clock_t ticks[T_N])
{
    int n;
    char *p;
    char buf[256];

    if (!fgets(buf, sizeof(buf), fp))
        return 0;
    n = strlen(buf);
    if (!n || buf[--n] != '\n')
        return 0;
    buf[n] = '\0';
    if (strncmp(buf, "cpu", 3))
        return 0;
    p = &buf[3];
    if (*p != ' ') {
        n = strtoll(p, &p, 10);
        if (n != cpu)
            return 0;
    }
    for (n = 0; n < T_N; ++n)
        ticks[n] = strtoll(p, &p, 10);
    return 1;
}


/*
 * Total the amount of CPU time that is really being used.
 */
static clock_t
cpu_total(clock_t *new, clock_t *old)
{
    clock_t cpu;

    cpu = new[T_USER]    - old[T_USER]
        + new[T_NICE]    - old[T_NICE]
        + new[T_KERNEL]  - old[T_KERNEL]
        + new[T_IOWAIT]  - old[T_IOWAIT]
        + new[T_IRQ]     - old[T_IRQ]
        + new[T_SOFTIRQ] - old[T_SOFTIRQ];
    return cpu;
}


/*
 * Show a label.
 */
void
stat_labl(stat_t *stat, char *name)
{
    new(stat);
    xsprint(&stat->xstr, "%s", name);
}


/*
 * Show a label designated by a node.
 */
void
stat_labn(stat_t *stat, char *name, int rank)
{
    new(stat);
    xsprint(&stat->xstr, "n%d: %s", rank, name);
}


/*
 * End a line.
 */
void
stat_endl(stat_t *stat)
{
    end(stat);
}


/*
 * Show a number.
 */
void
stat_long(stat_t *stat, char *name, int64_t l)
{
    xstr_t *xp = &stat->xstr;

    if (!l)
        return;

    tab(stat, name);
    if (s_on(FULL))
        long_full(xp, l);
    else
        long_abbr(xp, l);
    fit(stat);
}


/*
 * Show a number in full precision.
 */
void
stat_full(stat_t *stat, char *name, int64_t l)
{
    xstr_t *xp = &stat->xstr;

    if (!l)
        return;

    tab(stat, name);
    long_full(xp, l);
    fit(stat);
}


/*
 * Show a number in full precision inserting commas as appropriate.
 */
static void
long_full(xstr_t *xstr, int64_t l)
{
    if (l < 0) {
        l = -l;
        xsprint(xstr, "-");
    }

    if (l < 1000)
        xsprint(xstr, "%d", l);
    else {
        long_full(xstr, l/1000);
        xsprint(xstr, ",%03d", l%1000);
    }
}


/*
 * Show a numeric statistic in a nice form.
 */
static void
long_abbr(xstr_t *xstr, int64_t l)
{
    int i;
    double d;

    if (l < 0) {
        l = -l;
        xsprint(xstr, "-");
    }

    if (l < 1000) {
        xsprint(xstr, "%ld", l);
        return;
    }

    d = l;
    for (i = 0; i < 5; i++) {
        d /= 1000;
        if (d < 1000 || i == 4) {
            p3(xstr, d);
            xsprint(xstr, "%c", "KMGTP"[i]);
            return;
        }
    }
}


/*
 * Show a rate in a nice form.
 */
void
stat_rate(stat_t *stat, char *name, double d)
{
    int i;
    xstr_t *xp = &stat->xstr;

    if (!d)
        return;

    tab(stat, name);
    if (d < 0) {
        d = -d;
        xsprint(xp, "-");
    }

    for (i = 0;; i++) {
        if (d < 1000 || i == 5) {
            p3(xp, d);
            if (i)
                xsprint(xp, "%c", " KMGTP"[i]);
            xsprint(xp, "/sec");
            break;
        }
        d /= 1000;
    }
    fit(stat);
}


/*
 * Show a time statistic.
 */
void
stat_time(stat_t *stat, char *name, double time)
{
    int i;
    xstr_t *xp = &stat->xstr;

    if (!time)
        return;

    tab(stat, name);
    if (time < 0) {
        time = -time;
        xsprint(xp, "-");
    }

    time *= NANO;
    for (i = 0; i < 3; i++) {
        if (time < (1000-ROUND)) {
            p3(xp, time);
            xsprint(xp, "%cs", "num"[i]);
            return;
        }
        time /= 1000;
    }

    if (time < (60-ROUND)) {
        p3(xp, time);
        xsprint(xp, "s");
    } else {
        int h, m, s;
        int r = time + ROUND;

        s = r % 60; r /= 60;
        m = r % 60; r /= 60;
        h = r;

        if (h)
            xsprint(xp, "%d:%02d:%02d", h, m, s);
        else
            xsprint(xp, "%d:%02d", m, s);
    }
    fit(stat);
}


/*
 * Put out three digits of precision.
 */
static void
p3(xstr_t *xstr, double d)
{
    if (d >= 100-ROUND)
        xsprint(xstr, "%.0f", d);
    else if (d >= 10-ROUND)
        xsprint(xstr, "%.1f", d);
    else if (d >= 1-ROUND)
        xsprint(xstr, "%.2f", d);
    else
        xsprint(xstr, "%.3f", d);

}


/*
 * Start a new line.
 */
static void
new(stat_t *stat)
{
    int i = stat->xstr.i;

    stat->linei = i;
    stat->wordi = i;
}


/*
 * Tab over and start a new field.
 */
static void
tab(stat_t *stat, char *name)
{
    xstr_t *xp = &stat->xstr;
    int n = TSTOP - (xp->i - stat->linei);

    if (n > 0)
        xsprint(xp, "%*s", n, "");
    else
        xsprint(xp, " ");
    if (name)
        xsprint(xp, "%s=", name);
}


/*
 * In an expandable string, wrap the last item we placed if it does not fit.
 */
static void
fit(stat_t *stat)
{
    int k;
    int m;
    int n;
    char *q;
    int l = stat->linei;
    int w = stat->wordi;
    xstr_t *xp = &stat->xstr;
    int i = xp->i;
    char *p = xp->p;

    if (i - l < LSIZE || w - l <= TSTOP) {
        stat->wordi = i;
        return;
    }

    for (m = w; m < i; m++)
        if (p[m] != ' ')
            break;

    n = i - m;
    k = TSTOP + 1 - (m - w);
    p = xasubs(xp, i-1+k);
    q = p - k;
    xp->i += k;

    while (n--)
        *p-- = *q--;
    while (k--)
        *p-- = ' ';

    ((char *)xp->p)[w] = '\n';
    stat->linei = w + 1;
    stat->wordi = xp->i;
}


/*
 * End a line.
 */
static void
end(stat_t *stat)
{
    xstr_t *xp = &stat->xstr;

    if (stat->linei == stat->wordi)
        xp->i = stat->linei;
    else
        xsprint(xp, "\n");
}
