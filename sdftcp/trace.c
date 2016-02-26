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

/*
 * File: trace.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Useful for tracing.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <inttypes.h>
#include "trace.h"
#include "msg_cat.h"
#include "msg_msg.h"
#include "msg_map.h"


/*
 * Structure to associate pthreads with names.
 */
typedef struct pthname {
    struct pthname *next;
    pthread_t       self;
    char           *name;
} pthname_t;


/*
 * Structure to associate fthreads with names.
 */
typedef struct fthname {
    struct fthname *next;
    uint64_t        id;
    char           *name;
} fthname_t;


/*
 * Global variables.
 */
uint64_t MsgDebug;


/*
 * Static variables.
 */
static char            *NodeName;
static pthname_t       *PthNameList;
static uint64_t         PthNameNext;
static pthread_mutex_t  PthNameLock;
static fthname_t       *FthNameList;
static uint64_t         FthNameNext;
static fthSpinLock_t    FthNameLock;


/*
 * Tracing options.
 */
static opts_t DebugOpts[] ={
    { "ubug", MT_UBUG                               },
    { "abug", MT_ABUG                               },
    { "bbug", MT_BBUG                               },
    { "cbug", MT_CBUG                               },
    { "dbug", MT_DBUG                               },
    { "free", MT_FREE                               },
    { "tcps", MT_TCPS                               },
    { "tcpr", MT_TCPR                               },
    { "cast", MT_CAST                               },
    { "meta", MT_META                               },
    { "mapn", MT_MAPN                               },
    { "send", MT_SEND                               },
    { "recv", MT_RECV                               },
    { "post", MT_POST                               },
    { "poll", MT_POLL                               },
    { "smsg", MT_SMSG                               },

    { "logs", MT_LOGS                               },
    { "time", MT_TIME                               },
    { "less", MT_LESS                               },
    { "all",  MT_ALL                                },

    { "many", MT_ALL & ~(MT_CAST|MT_FREE|MT_LESS)   },
    { "most", MT_ALL & ~(MT_CAST|MT_FREE|MT_TIME)   },
    {                                               }
};


/*
 * Initialize.
 */
void
trace_init(void)
{
    if (pthread_mutex_init(&PthNameLock, NULL) < 0)
        fatal_sys("pthread_mutex_init failed");
    FTH_SPIN_INIT(&FthNameLock);
}


/*
 * Clean up.
 */
void
trace_exit(void)
{
    fthname_t *f;

    if (NodeName) {
        plat_free(NodeName);
        NodeName = NULL;
    }

    FTH_SPIN_LOCK(&FthNameLock);
    f = FthNameList;
    FthNameList = NULL;
    FTH_SPIN_UNLOCK(&FthNameLock);

    while (f) {
        fthname_t *g = f;

        f = f->next;
        plat_free(g->name);
        plat_free(g);
    }

    pthread_mutex_destroy(&PthNameLock);
}


/*
 * Use when faking out fthreads.
 */
int
trace_fake(void *a)
{
    return 0;
}


/*
 * Set the debugging flags appropriately.  str is the string that might be
 * passed from the command line.
 */
void
trace_setp_debug(char *str)
{
    opts_set(&MsgDebug, DebugOpts, "msg_debug", str);
}


/*
 * Get our node name.
 */
char *
trace_getnode(void)
{
    int n;

    if (NodeName)
        return NodeName;
    n = sdf_msg_myrank();
    if (n >= 0) {
        char buf[64];

        snprintf(buf, sizeof(buf), "n%d", n);
        NodeName = strdup_q(buf);
        return NodeName;
    }
    return NULL;
}


/*
 * Set our node name.
 */
void
trace_setnode(char *name)
{
    if (NodeName)
        plat_free(NodeName);
    NodeName = strdup_q(name);
    if (!NodeName)
        fatal("out of space");
}


/*
 * Get the name of our pth thread.
 */
char *
trace_getpth(void)
{
    char name[64];
    pthname_t *p;
    pthread_t self = pthread_self();

    if (!self)
        return NULL;

    for (p = PthNameList; p; p = p->next)
        if (self == p->self)
            return p->name;

    snprintf(name, sizeof(name), "p%ld", PthNameNext++);
    return trace_setpth(name);
}


/*
 * Set the name of our pth thread.
 */
char *
trace_setpth(char *name)
{
    pthname_t *p = plat_malloc(sizeof(*p));
    char *s = strdup_q(name);

    if (!p || !s)
        fatal("out of space");

    p->self = pthread_self();
    p->name = s;

    pthread_mutex_lock(&PthNameLock);
    p->next = PthNameList;
    PthNameList = p;
    pthread_mutex_unlock(&PthNameLock);
    return p->name;
}


/*
 * Get the name of our fth thread.
 */
char *
trace_getfth(void)
{
    char name[64];
    fthname_t *f;
    uint64_t id = fth_uid();

    if (!id)
        return NULL;

    for (f = FthNameList; f; f = f->next)
        if (id == f->id)
            return f->name;

    snprintf(name, sizeof(name), "f%ld", FthNameNext++);
    return trace_setfth(name);
}


/*
 * Set the name of our fth thread.
 */
char *
trace_setfth(char *name)
{
    fthname_t *f = plat_malloc(sizeof(*f));
    char *s = strdup_q(name);

    if (!f || !s)
        fatal("out of space");

    f->id   = fth_uid();
    f->name = s;

    FTH_SPIN_LOCK(&FthNameLock);
    f->next = FthNameList;
    FthNameList = f;
    FTH_SPIN_UNLOCK(&FthNameLock);
    return f->name;
}


/*
 * Print out a message with relevant information.
 */
int
trace_print(char *file, int line, const char *func, char *type,
            int id, int cat, int sys, int level, char *fmt, ...)
{
    xstr_t xstr;
    va_list alist;
    int sdfmode = !t_on(LESS) && type;

    /*
     * If we are not using SDF detailed logging and if it is a sdf_log[defitw]
     * type of message, return unless we have LOGS (SDF logging) turned on.
     */
    if (!sdfmode && (level >= 0 && !t_on(LOGS)))
        return 0;
    
    xsinit(&xstr);
    if (sdfmode)
        xsprint(&xstr, "\n    ");
    else if (type) {
        xsprint(&xstr, "%s: ", type);
        if (t_on(TIME)) {
            struct tm tm;
            struct timespec ts;

            clock_gettime(CLOCK_REALTIME, &ts);
            localtime_r(&ts.tv_sec, &tm);
            xsprint(&xstr, "%02d:%02d:%02d.%03d: ", tm.tm_hour,
                    tm.tm_min, tm.tm_sec, (int)(ts.tv_nsec/(NANO/MSEC)));
        }
        trace_prefix(&xstr);
    }

    va_start(alist, fmt);
    xsvprint(&xstr, fmt, alist);
    va_end(alist);
    if (sys && errno) {
        char buf[128];

        if (plat_strerror_r(errno, buf, sizeof(buf)) == 0)
            xsprint(&xstr, " (%s)", buf);
        else
            xsprint(&xstr, " (%d)", errno);
    }
    xsprint(&xstr, "\n");

#ifndef NOSDF
    if (sdfmode) {
        if (level < 0)
            level = PLAT_LOG_LEVEL_DEBUG;
        plat_log_msg_forward(file, line, func, id,
                             cat, level, "%s", (char *)xstr.p);
        xsfree(&xstr);
        return 0;
    }
#endif

    fputs(xstr.p, stderr);
    fflush(stderr);
    xsfree(&xstr);
    return 0;
}


/*
 * Print out a debugging message in a multi-node, multi-threading environment.
 */
void
trace_prefix(xstr_t *xp)
{
    char *nname = trace_getnode();
    char *pname = trace_getpth();
    char *tname = trace_getfth();

    if (nname)
        xsprint(xp, "%s: ", nname);
    if (pname)
        xsprint(xp, "%s: ", pname);
    if (tname)
        xsprint(xp, "%s: ", tname);
}


/*
 * Print out an error message and exit.
 */
void
panic(char *fmt, ...)
{
    va_list alist;
    
    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
    plat_exit(1);
}


/*
 * Print to standard output.
 */
void
printm(char *fmt, ...)
{
    va_list alist;
    
    va_start(alist, fmt);
    vfprintf(stdout, fmt, alist);
    va_end(alist);
    fprintf(stdout, "\n");
}


/*
 * Print to standard error.
 */
void
printe(char *fmt, ...)
{
    va_list alist;
    
    va_start(alist, fmt);
    vfprintf(stderr, fmt, alist);
    va_end(alist);
    fprintf(stderr, "\n");
}
