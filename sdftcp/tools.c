/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: tools.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Some helpful tools.
 */
#include <sched.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include "tools.h"
#include "trace.h"
#include "msg_cat.h"


/*
 * Allow all CPUs to be used.
 */
void
use_all_cpus(void)
{
    int i;
    int n;
    cpu_set_t cpus;

    n = sysconf(_SC_NPROCESSORS_ONLN);
    CPU_ZERO(&cpus);
    for (i = 0; i < n; i++)
        CPU_SET(i, &cpus);
    if (sched_setaffinity(0, sizeof(cpus), &cpus) < 0)
        fatal_sys("sched_setaffinity failed");
}


/*
 * Return the number of cpus enabled.
 */
int
enabled_cpus(void)
{
    int i;
    int n;
    int u;
    cpu_set_t cpus;

    n = sysconf(_SC_NPROCESSORS_ONLN);
    if (sched_getaffinity(0, sizeof(cpus), &cpus) < 0)
        fatal_sys("sched_setaffinity failed");

    u = 0;
    for (i = 0; i < n; i++)
        if (CPU_ISSET(i, &cpus))
            u++;
    return u;
}


/*
 * Return the next item in a semicolon separated line.
 */
char *
semi_item(char **pp)
{
    int n;
    char *endp;
    char *retp;
    char *strp = *pp;

    while (*strp == ' ' || *strp == '\t')
        strp++;
    if (*strp == '\0')
        return NULL;
    endp = strchrnul(strp, ';');
    n = endp - strp;
    retp = m_malloc(n+1, "semi_item:N*char");
    memcpy(retp, strp, n);
    retp[n] = '\0';
    if (*endp != '\0')
        endp++;
    *pp = endp;
    return retp;
}


/*
 * Copy a string ensuring that it is terminated with a null byte.
 */
void
strncopynull(char *dptr, size_t dlen, const char *sptr, size_t slen)
{
    int len;

    len = dlen -1;
    if (len < 0)
        return;
    if (slen && len > slen)
        len = slen;
    strncpy(dptr, sptr, len);
    dptr[dlen-1] = '\0';
}


/*
 * Copy a string from sptr to dptr.  dlen is the size of our destination given.
 * slen, if non-zero, is the maximum size of the source string.  The string is
 * copied and the rest of dptr is zero filled.  The string will be truncated if
 * it is too long and is guaranteed to be null terminated.
 */
void
strncopyfill(char *dptr, size_t dlen, const char *sptr, size_t slen)
{
    int c;
    int len;
    char *d = dptr;

    len = dlen - 1;
    if (len < 0)
        return;
    if (slen && len > slen)
        len = slen;

    while (len--) {
        *d++ = c = *sptr++;
        if (!c)
            break;
    }

    len = dlen - (d-dptr);
    if (len >= 16)
        memset(d, 0, len);
    else
        while (len--)
            *d++ = 0;
}


/*
 * Initialize an expandable string.
 */
void
xsinit(xstr_t *xp)
{
    xainit(xp, sizeof(char), 256, 0);
}


/*
 * Print to an expandable string in the manner of printf.
 */
void
xsprint(xstr_t *xp, char *fmt, ...)
{
    int s;
    va_list alist;
    
    do {
        va_start(alist, fmt);
        s = xsvprint(xp, fmt, alist);
        va_end(alist);
    } while (!s);
}


/*
 * Print to an expandable string in the manner of vsprintf.
 */
int
xsvprint(xstr_t *xp, char *fmt, va_list alist)
{
    int size;
    int left = xp->n - xp->i;
    int need = 1 + vsnprintf(&((char *)xp->p)[xp->i], left, fmt, alist);

    if (need <= left) {
        xp->i += need - 1;
        return 1;
    }

    size = xp->n * 2;
    if (size < need)
        size = need;
    xp->p = realloc_q(xp->p, size);
    xp->n = size;
    return 0;
}


/*
 * Free an expandable string.
 */
void
xsfree(xstr_t *xp)
{
    xafree(xp);
}


/*
 * Initialize an expandable array.
 *  xp    - The expandable array.
 *  esize - Size of each element.
 *  nelem - Number of elements.
 *  clear - If set, initialize new elements to 0.
 */
void *
xainit(xstr_t *xp, int esize, int nelem, int clear)
{
    int n = esize * nelem;

    xp->i = 0;
    xp->n = nelem;
    xp->s = esize;
    xp->c = clear;
    xp->p = malloc_q(n);

    if (clear)
        memset(xp->p, 0, n);
    return xp->p;
}


/*
 * Ensure that an expandable array has room to hold the given index and return
 * a pointer to that element.
 */
void *
xasubs(xstr_t *xp, int i)
{
    if (i >= xp->n) {
        int n = xp->n * 2;
        if (n <= i)
            n = i+1;
        xp->p = realloc_q(xp->p, n*xp->s);
        if (xp->c)
            memset(xp->p + (xp->n * xp->s), 0, (n-xp->n) * xp->s);
        xp->n = n;
    }
    return xp->p + i*xp->s;
}


/*
 * Free an expandable array.
 */
void
xafree(xstr_t *xp)
{
    plat_free(xp->p);
    xp->s = 0;
    xp->i = 0;
    xp->n = 0;
    xp->p = NULL;
}


/*
 * Allocate and clear memory.
 */
void *
malloc_z(size_t size, char *msg)
{
    void *ptr = m_malloc(size, msg);

    memset(ptr, 0, size);
    return ptr;
}


/*
 * Call realloc and complain on failure.  See comment to m_malloc.
 */
void *
m_realloc(void *ptr, size_t size, char *msg)
{
    void *new = realloc_q(ptr, size);

    t_free(0, "realloc_q %s %p %ld => %p", msg, ptr, size, new);
    return new;
}


/*
 * Allocate a string that is generated with printf type arguments and complain
 * on failure.  See comment to m_malloc.
 */
char *
m_asprintf(char *msg, char *fmt, ...)
{
    char *ptr;
    va_list alist;
    
    va_start(alist, fmt);
    if (plat_vasprintf(&ptr, fmt, alist) < 0)
        panic("plat_vasprintf failed: %s", fmt);
    va_end(alist);
    t_free(0, "q_asprintf %s %s => %p", msg, fmt, ptr);
    return ptr;
}


/*
 * Duplicate a string and complain on failure.  See comment to m_malloc.
 */
void *
m_strdup(char *str, char *msg)
{
    void *ptr = strdup_q(str);

    t_free(0, "strdup_q %s %s => %p", msg, str, ptr);
    return ptr;
}


/*
 * Call malloc and complain on failure.  This is one of a series of m_ calls to
 * manage memory used primarily by the messaging system.  They should be
 * consistent amongst themselves.  If one calls an m_ call to allocate memory,
 * one should call m_free to free it.  These should also call panic rather than
 * fatal since fatal could end up calling malloc and on out of memory errors,
 * we want to exit as quickly as possible since almost nothing will function.
 * For those who believe in not exiting on memory errors, it would be easy to
 * change these.
 */
void *
m_malloc(size_t size, char *msg)
{
    void *ptr = malloc_q(size);

    t_free(0, "malloc_q %s %ld => %p", msg, size, ptr);
    return ptr;
}


/*
 * Free some memory.  See comment to m_free.
 */
void
m_free(void *ptr)
{
    t_free(0, "q_free %p =>", ptr);
    plat_free(ptr);
}


/*
 * Call malloc and complain on failure.  This needs to call panic rather than
 * fatal since fatal could end up calling malloc and recursing.
 */
void *
realloc_q(void *ptr, size_t size)
{
    void *new;

    if (!size) {
        plat_free(ptr);
        return NULL;
    }

    new = plat_realloc(ptr, size);
    if (!new)
        panic("realloc %ld failed", size);
    return new;
}


/*
 * Call malloc and complain on failure.  See comment to realloc_q.
 */
void *
malloc_q(size_t size)
{
    void *ptr = plat_malloc(size);

    if (!ptr)
        panic("malloc %ld failed", size);
    return ptr;
}


/*
 * Duplicate a string and complain on failure.  See comment to realloc_q.
 */
void *
strdup_q(char *str)
{
    void *ptr = plat_strdup(str);

    if (!ptr)
        panic("plat_strdup %ld failed", (long)strlen(str)+1);
    return ptr;
}
