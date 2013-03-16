/*
 * File:   sdf/platform/memory_fault.c
 * Author: drew
 *
 * Created on September 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: memory_fault.c 10527 2009-12-12 01:55:08Z drew $
 */

#include <sys/time.h>
#include <sys/resource.h>

#include "platform/logging.h"
#include "platform/platform.h"
#include "platform/string.h"
#include "platform/unistd.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT, PLAT_LOG_CAT_PLATFORM_ALLOC, "memory_fault");

enum fault_how {
    FAULT_READ,
    FAULT_WRITE
};

static int plat_fault(void *buf, size_t len, enum fault_how);

int
plat_read_fault(void *buf, size_t len) {
    return (plat_fault(buf, len, FAULT_READ));
}

int
plat_write_fault(void *buf, size_t len) {
    return (plat_fault(buf, len, FAULT_WRITE));
}

/*
 * XXX: Note that when Linux is overcommitting the faults may result
 * in a SIGSEGV.  We may want to return a status so that  the user
 * gets a better message (over-committed vs. something that smells like
 * a software bug.
 */

static int
plat_fault(void *buf, size_t len, enum fault_how how) {
    int status;
    int page_size;
    volatile char *ptr;
    volatile char *end;
    // char ignore;
    struct timeval tv_start;
    struct rusage rusage_start;
    struct timeval tv_end;
    struct rusage rusage_end;
    struct timeval tv_delta;
    long usecs;
    unsigned long pages;

    status = getrusage(RUSAGE_SELF, &rusage_start);
    plat_assert(!status);

    status = gettimeofday(&tv_start, NULL);
    plat_assert(!status);

    for (ptr = (volatile char *)buf, end = ((volatile char *)buf) +
         len, page_size = getpagesize(); ptr < end; ptr += page_size) {
        switch (how) {
        case FAULT_READ:
            // ignore = *ptr;
            break;
        case FAULT_WRITE:
            *ptr = 0;
            break;
        }
    }

    status = gettimeofday(&tv_end, NULL);
    plat_assert(!status);

    status = getrusage(RUSAGE_SELF, &rusage_end);
    plat_assert(!status);

    tv_delta.tv_sec = tv_end.tv_sec - tv_start.tv_sec;
    tv_delta.tv_usec = tv_end.tv_usec - tv_start.tv_usec;
    if (tv_delta.tv_usec < 0) {
        tv_delta.tv_usec += 1000000;
        tv_delta.tv_sec--;
    }
    usecs = tv_delta.tv_sec * 1000000 + tv_delta.tv_usec;

    pages = len / page_size;

    /*
     * XXX: Shouldn't be INFO, but we should change the default compile time
     * priority to just a notch up from TRACE if we're still concerned.
     */
    plat_log_msg(20951, LOG_CAT, PLAT_LOG_LEVEL_DEBUG,
                 "Faulted %p len 0x%lx, %ld pages in %ld secs %ld usecs"
                 " %.2g usecs per maj_flt %ld min_flt %ld",
                 buf, (unsigned long)len, pages, tv_delta.tv_sec,
                 tv_delta.tv_usec, ((double)usecs)/pages,
                 rusage_end.ru_majflt - rusage_start.ru_majflt,
                 rusage_end.ru_minflt - rusage_start.ru_minflt);

    return (0);
}
