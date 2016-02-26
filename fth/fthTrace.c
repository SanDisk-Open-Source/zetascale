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
 * File:   fthTrace.c
 * Author: Josh Dybnis
 *
 * Created on September 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <platform/assert.h>
#include <string.h>
#ifdef ENABLE_FTH_TRACE
#include "fthTrace.h"
#include "fthSched.h"

__thread fthTraceBuffer_t *traceBuffer = NULL;
uint64_t fthTraceFlagMask = 0;
static const char *fthTraceFlags = "";

void fthTraceSchedInit (void) {
    extern fth_t *fth;
    plat_assert(fth->scheds[curSchedNum]->traceBuffer == NULL);

    size_t size = sizeof(fthTraceBuffer_t) + sizeof(fthTraceRecord_t) * FTH_TRACE_BUFFER_SIZE;
    fthTraceBuffer_t *tb = (fthTraceBuffer_t *)malloc(size);
    plat_assert(tb);
    memset(tb, 0, size);
    fth->scheds[curSchedNum]->traceBuffer = tb;
}

void fthSetTraceLevel (const char *flags) {
    if (flags == NULL) {
        flags = "";
    }
    plat_assert(strlen(flags) % 2 == 0); // a well formed <flags> should be an even number of characters long
    fthTraceFlags = flags;
    fthTraceFlagMask = 0;
    for (int i = 0; flags[i]; i+=2) {
        fthTraceFlagMask |= 1 << (flags[i] - 'A');
    }
}

static inline void dumpTraceRecord (FILE *file, fthTraceRecord_t *r, uint64_t offset) {
    int flag  =  (size_t)r->format >> 56;
    int level = ((size_t)r->format << 48) & 0xF;
    const char *f = strchr(fthTraceFlags, flag);

    // print trace record if it is active at the current trace level
    if (f != NULL && level <= f[1]) {
        char s[3] = {flag, level, '\0'};
        fprintf(file, "%09llu %d %p %s ", ((long long unsigned)r->timestamp - offset)>>6, curSchedNum, r->fthId, s);
        const char *format = (const char *)(((size_t)r->format << 16) >> 16); // strip out the embedded flags
        fprintf(file, format, r->value1, r->value2);
        fprintf(file, "\n");
    }
}

// dump out a trace buffer to <file>
static void dumpTraceBuffer (FILE *file, fthTraceBuffer_t *tb, uint64_t offset) {
    int i;
    if (tb->head > FTH_TRACE_BUFFER_SIZE) {
        for (i = tb->head & FTH_TRACE_BUFFER_MASK; i < FTH_TRACE_BUFFER_SIZE; ++i) {
            dumpTraceRecord(file, tb->recs + i, offset);
        }
    }

    for (i = 0; i < (tb->head & FTH_TRACE_BUFFER_MASK); ++i) {
        dumpTraceRecord(file, tb->recs + i, offset);
    }
}

void fthTraceDump (const char *fileName) {
    extern fth_t *fth;
    extern int schedNum;
    uint64_t offset = (uint64_t)-1;
    int i;

    // find the min timestamp accross all trace buffers.
    for (i = 0; i < schedNum; ++i) {
        fthSched_t *s = fth->scheds[i];
        if (s->traceBuffer != NULL && s->traceBuffer->head != 0) {
            uint64_t x = s->traceBuffer->recs[0].timestamp;
            if (x < offset) {
                offset = x;
            }
            if (s->traceBuffer->head > FTH_TRACE_BUFFER_SIZE) {
                x = s->traceBuffer->recs[s->traceBuffer->head & FTH_TRACE_BUFFER_MASK].timestamp;
                if (x < offset) {
                    offset = x;
                }
            }
        }
    }

    if (offset != (uint64_t)-1) {
        FILE *file = fopen(fileName, "w");
        plat_assert(file);
        for (i = 0; i < schedNum; ++i) {
            fthSched_t *s = fth->scheds[i];
            if (s->traceBuffer != NULL) {
                dumpTraceBuffer(file, s->traceBuffer, offset);
            }
        }
        fclose(file);
    }
}
#endif
