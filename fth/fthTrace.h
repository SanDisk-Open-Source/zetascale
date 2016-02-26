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
 * File:   fthTrace.h
 * Author: Josh Dybnis
 *
 * Created on September 8, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 */
#ifndef _FTH_TRACE_H
#define _FTH_TRACE_H

/**
 *  Trace Categories
 *  c -- chunk lock
 *  r -- RCU
 */

#include "platform/types.h"
#include "fth.h"

#ifndef ENABLE_FTH_TRACE
#define TRACE(...) do { } while (0)

#else
#define TRACE(flag, msg, v1, v2) fthTrace((flag), (msg), (size_t)(v1), (size_t)(v2))
#define FTH_TRACE_BUFFER_SCALE 16
#define FTH_TRACE_BUFFER_SIZE (1 << FTH_TRACE_BUFFER_SCALE)
#define FTH_TRACE_BUFFER_MASK (FTH_TRACE_BUFFER_SIZE - 1)

typedef struct fthTraceRecord {
    uint64_t timestamp; // in some arbitrary units
    const char *format; // Union of a pointer to printf format string + two bytes of embedded trace flags.
                        // The two most significant bytes of the word are the trace flags.
                        // The format string can contain up to two parameters.
    void *fthId;        // fth thread id 
    size_t value1;      // first printf parameter
    size_t value2;      // second printf parameter
} fthTraceRecord_t;

/** circular buffer */
typedef struct fthTraceBuffer {
    uint32_t head; // current head of circular buffer
    fthTraceRecord_t recs[0];
} fthTraceBuffer_t;

extern __thread fthTraceBuffer_t *traceBuffer;

/** @brief called once per scheduler */
void fthTraceSchedInit (void);

/** @brief dump trace messages to a file. The message format is:
 *
 *      <timestamp> <scheduler number> <fth thread id> <trace flag> <trace message>
 */
void fthTraceDump (const char *fileName);

/** @param flag <IN> indicates what kind of trace messages should be included in the dump. It is a sequence of letters
 *                   followed by numbers (e.g. "x1c9N2g3"). The letters indicate trace categories and the numbers are
 *                   trace levels for each category. If a trace category appears in the parameter, then messages from
 *                   that category will be included in the dump if their trace level is less than or equal to the one
 *                   specified in the parameter. Trace categories are case sensitive.
 */
void fthSetTraceLevel (const char *flags);

/** @param flag <IN> a two character string containing a letter followed by a number (e.g. "f3"). The letter indicates
 *                   a trace category, and the number a trace level. It controls whether or not the trace message gets
 *                   included in the dump. The message is only included when its specified category is enabled at a 
 *                   trace level greater than or equal to the one in the parameter. Trace categories are case sensitive
 */
static inline void fthTrace (const char *flag, const char *format, size_t value1, size_t value2)
{
    extern uint64_t fthTraceFlagMask;
    extern __thread fthTraceBuffer_t *traceBuffer;

    if (PLAT_UNLIKELY(fthTraceFlagMask & (1 << (flag[0] - 'A')))) {
        uint64_t timestamp = rdtsc(); 
        // embed <flags> in <format> so we don't have to make the TraceRecord_t any bigger than it already is
        format = (const char *)((size_t)format | (uint64_t)flag[0] << 56 | (uint64_t)flag[1] << 48);
        traceBuffer->recs[traceBuffer->head++ & FTH_TRACE_BUFFER_MASK] = 
            (fthTraceRecord_t){ timestamp, format, fthId(), value1, value2 };
    }
}
#endif
#endif
