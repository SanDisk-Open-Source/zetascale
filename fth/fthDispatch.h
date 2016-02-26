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
 * File:   fthDispatch.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthDispatch.h,v 1.1 2009/03/19 23:42:58 jbertoni Exp jbertoni $
 */

// Resolve include order issue. fthThreadQ.h needs to have the fthThread 
// defined so that its inline definitions can be resolved.
#include "fthThreadQ.h"

#ifndef _FTH_DISPATCH_H
#define _FTH_DISPATCH_H

#include "platform/types.h"

// Dispatcher area in both fth Threads and scheduler
typedef struct fthDispatchArea {

#ifdef __x86_64__
#ifdef fthAsmDispatch   
    // Register save area for thread context ***ORDER***
    uint64_t pc;
    uint64_t rsp;
    uint64_t rbx;
    uint64_t rbp;
    uint64_t r12;
    uint64_t r13;
    uint64_t r14;
    uint64_t r15;
#endif

    union {
        struct fthSched *sched;              // Pointer to scheduler thread
        struct fthThread *thread;            // Pointer to dispatched thread
    };

    uint32_t floating;

#ifdef fthSetjmpLongjmp
    sigjmp_buf env;
#endif
#endif
    
    void (*startRoutine)(uint64_t arg);
    uint64_t arg;                            // Initial and return argument
    
    void *stack;                             // Stack base from malloc
    int stackSize;                           // Remember for release
    int stackId;                             // valgrind stack id

} fthDispatchArea_t;
    
//
// C defs for assemmbler routines
#ifdef fthSetjmpLongjmp
void fthStackSwitch(struct fthThread *thread, uint64_t newStack);
#endif

#ifdef fthAsmDispatch
struct fthThread *fthToScheduler(struct fthThread *thread, uint64_t flags);
struct fthThread *fthDispatch(struct fthThread *thread);
void fthSaveSchedulerContext(struct fthSched *sched);
#endif


#endif
