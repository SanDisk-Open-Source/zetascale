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
 * File:   fthSignal.h
 * Author: drew
 *
 * Created on March 25, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSignal.h 6477 2009-03-28 07:00:47Z drew $
 */

#ifndef _FTH_SIGNAL_H
#define _FTH_SIGNAL_H

#include "platform/signal.h"

/**
 * @brief Initialize fthSignal subsystem.
 *
 * May be called before fthSignal so that all dynamic allocations are done 
 * before a baseline snapshot. Otherwise initialization will be implicit in
 * the first fthSignal call.
 */
void fthSignalInit();

/**
 * @brief Shutdown fthSignal subsystem
 *
 * May be called from an fthThread or pthread.  The caller blocks synchronously
 * on the signal handling thread's termination.
 */
void fthSignalShutdown();

/** 
 * @brief Handle signal in signal handling pthread
 *
 * One of the #fthInit functions must have previously been called.
 *
 * Technically speaking, conventional signal handlers are only allowed
 * to modify sigatomic_t variables although in practice you can extend
 * that to making system calls.  Most library functions are right out
 * because libraries aren't re-entrant.
 * 
 * fthSignal causes the handlers to be called non-reentrantly 
 * in a single fthThread, with handlers being allowed to do all of the 
 * things pthreads (with caveats on blocking the scheduler) and 
 * fthThreads do.
 *
 * Signal handlers are not SA_ONESHOT.
 * 
 * @param <IN> signum signal
 * @param <IN> handler function to apply
 * @return Previous function 
 */

sighandler_t fthSignal(int signum, sighandler_t handler);

#endif
