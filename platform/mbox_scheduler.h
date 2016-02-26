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

#ifndef MBOX_SCHEDULER_H
#define MBOX_SCHEDULER_H

/*
 * File:   sdf/platform/mbox_scheduler.h
 * Author: drew
 *
 * Created on March 30, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mbox_scheduler.h 922 2008-04-07 17:08:05Z drew $
 */

/**
 * Fth mbox based closure scheduler.
 *
 * Provide a closure scheduler implementation which dispatches activations
 * out of a single fth thread.
 */
#include "platform/closure.h"
#include "platform/defs.h"

__BEGIN_DECLS

/**
 * @brief Allocate scheduler
 */
plat_closure_scheduler_t *plat_mbox_scheduler_alloc();

/**
 * @brief Main function passed to fth thread
 *
 * The scheduler runs until plat_closure_scheduler_shutdown is called.
 *
 * @param arg <IN> #plat_mbox_scheduler_alloc return cast to uint64_t
 */
void plat_mbox_scheduler_main(uint64_t arg);

__END_DECLS

#endif /* ndef MBOX_SCHEDULER_H */
