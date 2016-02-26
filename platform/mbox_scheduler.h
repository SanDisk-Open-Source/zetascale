/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
