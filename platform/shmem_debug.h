/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   shmem_debug.h
 * Author: gshaw
 *
 * Created on June 12, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com
 *
 * $Id: $
 */

#ifndef SHMEM_DEBUG_H
#define SHMEM_DEBUG_H

/**
 * @brief Declarations to support various shmem debugging services
 *
 * For now, the only debugging service is the management of stack
 * backtraces.
 */

/**
 * @brief Compile-time configuration
 *
 */

#define SHMEM_DEBUG_BACKTRACE 1

#ifdef SHMEM_DEBUG_BACKTRACE

enum {
    SHMEM_BACKTRACE_NONE = -1
};

extern void shmem_backtrace_init(void);
extern void shmem_backtrace_fini(void);

extern void shmem_backtrace_enable(void);
extern void shmem_backtrace_disable(void);
extern void shmem_backtrace_debug(void);
extern int shmem_save_backtrace(void);
extern void shmem_backtrace_dump(void);

#endif /* def SHMEM_DEBUG_BACKTRACE */

#endif /* ndef SHMEM_DEBUG_H */
