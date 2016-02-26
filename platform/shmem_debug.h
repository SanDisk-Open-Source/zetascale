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
