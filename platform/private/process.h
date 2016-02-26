/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_PROCESS_H
#define PLATFORM_PROCESS_H  1

/*
 * File:   process.h
 * Author: drew
 *
 * Created on January 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: process.h 587 2008-03-14 00:13:21Z drew $
 */

/**
 * Process representation for crash-consistent shm + IPC between user scheduled
 * threads.
 *
 * Between process connection and process detach (explicit or by death)
 * this is owned by the process itself.  After process death this is used
 * by shmemd for cleanup.
 */

#include "platform/shmem.h"
#include "platform/shmem_ptrs.h"
#include "platform/types.h"

#include "shmem_internal.h"

/*
 * Expands to "proc0" on little endian systems.  Subsequent header changes
 * will have a different last (high) byte to detect interoperability 
 * problems (software upgrades on one node will be all-or-nothing).
 */

#define PLAT_PROCESS_MAGIC 0x306f7270

struct plat_process {
    /** PLAT_PROCESS_MAGIC */
    int32_t magic;

    /**
     * Pid of process attaching used to implement fail-stop behavior. Any
     * abnormal termination in fail-stop mode results in all attached 
     * processes being terminated by a SIGTERM, SIGKILL sequence.
     */
    pid_t pid;

    /**
     * Name of the unix domain socket bound by this for incoming shmemd
     * connections.  When shmemd terminates abnormally and is restarted, the
     * new shmemd iterates over all processes and reaps those which no longer
     * have their socket file flock'd.  It connects to the rest via a unix
     * domain socket with this endpoint.
     */
    union shmem_socket_addr reconnect_addr;

    /** Next.  Linked  list is single reader/writer (current shmemd) */
    plat_process_sp_t next;
    /** Prev.  Linked  list is single reader/writer (current shmemd) */
    plat_process_sp_t prev;
};

#endif /* ndef PLATFORM_PROCESS_H */
