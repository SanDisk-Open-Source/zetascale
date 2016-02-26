/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: msg_sdf.h
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#if 0
#ifndef MSG_SDF_H
#define MSG_SDF_H 1

#include <stdint.h>
#include "msg_msg.h"

/*
 * Passed to msg_sdf_init.
 */
typedef struct msg_sdf_init {
    int         argc;                   /* WO: Argument count */
    char      **argv;                   /* WO: Arguments */
    int32_t     rank;                   /* RW: My rank */
    uint32_t    flags;                  /* RW: MPI flags */
    uint32_t    nodes;                  /* RW: Number of nodes */
    uint32_t    debug;                  /* WO: Messaging debug */
    msg_init_t  init;                   /* RW: TCP messaging */
} msg_sdf_init_t;


/*
 * Global variables.
 */
extern int MeRank;
extern int NewMsg;


/*
 * Function prototypes.
 */
void    msg_sdf_exit(void);
void    msg_sdf_wait(void);
void    msg_sdf_init(msg_sdf_init_t *msg_sdf);
int     msg_sdf_myrank(void);
int     msg_sdf_lowrank(void);
int     msg_sdf_numranks(void);
int    *msg_sdf_ranks(int *np);

#endif /* MSG_SDF_H */
#endif
