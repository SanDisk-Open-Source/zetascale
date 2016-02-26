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
