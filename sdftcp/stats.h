/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: stats.h
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#ifndef STATS_H
#define STATS_H

#include "tools.h"
#include "msg_msg.h"


/*
 * Flags.
 */
#define MS_LIVE 0x0001                  /* Liveness */
#define MS_SEQN 0x0002                  /* Sequence */
#define MS_RATE 0x0004                  /* Rate */
#define MS_FULL 0x0008                  /* Full precision */
#define MS_ALL  0x00ff                  /* Everything */


/*
 * For handling flags.
 */
#define s_on(flag) (StatBits&(MS_ ## flag))


/*
 * Statistics.
 */
typedef struct {
    xstr_t xstr;                        /* Expandable string */
    int    linei;                       /* Line index */
    int    wordi;                       /* Word index */
} stat_t;


/*
 * Function prototypes.
 */
void stat_exit(void);
void stat_init(void);
void stat_endl(stat_t *stat);
void stat_free(stat_t *stat);
void stat_make(stat_t *stat);
void stat_labl(stat_t *stat, char *name);
void stat_labn(stat_t *stat, char *name, int rank);
void stat_rate(stat_t *stat, char *name, double d);
void stat_full(stat_t *stat, char *name, int64_t l);
void stat_long(stat_t *stat, char *name, int64_t l);
void stat_time(stat_t *stat, char *name, double time);


/*
 * Global variables.
 */
extern uint64_t StatBits;

#endif /* STATS_H */
