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
