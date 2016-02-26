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
 * File: trace.h
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#ifndef MSG_TRACE_H
#define MSG_TRACE_H

#include "tools.h"
#ifdef NOSDF
#include "msg_fthfake.h"
#include "msg_sdffake.h"
#else
#include "msg_sdfincl.h"
#endif


/*
 * Bits for tracing.
 */
#define MT_USER 0x000000                /* User print */
#define MT_UBUG 0x000001                /* User debugging */
#define MT_ABUG 0x000002                /* General debugging */
#define MT_BBUG 0x000004                /* General debugging */
#define MT_CBUG 0x000008                /* General debugging */
#define MT_DBUG 0x000010                /* General debugging */
#define MT_FREE 0x000020                /* Freelist */
#define MT_TCPS 0x000040                /* TCP send */
#define MT_TCPR 0x000080                /* TCP receive */
#define MT_CAST 0x000100                /* Broadcast layer */
#define MT_META 0x000400                /* Metadata */
#define MT_MAPN 0x001000                /* Node mapping */
#define MT_SEND 0x002000                /* Send */
#define MT_RECV 0x004000                /* Receive */
#define MT_POST 0x008000                /* Post */
#define MT_POLL 0x010000                /* Poll */
#define MT_SMSG 0x020000                /* SDF messaging */

#define MT_LOGS 0x080000                /* Show SDF logging */
#define MT_TIME 0x100000                /* Show time information */
#define MT_LESS 0x200000                /* Less debugging information */
#define MT_ALL  0xffffff                /* Everything turned on */


/*
 * Trace functions.
 */
#define t_user(fmt, args...)      _tp(USER, NULL,   0,  fmt, ##args)
#define t_ubug(fmt, args...)      _tp(UBUG, "UBUG", 0,  fmt, ##args)
#define t_abug(fmt, args...)      _tp(ABUG, "ABUG", 0,  fmt, ##args)
#define t_bbug(fmt, args...)      _tp(BBUG, "BBUG", 0,  fmt, ##args)
#define t_cbug(fmt, args...)      _tp(CBUG, "CBUG", 0,  fmt, ##args)
#define t_dbug(fmt, args...)      _tp(DBUG, "DBUG", 0,  fmt, ##args)

#define t_free(id, fmt, args...)  _tp(FREE, "FREE", id, fmt, ##args)
#define t_tcps(id, fmt, args...)  _tp(TCPS, "TCPS", id, fmt, ##args)
#define t_tcpr(id, fmt, args...)  _tp(TCPR, "TCPR", id, fmt, ##args)
#define t_cast(id, fmt, args...)  _tp(CAST, "CAST", id, fmt, ##args)
#define t_meta(id, fmt, args...)  _tp(META, "META", id, fmt, ##args)
#define t_mapn(id, fmt, args...)  _tp(MAPN, "MAPN", id, fmt, ##args)
#define t_send(id, fmt, args...)  _tp(SEND, "SEND", id, fmt, ##args)
#define t_recv(id, fmt, args...)  _tp(RECV, "RECV", id, fmt, ##args)
#define t_post(id, fmt, args...)  _tp(POST, "POST", id, fmt, ##args)
#define t_poll(id, fmt, args...)  _tp(POLL, "POLL", id, fmt, ##args)
#define t_smsg(id, fmt, args...)  _tp(SMSG, "SMSG", id, fmt, ##args)

#define LOG_ID PLAT_LOG_ID_INITIAL
#define t_on(flag) (MT_##flag == 0 || (MsgDebug&(MT_##flag)))
#define _tp(flag, type, id, fmt, args...)                   \
    do {                                                    \
        if (id)                                             \
            ffdc_log(__LINE__, id, LOG_CAT,                 \
                     PLAT_LOG_LEVEL_TRACE, fmt, ##args);    \
        if (t_on(flag))                                     \
            trace_print(__FILE__, __LINE__,                 \
                        __PRETTY_FUNCTION__, type, id,      \
                        LOG_CAT, 0, -1, fmt, ##args);       \
    } while (0)


/*
 * Function prototypes
 */
int     trace_fake(void *a);
int     trace_print(char *file, int line, const char *func, char *type,
                    int id, int cat, int sys, int level, char *fmt, ...)
                     __attribute__((format(printf, 9, 10)));
void    trace_exit(void);
void    trace_init(void);
void    trace_prefix(xstr_t *xp);
void    trace_setnode(char *name);
void    trace_setp_debug(char *str);
char   *trace_getfth(void);
char   *trace_getpth(void);
char   *trace_getnode(void);
char   *trace_setfth(char *name);
char   *trace_setpth(char *name);

void    panic(char *fmt, ...)  __attribute__((format(printf, 1, 2)));
void    printd(char *fmt, ...) __attribute__((format(printf, 1, 2)));
void    printe(char *fmt, ...) __attribute__((format(printf, 1, 2)));
void    printm(char *fmt, ...) __attribute__((format(printf, 1, 2)));


/*
 * Global variables.
 */
extern uint64_t MsgDebug;

#endif /* MSG_TRACE_H */
