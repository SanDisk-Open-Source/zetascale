/*
 * File:   fthSchedType.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: fthSchedType.h 396 2008-02-29 22:55:43Z jim $
 */
#ifndef  _FTH_SCHED_TYPE_H
#define  _FTH_SCHED_TYPE_H

// Defines the type of stack control used by the scheduler.  Pick one.
#define fthAsmDispatch 1
//#define fthSetjmpLongjmp 1

#ifndef FTH_MAX_SCHEDS
// #define FTH_MAX_SCHEDS 16
#define FTH_MAX_SCHEDS 1
#endif

#endif
