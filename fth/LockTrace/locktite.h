/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/******************************************************************
 *
 * locktite -- A program to to analyze fth lock trace files.
 *
 * Brian O'Krafka   10/15/08
 *
 ******************************************************************/

#ifndef _LOCKTITE_H
#define _LOCKTITE_H

typedef int BOOLEAN;

#define FALSE  0
#define TRUE   1

#define LINE_LEN   10000

#define STRINGLEN  100
typedef char  STRING[STRINGLEN];

#define SOURCE_MAP_SIZE  10000000
#define FTH_MAP_SIZE     1000

#define MAX_SRC_DIRS   1000

#define GET_RECTYPE(rec)   (((rec).type_sched) & 0xffffffff)
#define GET_NSCHED(rec)    ((((rec).type_sched)>>32) & 0xffffffff)
#define SET_NSCHED(rec, n) (((rec).type_sched) |= (n << 32))


typedef struct {
    int       ndirs;
    char     *dirs[MAX_SRC_DIRS];
    SDFTLMap_t   map;
} SRCDATA;

    /* these MUST be synchronized with those in fthLock.h! */

#define LOCK_END_REC             0
#define LOCK_RD_REC              1
#define LOCK_WR_REC              2
#define LOCK_ADHOC_INIT_REC      3
#define LOCK_INIT_REC            4
#define UNLOCK_WR_REC            5
#define UNLOCK_RD_Z_REC          6
#define UNLOCK_RD_NZ_REC         7
#define DEMOTE_REC               8
#define TRY_RD_SUCC_REC          9
#define TRY_RD_FAIL_REC          10
#define TRY_WR_SUCC_REC          11
#define TRY_WR_FAIL_REC          12
#define N_RECTYPES               3

typedef struct {
    int      i;
    char    *name;
} RECTYPE_INFO;

static RECTYPE_INFO  RecType[] = {
    {LOCK_END_REC,        "END"},
    {LOCK_RD_REC,         "LOCK_RD"},
    {LOCK_WR_REC,         "LOCK_WR"},
    {LOCK_ADHOC_INIT_REC, "ADHOC_INIT"},
    {LOCK_INIT_REC,       "INIT"},
    {UNLOCK_WR_REC,       "UNLOCK_WR"},
    {UNLOCK_RD_Z_REC,     "UNLOCK_RD_Z"},
    {UNLOCK_RD_NZ_REC,    "UNLOCK_RD_NZ"},
    {DEMOTE_REC,          "DEMOTE"},
    {TRY_RD_SUCC_REC,     "RD_SUCC"},
    {TRY_RD_FAIL_REC,     "RD_FAIL"},
    {TRY_WR_SUCC_REC,     "WR_SUCC"},
    {TRY_WR_FAIL_REC,     "WR_FAIL"},
};

/*****************************************************************
 *
 *  Trace Record Formats:
 *  ---------------------
 *
 *  (Note: all fields are uint64_t)
 *
 *  adhoc init record:   (type_sched, t, plock, fth, bt0, bt1, ..., btn-1)
 *
 *  regular init record: (type_sched, t, plock, fth, bt0, bt1, ..., btn-1)
 *
 *  lock read record:    (type_sched, t, plock, fth)
 *
 *  lock write record:   (type_sched, t, plock, fth)
 *
 *  unlock record:       (type_sched, t, plock, fth)
 *
 *  demote record:       (type_sched, t, plock, fth)
 *
 *  try read succeed:    (type_sched, t, plock, fth)
 *
 *  try read fail:       (type_sched, t, plock, fth)
 *
 *  try write succeed:   (type_sched, t, plock, fth)
 *
 *  try write fail:      (type_sched, t, plock, fth)
 *
 *  end of trace file:   (0)
 *
 *****************************************************************/

#define BT_SIZE    4

#define SHORT_REC_SIZE  4
#define LONG_REC_SIZE   (SHORT_REC_SIZE + BT_SIZE)

typedef struct {
    uint64_t     type_sched; /*  upper 32 bits: scheduler; 
                              *  lower 32 bits: rectype */
    uint64_t     t;
    uint64_t     plock;
    uint64_t     fth;
    uint64_t     bt[BT_SIZE];
} RECORD;

#define NO_INTERVAL   0
#define RD_INTERVAL   1
#define WR_INTERVAL   2

typedef struct {
    uint64_t     counts[N_RECTYPES];
    double       n_rd;
    double       sum_time_rd;
    double       sum2_time_rd;
    double       n_wr;
    double       sum_time_wr;
    double       sum2_time_wr;
    int          interval_type;
    uint64_t     t_lock;
} PER_LOCK_STATS;

typedef struct ld {
    long long        nrec;
    RECORD           rec;
    PER_LOCK_STATS   stats;
    struct ld       *next;
} LOCKDATA;

#define MAX_FTH_BT  10

typedef struct {
    uint64_t         fth;
    int              i;
    char            *func_name;
    RECORD           rec;
} FTHDATA;

#define MAX_T                 UINT64_MAX
#define MAX_SCHED              20
#define MAX_FTH              1000
#define MAX_FILES_PER_SCHED  10000

typedef struct {
    int          nsched;
    int          files_per_sched[MAX_SCHED];
    int          schedmap[MAX_SCHED];
    long long    file_counts[MAX_SCHED][MAX_FILES_PER_SCHED];
} FILEDATA;

typedef struct {
    double counts[MAX_SCHED][MAX_FTH][N_RECTYPES];
    double schedrec_counts[MAX_SCHED][N_RECTYPES];
    double fthrec_counts[MAX_FTH][N_RECTYPES];
    double sched_counts[MAX_SCHED];
    double fth_counts[MAX_FTH];
} STATS;

typedef struct {
    uint64_t    addr;
    int         line;
    char       *fname;
    char       *sline;
    char       *sline_single;
    char       *func;
} SRCENTRY;

#define MAX_LOCKS   10000000
    
typedef struct {
    uint64_t       t_base;
    char          *trace_dir;
    char          *executable;
    char          *srcdir;
    char          *srcpaths_file;
    int            nfiles;
    struct dirent **eps;
    FILE          *fmerge;
    SRCDATA        srcdata;
    SDFxTLMap_t    srcmap;
    SDFxTLMap_t    gdbmap;
    SDFTLMap_t     lockmap;
    uint64_t       nrecs;
    uint64_t       nrecs_skip;
    int            nsched;
    int            nfth;
    SDFxTLMap_t    fthmap;
    uint64_t       inv_fthmap[MAX_FTH];
    FILEDATA       filedata;
    char          *mergefile;
    STATS          stats;
    int            nlocks;
    LOCKDATA      *sorted_locks[MAX_LOCKS];
    long long      n_neg_time;
    double         f_time_normalize;
} LOCKANALYZER;

#endif   // _LOCKTITE_H
