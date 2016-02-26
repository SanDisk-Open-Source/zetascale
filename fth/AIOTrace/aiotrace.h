/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/******************************************************************
 *
 * ata -- A program to to analyze AIO trace files.
 *
 * Brian O'Krafka   10/15/08
 *
 ******************************************************************/

#ifndef _AIOTRACE_H
#define _AIOTRACE_H

typedef int BOOLEAN;

#define FALSE  0
#define TRUE   1

#define LINE_LEN   10000

#define STRINGLEN  100
typedef char  STRING[STRINGLEN];

#define FTH_MAP_SIZE     1000

   // These #define's and RECORD must track fth.h!
#define AIO_TRACE_WRITE_FLAG            (1<<0)
#define AIO_TRACE_SCHED_MISMATCH_FLAG   (1<<1)
#define AIO_TRACE_ERR_FLAG              (1<<2)

typedef struct {
    uint64_t       t_start;
    uint64_t       t_end;
    uint64_t       fth;
    int            fd;
    uint32_t       size;
    uint64_t       submitted:32;
    uint64_t       flags:8;
    uint64_t       nsched:8;
    uint64_t       spare:16;
} RECORD;

#define READ_REC                 0
#define WRITE_REC                1
#define N_RECTYPES               2

#define END_REC     99

typedef struct {
    int      i;
    char    *name;
} RECTYPE_INFO;

static RECTYPE_INFO  RecType[] = {
    {READ_REC,        "RD"},
    {WRITE_REC,       "WR"},
};

#define MAX_T                UINT64_MAX
#define MAX_SCHED            20
#define MAX_FTH              1000
#define MAX_FILES_PER_SCHED  10000
#define MAX_FD               1024

typedef struct {
    int          nsched;
    int          files_per_sched[MAX_SCHED];
    int          schedmap[MAX_SCHED];
    long long    file_counts[MAX_SCHED][MAX_FILES_PER_SCHED];
} FILEDATA;

typedef struct {
    uint64_t         fth;
    int              i;
    RECORD           rec;
} FTHDATA;

typedef struct {
    double counts[MAX_SCHED][MAX_FTH][N_RECTYPES];
    double schedrec_counts[MAX_SCHED][N_RECTYPES];
    double fthrec_counts[MAX_FTH][N_RECTYPES];
    double sched_counts[MAX_SCHED];
    double fth_counts[MAX_FTH];
} STATS;

typedef struct {
    uint64_t       t_base;
    char          *trace_dir;
    char          *plot_fname;
    int            nfiles;
    struct dirent **eps;
    uint64_t       nrecs;
    uint64_t       nrecs_skip;
    uint64_t       nrecs_skip_post_merge;
    uint64_t       n_dump_recs;
    int            nsched;
    int            nfth;
    SDFxTLMap_t    fthmap;
    uint64_t       inv_fthmap[MAX_FTH];
    FILEDATA       filedata;
    char          *mergefile;
    FILE          *fmerge;
    STATS          stats;
    long long      n_neg_time;
    double         f_time_normalize;
    int            nfd;
} AIO_ANALYZER;

#endif   // _AIOTRACE_H
