/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/**
 * File: ht_snapshot.h
 * Description: hotkey snapshot
 *      process snapshot from hotkey buckets.
 * Author: Norman Xu Hickey Liu Mingqiang Zhuang
 */

#ifndef HT_SNAPSHOT_H
#define HT_SNAPSHOT_H

#include "ht_types.h"
#include "ht_report.h"

/**
 * Snapshot structure
 */
typedef struct ReportSnapShot {
    uint16_t        *winner_sorted;
    uint16_t        total;     
    uint16_t        used;
    uint16_t        cur;                  
} ReportSnapShot_t;

/**
 * use to copy the winner list
 */
typedef struct ReportCopyWinner {
    ReportWinnerList_t      *winner_head;
    ReportEntry_t           *winners;
    char                    *key_table;
    struct ReportSnapShot   snapshot;
    struct ReportSortEntry  *ref_sort;
} ReportCopyWinner_t;

/**
 * build snapshot for one instance
 */
char *build_instance_snapshot(ReporterInstance_t *rpt, int ntop,
                             char *rpbuf, int rpsize, 
                             struct tm *last_tm, int trace_ip);

/**
 * Sort winner bucket lists with merge-sorting.
 * Called by extract_hotkeys.
 */
void extract_hotkeys_by_sort(ReporterInstance_t *rpt, 
                             ReportCopyWinner_t *copy_winner, int maxtop);

/**
 * Reset winner lists with current entry which removed during sorting
 * and clean the contents.
 */
void reset_winner_lists(ReporterInstance_t *rpt);

/**
 * dump hot client from client reference count after sorting items
 */
void dump_hot_client(ReportSortEntry_t *clients, uint64_t nbuckets,
                     int ntop, int rpsize, char *rpbuf);

#endif
