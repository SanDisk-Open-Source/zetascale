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

/**
 * File: ht_snapshot.c
 * Description: hotkey snapshot
 *      process snapshot from hotkey buckets.
 * Author: Norman Xu Hickey Liu Mingqiang Zhuang
 */

#include<sys/types.h>
#include<sys/stat.h>
#include<dirent.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "ht_snapshot.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/string.h"
#include "platform/time.h"

#include "platform/logging.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_HOTKEY, PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      "hotkey");

static void reset_winner_lists_cur(ReportWinnerList_t *lists, int nlists);
static int compare(const void *a, const void *b);
static int no_entry_in_lists(ReporterInstance_t *rpt, 
                             ReportWinnerList_t *lists, int nlists);

/*
 * Dump snapshot to file with the format wihch would be post here.
 * It is used to support history snapshots cacluating.
 */
static char *dump_snapshot(ReporterInstance_t *rpt, 
                           ReportCopyWinner_t *copy_winner, 
                           int ntop, char *rpbuf, int rpsize, 
                           struct tm *last_tm, int trace_ip);

extern int plat_assert_debug(int true_flag, const char *fun_name, int line);


/*
 * Compare function for qsort
 */
static int
compare(const void *a, const void *b)
{
    return (((ReportSortEntry_t *)b)->refcount -
            ((ReportSortEntry_t *)a)->refcount);
}

/*
 * Judge whether entry heads are null for all lists.
 * Return 0 if one entry exists, or else reutrn 1.
 */
static int
no_entry_in_lists(ReporterInstance_t *rpt, 
                  ReportWinnerList_t *lists, int nlists)
{
    int i = 0;

    for (i = 0; i < nlists; i++) {
        if (lists[i].cur_itr < lists[i].used
            && (int)lists[i].cur_itr < rpt->nkeys_per_winner_list) {
            return (0);
        }
    }
    return (1);
}


/*
 * Reset cur entry for winner lists as it's not grantee during updating.
 */
static void
reset_winner_lists_cur(ReportWinnerList_t *lists, int nlists)
{
    int i = 0;

    for (i = 0; i < nlists; i++) {
        lists[i].cur_itr = 0;
    }
}

/*
 * Reset winner lists with current entry which removed during sorting
 * and clean the contents.
 */
void
reset_winner_lists(ReporterInstance_t *rpt)
{
    int i,j,k                       = 0;
    int ncandidate_per_winner_list  = rpt->nbuckets_per_winner_list 
                                      * rpt->ncandidates_per_list;
    ReportWinnerList_t *winner_head = rpt->winner_head;    
    ReportEntry_t *winner           = NULL;
    ReportEntry_t *candidate        = NULL;
    
    for (i = 0; i < rpt->nwinner_head; i++) {
        HotkeyLock(winner_head[i].lock, winner_head[i].lock_wait);
        winner_head[i].threshold   = THRESHOLD;
        winner_head[i].cur_itr     = 0;
        winner_head[i].used        = 0;
        winner_head[i].key_used    = 0;

        winner = &rpt->winners[WLI_TO_WI(rpt, i)];
        for (j = 0; j < rpt->nwinners_per_list; j++) {
            winner[j].key_index   = INVALID_INDEX;
            winner[j].client_ip   = 0;
            winner[j].refcount    = 0;
            winner[j].syndrome    = 0;        
        }

        candidate = &rpt->candidates[WLI_TO_CI(rpt, i)];
        for (k = 0; k < ncandidate_per_winner_list; k++) {
            candidate[k].syndrome     = 0;
            candidate[k].refcount     = 0;
            candidate[k].client_ip    = 0;
            candidate[k].key_index    = INVALID_INDEX;        
        }
        HotkeyUnlock(winner_head[i].lock, winner_head[i].lock_wait);
    }
}

/**
 * copy winner list for sort
 */
static int
copy_winner_list(ReporterInstance_t *rpt, ReportCopyWinner_t *copy_winner)
{
    int i                           = 0;
    int winner_head_size            = rpt->nwinner_head 
                                      * sizeof(ReportWinnerList_t);
    int winner_size                 = rpt->nwinners 
                                      * sizeof(ReportEntry_t);
    int key_table_size              = rpt->nkeys_per_winner_list 
                                      * MAX_KEY_LEN * rpt->nwinner_head;
    ReportWinnerList_t *winner_head = rpt->winner_head; 
    int sorted_winner_size          = rpt->nsort_winners * sizeof(uint16_t);
    int sorted_ref_size             = sizeof(ReportSortEntry_t) 
                                      * rpt->nwinner_head;

    copy_winner->winner_head = plat_alloc(winner_head_size);
    if (copy_winner->winner_head == NULL) {
        plat_log_msg(100019,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Fail to allocate memory for copy of winner head.");  
        return -1;  
    }
    memset(copy_winner->winner_head, 0, winner_head_size);

    copy_winner->winners = plat_alloc(winner_size);
    if (copy_winner->winners == NULL) {
        plat_log_msg(100020,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Fail to allocate memory for copy of winners.");  
        return -1;      
    }
    memset(copy_winner->winners, 0, winner_size);

    copy_winner->key_table = plat_alloc(key_table_size);
    if (copy_winner->key_table == NULL) {
        plat_log_msg(100021,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Fail to allocate memory for copy of key table.");  
        return -1;       
    }
    memset(copy_winner->key_table, 0, key_table_size);

    for (i = 0; i < rpt->nwinner_head; i++) {
        HotkeyLock(winner_head[i].lock, winner_head[i].lock_wait);
        memcpy(&copy_winner->winner_head[i], &winner_head[i], 
               sizeof(ReportWinnerList_t));

        memcpy(&copy_winner->winners[WLI_TO_WI(rpt, i)], 
               &rpt->winners[WLI_TO_WI(rpt, i)], 
               sizeof(ReportEntry_t) * rpt->nwinners_per_list);

        memcpy(&copy_winner->key_table[WLI_TO_WKI(rpt, i) * MAX_KEY_LEN], 
               &rpt->key_table[WLI_TO_WKI(rpt, i) * MAX_KEY_LEN],
               MAX_KEY_LEN * rpt->nkeys_per_winner_list);
        HotkeyUnlock(winner_head[i].lock, winner_head[i].lock_wait);
    }

    copy_winner->snapshot.winner_sorted = plat_alloc(sorted_winner_size);
    if (copy_winner->snapshot.winner_sorted == NULL) {
        plat_log_msg(100022,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Fail to allocate memory for snapshot sorted winners.");  
        return -1;       
    }
    memset(copy_winner->snapshot.winner_sorted, 0, sorted_winner_size);
    copy_winner->snapshot.total = rpt->nsort_winners;
    copy_winner->snapshot.cur   = 0;
    copy_winner->snapshot.used  = 0;

    copy_winner->ref_sort   = plat_alloc(sorted_ref_size);
    if (copy_winner->ref_sort == NULL) {
        plat_log_msg(100023,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Fail to allocate memory for ref sorted winners.");  
        return -1;       
    }
    memset(copy_winner->ref_sort, 0, sorted_ref_size);

    return 0;
}

/**
 * free the temp memory for storing winner list
 */
static void
free_copy_winner_list(ReportCopyWinner_t *copy_winner)
{
    if (copy_winner->winner_head != NULL) {
        plat_free(copy_winner->winner_head);
        copy_winner->winner_head = NULL;
    }

    if (copy_winner->winners != NULL) {
        plat_free(copy_winner->winners);
        copy_winner->winner_head = NULL;    
    }

    if (copy_winner->key_table != NULL) {
        plat_free(copy_winner->key_table);
        copy_winner->key_table = NULL;    
    }

    if (copy_winner->snapshot.winner_sorted != NULL) {
        plat_free(copy_winner->snapshot.winner_sorted);
        copy_winner->snapshot.winner_sorted = NULL;
    }

    if (copy_winner->ref_sort != NULL) {
        plat_free(copy_winner->ref_sort);
        copy_winner->ref_sort = NULL;
    }
}

/*
 * Sort winner bucket lists with merge-sorting.
 * Called by extract_hotkeys.
 */
void
extract_hotkeys_by_sort(ReporterInstance_t *rpt, 
                        ReportCopyWinner_t *copy_winner, int maxtop)
{
    int i                               = 0;
    int sorted_num                      = 0;
    ReportWinnerList_t *lists           = copy_winner->winner_head;
    int nlists                          = rpt->nwinner_head;
    ReportEntry_t *winners              = copy_winner->winners;
    ReportSnapShot_t *snapshot          = &copy_winner->snapshot;
    ReportSortEntry_t *ref_sort         = copy_winner->ref_sort;
    ReportWinnerList_t *max_list_head   = NULL;
    int winner_index                    = 0;  

    reset_winner_lists_cur(lists, nlists);

    /* initial ref_sort list with head entry from each lists */
    for (i = 0; i < nlists; i++) {
        ref_sort[i].index       = i;
        ref_sort[i].refcount    = winners[i * rpt->nwinners_per_list].refcount;
    }

    while (!no_entry_in_lists(rpt, lists, nlists)) {
        qsort(ref_sort, nlists, sizeof(ReportSortEntry_t), compare);

        if (ref_sort[0].refcount == 0) {
            /* all items are filled with 0 refcount */
            break;
        }

        if (lists[ref_sort[0].index].used == 0) {
            plat_log_msg(21063,
                         LOG_CAT_HOTKEY,
                         PLAT_LOG_LEVEL_WARN,
                         "max_list_head is NULL, list=%p",
                         (void*) &lists[ref_sort[0].index]);
            break;
        }

        /* fetch new head from lists */
        max_list_head = &lists[ref_sort[0].index];
        winner_index = ref_sort[0].index * rpt->nwinners_per_list
                       + max_list_head->cur_itr;
        if (max_list_head->cur_itr < max_list_head->used 
            && max_list_head->cur_itr < rpt->nkeys_per_winner_list
            && winners[winner_index].key_index != INVALID_INDEX) {

            snapshot->winner_sorted[snapshot->used++] = winner_index;
            max_list_head->cur_itr++;
            if (++sorted_num >= maxtop) {
                break;
            }

            if (max_list_head->cur_itr < max_list_head->used
                && max_list_head->cur_itr < rpt->nkeys_per_winner_list
                && winners[winner_index + 1].key_index != INVALID_INDEX) {
                ref_sort[0].refcount = winners[winner_index + 1].refcount;
            } else {
                ref_sort[0].refcount = 0;
            }
        } else {
            /* invalid flag to fill sort item head */
            ref_sort[0].refcount = 0;
        }
    }

    reset_winner_lists_cur(lists, nlists);
}

/*
 * dump hot client from client reference count after sorting items
 */
void
dump_hot_client(ReportSortEntry_t *clients, uint64_t nbuckets,
                int ntop, int rpsize, char *rpbuf)
{
    if (ntop > nbuckets) {
        plat_log_msg(21064,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_WARN,
                     "ntop is larger than nbuckets: ntop=%d,nbuckets=%"PRIu64,
                     ntop, nbuckets);
        ntop = nbuckets;
    }
    qsort(clients, nbuckets, sizeof(ReportSortEntry_t), compare);

    char *tmp   = rpbuf;
    int loops   = 0;
    int i       = 0;
    struct in_addr addr;

    for (i = 0; i < nbuckets; i++) {
        if (clients[i].refcount == 0) {
            break;
        }

        if (++loops <= ntop) {
            addr.s_addr = (clients[i].index);
            rpbuf += snprintf(rpbuf, rpsize, "%d %s\n",
                              clients[i].refcount,
                              inet_ntoa(addr));
        }
    }

    rpbuf = tmp;
}

/*
 * Dump snapshot to file with the format wihch would be post here.
 * It is used to support history snapshots cacluating.
 */
static char *
dump_snapshot(ReporterInstance_t *rpt, ReportCopyWinner_t *copy_winner, 
              int ntop, char *rpbuf, int rpsize, 
              struct tm *last_tm, int trace_ip)
{
    ReportSnapShot_t *snapshot  = &copy_winner->snapshot;
    ReportEntry_t *winner       = &copy_winner->winners[snapshot->winner_sorted[0]];
    char *pos                   = rpbuf;
    int loops                   = 0;
    int key_index               = 0;
    char *key_ptr               = NULL;
    struct in_addr addr;

    if (ntop > snapshot->used) {
        plat_log_msg(100012,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_DEBUG,
                     "ntop is larger than statop: ntop=%d, statop=%d",
                     ntop, snapshot->used);
        ntop = snapshot->used;
    }

    if (rpt->cmd == HOTKEY_CMD_GET) {
        pos += snprintf(pos, rpsize, "HOTKEY_GET\r\n");
    } else if (rpt->cmd == HOTKEY_CMD_UPD) {
        pos += snprintf(pos, rpsize, "HOTKEY_UPDATE\r\n");
    } else {
        pos += snprintf(pos, rpsize, "HOTKEY_ACCESS\r\n");
    }

    if (snapshot->used == 0) {
        return pos;
    }

    while (winner != NULL) {
        if (winner->key_index != INVALID_INDEX) {
            if (++loops <= ntop) {
                addr.s_addr = winner->client_ip;
                key_index = WINNER_KEY_INDEX(rpt, winner->syndrome);
                key_ptr = copy_winner->key_table + (key_index + winner->key_index)
                          * MAX_KEY_LEN;
                pos += snprintf(pos, rpsize, "%s %d %s\n",
                                (trace_ip ? inet_ntoa(addr): "0.0.0.0"),
                                winner->refcount, key_ptr);
            } else {
                break;
            }
        }

        if (++snapshot->cur < snapshot->used) {
            winner = &copy_winner->winners[snapshot->winner_sorted[snapshot->cur]];
        } else {
            break;
        }
    }

    snapshot->cur = 0;
    snapshot->used = 0;

    return pos;
}

/*
 * build snapshot for one instance
 */
char * 
build_instance_snapshot(ReporterInstance_t *rpt, int ntop,
                        char *rpbuf, int rpsize, 
                        struct tm *last_tm, int trace_ip)
{
    char *pos                       = NULL;
    ReportCopyWinner_t copy_winner;

    if (copy_winner_list(rpt, &copy_winner) != 0) {
        free_copy_winner_list(&copy_winner);
        return NULL;
    }

    extract_hotkeys_by_sort(rpt, &copy_winner, ntop);
    pos = dump_snapshot(rpt, &copy_winner, ntop, rpbuf, rpsize, last_tm, trace_ip);

    free_copy_winner_list(&copy_winner);

    return pos;
}
