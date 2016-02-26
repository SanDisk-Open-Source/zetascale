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
 * File: ht_report.c
 * Description: hotkey report
 *      report the top N hot keys
 * Author: Norman Xu Hickey Liu Mingqiang Zhuang
 */

#include "ht_report.h"
#include "ht_types.h"
#include "ht_alloc.h"
#include "ht_snapshot.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/stdio.h"
#include "platform/time.h"
#include "platform/logging.h"
#include "utils/hash.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_HOTKEY, PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      "hotkey");


#ifdef CONFIG_TEST_SUPPORT
static int           N_BUCKET_PER_SLAB;
static int           MIN_WINNERS_PER_LIST;
static int           MIN_CANDIDATES_PER_LIST;
static float         CANDIDATE_RATIO;
static int           THRESHOLD;
#endif

#define INSTANCE_UNINIT            0x01830401
#define INSTANCE_RELEASED          0x01830402
#define INSTANCE_AVAILABEL         0x01830403

static int new_hotkey_reporter_instance(Reporter_t *reporter, int index,
                                        int cmd);

int plat_assert_debug(int true_flag,  const char *fun_name, int line);

/*
 * Initial reporter with memory management for keyhot reporter
 */
static Reporter_t *
hotkey_init_reporter(Reporter_t *reporter, void *mm_ptr, int mm_size,
                     int nbuckets, int maxtop, int hotkey_mode, int client_mode)
{
    int i                   = 0;
    int instance_size       = 0;
    int client_size         = 0;
    ReporterInstance_t *rpt = NULL;

    reporter = hot_key_alloc(mm_ptr, mm_size,
                             0, sizeof(Reporter_t));

    reporter->maxtop            = maxtop;
    reporter->mm_size           = mm_size;
    reporter->mm_used           = sizeof(Reporter_t);
    reporter->mm_ptr            = mm_ptr;
    reporter->mm_pstart         = mm_ptr;
    reporter->cur_instance      = 0;
    time_t now                  = time(NULL);
    reporter->last_tm           = localtime(&now);
    reporter->hotkey_mode       = hotkey_mode;
    reporter->client_mode       = client_mode;
    reporter->dump_num          = 0;

    instance_size     =  sizeof(ReporterInstance_t) * MAX_INSTANCES;
    reporter->instances   = hot_key_alloc(mm_ptr, mm_size,
                                          reporter->mm_used, instance_size);
    reporter->mm_used    += instance_size;

    for (i = 0; i < MAX_INSTANCES; i++) {
        rpt = &reporter->instances[i];

        if (reporter->hotkey_mode == HOTKEY_SEPARATE) {
            rpt->ncandidates_per_list   = CANDIDATES_PER_LIST / MAX_INSTANCES;
            rpt->nwinners_per_list      = WINNERS_PER_LIST / MAX_INSTANCES;
            rpt->nsort_winners          = (reporter->maxtop + MAX_INSTANCES - 1) 
                                          / MAX_INSTANCES;
        } else {
            rpt->ncandidates_per_list   = CANDIDATES_PER_LIST;
            rpt->nwinners_per_list      = WINNERS_PER_LIST;  
            rpt->nsort_winners          = reporter->maxtop;     
        }

        rpt->nkeys_per_winner_list      = rpt->nwinners_per_list / 4;
        rpt->nbuckets_per_winner_list   = BUCKET_PER_SLAB;
        rpt->ncandidates                = BUCKETS_SIZE * rpt->ncandidates_per_list;
        rpt->nwinner_head               = BUCKETS_SIZE / rpt->nbuckets_per_winner_list;
        rpt->nwinners                   = rpt->nwinner_head * rpt->nwinners_per_list;
        if (rpt->cmd == INSTANCE_RELEASED) {
            rpt->cmd = INSTANCE_AVAILABEL;
        } else {
            rpt->cmd = INSTANCE_UNINIT;
        }
    }

    client_size = sizeof(ReportClientEntry_t) *
                  CLIENT_BUCKETS * NCLINETS_PER_BUCKET;
    reporter->client_ref = hot_key_alloc(reporter->mm_ptr,
                                         reporter->mm_size,
                                         reporter->mm_used,
                                         client_size);
    reporter->mm_used  += client_size;
    memset(reporter->client_ref, 0, client_size);

   return (reporter);
}

/*
 * Initial winner lists in reporter.
 * Alloc winner list and then alloc winner entries.
 */
static void
hotkey_init_winners(Reporter_t *preport, int index)
{
    int i                           = 0;
    int winner_head_size            = 0;
    int key_table_size              = 0;
    int winners_size                = 0;
    ReportWinnerList_t *winner_head = NULL;
    ReportEntry_t *winner           = NULL;
    ReporterInstance_t *rpt         = &preport->instances[index];

    winner_head_size        = sizeof(ReportWinnerList_t) * rpt->nwinner_head;
    rpt->winner_head        = hot_key_alloc(preport->mm_ptr, preport->mm_size,
                                            preport->mm_used, winner_head_size);
    memset(rpt->winner_head, 0, winner_head_size);
    preport->mm_used += winner_head_size;

    key_table_size = rpt->nkeys_per_winner_list * MAX_KEY_LEN 
                     * rpt->nwinner_head;
    rpt->key_table = hot_key_alloc(preport->mm_ptr, preport->mm_size,
                                   preport->mm_used, key_table_size);
    memset(rpt->key_table, 0, key_table_size);
    preport->mm_used += key_table_size;

    winners_size = rpt->nwinners * sizeof(ReportEntry_t);
    rpt->winners = hot_key_alloc(preport->mm_ptr, preport->mm_size,
                                 preport->mm_used, winners_size);
    memset(rpt->winners, 0, winners_size);
    preport->mm_used += winners_size;
    
    for (i = 0; i < rpt->nwinner_head; i++) {
        winner_head               = &rpt->winner_head[i];
        winner_head->threshold    = THRESHOLD;
        HotkeyInitLock(winner_head->lock);
    }

    for (i = 0; i < rpt->nwinners; i++) {
        winner = &rpt->winners[i];
        winner->key_index = INVALID_INDEX;
    }
}


/*
 * Initial candidiate. Alloc space for candidate entries.
 */
static void
hotkey_init_candidates(Reporter_t *preport, int index)
{
    int i                   = 0;
    int candidates_size     = 0;
    ReportEntry_t *candidate = NULL;
    ReporterInstance_t *rpt = &preport->instances[index];

    candidates_size = rpt->ncandidates * sizeof(ReportEntry_t);
    rpt->candidates = hot_key_alloc(preport->mm_ptr, preport->mm_size,
                                    preport->mm_used, candidates_size);
    memset(rpt->candidates, 0, candidates_size);
    preport->mm_used += candidates_size;
    
    for (i = 0; i < rpt->ncandidates; i++) {
        candidate = &rpt->candidates[i];
        candidate->key_index = INVALID_INDEX;
    }
}


/*
 * Look up candidate list with LRU
 * 1. search syndrome exists in candidate list;
 * 2. if exist, update LRU with refcount++;
 * No matter candidate list is full or not.
 * Return 1 if success, else return 0.
 */
static int
lookup_candidate(ReporterInstance_t *rpt, uint64_t syndrome,
                 uint32_t client_ip, client_mode_t mode)
{
    int i                       = 0;
    int match                   = 0;
    int candidate_index         = CANDIDATE_INDEX(rpt, syndrome);
    ReportEntry_t  *candidate   = &rpt->candidates[candidate_index];
  
    for (i = 0; i < rpt->ncandidates_per_list; i++, candidate++) {
        if (mode == CLIENT_ACCESS) {
            match = ((candidate->syndrome != 0) && 
                     (candidate->syndrome == syndrome));
        } else {
            match = ((candidate->syndrome != 0) &&
                     (candidate->syndrome == syndrome) &&
                     (candidate->client_ip == client_ip));
        }

        if (match) {
            candidate->refcount++;
            return (1);
        }
    }

    return (0);
}

/*
 * Force update candidate, delete the head, add new candiate to 
 * tail. Candidate has no entry for syndrome. 
 * Return 1 if success, else return 0.
 */
static int
force_update_candidate_list(ReporterInstance_t *rpt, uint64_t syndrome,
                            uint32_t client_ip, int refcount)
{
    int tail_index            = 0;
    int candidate_index       = CANDIDATE_INDEX(rpt, syndrome);
    ReportEntry_t  *candidate = &rpt->candidates[candidate_index];

    /*
     *  no matter candidate list is full or not, delete the head
     *  and add the new candidate at the tail
     */
    tail_index = rpt->ncandidates_per_list - 1;
    if (candidate[tail_index].syndrome != 0) {
        /* the tail candidate is used, move forward */
        memmove(candidate, &candidate[1], 
                (rpt->ncandidates_per_list - 1) * sizeof(ReportEntry_t));
    }
    candidate[tail_index].syndrome = syndrome;
    candidate[tail_index].client_ip = client_ip;
    candidate[tail_index].refcount = refcount;
    candidate[tail_index].key_index = INVALID_INDEX;

    return (0);
}

/*
 * Compare function for qsort
 */
static int
compare_refcount(const void *a, const void *b)
{
    return (((ReportEntry_t *)b)->refcount - ((ReportEntry_t *)a)->refcount);
}

/*
 * Update winner list with refcount
 * 1. search syndrome exists in winner list;
 * 2. if exist, update entry with refcount++, and then sort list by
 *    refcount;
 * Return 1 if success, else return 0.
 */
static int
update_winner_list(ReporterInstance_t *rpt, uint64_t syndrome, 
                   char *key, int key_len, uint32_t client_ip, 
                   client_mode_t mode, int lookup)
{
    int i                           = 0;
    int tail_refcount               = 0;
    ReportWinnerList_t *winner_head = &rpt->winner_head[WINNER_LIST_INDEX(rpt, syndrome)];
    int tail_index                  = WINNER_INDEX(rpt, syndrome) 
                                      + rpt->nwinners_per_list - 1;
    ReportEntry_t *winner           = &rpt->winners[WINNER_INDEX(rpt, syndrome)];
    int key_index                   = WINNER_KEY_INDEX(rpt, syndrome);
    char *key_ptr                   = NULL;
    int match                       = 0;

    /* find the new_head entry in list with syndrome */
    if (lookup) {
        for (i = 0; i < rpt->nwinners_per_list; i++) {
            if (winner_head->used == 0) {
               /*
                * no need to sort as already empty entry met,
                * just return
                */
                return (0);            
            }

            /*
             * look up the syndrome and key
             */
            if (mode == CLIENT_ACCESS) {
                match = ((winner[i].syndrome != 0) && 
                         (winner[i].syndrome == syndrome));
            } else {
                match = ((winner[i].syndrome != 0) &&
                         (winner[i].syndrome == syndrome) &&
                         (winner[i].client_ip == client_ip));
            }

            if (match) {
                winner[i].refcount++;
                break;
            }
        } 

        if (!match) {
            return (0);
        }
    }

    /* sort winner list */
    qsort(&rpt->winners[WINNER_INDEX(rpt, syndrome)], rpt->nwinners_per_list, 
          sizeof(ReportEntry_t), compare_refcount);

    /** 
     *  all the keys for the winner list are used, we should ensure
     *  the front several winners has key.
     */
    for (i = 0; i < rpt->nkeys_per_winner_list; i++) {
        match = 0;
        if (mode == CLIENT_ACCESS) {
            match = ((winner[i].syndrome != 0) && 
                     (winner[i].syndrome == syndrome) &&
                     (winner[i].key_index == INVALID_INDEX));
        } else {
            match = ((winner[i].syndrome != 0) &&
                     (winner[i].syndrome == syndrome) &&
                     (winner[i].client_ip == client_ip) &&
                     (winner[i].key_index == INVALID_INDEX));
        }

        if (match) {
            if (winner_head->key_used < rpt->nkeys_per_winner_list) {
                key_ptr = rpt->key_table + (key_index + winner_head->key_used)
                          * MAX_KEY_LEN;
                snprintf(key_ptr, key_len + 1, "%s", key);
                winner[i].key_index = winner_head->key_used;
                winner_head->key_used++;
            } else {
                /* check whether a winner needn't own key */
                if (winner[rpt->nkeys_per_winner_list].key_index != INVALID_INDEX) {
                    key_ptr = rpt->key_table 
                        + (key_index + winner[rpt->nkeys_per_winner_list].key_index )
                        * MAX_KEY_LEN;
                    snprintf(key_ptr, key_len + 1, "%s", key);
                    winner[i].key_index = winner[rpt->nkeys_per_winner_list].key_index;
                    winner[rpt->nkeys_per_winner_list].key_index = INVALID_INDEX;
                }                
            }
            break;
        }
    }

    /* update threshold */
    tail_refcount = rpt->winners[tail_index].refcount;
    if (tail_refcount > 0 && winner_head->threshold < tail_refcount) {
        winner_head->threshold = tail_refcount;
    }

    return (1);
}

/*
 * Compare and swap candiate head with winner tail if candidate head
 * refcount is over threshold.
 * Return 1 if success, else return 0.
 */
static int
compare_and_swap_candidate_winner(ReporterInstance_t *rpt, 
                                  uint64_t syndrome,
                                  uint32_t client_ip)
{
    int i                           = 0;
    int max_refcount                = 0;
    int max_refcount_index          = 0;
    ReportWinnerList_t *winner_head = &rpt->winner_head[WINNER_LIST_INDEX(rpt, syndrome)];
    int tail_index                  = WINNER_INDEX(rpt, syndrome) 
                                      + rpt->nwinners_per_list - 1;
    ReportEntry_t *winner_tail      = &rpt->winners[tail_index];
    ReportEntry_t old_winner_tail;
    int candidate_index             = CANDIDATE_INDEX(rpt, syndrome);
    ReportEntry_t  *candidate       = &rpt->candidates[candidate_index];

    /* look up the candidate with maximum refcount */
    for (i = 0; i < rpt->ncandidates_per_list; i++) {
        if (max_refcount < candidate[i].refcount) {
            max_refcount = candidate[i].refcount;
            max_refcount_index = i;
        }
    }

    /* compare the candidate head refcount with threshold */
    if (max_refcount > winner_head->threshold &&
        max_refcount > winner_tail->refcount) {

        /* swap winner tail with candidate head by coping content */
        old_winner_tail.syndrome    = winner_tail->syndrome;
        old_winner_tail.refcount    = winner_tail->refcount;
        old_winner_tail.client_ip   = winner_tail->client_ip;

        winner_tail->syndrome       = candidate[max_refcount_index].syndrome;
        winner_tail->refcount       = candidate[max_refcount_index].refcount;
        winner_tail->client_ip      = client_ip;
        winner_tail->key_index      = INVALID_INDEX;

        if (old_winner_tail.syndrome == 0) {
            if (winner_head->used < rpt->nwinners_per_list) {
                winner_head->used++;
            }

            /* delete the candidate with maximum refcount */
            if (max_refcount_index < rpt->ncandidates_per_list - 1) {
                memmove(&candidate[max_refcount_index], 
                        &candidate[max_refcount_index + 1],
                        (rpt->ncandidates_per_list - max_refcount_index - 1)
                        * sizeof(ReportEntry_t));
            }
            /* reset the last candidate */
            candidate[rpt->ncandidates_per_list - 1].client_ip = 0;
            candidate[rpt->ncandidates_per_list - 1].key_index = INVALID_INDEX;
            candidate[rpt->ncandidates_per_list - 1].refcount = 0;
            candidate[rpt->ncandidates_per_list - 1].syndrome = 0;
        } else {
            force_update_candidate_list(rpt, old_winner_tail.syndrome, 
                                        old_winner_tail.client_ip, 
                                        old_winner_tail.refcount);
        }

        return (1);
    }

    return (0);
}


/*
 *  Calculate the exactly memory exhausted in hotkey
 *  The calcualation method must follow the init methods
 *  for each structure that in Reporter_t.
 */
int
calc_hotkey_memory(int maxtop, int nbuckets, int reporter_flag)
{
#ifdef CONFIG_TEST_SUPPORT
    FILE *fd;
    char item[50];
    int value;
    fd = fopen("hotkey.config", "r");
    if (fd == NULL) {
        perror("config file open failed");
    }
    while (fscanf(fd, "%s %d", item, &value) != -1) {
        if (strcmp(item, "N_BUCKET_PER_SLAB") == 0) {
            N_BUCKET_PER_SLAB = value;
        } else if (strcmp(item, "MIN_WINNERS_PER_LIST") == 0) {
            MIN_WINNERS_PER_LIST = value;
        } else if (strcmp(item, "MIN_CANDIDATES_PER_LIST") == 0) {
            MIN_CANDIDATES_PER_LIST = value;
        } else if (strcmp(item, "CANDIDATE_RATIO") == 0) {
            CANDIDATE_RATIO = (float)(value / 100);
        } else if (strcmp(item, "THRESHOLD") == 0) {
            THRESHOLD = value;
        }
    }

    fclose(fd);
#endif
    int hotkey_mode = HOTKEY_ACCESS;
    int flag        = reporter_flag & HOTKEY_CLI_FLAGS_MASK;

    if (flag == 0x01 || flag == 0x03) {
        hotkey_mode = HOTKEY_SEPARATE;
    }

    int ncandidates_per_list    = 0;
    int nwinners_per_list       = 0;
    // int snapshot_entries        = 0;
    if (hotkey_mode == HOTKEY_SEPARATE) {
        ncandidates_per_list    = CANDIDATES_PER_LIST / MAX_INSTANCES;
        nwinners_per_list       = WINNERS_PER_LIST / MAX_INSTANCES;
        // snapshot_entries        = (maxtop + MAX_INSTANCES - 1) / MAX_INSTANCES;
    } else {
        ncandidates_per_list    = CANDIDATES_PER_LIST;
        nwinners_per_list       = WINNERS_PER_LIST;    
        // snapshot_entries        = maxtop;  
    }

    int nkeys_per_winner_list   = nwinners_per_list / 4;
    int nbuckets_per_slab       = BUCKET_PER_SLAB;
    int ncandidates             = BUCKETS_SIZE * ncandidates_per_list;
    int nwinner_head            = BUCKETS_SIZE / nbuckets_per_slab;
    int nwinners                = nwinner_head * nwinners_per_list;

    /* calculate reporter instance cost memory */
    int mem_size        = 0;
    mem_size           += sizeof(Reporter_t);
    mem_size           += sizeof(ReporterInstance_t) * MAX_INSTANCES;

    int hotclient_size  = sizeof(ReportClientEntry_t) *
                          CLIENT_BUCKETS * NCLINETS_PER_BUCKET;
    mem_size           += hotclient_size;

    /* calculate each instance cost memory */
    int each_instance_cost  = 0;
    int winner_head_size    = sizeof(ReportWinnerList_t) * nwinner_head;
    int winners_size        = nwinners * sizeof(ReportEntry_t);
    each_instance_cost     += winner_head_size;
    each_instance_cost     += winners_size;

    int key_table_size      = nkeys_per_winner_list * MAX_KEY_LEN * nwinner_head;
    each_instance_cost     += key_table_size;

    int candidates_size     = ncandidates * sizeof(ReportEntry_t);
    each_instance_cost     += candidates_size;

    /* sum memory is report + all instance */
    if (hotkey_mode == HOTKEY_ACCESS) {
        mem_size += each_instance_cost;
    } else {
        mem_size += MAX_INSTANCES * each_instance_cost;
    }

#ifdef CONFIG_TEST_SUPPORT
    fd = fopen("hotkey.config.memory", "w");
    if (fd == NULL) {
        perror("config file open failed");
    }
    fprintf(fd, "mem %d\n", mem_size);
    fclose(fd);
#endif

    return (mem_size);
}


/*
 * Init hot key report system. nbuckets is the bucket total number.
 * maxtop is the maxinum top value the hot key reporter should support.
 * hotkey_mode : [CLIENT_ACCESS | CLIENT_SEPARATE]
 *                [HOTKEY_ACCESS | HOTKEY_SEPARATE]
 */
Reporter_t *
hot_key_init(void *mm_ptr, int mm_size, int nbuckets, int maxtop,
             int reporter_flag)
{
    Reporter_t *reporter = NULL;

    /* call hotkey instance init directory without register/release */
    int hotkey_mode = HOTKEY_ACCESS;
    int client_mode = CLIENT_ACCESS;
    int flag        = reporter_flag & HOTKEY_CLI_FLAGS_MASK;

    switch (flag) {
        case 0x00:
            hotkey_mode = HOTKEY_ACCESS;
            client_mode = CLIENT_ACCESS;
            break;
        case 0x01:
            hotkey_mode = HOTKEY_SEPARATE;
            client_mode = CLIENT_ACCESS;
            break;
        case 0x02:
            hotkey_mode = HOTKEY_ACCESS;
            client_mode = CLIENT_SEPARATE;
            break;
        case 0x03:
            hotkey_mode = HOTKEY_SEPARATE;
            client_mode = CLIENT_SEPARATE;
            break;
        default:
            plat_log_msg(110002,
                         LOG_CAT_HOTKEY,
                         PLAT_LOG_LEVEL_ERROR,
                         "flag is incorrect, flag=%d",
                         flag);
            return (NULL);
    }

    memset(mm_ptr, 0, mm_size);
    reporter = hotkey_init_reporter(reporter, mm_ptr, mm_size, nbuckets,
                                    maxtop, hotkey_mode, client_mode);

    if (hotkey_mode == HOTKEY_SEPARATE) {
        new_hotkey_reporter_instance(reporter, 0, HOTKEY_CMD_GET);
        new_hotkey_reporter_instance(reporter, 1, HOTKEY_CMD_UPD);
    } else if (hotkey_mode == HOTKEY_ACCESS) {
        new_hotkey_reporter_instance(reporter, 0, HOTKEY_CMD_ALL);
    }

    /*
     * dump_reporter_instance(reporter, &reporter->instances[0],
     * ALL_LISTS);
     */
    return (reporter);
}

/*
 * New reporter instance for special CMD.
 */
static int
new_hotkey_reporter_instance(Reporter_t *reporter, int index, int cmd)
{
    hotkey_init_winners(reporter, index);
    hotkey_init_candidates(reporter, index);

    reporter->instances[index].cmd = cmd;
    ++reporter->cur_instance;

    return (0);
}

/*
 * Update hot clients for supporting hotclient.
 */
static int 
hotkey_update_hotclient(uint32_t client_ip, ReportClientEntry_t *clients) {
    struct in_addr addr;
    addr.s_addr                 = client_ip;
    const char *addr_str        = (const char *)inet_ntoa(addr);
    uint64_t syndrome           = hashb((unsigned char *)addr_str, 
                                        strlen(addr_str), 0);
    int index                   = syndrome % CLIENT_BUCKETS;
    int loop                    = 0;
    ReportClientEntry_t *client = &clients[index*NCLINETS_PER_BUCKET];
    
    while (client != NULL) {
        int found =  __sync_bool_compare_and_swap(&client->client_ip, 0, client_ip);
        if (found == 1) {
            __sync_fetch_and_add(&client->refcount, 1);
            return (0);
        }

        if (client->client_ip == client_ip) {
            __sync_fetch_and_add(&client->refcount, 1);
            break;
        }
        client++;
        if (++loop > NCLINETS_PER_BUCKET)
            break;
    }

    return (0);
}

/*
 * Update instance hotkeys with sepcified winner and candidate list
 */
static int
hotkey_update_instance(ReporterInstance_t *rpt, 
                       char *key, int key_len, uint64_t hash,
                       uint32_t client_ip, client_mode_t mode)
{
    ReportWinnerList_t *winner_head = 
        &rpt->winner_head[WINNER_LIST_INDEX(rpt, hash)];

    HotkeyLock(winner_head->lock, winner_head->lock_wait);
    if (lookup_candidate(rpt, hash, client_ip, mode)) {
        /* compare and swap candidate head with winner tail */
        if (compare_and_swap_candidate_winner(rpt, hash, client_ip) == 1) {
            /*
            * update winner list resorted by refcount,
            * no need to update entry refcount
            */
            update_winner_list(rpt, hash, key, key_len, client_ip, mode, 0);
        }
    } else {
        /*
         * update winner list resorted by refcount,
         * need to update entry refcount
         */
        if (update_winner_list(rpt, hash, key, key_len, client_ip, mode, 1) == 0) {
            /* no entry in winner list, update candidate list */
            force_update_candidate_list(rpt, hash, client_ip, 1);
        } 
    }
    HotkeyUnlock(winner_head->lock, winner_head->lock_wait);

    return (0);
}


/*
 * convert from m$d CMD to hotkey CMD
 * m$d CMD refer to command_id in command.h
 */
static int
convert_to_hotkey_cmd(int cmd) {

    switch (cmd) {
        case 1: /* CMD_GET */
        case 2:    /* CMD_GETS */
            cmd = HOTKEY_CMD_GET;
            break;
        case 3: /* CMD_SET */
        case 4: /* CMD_ADD */
        case 5: /* CMD_REPLACE */
        case 6: /* CMD_APPEND */
        case 7: /* CMD_PREPEND */
        case 8: /* CMD_CAS */
        case 9: /* CMD_INCR */
        case 10: /* CMD_DECR */
        case 11: /* CMD_DELETE */
            cmd = HOTKEY_CMD_UPD;
            break;
    }

    return (cmd);
}


/*
 * Update hot key according to keys, commands and clients;
 * It is supposed to be called after client commands have been parsed.
 * The command parameter could be HOTKEY_CMD_GET, CMD_SET, CMD_ADD, etc.
 * It is thread-safe with locks.
 */
int
hot_key_update(Reporter_t *preporter, char *key, int key_len,
               uint64_t hash, uint32_t bucket, int command,
               uint32_t client_ip)
{
    int i                   = 0;
    ReporterInstance_t *rpt = NULL;
    int ninstances          = preporter->cur_instance;

    command = convert_to_hotkey_cmd(command);
    if (command != HOTKEY_CMD_GET && command != HOTKEY_CMD_UPD) {
        plat_log_msg(21052,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "unexpected command : %d",
                     command);
        return (-1);
    }

    for (i = 0; i < ninstances; i++) {
        rpt = &preporter->instances[i];

        if (ninstances == 1) {
            if (rpt->cmd != HOTKEY_CMD_ALL) {
                plat_log_msg(110003,
                             LOG_CAT_HOTKEY,
                             PLAT_LOG_LEVEL_ERROR,
                             "unexpected command type: %d for 1 instant",
                             rpt->cmd);

                return (-1);
            }
        } else if (ninstances == MAX_INSTANCES) {
            if (rpt->cmd != HOTKEY_CMD_GET &&
                rpt->cmd != HOTKEY_CMD_UPD) {
                plat_log_msg(110005,
                             LOG_CAT_HOTKEY,
                             PLAT_LOG_LEVEL_ERROR,
                             "unexpected command type:%d for 2 instants",
                             rpt->cmd);
                return (-1);
            }
        }

        if (ninstances == MAX_INSTANCES && rpt->cmd != command) {
            continue;
        }

        hotkey_update_instance(rpt, key, key_len, hash, client_ip, 
                               preporter->client_mode);
    }

    hotkey_update_hotclient(client_ip, preporter->client_ref);

    return (0);
}


/*
 * Caller want to know the ntop hot keys. It should provide string buffer.
 * command is specified as hotkey command type.
 */
int
hot_key_report(Reporter_t *preporter, int ntop, char *rpbuf, int rpsize,
               int command, uint32_t client_ip)
{
    int i                       = 0;
    ReporterInstance_t *rpt     = NULL;
    int trace_ip                = 0;
    char *pos                   = rpbuf;
    int ninstances              = preporter->cur_instance;
    int dump_ops                = 0;
    int dump_num                = __sync_add_and_fetch(&preporter->dump_num, 1);

    if (dump_num > MAX_DUMP_NUM) {
        (void)__sync_sub_and_fetch(&preporter->dump_num, 1);
        return -1;
    }

    if (preporter->client_mode == CLIENT_SEPARATE) {
        trace_ip = 1;
    }

    command = convert_to_hotkey_cmd(command);
    if (command != HOTKEY_CMD_GET && command != HOTKEY_CMD_UPD) {
        plat_log_msg(21052,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "unexpected command : %d",
                     command);
        (void)__sync_sub_and_fetch(&preporter->dump_num, 1);
        return (-1);
    }

    if (ntop > preporter->maxtop) {
        plat_log_msg(21066,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_DEBUG,
                     "ntop is larger than maxtop: ntop=%d, maxtop=%d",
                     ntop, preporter->maxtop);
        ntop = preporter->maxtop;
    }

    if (ninstances == MAX_INSTANCES) {
        dump_ops = (ntop + MAX_INSTANCES - 1) / MAX_INSTANCES;
    } else {
        dump_ops = ntop;
    }
    
    for (i = 0; i < ninstances; i++) {
        rpt = &preporter->instances[i];

        /* only dump one hotkey */
        if (i == 1 && ntop == 1) {
            continue;
        } else if (i == 1 && ntop % MAX_INSTANCES != 0) {
            /* FIXME: only MAX_INSTANCES==2 can do like this */
            dump_ops--;
        }

        pos = build_instance_snapshot(rpt, dump_ops, pos, rpsize,  
                                      preporter->last_tm, trace_ip);

        if (pos == NULL) {
            (void)__sync_sub_and_fetch(&preporter->dump_num, 1);
            return -1;
        }
    }
    (void)__sync_sub_and_fetch(&preporter->dump_num, 1);

    return (0);
}

/*
 * Caller want to know the ntop hot clients. It should provide string buffer.
 */
int
hot_client_report(Reporter_t *preporter, int ntop, char *rpbuf, int rpsize)
{
    int i                           = 0;
    char *pos                       = rpbuf + strlen(rpbuf);
    ReportSortEntry_t *sort_client  = NULL;
    int nlists                      = CLIENT_BUCKETS * NCLINETS_PER_BUCKET;
    int dump_num                    = __sync_add_and_fetch(&preporter->dump_num, 1);

    if (dump_num > MAX_DUMP_NUM) {
        (void)__sync_sub_and_fetch(&preporter->dump_num, 1);
        return -1;
    }

    sort_client = plat_alloc(sizeof(ReportSortEntry_t) 
                             * preporter->instances[0].nwinner_head);
    if (sort_client == NULL) {
        plat_log_msg(100016,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "Allocate memory for sorted client failed."); 
        (void)__sync_sub_and_fetch(&preporter->dump_num, 1); 
        return -1;  
    }

    pos += snprintf(pos, rpsize, "HOTCLIENT\r\n");
    if (nlists > preporter->instances[0].nwinner_head) {
        nlists = preporter->instances[0].nwinner_head;
    }
    for (i = 0; i < nlists; i++) {
        sort_client[i].index       = preporter->client_ref[i].client_ip;
        sort_client[i].refcount    = preporter->client_ref[i].refcount;
    }

    dump_hot_client(sort_client, nlists, ntop, rpsize, pos);

    plat_free(sort_client);
    (void)__sync_sub_and_fetch(&preporter->dump_num, 1);

    return (0);
}



/*
 * Clean up reporter instance
 */
int
hot_key_cleanup(Reporter_t *preporter)
{
    if (__sync_bool_compare_and_swap(&preporter->dump_num, 0, MAX_DUMP_NUM)) {
        plat_free(preporter);
    } else {
        return -1;
    }

    return (0);
}

/*
 * Reset reporter hotkeys including hotkey in winner lists and clients
 */
int
hot_key_reset(Reporter_t *preporter)
{
    int i                           = 0;
    ReporterInstance_t *rpt         = NULL;
    ReportClientEntry_t *clients    = preporter->client_ref;
    int nbuckets                    = NCLINETS_PER_BUCKET * CLIENT_BUCKETS;

    for (i = 0; i < preporter->cur_instance; i++) {
        rpt = &preporter->instances[i];
        reset_winner_lists(rpt);
    }

    memset(clients, 0, sizeof(ReportClientEntry_t) * nbuckets);

    return (0);
}


#define HOT_KEY_VERSION         1

/**
 * Report hotkey support version
 */
int hot_key_report_version( char ** bufp, int * lenp )
{
    return plat_snprintfcat( bufp, lenp,
                             "hotkey %d.0.0\r\n", HOT_KEY_VERSION );
}

int plat_assert_debug(int true_flag, const char *fun_name, int line)
{
    if (!true_flag) {
        plat_log_msg(110001,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_ERROR,
                     "hotkey assertion failed: %s %d",
                     fun_name, line);
    }

    return (true_flag);
}
