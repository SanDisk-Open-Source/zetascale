/**
 * File: ht_types.h
 * Description: hotkey types
 *      types defination for hotkey reporter
 * Author: Norman Xu Hickey Liu Mingqiang Zhuang
 */
#ifndef HT_TYPES_H
#define HT_TYPES_H

#include <inttypes.h>
#include "fth/fth.h"

#define NCLINETS_PER_BUCKET         32
#define CLIENT_BUCKETS              128

#define MAX_KEY_LEN                 251   /* an extra charactor for '\0' */
#define MAX_INSTANCES               2
#define THRESHOLD                   0

#define BUCKET_PER_SLAB             16
#define WINNERS_PER_LIST            16
#define CANDIDATES_PER_LIST         4
#define MAX_DUMP_NUM                4

#define INVALID_INDEX ((uint8_t)0xff)
#define HASH_POWER 16
#define HASH_SIZE(n) ((uint32_t)1<<(n))
#define HASH_MASK(n) (HASH_SIZE(n)-1)
#define BUCKETS_SIZE (HASH_SIZE(HASH_POWER))
#define BUCKET_INDEX(syndrome) (syndrome & HASH_MASK(HASH_POWER))

/* syndrome to candidate index */
#define CANDIDATE_INDEX(rpt, syndrome) \
    (BUCKET_INDEX(syndrome) * rpt->ncandidates_per_list)

/* syndrome to winner list index */
#define WINNER_LIST_INDEX(rpt, syndrome) \
    ((int)(BUCKET_INDEX(syndrome) / rpt->nbuckets_per_winner_list))

/* syndrome to winner index */
#define WINNER_INDEX(rpt, syndrome) \
    (WINNER_LIST_INDEX(rpt, syndrome) * rpt->nwinners_per_list)

/* winner list index to winner index */
#define WLI_TO_WI(rpt, wli) \
    (wli * rpt->nwinners_per_list)

/* syndrome to key index */
#define WINNER_KEY_INDEX(rpt, syndrome) \
    (WINNER_LIST_INDEX(rpt, syndrome) * rpt->nkeys_per_winner_list)

/* winner list index to winner key index */
#define WLI_TO_WKI(rpt, wli)    \
    (wli * rpt->nkeys_per_winner_list)

/* winner list index to candidate index */
#define WLI_TO_CI(rpt, wli)    \
    (wli * rpt->nbuckets_per_winner_list * rpt->ncandidates_per_list)

#ifdef FAKE_LOCK
#define HotkeyLockType           void*
#define HotkeyWaitType           void*
#define HotkeyInitLock(x)        (x)
#define HotkeyLock(x, w)         (w, x);
#define HotkeyUnlock(x, w)       (x, w);
#else
#define HotkeyLockType           fthLock_t
#define HotkeyWaitType           fthWaitEl_t *
#define HotkeyInitLock(x)        fthLockInit(&(x))
#define HotkeyLock(x, w)         (w = fthLock(&(x), 1, NULL))
#define HotkeyUnlock(x, w)       fthUnlock(w)
#endif

/**
 * dump hash utility types
 */
typedef enum {WINNERS, CANDIDATES, SNAPSHOTS, ALL_LISTS} dump_type_t;

/**
 * Command types for hotkey
 */
typedef enum {HOTKEY_CMD_ALL = 0, HOTKEY_CMD_GET, HOTKEY_CMD_UPD}
            cmd_type_t;

/**
 * traced mode for reporter instances
 */
typedef enum {HOTKEY_ACCESS, HOTKEY_SEPARATE} hotkey_mode_t;

/**
 * client mode for reporter instances.
 */
typedef enum {CLIENT_ACCESS, CLIENT_SEPARATE} client_mode_t;

/**
 * report entry in buckets
 */
typedef struct ReportEntry {
    uint64_t    syndrome;
    uint32_t    client_ip;
    uint32_t    refcount:24;
    uint32_t    key_index:8;
} ReportEntry_t;

/**
 * Winner list with alias for slab list
 */
typedef struct ReportWinnerList {
    HotkeyLockType  lock;
    HotkeyWaitType  lock_wait;
    uint16_t        used;
    uint16_t        cur_itr;
    uint32_t        threshold:24;
    uint32_t        key_used:8;
} ReportWinnerList_t;

/**
 * Sort Reference list for sorting
 */
typedef struct ReportSortEntry {
    uint32_t    refcount;
    int         index;
} ReportSortEntry_t;

/**
 * Hot client Reference Entry
 */
typedef struct ReportClientEntry {
    uint32_t    refcount;
    uint32_t    client_ip;
} ReportClientEntry_t;

#endif
