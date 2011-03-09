/*
 * File: ht_alloc.c
 * Description: hotkey memory alloc/free management
 * Author: Norman Xu Hickey Liu
 */

/*
 * Alloc memory for hotkey related data structures from reporter
 * controlled memory buffer.
 */
#include "platform/stdio.h"
#include "ht_alloc.h"
#include "platform/stdlib.h"
#include "platform/logging.h"

PLAT_LOG_SUBCAT_LOCAL(LOG_CAT_HOTKEY, PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                      "hotkey");


/*
 * alloc buffer from buffer pool
 */

void*
hot_key_alloc(void *mm_ptr, int mm_size, int mm_used, int size)
{
    void *p;
    if (mm_used + size > mm_size) {
        p = NULL;
    } else {
        p = mm_ptr + mm_used;
        /*
         *  exhausted size would be added after hot_key_alloc:
         *  mm_used += size;
         */
    }

    if (p == NULL) {
        plat_log_msg(21049,
                     LOG_CAT_HOTKEY,
                     PLAT_LOG_LEVEL_FATAL,
                     "No enough memory for hotkey, need %d avaiable %d",
                     size, mm_size - mm_used);
    }

    plat_log_msg(21050,
                 LOG_CAT_HOTKEY,
                 PLAT_LOG_LEVEL_TRACE,
                 "alloc: start=%p, end=%p, offset=%d, used=%d\n",
                 mm_ptr+mm_used, mm_ptr+mm_used+size, size, mm_used);

    return (p);
}

/*
 * Free memory for hotkey allocated buffers, and recycled by reporter.
 */
void
hot_key_free(void *reporter, void *item)
{
}
