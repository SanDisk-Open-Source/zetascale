/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: ht_alloc.h
 * Description: hotkey memory alloc/free management
 * Author: Norman Xu Hickey Liu
 */
#ifndef HT_ALLOC_H
#define HT_ALLOC_H

/* 
 * Alloc memory for hotkey related data structures from reporter
 * controlled memory buffer.
 */
void* hot_key_alloc(void *mm_ptr, int mm_size, int mm_used, int size);

/* 
 * Free memory for hotkey allocated buffers, and recycled by reporter.
 */
void hot_key_free(void *reporter, void *item);

#endif


