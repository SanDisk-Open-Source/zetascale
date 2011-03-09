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


