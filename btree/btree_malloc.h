/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */


#ifndef _BTREE_MALOC_H_

#define _BTREE_MALOC_H_
#include <stdbool.h>
#include <stdint.h>

uint64_t
get_tod_usecs(void);

void
btree_memcpy(void *dst, const void *src, size_t length, bool dry_run);

void	*
btree_malloc( size_t n);


void
btree_free( void *v);


#endif 
