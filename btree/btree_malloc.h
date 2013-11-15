
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
