/************************************************************************
 * 
 *  btree_list.h  May. 17, 2013   Harihara Kadayam
 * 
 *  Generic list implementation used by btree
 * 
 ************************************************************************/

#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include "btree_raw.h"

#ifndef __BTREE_LIST_H
#define __BTREE_LIST_H

typedef struct blist_node {
	struct blist_node       *next;
	void                    *data1;
	void                    *data2;
	void                    *data3;
	void                    *data4;
} blist_node_t;

typedef struct {
	blist_node_t *head;
	blist_node_t *tail;
	int          cnt;
} blist_t;

extern blist_t *blist_init(void);
extern btree_status_t blist_push_node_from_head(blist_t *l, void *data1, void *data2, void *data3, void *data4);
extern btree_status_t blist_push_list_from_head(blist_t *l, blist_t *src_list);
extern btree_status_t blist_push_node_from_tail(blist_t *l, void *data1, void *data2, void *data3, void *data4);
extern int blist_pop_node_from_head(blist_t *l, void **data1, void **data2, void **data3, void **data4);
extern void blist_end(blist_t *l, int free_nodes);

#endif // __BTREE_STACK_H
