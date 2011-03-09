/*
 * File: msg_int.h
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 */
#ifndef MSG_INT_H
#define MSG_INT_H

#include "locks.h"


/*
 * Type definitions.
 */
typedef int cb_func_t(void *arg);


/*
 * Callback list entry.
 */
typedef struct cb_each {
    struct cb_each *next;               /* Next entry */
    cb_func_t      *func;               /* Function to call */
    void           *arg;                /* Argument */
} cb_each_t;


/*
 * Callback list header.
 */
typedef struct cb_list {
    cb_each_t *head;                    /* Head of list */
    cb_each_t *tail;                    /* Tail of list */
    wlock_t   *lock;                    /* For locking accesses */
} cb_list_t;


/*
 * Option table.
 */
typedef struct {
    char     *name;                     /* Parameter name */
    uint64_t  bits;                     /* Corresponding bits */
} opts_t;


/*
 * Function prototypes.
 */
int  cb_callone(cb_list_t *list, void *arg);
int  cb_add(cb_list_t *fclist, cb_func_t func, void *arg);
void cb_free(cb_list_t *list);
void cb_callrem(cb_list_t *list);
void cb_init(cb_list_t *list, int lock);
void cb_callarg(cb_list_t *list, void *arg);
void opts_set(uint64_t *all, opts_t *opts, char *name, char *data);

#endif /* MSG_INT_H */
