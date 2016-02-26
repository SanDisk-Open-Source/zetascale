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

/*
 * File: msg_int.c
 * Author: Johann George
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Internal routines.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tools.h"
#include "trace.h"
#include "msg_int.h"
#include "msg_cat.h"


/*
 * Initialize a callback list.  If lock is set, we use locking.  We can avoid
 * locking if only a single thread is adding to the list and items are never
 * removed.
 */
void
cb_init(cb_list_t *list, int lock)
{
    if (lock)
        list->lock = wl_init();
}


/*
 * Free a callback list.
 */
void
cb_free(cb_list_t *list)
{
    if (list->lock)
        wl_free(list->lock);
}


/*
 * Add a callback to a callback list.  Return 1 if the list was empty.
 */
int
cb_add(cb_list_t *list, cb_func_t func, void *arg)
{
    int empty = 0;
    cb_each_t *each = m_malloc(sizeof *each, "cb_each_t");

    each->next = NULL;
    each->func = func;
    each->arg  = arg;

    if (list->lock)
        wl_lock(list->lock);
    else
        barrier();

    if (!list->head) {
        empty = 1;
        list->head = list->tail = each;
    } else
        list->tail = list->tail->next = each;

    if (list->lock)
        wl_unlock(list->lock);
    return empty;
}


/*
 * Call all functions on a callback list and remove those entries.
 */
void
cb_callrem(cb_list_t *list)
{
    cb_each_t *each;

    if (!list->head)
        return;

    if (list->lock)
        wl_lock(list->lock);
    each = list->head;
    list->head = NULL;
    list->tail = NULL;
    if (list->lock)
        wl_unlock(list->lock);

    while (each) {
        cb_each_t *next = each->next;

        (*each->func)(each->arg);
        m_free(each);
        each = next;
    }
}


/*
 * Call all functions on a callback list with a given argument.
 */
void
cb_callarg(cb_list_t *list, void *arg)
{
    cb_each_t *each;

    if (list->lock)
        wl_lock(list->lock);
    for (each = list->head; each; each = each->next)
        (*each->func)(arg);
    if (list->lock)
        wl_unlock(list->lock);
}


/*
 * Call each function in succession on a callback list with the given argument
 * until one of them returns true (1).  If all functions returns false (0),
 * return false, otherwise return true.
 */
int
cb_callone(cb_list_t *list, void *arg)
{
    cb_each_t *each;

    if (list->lock)
        wl_lock(list->lock);
    for (each = list->head; each; each = each->next)
        if ((*each->func)(arg))
            break;
    if (list->lock)
        wl_unlock(list->lock);
    return each ? 1 : 0;
}


/*
 * Parse a set of options and return a bit field consisting of the appropriate
 * ones turned on.
 */
void
opts_set(uint64_t *all, opts_t *opts, char *name, char *data)
{
    char buf[64];
    int lc = '+';
    char *ptr = data;

    for (;;) {
        int c;
        int n;
        opts_t *opt;
        uint64_t bits;

        n = 0;
        for (;;) {
            c = *ptr++;
            if (c == '\0' || c == '-' || c == '+')
                break;
            if (n < sizeof(buf))
                buf[n++] = tolower(c);
        }
        if (n == sizeof(buf))
            break;
        buf[n] = '\0';

        for (opt = opts; opt->name; opt++)
            if (strcmp(opt->name, buf) == 0)
                break;

        if (opt->name)
            bits = opt->bits;
        else {
            char *end;
            bits = strtol(buf, &end, 10);
            if (end != &buf[n])
                break;
        }

        if (lc == '+')
            *all |= bits;
        else if (lc == '-')
            *all &= ~bits;
        if (c == '\0')
            return;
        lc = c;
    }
    fatal("bad %s value: %s (%s)", name, data, buf);
}
