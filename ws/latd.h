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

/**********************************************************************
 *
 *  latd.h   8/29/16   Brian O'Krafka   
 *
 *  Lookaside map for tracking the most current copies of data
 *  that are in destage buffers that haven't completed writing
 *  to storage.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _LATD_H
#define _LATD_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <time.h>
#include <string.h>

struct latd;

#define MAX_KEY_LEN   4096

typedef struct latd_entry {
    void          *pdata;
    void          *latd_handle;
} latd_entry_t;

    /*   Callback for doing consistency checks on each
     *   lookaside table entry in calls to latd_check().
     */
typedef int (latd_check_cb_t)(FILE *f, void *pdata, char *key, uint32_t key_len, void *ae_pdata, int locked, int is_write_lock);

struct latd *latd_init(uint32_t n_buckets, uint32_t datasize, uint32_t n_free_lists);
void latd_destroy(struct latd *pal);
void latd_dump(FILE *f, struct latd *pal);
int latd_check(FILE *f, struct latd *pal, latd_check_cb_t *latd_check_cb, void *pdata);
latd_entry_t latd_read_start(struct latd *pal, uint64_t cguid, char *key, uint32_t key_len);
latd_entry_t latd_write_start(struct latd *pal, uint64_t cguid, char *key, uint32_t key_len);
latd_entry_t latd_start(struct latd *pal, uint64_t cguid, char *key, uint32_t key_len, int write_flag);
latd_entry_t latd_create_start(struct latd *pal, uint64_t cguid, char *key, uint32_t key_len, int must_not_exist);
void latd_read_end(void *handle);
void latd_write_end(void *handle);
void latd_create_end(void *handle);
void latd_write_end_and_delete(void *handle);

#endif
