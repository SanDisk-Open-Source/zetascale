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
 *  lat.h   8/29/16   Brian O'Krafka   
 *
 *  Lookaside map for tracking the most current copies of data
 *  that are in serialization buffers that haven't been written
 *  to storage.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#ifndef _LAT_H
#define _LAT_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include <string.h>

struct alat;

typedef struct alat_entry {
    void          *pdata;
    void          *lat_handle;
} alat_entry_t;

    /*   Callback for doing consistency checks on each
     *   lookaside table entry in calls to alat_check().
     */
typedef int (lat_check_cb_t)(FILE *f, void *pdata, uint64_t addr, void *ae_pdata, int locked, int is_write_lock);

struct alat *alat_init(uint32_t n_buckets, uint32_t datasize, uint32_t n_free_lists);
void alat_destroy(struct alat *pal);
void alat_dump(FILE *f, struct alat *pal);
int alat_check(FILE *f, struct alat *pal, lat_check_cb_t *lat_check_cb, void *pdata);
alat_entry_t alat_read_start(struct alat *pal, uint64_t addr);
alat_entry_t alat_write_start(struct alat *pal, uint64_t addr);
alat_entry_t alat_start(struct alat *pal, uint64_t addr, int write_flag);
alat_entry_t alat_create_start(struct alat *pal, uint64_t addr, int must_not_exist);
void alat_read_end(void *handle);
void alat_write_end(void *handle);
void alat_create_end(void *handle);
void alat_write_end_and_delete(void *handle);

#endif
