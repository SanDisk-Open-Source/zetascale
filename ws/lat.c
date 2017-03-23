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
 *  lat.c   8/29/16   Brian O'Krafka   
 *
 *  Lookaside map for tracking the most current copies of data
 *  that are in serialization buffers that haven't been written
 *  to storage.
 *
 * (c) Copyright 2016  Western Digital Corporation
 *
 **********************************************************************/

#include <sys/syscall.h>
#include "lat_internal.h"

__thread long LATThreadId=0;

static long my_tid()
{
    if (LATThreadId == 0) {
        LATThreadId = syscall(SYS_gettid);
    }
    return(LATThreadId);
}

static uint64_t alat_hash(uint64_t addr);

struct alat *alat_init(uint32_t n_buckets, uint32_t datasize, uint32_t n_free_lists)
{
    uint32_t   i;
    alat_t    *pal;

    //  xxxzzz fix this temporary hack!
    n_buckets = 1; // for debugging a nasty bug

    pal = (alat_t *) malloc(sizeof(alat_t));
    if (pal == NULL) {
        return(NULL);
    }
    pal->n_buckets       = n_buckets;
    pal->datasize        = datasize;
    pal->n_links_used    = 0;
    pal->n_links_total   = 0;
    pal->locks           = (RWLOCK *) malloc(n_buckets*sizeof(RWLOCK));
    if (pal->locks == NULL) {
        free(pal);
        return(NULL);
    }

    pal->lock_thrds = (long *) malloc(n_buckets*sizeof(long));
    if (pal->lock_thrds == NULL) {
        free(pal->locks);
        free(pal);
        return(NULL);
    }
    for (i=0; i<n_buckets; i++) {
	RWLOCK_INIT(&(pal->locks[i]));
        pal->lock_thrds[i] = 0;
    }

    pal->buckets         = (alat_link_t **) malloc(n_buckets*sizeof(alat_link_t *));
    if (pal->buckets == NULL) {
        free(pal->lock_thrds);
        free(pal->locks);
        free(pal);
        return(NULL);
    }
    for (i=0; i<n_buckets; i++) {
        pal->buckets[i] = NULL;
    }

    pal->n_free_lists = n_free_lists;
    pal->free_list_locks = (RWLOCK *) malloc(n_free_lists*sizeof(RWLOCK));
    if (pal->free_list_locks == NULL) {
        free(pal->buckets);
        free(pal->lock_thrds);
        free(pal->locks);
        free(pal);
        return(NULL);
    }
    pal->free_lock_thrds = (long *) malloc(n_free_lists*sizeof(long));
    if (pal->free_lock_thrds == NULL) {
        free(pal->free_list_locks);
        free(pal->buckets);
        free(pal->lock_thrds);
        free(pal->locks);
        free(pal);
        return(NULL);
    }
    for (i=0; i<n_free_lists; i++) {
	RWLOCK_INIT(&(pal->free_list_locks[i]));
	pal->free_lock_thrds[i] = 0;
    }

    pal->free_lists      = (alat_link_t **) malloc(n_free_lists*sizeof(alat_link_t *));
    if (pal->free_lists == NULL) {
        free(pal->free_lock_thrds);
        free(pal->free_list_locks);
        free(pal->buckets);
        free(pal->lock_thrds);
        free(pal->locks);
        free(pal);
        return(NULL);
    }
    for (i=0; i<n_free_lists; i++) {
        pal->free_lists[i] = NULL;
    }

    return(pal);
}

static alat_link_t *get_alat_link(struct alat *pal, uint64_t addr)
{
    uint64_t        h;
    alat_link_t    *pl;
    uint32_t        n_free;

    h        = alat_hash(addr);
    n_free = h % pal->n_free_lists;

    RW_WR_LOCK(&(pal->free_list_locks[n_free]));
    pal->free_lock_thrds[n_free] = my_tid();

    pl = pal->free_lists[n_free];
    if (pl != NULL) {
	pal->free_lists[n_free] = pl->next;
	pal->free_lock_thrds[n_free] = 0;
	RW_WR_UNLOCK(&(pal->free_list_locks[n_free]));
    } else {
	pal->free_lock_thrds[n_free] = 0;
	RW_WR_UNLOCK(&(pal->free_list_locks[n_free]));
	pl = (alat_link_t *) malloc(sizeof(alat_link_t) + pal->datasize);
	pl->table = pal;
	__sync_fetch_and_add(&(pal->n_links_total), 1);
    }

    __sync_fetch_and_add(&(pal->n_links_used), 1);
    pl->n_free = n_free;

    return(pl);
}

static void free_alat_entry(struct alat *pal, alat_link_t *pl)
{
    uint32_t        n_free;

    n_free = pl->n_free;

    RW_WR_LOCK(&(pal->free_list_locks[n_free]));
    pal->free_lock_thrds[n_free] = my_tid();

    pl->next = pal->free_lists[n_free];
    pal->free_lists[n_free] = pl;

    pal->free_lock_thrds[n_free] = 0;
    RW_WR_UNLOCK(&(pal->free_list_locks[n_free]));
    __sync_fetch_and_add(&(pal->n_links_used), -1);
}

void alat_destroy(struct alat *pal)
{
    uint32_t       i;
    alat_link_t   *pe, *pe_next;

    //  assumes nobody is using the structure anymore!
    for (i=0; i<pal->n_buckets; i++) {
       for (pe=pal->buckets[i]; pe != NULL; pe = pe_next) {
           pe_next = pe->next;
	   free(pe);
       }
    }
    for (i=0; i<pal->n_free_lists; i++) {
       for (pe=pal->free_lists[i]; pe != NULL; pe = pe_next) {
           pe_next = pe->next;
	   free(pe);
       }
    }
    free(pal->free_lists);
    free(pal->free_list_locks);
    free(pal->buckets);
    free(pal->locks);
    free(pal);
}

alat_entry_t alat_read_start(struct alat *pal, uint64_t addr)
{
    return(alat_start(pal, addr, 0));
}

// NOTE: this expects that entry already exists
alat_entry_t alat_write_start(struct alat *pal, uint64_t addr)
{
    return(alat_start(pal, addr, 1));
}

alat_entry_t alat_start(struct alat *pal, uint64_t addr, int write_flag)
{
    uint64_t       h;
    uint32_t       n_bucket;
    alat_entry_t   ae;
    alat_link_t   *pl;

    h        = alat_hash(addr);
    n_bucket = h % pal->n_buckets;

    if (write_flag) {
	RW_WR_LOCK(&(pal->locks[n_bucket]));
	pal->lock_thrds[n_bucket] = my_tid();
        // fprintf(stderr, "WR_LOCK(%d): alat_start, addr=%"PRIu64", tid=%ld\n", n_bucket, addr, my_tid());
    } else {
	RW_RD_LOCK(&(pal->locks[n_bucket]));
	pal->lock_thrds[n_bucket] = my_tid();
    }

    for (pl = pal->buckets[n_bucket]; pl != NULL; pl = pl->next) {
        if (pl->addr == addr) {
	    break;
	}
    }
    if (pl == NULL) {
	if (write_flag) {
	    pal->lock_thrds[n_bucket] = 0;
	    RW_WR_UNLOCK(&(pal->locks[n_bucket]));
            // fprintf(stderr, "WR_UNLOCK(%d): alat_start, addr=%"PRIu64", tid=%ld\n", n_bucket, addr, my_tid());
	} else {
	    pal->lock_thrds[n_bucket] = 0;
	    RW_RD_UNLOCK(&(pal->locks[n_bucket]));
	}
	ae.pdata      = NULL;
	ae.lat_handle = NULL;
    } else {
	ae.pdata      = pl->data;
	ae.lat_handle = (void *) pl;
    }

    return(ae);
}

alat_entry_t alat_create_start(struct alat *pal, uint64_t addr, int must_not_exist)
{
    uint64_t       h;
    uint32_t       n_bucket;
    alat_entry_t   ae;
    alat_link_t   *pl;

    h        = alat_hash(addr);
    n_bucket = h % pal->n_buckets;

    RW_WR_LOCK(&(pal->locks[n_bucket]));
    // fprintf(stderr, "WR_LOCK(%d): alat_create_start, addr=%"PRIu64", tid=%ld\n", n_bucket, addr, my_tid());
    pal->lock_thrds[n_bucket] = my_tid();

    for (pl = pal->buckets[n_bucket]; pl != NULL; pl = pl->next) {
        if (pl->addr == addr) {
	    break;
	}
    }
    if (pl != NULL) {
        //  object already exists
	if (must_not_exist) {
	    pal->lock_thrds[n_bucket] = 0;
	    RW_WR_UNLOCK(&(pal->locks[n_bucket]));
	    ae.pdata      = NULL;
	    ae.lat_handle = NULL;
            // fprintf(stderr, "WR_UNLOCK(%d): alat_create_start, addr=%"PRIu64", tid=%ld\n", n_bucket, addr, my_tid());
	} else {
	    //  keep write lock and return entry
	    ae.pdata      = pl->data;
	    ae.lat_handle = (void *) pl;
	}
	return(ae);
    }

    pl           = get_alat_link(pal, addr);
    pl->addr     = addr;
    pl->n_bucket = n_bucket;
    pl->prev     = NULL;
    pl->next     = pal->buckets[n_bucket];
    if (pl->next != NULL) {
        pl->next->prev = pl;
    }
    pal->buckets[n_bucket] = pl;

    ae.pdata      = pl->data;
    ae.lat_handle = (void *) pl;

    return(ae);
}

void alat_read_end(void *handle)
{
    alat_link_t  *pl;

    pl = (alat_link_t *) handle;
    pl->table->lock_thrds[pl->n_bucket] = 0;
    RW_RD_UNLOCK(&(pl->table->locks[pl->n_bucket]));
}

void alat_write_end(void *handle)
{
    alat_link_t  *pl;

    pl = (alat_link_t *) handle;
    pl->table->lock_thrds[pl->n_bucket] = 0;
    RW_WR_UNLOCK(&(pl->table->locks[pl->n_bucket]));
    // fprintf(stderr, "WR_UNLOCK(%d): alat_write_end, addr=%"PRIu64", tid=%ld\n", pl->n_bucket, pl->addr, my_tid());
}

void alat_create_end(void *handle)
{
    alat_link_t  *pl;

    pl = (alat_link_t *) handle;
    pl->table->lock_thrds[pl->n_bucket] = 0;
    RW_WR_UNLOCK(&(pl->table->locks[pl->n_bucket]));
    // fprintf(stderr, "WR_UNLOCK(%d): alat_create_end, addr=%"PRIu64", tid=%ld\n", pl->n_bucket, pl->addr, my_tid());
}

void alat_write_end_and_delete(void *handle)
{
    alat_link_t  *pl;
    alat_t       *pal;

    pl       = (alat_link_t *) handle;
    pal      = pl->table;

    // unlink the link from the doubly linked bucket list

    if (pl->prev == NULL) {
        pal->buckets[pl->n_bucket] = pl->next;
    } else {
        pl->prev->next = pl->next;
    }

    if (pl->next != NULL) {
        pl->next->prev = pl->prev;
    }

    pl->table->lock_thrds[pl->n_bucket] = 0;
    RW_WR_UNLOCK(&(pal->locks[pl->n_bucket]));
    // fprintf(stderr, "WR_UNLOCK(%d): alat_write_end_and_delete, addr=%"PRIu64", tid=%ld\n", pl->n_bucket, pl->addr, my_tid());
    free_alat_entry(pal, pl); // must do this after unlocking!
}

void alat_dump(FILE *f, struct alat *pal)
{
    alat_link_t    *pl;
    uint32_t        n_bucket;
    char           *slock;
    int             ret;

    for (n_bucket=0; n_bucket<pal->n_buckets; n_bucket++) {
	for (pl = pal->buckets[n_bucket]; pl != NULL; pl = pl->next) {
	    ret = RW_WR_TRYLOCK(&(pal->locks[pl->n_bucket]));
	    if (ret == 0) {
	        RW_WR_UNLOCK(&(pal->locks[pl->n_bucket]));
	        slock = "NO";
	    } else {
		ret = RW_RD_TRYLOCK(&(pal->locks[pl->n_bucket]));
		if (ret == 0) {
		    RW_RD_UNLOCK(&(pal->locks[pl->n_bucket]));
		    slock = "RD";
		} else {
		    slock = "WR";
		}
	    }
	    fprintf(f, "addr=%"PRIu64" [bucket=%d, %s LOCK]\n", pl->addr, n_bucket, slock);
	}
    }
}

int alat_check(FILE *f, struct alat *pal, lat_check_cb_t *lat_check_cb, void *pdata)
{
    int             locked, is_write_lock;
    alat_link_t    *pl;
    uint32_t        n_bucket;
    int             ret = 0;
    int             ret2;

    for (n_bucket=0; n_bucket<pal->n_buckets; n_bucket++) {
	for (pl = pal->buckets[n_bucket]; pl != NULL; pl = pl->next) {
	    ret = RW_WR_TRYLOCK(&(pal->locks[pl->n_bucket]));
	    if (ret == 0) {
	        RW_WR_UNLOCK(&(pal->locks[pl->n_bucket]));
		locked        = 0;
                is_write_lock = 0;
	    } else {
		ret = RW_RD_TRYLOCK(&(pal->locks[pl->n_bucket]));
		if (ret == 0) {
		    RW_RD_UNLOCK(&(pal->locks[pl->n_bucket]));
		    is_write_lock = 0;
		    locked        = 1;
		} else {
		    is_write_lock = 1;
		    locked        = 1;
		}
	    }
	    ret2 = lat_check_cb(f, pdata, pl->addr, pl->data, locked, is_write_lock);
	    if (ret2) {
	        ret = 1;
	    }
	}
    }
    return(ret);
}

static uint64_t alat_hash(uint64_t addr)
{
    uint64_t   h;

    h = wshash((unsigned char *) &addr, sizeof(addr), 0);
    return(h);
}

