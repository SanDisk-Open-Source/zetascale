/*
 * File:   sdf/platform/aoset.c
 * Author: gshaw
 *
 * Created on June 3, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: aoset.c,v 1.2 2008/06/25 18:45:31 gshaw Exp gshaw $
 */

#include <ctype.h>
#include <stdint.h>

#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/errno.h"
#include "platform/stdio.h"

#include "platform/aoset.h"

#ifndef UCHAR_MAX
#define UCHAR_MAX (__SCHAR_MAX__ + __SCHAR_MAX__ + 1)
#endif

/**
 * @brief Common C language "extensions".
 *
 * This stuff should be in a "well-known" include file.
 */

#define ELEMENTS(var) (sizeof (var) / sizeof (*var))

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define dprintf(fmt, ...) \
    ({ if (dbug) { fprintf(stderr, fmt, ## __VA_ARGS__); }; })

/*
 * Atomically fetch a value.  Do not modify it.
 */
#define SYNC_FETCH(ptr) __sync_fetch_and_or(ptr, 0)
#define SYNC_INCR(ptr) __sync_fetch_and_add(ptr, 1)
#define SYNC_DECR(ptr) __sync_fetch_and_sub(ptr, 1)

static __inline__ uint32_t
uint32_is_power_of_2(uint32_t n)
{
    return ((n & (n - 1)) == 0);
}

static __inline__ uint64_t
uint64_is_power_of_2(uint64_t n)
{
    return ((n & (n - 1)) == 0);
}

/**
 * @brief Test whether the given expression is a power of two,
 * without side effects.
 */
#define ispowerof2(n_expr)                                      \
    ({                                                          \
        typeof(n_expr) n = (n_expr);                            \
        ((n & (n - 1)) == 0);                                   \
    })


#ifdef AOSET_DEBUG

static uint_t
is_printable_zstring(void *mem, size_t sz)
{
    char *cmem;
    uint_t i;

    cmem = (char *)mem;
    if (cmem[sz - 1] != '\0') {
        return (0);
    }
    if (sz > 0) {
        --sz;
    }
    for (i = 0; i < sz; ++i) {
        if (!isprint(cmem[i])) {
            return (0);
        }
    }
    return (1);
}

static void
eprint_blob(void *mem, size_t sz)
{
    uint_t *ip;
    uint_t ni;

    ip = (uint_t *)mem;
    ni = sz / sizeof (uint_t);
    while (ni != 0) {
        eprintf(" %x", *ip);
        ++ip;
        --ni;
    }
}

#endif /* def AOSET_DEBUG */

#define AOSET_MAGIC 1234

#define ALLOC_PAD 0

#ifdef AOSET_TEST_SMALL
/*
 * XXX Small values to stress test logic to grow tables.
 */
#define INIT_ENTRIES   4
#define INIT_SLOTS    16

#else

#define INIT_ENTRIES  512
#define INIT_SLOTS    512

#endif /* def AOSET_TEST_SMALL */

/*
 * Implementation of Append-only sets.
 */

struct hash_ent {
    uint_t  h_hash;
    gvptr_t blob_mem;
    size_t  blob_size;
    uint_t  h_next;
};

typedef struct hash_ent hash_ent_t;

struct aoset_impl {
    uint32_t        aos_magic;
    uint_t          aos_alloc;
    uint_t          aos_size;
    gvptr_t         aos_hvec;       /* Ref type is hash_ent_t *_         */
    gvptr_t         aos_table;      /* Ref type is int *_                */
    uint_t          aos_tsize;
    uint_t          aos_size_init;  /* Initial size (number of entries)  */
    uint_t          aos_size_grow;  /* Grow by at most this many entires */
};

typedef struct aoset_impl aoset_impl_t;

#ifdef AOSET_DEBUG
static uint_t dbug = 0;
#endif


/*
 * Compute hash function of a given blob.
 *
 * CAVEAT: Quick and dirty hash function.  We could do better.
 */
uint_t
blob_hash(void *blob_mem, size_t blob_size)
{
    unsigned char *mem;
    size_t i;
    uint_t h;

    mem = (unsigned char *)blob_mem;
    h = 0;
    for (i = 0; i < blob_size; ++i) {
        h += mem[i] + i;
#if 0
        h = (h << 1) + mem[i] + i;
        if (h > 0xffffffff) {
            h ^= (h >> 16) & 0xffff;
        }
#endif
    }
    return (h);
}

/*
 * Free any possible allocated parts of an aoset_impl_t.
 */

static void
aoset_free_metadata(aoset_impl_t *setp)
{
    size_t sz;

    if (!GVPTR_IS_NULL(setp->aos_hvec)) {
        sz = setp->aos_alloc * sizeof (hash_ent_t) + ALLOC_PAD;
        GVPTR_FREE(setp->aos_hvec, sz);
        GVPTR_NULLIFY(setp->aos_hvec);
    }

    if (!GVPTR_IS_NULL(setp->aos_table)) {
        sz = setp->aos_tsize * sizeof (int) + ALLOC_PAD;
        GVPTR_FREE(setp->aos_table, sz);
        GVPTR_NULLIFY(setp->aos_table);
    }
}

/*
 * Initialize the hash table entries to all -1.
 *
 * Valid indices are 0 .. n-1, so the value, -1, is used
 * to indicate an empty slot, rather than 0.
 */
static void
aoset_init_table(aoset_impl_t *setp)
{
    int *tbl;
    uint_t i;

    tbl = (int *)GVPTR_REF(setp->aos_table);
    for (i = 0; i < setp->aos_tsize; ++i) {
        tbl[i] = -1;
    }
}

/*
 * Allocate the arrays needed to support an aoset_t, and initialize all values.
 */
int
aoset_init(aoset_impl_t *setp, uint_t size0, uint_t grow)
{
    size_t sz;
    uint_t slots;

    setp->aos_magic = AOSET_MAGIC;
    if (size0 == 0) {
        size0 = INIT_ENTRIES;
    }
    setp->aos_size_init = size0;
    setp->aos_size_grow = grow;
    setp->aos_alloc = size0;
    setp->aos_size = 0;
    GVPTR_NULLIFY(setp->aos_hvec);
    GVPTR_NULLIFY(setp->aos_table);

    /*
     * Use next power of two, >= the given initial size.
     */
    for (slots = 16; slots < size0; slots *= 2) {
        /* empty */
    }
    setp->aos_tsize = slots;

    sz = setp->aos_alloc * sizeof (hash_ent_t) + ALLOC_PAD;
    GVPTR_ALLOC(setp->aos_hvec, sz);
    if (GVPTR_IS_NULL(setp->aos_hvec)) {
        aoset_free_metadata(setp);
        return (-ENOMEM);
    }

    sz = setp->aos_tsize * sizeof (int) + ALLOC_PAD;
    GVPTR_ALLOC(setp->aos_table, sz);
    if (GVPTR_IS_NULL(setp->aos_table)) {
        aoset_free_metadata(setp);
        return (-ENOMEM);
    }

    /*
     * Initialize all the elements of a newly allocated aoset_impl_t.
     * The array of entries does not need to be initialized, because
     * nothing ever tries to access entries larger than the current size
     * of the set.
     *
     * But, the hash table does need to be initialized, because it is
     * accessed by a hash value.
     */
    aoset_init_table(setp);
    return (0);
}

/*
 * Create an append-only set.  Take advice on how many items to allow for,
 * initially, and how much to grow the table(s), if and when it becomes
 * necessary.
 */
aoset_hdl_t
aoset_create_tune(uint_t size0, uint_t grow)
{
    aoset_hdl_t aoset_hdl;
    aoset_impl_t *setp;
    int ret;

    GVPTR_ALLOC(aoset_hdl, sizeof (aoset_impl_t) + ALLOC_PAD);
    if (GVPTR_IS_NULL(aoset_hdl)) {
        return (aoset_hdl);
    }
    setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    ret = aoset_init(setp, size0, grow);
    if (ret != 0) {
        GVPTR_FREE(aoset_hdl, sizeof (aoset_impl_t) + ALLOC_PAD);
        GVPTR_NULLIFY(aoset_hdl);
        return (aoset_hdl);
    }
    return (aoset_hdl);
}

/*
 * Create an append-only set, using all default values for tunable parameters.
 * Keep it simple.  Do something reasonable.
 */
aoset_hdl_t
aoset_create(void)
{
    return (aoset_create_tune(0, 0));
}

/*
 * Program logic
 * -------------
 * First, free all the objects.
 * then, free the array of descriptors of objects.
 * then, free the hash table.
 * then, free the aoset, itself.
 *
 * Not mt-safe.  Must be done only after aoset has been quiesced.
 */

int
aoset_destroy(aoset_hdl_t aoset_hdl)
{
    aoset_impl_t *setp;
    hash_ent_t *hvec;
    int i;

    if (GVPTR_IS_NULL(aoset_hdl)) {
        return (-EINVAL);
    }
    setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    if (setp->aos_magic != AOSET_MAGIC) {
        return (-EINVAL);
    }

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    for (i = 0; i < setp->aos_size; ++i) {
        GVPTR_FREE(hvec[i].blob_mem, hvec[i].blob_size + ALLOC_PAD);
    }

    aoset_free_metadata(setp);
    GVPTR_FREE(aoset_hdl, sizeof (aoset_impl_t) + ALLOC_PAD);
    return (0);
}

#define AOSET_OP_FIND 0
#define AOSET_OP_ADD 1

/*
 * Expand an append-only set.
 *
 * Double the size of the array of entries.
 * Note: This function only expands the size of the array of entries;
 * it does not expand the size of the hash table.  Therefore, chains
 * will tend to get longer, because the same number of hash slots
 * "cover" more entries.
 *
 * Implementation detail: current policy is to double the size of the array.
 */

static int
aoset_expand_entries(aoset_impl_t *setp)
{
    gvptr_t old_hvec_hdl;
    gvptr_t new_hvec_hdl;
    hash_ent_t *old_hvec;
    hash_ent_t *new_hvec;
    size_t old_size;
    size_t new_size;
    uint_t n;

    n = setp->aos_alloc;
    old_size = n * sizeof (hash_ent_t);
    n *= 2;
    new_size = n * sizeof (hash_ent_t);
    old_hvec_hdl = setp->aos_hvec;
    GVPTR_ALLOC(new_hvec_hdl, new_size + ALLOC_PAD);
    if (GVPTR_IS_NULL(new_hvec_hdl)) {
        return (-ENOMEM);
    }
    old_hvec = GVPTR_REF(old_hvec_hdl);
    new_hvec = GVPTR_REF(new_hvec_hdl);
    memcpy(new_hvec, old_hvec, old_size);
    GVPTR_FREE(old_hvec_hdl, old_size);
    setp->aos_hvec = new_hvec_hdl;
    setp->aos_alloc = n;
    return (0);
}

#if defined(XXX_IMPLEMENT_EXPAND_SLOTS)

/*
 * Expand the number of slots in the hash table and rehash all entries,
 * using the new, larger table.
 */
static int
aoset_expand_slots(aoset_impl_t *setp)
{
    int *new_table;
    size_t new_size;
    uint_t n;

    n = setp->aos_tsize;
    n *= 2;
    new_size = n * sizeof (int);
    new_table = (int *)plat_realloc(setp->aos_table, new_size);
    if (new_table == NULL) {
        return (-ENOMEM);
    }
    /* XXX TODO: rehash all entries */
    return (0);
}

#endif /* defined(XXX_IMPLEMENT_EXPAND_SLOTS) */

static int
search_chain(aoset_impl_t *setp, void *obj, size_t objsize, uint_t obj_hash, uint_t slot)
{
    hash_ent_t *hvec;
    hash_ent_t *entp;
    int *tbl;
    int enti;

    if (setp->aos_magic != AOSET_MAGIC) {
        return (-EINVAL);
    }

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    tbl = (int *)GVPTR_REF(setp->aos_table);
    enti = tbl[slot];

    while (enti != -1) {
        entp = hvec + enti;
        if (entp->h_hash == obj_hash) {
            if (entp->blob_size == objsize) {
                void *set_blob_mem = GVPTR_REF(entp->blob_mem);

                if (memcmp(obj, set_blob_mem, objsize) == 0) {
                    return (enti);
                }
            }
        }
        enti =  entp->h_next;
    }
    return (-ENOENT);
}

static int
aoset_append_blob(
    aoset_impl_t *setp,
    void *obj,
    size_t objsize,
    uint_t obj_hash,
    uint_t slot)
{
    gvptr_t save_obj_hdl;
    void *save_obj;
    hash_ent_t *hvec;
    hash_ent_t *entp;
    int *tbl, *bktp;
    int bkt_hd1, bkt_hd2;
    uint_t n1, n2;
    int ret;

    /*
     * Save a local copy of the object.
     */
    GVPTR_ALLOC_REF(save_obj_hdl, save_obj, objsize + ALLOC_PAD);
    if (GVPTR_IS_NULL(save_obj_hdl)) {
        return (-ENOMEM);
    }
    memcpy(save_obj, obj, objsize);

    /*
     * Keep trying to update the set, until we either succeed
     * (win any potential race), or until we give up because
     * another thread has added the same object we are trying
     * to add.
     */

    for (;;) {
        n1 = SYNC_FETCH(&setp->aos_size);
        /*
         * Since each append operation increments the size of the set,
         * the current size also acts as a version number.
         */
        if (n1 >= setp->aos_alloc) {
            ret = aoset_expand_entries(setp);
            if (ret < 0) {
                return (ret);
            }
        }
    
        n2 = SYNC_INCR(&setp->aos_size);
        hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
        if (n2 == n1) {
            /*
             * We won the race to insert into the array of blobs.
             * Now, we just need to insert into the hash table.
             * At this stage, we do not need to worry about backing
             * off on inserting into the array of blobs, since the
             * size has been updated atomically, successfully, so
             * our ownership of blob[n1] is secure.  Therefore,
             * we just have to do what it takes to insert into the
             * linked list associated with this object's hash bucket.
             *
             * In the per bucket linked lists order does not matter,
             * so we just insert at the head of the bucket, because
             * it is easier.
             */
            entp = hvec + n1;
            entp->blob_mem = save_obj_hdl;
            entp->blob_size = objsize;
            entp->h_hash = obj_hash;
            entp->h_next = -1;
            tbl = (int *)GVPTR_REF(setp->aos_table);
            bktp = &tbl[slot];
            bkt_hd1 = SYNC_FETCH(bktp);
            for (;;) {
                entp->h_next = bkt_hd1;
                bkt_hd2 = __sync_val_compare_and_swap(bktp, bkt_hd1, n1);
                if (bkt_hd2 == bkt_hd1) {
                    break;
                }
                bkt_hd1 = bkt_hd2;
            }
            return (n1);
        } else {
            /*
             * We lost the race to update setp->aos_size.
             * Check to see if any of the new objects are the
             * same as the one we are trying to insert;
             * if so, then back off, because somebody already
             * did the work for us.  Unlikely, but possible.
             *
             * If there are no duplicates, then try the append
             * all over again.  It is not quite starting from
             * square one, because we do not have to deallocate
             * and then reallocate the new blob.
             */
            ret = search_chain(setp, obj, objsize, obj_hash, slot);
            if (ret >= 0) {
                    /*
                     * We lost the update race, and now the new object
                     * is a duplicate.  Back off.  Free the new object,
                     * and return the index of the one that was already
                     * appended to the set.
                     */
                    GVPTR_FREE(save_obj_hdl, objsize);
                    return (ret);
            } else if (ret != -ENOENT) {
                return (ret);
            }
        }
    }
    /* NOTREACHED */
    plat_abort();
    return (-EINVAL);
}

/*
 * Probe for a matching element in the set.
 *
 * Implementation private.
 *
 * Do the underlying work of aoset_find() and aoset_add().
 */
static int
aoset_probe(aoset_impl_t *setp, void *obj, size_t objsize, uint_t op)
{
    uint_t obj_hash;
    uint_t mask, slot;
    int ret;

    if (setp->aos_magic != AOSET_MAGIC) {
        return (-EINVAL);
    }

    /*
     * Extra test: ASSERT(is_power_of_2(setp->aos_tsize));
     */
    obj_hash = blob_hash(obj, objsize);
    mask = setp->aos_tsize - 1;
    slot = obj_hash & mask;

#ifdef AOSET_DEBUG
    if (dbug) {
        char *objstr;

        objstr = is_printable_zstring(obj, objsize) ? obj : "<obj>";
        eprintf(" -- %s: blob_hash('%s',%llu) = 0x%x, mask=%x, slot=%u\n",
                __func__, objstr, (unsigned long long)objsize, obj_hash, mask, slot);
    }
#endif

    ret = search_chain(setp, obj, objsize, obj_hash, slot);
    if (ret == -ENOENT && op == AOSET_OP_ADD) {
        ret = aoset_append_blob(setp, obj, objsize, obj_hash, slot);
    }
    return (ret);
}

int
aoset_find(aoset_hdl_t aoset_hdl, void *obj, size_t objsize)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);

    return (aoset_probe(setp, obj, objsize, AOSET_OP_FIND));
}

int
aoset_add(aoset_hdl_t aoset_hdl, void *obj, size_t objsize)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);

    return (aoset_probe(setp, obj, objsize, AOSET_OP_ADD));
}

int
aoset_get(aoset_hdl_t aoset_hdl, int index, gvptr_t *objr, size_t *objsizer)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    hash_ent_t *hvec;
    hash_ent_t *entp;

    if (index >= setp->aos_size) {
        GVPTR_TOXIFY(*objr);
        *objsizer = 0;
        return (-ENOENT);
    }

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    entp = hvec + index;
    *objr = entp->blob_mem;
    *objsizer = entp->blob_size;
    return (0);
}

#ifdef AOSET_DEBUG

void
aoset_hash_histogram(aoset_impl_t *setp)
{
    hash_ent_t *hvec;
    int *tbl;
    uint_t slot;    /* hash bucket index */
    // uint_t n;       /* number of entries in entire hash table */
    uint_t b;       /* number of hash buckets */
    uint_t freq[64];
    uint_t f;       /* frequency category */
    uint_t maxcl;   /* maximum chain length actually encountered */

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    tbl = (int *)GVPTR_REF(setp->aos_table);
    // n = setp->aos_size;
    b = setp->aos_tsize;

    for (f = 0; f < ELEMENTS(freq); ++f) {
        freq[f] = 0;
    }

    /*
     * Visit each hash bucket.  Follow the chain and compute chain length.
     * Update the frequency (count) of chains of that length.
     * Chains longer than ELEMENTS(freq) have length of "many".
     */
    maxcl = 0;
    for (slot = 0; slot < b; ++slot) {
        int enti;
        uint_t di;

        di = 0;
        enti = tbl[slot];
        while (enti != -1) {
            ++di;
            enti = hvec[enti].h_next;
        }
        f = di;
        if (f >= ELEMENTS(freq)) {
            f = ELEMENTS(freq) - 1;
        }
        if (f > maxcl) {
            maxcl = f;
        }
        ++freq[f];
    }

    eprintf(" len count\n");
    eprintf(" --- --------\n");
    for (f = 0; f <= maxcl; ++f) {
        eprintf(" %3u %8u\n", f, freq[f]);
    }
}

uint_t
aoset_hash_efficiency(aoset_impl_t *setp)
{
    hash_ent_t *hvec;
    int *tbl;
    uint_t slot;    /* hash bucket index */
    uint_t n;       /* number of entries in entire hash table */
    uint_t b;       /* number of hash buckets */
    uint_t d;       /* depth */
    uint_t r;       /* remainder */
    uint_t s;       /* slop */

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    tbl = (int *)GVPTR_REF(setp->aos_table);

    n = setp->aos_size;
    b = setp->aos_tsize;
    d = n / b;
    r = n - (b * d);
    s = 0;
    for (slot = 0; slot < b; ++slot) {
        hash_ent_t *entp;
        int enti;
        uint_t di;

        di = 0;
        enti = tbl[slot];
        while (enti != -1) {
            ++di;
            entp = hvec + enti;
            enti = entp->h_next;
        }
        if (di > d) {
            s += di - d;
            if (r > 0) {
                --s;
                --r;
            }
        }
    }

    return (((n - s) * 100) / n);
}

void
aoset_debug(uint_t lvl)
{
    dbug = lvl;
}

/*
 * Do consistency check of an entire aoset, a la fsck for filesystems.
 * Ensure that all blobs are reachable by the hash table, and that
 * the reference count of each blob is exactly 1.
 *
 * Not mt-safe.  Must be done only after aoset has been quiesced.
 */

int
aoset_check(aoset_hdl_t aoset_hdl)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    hash_ent_t *hvec;
    int *tbl;
    int enti;
    unsigned char *refcount;
    size_t sz;
    uint_t i;
    uint_t err;

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    tbl = (int *)GVPTR_REF(setp->aos_table);
    sz = setp->aos_size;
    refcount = (unsigned char *)plat_malloc(sz);
    if (refcount == NULL) {
        return (-ENOMEM);
    }
    memset(refcount, 0, sz);

    /*
     * Compute reference count for every blob.
     * Follow the chain in every hash table bucket.
     */
    for (i = 0; i < setp->aos_tsize; ++i) {
        enti = tbl[i];
        while (enti >= 0) {
            uint_t count;

            /*
             * Increment reference count for this blob index.
             * Use saturation arithmetic, just in case.
             */
            count = refcount[enti];
            if (count < UCHAR_MAX) {
                ++count;
                refcount[enti] = count;
            }
            enti = hvec[enti].h_next;
        }
    }

    /*
     * Verify that every blob is referenced exactly once.
     */
    err = 0;
    for (i = 0; i < sz; ++i) {
        if (refcount[i] != 1) {
            eprintf("Bad ref count blob[%u] = %u\n", i, refcount[i]);
            err = 1;
        }
    }

    plat_free(refcount);

    if (err) {
        return (-EFAULT);
    }
    return (0);
}

void
aoset_eprint_hash(aoset_hdl_t aoset_hdl)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    hash_ent_t *hvec;
    int *tbl;
    uint_t i;
    int index;

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    tbl = (int *)GVPTR_REF(setp->aos_table);
    for (i = 0; i < setp->aos_tsize; ++i) {
        index = tbl[i];
        if (index >= 0) {
            eprintf(" t[%3u] = %d", i, index);
            eprintf(" = { hash=%x, next=%d }",
                hvec[index].h_hash, hvec[index].h_next);
            eprintf("\n");
        }
    }
}

/*
 * Show all blobs in the set.  Print to stderr.
 */
void
aoset_eprint_blobs(aoset_hdl_t aoset_hdl)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    hash_ent_t *hvec;
    uint_t i;

    hvec = (hash_ent_t *)GVPTR_REF(setp->aos_hvec);
    for (i = 0; i < setp->aos_size; ++i) {
        void *mem;
        size_t sz;

        mem = GVPTR_REF(hvec[i].blob_mem);
        sz = hvec[i].blob_size;
        eprintf(" blob[%3u] @%p: hash=%x\n          ",
            i, mem, hvec[i].h_hash);
        if (is_printable_zstring(mem, sz)) {
            eprintf("'%s'", (char *)mem);
        } else {
            eprint_blob(mem, sz);
        }
        eprintf("\n");
    }
}

void
aoset_eprint_stats(aoset_hdl_t aoset_hdl)
{
    aoset_impl_t *setp = (aoset_impl_t *)GVPTR_REF(aoset_hdl);
    uint_t i;

    i = aoset_hash_efficiency(setp);
    eprintf("Hash efficiency(0..100): %u\n", i);
    aoset_hash_histogram(setp);
}

void
aoset_diag(aoset_hdl_t aoset_hdl)
{
    eprintf("<check>\n");
    aoset_check(aoset_hdl);
    eprintf("</check>\n");

    eprintf("<hash>\n");
    aoset_eprint_hash(aoset_hdl);
    eprintf("</hash>\n");

    eprintf("<blobs>\n");
    aoset_eprint_blobs(aoset_hdl);
    eprintf("</blobs>\n");

    eprintf("<stats>\n");
    aoset_eprint_stats(aoset_hdl);
    eprintf("</stats>\n");
}

#endif /* def AOSET_DEBUG */
