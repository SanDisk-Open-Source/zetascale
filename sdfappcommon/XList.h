/*
 * File:   XList.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: XList.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Linked list routines w/o locks (uses CDS logic)
 *
 *  The _SHMEM versions work with SHMEM pointers and require the prior use of PLAT_SP
 *
 * Similar to PLAT_shemem stuff:
 *  XLIST_H - creates header definitions
 *  XLIST_SHMEM_H - creates header defintions where objects pointers are shmem pointers
 *  XLIST_IMPL - creates actual code
 *     xxx_xlist_enqueue - pushes new value on list
 *     xxx_xlist_dequeue - returns value from list (or returns NULL)
 *  XLIST_SHMEM_IMPL - creates actual code for shmem versions
 *     xxx_xlist_enqueue - pushes new value on list
 *     xxx_xlist_dequeue - returns value from list (or returns xxx_sp_null)
 *
 *  Example of non-shmem version
 *  @code
 *    struct foo;
 *    typedef struct foo {
 *      int myData;
 *      struct foo *next_foo_ptr;
 *    } foo_t;
 *
 *   XLIST_H(bar, foo, next_foo_ptr);
 *   XLIST_IMPL(bar, foo, next_foo_ptr);
 *
 *   foo_t *head, *tail;
 *   head = NULL;
 *   tail = NULL;
 *
 *   foo_t *newobj = plat_alloc(sizeof(foo_t));
 *   bar_xlist_enqueue(&head, &tail, newobj);
 *
 *   foo_t *oldobj = bar_xlist_dequeue(&head, &tail);
 *
 *  @endcode
 *
 *  Example of shmem version:
 *  @code
 *    struct foo;
 *    PLAT_SP(foo_sp, foo);
 *    struct foo {
 *      int myData;
 *      foo_sp_t next_foo_shmem_ptr;
 *    };
 *
 *   PLAT_SP_IMPL(foo_sp, foo);
 *
 *   XLIST_SHMEM_H(bar, foo, next_foo_shmem_ptr);
 *   XLIST_SHMEM_IMPL(bar, foo, next_foo_shmem_ptr);
 *
 *   foo_sp_t head, tail;
 *   head = foo_sp_null;
 *   tail = foo_sp_null;
 *
 *   foo_sp_t newobj = foo_sp_alloc();
 *   bar_xlist_enqueue(&head, &tail, obj);
 *
 *   foo_sp_t oldobj = bar_xlist_dequeue(&head, &tail);
 *
 *  @endcode
 *
 */

#define XLIST_H(name, st, next_field)                                                           \
    extern struct st * name ## _xlist_dequeue(struct st **head, struct st **tail);              \
    extern void name ## _xlist_enqueue(struct st **head, struct st **tail, struct st *obj)

#define XLIST_IMPL(name, st, next_field)                                                        \
void name ## _xlist_enqueue(struct st **head, struct st **tail, struct st *obj) {               \
    obj->next_field = NULL;                                                                     \
    /* Swap out the tail and remember the old tail */                                           \
    struct st *old_tail = (struct st *) __sync_lock_test_and_set(tail, obj);                    \
    if (old_tail == NULL) {                                                                     \
        /* Swap out the head with this element if the head was null */                          \
        (void) __sync_bool_compare_and_swap(head, NULL, obj);                                   \
    } else {                                 /* The previous tail was not null */               \
        /* Swap the old tail-next pointer.  It should be null. */                               \
        /* If the old tail was just allocated then the dequeue code could be waiting for        \
           this code to complete the operation with this store.  If we are unlucky then         \
           this thread could swap out here the the dequeue-er could wait a long time            \
           for this to complete.  */                                                            \
        old_tail->next_field = obj;                                                             \
    }                                                                                           \
}                                                                                               \
                                                                                                \
struct st * name ## _xlist_dequeue(struct st **head, struct st **tail) {                        \
    struct st *obj;                                                                             \
    do {                                                                                        \
        obj = *((struct st * volatile *) head);                                                 \
        if (obj == NULL) {                                                                      \
            return (NULL);                   /* Quick exit if nothing queued */                 \
        }                                                                                       \
    } while ((obj == (struct st *) -1) ||   /* Wait for race to complete */                     \
              !__sync_bool_compare_and_swap(head, obj, -1)); /* Replace head with -1 */         \
    struct st *obj_next = obj->next_field;   /* Get the next head and keep it safe */           \
    *head = obj_next;                        /* Replace -1 with the real next head */           \
    if (obj_next == NULL) {                  /* If new head if NULL */                          \
        /* We try to nullify the tail if it points to this object (which means                  \
           that we are removing the last object on the list).  */                               \
        if (__sync_bool_compare_and_swap(tail, obj, NULL) == 0) {                               \
            /* The nullify failed because the tail has changed because an enqueue-er            \
               has added a new element to the end and it is current trying to write the         \
               next pointer to this object (which isn't needed any longer since this            \
               thread has dequeued it but there is a race and the enqueue-er                    \
               doesn't know that).  So, we have to keep looping until the enqueue-er            \
               compeltes the update so that we don't rip the rug out from under that            \
               thread by, say, deallocating this object before the next pointer is              \
               updated. */                                                                      \
            do {                             /* This could spin indefinitely...*/               \
            } while (((volatile struct st *) obj)->next_field == NULL);                         \
            *head = obj->next_field;         /* New head replaces NULL */                       \
        }                                                                                       \
    }                                                                                           \
                                                                                                \
    return (obj);                                                                               \
                                                                                                \
}

//
// SHEMEM versions
//

#define XLIST_SHMEM_H(name, st, next_field)                                                         \
    extern struct st ## _sp name ## _xlist_dequeue(struct st ## _sp *head, struct st ## _sp *tail);                      \
    extern void name ## _xlist_enqueue(struct st ## _sp *head, struct st ## _sp *tail, struct st ## _sp obj_shmem)

#define XLIST_SHMEM_IMPL(name, st, next_field)                                                      \
void name ## _xlist_enqueue(struct st ## _sp *head, struct st ## _sp *tail, struct st ## _sp obj_shmem) {                \
    struct st *obj = st ## _sp_rwref(&obj, obj_shmem);                                        \
    obj->next_field = st ## _sp_null;                                                               \
    /* Swap out the tail and remember the old tail */                                               \
    struct st ## _sp old_tail;                                                                      \
    do {                                                                                            \
        old_tail = *((struct st ## _sp * volatile) tail);                                            \
    } while (__sync_bool_compare_and_swap(&tail->base.int_base, old_tail.base.int_base, obj_shmem.base.int_base) == 0); \
                                                                                                    \
    if (st ## _sp_is_null(old_tail)) {                                                              \
        /* Swap out the head with this element if the head was null */                              \
        (void) __sync_bool_compare_and_swap(&head->base.int_base, st ## _sp_null.base.int_base, obj_shmem.base.int_base); \
                                                                                                    \
    } else {                                 /* The previous tail was not null */                   \
        /* Swap the old tail-next pointer.  It should be null. */                                   \
        /* If the old tail was just allocated then the dequeue code could be waiting for            \
           this code to complete the operation with this store.  If we are unlucky then             \
           this thread could swap out here the the dequeue-er could wait a long time                \
           for this to complete.  */                                                                \
        struct st *old_tail_ref = st ## _sp_rwref(&old_tail_ref, old_tail);                   \
        old_tail_ref->next_field.base.int_base = obj_shmem.base.int_base;                           \
        st ## _sp_rwrelease(&old_tail_ref);                                                         \
    }                                                                                               \
}                                                                                                   \
                                                                                                    \
struct st ## _sp name ## _xlist_dequeue(struct st ## _sp *head, struct st ## _sp *tail) {           \
    struct st ## _sp obj_shmem;                                                                     \
    do {                                                                                            \
        obj_shmem = *((struct st ## _sp * volatile) head);                                          \
        if (st ## _sp_is_null(obj_shmem)) {                                                         \
            return (st ## _sp_null);            /* Quick exit if nothing queued */                  \
        }                                                                                           \
    } while ((obj_shmem.base.int_base == -1) ||                                                     \
             !__sync_bool_compare_and_swap(&head->base.int_base, obj_shmem.base.int_base, -1));     \
                                                                                                    \
    const struct st *obj = st ## _sp_rref(&obj, obj_shmem);                                         \
    struct st ## _sp obj_next_shmem = obj->next_field; /* Get the next head and keep it safe */     \
    *head = obj_next_shmem;                          /* New head replaces -1 */                     \
    if (st ## _sp_is_null(obj_next_shmem)) {         /* If this was the last element */             \
        /* We try to nullify the tail if it points to this object (which means                      \
           that we are removing the last object on the list).  */                                   \
        if (!__sync_bool_compare_and_swap(&tail->base.int_base, obj_shmem.base.int_base, st ## _sp_null.base.int_base)) { \
                                                                                                    \
            /* The nullify failed because the tail has changed because an enqueue-er                \
               has added a new element to the end and it is current trying to write the             \
               next pointer to this object (which isn't needed any longer since this                \
               thread has dequeued it but there is a race and the enqueue-er                        \
               doesn't know that).  So, we have to keep looping until the enqueue-er                \
               compeltes the update so that we don't rip the rug out from under that                \
               thread by, say, deallocating this object before the next pointer is                  \
               updated. */                                                                          \
            do {                                  /* This could spin indefinitely...*/              \
            } while (st ## _sp_is_null(((volatile struct st *) obj)->next_field)); \
            *head = obj->next_field;             /* New head replaces NULL */                       \
        }                                                                                           \
    }                                                                                               \
   st ## _sp_rrelease(&obj);                                                                        \
   return ((struct st ## _sp) obj_shmem);                                                           \
                                                                                                    \
}
