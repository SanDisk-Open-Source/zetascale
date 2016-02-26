/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   fthlll_c.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthlll_c.h 396 2008-02-29 22:55:43Z jim $
 */

/**
 * @brief Linked list with locks.  This file is included from a user-defined .c file.
 
 * The file is usually just 2 lines:  include <name>.h and include fthlll_c.h.
 *
 * The user should also have a corresponding .h file and include fthlll.h after
 * setting up a few macros.  See fthlll.h for details
 */

/**
 * @brief Init the linked list header
 */
LLL_INLINE void LLL_NAME(_lll_init)(LLL_NAME(_lll_t) *ll) {
    ll->head = LLL_NULL;
    ll->tail = LLL_NULL;
    ll->spin = 0;
}

/**
 * @brief Init the linked list element
 */
LLL_INLINE void LLL_NAME(_el_init)(LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    el-> LLL_EL_FIELD . next = LLL_NULL;
    el-> LLL_EL_FIELD . prev = LLL_NULL;
    el-> LLL_EL_FIELD . spin = 0;
    LLL_REF_RELEASE(el);
}

/**
 * @brief Return true is list is empty
 */
LLL_INLINE int LLL_NAME(_is_empty)(LLL_NAME(_lll_t) *ll) {
    return (LLL_IS_NULL(ll->head));           // This is atomic
}

/**
 * @brief Lock the list
 */
LLL_INLINE void LLL_NAME(_spinLock)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_LOCK(&ll->spin);
}

/**
 * @brief Unlock the list
 */
LLL_INLINE void LLL_NAME(_spinUnlock)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_UNLOCK(&ll->spin);
}

/**
 * @brief Get the head of the list (for list walkers)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_head)(LLL_NAME(_lll_t) *ll) {
    return(ll->head);
}

/**
 * @brief Get the tail of the list (for list walkers)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_tail)(LLL_NAME(_lll_t) *ll) {
    return(ll->tail);
}

/**
 * @brief Get the next of the list (for list walkers)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_next)(LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    LLL_EL_PTR_TYPE rv = el-> LLL_EL_FIELD . next;
    LLL_REF_RELEASE(el);
    return (rv);
}

/**
 * @brief Get the previous of the list (for list walkers)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_prev)(LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    LLL_EL_PTR_TYPE rv = el-> LLL_EL_FIELD . prev;
    LLL_REF_RELEASE(el);
    return (rv);
}

/**
 * @brief insert onto the list w/o spin lock
 *
 * Should have spin lock held prior to call to avoid race (prevObj might be removed)
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param prevobj <IN> Previous object pointer (inserted after) (shmem if shmem-based linked list)
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_insert_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE prevObj, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    if (LLL_IS_NULL(prevObj)) {              // If inserting on head of list
        el-> LLL_EL_FIELD . prev = LLL_NULL;
        el-> LLL_EL_FIELD . next = ll->head;
        ll->head = obj;
    } else {                                 // Insert in middle
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, prevObj);
        el-> LLL_EL_FIELD . next = prev-> LLL_EL_FIELD . next;
        el-> LLL_EL_FIELD . prev = prevObj;
        prev-> LLL_EL_FIELD . next = obj;
        LLL_REF_RELEASE(prev);
    }
    if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { // If end of list
        ll->tail = obj;
    } else {
        LLL_EL_REF_TYPE next;
        LLL_REF(next, el-> LLL_EL_FIELD . next);
        next-> LLL_EL_FIELD . prev = obj;
        LLL_REF_RELEASE(next);
    }
    LLL_REF_RELEASE(el);
}

/**
 * @brief push to the end of the list
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_push)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    FTH_SPIN_LOCK(&ll->spin);
    el-> LLL_EL_FIELD . next = LLL_NULL;
    el-> LLL_EL_FIELD . prev = ll->tail;
    if (LLL_IS_NULL(ll->tail)) {
        ll->head = obj;                      /* First element on list */
    } else {
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = obj;    /* Chain off old tail */
        LLL_REF_RELEASE(tail);
    }
    ll->tail = obj;
    FTH_SPIN_UNLOCK(&ll->spin);
    LLL_REF_RELEASE(el);
}

/**
 * @brief push to the end of the list - nospin
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_push_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    el-> LLL_EL_FIELD . next = LLL_NULL;
    el-> LLL_EL_FIELD . prev = ll->tail;
    if (LLL_IS_NULL(ll->tail)) {
        ll->head = obj;                      /* First element on list */
    } else {
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = obj;    /* Chain off old tail */
        LLL_REF_RELEASE(tail);
    }
    ll->tail = obj;
    LLL_REF_RELEASE(el);
}

/**
 * @brief unshift onto the front of the list
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_unshift)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    FTH_SPIN_LOCK(&ll->spin);
    el-> LLL_EL_FIELD . next = ll->head;
    el-> LLL_EL_FIELD . prev = LLL_NULL;
    if (LLL_IS_NULL(ll->head)) {
        ll->tail = obj;                      /* First element on list */
    } else {
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = obj;    /* Chain off old head */
        LLL_REF_RELEASE(head);
    }
    ll->head = obj;
    FTH_SPIN_UNLOCK(&ll->spin);
    LLL_REF_RELEASE(el);
}

/**
 * @brief unshift onto the front of the list - nospin
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_unshift_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    el-> LLL_EL_FIELD . next = ll->head;
    el-> LLL_EL_FIELD . prev = LLL_NULL;
    if (LLL_IS_NULL(ll->head)) {
        ll->tail = obj;                      /* First element on list */
    } else {
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = obj;    /* Chain off old head */
        LLL_REF_RELEASE(head);
    }
    ll->head = obj;
    LLL_REF_RELEASE(el);
}

/**
 * @brief pop off the back of the list
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->tail)) {
        FTH_SPIN_UNLOCK(&ll->spin);
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->tail;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->tail = el-> LLL_EL_FIELD . prev;
    if (LLL_IS_NULL(ll->tail)) {             /* if list now empty */
        ll->head = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(tail);
    }

    FTH_SPIN_UNLOCK(&ll->spin);
    LLL_REF_RELEASE(el);

    return (rv);
}

/**
 * @brief pop off the back of the list - precheck
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_precheck)(LLL_NAME(_lll_t) *ll) {
    if (LLL_IS_NULL(ll->tail)) {             /* Precheck before spin for empty list */
        return (LLL_NULL);                   /* List is empty */
    }

    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->tail)) {
        FTH_SPIN_UNLOCK(&ll->spin);
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->tail;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->tail = el-> LLL_EL_FIELD . prev;
    if (LLL_IS_NULL(ll->tail)) {             /* if list now empty */
        ll->head = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(tail);
    }

    FTH_SPIN_UNLOCK(&ll->spin);
    LLL_REF_RELEASE(el);

    return (rv);
}

/**
 * @brief pop off the back of the list - nospin
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_nospin)(LLL_NAME(_lll_t) *ll) {
    if (LLL_IS_NULL(ll->tail)) {
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->tail;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->tail = el-> LLL_EL_FIELD . prev;
    if (LLL_IS_NULL(ll->tail)) {             /* if list now empty */
        ll->head = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(tail);
    }

    LLL_REF_RELEASE(el);

    return (rv);
}

/**
 * @brief shift off the front of the list
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->head)) {
        FTH_SPIN_UNLOCK(&ll->spin);
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->head;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->head = el-> LLL_EL_FIELD . next;
    LLL_REF_RELEASE(el);
    if (LLL_IS_NULL(ll->head)) {             /* if list now empty */
        ll->tail = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(head);
    }

    FTH_SPIN_UNLOCK(&ll->spin);

    return (rv);
}

/**
 * @brief shift off the front of the list - with precheck
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_precheck)(LLL_NAME(_lll_t) *ll) {
    if (LLL_IS_NULL(ll->head)) {             /* Precheck before spin for empty list*/
        return (LLL_NULL);                   /* List is empty */
    }
    
    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->head)) {
        FTH_SPIN_UNLOCK(&ll->spin);
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->head;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->head = el-> LLL_EL_FIELD . next;
    LLL_REF_RELEASE(el);
    if (LLL_IS_NULL(ll->head)) {             /* if list now empty */
        ll->tail = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(head);
    }

    FTH_SPIN_UNLOCK(&ll->spin);

    return (rv);
}

/**
 * @brief shift off the front of the list - nospin
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_nospin)(LLL_NAME(_lll_t) *ll) {
    if (LLL_IS_NULL(ll->head)) {
        return (LLL_NULL);                   /* List is empty */
    }

    LLL_EL_PTR_TYPE rv = ll->head;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->head = el-> LLL_EL_FIELD . next;
    LLL_REF_RELEASE(el);
    if (LLL_IS_NULL(ll->head)) {             /* if list now empty */
        ll->tail = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(head);
    }

    return (rv);
}

/**
 * @brief remove an element from anywhere on the list
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_remove)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    FTH_SPIN_LOCK(&ll->spin);

    if (LLL_IS_NULL(el-> LLL_EL_FIELD . prev)) { /* Head of list */
        ll->head = el-> LLL_EL_FIELD . next; /* Unchain */
        if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* if only element */
            ll->tail = LLL_NULL;             /* Reset tail pointer */
        } else {
            LLL_EL_REF_TYPE next;
            LLL_REF(next, el-> LLL_EL_FIELD . next);
            next-> LLL_EL_FIELD . prev = LLL_NULL; /* Change next/prev */
            LLL_REF_RELEASE(next);
        }

    } else if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* Tail of list (and not only el) */
        ll->tail = el-> LLL_EL_FIELD . prev; /* Unchain */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = LLL_NULL; /* Change prev/next */
        LLL_REF_RELEASE(prev);

    } else {                                 /* Neither head nor tail */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = el-> LLL_EL_FIELD . next; /* Unchain (can't be first el) */
        LLL_REF_RELEASE(prev);
        
        LLL_EL_REF_TYPE next;
        LLL_REF(next, el-> LLL_EL_FIELD . next);
        next-> LLL_EL_FIELD . prev = el-> LLL_EL_FIELD . prev; /* Unchain (can't be last el) */
        LLL_REF_RELEASE(next);

    }

    FTH_SPIN_UNLOCK(&ll->spin);
    LLL_REF_RELEASE(el);
}


/**
 * @brief remove an element from anywhere on the list - spin lock held by caller
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_remove_nospin)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);

    if (LLL_IS_NULL(el-> LLL_EL_FIELD . prev)) { /* Head of list */
        ll->head = el-> LLL_EL_FIELD . next; /* Unchain */
        if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* if only element */
            ll->tail = LLL_NULL;             /* Reset tail pointer */
        } else {
            LLL_EL_REF_TYPE next;
            LLL_REF(next, el-> LLL_EL_FIELD . next);
            next-> LLL_EL_FIELD . prev = LLL_NULL; /* Change next/prev */
            LLL_REF_RELEASE(next);
        }

    } else if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* Tail of list (and not only el) */
        ll->tail = el-> LLL_EL_FIELD . prev; /* Unchain */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = LLL_NULL; /* Change prev/next */
        LLL_REF_RELEASE(prev);

    } else {                                 /* Neither head nor tail */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = el-> LLL_EL_FIELD . next; /* Unchain (can't be first el) */
        LLL_REF_RELEASE(prev);
        
        LLL_EL_REF_TYPE next;
        LLL_REF(next, el-> LLL_EL_FIELD . next);
        next-> LLL_EL_FIELD . prev = el-> LLL_EL_FIELD . prev; /* Unchain (can't be last el) */
        LLL_REF_RELEASE(next);

    }

    LLL_REF_RELEASE(el);
}


//  Linked list routines with element locking

/**
 * @brief pop off the back of the list atomically locking the element
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_pop_lock)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->tail)) {
        FTH_SPIN_UNLOCK(&ll->spin);              /* Release the linked-list lock */
        return (LLL_NULL);
    }

    LLL_EL_PTR_TYPE rv = ll->tail;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->tail = el-> LLL_EL_FIELD . prev;
    if (LLL_IS_NULL(ll->tail)) {             /* if list now empty */
        ll->head = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE tail;
        LLL_REF(tail, ll->tail);
        tail-> LLL_EL_FIELD . next = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(tail);
    }

    FTH_SPIN_LOCK(&(el->LLL_EL_FIELD . spin)); /* Get the element spinlock */
    FTH_SPIN_UNLOCK(&ll->spin);              /* Release the linked-list lock */

    return (rv);
}


/**
 * @brief shift off the front of the list atomically locking the element
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @return Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE LLL_EL_PTR_TYPE LLL_NAME(_shift_lock)(LLL_NAME(_lll_t) *ll) {
    FTH_SPIN_LOCK(&ll->spin);
    if (LLL_IS_NULL(ll->head)) {
        FTH_SPIN_UNLOCK(&ll->spin);              /* Release the linked-list lock */
        return (LLL_NULL);
    }
    
    LLL_EL_PTR_TYPE rv = ll->head;           /* Item to return */
    LLL_EL_REF_TYPE el;
    LLL_REF(el, rv);
    ll->head = el-> LLL_EL_FIELD . next;
    if (LLL_IS_NULL(ll->head)) {             /* if list now empty */
        ll->tail = LLL_NULL;
    } else {                                 /* List still not empty */
        LLL_EL_REF_TYPE head;
        LLL_REF(head, ll->head);
        head-> LLL_EL_FIELD . prev = LLL_NULL; /* Backpointer of new head */
        LLL_REF_RELEASE(head);
    }

    FTH_SPIN_LOCK(&(el->LLL_EL_FIELD . spin)); /* Get the element spinlock */
    FTH_SPIN_UNLOCK(&ll->spin);              /* Release the linked-list lock */
    LLL_REF_RELEASE(el);

    return (rv);
}


/**
 * @brief remove an element from anywhere on the list atomically locking the element
 *
 * @param ll <IN> Pointer to linked-list header structure
 * @param obj <IN> Object pointer (shmem if shmem-based linked list)
 */
LLL_INLINE void LLL_NAME(_remove_lock)(LLL_NAME(_lll_t) *ll, LLL_EL_PTR_TYPE obj) {
    LLL_EL_REF_TYPE el;
    LLL_REF(el, obj);
    FTH_SPIN_LOCK(&ll->spin);

    if (LLL_IS_NULL(el-> LLL_EL_FIELD . prev)) { /* Head of list */
        ll->head = el-> LLL_EL_FIELD . next; /* Unchain */
        if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* if only element */
            ll->tail = LLL_NULL;             /* Reset tail pointer */
        } else {
            LLL_EL_REF_TYPE next;
            LLL_REF(next, el-> LLL_EL_FIELD . next);
            next-> LLL_EL_FIELD . prev = LLL_NULL; /* Change next/prev */
            LLL_REF_RELEASE(next);
        }

    } else if (LLL_IS_NULL(el-> LLL_EL_FIELD . next)) { /* Tail of list (and not only el) */
        ll->tail = el-> LLL_EL_FIELD . prev; /* Unchain */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = LLL_NULL; /* Change prev/next */
        LLL_REF_RELEASE(prev);

    } else {                                 /* Neither head nor tail */
        LLL_EL_REF_TYPE prev;
        LLL_REF(prev, el-> LLL_EL_FIELD . prev);
        prev-> LLL_EL_FIELD . next = el-> LLL_EL_FIELD . next; /* Unchain (can't be first el) */
        LLL_REF_RELEASE(prev);
        
        LLL_EL_REF_TYPE next;
        LLL_REF(next, el-> LLL_EL_FIELD . next);
        next-> LLL_EL_FIELD . prev = el-> LLL_EL_FIELD . prev; /* Unchain (can't be last el) */
        LLL_REF_RELEASE(next);

    }

    FTH_SPIN_LOCK(&(el->LLL_EL_FIELD . spin)); /* Get the element spinlock */
    FTH_SPIN_UNLOCK(&ll->spin);              /* Release the linked-list lock */
    LLL_REF_RELEASE(el);

}
