/*
 * File: sdf_msg_binding.c
 * Author: Drew Eckhardt
 * (c) Copyright 2008-2009, Schooner Information Technology, Inc.
 */
#define _POSIX_C_SOURCE 200112  /* XXX - Where should this be? */

#include <stdio.h>
#include <search.h>
#include "sdf_msg_action.h"


#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG


/** @brief tfind tree and its lock (correct but not performant) */
struct sdf_msg_binding_tree {
    void *root;                 /* root node */
    /* Lock order is tree then msg binding */
    pthread_rwlock_t lock;
};

/** @brief a single message binding */
struct sdf_msg_binding {
    struct sdf_msg_action *action; /* action bound to this tuple */

    vnode_t dest_vnode;         /* vnode */
    service_t dest_service;     /* service */

    /*
     * Lock order is tree then msg binding. Held from lookup in tree until
     * message is posted on read.
     */
    pthread_rwlock_t lock;
};

static struct sdf_msg_binding_tree sdf_msg_binding_tree = {
    NULL, PTHREAD_RWLOCK_INITIALIZER
};


static int
sdf_compare_msg_bindings(const void *lhs_void, const void *rhs_void)
{
    int ret;
    struct sdf_msg_binding *lhs = (struct sdf_msg_binding *)lhs_void;
    struct sdf_msg_binding *rhs = (struct sdf_msg_binding *)rhs_void;

    if (lhs->dest_vnode > rhs->dest_vnode) {
        ret = 1;
    } else if (lhs->dest_vnode < rhs->dest_vnode) {
        ret = -1;
    } else if (lhs->dest_service > rhs->dest_service) {
        ret = 1;
    } else if (lhs->dest_service < rhs->dest_service) {
        ret = -1;
    } else {
        plat_assert(lhs->dest_service == rhs->dest_service);
        ret = 0;
    }

    return ret;
}


static struct sdf_msg_binding *
sdf_find_and_read_lock_msg_binding(vnode_t dest_vnode, service_t dest_service)
{
    struct sdf_msg_binding binding;
    int status;
    struct sdf_msg_binding **found;
    struct sdf_msg_binding *ret;

    binding.dest_vnode = dest_vnode;
    binding.dest_service = dest_service;

    status = pthread_rwlock_rdlock(&sdf_msg_binding_tree.lock);
    plat_assert(!status);
    found = (struct sdf_msg_binding **)tfind(&binding,
                                             &sdf_msg_binding_tree.root,
                                             sdf_compare_msg_bindings);
    ret = found ? *found : NULL;

    if (ret) {
        status = pthread_rwlock_rdlock(&ret->lock);
        plat_assert(!status);
    }

    status = pthread_rwlock_unlock(&sdf_msg_binding_tree.lock);
    plat_assert(!status);

    return ret;
}


/*
 * See if an incoming message matches a binding.  If it does not, return 0.  If
 * it does, return 1 if successful and -1 otherwise.
 */
int
sdf_msg_binding_match(sdf_msg_t *msg)
{
    int ret;
    int status;
    struct sdf_msg_binding *binding =
        sdf_find_and_read_lock_msg_binding(msg->msg_dest_vnode,
                                           msg->msg_dest_service);

    if (!binding)
        return 0;

    ret = sdf_msg_action_apply(binding->action, msg);
    status = pthread_rwlock_unlock(&binding->lock);
    plat_assert(!status);
    if (!ret)
        return 1;

    sdf_msg_free(msg);
    return -1;
}


struct sdf_msg_binding *
sdf_msg_binding_create(struct sdf_msg_action *action,
                       vnode_t dest_vnode,
                       service_t dest_service) {
    struct sdf_msg_binding *ret;
    int failed;
    int status;

    ret = plat_alloc(sizeof (*ret));
    failed = !ret;
    if (ret) {
        ret->action = action;
        ret->dest_vnode = dest_vnode;
        ret->dest_service = dest_service;
        status = pthread_rwlock_init(&ret->lock, NULL);
        plat_assert(!status);
    }

    if (!failed) {
        status = pthread_rwlock_wrlock(&sdf_msg_binding_tree.lock);
        plat_assert(!status);

        /*
         * Sanity check that this does not exist separately because
         * the tsearch behavior does not match the documentation and
         * we can't count on that bug staying unfixed.
         *
         * Specifically the Centos 5.1 Linux man page says that
         * tsearch returns a pointer to the item like tfind, but on
         * insertion it's returning the address of the binary tree node
         * where the first entry is instead.  Depending on this behavior
         * could cause a segmentation fault if we tried to double indirect
         * and the library changes; so we just punt.
         */
        if (tfind(ret, &sdf_msg_binding_tree.root, sdf_compare_msg_bindings)) {
            failed = 1;
        }

        if (!failed && !tsearch(ret, &sdf_msg_binding_tree.root,
                                sdf_compare_msg_bindings)) {
            failed = 1;
        }

        pthread_rwlock_unlock(&sdf_msg_binding_tree.lock);
    }

    if (failed && ret) {
        status = pthread_rwlock_destroy(&ret->lock);
        plat_assert(!status);
        plat_free(ret);
        ret = NULL;
    }

    if (ret)
        sdf_msg_new_binding(dest_service);
    return (ret);
}

void
sdf_msg_binding_free(struct sdf_msg_binding *binding) {
    int status;
    struct sdf_msg_binding **other;

    status = pthread_rwlock_wrlock(&sdf_msg_binding_tree.lock);
    plat_assert(!status);
    status = pthread_rwlock_wrlock(&binding->lock);
    plat_assert(!status);

    other = (struct sdf_msg_binding **)tfind(binding,
                                             &sdf_msg_binding_tree.root,
                                             sdf_compare_msg_bindings);
    plat_assert(other);
    plat_assert(*other == binding);
    tdelete(binding, &sdf_msg_binding_tree.root, sdf_compare_msg_bindings);

    status = pthread_rwlock_unlock(&binding->lock);
    plat_assert(!status);
    status = pthread_rwlock_destroy(&binding->lock);
    plat_assert(!status);
    status = pthread_rwlock_unlock(&sdf_msg_binding_tree.lock);
    plat_assert(!status);
}

static void
nothing(void *arg) {
}

static __attribute__((destructor)) void
sdf_msg_binding_tree_destroy() {
    int status;

    /* User is responsible for freeing contents; so let them leak */
    if (sdf_msg_binding_tree.root) {
        tdestroy(sdf_msg_binding_tree.root, &nothing);
    }
    status = pthread_rwlock_destroy(&sdf_msg_binding_tree.lock);
    plat_assert(!status);
}
