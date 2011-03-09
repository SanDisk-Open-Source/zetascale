/*
 * File:   sdf/platform/alloc_stack.h
 * Author: drew
 *
 * Created on March 30, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: mbox_scheduler.c 3444 2008-09-17 10:13:48Z drew $
 */
#include <stddef.h>

#include "platform/closure.h"
#include "platform/mbox_scheduler.h"
#include "platform/stdlib.h"

#include "fth/fth.h"
#include "fth/fthMbox.h"

struct plat_mbox_scheduler {
    plat_closure_scheduler_t base;
    fthMbox_t work;
    plat_closure_scheduler_shutdown_t shutdown;
};

static plat_closure_activation_base_t *
scheduler_alloc_activation(plat_closure_scheduler_t *self, size_t size);
static void
scheduler_add_activation(plat_closure_scheduler_t *self,
                         plat_closure_activation_base_t *activation);
static void
scheduler_shutdown(plat_closure_scheduler_t *self,
                   plat_closure_scheduler_shutdown_t shutdown);

plat_closure_scheduler_t *
plat_mbox_scheduler_alloc() {
    struct plat_mbox_scheduler *sched;

    sched = plat_calloc(1, sizeof (*sched));
    if (sched) {
        sched->base.alloc_activation_fn = &scheduler_alloc_activation;
        sched->base.add_activation_fn = &scheduler_add_activation;
        sched->base.shutdown_fn = &scheduler_shutdown;

        fthMboxInit(&sched->work);

        sched->shutdown = plat_closure_scheduler_shutdown_null;
    }

    return (&sched->base);
}

void
plat_mbox_scheduler_main(uint64_t arg) {
    struct plat_mbox_scheduler *sched = (struct plat_mbox_scheduler *)arg;

    plat_closure_activation_base_t *activation;

    while ((activation =
            (plat_closure_activation_base_t *)fthMboxWait(&sched->work))) {
        plat_closure_scheduler_set(&sched->base);
        (activation->do_fn)(&sched->base, activation);
        plat_free(activation);
        plat_closure_scheduler_set(NULL);
    }

    if (!plat_closure_scheduler_shutdown_is_null
        (&sched->shutdown)) {
        if (sched->shutdown.base.context == &sched->base ||
            sched->shutdown.base.context ==
            PLAT_CLOSURE_SCHEDULER_ANY) {
            (*sched->shutdown.fn)(&sched->base, sched->shutdown.base.env);
        } else {
            plat_closure_apply(plat_closure_scheduler_shutdown,
                               &sched->shutdown);
        }
    }

    plat_free(sched);
}

static plat_closure_activation_base_t *
scheduler_alloc_activation(plat_closure_scheduler_t *self, size_t size) {
    return (plat_closure_activation_base_t *)plat_alloc(size);
}

static void
scheduler_add_activation(plat_closure_scheduler_t *self,
                         plat_closure_activation_base_t *activation) {
    struct plat_mbox_scheduler *sched = (struct plat_mbox_scheduler *)self;

    fthMboxPost(&sched->work, (uint64_t)activation);
}

static void
scheduler_shutdown(plat_closure_scheduler_t *self,
                   plat_closure_scheduler_shutdown_t shutdown) {
    struct plat_mbox_scheduler *sched = (struct plat_mbox_scheduler *)self;

    sched->shutdown = shutdown;
    fthMboxPost(&sched->work, 0);
}
