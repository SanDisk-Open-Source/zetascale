/*
 * File: recovery.h
 * Author: Johann George
 * Copyright (c) 2010, Schooner Information Technology, Inc.
 */

#ifndef _RECOVERY_H
#define _RECOVERY_H

#include "shared/private.h"
#include "protocol/action/action_internal_ctxt.h"


/*
 * Home flash NOP command extra data.
 */
typedef struct {
    uint16_t rec_ver;                   /* Recovery version */
} sdf_hfnop_t;



/*
 * Linkage to fast recovery code.
 */
struct sdf_rec_funcs {
    /*
     * Copies a container having the cguid from the surviving node with the
     * given rank.  Returns -1 if the other node cannot handle fast recovery, 0
     * if the recovery failed and 1 if we succeeded.  Called from
     * simple_replicator_start_new_replica in simple_replication.c
     */
    int (*ctr_copy)(vnode_t rank,
                    struct shard *shard,
                    SDF_action_init_t *pai);

    /*
     * Called when a message is received by home flash that is intended for
     * recovery.  Called from pts_main in home_flash.c.
     */
    void (*msg_recv)(sdf_msg_t *msg,
                     SDF_action_init_t *pai,
                     SDF_action_state_t *pas,
                     struct flashDev *flash);

    /*
     * Called to fill a HFNOP structure.
     */
    void (*nop_fill)(sdf_hfnop_t *nop);

    /*
     * Called before we send a delete to the other side.
     */
    void (*prep_del)(vnode_t rank, struct shard *shard);
} *sdf_rec_funcs;
 

#endif /* _RECOVERY_H */
