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
 * File: sdf_msg_sync.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#include <stdio.h>
#include "sdf_msg.h"
#include "sdf_fth_mbx.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"


/*
 * For convenience.
 */
typedef struct sdf_queue_pair qpair_t;


/*
 * Static variables.
 */
static int        MyRank;
static int        NRanks;
static int       *Ranks;
static qpair_t   *Queue;
static qpair_t  **Queues;


/*
 * Function prototypes.
 */
static void         sendsync(int dst, sdf_fth_mbx_t *sfm);
static void         recvsync(qpair_t *queue, int dst, int src);
static qpair_t     *create_qpair(int snode, int dnode);


/*
 * Initialize.
 */
void
sdf_msg_sync_init(void)
{
    Ranks = sdf_msg_ranks(&NRanks);
    MyRank = sdf_msg_myrank();
    if (NRanks < 2)
        return;

    if (MyRank != Ranks[0])
        Queue = create_qpair(MyRank, Ranks[0]);
    else {
        int i;

        Queues = m_malloc((NRanks-1) * sizeof(*Queues), "N*qpair_t");
        memset(Queues, 0, (NRanks-1) * sizeof(*Queues));
        for (i = 1; i < NRanks; i++)
            Queues[i-1] = create_qpair(Ranks[0], Ranks[i]);
    }
}


/*
 * Exit.
 */
void
sdf_msg_sync_exit(void)
{
    if (NRanks < 2) {
        plat_free(Ranks);
        return;
    }

    if (MyRank != Ranks[0])
        sdf_delete_queue_pair(Queue);
    else {
        int i;

        for (i = 1; i < NRanks; i++)
            sdf_delete_queue_pair(Queues[i-1]);
        m_free(Queues);
    }
    plat_free(Ranks);
}


/*
 * Synchronize all nodes.
 */
void
sdf_msg_sync(void)
{
    t_smsg(0, "=> sdf_msg_sync()");
    if (NRanks > 1) {
        sdf_fth_mbx_t sfm;

        memset(&sfm, 0, sizeof(sfm));
        sfm.actlvl = SACK_NONE_FTH;
        sfm.release_on_send = 1;

        if (MyRank != Ranks[0]) {
            sendsync(Ranks[0], &sfm);
            recvsync(Queue, MyRank, Ranks[0]);
        } else {
            int i;

            for (i = 1; i < NRanks; i++)
                recvsync(Queues[i-1], Ranks[0], Ranks[i]);
            for (i = 1; i < NRanks; i++)
                sendsync(Ranks[i], &sfm);
        }
    }
    t_smsg(0, "sdf_msg_sync() =>");
}


/*
 * Send a sync message.
 */
static void
sendsync(int dst, sdf_fth_mbx_t *sfm)
{
    int s;
    sdf_msg_t *msg = sdf_msg_alloc(0);

    if (!msg)
        fatal("sdf_msg_alloc failed");
    t_smsg(0, "send sync: n%d => n%d", MyRank, dst);
    s = sdf_msg_send(msg, 0, dst, SDF_SYNC, MyRank, SDF_SYNC, 0, sfm, NULL);
    if (s < 0)
        fatal("sdf_msg_send failed");
}


/*
 * Receive a sync message.
 */
static void
recvsync(qpair_t *queue, int dst, int src)
{
    sdf_msg_t *msg = sdf_msg_recv(queue);

    t_smsg(0, "recv sync: n%d <= n%d", dst, src);
    sdf_msg_free(msg);
}


/*
 * Create a queue pair and complain on failure.
 */
static qpair_t *
create_qpair(int snode, int dnode)
{
    qpair_t *qpair = sdf_create_queue_pair(snode, dnode,
                                    SDF_SYNC, SDF_SYNC, SDF_WAIT_CONDVAR);
    
    if (!qpair)
        fatal("failed to create qpair sn %d sp %d dn %d dp %d wt %d",
            snode, SDF_SYNC, dnode, SDF_SYNC, SDF_WAIT_CONDVAR);
    return qpair;
}
