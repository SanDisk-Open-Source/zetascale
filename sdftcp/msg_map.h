/*
 * File: msg_map.h
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * The interface is as follows:
 *
 *  * msg_map_init and msg_map_exit are called to startup and shutdown the
 *    mapping service.
 *
 *  * Before one calls msg_send, one must call msg_map_send to map the SDF
 *    index (between 0 and N-1) to an index the messaging system requires.  -1
 *    is returned if the mapping is not found.  Might happen if the node has
 *    dropped off.
 *
 *  * Similarly, if one receives a message, msg_map_recv will map the messaging
 *    system index to a SDF index.  -1 is returned if the mapping is not found.
 *    Again, might happen if the node has dropped off.
 *
 *  * One calls msg_map_poll instead of msg_poll.  It takes care of liveness
 *    events and other such things.
 *
 *  * When msg_map_init is called, if noden is 0, we use MPI.  Otherwise, it
 *    waits for noden nodes to come online before returning.  Nodes are not
 *    marked active until the other side deems them active.  To mark one's node
 *    as active to your colleagues, one calls msg_map_alive.
 *
 *  * If *myrank is not -1, that becomes our rank.  If it is -1, we look in the
 *    properties file for entries that look like this.
 *
 *      NODES = 2
 *      NODE[0].CLUSTER = lab02
 *      NODE[1].CLUSTER = lab03
 *
 *    This would indicate that if we are lab02, our rank should be 0 and if we
 *    are lab03, our rank should be 1.  If an entry is not found in the
 *    properties file, we determine our rank dynamically.
 */
#ifndef MSG_MAP_H
#define MSG_MAP_H

#include "msg_msg.h"

int        *msg_map_ranks(int *np);
int         msg_map_lowrank(void);
int         msg_map_numranks(void);
int         msg_map_recv(int nno);
int         msg_map_send(int sdf);
void        msg_map_exit(void);
void        msg_map_alive(void);
void        msg_map_init(ntime_t etime, int noden, int *myrank);
msg_info_t *msg_map_poll(ntime_t etime);

#endif /* MSG_MAP_H */
