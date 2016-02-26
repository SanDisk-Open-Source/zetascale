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

#ifndef TCP_H
#define TCP_H 1

/*
 * File:   tcp.h
 * Author: Guy Shaw
 *
 * Created on September 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

//  +   +   +   +   +   +   +   +   +   +   +   +   +   +   +   +
//	|	|	|	|	|	|	|	|
#include <sys/uio.h>        // Import struct iovec

#define TCP_EXPLAIN 0x01    // Explain failures on stderr
#define TCP_DEBUG   0x02    // Show trace messages on stderr, not just failure

/*
 * Opaque descriptor.
 */
struct tcp;
typedef struct tcp TCP;

/*
 * Visible data types.
 */

/*
 * We use a scatter/gather list with a fixed number of elements,
 * as a reasonable trade-off between simplicity and flexibility.
 * Four elements is good enough for most of our needs.
 */
#define MSG_SGE 4

typedef unsigned long TAG;
typedef unsigned long RANK;
typedef struct iovec iovec_t;

struct smsg {
    TAG      stag;
    TAG      dtag;
    RANK     drank;
    iovec_t  sgv[MSG_SGE];
};

struct rmsg {
    TAG      stag;
    TAG      dtag;
    RANK     srank;
    void    *buf;
    size_t   len;
};

typedef struct smsg SMSG;
typedef struct rmsg RMSG;
typedef void (SFUNC)(SMSG *, void *);

/*
 * Function prototypes.
 */

/**
 * @brief Initialize
 * @param tcpref -- Set to a new TCP *.
 *    On early failure *tcpref is NULL, but *tcpref can be non-NULL, even
 *    on failure, in order to retain information about any errors
 *    that were encountered.
 *
 * @param flags  -- control debugging trace messages and failure messages.
 * @return 0 on success -errno on failure.
 * *tcpref is an opaque descriptor for a new TCP connection.
 */
extern int tcp_init(TCP **tcpref, int flags);

/**
 * @brief How many nodes are in the given TCP messaging "cluster".
 * @return On success: >0, "node" count; on failure: <0, -(error_code)
 */
extern int tcp_node_count(TCP *tcp);

/**
 * @brief Which node am I?
 * @return node number of the current process (later, thread?)
 * returns <0 in the event of any failure, such as invalid data.
 */
extern int   tcp_me(TCP *tcp);

/**
 * @brief Underlying file descriptor of a given node.
 */
extern int   tcp_send_fd(TCP *tcp, int nno);
extern int   tcp_recv_fd(TCP *tcp, int nno);

/**
 *
 */

/*
 * Send a message.
 */
extern int tcp_send(TCP *tcp, SMSG *smsg, SFUNC *func, void *arg);

/*
 * Receive a message.
 */
extern int tcp_recv(TCP *tcp, RMSG *rmsg, long timeout);

/*
 * Receive a message having a given TAG value.
 * Enque any messages that do not match that value.
 */
extern int tcp_recv_tag(TCP *tcp, RMSG *rmsg, TAG tag, long timeout);

/**
 * @brief Perform orderly shutdown of the TCP messaging cluster.
 * Release any kernel resources and memory used to support it.
 */
extern int   tcp_exit(TCP *tcp);

#endif /* ndef TCP_H */
