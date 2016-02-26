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

#ifndef TCP_IMPL_H
#define TCP_IMPL_H 1

/*
 * File:   tcp_impl.h
 * Author: Guy Shaw
 *
 * Created on September 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Internal data structures that implement messaging cluster configuration.
 */

#define MCC_MAGIC 0x6367736d /* msgc -- message configuration */

#define MCN_MAGIC 0x6e67736d /* msgn -- message node */

typedef unsigned int uint_t;
typedef unsigned short ushort_t;

typedef struct sockaddr sockaddr_t;
typedef unsigned short port_t;

/*
 * Queue of messages already received over the wire, but not delivered
 * because they do not yet match any TAGs of interest.
 *
 * Optimizations for later -- maybe:
 *   1. keep track of head of list and tail of list, for fast append.
 *   2. unroll linked list.
 *   3. multiple lists, one per hash value of hash(tag).
 */
struct msgq {
    struct msgq *mq_next;
    RMSG         mq_rmsg;
};

typedef struct msgq MSGQ;

struct mc_node {
    int           mcn_magic;
    char         *mcn_host;       /** @brief official hostname of this node */
    port_t        mcn_port;       /** @brief my server port */
    sockaddr_t    mcn_addr;
    int           mcn_sockfd;     /** @brief server listens on this socket */
    int           mcn_connfd;     /** @brief client sends to this socket */
    int           mcn_flags;      /** @brief control debug messages */
    const char   *mcn_syscall;    /** @brief name of function that failed */
    int           mcn_err;        /** @brief errno after failed syscall */
};

typedef struct mc_node mc_node_t;

struct mc_config {
    int         mcc_magic;
    int         mcc_size;   /** @brief size of cluster, number of nodes */
    int         mcc_hosts;  /** @brief number of unique host names */
    int         mcc_self;   /** @brief my own node number */
    int         mcc_flags;  /** @brief control debug messages */
    mc_node_t **mcc_nodes;  /** @brief array of node descriptors */
    MSGQ       *mcc_rcvq;   /** @brief queue of received messages */
};

typedef struct mc_config mc_config_t;

/**
 * @brief Preamble to the message that is to follow.
 */

struct preamble {
    TAG      stag;      // @brief source tag
    TAG      dtag;      // @brief destination tag
    RANK     srank;     // @brief source rank
    size_t   size;      // @brief total size of message to follow
};

typedef struct preamble preamble_t;

/*
 * Function prototypes.
 */

#endif /* ndef TCP_IMPL_H */
