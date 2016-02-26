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
 * File:   sdf_msg_mpi.h
 * Author: Tom Riddle
 *
 * Created on February 25, 2008, 3:45 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_mpi.h 1379 2008-05-26 02:33:17Z tomr $
 */

#ifndef _SDF_MSG_MPI_H
#define _SDF_MSG_MPI_H

/*
 * XXX drew 2008/05/21 this is only used by sdf_msg_mpi and could easily
 * be embedded there.
 */

#ifdef __cplusplus
extern "C" {
#endif

/* mpi should be included before assert, etc. poisoning occurs */
#if 0
#ifdef MPI_BUILD
#include "mpi.h"
#endif
#endif

#include "sdf_msg.h"
#include "sdfappcommon/XList.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "fth/fthXMbox.h"
#include "applib/XMbox.h"
#include "sdfappcommon/XMbox.h"
#include "sdf_msg_types.h"

typedef struct sdf_msg_bin_init {
    uint32_t binid;            /* used to identify the init seq num with bin itself */
    uint32_t max_msgsize;      /* max msg size message bin will allow without truncation */
    uint32_t sdf_familyid;     /* using this to keep track of registered bins */
    uint32_t protocol_type;    /* general protocol */
    uint32_t fam_msg_type;     /* specific binning can be done on a message level */
    uint32_t sdf_msg_rnode;    /* the receiving node */
    int      sdf_msg_snode;    /* the expected sending node */
    uint32_t num_buffs;        /* number of total msg buffers in a bin */
    uint32_t free_buffs;       /* number of returned or free buffers ready to post */
    uint32_t post_buffs;       /* number of active preposted buffers */
    uint32_t proc_buffs;       /* num of buffs being processed by receiver */
    uint32_t got_buffs;        /* num of buffs with new msgs seen by receiver */
    cbuff    tagbuff[DBNUM];   /* brain dead tracker for now */
    cbuff    *r_buf[DBNUM];    /* buff pointers for the statically created buffs */
    cbuff    *unexp_buf[DBNUM];    /* pointer for unexpected buffers which are alloc'd */
    cbuff    r_buffsze[DBNUM][MBSZE]; /* the actual receive buffers */
#ifdef MPI_BUILD
    MPI_Request mreqs[DBNUM];  /* array of MPI_Requests for static PP receive buffers */
    MPI_Status rstat[DBNUM];   /* PP buffer receive status */
    MPI_Request sreqs[DBNUM];  /* array of MPI_Requests for dynamic send buffers */
#endif
    int bbindx;                /* big buffer counter */
    struct sdf_msg_bbmsg *bbhead;  /* LL head for the big buff tracker */
    struct sdf_msg_bbmsg *bbtail;  /* LL tail for the big buff tracker */
    struct sdf_msg_bbmsg *bbmsg;   /* alloc this one */


    int indx[DBNUM];           /* this is the index into the array for the statically allocated buffers */
    struct sdf_queue_pair *node_q;   /* each node bin needs to be linked with a send queue */
    struct sdf_msg_xchng *head;      /* LL head for buffer tracking */
    struct sdf_msg_xchng *tail;      /* LL tail for buffer tracking */
    struct sdf_msg_xchng *xchng[DBNUM];      /* LL pointer array  */
    struct sdf_msg_xchng st_xchng[DBNUM];    /* LL struct array  */
} sdf_msg_bin_init_t;


/* SDF message struct for big msg notification and internal system messages only */

typedef struct sdf_sys_msg {
    uint32_t sys_command;        /* this is the general command type to decribe operation type */
    struct sdf_msg_bin_init *ndbin;  /* struct pointer for msg bin tracking */
    bufftrkr_t buff_seq;             /* tracker num for preposted "bin buffs" */
    serviceid_t msg_src_proto;   /* source base protocol */
    serviceid_t msg_dest_proto;  /* dest protocol  */
    uint16_t msg_send_node;      /* sending node's id */
    uint16_t msg_dest_node;      /* destination node id */
    uint16_t msg_mpi_type;       /* mpi data type */
    uint16_t msg_dtag;           /* tag offset */
    uint16_t msg_type;           /* message type */
    uint16_t msg_offset;         /* message offset */
    uint32_t msg_blen;           /* buffer length */
    uint64_t msg_req_id;         /* unique sequence num for this req */
    uint32_t data_wd1;           /* spare */
    cbuff    *bin_buff;
    uint32_t ll_cnt;
    uint32_t xindx;
    uint32_t binnum;
    uint32_t data_wd2;           /* spare */
} sdf_sys_msg_t;

/* LL for big buffer tracking */
typedef struct sdf_msg_bbmsg {
    sdf_msg_t *msg;
    int cntr;
    struct sdf_msg_bbmsg **hd;
    struct sdf_msg_bbmsg **tl;
    struct sdf_msg_bbmsg *bbBuffQ;
} sdf_msg_bbmsg_t;

/* LL for client buffer release tracking */
typedef struct sdf_msg_relmsg {
    sdf_msg_t *msg;
    uint64_t cntr;
    uint32_t mflags;
    struct sdf_msg_relmsg *hd;
    struct sdf_msg_relmsg *tl;
    struct sdf_msg_relmsg *relBuffQ;
} sdf_msg_relmsg_t;

#ifdef __cplusplus
}
#endif

#endif /* _SDF_MSG_MPI_H */
