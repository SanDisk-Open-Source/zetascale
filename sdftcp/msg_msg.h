/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: msg_msg.h
 * Author: Johann George, Norman Xu, Enson Zheng
 * Copyright (c) 2008-2009, Schooner Information Technology, Inc.
 * $Id: sdfmt_msg.h 5542 2009-01-12 15:46:24Z gxu $
 *
 * See README for documentation.
 */
#ifndef MSG_MSG_H
#define MSG_MSG_H

#include <stdint.h>
#include <net/if.h>
#include <sys/uio.h>
#include <sys/types.h>
#include "tools.h"


/*
 * Parameters
 */
#define MSG_NSGE 4                      /* Number of gather entries */

/*
 * Events that could be received from the messaging system.
 */
#define MSG_ENONE 0                     /* No event has occurred */
#define MSG_EJOIN 1                     /* Node has joined */
#define MSG_EDROP 2                     /* Node has dropped off */
#define MSG_EIADD 3                     /* Interface has been added */
#define MSG_EIDEL 4                     /* Interface has been deleted */
#define MSG_ERECV 5                     /* Message has been received */
#define MSG_ESENT 6                     /* Message has been sent */


/*
 * Message send flags.
 */
#define MS_USESEQ 1                     /* Do not generate sequence number */


/*
 * Macro functions
 */
#define msg_retn2h(v) (MsgLilEndian ? v : msg_getintn(&v, sizeof(v)))
#define msg_setn2h(v) (MsgLilEndian ? 0 : (v = msg_getintn(&v, sizeof(v))))
#define msg_seth2n(v) (MsgLilEndian ? 0 : msg_setintn(&v, sizeof(v), v))


/*
 * Type definitions.
 */
typedef struct   msg_info msg_info_t;
typedef struct   msg_send msg_send_t;
typedef int      msg_post_find_t(msg_info_t *);
typedef void     msg_post_call_t(msg_info_t *);
typedef void     (*msg_send_call_t)(msg_send_t *);
typedef void     msg_livefunc_t(int live, int rank, void *arg);
typedef int16_t  msg_tag_t;
typedef int32_t  msg_rank_t;
typedef int64_t  msg_seqn_t;
typedef uint16_t msg_qos_t;
typedef uint16_t msg_port_t;
typedef uint64_t msg_id_t;
typedef struct   iovec iovec_t;


/*
 * Message initialization parameter structure.  These are the parameters that
 * are passed to msg_init.
 */
typedef struct msg_init {
    char       *alias;                  /* Node alias */
    char       *iface;                  /* Interfaces */
    ntime_t     dead;                   /* Minimum time node stays dead */
    ntime_t     live;                   /* Node liveness time */
    ntime_t     ping;                   /* Ping interval */
    ntime_t     linklive;               /* Link liveness time */
    uint64_t    affinity;               /* Thread affinity */
    uint32_t    nhold;                  /* Number of hold info buffers */
    uint16_t    class;                  /* Class */
    uint16_t    nconn;                  /* Number of connections */
    uint16_t    nobcast;                /* Do not use broadcasting */
    uint16_t    nthreads;               /* Number of messaging threads */
    uint16_t    ntunique;               /* Threads with unique affinity */
    uint16_t    mustlive;               /* Assign liveness its own thread */
    msg_port_t  tcpport;                /* TCP port */
    msg_port_t  udpport;                /* UDP port */
} msg_init_t;


/*
 * Message information structure.  Returned by msg_poll.  The fields labeled I
 * are only used internally.
 */
struct msg_info {
    msg_info_t      *link;              /* I: Many purposes */
    msg_post_call_t *func;              /* I: Post function */
    void            *parg;              /* I: Post argument */
    uint16_t         type;              /* Event type */
    int              nno;               /* Node number */
    msg_tag_t        stag;              /* Source tag (ERECV/ESENT) */
    msg_tag_t        dtag;              /* Destination tag (ERECV/ESENT) */
    uint32_t         len;               /* Message length (ERECV/ESENT) */
    msg_id_t         mid;               /* Message id (ERECV/ESENT/ECOMP) */
    msg_seqn_t       seqn;              /* Sequence number */
    char            *data;              /* Data (EIADD, EIDEL, ERECV) */
    char            *error;             /* Error message */
};


/*
 * Message send structure.  msg_salloc is called to return a new empty
 * structure.  Fields not labeled with UI may be set by the caller who then
 * calls msg_send.  Once the send actually happens, the structure is
 * automatically freed with msg_sfree.  Before it is freed, if func has been
 * set, it is then called with a pointer to this structure.  If func is not set
 * and data is set, data is freed.
 */
struct msg_send {
    int              nno;               /* Node number */
    msg_tag_t        stag;              /* Source tag */
    msg_tag_t        dtag;              /* Destination tag */
    msg_qos_t        qos;               /* Quality of service */
    uint16_t         flags;             /* Flags */
    uint16_t         nsge;              /* Number of gather entries */
    msg_id_t         sid;               /* Sender id */
    msg_seqn_t       seqn;              /* Sequence number */
    iovec_t          sge[MSG_NSGE];     /* Gather entries */
    msg_send_call_t  func;              /* Called when structure is freed */
    void            *data;              /* See structure comment */
    msg_send_t      *link;              /* UI: For housekeeping */
    size_t           sent;              /* UI: Number of bytes sent */
    uint16_t         usge;              /* UI: Unfinished gather index */
    size_t           ubufsize;          /* UI: Unfinished buffer size */
};


/*
 * Message connection structure.
 */
typedef struct {
    char       *raddr;                  /* Remote address */
    char       *cface;                  /* Interface name */
} msg_conn_t;


/*
 * Message node structure.
 */
typedef struct {
    int         nno;                    /* Node number */
    msg_port_t  port;                   /* Port */
    char       *name;                   /* Node name */
    int         nconn;                  /* Number of connections */
    msg_conn_t *conns;                  /* Connections */
} msg_node_t;


/*
 * Versioning prototypes.
 */
int tcp_report_version(char **buf, int *len);


/*
 * Message function prototypes.
 */
void        msg_livecall(int on, int fth, msg_livefunc_t *func, void *arg);
int         msg_hlen(int rank);
int         msg_mynodeno(void);
int         msg_trace(int flags);
int         msg_nno2rank(int nno);
int         msg_ismyip(char *name);
int         msg_rank2nno(int rank);
int         msg_work(ntime_t etime);
int         msg_cmpnode(int n1, int n2);
int         msg_getnode(msg_node_t *mnode, int nno);
int         msg_setlive(char *name, msg_init_t *init);
int         msg_addraddr(char *name, char *addr, char *iface);
void        msg_exit(void);
void        msg_wake(void);
void        msg_drain(void);
void        msg_affinity(int n);
void        msg_nodedrop(int nno);
void        msg_setflags(char *str);
void        msg_setstats(char *str);
void        msg_addrname(char *name);
void        msg_init(msg_init_t *init);
void        msg_send(msg_send_t *send);
void        msg_sfree(msg_send_t *send);
void        msg_ifree(msg_info_t *info);
void        msg_setrank(int nno, int rank);
void        msg_freenodes(msg_node_t *nodes);
void        msg_setstate(int nno, int state);
void        msg_call_post(msg_post_find_t *func);
void        msg_setintn(void *ptr, int len, uint64_t val);
char       *msg_nodename(int nno);
char       *msg_idata(msg_info_t *info);
int64_t     msg_getintn(void *ptr, int len);
ntime_t     msg_ntime(void);
ntime_t     msg_endtime(ntime_t ntime);
msg_info_t *msg_ialloc(void);
msg_info_t *msg_poll(int64_t timeout);
msg_info_t *msg_want(ntime_t etime, msg_info_t *want1, msg_info_t *want2);
msg_node_t *msg_getnodes(int nno, int *no_nodes);
msg_send_t *msg_salloc(void);


/*
 * Global variables.
 */
int MsgLilEndian;

#endif /* MSG_MSG_H */
