/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf_msg.h
 * Author: Brian Horn
 *
 * Created on February 20, 2008, 7:45 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg.h,v 1.1 2008/05/22 09:48:07 drew Exp drew $
 */

#ifndef _SDF_MSG_H
#define _SDF_MSG_H

#define __STD_LIMIT_MACROS
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>
#include "platform/types.h"
#include "platform/unistd.h"
#include "platform/assert.h"
#include "platform/shmem.h"
#include "platform/types.h"
#include "sys/types.h"
#include "sys/socket.h"
#include "sys/un.h"
#include "platform/socket.h"
#include "platform/unistd.h"
#include "common/sdftypes.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "fth/fthSpinLock.h"


#ifdef __cplusplus
extern "C" {
#endif

/* FIXME */
#if 0
/* FIXME hardcode in order to build for now */
#define SDF_USE_CONDITION_VARIABLES
/* #define SDF_USE_SEMAPHORES */
#endif

/* @brief key size setting for mkey */
#define MSG_KEYSZE 17

/* XXX: These need to be unified with common/sdftypes.h */
typedef uint16_t clusterid_t;
typedef uint32_t serviceid_t;

/** @brief Out-of-band values for vnode_t */
enum vnode_special {
    VNODE_UNKNOWN = UINT32_MAX,
    VNODE_ANY = UINT32_MAX - 1,
    VNODE_LIMIT = UINT32_MAX - 1
};
typedef uint32_t taskid_t;
typedef uint64_t seqnum_t;
typedef uint32_t service_t;
typedef uint64_t msgtime_t;

/** @brief Out-of-band values for SDF_vnode_t */
enum service_special {
    SERVICE_UNKNOWN = UINT32_MAX,
    SERVICE_ANY = UINT32_MAX - 1,
    SERVICE_LIMIT = UINT32_MAX - 1
} SDF_special_vnodes_t;


typedef uint32_t boolean_t;
typedef uint16_t msg_type_t;
typedef uint16_t bufftrkr_t;


/*
 * Function prototypes.
 */
int sdf_msg_report_version(char *buf, int len);


/*
 * Typedefs for convenience.
 */
typedef struct sdf_msg_binding sdf_msg_binding_t;


/* Externs not here due to order-of-includes */
struct sdf_msg_action;
/* Deprecated */
struct sdf_fth_mbx;
struct sdf_resp_mbx;

/*
 * FIXME: recalculate hdr size based on additions
 *
 * xxx total bytes in this message header, not including the payload
 */
typedef struct sdf_msg {
    uint16_t cur_ver;                /* Current version */
    uint16_t sup_ver;                /* Supported version */
    msg_type_t msg_type;             /* message type */
    uint32_t msg_flags;              /* SDF_msg_flags */
    union {
        struct {
            int64_t sent_id;         /* Sent id */
            int64_t resp_id;         /* Response id */
        };
        char mkey[MSG_KEYSZE];       /* hash key for mbox decoupling */
        int64_t mkey_int;            /* hash key (going forward) */
    };

    serviceid_t msg_src_service;     /* source service id */
    serviceid_t msg_dest_service;    /* dest service id aka protocol */
    vnode_t msg_src_vnode;           /* source virtual node id */
    vnode_t msg_dest_vnode;          /* destination virtual node id */
    uint32_t msg_len;                /* # bytes in msg hdr + payload */
    struct sdf_msg *next;            /* Used in a variety of cases */

    uint16_t msg_version;            /* version of this structure */
    clusterid_t msg_clusterid;       /* identifies cluster membership */
    bufftrkr_t buff_seq;             /* tracker num for preposted "bin buffs" */
    uint64_t msg_conversation;       /* Unique number for any service type of request-response msg */
    seqnum_t msg_seqnum;             /* Unique number for service specific generated send */
    seqnum_t msg_in_order_ack;       /* ack# up to & including */
    uint64_t msg_out_of_order_acks;  /* acks past in order point */
    uint64_t msg_out_of_order_nacks; /* negative acknowledgements */
    msgtime_t msg_timestamp;         /* monotonically increasing time */
    /**
     * In the over-the-wire protocol,
     * msg_flags & SDF_MSG_MBX_RESP_EXPECTED implies that the response
     * will have repakrpmbx_from_req set to the request's akrpmbx.
     */
    struct sdf_fth_mbx *akrpmbx;     /* fth mbox struct for acks, resp */
    struct sdf_fth_mbx *akrpmbx_from_req;     /* reply to akrpmbx from request */

    fthThread_t *fthid;              /* FIXME temp for debug tracking of posting thread */
    struct sdf_msg_bin_init *ndbin;  /* struct pointer for msg bin tracking */
    struct sdf_queue_item *msg_q_item;      /* have to track this and release if present */
    uint64_t msg_sendstamp;          /* monotonically increasing total sent msgs per node  */

    /*
     * XXX: drew 2009-01-05 Alignment is to coarsest granularity to x86_64.
     * Should probably come out of the same place that's providing the
     * alignment for flash, etc.
     */
    char msg_payload[]               /* Guts of the message */
        __attribute__((aligned(16)));
} sdf_msg_t;

PLAT_SP_VAR_OPAQUE(sdf_msg_sp, struct sdf_msg);

#define SDF_PAYLOAD_ADDR(msg)   (&msg->msg_payload[0])

typedef enum sdf_q_item_type {
    SDF_ENGINE_MESSAGE_NORMAL = 0,
    SDF_ENGINE_SCOREBOARD_ITEM_IN_QUEUE,
    SDF_ENGINE_SHUTDOWN_QUEUE
} sdf_q_item_type_t;

typedef enum sdf_msg_version_type {
    SDF_MSG_VERSION = 1,
    SDF_MSG_SRVERSION,
    SDF_MSG_SYNTHETIC
} sdf_msg_version_type_t;

struct sdf_queue_item {
    struct sdf_msg *q_msg;
    uint32_t q_taskid;
    sdf_q_item_type_t type;
};

typedef struct sdf_queue_item *sdf_queue_item_t;

/* SDF_QUEUE_SIZE must fit in fields q_fill_index/q_emtpy_index */
#define SDF_QUEUE_SIZE  256

enum sdf_queue_status {
    QUEUE_SUCCESS = 0,
    QUEUE_FULL,
    QUEUE_NOQUEUE,
    QUEUE_EMPTY,
    QUEUE_PTH_WAIT_ERR
};

#define B_FALSE 0
#define B_TRUE  1

typedef struct q_control q_control_t;
struct q_control {
    int (*send)(q_control_t *);
    int sock;
    struct sockaddr_un socka;
    int sockalen;
};

/* Various ways of waiting on a struct sdf_queue */
typedef enum sdf_queue_wait_type {
    SDF_WAIT_SEMAPHORE,
    SDF_WAIT_CONDVAR,
    SDF_WAIT_FTH
} sdf_qwt_t;

/**
 * @brief Queue with fixed size, multiple writers, any number of readers
 * but multiple writers may not be performant.
 */
typedef struct sdf_queue {
    /**
     * @brief Lock for sdf_post
     *
     * Although atomic operation use makes sdf_post interoperate correctly
     * without read locks the separate update of sdf_queue[i] and
     * q_atomic_val.q_fill_index would allow competing writers to
     * overwrite each others entries with failures leaving objects in
     * the queue twice.
     *
     * Protect against this.
     */
    fthSpinLock_t post_spin;

    sdf_queue_item_t sdf_queue[SDF_QUEUE_SIZE]; /* Actual circular queue */

    union q_atomic {                    /* Info about queue above */
        struct {
            uint32_t q_fill_index : 8;  /* Index to enqueue next entry */
            uint32_t q_empty_index : 8; /* Index to dequeue next entry */
            uint32_t qfull : 1;         /* Is the queue full? */
            uint32_t qempty : 1;        /* Is the queue empty? */
        } q_atomic_bits;                /* As bit fields */
        uint32_t q_atomic_int;          /* As integer value */
    } q_atomic_val;                     /* Atomically update information */

    enum sdf_queue_wait_type q_wait_type; /* Which waiting mechanism */
    union {
        sem_t q_semaphore;              /* Something to wait upon */
        struct {
            pthread_mutex_t q_mutex;    /* protects two fields below */
            boolean_t q_waiter;         /* Indicates that someone is waiting */
            pthread_cond_t q_cv;        /* Something to wait upon */
        } q_condvar;
        fthThreadQ_lll_t  q_fth_waiters; /* List of fth waiters */
    } q_wait_obj;                       /* The thing to wait upon */
    void *readq;                        /* Read queue */
    char queue_name[257];           /* For debugging */
} sdf_queue_t;

typedef struct sdf_queue_pair {
    struct sdf_queue *q_in;     /* queue service thread to msg thread */
    struct sdf_queue *q_out;    /* queue msg thread to service thread */
    uint64_t sdf_conversation;  /* monotonically increasing */
    seqnum_t sdf_next_seqnum;   /* next out sequence # to assign */
    seqnum_t sdf_last_ack;      /* last in-order seq # acknowledged */
    seqnum_t sdf_out_of_order_acks; /* out of order acks */
    seqnum_t sdf_negative_ack;     /* negative ack # */
    vnode_t src_vnode;          /* source node */
    vnode_t dest_vnode;         /* destination node */
    service_t src_service;      /* source service */
    service_t dest_service;     /* destination service */
    uint32_t flags;             /* tbd */
} sdf_queue_pair_t;

struct sdf_outstanding_msgs {
    uint64_t out_last_xmit;          /* time last transmitted */
    uint32_t   out_retries;            /* count of retransmit retries */
    struct sdf_queue_item *out_msgs; /* message xmitted, but not acked */
};

/**
 * @brief Enqueue
 *
 * Multiple writers are supported with a spin-lock for concurrency control
 * so this is correct but may not be performant.
 *
 * @return  QUEUE_SUCCESS on success, QUEUE_FULL when queue is full.
 */
int sdf_post(struct sdf_queue *queue, struct sdf_queue_item *q_item);

/* the token "wait" was giving fits to the plat/poisoned pragmas */
struct sdf_queue_item *sdf_fetch(struct sdf_queue *queue, boolean_t should_wait);

boolean_t sdf_check_queue(struct sdf_queue *queue);


/** @brief Return the size of the message payload */

int
sdf_msg_get_payloadsze(struct sdf_msg *msg);

struct sdf_queue *
sdf_create_queue(void *(*alloc_fn)(int64_t mem_size, q_control_t *),
                 q_control_t *qc, enum sdf_queue_wait_type,
                 char *queue_name);

/**
 * @brief Create queue pair
 *
 * The combinations src_vnode == VNODE_ANY, src_service == SERVICE_ANY,
 * dest_vnode != VNODE_ANY, dest_service != VNODE_ANY
 *
 * and
 *
 * src_vnode != VNODE_ANY, src_service != SERVICE_ANY,
 * dest_vnode == VNODE_ANY, dest_service == VNODE_ANY
 *
 * are supported and may be used to associate a given queue pair with
 * all inbound or outbound traffic.
 *
 * For example, this wildcard behavior allows the FLSH service to use a
 * single queue pair for handling traffic from any endpoint.
 *
 * XXX The underlying implementation of a single queue might cause fairness
 * issues.
 */
struct sdf_queue_pair *
sdf_create_queue_pair(vnode_t src_vnode, vnode_t dest_vnode, service_t src_service,
                      service_t dest_service, enum sdf_queue_wait_type);

/**
 * @brief Route messages sent to node, service to given mbx
 *
 * The first match out of response mailbox, binding, and queue pair
 * is used.
 *
 * @param action <IN> #sdf_msg_action to which this is attached.  This
 * should remain in existance until the sdf_msg_binding is deleted.
 * The action may be multiplexed  between multiple msg_bindings.
 * sdf_msg_action is used instead of an fthMbox because it
 * supports more complex functionality such as invoking closures on
 * message receipt.  Another possibility would be storing a {user data,
 * message} pair so non-message events can also be multiplexed like
 * release-on-send.
 *
 * @param node <IN> specific node this is attached to.  At most one
 * binding may be associated with a given node, service tuple.
 *
 * @param service <IN> specific port this is attached to.  At most
 * one binding may be associated with a given node, service tuple.
 *
 * @return NULL on failure, non-null on success.  Use
 * #sdf_msg_binding_free on the result.
 */
struct sdf_msg_binding *
sdf_msg_binding_create(struct sdf_msg_action *action, vnode_t node,
                       service_t service);

/**
 * @brief Undo #sdf_msg_bind_endpoint
 *
 * Messages will not be posted to binding->mbox on behalf of this binding
 * after #sdf_msg_binding_free has
 *
 * @param binding <IN> binding to free.
 */
void sdf_msg_binding_free(struct sdf_msg_binding *binding);

/**
 * @brief Delete queue pair
 *
 * FIXME: Currently there is no way to stop enqueing which means that
 * sdf_msg buffers on the list will be leaked.  We should change to
 * sdf_msg_wrapper envelope based interfaces where the envelope is
 * reference counted.
 *
 * FIXME: The queue code is built on top of POSIX tfind() which does
 * no locking.
 */
void sdf_delete_queue_pair(struct sdf_queue_pair *);

/**
 * @brief Allocate sdf_msg with requested payload size
 *
 * FIXME: The queue code is built on top of POSIX tfind() which does
 * no locking.
 */
struct sdf_msg *sdf_msg_alloc(uint32_t size);

/** @brief Free message for all sdfmsg allocated messages */
void sdf_msg_free(struct sdf_msg *msg);

/**
 * @brief Initialize message as if it went through our protocol stack.
 *
 * XXX: drew 2008-12-29 For symetry, ar_mbx_from_req should be a
 * sdf_resp_mbx structure.
 *
 * @param msg <IN> Pointer to sdf_msg() structure allocated with
 * #sdf_msg_alloc.  The caller is not responsible for providing
 * any fields except payload.  The caller must free #msg but not
 * until after the MPI code is done handling it asynchronously.  This
 * may be accomplished by setting the #ar_mbx actlvl field to SACK_ONLY_FTH
 * or SACK_BOTH_FTH with abox field non-NULL which will deliver #msg to
 * that fth mailbox.
 *
 * @param len <IN> Payload length
 *
 * @param ar_mbx <IN> Pointer to sdf_fth_mbx structure which is must exist
 * until the requested ack and/or resp have been received.
 *
 * @param response_mbx <IN> The caller must first invoke
 * #sdf_msg_get_response_mbx on the original request to get the value of this
 * field.  Initial requests must use NULL.
 *
 * @return 0 on success, non-zero otherwise.
 */
int
sdf_msg_init_synthetic(struct sdf_msg *msg,
                       uint32_t len,
                       vnode_t dest_node,
                       service_t dest_service,
                       vnode_t src_node,
                       service_t src_service,
                       msg_type_t msg_type,
                       struct sdf_fth_mbx *ar_mbx,
                       struct sdf_resp_mbx *mresp);

/**
 * @brief Send a message
 *
 * @param msg <IN> Pointer to sdf_msg() structure allocated with
 * #sdf_msg_alloc.  The caller is not responsible for providing
 * any fields except payload.
 *
 * Except when ar_mbx->actlvl == SACK_NONE_FTH the frame work calls
 * sdf_msg_free.  In that case the caller must wait until the message
 * has been sent asynchronouosly which is only detectable via a response
 * from the other end.
 *
 * FIXME: Performant replication will require a different ownership model
 * that allows the same message payload to be sent to multiple destinations.
 *
 * @param len <IN> Payload length
 *
 * @param ar_mbx <IN> Pointer to sdf_fth_mbx structure which is must exist
 * until any requested acknowledgement on send or response have been
 * received.
 *
 * Responses will be delivered to the ar_mbx for both actual responses
 * and errors detected by the sdf_msg code, originating from either the
 * original request or the SDF messaging layer.  Errors provided by the
 * sdfmsg code will have type #SDF_MSG_ERROR and a payload formatted
 * as an #sdf_msg_error_payload.
 *
 * At most one response (whether real or synthetic) will be delivered
 * to the ar_mbx rbox action unless a timeout other than
 * #SDF_FTH_MBX_TIMEOUT_NONE was specified in which case exactly one response
 * will be delivered within the specified time interval.
 *
 * @param response_mbx <IN> The caller must first invoke
 * #sdf_msg_get_response on the original request to get the value of this
 * field.  Initial requests must use NULL.
 *
 * @return 0 on success, non-zero otherwise.
 */
int sdf_msg_send(struct sdf_msg *msg,
                 uint32_t len,
                 vnode_t dest_node,
                 service_t dest_service,
                 vnode_t src_node,
                 service_t src_service,
                 msg_type_t msg_type,
                 struct sdf_fth_mbx *ar_mbx,
                 struct sdf_resp_mbx *response_mbx);

/**
 * @brief Synchronously send a messasge and receive its response.
 *
 * Currently, only calls from an fthThread and with rel == SACK_REL_YES.
 *
 * @param msg <IN> Pointer to sdf_msg() structure allocated with
 * #sdf_msg_alloc.  The caller is not responsible for providing
 * any fields except payload.
 *
 * When rel == SACK_REL_YES this always consumes the msg argument.
 *
 * @param len <IN> Payload length
 *
 * @param ar_mbx <IN> Pointer to sdf_fth_mbx structure which is must exist
 * until any requested acknowledgement on send or response have been
 * received.
 *
 * FIXME: The messaging code must change so that there is a layer of
 * indirection between responses and mailboxes.
 *
 * @param rel <IN> When SACK_REL_YES the message will be freed.
 *
 * @return response on success (which must be freed with  sdf_msg_free) and
 * NULL on failure.
 */
struct sdf_msg *
sdf_msg_send_receive(struct sdf_msg *msg,
                     uint32_t len,
                     vnode_t dest_node,
                     service_t dest_service,
                     vnode_t src_node,
                     service_t src_service,
                     msg_type_t msg_type,
                     /* XXX: Should be num SDF_msg_SACK_rel when include order is resolved */
                     int rel);
/**
 * @brief Return an mresp struct with default values
 *
 */
struct sdf_resp_mbx *
sdf_msg_initmresp(struct sdf_resp_mbx *mresp);


/*
 * @brief Return the fth mailbox that the response would go to
 */
fthMbox_t *
sdf_msg_response_rbox(struct sdf_resp_mbx *mresp);


/**
 * @brief Return an appropriate value #sdf_msg_send's response_mbx arg
 *
 * Safe for use in simulated environments
 * (notably sdf/protocol/replication/tests)
 */
struct sdf_fth_mbx *
sdf_msg_get_response_mbx(struct sdf_msg *msg);

/** @brief Return the hash key and the response mbox that were provided in the requesting msg */
struct sdf_resp_mbx *
sdf_msg_get_response(struct sdf_msg *msg, struct sdf_resp_mbx *mresp);

/** @brief Return error status, SDF_SUCCESS if this is not an error message */
SDF_status_t sdf_msg_get_error_status(struct sdf_msg *msg);

/**
 * @brief call for the release of a send buffer executed by the sdf messaging thread only
 *  this is done when instructed by the sender see SACK levels
 */
int sdf_msg_sbuff_ack(service_t dest_service, struct sdf_msg *msg,
                      struct sdf_fth_mbx *ackmbx);

/*
 * call for the receive of a message off of a created queue arg1, arg2 is for flags (currently not used)
 * arg 3 is the wait type ie: B_TRUE yes I want to block or sleep, B_FALSE just poll it and continue
 */

struct sdf_msg *sdf_msg_receive(struct sdf_queue *, uint32_t, boolean_t);

/* This is dependent upon the underlying transport mechanism */
typedef struct queue_pair *sdf_endpoint_t;

#define SDF_TASKID_WILDCARD    (-1)

#ifdef __cplusplus
}
#endif

#endif /* _SDF_MSG_H */
