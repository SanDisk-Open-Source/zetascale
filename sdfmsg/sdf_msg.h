/*
 * File: sdf_msg.h
 * Author: Johann George
 * (c) Copyright 2008-2009, Schooner Information Technology, Inc.
 */

#ifndef _SDF_MSG_H
#define _SDF_MSG_H

#include <stdint.h>
#include <semaphore.h>
#include "fth/fthMbox.h"
#include "fth/fthSpinLock.h"
#include "sdftcp/stats.h"
#include "sdftcp/msg_int.h"
#include "sdftcp/msg_msg.h"
#include "common/sdftypes.h"
// #include "agent/agent_config.h"


/*
 * Constants.
 */
#define SERVICE_ANY   (UINT32_MAX - 1)
#define VNODE_ANY     (UINT32_MAX - 1)
#define VNODE_UNKNOWN (UINT32_MAX)


/*
 * Name changes.
 */
#define msg_sdf_myrank()   sdf_msg_myrank()
#define msg_sdf_ranks(a)   sdf_msg_ranks(a)
#define msg_sdf_lowrank()  sdf_msg_lowrank()
#define msg_sdf_numranks() sdf_msg_numranks()


/*
 * Structures not yet defined due to order of includes.
 */
struct sdf_fth_mbx;
struct sdf_resp_mbx;
struct sdf_msg_action;


/*
 * Type definitions.
 */
typedef uint32_t service_t;
typedef uint16_t msg_type_t;
typedef struct   sdf_msg_binding sdf_msg_binding_t;
 

/*
 * Enumerations.
 */
typedef enum sdf_queue_wait_type {
    SDF_WAIT_CONDVAR,
    SDF_WAIT_FTH
} sdf_qwt_t;


/*
 * Message header.  I believe that only the first five entries and the payload
 * are actually relevant across the wire.  Seems a bit sad to send all this
 * other stuff.
 */
typedef struct sdf_msg {
    uint16_t   cur_ver;                 /* Current version */
    uint16_t   sup_ver;                 /* Supported version */
    msg_type_t msg_type;                /* Message type */
    uint16_t   msg_flags;               /* Flags */
    int64_t    resp_id;                 /* Response id */

    struct sdf_msg *next;               /* For convenience */
    int64_t         sent_id;            /* Sent id */
    service_t       msg_src_service;    /* Source service */
    service_t       msg_dest_service;   /* Destination service */
    vnode_t         msg_src_vnode;      /* Source rank */
    vnode_t         msg_dest_vnode;     /* Destination rank */
    uint32_t        msg_len;            /* Length including header */

    char msg_payload[]                  /* Payload */
        __attribute__((aligned(16)));
} sdf_msg_t;


/*
 * Queue used for reading.
 */
typedef struct sdf_queue {
    void *readq;                        /* Read queue */
} sdf_queue_t;


/*
 * Now only a single queue.
 */
typedef struct sdf_queue_pair {
    sdf_queue_t *q_out;                 /* Queue */
    vnode_t      dest_vnode;            /* Destination rank */
    service_t    src_service;           /* Source service */
    service_t    dest_service;          /* Destination service */
} sdf_queue_pair_t;


/*
 * Not sure what this is for.
 */
PLAT_SP_VAR_OPAQUE(sdf_msg_sp, struct sdf_msg);


/*
 * Internal function prototypes.
 */
void sdf_msg_int_fcall(cb_func_t func, void *arg);


/*
 * Function prototypes.
 */
int  sdf_msg_myrank(void);
int  sdf_msg_lowrank(void);
int  sdf_msg_hlen(int rank);
int  sdf_msg_numranks(void);
int  sdf_msg_is_live(int rank);
int  sdf_msg_free_buff(sdf_msg_t *msg);
int  sdf_msg_stopmsg(uint32_t a, uint32_t b);
int  sdf_msg_setliven(char *name, char *data);
int  sdf_msg_report_version(char **buf, int *len);
int  sdf_msg_send(sdf_msg_t *msg, uint32_t len, vnode_t drank,
                  service_t dserv, vnode_t srank, service_t sserv,
                  msg_type_t msg_type, struct sdf_fth_mbx *sfm,
                  struct sdf_resp_mbx *srm);
int  sdf_msg_init_synthetic(sdf_msg_t *msg, uint32_t len, vnode_t drank,
                            service_t dserv, vnode_t srank, service_t sserv,
                            msg_type_t type, struct sdf_fth_mbx *sfm,
                            struct sdf_resp_mbx *srm);

void sdf_msg_exit(void);
void sdf_msg_stop(void);
void sdf_msg_alive(void);
void sdf_msg_drain(void);
void sdf_msg_free(sdf_msg_t *msg);
void sdf_msg_bless(sdf_msg_t *msg);
void sdf_msg_new_binding(int service);
void sdf_msg_abox_term(fthMbox_t *abox);
void sdf_msg_mbox_stop(fthMbox_t *mbox);
void sdf_msg_rbox_term(fthMbox_t *rbox);
void sdf_msg_init(int argc, char *argv[]);
void sdf_msg_setinitn(char *name, char *data);
void sdf_msg_call_stat(void (*func)(stat_t *));
void sdf_msg_post(sdf_queue_pair_t *qpair, sdf_msg_t *msg);
void sdf_delete_queue_pair(sdf_queue_pair_t *qpair);

int64_t  sdf_msg_getp_int(char *par);
uint64_t sdf_msg_mbox_try(fthMbox_t *mbox);
uint64_t sdf_msg_mbox_wait(fthMbox_t *mbox);

int       *sdf_msg_ranks(int *np);
fthMbox_t *sdf_msg_response_rbox(struct sdf_resp_mbx *srm);
sdf_msg_t *sdf_msg_alloc(uint32_t size);
sdf_msg_t *sdf_msg_calloc(uint32_t size);
sdf_msg_t *sdf_msg_recv(sdf_queue_pair_t *qpair);
sdf_msg_t *sdf_msg_receive(sdf_queue_t *queue);
sdf_msg_t *sdf_msg_send_receive(sdf_msg_t *smsg, uint32_t len, vnode_t drank,
                                service_t dserv, vnode_t srank,
                                service_t sserv, msg_type_t type, int release);

SDF_status_t         sdf_msg_get_error_status(sdf_msg_t *msg);
sdf_queue_pair_t    *sdf_create_queue_pair(vnode_t snode, vnode_t dnode,
                                           service_t sserv, service_t dserv,
                                           sdf_qwt_t wtype);
struct sdf_resp_mbx *sdf_msg_initmresp(struct sdf_resp_mbx *srm);
struct sdf_resp_mbx *sdf_msg_get_response(sdf_msg_t *msg,
                                          struct sdf_resp_mbx *srm);


/*
 * Binding function prototypes.
 */
int                     sdf_msg_binding_match(sdf_msg_t *msg);
void                    sdf_msg_binding_free(struct sdf_msg_binding *binding);
struct sdf_msg_binding *sdf_msg_binding_create(struct sdf_msg_action *action,
                                               vnode_t node, service_t service);
 
#endif /* _SDF_MSG_H */
