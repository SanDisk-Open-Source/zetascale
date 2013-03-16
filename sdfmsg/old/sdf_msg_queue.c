/*
 * File: sdf_msg_queue.c
 * Author: Brian Horn
 * (c) Copyright 2008-2009, Schooner Information Technology, Inc.
 */
#define _POSIX_C_SOURCE 200112  /* XXX - Where should this be? */

#include <errno.h>
#include <pthread.h>
#include <search.h>
#include <time.h>
#include "platform/types.h"
#include "platform/assert.h"
#include "platform/string.h"
#include "platform/shmem.h"
#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/time.h"
#include "sdftcp/msg_trace.h"

#include "sdf_fth_mbx.h"
#include "sdf_msg_int.h"
#include "sdf_msg_hmap.h"
#include "sdf_msg_types.h"
#include "sdf_msg_action.h"
#include "sdf_msg_binding.h"
#include "sdf_msg_wrapper.h"


/*
 * Function prototypes.
 */
static int check_mkey(sdf_msg_t *msg);


extern int MeRank;                    /* this is so the debug prints put out the right node info */
extern msg_state_t *sdf_msg_rtstate;     /* during runtime track simple msg engine state, allow sends */
extern int sdf_msg_register_queue_pair(struct sdf_queue_pair *q_pair);
extern void sdf_msg_send_outgoing(struct sdf_msg *msg);  /* the msg is sent via MPI at the moment */
extern struct SDFMSGMap respmsg_tracker;
extern uint32_t allow_posts;

static int num_msgs_sent;

#define LOG_CAT PLAT_LOG_CAT_SDF_SDFMSG
#define XXX_UNUSED 0

enum {
    SDF_MSG_SUCCESS = 0,
    SDF_MSG_FAILURE = 1,
    SDF_MSG_SYNTH_INIT = 2
};

#define DBGP 0

static void
sdf_mem_free(void *ptr) {
    return (plat_free(ptr));
}

/* ARGSUSED1 */
static void*
sdf_mem_alloc(int64_t sze, q_control_t *qc) {

  return (plat_alloc((uint32_t)sze));
}

void
sdf_msg_free(struct sdf_msg *msg) {
    if (msg->msg_q_item) {
        sdf_mem_free(msg->msg_q_item);
    }
    sdf_mem_free(msg);
}

/* quick way to get the payload size */

int
sdf_msg_get_payloadsze(struct sdf_msg *msg) {
    return(msg->msg_len - (((char *) msg->msg_payload) - ((char *) msg)));
}

struct sdf_msg *
sdf_msg_alloc(uint32_t size) {
    struct sdf_msg *msg = (struct sdf_msg *)sdf_mem_alloc(size + sizeof(struct sdf_msg), NULL);
    /*
     * As long as we're providing header fields which precludes the user from
     * doing their own simple zero operation we should zero everything so 
     * valgrind can correctly detect memory issues.
     */
    memset(msg, 0, sizeof(*msg));
    msg->msg_flags = 0;
    msg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF; /* flag this so we can release it properly later */
    return(msg);
}

struct sdf_queue_item *
sdf_queue_item_alloc(uint32_t size) {

    struct sdf_queue_item *q_item = (struct sdf_queue_item *)plat_alloc(sizeof(*q_item));
    if (q_item) {
        q_item->q_msg = NULL;
        q_item->q_taskid = 0;
        q_item->type = SDF_ENGINE_MESSAGE_NORMAL;
    }

    return (q_item);
}

enum sdf_queue_event {
    SDF_QUEUE_DATA_AVAILABLE,
    SDF_QUEUE_CONNECTION_AVAILABLE,
    SDF_QUEUE_CONNECTION_LOST,
};


#define queue_assert(expr) \
    if (! (expr)) { \
        printf("%s: assertion failed file %s line %d qfull %d qempty %d " \
                "fill index %d empty index %d\n", \
                __func__, \
                __FILE__, \
                __LINE__, \
                qval.q_atomic_bits.qempty, \
                qval.q_atomic_bits.qfull, \
                qval.q_atomic_bits.q_fill_index, \
                qval.q_atomic_bits.q_empty_index); \
        fflush(stdout); \
        plat_assert(0); \
    }

/* Soon to be deprecated - only left in home_thread such not to alter the API for homedir_actions() */
struct sdf_fth_mbx *
sdf_msg_get_response_mbx(struct sdf_msg *msg) {
    fatal("deprecated");
    return ((msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_EXPECTED) ?
            msg->akrpmbx : NULL);
}


SDF_status_t
sdf_msg_get_error_status(struct sdf_msg *msg) {
    struct sdf_msg_error_payload *error_payload;
    SDF_status_t ret;

    if (msg->msg_type != SDF_MSG_ERROR) {
        ret = SDF_SUCCESS;
    } else {
        error_payload = (struct sdf_msg_error_payload *)msg->msg_payload;
        ret = error_payload->error;
    }

    return (ret);
}

/* sdf_msg_initmresp() init the mresp mbx to all default conditions */

struct sdf_resp_mbx *
sdf_msg_initmresp(struct sdf_resp_mbx *mresp) {

    plat_assert(mresp);
    mresp->mkey[MSG_KEYSZE - 1] = '\0';
    strncpy(mresp->mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
    mresp->use_mkey_int = 0;
    mresp->rbox = NULL;
    return(mresp);
}


/*
 * Return the fth mailbox that the response would go to.
 */
fthMbox_t *
sdf_msg_response_rbox(sdf_resp_mbx_t *mresp)
{
    return mresp->rbox->rbox;
}


/*
 * sdf_msg_get_response() is a replacement for the previous and soon to be
 * deprecated transmitting of the raw mbox pointer. This grabs both the resp
 * mbox pointer and the hashed key for the response that was generated on the
 * request. To minimize the pain in integrating into the sdf_msg_send API we
 * have a struct that stores both and this fills it in
 */
struct sdf_resp_mbx *
sdf_msg_get_response(struct sdf_msg *msg, struct sdf_resp_mbx *mresp) {

    if (!mresp)
        fatal("sdf_msg_get_response: bad parameter: mresp is NULL");
    if (!msg)
        fatal("sdf_msg_get_response: bad parameter: msg is NULL");
    if (msg->msg_flags & SDF_MSG_FLAG_MKEY_INT)
        fatal("not expecting mkey_int to be used yet");

    msg->mkey[MSG_KEYSZE-1] = '\0';
    logt("get_response msg=%p resp=%p flag=%x mkey=%s src=%d dst=%d",
         msg, mresp, msg->msg_flags, msg->mkey,
         msg->msg_src_vnode, msg->msg_dest_vnode);

    if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) {
        logt("get_response: msg %p: RESP_INCLUDED", msg);
        mresp->rbox = msg->akrpmbx;
        /* note here we are using rbox in mresp to get the response not
         * akrpmbx; */

        if (msg->msg_flags & SDF_MSG_FLAG_MKEY_INT) {
            mresp->mkey_int = msg->mkey_int;
            mresp->use_mkey_int = 1;

            logt("retrieving mkey_int msg %p msg_flags 0x%x mresp %p"
                 " mrespmkey_int 0x%llx mresp->rbox %p akrpmbx %p orig %p",
                    msg, msg->msg_flags, mresp, (long long)mresp->mkey_int,
                    mresp->rbox, msg->akrpmbx, msg->akrpmbx_from_req);
        } else {
            strcpy(mresp->mkey, msg->mkey);
            if (!check_mkey(msg))
                fatal("sdf_msg_get_response: message has no return address");

            mresp->use_mkey_int = 0;
            if (mresp->rbox)
                mresp->rbox = msg->akrpmbx;
            logt("retrieving mkey and response in msg %p msg_flags 0x%x"
                 " akrpmbx %p mresp %p msg_mkey %s"
                 " respmkey %s mresprbox %p",
                    msg, msg->msg_flags, msg->akrpmbx, mresp,
                    msg->mkey, mresp->mkey, mresp->rbox);
        }
    } else if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_EXPECTED) {
        mresp->rbox = msg->akrpmbx;
        msg->mkey[MSG_KEYSZE - 1] = '\0';
        strcpy(mresp->mkey, msg->mkey);     
        if (!check_mkey(msg))
            fatal("sdf_msg_get_response: message has no return address");
        logt("get_response: msg %p: RESP_EXPECTED", msg);
    } else {
        fatal("sdf_msg_get_response: bad parameter: msg, flags=%x",
                                                        msg->msg_flags);
    }
    return mresp;
}


/*
 * Check the mkey and see if it is valid.
 */
static int
check_mkey(sdf_msg_t *msg)
{
    if (MeRank != msg->msg_dest_vnode)
        if (strncmp(msg->mkey, MSG_DFLT_KEY, MSG_KEYSZE) == 0)
            return 0;
    return 1;
}


/*
 * sdf_post() add q_item to queue and wakes up anyone waiting on the queue.
 * returns QUEUE_SUCCESS if queued, QUEUE_FULL if queue is full.
 */
int
sdf_post(struct sdf_queue *queue, struct sdf_queue_item *q_item)
{
    union q_atomic old_qval, qval;
    int ret;


    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: posting to queue \"%s\" %p\n", MeRank, queue->queue_name, queue);

    /* See #sdf_queue post_spin comment */
    FTH_SPIN_LOCK(&queue->post_spin);

    do {
        old_qval = qval = queue->q_atomic_val;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                    "\nNode %d: queue %p full %d empty %d fill_index %d "
                    "empty index %d\n", MeRank,
                    queue, qval.q_atomic_bits.qfull,
                    qval.q_atomic_bits.qempty, qval.q_atomic_bits.q_fill_index,
                    qval.q_atomic_bits.q_empty_index);
       /* Cannot be both full and empty at the same time */
       queue_assert(! (qval.q_atomic_bits.qfull && qval.q_atomic_bits.qempty));

       /*
        * Either the fill and empty pointers are different or
        * queue is one of empty or full.
        */
       queue_assert((qval.q_atomic_bits.q_fill_index !=
                     qval.q_atomic_bits.q_empty_index)
                    || qval.q_atomic_bits.qfull | qval.q_atomic_bits.qempty);

        if (qval.q_atomic_bits.qfull) {
            /* See #sdf_queue post_spin comment */
            FTH_SPIN_UNLOCK(&queue->post_spin);
            return (QUEUE_FULL);
        }

        queue->sdf_queue[qval.q_atomic_bits.q_fill_index] = q_item;
        qval.q_atomic_bits.qempty = B_FALSE;
        if (++qval.q_atomic_bits.q_fill_index == SDF_QUEUE_SIZE)
            qval.q_atomic_bits.q_fill_index = 0;
        if (qval.q_atomic_bits.q_fill_index
               == qval.q_atomic_bits.q_empty_index)
            qval.q_atomic_bits.qfull = B_TRUE;
    } while (! __sync_bool_compare_and_swap(
                &queue->q_atomic_val.q_atomic_int,
                old_qval.q_atomic_int,
                qval.q_atomic_int));

    /* See #sdf_queue post_spin comment */

    FTH_SPIN_UNLOCK(&queue->post_spin);

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: queue %p full %d empty %d fill_index %d empty index %d wtype %d\n",
                 MeRank, queue, qval.q_atomic_bits.qfull,
                 qval.q_atomic_bits.qempty, qval.q_atomic_bits.q_fill_index,
                 qval.q_atomic_bits.q_empty_index, queue->q_wait_type);

    switch (queue->q_wait_type) {
    case SDF_WAIT_SEMAPHORE:
        /* On error notify */
        ret = sem_post(&queue->q_wait_obj.q_semaphore);
	if (ret) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "\nNode %d: semaphore unlock error - post to queue %p failed -- full %d empty %d\n",
                         MeRank, queue, qval.q_atomic_bits.qfull, qval.q_atomic_bits.qempty);
            return (QUEUE_PTH_WAIT_ERR);
	}
        break;
    case SDF_WAIT_CONDVAR:
        /* On error notify */
        ret = pthread_mutex_lock(&queue->q_wait_obj.q_condvar.q_mutex);
        if (queue->q_wait_obj.q_condvar.q_waiter)
            pthread_cond_signal(&queue->q_wait_obj.q_condvar.q_cv);
        ret = pthread_mutex_unlock(&queue->q_wait_obj.q_condvar.q_mutex);
	if (ret) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "\nNode %d: mutex unlock error - post to queue %p failed -- full %d empty %d\n",
                         MeRank, queue, qval.q_atomic_bits.qfull, qval.q_atomic_bits.qempty);
            return (QUEUE_PTH_WAIT_ERR);
	}

        break;
    case SDF_WAIT_FTH:
        {
            fthThread_t *thr = NULL;

            thr = fthThreadQ_shift(&queue->q_wait_obj.q_fth_waiters);

            if (thr != NULL) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: fthResume queue %p thr %p\n",
	                     MeRank, queue, thr);
                fthResume(thr, 0);
            }
        }
        break;
    default:
        plat_abort();
        break;
    }

    return (QUEUE_SUCCESS);
}

/**
 * @brief sdf_fetch() the specific queue is checked for the outgoing message, it will
 * grab the queue_item from the queue and returns it
 */
sdf_queue_item_t
sdf_fetch(struct sdf_queue *queue, boolean_t wait)
{
    union q_atomic old_qval, qval;
    struct sdf_queue_item *q_item;
    int ret = 0;

    do {
        old_qval = qval = queue->q_atomic_val;

	if (DBGP) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                   "\nNode %d: %s: wait %d queueval 0x%x in %d out %d empty %d full %d \n", MeRank, __func__,
                   wait, qval.q_atomic_int, qval.q_atomic_bits.q_fill_index, qval.q_atomic_bits.q_empty_index,
                   qval.q_atomic_bits.qempty, qval.q_atomic_bits.qfull );
	}
           /* Cannot be both full and empty at the same time */
        queue_assert(! (qval.q_atomic_bits.qfull && qval.q_atomic_bits.qempty));
           /* Either the fill and empty pointers are different or queue is empty or full */
        queue_assert((qval.q_atomic_bits.q_fill_index != qval.q_atomic_bits.q_empty_index)
                         || qval.q_atomic_bits.qfull || qval.q_atomic_bits.qempty);

        if (qval.q_atomic_bits.qempty) {
            switch (queue->q_wait_type) {
        /* XXX add error handling */
                case SDF_WAIT_SEMAPHORE:
                    if (wait) {
                        ret = sem_wait(&queue->q_wait_obj.q_semaphore);
                    }
                    else
                        ret = sem_trywait(&queue->q_wait_obj.q_semaphore);
                    break;
                case SDF_WAIT_CONDVAR:
                    if (wait) {
                        ret = pthread_mutex_lock(&queue->q_wait_obj.q_condvar.q_mutex);
                        queue->q_wait_obj.q_condvar.q_waiter = B_TRUE;
                        ret = pthread_cond_wait(
                                        &queue->q_wait_obj.q_condvar.q_cv,
                                        &queue->q_wait_obj.q_condvar.q_mutex);
                        queue->q_wait_obj.q_condvar.q_waiter = B_FALSE;
                        ret = pthread_mutex_unlock(
                                &queue->q_wait_obj.q_condvar.q_mutex);
                    }
                    else {
                        return (NULL);
                    }
                    break;
                case SDF_WAIT_FTH:
                    if (wait) {
                        while (queue->q_atomic_val.q_atomic_bits.qempty) {
                            fthThread_t *self;

                            self = fthSelf();
                            FTH_SPIN_LOCK(&self->spin);
                            fthThreadQ_push(&queue->q_wait_obj.q_fth_waiters,
                                            self);
                            FTH_SPIN_UNLOCK(&self->spin);
                            (void) fthWait();
                        }
                    }
                    else
                        return (NULL);
                    break;
                default:
                    plat_abort();
                    break;
                }

        }
        if (ret < 0)
            return (NULL);

        q_item = queue->sdf_queue[qval.q_atomic_bits.q_empty_index];
        qval.q_atomic_bits.qfull = B_FALSE;
        if (++qval.q_atomic_bits.q_empty_index == SDF_QUEUE_SIZE)
            qval.q_atomic_bits.q_empty_index = 0;
        if (qval.q_atomic_bits.q_empty_index ==
                qval.q_atomic_bits.q_fill_index)
            qval.q_atomic_bits.qempty = B_TRUE;
    } while (! __sync_bool_compare_and_swap(
            &queue->q_atomic_val.q_atomic_int,
            old_qval.q_atomic_int,
            qval.q_atomic_int));

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: q_item %p q_msg %p\n", MeRank, q_item, q_item->q_msg);

        if (q_item->q_msg) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: vers %d clustid %d sn %d src_srv %d dn %d dest_srv %d convers %lu msg_type %d\n",
                         MeRank,
                         q_item->q_msg->msg_version,
                         q_item->q_msg->msg_clusterid,
                         q_item->q_msg->msg_src_vnode,
                         q_item->q_msg->msg_src_service,
                         q_item->q_msg->msg_dest_vnode,
                         q_item->q_msg->msg_dest_service,
                         q_item->q_msg->msg_conversation,
                         q_item->q_msg->msg_type);
        }

    return (q_item);
}

/* Returns true if the queue is empty, else false */
boolean_t
sdf_anything_to_fetch(struct sdf_queue *queue)
{
    return (queue->q_atomic_val.q_atomic_bits.qempty);
}

/* XXX - protect below by a mutex */
static void *queue_pair_root = NULL;

int
sdf_compare_queue_pairs(const void *q1, const void *q2)
{
    struct sdf_queue_pair *queue1 = (struct sdf_queue_pair *) q1;
    struct sdf_queue_pair *queue2 = (struct sdf_queue_pair *) q2;

    if (queue1->dest_vnode < queue2->dest_vnode)
        return (-1);
    if (queue1->dest_vnode > queue2->dest_vnode)
        return (1);
    if (queue1->dest_service < queue2->dest_service)
        return (-1);
    if (queue1->dest_service > queue2->dest_service)
        return (1);
    if (queue1->src_vnode < queue2->src_vnode)
        return (-1);
    if (queue1->src_vnode > queue2->src_vnode)
        return (1);
    if (queue1->src_service < queue2->src_service)
        return (-1);
    if (queue1->src_service > queue2->src_service)
        return (1);
    return (0);
}

/*
 * Given a destinate vnode and a destination service, find a queue pair
 * that matches it.  Returns NULL if not present.
 */
static struct sdf_queue_pair *
sdf_find_queue_pair_exact(
    vnode_t dest_vnode,
    service_t dest_service,
    vnode_t src_vnode,
    service_t src_service)
{
    void *ret;
    struct sdf_queue_pair q_pair;

    q_pair.dest_vnode = dest_vnode;
    q_pair.dest_service = dest_service;
    q_pair.src_vnode = src_vnode;
    q_pair.src_service = src_service;

    ret = tfind(&q_pair, &queue_pair_root, sdf_compare_queue_pairs);
    return ((ret != NULL) ? * (struct sdf_queue_pair **) ret : NULL);
}


/*
 * Find the matching queue pair when passed the nodes and service aka
 * protocols. Returns NULL if not present.
 */
static struct sdf_queue_pair *
sdf_find_queue_pair(vnode_t dest_vnode, service_t dest_service,
                    vnode_t src_vnode, service_t src_service)
{
    struct sdf_queue_pair *ret;

    ret = sdf_find_queue_pair_exact(dest_vnode, dest_service,
                                    src_vnode, src_service);
    if (!ret) {
        ret = sdf_find_queue_pair_exact(VNODE_ANY, SERVICE_ANY,
                                        src_vnode, src_service);
    }
    if (!ret) {
        ret = sdf_find_queue_pair_exact(dest_vnode, dest_service,
                                        VNODE_ANY, SERVICE_ANY);
    }
    return ret;
}


/*
 * Add a queue pair to our list.
 */
int
sdf_insert_queue_pair(struct sdf_queue_pair *q_pair)
{
    void *ret;

    /* XXX - Must protect queue_pair_root with mutex */
    ret = tfind((void *)q_pair, &queue_pair_root, sdf_compare_queue_pairs);
    if (ret)
        return SDF_MSG_FAILURE;
    ret = tsearch((void *)q_pair, &queue_pair_root, sdf_compare_queue_pairs);
    return ret ? SDF_MSG_SUCCESS : SDF_MSG_FAILURE;
}


/*
 * Remove a queue pair.
 */
int
sdf_remove_queue_pair(struct sdf_queue_pair *q_pair)
{
    void *ret;

    logt("deleting queue pair sn=%d dn=%d ss=%d ds=%d",
        q_pair->src_vnode, q_pair->dest_vnode,
        q_pair->src_service, q_pair->dest_service);
    ret = tdelete((void *)q_pair, &queue_pair_root, sdf_compare_queue_pairs);
    return ret ? SDF_MSG_SUCCESS : SDF_MSG_FAILURE;
}


struct sdf_queue *
sdf_create_queue(
    void * (*alloc_fn)(int64_t, q_control_t *),
    q_control_t *qc,
    enum sdf_queue_wait_type wait_type,
    char *queue_name)
{
    struct sdf_queue *queue;
    int ret;

    /* FIXME do we still need these */
#if 0
    pthread_mutexattr_t mattr;
    pthread_condattr_t cattr;
#endif
    /* Allocate shared memory for each queue */
    queue = (*alloc_fn)((int64_t)(sizeof(*queue)), qc);
    if (queue == NULL)
        return NULL;

    queue->q_wait_type = wait_type;

    switch (wait_type) {
    case SDF_WAIT_SEMAPHORE:
        /* XXX - On error do what? */
        ret = sem_init(&queue->q_wait_obj.q_semaphore, 1, 0);
        queue->q_wait_obj.q_condvar.q_waiter = B_FALSE;
        break;
    case SDF_WAIT_CONDVAR:
        /* XXX - On error do what? */
        ret = pthread_mutex_init(&queue->q_wait_obj.q_condvar.q_mutex, NULL);
        ret = pthread_cond_init(&queue->q_wait_obj.q_condvar.q_cv, NULL);
        queue->q_wait_obj.q_condvar.q_waiter = B_FALSE;
        break;
    case SDF_WAIT_FTH:
        /* FIXME Are we supposed to init the waiters here ? */
        fthThreadQ_lll_init(&queue->q_wait_obj.q_fth_waiters);
        break;
    default:
        plat_abort();
    }

    queue->q_atomic_val.q_atomic_int = 0;
    queue->q_atomic_val.q_atomic_bits.qempty = B_TRUE;
    queue->q_atomic_val.q_atomic_bits.qfull = B_FALSE;
    queue->q_atomic_val.q_atomic_bits.q_fill_index = 0;
    queue->q_atomic_val.q_atomic_bits.q_empty_index = 0;
    queue->post_spin = 0;

    strncpy(queue->queue_name, queue_name, 256);
    queue->queue_name[256] = '\0';

    return (queue);
}


/*
 * Create a connection oriented message.  queue pair src and dest nodes with
 * the associated src and destination service/protocol is required along with
 * the wait type which defines the FTh or Pthread.
 * Returns NULL if the creation fails otherwise you'll get the queue
 * pointer
 */
struct sdf_queue_pair *
sdf_create_queue_pair(
    vnode_t src_vnode,
    vnode_t dest_vnode,
    service_t src_service,
    service_t dest_service,
    enum sdf_queue_wait_type wait_type)
{
    struct sdf_queue_pair *q_pair;
    struct sdf_queue *queue;
    char q_name[257];
    int errlvl = 0;

    /* When creating a queue pair we check to see if the nodes are actually
     * there and active */

    if (sdf_msg_node_check(src_vnode, dest_vnode))
        return NULL;

    q_pair = sdf_find_queue_pair(dest_vnode, dest_service,
                        src_vnode, src_service);
    if (q_pair)
        return q_pair;

    do {
        /* Allocate shared memory for queue pair */
        q_pair = sdf_mem_alloc(sizeof(*q_pair), NULL);
        if (q_pair == NULL) {
            errlvl = 1;
            break;
        }
        /* Create inbound queue */
        sprintf(q_name, "sn%d/ss%d/dn%d/ds%d-in",
                src_vnode, src_service, dest_vnode, dest_service);
        queue = sdf_create_queue(sdf_mem_alloc, NULL, wait_type, q_name);
        if (queue == NULL) {
            errlvl = 2;
            break;
        }
        q_pair->q_in = queue;

        /* Create outbound queue */
        sprintf(q_name, "sn%d/ss%d/dn%d/ds%d-out",
                src_vnode, src_service, dest_vnode, dest_service);
        queue = sdf_create_queue(sdf_mem_alloc, NULL, wait_type, q_name);
        if (queue == NULL) {
            errlvl = 3;
            break;
        }
        q_pair->q_out = queue;

        q_pair->src_vnode = src_vnode;
        q_pair->dest_vnode = dest_vnode;
        q_pair->src_service = src_service;
        q_pair->dest_service = dest_service;

        q_pair->sdf_conversation = 0;
        q_pair->sdf_next_seqnum = 0;
        q_pair->sdf_last_ack = 0;

        /* And now keep track of this new queue pair */
        if (sdf_insert_queue_pair(q_pair) == SDF_MSG_FAILURE) {
            errlvl = 4;
            break;
        }
        logt("created queue pair sn %d dn %d ss %d ds %d wt %d",
            src_vnode, dest_vnode, src_service, dest_service, wait_type);

        /* Let the msg engine know what queues have been created in order to
         * skip checking unused bins */
        if (sdf_msg_register_queue_pair(q_pair)) {
            loge("queue pair conflict sn %d dn %d ss %d ds %d wt %d",
                src_vnode, dest_vnode, src_service, dest_service, wait_type);
	}

        /* Return queue pair */
        return q_pair;
    } while (0);

    loge("create queue pair failed err %d sn %d dn %d ss %d ds %d",
                 errlvl, src_vnode, dest_vnode, src_service, dest_service);

    sdf_delete_queue_pair(q_pair);
    return NULL;
}

void
sdf_delete_queue(struct sdf_queue *queue)
{
    int ret;

    switch (queue->q_wait_type) {
    case SDF_WAIT_SEMAPHORE:
        /* On error do??? */
        ret = sem_destroy(&queue->q_wait_obj.q_semaphore);
        break;
    case SDF_WAIT_CONDVAR:
        ret = pthread_mutex_destroy(&queue->q_wait_obj.q_condvar.q_mutex);
        ret = pthread_cond_destroy(&queue->q_wait_obj.q_condvar.q_cv);
        break;
    case SDF_WAIT_FTH:
        break;
    default:
        plat_abort();
        break;
    }

    sdf_mem_free(queue);
}

void
sdf_delete_queue_pair(struct sdf_queue_pair *q_pair)
{
    sdf_remove_queue_pair(q_pair);
    sdf_mem_free(q_pair->q_in);
    sdf_mem_free(q_pair->q_out);
    sdf_mem_free(q_pair);
}

/**
 * FIXME: Note that without locking there is no guarantee the messages
 * will actually be delivered in sequence number order even when not
 * sent
 */
static int
sdf_msg_init_common(struct sdf_queue_pair **q_pair_out,
                    struct sdf_msg *msg,
                    uint32_t len,
                    vnode_t dest_node,
                    service_t dest_service,
                    vnode_t src_node,
                    service_t src_service,
                    msg_type_t msg_type,
                    sdf_fth_mbx_t *ar_mbx,
                    sdf_resp_mbx_t *mresp_from_req,
                    uint32_t sflags)

{
    struct sdf_queue_pair *q_pair;

    /* First find the matching queue pair in this is not a synthetic call */
    q_pair = sdf_find_queue_pair(dest_node, dest_service, src_node,
                                 src_service);
    /* if this is a init synth msg bypass the queue check */
    if (sflags) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: SEND with no queue FTH Mbox only -- version %d\n", MeRank, msg->msg_version);
        msg->msg_seqnum = 0;
        msg->msg_in_order_ack = 0;
    }
    else {
        if (q_pair == NULL) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                         "\nNode %d: ERROR - No q_pair for msg %p  sn %d dn %d ss %d (%s) ds %d (%s) type %d (%s)\n", MeRank,
                         msg, src_node, dest_node,
                         src_service, SDF_msg_protocol_to_string(src_service),
                         dest_service, SDF_msg_protocol_to_string(dest_service),
                         msg_type, SDF_msg_type_to_string(msg_type));
            return (QUEUE_NOQUEUE);
        }
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: msg_%p request found q_pair %p\n", MeRank, msg, q_pair);
        msg->msg_seqnum = __sync_fetch_and_add(&q_pair->sdf_next_seqnum, 1);
        msg->msg_in_order_ack = q_pair->sdf_last_ack;
        *q_pair_out = q_pair;
    }

    msg->msg_version = SDF_MSG_VERSION;
    msg->msg_clusterid = SDF_MSG_CLUSTERID;
#if 0
    msg->msg_len = sizeof(struct sdf_msg) + len;
#else
    msg->msg_len = ((char *) msg->msg_payload - (char *) msg) + len;
#endif
    msg->msg_src_service = src_service;
    msg->msg_dest_service = dest_service;
    msg->msg_src_vnode = src_node;
    msg->msg_dest_vnode = dest_node;
    msg->msg_type = msg_type;
    msg->buff_seq = 0;
    msg->msg_conversation = 0;
    msg->msg_out_of_order_acks = 0;
    msg->msg_out_of_order_nacks = 0;
    msg->msg_timestamp = get_the_nstimestamp();
    msg->akrpmbx = ar_mbx;
    msg->fthid = fthId();
        
    if (msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: msg_request ndbin %p flags\n", MeRank, msg->ndbin);
    } else {
        msg->ndbin = NULL;
    }
    msg->msg_q_item = NULL;

    msg->msg_flags &= ~(SDF_MSG_FLAG_MBX_RESP_EXPECTED|
                        SDF_MSG_FLAG_MBX_RESP_INCLUDED|
                        SDF_MSG_FLAG_MBX_SACK_EXPECTED|
                        SDF_MSG_FLAG_MKEY_INT);

    /* helper macros are utilized here to decipher the actlvl */

    if (ar_mbx && ((ar_mbx->actlvl != SACK_MODERN && ar_mbx->abox &&
                    sdf_msg_sack_ack((SDF_msg_SACK)ar_mbx->actlvl)) ||
                   (ar_mbx->actlvl == SACK_MODERN && ar_mbx->aaction))) {
        msg->msg_flags |= SDF_MSG_FLAG_MBX_SACK_EXPECTED;
    }
    if (ar_mbx && ((ar_mbx->actlvl != SACK_MODERN && ar_mbx->rbox &&
                   sdf_msg_sack_resp((SDF_msg_SACK)ar_mbx->actlvl)) ||
        (ar_mbx->actlvl == SACK_MODERN && ar_mbx->raction))) {
        if (allow_posts) {
            msg->msg_conversation = __sync_fetch_and_add(&sdf_msg_rtstate->respcntr, 1);
        } else {
            msg->msg_conversation = 0;
        }
        msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_EXPECTED;
    }

    if (!ar_mbx || ar_mbx->release_on_send)
        msg->msg_flags |= SDF_MSG_FLAG_FREE_ON_SEND;
    /* if this is a valid response message, grab response mbx pointer and mkey
     * from mresp */
    if (mresp_from_req) {
        msg->akrpmbx_from_req = mresp_from_req->rbox;
        msg->msg_flags |= SDF_MSG_FLAG_MBX_RESP_INCLUDED;
        /* stuff the mkey field with something for now */
        msg->mkey[MSG_KEYSZE - 1] = '\0';
        if (q_pair_out) {
            strcpy(msg->mkey, mresp_from_req->mkey);
        } else { /* synthetic gets loaded with default */
            strncpy(msg->mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        }
    } else {
        msg->akrpmbx_from_req = NULL;
        /* stuff the mkey field with something so we have valid log prints
         * later */
        strncpy(msg->mkey, MSG_DFLT_KEY, (MSG_KEYSZE - 1));
        msg->mkey[MSG_KEYSZE - 1] = '\0';
    }

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: fth %p msg %p dn %d ds %d sn %d ss %d typ %d len %d\n"
                 "        flgs 0x%x seqnum %lu ar_mbx %p akrpmbx_from_req %p mkey %s mresp %p \n",
                 MeRank, fthId(), msg, dest_node, dest_service, src_node, src_service, 
                 msg_type, msg->msg_len, msg->msg_flags,
                 msg->msg_seqnum, ar_mbx, msg->akrpmbx_from_req, msg->mkey, mresp_from_req);


    return (SDF_MSG_SUCCESS);
}

    /* @brief sdf_msg_init_synthetic() quick way to fill in the header of a
     * virgin message and sets the flags with the appropriate info when posting
     * directly to an fth mbox for a send. Used in conjuction with fastpath
     * sends in action and home thread communication.  @returns 0 upon success
     * or else check enum sdf_queue_status for return error types
     */


int
sdf_msg_init_synthetic(struct sdf_msg *msg,
                       uint32_t len,
                       vnode_t dest_node,
                       service_t dest_service,
                       vnode_t src_node,
                       service_t src_service,
                       msg_type_t msg_type,
                       sdf_fth_mbx_t *ar_mbx,
                       sdf_resp_mbx_t *mresp) {

    msg->msg_version = SDF_MSG_SYNTHETIC;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: fth %p msg %p dn %d ds %d sn %d ss %d typ %d len %d\n"
                 "        ar_mbx %p mresp %p\n",
                 MeRank, fthId(), msg, dest_node, dest_service, src_node, src_service, 
                 msg_type, len, ar_mbx, mresp);

    return (sdf_msg_init_common(NULL, msg, len, dest_node, dest_service,
                                src_node, src_service, msg_type,
                                ar_mbx, mresp, SDF_MSG_SYNTH_INIT));
}

/*
 * Used to send a payload ready sdf request message from either a pthread or an
 * FTH thread to the sdf messaging pthread.
 * Returns SDF_MSG_SUCCESS or SDF_MSG_FAILURE.
 */
int
sdf_msg_send(
    struct sdf_msg *msg,
    uint32_t len,
    vnode_t dnode,
    service_t dserv,
    vnode_t snode,
    service_t sserv,
    msg_type_t msg_type,
    sdf_fth_mbx_t *ar_mbx,
    sdf_resp_mbx_t *ar_mbx_from_req)
{
    int status;
    struct sdf_queue_item *q_item = NULL;
    struct sdf_queue_pair *q_pair = NULL;

    logt("sdf_msg_send msg=%p sn=%d dn=%d ss=%d ds=%d len=%d sfm=%p resp=%p",
        msg, snode, dnode, sserv, dserv, len,
        ar_mbx, ar_mbx_from_req);

    if (!allow_posts)
        fatal("messaging not enabled");
    if (snode != MeRank)
        fatal("sdf_msg_send: snode is %d, should be %d", snode, MeRank);

    /* NOTE: sender should not do an init_synthetic if doing an sdf_msg_send()
     * it will conflict */
    status = sdf_msg_init_common(&q_pair, msg, len, dnode, dserv,
                snode, sserv, msg_type, ar_mbx, ar_mbx_from_req, 0);
    if (status != SDF_MSG_SUCCESS)
        return status;
    if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED)
        if (!check_mkey(msg))
            fatal("sdf_msg_send: bad response parameter");

    /* msg fastpath - same node messages will bypass the queue posting and
     * subsequent messaging thread handling unless directed not to at compile
     * time. Any sdf_msg_send will call into the same thread safe mechanism
     * that the message thread does. Since the delivered buffer is the same the
     * sent buffer we do not conduct a local free, as the message thread would
     * with a free on send flag, the receiver needs to free it directly as with
     * any delivered buffer */

#ifndef MSG_DISABLE_FP
    if (msg->msg_src_vnode == msg->msg_dest_vnode) {
        int ret;
        int l = ar_mbx ? ar_mbx->actlvl : 0;

        msg->msg_sendstamp =
            __sync_fetch_and_add(&sdf_msg_rtstate->sendstamp, 1);
        if (msg->msg_flags & SDF_MSG_FLAG_FREE_ON_SEND)
            msg->msg_flags ^= SDF_MSG_FLAG_FREE_ON_SEND;
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
            "\nNode %d: FASTPATH Delivery of msg %p to dn %d"
            " flags 0x%x sendstamp %lu\n",
            msg->msg_src_vnode, msg,  msg->msg_dest_vnode,
            msg->msg_flags, msg->msg_sendstamp);
        ret =  sdf_do_receive_msg(msg);
        if (l == SACK_ONLY_FTH || l == SACK_BOTH_FTH)
            fthMboxPost(ar_mbx->abox, get_the_nstimestamp());
        return ret;
    }
#endif

    q_item = sdf_queue_item_alloc(sizeof(*q_item));
    /* have to track q_item to dealloc on msg buffer release */
    msg->msg_q_item = q_item;
    q_item->q_msg = msg;

    /* Finally post the message */
    status = sdf_post(q_pair->q_in, q_item);

    if (status) {
        if (q_item)
            sdf_mem_free(q_item);
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
            "Node %d: QUEUE POST FAILED status %d dn %d ds %d sn %d ss %d"
            " type %d seq %lu time %lu msg %p\n",
            MeRank, status, dnode, dserv, snode, sserv,
            msg_type, msg->msg_seqnum, msg->msg_timestamp, msg);
    }
    return (status);
}


/** @brief synchronous request/reply call for message based RPC - FTH only */

struct sdf_msg *
sdf_msg_send_receive(
    struct sdf_msg *msg,
    uint32_t len,
    vnode_t dest_node,
    service_t dest_service,
    vnode_t src_node,
    service_t src_service,
    msg_type_t msg_type,
    int rel)
{
    struct sdf_msg *ret = NULL;
    struct sdf_msg *send_msg = msg;
    int failed;
    struct sdf_fth_mbx *ar_mbx;
    fthMbox_t resp_mbox;

    plat_assert(fthSelf());
    /** For the error case */
    plat_assert(rel == SACK_REL_YES);

    send_msg = msg;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: MSG SEND/RECV sending dn %d ds %d sn %d ss %d type %d len %d\n",
                 MeRank, dest_node, dest_service, src_node, src_service, msg_type, msg->msg_len);

    fthMboxInit(&resp_mbox);
    ar_mbx = sdf_fth_mbx_resp_mbox_alloc(&resp_mbox, rel, 
                                         SDF_FTH_MBX_TIMEOUT_NONE);

    failed = !ar_mbx;
    if (!failed) {
        /* let's flag this guy for debugging */
        msg->msg_version = SDF_MSG_SRVERSION;
        msg->msg_flags |= SDF_MSG_FLAG_ALLOC_BUFF;
        failed = sdf_msg_send(msg, len, dest_node, dest_service,
                              src_node, src_service, msg_type, ar_mbx, NULL);
        send_msg = NULL;
    }

    if (!failed) {
        ret = (struct sdf_msg *)fthMboxWait(&resp_mbox);
        plat_assert(ret);
    }

    if (failed && send_msg && rel == SACK_REL_YES) {
//        sdf_msg_free(send_msg);
    }

    if (ar_mbx) {
        sdf_fth_mbx_free(ar_mbx);
    }

    return (ret);
}

/*
 * @brief sdf_msg_receive() Used to pull a message from a send queue via q_item
 * by the sdf message pthread by calling sdf_fetch(). The the final step is 
 * transmitting the msg buffer with the MPI_Send.
 */

struct sdf_msg *
sdf_msg_receive(
    struct sdf_queue *queue,
    uint32_t flags,
    boolean_t wait)
{
    struct sdf_queue_item *q_item;
    q_item = sdf_fetch(queue, wait);
    return (q_item != NULL ? q_item->q_msg : NULL);
}

/*
 * check the desired queue and send the total number of pending messages there
 * this will repeat until there are no others to send. The final act of send via MPI
 * is done with send_outgoing(), same node messages are delivered there also
 *
 * This is only called from within the MPI messaging thread.
 */


static void
sdf_read_queue_and_forward(struct sdf_queue *queue)
{
    struct sdf_msg *msg;

    do {
        /* MOVEOFFMPI - bypass sends as we're waiting for big msg completion */
	if (sdf_msg_rtstate->sdf_msg_runstat == 1) {
            break;
	}
        msg = sdf_msg_receive(queue, 0, B_FALSE);
        if (msg != NULL) {
            /* bypass the hashing of respmbx when sending msgs on the same node */
            if ((msg->msg_dest_vnode != msg->msg_src_vnode) && (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_EXPECTED)) {
                SDFMSGMapEntry_t *tst = sdf_msg_resptag(msg); /* hash and decouple the resp mbox pointer */
                msg->mkey[MSG_KEYSZE - 1] = '\0';
                strcpy(msg->mkey, tst->contents->mkey); /* save the mkey into the msg hdr */
                /* DEBUG only inc when sending a msg that expects a response, dec when that comes in */
                sdf_msg_rtstate->resp_n_flight++;
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Sending ReqResp msg %p to dn %d w/HASH mkey %s har_mbx %p\n"
                "        hrbox %p fth rbox %p rplmbx %p respnflght %lu seqnum %lu\n", 
                             MeRank, msg, msg->msg_dest_vnode, tst->contents->mkey,
                             tst->contents->ar_mbx,  tst->contents->respmbx,
                             msg->akrpmbx->rbox, tst->contents->ar_mbx_from_req, sdf_msg_rtstate->resp_n_flight,
                             tst->contents->msg_seqnum);
            } else if ((msg->msg_dest_vnode != msg->msg_src_vnode) && (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED)) {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Sending RESP Msg %p to dn %d srcsvc %d destsvr %d sn %d type %d\n"
                "        seq %d flags 0x%x len %d conver %ld tmstmp %ld ar_mbx %p\n"
                "        akrpmbx_from_req %p ndbin %p sendstmp %ld mkey %s\n",
                             MeRank, msg, msg->msg_dest_vnode, msg->msg_src_service, msg->msg_dest_service, 
                             msg->msg_src_vnode, msg->msg_type, msg->buff_seq, msg->msg_flags, msg->msg_len,
                             msg->msg_conversation, msg->msg_timestamp, msg->akrpmbx, msg->akrpmbx_from_req,
                             msg->ndbin, msg->msg_sendstamp, msg->mkey);
            } else {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                "\nNode %d: Sending Msg %p to dn %d srcsvc %d destsvr %d sn %d type %d\n"
                "        seq %d flags 0x%x len %d conver %ld tmstmp %ld ar_mbx %p\n"
                "        akrpmbx_from_req %p ndbin %p sendstmp %ld mkey %s\n",
                             MeRank, msg, msg->msg_dest_vnode, msg->msg_src_service, msg->msg_dest_service, 
                             msg->msg_src_vnode, msg->msg_type, msg->buff_seq, msg->msg_flags, msg->msg_len,
                             msg->msg_conversation, msg->msg_timestamp, msg->akrpmbx, msg->akrpmbx_from_req,
                             msg->ndbin, msg->msg_sendstamp, msg->mkey);
            }
            sdf_msg_send_outgoing(msg);
            num_msgs_sent++;
        }
    } while (msg != NULL);
}

/*
 * Check this queue and send any messages on the network.
 */
/* ARGSUSED2 */

static void
sdf_fetch_queues_and_forward(const void *q, const VISIT which, const int depth)
{
    struct sdf_queue_pair *q_pair;

    switch (which) {
    case postorder:
    case leaf:
        q_pair = * (struct sdf_queue_pair **) q;
        sdf_read_queue_and_forward(q_pair->q_in);
        break;
    case preorder:
    case endorder:
        break;

#if defined(DEBUG)
    default:
        plat_abort();
#endif
    }
}


/*
 * Walk through all the queue pairs and fetch any messages waiting to be sent.
 */
static void
sdf_check_queue_pairs(void)
{
    twalk(queue_pair_root, sdf_fetch_queues_and_forward);
}

static void
sdf_check_network_connections(void)
{
    return;
}

/*
 * Main message transport thread.  Reads from queues and
 * puts the messages on the network as needed.  Also gets
 * messages from the network and puts them on queues.
 */
int
sdf_msg_transport(void)
{
    num_msgs_sent = 0;

    sdf_check_queue_pairs();
    sdf_check_network_connections();
    return (num_msgs_sent);
}

sdf_endpoint_t
sdf_connect(
    vnode_t src_vnode,
    vnode_t dest_vnode,
    service_t src_service,
    service_t dest_service)
{
    /* Allocate shared memory */
    /* Send shared memory keys through named pipe */
     return(0);

}

int
sdf_disconnect(sdf_endpoint_t ep)
{
  return(0);
}

/* XXX - protect below by a mutex */
static void *sdf_service_root = NULL;

struct sdf_service_register_entry {
    service_t service;
    void * (*funcp)(struct sdf_msg *, void *);
    void *arg;
};

int
sdf_compare_service(const void *v1, const void *v2)
{
    struct sdf_service_register_entry *s1;
    struct sdf_service_register_entry *s2;

    s1 = (struct sdf_service_register_entry *)v1;
    s2 = (struct sdf_service_register_entry *)v2;

    if (s1->service < s2->service)
        return (-1);
    if (s1->service > s2->service)
        return (1);
    return (0);
}

/*
 * API for registering what function should be invoked when
 * an sdf message is received by the sdf messagine engine.
 */
int
sdf_register_service(
    service_t service,
    void * (*funcp)(struct sdf_msg *, void *),
    void *arg)
{
    /* Insert this service into the tree */
    void *ret;
    struct sdf_service_register_entry *entry;

printf("%s: registering service %d function %p arg %p\n", __func__, service, funcp, arg);
fflush(stdout);

    entry = sdf_mem_alloc(sizeof(*entry), NULL);
    plat_assert(entry);
    entry->service = service;
    entry->funcp = funcp;
    entry->arg = arg;
    /* XXX - Must protect sdf_service_root with mutex */
    /* XXX - worry about duplicate entries for now? */
    ret = tsearch((void *)entry, &sdf_service_root, sdf_compare_service);
    return ((ret != NULL) ? SDF_MSG_SUCCESS : SDF_MSG_FAILURE);
}

/* Inverse of function above */
int
sdf_unregister_service(service_t service)
{
    void *ret;
    struct sdf_service_register_entry entry;

    entry.service = service;
    ret = tdelete((void *)&entry, &sdf_service_root, sdf_compare_service);
    if (ret != NULL)
        sdf_mem_free(ret);
    return ((ret != NULL) ? SDF_MSG_SUCCESS : SDF_MSG_FAILURE);
}

#if XXX_UNUSED
static struct sdf_service_register_entry *
sdf_find_service(service_t service)
{
    void *ret;
    struct sdf_service_register_entry entry;

    entry.service = service;

printf("%s: trying to find service %d\n", __func__, service);
fflush(stdout);
    ret = tfind(&entry, &sdf_service_root, sdf_compare_service);
printf("%s: found service %p\n", __func__, ret);
fflush(stdout);

    return (ret != NULL ? * (struct sdf_service_register_entry **) ret : NULL);
}
#endif


/** 
 * @brief  sdf_do_receive_msg() delivery of a received message from a bin check or msg error
 * 
 * same node has fastpath message delivery, essentially posts to a designated queue or fthMbox
 *  
 * @param sdf_msg <IN> basic msg structure, whether normal or timeout generated
 */
int
sdf_do_receive_msg(struct sdf_msg *msg)
{
    struct sdf_queue_pair *q_pair;
    int s;
    int ret = 0;
    struct sdf_queue_item *q_item;
    seqnum_t in_order_acks;
    seqnum_t out_of_order_acks;

    /* XXX - do sequence number/acks */
    in_order_acks = msg->msg_in_order_ack;
    out_of_order_acks = msg->msg_out_of_order_acks;
    msg->msg_q_item = NULL;         /* have to NULL the q_item in case it came back to us in the header */


    /* check message flags to see if this is a response message */
    SDFMSGMapEntry_t *hresp = NULL;
    if (msg->msg_flags & SDF_MSG_FLAG_MBX_RESP_INCLUDED) {
        /* this is seen as a response so compare mbox from the hash table if we're not on the same node */
        if (msg->msg_src_vnode != msg->msg_dest_vnode) {
            msg->mkey[MSG_KEYSZE - 1] = '\0';
            char *crp = msg->mkey;
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: num entries %d msg %p mkey = %s\n",
                         MeRank, (SDFMSGMapNEntries(&respmsg_tracker)), msg, msg->mkey);
            hresp = SDFMSGMapGet(&respmsg_tracker, crp); /* got one from the outside */
            if (hresp != NULL) { /* so far so good the key is cool */
                if (sdf_msg_hashchk(hresp, msg)) { /* now load the msg from the hashed struct stuff in the respmbox */
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                 "\nNode %d: ERROR Response msg %p has failed hashcheck mkey = %s Aborting\n",
                                 MeRank, msg, msg->mkey);
                    plat_assert((hresp->contents->respmbx == msg->akrpmbx_from_req->rbox));
                }
            } else {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "\nNode %d: ERROR Resp msg %p has no valid mkey = %s dn %d sn %d hresp %p\n"
                             "\n     This could be a late returning msg whose had an error posted already\n",
                             MeRank, msg, msg->mkey, msg->msg_dest_vnode, msg->msg_src_vnode, hresp);
                plat_assert(hresp != NULL);
            }
            if (sdf_msg_rtstate->resp_n_flight) {
                sdf_msg_rtstate->resp_n_flight--; /* ok to dec the ref counter */
            } else {
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                             "\nNode %d: Delivered msg %p with ret %d reqresp counter %lu\n", MeRank, msg,
                             ret, sdf_msg_rtstate->resp_n_flight);
            }
            if (hresp != NULL) {
                if (!(SDFMSGMapNEntries(&respmsg_tracker))) { /* there are still resp msgs in flight */
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                 "\nNode %d: Msg %p ERROR hash numentries is zero mkey = %s\n",
                                 MeRank, msg, msg->mkey);
                    /* FIXME may need to add in ordering for the oldest resp msg outstanding */
                } else {
                    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                                 "\nNode %d: Trying Hash Map Delete msg %p with ret %d mkey %s\n",
                                 MeRank, msg, ret, msg->mkey);
                    ret = SDFMSGMapDelete(&respmsg_tracker, msg->mkey);
                    if (ret) {
                        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                                     "\nNode %d: Failed Hash Map Delete msg %p with ret %d mkey %s\n",
                                     MeRank, msg, ret, hresp->contents->mkey);
                    }
                }
                plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                             "\nNode %d: Delivered Response msg %p with ret %d hcnt %d reqresp counter %lu\n",
                             MeRank, msg, ret, SDFMSGMapNEntries(&respmsg_tracker), sdf_msg_rtstate->resp_n_flight);
            }
        }

        plat_assert(msg->akrpmbx_from_req);
        plat_assert(msg->akrpmbx_from_req->actlvl > 0);
        plat_assert(msg->akrpmbx_from_req->actlvl <= SACK_MODERN);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Response msg %p sn %d ss %d dn %d ds %d type %d flgs 0x%x\n"
                     "        actlvl %d akrpmbx_from_req %p respbox %p mkey %s sndstmp %lu respnum %lu\n",
                     MeRank, msg, msg->msg_src_vnode, msg->msg_src_service, msg->msg_dest_vnode,
                     msg->msg_dest_service, msg->msg_type, msg->msg_flags,
                     msg->akrpmbx_from_req->actlvl, msg->akrpmbx_from_req,
                     (msg->akrpmbx_from_req == NULL ? NULL : msg->akrpmbx_from_req->rbox),
                     msg->mkey, msg->msg_sendstamp, msg->msg_conversation);


        /* whether its a same node or external response returned -- post it */
        ret = sdf_fth_mbx_deliver_resp(msg->akrpmbx_from_req, msg);

        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Out of Post of resp mbox %p msg %p ret %d\n", MeRank, 
                     msg->akrpmbx_from_req, msg, ret);

        return (ret);
    }

    /*
     * FIXME: We may want to look for wild card matches on the node
     * and/or service.
     */
    s = sdf_msg_binding_match(msg);
    if (s)
        return (s > 0) ? SDF_MSG_SUCCESS : SDF_MSG_FAILURE;

    plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                 "\nNode %d: UNBOUND msg %p Now looking for a Queue sn %d dn %d ss %d ds %d type %d flgs 0x%x\n",
                 MeRank, msg, msg->msg_src_vnode, msg->msg_dest_vnode, msg->msg_src_service,
                 msg->msg_dest_service, msg->msg_type, msg->msg_flags);

    q_pair = sdf_find_queue_pair(msg->msg_src_vnode, msg->msg_src_service,
                        msg->msg_dest_vnode, msg->msg_dest_service);
    if (!q_pair) {
        q_pair = sdf_create_queue_pair(msg->msg_dest_vnode,
                                       msg->msg_src_vnode,
                                       msg->msg_dest_service,
                                       msg->msg_src_service,
                                       SDF_WAIT_FTH);
    }

    if (q_pair != NULL) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Posted - msg %p to queue %p sn %d to dn %d ss %d ds %d type %d flgs 0x%x\n",
                     MeRank, msg, q_pair->q_out, msg->msg_src_vnode, msg->msg_dest_vnode, msg->msg_src_service,
	             msg->msg_dest_service, msg->msg_type, msg->msg_flags);

            /* We deallocate the q_item along with the msg buffer when it is released */
//            q_item = sdf_mem_alloc(sizeof(*q_item), NULL);
        
            q_item = sdf_queue_item_alloc(sizeof(*q_item));
            msg->msg_q_item = q_item; /* have to track q_item to dealloc on msg buffer release */
            q_item->q_msg = msg;

            ret = sdf_post(q_pair->q_out, q_item);
    } else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nNode %d: ERROR No Queue Found - Unhandled msg %p sn %d ss %d dn %d ds %d type %d flags 0x%x\n",
                     MeRank, msg, msg->msg_src_vnode, msg->msg_src_service, msg->msg_dest_vnode,
	             msg->msg_dest_service, msg->msg_type, msg->msg_flags);

        /* we should never see reserved protocol of SDF_DEBUG */
	plat_assert(msg->msg_dest_service);

        ret = SDF_MSG_FAILURE; /* this is 1 */
    }
    /* FIXME do we still want to do the nack ack dance */
    if (ret == QUEUE_SUCCESS) {
        if (msg->msg_seqnum == q_pair->sdf_last_ack + 1) {
            q_pair->sdf_last_ack++;
        } else if (msg->msg_seqnum > q_pair->sdf_last_ack) {
            q_pair->sdf_last_ack--;   /* out of order acks */
        } else {
            q_pair->sdf_last_ack = 0;
        } 
        ret = SDF_MSG_SUCCESS;
    }
    else {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_ERROR,
                     "\nFAILED to DELIVER-POST msg %p sn %d dn %d proto %d type %d\n",
                     msg, msg->msg_src_vnode, msg->msg_dest_vnode,
	             msg->msg_dest_service, msg->msg_type);
        ret = SDF_MSG_FAILURE;
        /* negative acks */
    }

    return (ret);
}

int
sdf_msg_sbuff_ack(service_t dest_service, struct sdf_msg *msg, sdf_fth_mbx_t *ackmbx) {
    int ret;
    sdf_msg_t *msgptr = msg;
    int nd = msg->msg_src_vnode;
    int flgs = msg->msg_flags;
    struct sdf_msg_bin_init *bb = msg->ndbin;

    if (msg->msg_flags & SDF_MSG_FLAG_MBX_SACK_EXPECTED) {
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                     "\nNode %d: Send Ack is Expected for msg %p msg_flags 0x%x\n", nd, msgptr, flgs);
        ret = sdf_fth_mbx_deliver_ack(ackmbx, msg);
    } else {
        ret = SDF_MSG_SUCCESS;
    }

    if (msg->msg_flags & SDF_MSG_FLAG_FREE_ON_SEND) {
         /* FIXME keep in mind that when we go to isends for all msg sizes we cannot release
          * the message before it is sent, just need to pull them of the LL that is yet to be created
          * but much like large messages do currently */
        if (!(msg->msg_flags & SDF_MSG_FLAG_STATIC_BUFF)) {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: Free on Send msg %p msg_flags 0x%x bin %p\n", nd, msgptr, flgs, bb);
            sdf_msg_free(msg);
        } else {
            plat_log_msg(PLAT_LOG_ID_INITIAL, LOG_CAT, PLAT_LOG_LEVEL_TRACE,
                         "\nNode %d: MPI Static buff %p detected on msg send - msg_flags 0x%x ndbin %p\n",
                         nd, msgptr, flgs, bb);
            sdf_msg_free_buff(msg);
//            plat_assert(0);
        }
        ret = SDF_MSG_SUCCESS;
    }

    return (ret);
}


/*
 * Return 1 to indicate that we do support MPI.
 */
int
sdf_msg_int_mpi(void)
{
    return 1;
}


/*
 * Establish that a new queue or binding was created.
 */
void
sdf_msg_int_new_binding(int service)
{
}


/*
 * Prepare to call a function from the messaging context.  We do not do
 * anything under the old messaging system.
 */
void
sdf_msg_int_fcall(cfunc_t func, void *arg)
{
    //func(arg);
}
