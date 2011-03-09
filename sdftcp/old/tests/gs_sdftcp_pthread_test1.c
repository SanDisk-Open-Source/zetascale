/*
 * File:   sdftcp_pthread_test1.c
 * Author: norman xu
 *
 * Created on September 22, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: $
 */

/*
 * Single test case in sdftcp. It will check the msg sequence.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>     // Import vfprintf(), et al

#include "platform/logging.h"
#define PLAT_OPTS_NAME(name) name ## _pthreadtest1
#include "platform/opts.h"
#include "platform/shmem.h"
#include "platform/fcntl.h"
#include "fth/fth.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
#include "platform/string.h"
#include "platform/errno.h"
#include "platform/stdlib.h"
#include "utils/properties.h"

#include "sdftcp/tcp.h"

static int tnode = 0;      /** @brief total number of nodes in "cluster" */
static int mnode = 0;      /** @brief my node number */
static int nseqsender = 0;
static int nseqreceiver = 0;
static int niteration = 0;
static int nthread = 0;

static const int dtag_base = 1000;
#define MAX_PTHREAD_NUM 12

pthread_t senderPthread[MAX_PTHREAD_NUM];
pthread_t receiverPthread[MAX_PTHREAD_NUM];

PLAT_LOG_CAT_LOCAL(LOCAL_CAT, "sdf/sdftcp/tests");

struct plat_opts_config_pthreadtest1 {
    int npthread;
    int niteration;
};

#define PLAT_OPTS_ITEMS_pthreadtest1() \
    item("npthread", "number of sender and receiver pair",          \
         NPTHREAD, parse_int(&config->npthread, optarg, NULL),  \
         PLAT_OPTS_ARG_REQUIRED)                               \
    item("niteration", "number of iterations",          \
         NITERATION, parse_int(&config->niteration, optarg, NULL),  \
         PLAT_OPTS_ARG_REQUIRED)

#define eprintf(fmt, ...) \
    fprintf(stderr, fmt, ## __VA_ARGS__)

#define debugprintf(fmt, ...) \
        plat_log_msg(PLAT_LOG_ID_INITIAL, LOCAL_CAT, PLAT_LOG_LEVEL_DEBUG, fmt, ## __VA_ARGS__)

#define traceprintf(fmt, ...) \
	plat_log_msg(PLAT_LOG_ID_INITIAL, LOCAL_CAT, PLAT_LOG_LEVEL_TRACE, fmt, ## __VA_ARGS__)
	
#ifdef _lint

void
tcp_panic(TCP *tcp, const char *fmt, ...)
{
    va_list args;

    if (tcp) {
        (void) tcp_exit(tcp);
    }
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

void
panic(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
}

#else

#define tcp_panic(tcp, fmt, ...)                                               \
    ({                                                                         \
        if (tcp) {                                                             \
            tcp_exit(tcp);                                                     \
        }                                                                      \
        fprintf(stderr, fmt, ## __VA_ARGS__);                                  \
        plat_exit(1);                                                          \
    })                                                                         \

#define panic(fmt, ...)                                                        \
    ({                                                                         \
        fprintf(stderr, fmt, ## __VA_ARGS__);                                  \
        plat_exit(1);                                                          \
    })                                                                         \

#endif
static const TAG TEST_STAG = 123;
/*
static TAG TEST_DTAGS[MAX_PTHREAD_NUM];
*/

void * SenderRoutine(void *arg)
{
    int local_seq = __sync_fetch_and_add(&nseqsender, 1);
    
    unsigned long local_dtag = local_seq + dtag_base;
    
    TCP *tcp = (TCP* )arg;
    SMSG smsg;
    char msgbuf1[128];
    size_t msglen;
    int ret;
    int typeno;
    int nno;
    
    for(typeno = 0; typeno < niteration; typeno++)
    {
	traceprintf("Send messages.\n");

	for (nno = 0; nno < tnode; ++nno) {
            int fd;

            if (nno == mnode) {
                continue;
            }
            fd = tcp_send_fd(tcp, nno);
            if (fd < 0) {
                tcp_panic(tcp, "tcp_send_fd failed; %d\n", fd);
            }

            /*
             * Send message with tag==TEST_DTAGS,
             * in order to test message selection and queueing of unselected
             * messages.
             */
            ret = snprintf(msgbuf1, sizeof (msgbuf1),
                "Hello from %d, dtag=%lu, sequence=%d", mnode, local_dtag, typeno);
            
            if (ret < 0 || ret >= sizeof (msgbuf1)) {
                tcp_panic(tcp, "snprintf failed\n");
            } else {
                msglen = ret;
            }
            smsg.stag = TEST_STAG;
            smsg.dtag = local_dtag;
            smsg.drank = nno;
            smsg.sgv[0].iov_base = msgbuf1;
            smsg.sgv[0].iov_len = msglen + 1;
            smsg.sgv[1].iov_base = NULL;
            ret = tcp_send(tcp, &smsg, NULL, 0);
            if (ret < 0) {
                tcp_panic(tcp, "tcp_send failed; %d\n", ret);
            }
        }
    }
    return NULL;
}

void * ReceiverRoutine(void *arg)
{
    int local_seq = __sync_fetch_and_add(&nseqreceiver, 1);
    unsigned long local_dtag = local_seq + dtag_base;
 
    TCP *tcp = (TCP* )arg;
    RMSG rmsg;
    int ret;
    int typeno;
    char msgbuf2[128];
    int nno;
    
    for(typeno = 0; typeno < niteration; typeno++)
    {
	/*
         * Get messages with tag == TEST_DTAGS[typeno].
         */
        traceprintf("Read messages with tag==%lu.\n", local_dtag);

        for (nno = 0; nno < tnode; ++nno) {
            int fd;

            if (nno == mnode) {
                continue;
            }
            fd = tcp_recv_fd(tcp, nno);
            if (fd < 0) {
                tcp_panic(tcp, "tcp_recv_fd failed; %d\n", fd);
            }
            ret = tcp_recv_tag(tcp, &rmsg, local_dtag, -1);
            if (ret < 0) {
                tcp_panic(tcp, "tcp_recv failed; %d\n", ret);
            }
            debugprintf("Received message from node %lu:'%s'\n", rmsg.srank, (char *)rmsg.buf);
            ret = snprintf(msgbuf2, sizeof (msgbuf2),
                "Hello from %d, dtag=%lu, sequence=%d", (int)rmsg.srank, rmsg.dtag, typeno);
            plat_assert_always(strcmp(rmsg.buf, msgbuf2) == 0);
            free(rmsg.buf);
        }
    }
    return NULL;
}


int
main(int argc, char **argv)
{
    struct plat_opts_config_pthreadtest1 config;
    TCP *tcp;
    int ret;
    
    int ncount;
    plat_opts_parse_pthreadtest1(&config, argc, argv);
    
    nthread = config.npthread > MAX_PTHREAD_NUM ? MAX_PTHREAD_NUM : config.npthread;
    niteration = config.niteration > 100000 ? 100000 : config.niteration;
    
    tcp = NULL;
    ret = tcp_init(&tcp, TCP_DEBUG | TCP_EXPLAIN);
    if (ret < 0) {
#ifdef TESTS_OK
    	tcp_panic(tcp, "tcp_init failed; %d\n", ret);
#else 
                printf("TCP TEST is not yet FUNCTIONAL...\n");
                plat_exit(0);
#endif
    }

/*
    for(typeno = 0; typeno < MAX_PTHREAD_NUM; typeno++)
    {
	TEST_DTAGS[typeno] = 1000 + typeno;
    }
*/
    
    tnode = tcp_node_count(tcp);
    mnode = tcp_me(tcp);
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    
    for (ncount = 0; ncount < nthread; ncount ++)
    {
        pthread_create(&senderPthread[ncount], &attr, &SenderRoutine, (void *)tcp);
        pthread_create(&receiverPthread[ncount], &attr, &ReceiverRoutine, (void *)tcp);
        debugprintf("Node %d: Created pthread num %d\n", mnode, ncount);
    }
    
    for (ncount = 0; ncount < nthread; ncount ++)
    {
        pthread_join(senderPthread[ncount], NULL);
        pthread_join(receiverPthread[ncount], NULL);
    }

    (void) tcp_exit(tcp);
    plat_exit(0);
}
#include "platform/opts_c.h"
