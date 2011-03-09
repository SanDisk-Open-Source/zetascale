/*
 * File:   sdftcp_single_test1.c
 * Author: norman xu
 *
 * Created on October 21, 2008
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
#define PLAT_OPTS_NAME(name) name ## _singletest1
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


PLAT_LOG_CAT_LOCAL(LOCAL_CAT, "sdf/sdftcp/tests");

#define PLAT_OPTS_ITEMS_singletest1() 

struct plat_opts_config_singletest1 {
};

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

#define MAX_MSG_TYPE 12
static const TAG TEST_STAG = 123;
static TAG TEST_DTAGS[MAX_MSG_TYPE];

int
main(int argc, char ** argv)
{
    struct plat_opts_config_singletest1 config;
    TCP *tcp;
    SMSG smsg;
    RMSG rmsg;
    char msgbuf1[128];
//    char msgbuf2[128];
    size_t msglen;
    int ret;
    int tnode;      /** @brief total number of nodes in "cluster" */
    int mnode;      /** @brief my node number */
    int nno;
    int typeno;
    
    plat_opts_parse_singletest1(&config, argc, argv);
    
    /*
     * initial TEST_DTAGS
     */
    for(typeno = 0; typeno < MAX_MSG_TYPE; typeno++)
    {
	TEST_DTAGS[typeno] = 1000 + typeno;
    }
    
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

    tnode = tcp_node_count(tcp);
    mnode = tcp_me(tcp);

    unsigned long seq = 1000;
    for(typeno = 0; typeno < MAX_MSG_TYPE; typeno++)
    {
	//debugprintf("Send messages.\n");

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
                "Hello from %d, dtag=%lu", mnode, TEST_DTAGS[typeno]);
            if (ret < 0 || ret >= sizeof (msgbuf1)) {
                tcp_panic(tcp, "snprintf failed\n");
            } else {
                msglen = ret;
            }
            smsg.stag = TEST_STAG;
            smsg.dtag = TEST_DTAGS[typeno];
            smsg.drank = nno;
            smsg.sgv[0].iov_base = msgbuf1;
            smsg.sgv[0].iov_len = msglen + 1;
            smsg.sgv[1].iov_base = NULL;
            ret = tcp_send(tcp, &smsg, NULL, 0);
            if (ret < 0) {
                tcp_panic(tcp, "tcp_send failed; %d\n", ret);
            }
        }

            /*
             * Get messages with tag == TEST_DTAGS[typeno].
             */
        traceprintf("Read messages with tag==%lu.\n", TEST_DTAGS[typeno]);

        for (nno = 0; nno < tnode; ++nno) {
            int fd;

            if (nno == mnode) {
                continue;
            }
            fd = tcp_recv_fd(tcp, nno);
            if (fd < 0) {
                tcp_panic(tcp, "tcp_recv_fd failed; %d\n", fd);
            }
            ret = tcp_recv(tcp, &rmsg, -1);
            if (ret < 0) {
                tcp_panic(tcp, "tcp_recv failed; %d\n", ret);
            }
            
            debugprintf("Received message with dtag %lu; Seq is %lu\n", rmsg.dtag, seq);
            plat_assert_always(seq == rmsg.dtag);
            //debugprintf("Received message from node %lu:'%s'\n", rmsg.srank, (char *)rmsg.buf);
            free(rmsg.buf);
        }
        seq++;
    }

    (void) tcp_exit(tcp);
    plat_exit(0);
}
#include "platform/opts_c.h"
