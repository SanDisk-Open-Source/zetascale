/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   msgtest_im.c
 * Author: Norman Xu, Enson Zheng
 * Copyright (c) 2009, Schooner Information Technology, Inc
 * 
 * Messaging internal interface.
 */

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>
#include "trace.h"
#include "msg_msg.h"
#include "msg_err.h"
#include "msgtest_common.h"


/*
 * Useful definitions.
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define DEFAULT_POLLTIME 1000 * 1000
#define MAX_BUFSIZE	1024

/*
 * Static variables.
 */
static msg_init_t	InitMsg;
static ntime_t 		polltime = 100 * DEFAULT_POLLTIME;
static int          Debug = 0;
static int 		    TimeToQuit = 0;    
	
/*
 * Some print function.
 */
void panic(char *fmt, ...);

/*
 * Internal static functions and variables
 */
static void		usage(void);
static void		parse_args(char **argv);
static void  	drop_alarm(int signo);
static void  	message_poll();
static void  	send_msg_to_nodes(char *buf);


/*
 * Main function, it will simply repeatedly call a info routine
 * which will call select and service everything that is ready.
 */
int
main(int argc, char **argv)
{
    int ret;
    char sendline[MAX_BUFSIZE];
    struct timeval tv;
    fd_set rset;

    /* parse input arguments and initialize initmsg with arguments  */
    parse_args(argv+1);

    /* register signal process function */
    if (SIG_ERR == signal(SIGINT, drop_alarm)) {
        error(SYS|DIE, "Register SIGINT signal function failed");
    }

    /* initiate msg system */
    msg_init(&InitMsg);

    while (!TimeToQuit) {
        FD_ZERO(&rset);
        FD_SET(0, &rset);
        /* now set select waiting time */
        tv.tv_sec = 0;
        tv.tv_usec = 50000;
        /* check whether there is some console input */
        ret = select(1, &rset, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            else
                error(SYS, "Select error");
        } else if (ret > 0) {
            /* there is some user input message */
            if (FD_ISSET(0, &rset)) {
                if (NULL != fgets(sendline, MAX_BUFSIZE, stdin)) {
                    /* get some words from input and send them out */
                    /* remove the newline character */
                    sendline[strlen(sendline) - 1] = '\0';
                    send_msg_to_nodes(sendline);
                } else {
                    error(BUG, "Input error");
                }
            }
        } else {
            /* timeout */
        }
        message_poll();
    }

    /* exit from msg system */
    printf("Exit IM system based on Messaging\n");
    msg_exit();

    return 0;
}


/*
 * Parse arguments.
 */
static void
parse_args(char **argv)
{
	for (;;) {
		char *arg = *argv++;

		if (!arg)
		    break;
		if (streq(arg, "-c") || streq(arg, "--msg_class"))
            InitMsg.class = numarg(arg, *argv++, 0);
		else if (streq(arg, "-d") || streq(arg, "--msg_debug"))
            Debug = numarg(arg, *argv++, 0);
		else if (streq(arg, "-p") || streq(arg, "--port"))
            InitMsg.udpport = numarg(arg, *argv++, 1);
		else if (streq(arg, "-n") || streq(arg, "--name"))
            InitMsg.alias = strarg(arg, *argv++);
		else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
	}
}


/*
 * Drop alarm routine. It will be called when user type Ctrl+C
 */
static void
drop_alarm(int signo)
{
    TimeToQuit = 1;
}


/*
 * Print out a usage message and exit
 */
static void
usage(void)
{
	char *s = 
        "Usage\n"
	    "   msgtest_hello Options"
	    "Options\n"
	    "   -n|--name N\n"
	    "       Set name.\n"
	    "   -c|--msg_class N\n"
	    "       Set class.\n"
	    "   -p|--port N\n"
	    "       Set port.\n"
	    "   -d|--msg_debug\n"
	    "       Print debugging information.\n"
		"	-h|--help\n"
		"		Print this message.";
	fputs(s, stderr);
	plat_exit(0);
}


/*
 * Message poll function
 */
static void
message_poll()
{
    msg_info_t *info;
    msg_node_t *nodes;
	ntime_t    endtime;
    int no_nodes;

	endtime = msg_endtime(polltime);
    info = msg_poll(endtime);
    if (!info) {
        return;
    }
    if (info->type == MSG_EDROP || info->type == MSG_EJOIN) {
        printf("%s: Node ID %d\n", info->type == MSG_EDROP ?
                        "DROP":"JOIN", info->nno);
    } else if (info->type == MSG_ESENT) {
        if (info->error) {
            printf("SEND ERROR: %s\n", info->error);
        }
    } else if (info->type == MSG_ERECV) {
        if (info->error) {
            printf("RECV ERROR: %s\n", info->error);
        } else {
            /* Receive a message and print it out */
            nodes = msg_getnodes(info->nno, &no_nodes);
            printf("%s: %s\n", nodes->name, info->data);
            msg_freenodes(nodes);
        }
    }
    msg_ifree(info);
}


/*
 * Send data to all nodes
 */
static void
send_msg_to_nodes(char *buf)
{
    int no_nodes;
    msg_node_t *nodes;

    nodes = msg_getnodes(0, &no_nodes);
    send_data_to_nodes(nodes, no_nodes, buf, strlen(buf) + 1);
    msg_freenodes(nodes);
}

