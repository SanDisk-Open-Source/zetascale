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
 * File: msgtest_remote.c
 * Author: Norman Xu
 * Copyright (C) 2009, Schooner Information Technology, Inc.
 *
 * Say hello to remote nodes continuous, it is very similiar with 
 * msgtest_hello. It never bcast to nodes but only send messages to 
 * nodes which is defined.
 */

#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include "msg_err.h"
#include "msg_map.h"
#include "msg_msg.h"
#include "msg_util.h"
#include "trace.h"
#include "msgtest_common.h"

/*
 * Useful definitions
 */
#define streq(a, b) (strcmp(a, b) == 0)
#define DEFAULT_POLLTIME 1000 * 1000
#define MAX_BUFSIZE 1024

/*
 * Static variables
 */
static msg_init_t InitMsg;
static ntime_t polltime = DEFAULT_POLLTIME;
static int Debug = 0;
static int TimeToQuit = 0;
static char *cfgfile;
/* 
 * Some print function.
 */
void panic(char *fmt, ...);


/*
 * Function prototypes
 */
static void parse_args(char **argv);
static void sayhellos(msg_node_t * nodes, int no_nodes);
static void drop_alarm(int signo);
static void usage();


/*
 * Main function. 
 */
int
main(int argc, char *argv[])
{
    int myid, no_nodes;
    msg_node_t  *mynode;
    msg_info_t  *info = NULL;
    msg_node_t  *nodes = NULL;
    msgtest_config_t config;
    ntime_t endtime;
    int active_nodes = 0, count = 0;
    int bconnected = 0;
    int i  = 0;

    /* parse input argument and initialize InitMsg with arguments */
    parse_args(argv + 1);
    msg_init(&InitMsg);
    msg_setbcast(0);
    memset(&config, 0, sizeof (msgtest_config_t));
    read_config(cfgfile, &config);
    
    for (i = 0; i < config.remote_nodes; i++) {
        t_user("New address: %s added in", config.addresses[i]);
        msg_nodeknow(config.addresses[i]);
    }

    /* register signal process function */
    if (SIG_ERR == signal(SIGINT, drop_alarm)) {
        t_user("Register signal function failed");
    }
    
    /* get myself */
    myid = msg_mynodeno();
    /* get other nodes */
    mynode = msg_getnodes(myid, &no_nodes);
    if (mynode) {
        print_nodes(mynode, 1);
        msg_freenodes(mynode);
    } else {
        panic("I can't get my node");
    }

    /* big loop to recv and send messages */
    while (!TimeToQuit) {
        /* retrieve all nodes information */
        nodes = msg_getnodes(0, &active_nodes);
        if (active_nodes > 1 && count % 1000 == 0) {
            sayhellos(nodes, active_nodes);
            usleep(2000);
        }
        /* free nodes */
        msg_freenodes(nodes);
        count++;

        endtime = msg_endtime(polltime);
        info = msg_poll(endtime);
        if (!info) {
            continue;
        } else if (info->type == MSG_EDROP || info->type == MSG_EJOIN) {
            /* retrieve all nodes information */
            nodes = msg_getnodes(0, &active_nodes);
            if (info->type == MSG_EJOIN) {
                bconnected = 1;
            }
            else {
                bconnected = 0;
            }
            t_user("%s: Node ID %d\n", info->type == MSG_EDROP ?
                   "DROP" : "JOIN", info->nno);
            print_nodes(nodes, active_nodes);
            msg_freenodes(nodes);
            nodes = NULL;
            msg_ifree(info);
        }  else if (info->type == MSG_ERECV) {
            if (info->error) {
                t_user("RECV ERROR: %s", info->error);
            }
            else {
                t_ubug("RECV: %s\n", info->data);
            }
            msg_ifree(info);
        } else if (info->type == MSG_ESENT) {
            if (info->error) {
                t_user("SEND ERROR: %s", info->error);
            }
            msg_ifree(info);
        } else {
            t_ubug("Other message type");
            msg_ifree(info);
        }
    }

    for (;;) {
        endtime = msg_endtime(polltime);
        info = msg_poll(endtime);
        if (!info)
            break;
        msg_ifree(info);
    }
    t_user("calling msg_exit");
    msg_exit();

    return 0;
}


/* 
 * Parse arguments
 */
static void
parse_args(char **argv)
{
    for (;;) {
        char *arg = *argv++;

        if (!arg)
            break;
        if (streq(arg, "-n") || streq(arg, "--msg_name"))
            InitMsg.alias = strarg(arg, *argv++);
        else if (streq(arg, "-i") || streq(arg, "--interface"))
            InitMsg.iface = strarg(arg, *argv++);
        else if (streq(arg, "-c") || streq(arg, "--msg_class"))
            InitMsg.class = numarg(arg, *argv++, 0);
        else if (streq(arg, "-p") || streq(arg, "--udp_port"))
            InitMsg.udpport = numarg(arg, *argv++, 0);
        else if (streq(arg, "-t") || streq(arg, "--tcp_port"))
            InitMsg.tcpport = numarg(arg, *argv++, 0);
        else if (streq(arg, "-f") || streq(arg, "--config_file"))
            cfgfile = strarg(arg, *argv++);
        else if (streq(arg, "-d") || streq(arg, "--msg_debug"))
            Debug = numarg(arg, *argv++, 0);
        else if (streq(arg, "-h") || streq(arg, "--help"))
            usage();
        else
            usage();
    }
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
        "      Set name.\n"
        "   -i|--interface i1:i2:...:in\n"
        "      Use only speciafied interfaces.\n"
        "   -c|--class N\n"
        "      Set class.\n"
        "   -p|--udpport N\n"
        "      Set udpport.\n"
        "   -t|--tcpport N\n"
        "      Set tcpport.\n"
        "   -f|--config_file\n"
        "      Set config_file."
        "   -d|--debug N\n" 
        "      Print debugging information.\n";
    fputs(s, stderr);
    plat_exit(0);
}


/*
 * Start the hello program
 */
static void
sayhellos(msg_node_t * nodes, int no_nodes)
{
    char sendbuf[MAX_BUFSIZE];
    msg_node_t *self = msg_getnodes(msg_mynodeno(), NULL);

    snprintf(sendbuf, MAX_BUFSIZE - 1, "Hello from %s", self->name);
    send_data_to_nodes(nodes, no_nodes, sendbuf, strlen(sendbuf) + 1);
    msg_freenodes(self);
}


/*
 * Drop alarm routine. It will be called when user type Ctrl+C
 */
static void
drop_alarm(int signo)
{
    TimeToQuit = 1;
}


