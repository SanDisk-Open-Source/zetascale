/*
 * File: msgtest_common.c
 * Author: Norman Xu, Jindong Hu
 * Copyright (C) 2009, Schooner Information Technology, Inc.
 * 
 * Common functions for test cases should be defined here
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
 * Print all nodes information
 */
void
print_nodes(msg_node_t * nodes, int no_nodes)
{
    msg_node_t *nodep = nodes;
    int count, addr_count;

    for (count = 0; count < no_nodes; count++, nodep++) {
        if (nodep->nconn == 0)
            continue;
        t_user("   node %d<%s>:", nodep->nno, nodep->name);
        for (addr_count = 0; addr_count < nodep->nconn; addr_count++) {
            t_user("      %s (%s)", nodep->conns[addr_count].raddr,
                   nodep->conns[addr_count].iface);
        }
    }
}


/*
 * Start the hello program
 */
void
send_data_to_nodes(msg_node_t *nodes, int no_nodes, void *data, size_t datalen)
{
    msg_send_t *sendmsg;
    void *sendbuf;
    int count;
    msg_node_t *nodep = nodes;
    msg_node_t *self;

    self = msg_getnodes(msg_mynodeno(), NULL);
    for (count = 0; count < no_nodes; count++, nodep++) {
        if (nodep->nno != 1 && nodep->nconn > 0) {
            sendbuf = (void *)plat_alloc(datalen);
            memset(sendbuf, 0, datalen);
            memcpy(sendbuf, data, datalen);
            sendmsg = msg_salloc();
            sendmsg->stag = sendmsg->dtag = sendmsg->qos = 0;
            sendmsg->sid = 0;
            sendmsg->nno = nodep->nno;
            sendmsg->nsge = 1;
            sendmsg->data = sendbuf;
            sendmsg->sge[0].iov_base = sendbuf;
            sendmsg->sge[0].iov_len = datalen;
            sendmsg->sge[1].iov_base = NULL;
            msg_send(sendmsg);
        }
    }
    msg_freenodes(self);
}


/*
 * Return a numeric argument
 */
int
numarg(char *opt, char *arg, int min)
{
    int n = atoi(strarg(opt, arg));

    if (n < min)
        panic("value to %s must be at least %d", opt, min);

    return n;
}


/*
 * Return a string argument. 
 */
char *
strarg(char *opt, char *arg)
{
    if (!arg)
        panic("%s requires an argument", opt);

    return arg;
}


/*
 * Read config from files
 */
void
read_config(char *filename, msgtest_config_t *config)
{
    FILE *cf = NULL;
    char buf[1024];
    char *ptr;

    cf = fopen(filename, "r");
    if (!cf) {
        t_user("Read config failed");
        return;
    }
    
    while (fgets(buf, sizeof (buf), cf)) {
        char *s;
        /* Scan for begin comment */
        for (s = buf; *s != '\0' && *s != '#'; ++s) 
            ;
        if (*s == '#') {
            if (s == buf) {
                continue;
            }
            *s = '\0';
        }
        while (s > buf && (s[-1] == ' ' || s[-1] == '\t' || s[-1] == '\n')) {
            --s;
            *s = '\0';
        }
        if (s == buf) {
            continue;
        }
        ptr = buf;
        while (*ptr == ' ' || *ptr == '\t') 
            ptr++;
        config->addresses[config->remote_nodes++] = plat_strdup(ptr);
    }
}


/*
 * return the current time in nanoseconds  
 */

ntime_t msg_gettime(void) {
     return getntime();
}

