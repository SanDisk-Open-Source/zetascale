/*
 * File: msg_hello.h
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 */
#ifndef MSG_HELLO_H
#define MSG_HELLO_H

#include <stdint.h>
#include "msg_cip.h"


/*
 * Versions.
 */
#define V1_HELLO    1                   /* Version 1 of hello message */
#define V2_HELLO    2                   /* Version 2 of hello message */
#define VS_HELLO    2                   /* The maximum version we support */


/*
 * Constants.
 *  NNSIZE - Size of a node name.
 */
#define NNSIZE      128


/*
 * Common to all hello messages.
 */
typedef struct {
    uint16_t cver;
} hello0_t;


/*
 * Hello message version 1.
 */
typedef struct {
    uint16_t cver;
    uint16_t class;
    uint16_t type;
    uint16_t port;
    cip_t    dip;
    char     name[100];
} hello1_t;


/*
 * Hello message version 2.
 */
typedef struct {
    uint16_t cver;
    uint16_t sver;
    uint16_t class;
    uint16_t type;
    uint16_t port;
    cip_t    dip;
    cip_t    uip;
    uint16_t nconn;
    char     name[128];
} hello2_t;


/*
 * A hello message buffer that is large enough to hold any hello message.
 */
typedef union {
    hello0_t hello0;
    hello1_t hello1;
    hello2_t hello2;
} hellobuf_t;


/*
 * Hello standardized version.
 *
 *  class   - The message class.  We only accept messages of our class.
 *  cver    - Current version of this message.
 *  dip     - The message destination IP.
 *  nconn   - The number of connections that are being requested.
 *  port    - The TCP port the recepient can connect to.
 *  sip     - The message source IP.
 *  sver    - Supported version of this message.
 *  type    - This is either a join message MSG_EJOIN or a drop message
 *            MSG_EDROP.
 *  uip     - The unique IP of the node that initiated the message.
 */
typedef struct {
    uint8_t     cver;
    uint8_t     sver;
    uint16_t    class;
    uint16_t    type;
    uint16_t    port;
    cip_t       uip;
    cip_t       sip;
    cip_t       dip;
    uint16_t    nconn;
    char        name[NNSIZE];
} hellostd_t;

#endif /* MSG_HELLO_H */
