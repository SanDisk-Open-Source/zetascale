/*
 * File: msg_cip.h
 * Author: Johann George
 * Copyright (c) 2009, Schooner Information Technology, Inc.
 */
#ifndef MSG_CIP_H
#define MSG_CIP_H

#include <stdint.h>

/*
 * An IP address that combines both IPv4 and IPv6 addresses.  Note that most of
 * the code assumes that the address is IPv4 but we should be able to extend it
 * in the future.  The elements v4[a-d] are purely so gdb prints this out
 * nicely as it does not seem to understand that uint8_t is not char.
 */
typedef struct cip {
    union {
        uint8_t  v4[4];
        uint32_t v4_d;
        struct {
            uint8_t v4a;
            uint8_t v4b;
            uint8_t v4c;
            uint8_t v4d;
        };
    };
    uint16_t v6[8];
} cip_t;

#endif /* MSG_CIP_H */
