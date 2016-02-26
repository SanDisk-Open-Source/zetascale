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
