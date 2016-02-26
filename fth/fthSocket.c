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
 * File:   fthSocket.c
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSocket.c 396 2008-02-29 22:55:43Z jim $
 */

//
// Test program for many fth functions
//


#include <stdarg.h>
#include <sys/socket.h>
#include <errno.h>

#include "fthSocket.h"
#include "fth.h"
#include "fthThread.h"

int fthSockWait(int count, ...) {
    va_list ap;
    char c;                                  // Dummy buffer

    while (1) {                              // Loop until we get data or error
        va_start(ap, count);                 // (re) init args
        for (int i = 0; i < count; i++) {    // For each arg
            int sock = va_arg(ap, int);      // Get the socket
            // Try to peek at one byte w/o blocking
            int size = recv(sock, &c, 1, MSG_DONTWAIT | MSG_PEEK);
            if (size == 1) {                 // if read worked
                va_end(ap);                  // Be clean
                return (sock);               // Return the ready socket
            } else if ((size != -1) || (errno != EAGAIN)) { // Only error we like
                va_end(ap);                  // Be clean
                return(sock);                // Let caller untangle
            }
        }

        // Nothing interesting - wait a while
        fthYield(FTH_SOCKET_YIELD_COUNT);    // Wait a long time
    }
    
}
