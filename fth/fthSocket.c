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
