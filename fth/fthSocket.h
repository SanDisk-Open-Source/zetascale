/*
 * File:   fthSocket.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http:                                     //www.schoonerinfotech.com/
 *
 * $Id: fthSocket.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Test program for many fth functions
//


#ifndef _FTH_SOCKET_H
#define _FTH_SOCKET_H

#include <stdarg.h>

// We wait a long time between checks of the socket since a socket op is
// both unusual and not time critical
#define FTH_SOCKET_YIELD_COUNT 100

int fthSockWait(int count, ...);

#endif

