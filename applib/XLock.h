/*
 * File:   XLock.h
 * Author: Jim
 *
 * Created on February 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: XLock.h 396 2008-02-29 22:55:43Z jim $
 */

//
// Cross-thread locking.  This is the PTHREAD-callable version
//


#ifndef _APPLIB_XLOCK_H
#define _APPLIB_XLOCK_H

#include "sdfappcommon/XLock.h"

void XLock(XLock_t *cross, int write);
int XTryLock(XLock_t *cross, int write);
void XUnlock(XLock_t *cross);

#endif

