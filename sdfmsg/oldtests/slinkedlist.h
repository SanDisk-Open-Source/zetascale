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
 * File:   slink.h
 * Author: Norman Xu
 *
 * Created on April 30, 2008, 9:42 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 * Simple Linked List
 * $Id: slink.h 308 2008-04-30 9:42:58Z norman $
 */

#ifndef SLINK_H_
#define SLINK_H_

struct sLinkEntry {
    char str[256];
    void * userdef_ptr;
    struct sLinkEntry* prev;
    struct sLinkEntry* next;
};

typedef struct sLinkEntry * sLinkPtr;

struct sLinkListProto {
    sLinkPtr head;
    sLinkPtr tail;
    sLinkPtr current;
    int length;
};

typedef struct sLinkListProto sLinkList, *sLinkListPtr;

//enum appendtype{appendtotail = 0, appendtohead, appendbefcurrent, appendaftercurrent};
//enum appendtype{deletefromtail = 0, deletefromhead, deletefromcurrent};

sLinkListPtr createsLinkList();
sLinkListPtr appendLinkEntry(sLinkListPtr plist, sLinkPtr pentry);
sLinkListPtr insertLinkEntry(sLinkListPtr plist, sLinkPtr pentry);
sLinkPtr deleteLinkEntry(sLinkListPtr plist);
void destorysLinkList(sLinkListPtr plist);

#endif /*SLINK_H_*/
