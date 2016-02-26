/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
