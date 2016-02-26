/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#include <stdlib.h>
#include "slinkedlist.h"
#include "platform/stdlib.h"

//create a emtry linked list
sLinkListPtr createsLinkList() {
    sLinkListPtr plinkedlist = (sLinkListPtr) plat_malloc(
            sizeof(struct sLinkListProto));
    plinkedlist->head = 0;
    plinkedlist->tail = 0;
    plinkedlist->current = 0;
    plinkedlist->length = 0;
    return plinkedlist;
}

//append a entry to tail of list
sLinkListPtr appendLinkEntry(sLinkListPtr plist, sLinkPtr pentry) {
    if (!plist && !pentry)
        return 0;

    if (plist->length == 0) {
        plist->head = plist->tail = plist->current = pentry;
        plist->length = 1;
        pentry->prev = pentry->next = 0;
        return plist;
    }

    plist->tail->next = pentry;
    pentry->prev = plist->tail;
    pentry->next = 0;
    plist->tail = pentry;
    plist->length++;

    return plist;
}

//delete the entry current pointer point to
sLinkPtr deleteLinkEntry(sLinkListPtr plist) {
    sLinkPtr pdel = 0;
    if (!plist)
        return 0;

    if (plist->length == 0)
        return 0;

    if (plist->length == 1) {
        pdel = plist->current;
        plist->head = 0;
        plist->tail = 0;
        plist->current = 0;
        plist->length = 0;
        return pdel;
    }

    if (plist->tail == plist->current) {
        pdel = plist->current;
        plist->current->prev->next = NULL;
        plist->current = plist->current->prev;
        plist->tail = plist->current;
        plist->length--;
        return pdel;
    }

    if (plist->head == plist->current) {
        pdel = plist->current;
        plist->current->next->prev = NULL;
        plist->current = plist->current->next;
        plist->head = plist->current;
        plist->length--;
        return pdel;
    }

    pdel = plist->current;
    plist->current->prev->next = plist->current->next;
    plist->current->next->prev = plist->current->prev;
    plist->current = plist->current->next;
    plist->length--;
    return pdel;
}

//insert the entry to the place the current pointer point to
//after insertion the current pointer will point to this new entry
sLinkListPtr insertLinkEntry(sLinkListPtr plist, sLinkPtr pentry) {
    if (!plist || !pentry)
        return 0;

    if (plist->length == 0) {
        plist->head = plist->tail = plist->current = pentry;
        plist->length = 1;
        pentry->prev = pentry->next = NULL;
        return plist;
    }

    if (plist->current == NULL)
        return 0;

    if (plist->current == plist->head) {
        plist->head->prev = pentry;
        pentry->next = plist->head;
        pentry->prev = NULL;
        plist->current = plist->head = pentry;
        plist->length++;
        return plist;
    }

    plist->current->prev->next = pentry;
    plist->current->prev = pentry;
    pentry->prev = plist->current->prev;
    pentry->next = plist->current;
    plist->current = pentry;
    plist->length++;
    return plist;
}

void destorysLinkList(sLinkListPtr plist) {
    sLinkPtr ptemp = 0;
    if (!plist)
        return;

    while (plist->head) {
        ptemp = plist->head;
        plist->head = plist->head->next;
        //plist->head->prev = 0;
        plat_free(ptemp);
        ptemp = 0;
    }

    plat_free(plist);
    plist = 0;
}
