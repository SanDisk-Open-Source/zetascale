/*
 * File:   utils/linkedlist.c
 * Author: Darpan Dinker
 *
 * Created on February 6, 2008, 11:16 AM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: linkedlist.c 309 2008-02-20 23:08:19Z darpan $
 */

#include "platform/logging.h"
#include "platform/assert.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"

#include "linkedlist.h"

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_SHARED
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG

// =============================================================================
#define DEBUG 0

ListPtr
List_create(const char *key, void* value, uint16_t keyLen)
{
    ListPtr l = (ListPtr)plat_alloc(sizeof (struct ListEntry));

    l->Next = NULL;
    l->Previous = NULL;
    l->key = key;
    l->value = value;
    l->keyLen = keyLen;

    return (l);
}

LinkedList
LinkedList_create()
{
    LinkedList ll = (LinkedList)plat_alloc(sizeof (struct LinkedListInstance));

    ll->Curr = NULL;
    ll->Head = NULL;
    ll->Tail = NULL;
    ll->size = 0;

    return (ll);
}

void
LinkedList_destroy(LinkedList ll)
{
    if (DEBUG) plat_log_msg(21744, LOG_CAT, LOG_DBG, "In LinkedList_destroy()");

    ListPtr temp = ll->Head;

    while (ll->Head != NULL) {
        ll->Head = ll->Head->Next;
        plat_free(temp);
        temp = ll->Head;
    }
    
    plat_free(ll);
}

uint32_t
LinkedList_getSize(LinkedList ll)
{
    // TODO put in macro
    return (ll->size);
}

SDF_boolean_t
LinkedList_isEmpty(LinkedList ll)
{
    // TODO put in macro
    return ((NULL == ll->Head) ? SDF_TRUE : SDF_FALSE);
}

void
LinkedList_printElements(LinkedList ll) {
    const int size = 25600;
    char str[size]; // TODO is it enough?
    int i = 0;

    plat_log_msg(21745, LOG_CAT, LOG_DBG, "In LinkedList_printElements()");

    ListPtr temp = ll->Head;
    i += snprintf(str+i, size-i, "List elements {");
    while (temp) {
        i += snprintf(str+i, size-i, "%s, ", temp->key);
        temp = temp->Next;
    }
    i += snprintf(str+i, size-i, "}");
    // plat_assert(i < size);
    plat_log_msg(PLAT_LOG_ID_INITIAL, PLAT_LOG_CAT_SDF_CLIENT,
                 PLAT_LOG_LEVEL_DEBUG, str);
}

void* LinkedList_replace(LinkedList ll, const char *key, void* value, uint16_t keyLen)
{
    if (DEBUG) plat_log_msg(21746, LOG_CAT, LOG_DBG, "In LinkedList_put(%s)\n", key);

    if (!key || !value || LinkedList_isEmpty(ll)) {
        return (NULL);
    }

    // atleast 1 entry in the list
    ListPtr temp = ll->Head;

    while (temp) {
        if (temp->keyLen == keyLen) {
            if (0 == strcmp(temp->key, key)) {
                void *prev = temp->value;
                temp->value = value;
                return (prev);
            }
        }
        temp = temp->Next;
    }

    return (NULL);
}

SDF_boolean_t
LinkedList_put(LinkedList ll, const char *key, void* value, uint16_t keyLen)
{
    
    if (DEBUG) plat_log_msg(21746, LOG_CAT, LOG_DBG, "In LinkedList_put(%s)\n", key);

    if (!key || !value) {
        return (SDF_FALSE);
    }

    if (LinkedList_isEmpty(ll)) { // 0 entries in the list
        ll->Head = List_create(key, value, keyLen);
        ll->Curr = ll->Tail = ll->Head;
        plat_assert(NULL != ll->Head);
        ll->size++;
        return (SDF_TRUE);
    }

    // atleast 1 entry in the list
    ListPtr temp = ll->Head;

    while (temp) {
        if (temp->keyLen == keyLen) {
            if (0 == strcmp(temp->key, key)) {
                plat_log_msg(21747, LOG_CAT, LOG_DBG, "LinkedList_put() - key[%s] already present\n", key);
                return (SDF_FALSE);
            }
        }
        temp = temp->Next;
    }

    // now add the new entry at the tail, i.e. list not sorted
    ListPtr node = List_create(key, value, keyLen);
    ll->Tail->Next = node;
    node->Previous = ll->Tail;
    ll->Tail = node;

    ll->size++;
    return (SDF_TRUE);
}

void*
LinkedList_remove(LinkedList ll, const char *key, uint16_t keyLen)
{
    // if(ll->DEBUG)
    //    debug("In LinkedList_remove()");

    if (!key) {
        plat_log_msg(21748, LOG_CAT, LOG_DBG, "In LinkedList_remove(), key is NULL *****\n");        
        return (NULL);
    }

    if (LinkedList_isEmpty(ll)) {
        plat_log_msg(21749, LOG_CAT, LOG_DBG, "In LinkedList_remove(%s), list is empty *****\n", key);
        return (NULL);
    }

    ListPtr temp;
    void* ret;

    if (ll->Head->keyLen == keyLen && 0 == strcmp(ll->Head->key, key)) {  // need to remove the 1st entry
        temp = ll->Head;
        ret = temp->value;

        if (1 == LinkedList_getSize(ll)) {
            plat_free(ll->Head);
            ll->Curr = ll->Head = ll->Tail = NULL;
        } else {
            if (ll->Curr == ll->Head) {
                ll->Curr = ll->Head->Next;
            }
            plat_free(ll->Head);
            ll->Head = ll->Curr;
            ll->Head->Previous = NULL;
        }
        ll->size--;
        return (ret);
    } else if (ll->Tail->keyLen == keyLen && 0 == strcmp(ll->Tail->key, key)) {
        // remove last entry, also we assume now that Head != Tail
        temp = ll->Tail;
        ret = temp->value;

        if (ll->Curr == ll->Tail) {
            ll->Curr = ll->Head;
        }
        ll->Tail = ll->Tail->Previous;
        plat_free(ll->Tail->Next);
        ll->Tail->Next = NULL;
        ll->size--;
        return (ret);
    } else { // remove in the middle somewhere

        // atleast 1 entry in the list
        ListPtr temp = ll->Head->Next;

        while (temp) {
            if (temp->keyLen == keyLen) {
                if (0 == strcmp(temp->key, key)) {
                    ret = temp->value;
                    if (ll->Curr == temp) {
                        ll->Curr = temp->Next;
                    }
                    temp->Previous->Next = temp->Next;
                    temp->Next->Previous = temp->Previous;
                    plat_free(temp);
                    ll->size--;
                    return (ret);
                }
                temp = temp->Next;
            }
        }
    }

    plat_log_msg(21750, LOG_CAT, LOG_DBG, "In LinkedList_remove(%s), could not find, sz of list =%d *****\n", 
                 key, ll->size);
    return (NULL);
}

void*
LinkedList_get(LinkedList ll, const char *key, uint16_t keyLen)
{
    if (!key) {
        plat_log_msg(21751, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_DEBUG,
                     "In LinkedList_get(), key is NULL *****\n");
        return (NULL);
    }

    if (LinkedList_isEmpty(ll)) {
        plat_log_msg(21752, LOG_CAT, LOG_DBG, "In LinkedList_get(%s), list is empty *****\n", key);
        return (NULL);
    }

    if (ll->Head->keyLen == keyLen && 0 == strcmp(ll->Head->key, key)) {
        return (ll->Head->value);
    }

    ListPtr temp = ll->Head;

    while (NULL != (temp = temp->Next)) {
        if (temp->keyLen == keyLen) {
            if (0 == strcmp(temp->key, key)) {
                return (temp->value);
            }
        }
    }

    plat_log_msg(21753, LOG_CAT, LOG_DBG, "In LinkedList_get(%s), could not find *****\n", key);
    return (NULL);
}
