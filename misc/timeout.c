/*
 * File:   sdf/misc/timeout.c
 * Author: drew
 *
 * Created on December 23, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: timeout.c 5287 2008-12-24 04:38:05Z drew $
 */

#include <stdio.h>

#include "platform/unistd.h"

#include "misc/misc.h"

/* Include seed in all core dumps */
static int timeout;

int
parse_timeout(const char *string) {
    int ret;

    ret = parse_int(&timeout, string, NULL);
    if (!ret) {
        alarm(timeout);
    }

    return (ret);
}
