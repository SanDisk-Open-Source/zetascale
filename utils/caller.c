/*
 * File:   caller.c
 * Author: Jim
 *
 * Created on December 31, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http: //www.schoonerinfotech.com/
 *
 * $Id: caller.c 396 2008-02-29 22:55:43Z jim $
 */


/**
 * @brief Get a pointer to the calling location for debug purposes (spin locks)
 *
 * @return Pointer to caller
 */
void *caller() {
    return (__builtin_return_address(0));
}


