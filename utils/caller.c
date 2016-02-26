/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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


