/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File:   sdf/platform/shmem.h
 * Author: drew
 *
 * Created on May 24, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_shmem.c 1196 2008-05-09 03:17:10Z drew $
 */

/**
 * Instantiate sdfmsg shared memory code
 */

#include "sdfmsg/sdf_msg.h"

PLAT_SP_VAR_OPAQUE_IMPL(sdf_msg_sp, struct sdf_msg)
