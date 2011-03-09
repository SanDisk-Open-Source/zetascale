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
