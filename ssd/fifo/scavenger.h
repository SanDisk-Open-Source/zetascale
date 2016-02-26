/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/** @file scavenger.h
 *  @brief ZS scavenger declarations.
 *
 *  This contains declaration of exported functions for ZS to
 *  scavenge expired objects from ZS
 *
 *  @author Tomy Cheru (tcheru)
 *  SanDisk Proprietary Material, © Copyright 2014 SanDisk, all rights reserved.
 *  http://www.sandisk.com
 */

ZS_status_t zs_start_scavenger_thread(struct ZS_state *zs_state );
ZS_status_t zs_stop_scavenger_thread();

