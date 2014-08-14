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

