/** @file scavenger.h
 *  @brief FDF scavenger declarations.
 *
 *  This contains declaration of exported functions for FDF to
 *  scavenge expired objects from FDF
 *
 *  @author Tomy Cheru (tcheru)
 *  SanDisk Proprietary Material, © Copyright 2014 SanDisk, all rights reserved.
 *  http://www.sandisk.com
 */

FDF_status_t fdf_start_scavenger_thread(struct FDF_state *fdf_state );
FDF_status_t fdf_stop_scavenger_thread();

