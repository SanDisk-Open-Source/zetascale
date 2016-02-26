/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/****************************************************************
 *
 * File:   response_tbl.h
 * Author: Brian O'Krafka
 *
 * Created on October 6, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: response_tbl.h 802 2008-03-29 00:44:48Z briano $
 * 
 ****************************************************************/

#ifndef _RESPONSE_TBL_H
#define _RESPONSE_TBL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    RA_None = 0,
    RA_Del_Loc,
    RA_Del_Rem,
    RA_Del_Both,
    RA_Set_Rem_New,
    RA_Set_Rem_Old,
    RA_Valid,
    RA_BAD,
    Resp_N_Actions
} SDF_resp_action_t;

#ifndef _RESPONSE_TBL_C
    extern char *RespActionStrings[];
#else
    char *RespActionStrings[] = {
	"RA_None",
	"RA_Del_Loc",
	"RA_Del_Rem",
	"RA_Del_Both",
	"RA_Set_Rem_New",
	"RA_Set_Rem_Old",
	"RA_Valid",
	"RA_BAD",
    };
#endif

extern void init_resp_tbl();
extern void lookup_resp_tbl(SDF_boolean_t eviction_flag, int flash_flags, 
			    SDF_boolean_t delete_flag,
			    int flash_retcode, SDF_status_t sdf_status,
			    int *pret, SDF_resp_action_t *paction);

#ifdef	__cplusplus
}
#endif

#endif /* _RESPONSE_TBL_H */
