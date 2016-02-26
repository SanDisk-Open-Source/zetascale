//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

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
