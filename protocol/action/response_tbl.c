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

/********************************************************************************
 *
 * File:   response_tbl.c
 * Author: Brian O'Krafka
 *
 * Created on October 6, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: response_tbl.c 308 2008-02-20 22:34:58Z briano $
 *
 *
 ********************************************************************************/

#define _RESPONSE_TBL_C

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "sdfmsg/sdf_msg_types.h"

#include "platform/stdlib.h"
#include "platform/stdio.h"
#include "platform/unistd.h"
#include "platform/shmem.h"
#include "platform/assert.h"
#include "platform/stats.h"
#include "common/sdftypes.h"
#include "flash/flash.h"
#include "protocol/action/response_tbl.h"

typedef enum {
    R_Evict = 0,
    R_Store,
    R_Valid,
    R_BAD,
    Resp_N_Modes
} SDF_resp_modes_t;

static char *RespModeStrings[] = {
    "R_Evict",
    "R_Store",
    "R_Valid",
    "R_BAD",
};

typedef enum {
    RF_None = 0,
    RF_Exst,
    RF_NExst,
    RF_Del,
    RF_Valid,
    RF_BAD,
    Resp_N_Flags
} SDF_resp_flags_t;

static char *RespFlagStrings[] = {
    "RF_None",
    "RF_Exst",
    "RF_NExst",
    "RF_Del",
    "RF_Valid",
    "RF_BAD",
};

typedef enum {
    RL_EOK = 0,
    RL_EXST,
    RL_NOEN,
    RL_other,
    RL_Valid,
    RL_othX,
    RL_othN,
    RL_othXN,
    RL_BAD,
    Resp_N_LocResps
} SDF_resp_loc_resps_t;

static char *RespLocStrings[] = {
    "RL_EOK",
    "RL_EXST",
    "RL_NOEN",
    "RL_other",
    "RL_Valid",
    "RL_othX",
    "RL_othN",
    "RL_othXN",
    "RL_BAD",
};

typedef enum {
    RR_SUCCESS = 0,
    RR_STOPPED,
    RR_CTNR_UNK,
    RR_DELFAIL,
    RR_EEXIST,
    RR_ENOENT,
    RR_other,
    RR_Valid,
    RR_othX,
    RR_othN,
    RR_othXN,
    RR_BAD,
    Resp_N_RemResps
} SDF_resp_rem_resps_t;

static char *RespRemStrings[] = {
    "RR_SUCCESS",
    "RR_STOPPED",
    "RR_CTNR_UNK",
    "RR_DELFAIL",
    "RR_EEXIST",
    "RR_ENOENT",
    "RR_other",
    "RR_Valid",
    "RR_othX",
    "RR_othN",
    "RR_othXN",
    "RR_BAD",
};

typedef enum {
    RC_SUCCESS = 0,
    RC_EEXIST,
    RC_ENOENT,
    RC_STOPPED,
    RC_CTNR_UNK,
    RC_other,
    RC_other2,
    RC_DELFAIL,
    RC_FAILURE,
    RC_Valid,
    RC_BAD,
    Resp_N_CombResps
} SDF_resp_cmb_resps_t;

char *RespCombStrings[] = {
    "RC_SUCCESS",
    "RC_EEXIST",
    "RC_ENOENT",
    "RC_STOPPED",
    "RC_CTNR_UNK",
    "RC_other",
    "RC_other2",
    "RC_DELFAIL",
    "RC_FAILURE",
    "RC_Valid",
    "RC_BAD",
};

typedef struct {
    SDF_resp_cmb_resps_t  comb_resp;
    SDF_resp_action_t     action;
} SDF_resp_tbl_t;

typedef struct {
    SDF_resp_modes_t       mode;
    SDF_resp_flags_t       flags;
    SDF_resp_loc_resps_t   loc_resp;
    SDF_resp_rem_resps_t   rem_resp;
    SDF_resp_tbl_t         action;
} SDF_resp_tbl_row_t;

static SDF_resp_tbl_t RespTbl[Resp_N_Modes][Resp_N_Flags][Resp_N_LocResps][Resp_N_RemResps];

static SDF_resp_tbl_row_t RespTblInit[] = {

	  /*************   Eviction Mode   ***************/

  /*  Mode     Flags    Local    Remote        Combined     Action    */

    {R_Evict, RF_None,  RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Evict, RF_None,  RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_None,  RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_None,  RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_None,  RL_EOK,   RR_othXN,    {RC_SUCCESS, RA_None}},

    {R_Evict, RF_None,  RL_othXN, RR_SUCCESS,  {RC_other,   RA_Del_Loc}},
    {R_Evict, RF_None,  RL_othXN, RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_None,  RL_othXN, RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_None,  RL_othXN, RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_None,  RL_othXN, RR_othXN,    {RC_other,   RA_None}},


    {R_Evict, RF_Exst,  RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Evict, RF_Exst,  RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EOK,   RR_EEXIST,   {RC_SUCCESS, RA_Set_Rem_New}},
    {R_Evict, RF_Exst,  RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EOK,   RR_othN,     {RC_SUCCESS, RA_None}},

    {R_Evict, RF_Exst,  RL_EXST,  RR_SUCCESS,  {RC_EEXIST,  RA_Set_Rem_Old}},
    {R_Evict, RF_Exst,  RL_EXST,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EXST,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EXST,  RR_EEXIST,   {RC_EEXIST,  RA_None}},
    {R_Evict, RF_Exst,  RL_EXST,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_EXST,  RR_othN,     {RC_EEXIST,  RA_None}},

    {R_Evict, RF_Exst,  RL_othN,  RR_SUCCESS,  {RC_other,   RA_Del_Rem}},
    {R_Evict, RF_Exst,  RL_othN,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_othN,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_othN,  RR_EEXIST,   {RC_other,   RA_Del_Rem}},
    {R_Evict, RF_Exst,  RL_othN,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_Exst,  RL_othN,  RR_othN,     {RC_other,   RA_None}},


    {R_Evict, RF_NExst, RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Evict, RF_NExst, RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_EOK,   RR_ENOENT,   {RC_SUCCESS, RA_Set_Rem_New}},
    {R_Evict, RF_NExst, RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_EOK,   RR_othX,     {RC_SUCCESS, RA_None}},

    {R_Evict, RF_NExst, RL_NOEN,  RR_SUCCESS,  {RC_ENOENT,  RA_Del_Rem}},
    {R_Evict, RF_NExst, RL_NOEN,  RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Evict, RF_NExst, RL_NOEN,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Evict, RF_NExst, RL_NOEN,  RR_ENOENT,   {RC_ENOENT,  RA_None}},
    {R_Evict, RF_NExst, RL_NOEN,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_NOEN,  RR_othX,     {RC_ENOENT,  RA_None}},

    {R_Evict, RF_NExst, RL_othX,  RR_SUCCESS,  {RC_other,   RA_Del_Rem}},
    {R_Evict, RF_NExst, RL_othX,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_othX,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_othX,  RR_ENOENT,   {RC_other,   RA_None}},
    {R_Evict, RF_NExst, RL_othX,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Evict, RF_NExst, RL_othX,  RR_othX,     {RC_other,   RA_None}},


    {R_Evict, RF_Del,   RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Evict, RF_Del,   RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Evict, RF_Del,   RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Evict, RF_Del,   RL_EOK,   RR_ENOENT,   {RC_SUCCESS, RA_None}},
    {R_Evict, RF_Del,   RL_EOK,   RR_othX,     {RC_DELFAIL, RA_None}},

    {R_Evict, RF_Del,   RL_othXN, RR_SUCCESS,  {RC_other,   RA_None}},
    {R_Evict, RF_Del,   RL_othXN, RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Evict, RF_Del,   RL_othXN, RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Evict, RF_Del,   RL_othXN, RR_ENOENT,   {RC_other,   RA_None}},
    {R_Evict, RF_Del,   RL_othXN, RR_othX,     {RC_DELFAIL, RA_None}},

//-------------------------------------------------------------------------

	  /*************   Store Mode   ***************/

  /*  Mode     Flags    Local    Remote        Combined     Action    */

    {R_Store, RF_None,  RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Store, RF_None,  RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_None,  RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_None,  RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_None,  RL_EOK,   RR_othXN,    {RC_other2,  RA_Del_Loc}},

    {R_Store, RF_None,  RL_othXN, RR_SUCCESS,  {RC_other,   RA_Del_Both}},
    {R_Store, RF_None,  RL_othXN, RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_None,  RL_othXN, RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_None,  RL_othXN, RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_None,  RL_othXN, RR_othXN,    {RC_other,   RA_Del_Loc}},


    {R_Store, RF_Exst,  RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Store, RF_Exst,  RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EOK,   RR_EEXIST,   {RC_SUCCESS, RA_Set_Rem_New}},
    {R_Store, RF_Exst,  RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EOK,   RR_othN,     {RC_other2,  RA_Del_Loc}},

    {R_Store, RF_Exst,  RL_EXST,  RR_SUCCESS,  {RC_EEXIST,  RA_Set_Rem_Old}},
    {R_Store, RF_Exst,  RL_EXST,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EXST,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EXST,  RR_EEXIST,   {RC_EEXIST,  RA_None}},
    {R_Store, RF_Exst,  RL_EXST,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_EXST,  RR_othN,     {RC_other2,  RA_Del_Loc}},

    {R_Store, RF_Exst,  RL_othN,  RR_SUCCESS,  {RC_other,   RA_Del_Both}},
    {R_Store, RF_Exst,  RL_othN,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_othN,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_othN,  RR_EEXIST,   {RC_FAILURE, RA_Del_Both}},
    {R_Store, RF_Exst,  RL_othN,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_Exst,  RL_othN,  RR_othN,     {RC_other,   RA_Del_Loc}},


    {R_Store, RF_NExst, RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Store, RF_NExst, RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_NExst, RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_NExst, RL_EOK,   RR_ENOENT,   {RC_SUCCESS, RA_Set_Rem_New}},
    {R_Store, RF_NExst, RL_EOK,   RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_NExst, RL_EOK,   RR_othX,     {RC_other2,  RA_Del_Loc}},

    {R_Store, RF_NExst, RL_NOEN,  RR_SUCCESS,  {RC_ENOENT,  RA_Del_Rem}},
    {R_Store, RF_NExst, RL_NOEN,  RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Store, RF_NExst, RL_NOEN,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Store, RF_NExst, RL_NOEN,  RR_ENOENT,   {RC_ENOENT,  RA_None}},
    {R_Store, RF_NExst, RL_NOEN,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_NExst, RL_NOEN,  RR_othX,     {RC_other2,  RA_Del_Loc}},

    {R_Store, RF_NExst, RL_othX,  RR_SUCCESS,  {RC_other,   RA_Del_Both}},
    {R_Store, RF_NExst, RL_othX,  RR_STOPPED,  {RC_STOPPED, RA_Del_Loc}},
    {R_Store, RF_NExst, RL_othX,  RR_CTNR_UNK, {RC_CTNR_UNK,RA_Del_Loc}},
    {R_Store, RF_NExst, RL_othX,  RR_ENOENT,   {RC_other,   RA_Del_Loc}},
    {R_Store, RF_NExst, RL_othX,  RR_DELFAIL,  {RC_DELFAIL, RA_Del_Loc}},
    {R_Store, RF_NExst, RL_othX,  RR_othX,     {RC_other,   RA_Del_Loc}},


    {R_Store, RF_Del,   RL_EOK,   RR_SUCCESS,  {RC_SUCCESS, RA_None}},
    {R_Store, RF_Del,   RL_EOK,   RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Store, RF_Del,   RL_EOK,   RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Store, RF_Del,   RL_EOK,   RR_ENOENT,   {RC_SUCCESS, RA_None}},
    {R_Store, RF_Del,   RL_EOK,   RR_othX,     {RC_DELFAIL, RA_None}},

    {R_Store, RF_Del,   RL_othXN, RR_SUCCESS,  {RC_other,   RA_None}},
    {R_Store, RF_Del,   RL_othXN, RR_STOPPED,  {RC_STOPPED, RA_None}},
    {R_Store, RF_Del,   RL_othXN, RR_CTNR_UNK, {RC_CTNR_UNK,RA_None}},
    {R_Store, RF_Del,   RL_othXN, RR_ENOENT,   {RC_other,   RA_None}},
    {R_Store, RF_Del,   RL_othXN, RR_othX,     {RC_DELFAIL, RA_None}},

//-------------------------------------------------------------------------

    {R_BAD, RF_BAD, RL_BAD, RR_BAD, {RC_BAD, RA_BAD}},
};

    // predeclarations
static SDF_resp_modes_t resp_mode(SDF_boolean_t eviction_flag);
static SDF_resp_flags_t resp_flags(int flash_flags, SDF_boolean_t delete_flag);
static SDF_resp_loc_resps_t resp_loc(int flash_retcode);
static SDF_resp_rem_resps_t resp_rem(SDF_status_t  sdf_status);

    // imported from action_new.c
extern int get_retcode(SDF_status_t status);

static void load_row_resp_other(SDF_resp_tbl_row_t *pr, SDF_resp_loc_resps_t loc_resp)
{
    SDF_resp_tbl_t  *paction;


    switch (pr->rem_resp) {
	case RR_othX:
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_other]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_EEXIST]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    break;
	case RR_othN:
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_other]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_ENOENT]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    break;
	case RR_othXN:
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_other]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_EEXIST]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][RR_ENOENT]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    break;
	default:
	    // purposefully empty
	    paction = &(RespTbl[pr->mode][pr->flags][loc_resp][pr->rem_resp]);
	    paction->comb_resp = pr->action.comb_resp;
	    paction->action    = pr->action.action;
	    break;
    }
}

void init_resp_tbl()
{
    int                  i;
    int                  i_mode, i_flags, i_loc, i_rem;
    SDF_resp_tbl_t   *paction;
    SDF_resp_tbl_row_t  *pr;
    SDF_boolean_t        abort_flag;

    for (i_mode = 0; i_mode < Resp_N_Modes; i_mode++) {
	for (i_flags = 0; i_flags < Resp_N_Flags; i_flags++) {
	    for (i_loc = 0; i_loc < Resp_N_LocResps; i_loc++) {
		for (i_rem = 0; i_rem < Resp_N_RemResps; i_rem++) {
		    paction = &(RespTbl[i_mode][i_flags][i_loc][i_rem]);
		    paction->comb_resp = RC_BAD;
		    paction->action    = RA_BAD;
		}
	    }
	}
    }

    for (i=0; 1; i++) {
        pr = &(RespTblInit[i]);
	if (pr->mode == R_BAD) {
	    break;
	}
	switch (pr->loc_resp) {
	    case RL_othX:
	        load_row_resp_other(pr, RL_other);
	        load_row_resp_other(pr, RL_EXST);
	        break;
	    case RL_othN:
	        load_row_resp_other(pr, RL_other);
	        load_row_resp_other(pr, RL_NOEN);
	        break;
	    case RL_othXN:
	        load_row_resp_other(pr, RL_other);
	        load_row_resp_other(pr, RL_EXST);
	        load_row_resp_other(pr, RL_NOEN);
	        break;
	    default:
	        load_row_resp_other(pr, pr->loc_resp);
	        break;
	}
    }

    /* check for missing entries! */

    abort_flag = SDF_FALSE;
    for (i_mode = 0; i_mode < R_Valid; i_mode++) {
	for (i_flags = 0; i_flags < RF_Valid; i_flags++) {
	    for (i_loc = 0; i_loc < RL_Valid; i_loc++) {
		for (i_rem = 0; i_rem < RR_Valid; i_rem++) {
		    paction = &(RespTbl[i_mode][i_flags][i_loc][i_rem]);
		    if (paction->comb_resp == RC_BAD) {
		        if ((i_flags != RF_Del) || (i_rem != RR_DELFAIL)) {
			    plat_log_msg(21159, 
				 PLAT_LOG_CAT_SDF_PROT, 
				 PLAT_LOG_LEVEL_FATAL,
				"Bad combined response in replication response table"
				"RespTbl[%s][%s][%s][%s]", 
				RespModeStrings[i_mode], RespFlagStrings[i_flags], 
				RespLocStrings[i_loc], RespRemStrings[i_rem]);
			    abort_flag = SDF_TRUE;
			}
		    }
		    if (paction->action == RA_BAD) {
		        if ((i_flags != RF_Del) || (i_rem != RR_DELFAIL)) {
			    plat_log_msg(21160, 
				 PLAT_LOG_CAT_SDF_PROT, 
				 PLAT_LOG_LEVEL_FATAL,
				"Bad action in replication response table"
				"RespTbl[%s][%s][%s][%s]", 
				RespModeStrings[i_mode], RespFlagStrings[i_flags], 
				RespLocStrings[i_loc], RespRemStrings[i_rem]);
			    abort_flag = SDF_TRUE;
			}
		    }
		}
	    }
	}
    }
    if (abort_flag) {
        plat_abort();
    }
}

void lookup_resp_tbl(SDF_boolean_t eviction_flag, int flash_flags, 
                     SDF_boolean_t delete_flag,
		     int flash_retcode, SDF_status_t sdf_status,
		     int *pret, SDF_resp_action_t *paction)
{
    SDF_resp_tbl_t  *prow;
    int              ret;

    prow = &(RespTbl[resp_mode(eviction_flag)]
                    [resp_flags(flash_flags, delete_flag)]
	            [resp_loc(flash_retcode)]
	            [resp_rem(sdf_status)]);

    switch (prow->comb_resp) {
        case RC_SUCCESS:  ret = FLASH_EOK;                 break;
        case RC_EEXIST:   ret = FLASH_EEXIST;              break;
        case RC_ENOENT:   ret = FLASH_ENOENT;              break;
        case RC_STOPPED:  ret = FLASH_ESTOPPED;            break;
        case RC_CTNR_UNK: ret = FLASH_RMT_EBADCTNR;        break;
        case RC_other:    ret = flash_retcode;             break;
        case RC_other2:   ret = get_retcode(sdf_status);   break;
        case RC_DELFAIL:  ret = FLASH_RMT_EDELFAIL;        break;
        case RC_FAILURE:  ret = FLASH_EINCONS;             break;
	default:
	    plat_assert(0);
	    break;
    }
    *paction = prow->action;
    *pret = ret;
}
    

static SDF_resp_modes_t resp_mode(SDF_boolean_t eviction_flag)
{
    SDF_resp_modes_t   rm;

    rm = (eviction_flag ? R_Evict : R_Store);

    return(rm);
}

static SDF_resp_flags_t resp_flags(int flash_flags, SDF_boolean_t delete_flag)
{
    SDF_resp_flags_t  rf;

    if (delete_flag) {
        rf = RF_Del;
    } else {
	switch (flash_flags) {
	    case FLASH_PUT_NO_TEST:       rf = RF_None;  break;
	    case FLASH_PUT_TEST_EXIST:    rf = RF_Exst;  break;
	    case FLASH_PUT_TEST_NONEXIST: rf = RF_NExst; break;
	    default:                      rf = RF_None;  plat_assert(0); break;
	}
    }
    return(rf);
}

static SDF_resp_loc_resps_t resp_loc(int flash_retcode)
{
    SDF_resp_loc_resps_t  rl;

    switch (flash_retcode) {
        case FLASH_EOK:    rl = RL_EOK;   break;
        case FLASH_ENOENT: rl = RL_NOEN;  break;
        case FLASH_EEXIST: rl = RL_EXST;  break;
	default:           rl = RL_other; break;
    }

    return(rl);
}

static SDF_resp_rem_resps_t resp_rem(SDF_status_t  sdf_status)
{
    SDF_resp_rem_resps_t  rr;

    switch (sdf_status) {
        case SDF_SUCCESS:               rr = RR_SUCCESS;  break;
        case SDF_STOPPED_CONTAINER:     rr = RR_STOPPED;  break;
        case SDF_CONTAINER_UNKNOWN:     rr = RR_CTNR_UNK; break;
        case SDF_RMT_CONTAINER_UNKNOWN: rr = RR_CTNR_UNK; break;
        case SDF_FLASH_EDELFAIL:        rr = RR_DELFAIL;  break;
        case SDF_FLASH_RMT_EDELFAIL:    rr = RR_DELFAIL;  break;
        case SDF_OBJECT_EXISTS:         rr = RR_EEXIST;   break;
        case SDF_OBJECT_UNKNOWN:        rr = RR_ENOENT;   break;
        default:                        rr = RR_other;    break;
    }

    return(rr);
}



