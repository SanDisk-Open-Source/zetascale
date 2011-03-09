/*
 * File:   protocol_common.h
 * Author: Brian O'Krafka
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: protocol_common.h 802 2008-03-29 00:44:48Z darpan $
 */

#ifndef _PROTOCOL_COMMON_H
#define _PROTOCOL_COMMON_H

#include "common/sdftypes.h"
#include "replication/meta_types.h"
#include "shared/sdf_sm_msg.h"

#ifdef __cplusplus
extern "C" {
#endif

    /*
     *  These are used for lists in the home directories.
     */

typedef struct del {
   uint64_t      id;   /* xxxzzz make this a union of thrd/transID */
   struct del   *next;
} DirEntryListLink;

typedef enum {
    AHCND=0, AHCWD, AHCOB, AHCOP, AHDOB, AHFLS, AHFLI, AHFLD, AHFID, AHGCN,
    AHGCD,   AHGTR, AHGRR, AHGTW, AHGWR, AHGWN, AHGWD, AHLKR, AHLRN, AHLKW,
    AHLWN,   AHULR, AHULW, AHUNP, AHUPG, AHUGC, AHICD, AHGCS, AHGWA, AHSOB,
    AHFLC,   AHFIC, AHSOP, AHPTA, AHSYN,

    FHCRC, FHCRF, FHDAT, FHDEC, FHDEF, FHGTF, FHPTC, FHPTF, FHXST, FHNXS,
    FHENM, FHENF, FHCSC, FHCSF, FHDSC, FHDSF, FHGSC, FHGSF, FHICC, FHICF,
    FHSTC, FHSTF, FHGLC, FHGLF, FHSCC, FHSCF, FHOSC, FHOSF, FHXXP, FHGXP,
    FHGDF, FHXDF, FHSSC, FHSSF, FHGIC, FHGIF, FHGCC, FHGCF, FHGNC, FHGNF,
    FHSRC, FHSRF, FHSPC, FHSPF, FHFLC, FHFLF, FHRVC, FHRVF, FHNPC, FHNPF,

    HAATR, HACSC, HACRC, HADEC, HAFLC, HAFIC, HAFCC, HAGRC, HAGWC, HALKC,
    HALNF, HALNS, HAREC, HAULC, HAUGC, HAUPC, HASTC, HASTF, HAIFC, HAPAF,
    HAPAC, HASYC, HASYF,

    HTATR, HTARF, HTAWF, HTDEL, HTFLS, HTFLI, HTFLC, HTINV, HTUGR, HTRDF,
    HTWRF, HTICD, HTWAF,

    TAAAK, TAATR, TAFRD, TAFWD, TAIVA, TAUGA, TAWPN, TAWTR, TAFWA,

    THDAK, THFCC, THFLD, THWPN, THWTR, THIDC,

    HRREP, RHREC, HRSET, HRCRE, HRDEL,

    HFGFF, HFPTF, HFCIF, HFCZF, HFDFF, HFXST, HFSEN, HFNEN,
    HFCSH, HFDSH, HFGBC, HFGIC, HFGLS, HFGSN, HFICD, HFGCS, HFSET, HFSCN,
    HFOSH, HFSSH, HFSRR, HFSPR, HFFLA, HFRVG, HFNOP,

    MMCSM, MMGSM, MMPSM, MMDSM, MMRSM, MMSMC,

    HALKF, HAULF, HACRF, HADEF, HAGRF, HAGWF, HAFLF, HAFIF, HACSF,
    HAICC, HAICF, HAGSC, HAGSF, HAWAC, HAWAF,

    // Added for writeback cache support
    HFFLS, HFFIV, HFINV, HFFLC, HFFLI, HFINC, HFPBD,
    FHFCC, FHFIC, FHINC, FHLCC, FHLIC, FHCIC, FHPBC,
    FHFCF, FHFIF, FHINF, FHLCF, FHLIF, FHCIF, FHPBF,

    /* Fast recovery */
    HFFGB, HFFGC, HFFGD, HFFSX, HFFRC,

    ZDUMY,

    N_SDF_PROTOCOL_MSGS,

    /** @brief Out-of-band value for message type */
    SDF_PROTOCOL_MSG_INVALID = -1
} SDF_protocol_msg_type_t;

typedef uint64_t  SDF_protocol_msg_fmt_t;

typedef enum {
    m_mtyp 	   = (1L<<0),
    m_tag_	   = (1L<<1),
    m_fnod 	   = (1L<<2),
    m_tnod 	   = (1L<<3),
    m_thrd 	   = (1L<<4),
    m_ncac	   = (1L<<5),
    m_tnid 	   = (1L<<6),
    m_tran    	   = (1L<<7),
    m_cgid 	   = (1L<<8),
    m_key_	   = (1L<<9),
    m_size 	   = (1L<<10),
    m_ofst 	   = (1L<<11),
    m_naks 	   = (1L<<12),
    m_anod	   = (1L<<13),
    m_fmpt         = (1L<<14),
    m_stat 	   = (1L<<15),
    m_rlst         = (1L<<16),
    m_slst         = (1L<<17),
    m_data 	   = (1L<<18),
    m_wstt 	   = (1L<<19),
    m_pin_ 	   = (1L<<20),
    m_rstt 	   = (1L<<21),
    m_enpt 	   = (1L<<22),
    m_shard        = (1L<<23),
    m_expt         = (1L<<24),
    m_invt         = (1L<<25),
    m_stky         = (1L<<26),
    m_ctst         = (1L<<27),
    m_crtt         = (1L<<28),
    m_shardcnt     = (1L<<29),
    m_seqno	   = (1L<<30),
    m_seqno_len    = (1L<<31),
    m_seqno_max    = (1L<<32),
    m_cookie1      = (1L<<33),
    m_cookie2      = (1L<<34),
    m_opmeta       = (1L<<35),
} SDF_protocol_msg_fields_t;

#define N_SDF_PROTOCOL_MSG_FMT_FIELDS  36

   /* the following must be kept in sync with preceding enums! */
#ifndef _PROTOCOL_COMMON_C
    extern SDF_protocol_fmt_info_t SDF_Protocol_Msg_Fmt_Info[];
#else
    SDF_protocol_fmt_info_t SDF_Protocol_Msg_Fmt_Info[N_SDF_PROTOCOL_MSG_FMT_FIELDS] =
    {
        {m_mtyp,        "msg_type"},
        {m_tag_,        "tag"},
        {m_fnod,        "from_node"},
        {m_tnod,        "to_node"},
        {m_thrd,        "thrd"},
        {m_ncac,        "cache"},
        {m_tnid,        "transid"},
        {m_tran,        "trans"},
        {m_cgid,        "cguid"},
        {m_key_,        "key"},
        {m_size,        "size"},
        {m_ofst,        "offset"},
        {m_naks,        "n_acks"},
        {m_anod,        "action_node"},
        {m_fmpt,        "flashmap_ptr"},
        {m_stat,        "status"},
        {m_rlst,        "replica_list"},
        {m_slst,        "state_list"},
        {m_data,        "data"},
        {m_wstt,        "wstate"},
        {m_pin_,        "pin"},
        {m_rstt,        "req_status"},
        {m_enpt,        "enum_ptr"},
        {m_shard,       "shard"},
        {m_expt,        "exptime"},
        {m_invt,        "invtime"},
        {m_stky,        "stat_key"},
        {m_ctst,        "ctnr_stat"},
        {m_crtt,        "create_time"},
        {m_shardcnt,     "shard_count"},
        {m_seqno,	 "sequence_start"},
        {m_seqno_len,	 "sequence_length"},
        {m_seqno_max,	 "sequence_max"},
        {m_cookie1,      "cookie1"},
        {m_cookie2,      "cookie2"},
	{m_opmeta,       "opmeta"},
    };
#endif

typedef enum {
    N_ACTION = 0,
    N_HOME,
    N_THIRD,
    N_FLASH,
    N_REPLICA,
    N_NONE
} SDF_protocol_nodes_t;

#define N_SDF_PROTOCOL_NODES  6

#ifndef _PROTOCOL_COMMON_C
    extern SDF_protocol_fmt_info_t SDF_Protocol_Nodes_Info[];
#else
    SDF_protocol_fmt_info_t SDF_Protocol_Nodes_Info[N_SDF_PROTOCOL_NODES] =
    {
        {N_ACTION,      "action"},
        {N_HOME,        "home"},
        {N_THIRD,       "third_party"},
        {N_FLASH,       "flash"},
        {N_REPLICA,     "replica"},
        {N_NONE,        "none"},
    };
#endif

typedef enum {
    C_REQST = 0,
    C_RESPS,
    C_THIRD,
    C_FINAL,
    C_REPLI,
    C_AKREP,
    C_NONE_
} SDF_protocol_mclass_t;

#define N_SDF_PROTOCOL_MCLASSES  7

#ifndef _PROTOCOL_COMMON_C
    extern SDF_protocol_fmt_info_t SDF_Protocol_MClass_Info[];
#else
    SDF_protocol_fmt_info_t SDF_Protocol_MClass_Info[N_SDF_PROTOCOL_MCLASSES] =
    {
        {C_REQST,     "request"},
        {C_RESPS,     "response"},
        {C_THIRD,     "third_party"},
        {C_FINAL,     "final_ack"},
        {C_REPLI,     "replicate"},
        {C_AKREP,     "ack_replicate"},
        {C_NONE_,     "NONE"},
    };
#endif

typedef struct {
   SDF_protocol_msg_type_t msgtype;
   char		          *shortname;
   char		          *name;
   SDF_protocol_mclass_t   mclass;
   SDF_protocol_nodes_t    src;
   SDF_protocol_nodes_t    dest;
   SDF_protocol_msg_fmt_t  format;
} SDF_Protocol_Msg_Info_t;

#ifndef _PROTOCOL_COMMON_C
    extern SDF_Protocol_Msg_Info_t SDF_Protocol_Msg_Info[];
#else
    SDF_Protocol_Msg_Info_t SDF_Protocol_Msg_Info[N_SDF_PROTOCOL_MSGS] = {
	{HAATR, "HAATR", "HA_abort_trans",               C_RESPS,    N_HOME,  N_ACTION, (0|m_tnid)},
	{HACSC, "HACSC", "HA_castout_complete",          C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HACSF, "HACSF", "HA_castout_failed",            C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HACRC, "HACRC", "HA_create_complete",           C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HACRF, "HACRF", "HA_create_failed",             C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HADEC, "HADEC", "HA_delete_complete",           C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HADEF, "HADEF", "HA_delete_failed",             C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAFLC, "HAFLC", "HA_flush_complete",            C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAFLF, "HAFLF", "HA_flush_failed",              C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAFIC, "HAFIC", "HA_flush_inval_complete",      C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAFIF, "HAFIF", "HA_flush_inval_failed",        C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAFCC, "HAFCC", "HA_flush_container_complete",  C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAIFC, "HAIFC", "HA_flush_inval_container_complete",  C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAGSC, "HAGSC", "HA_get_container_stat_complete",  C_RESPS,    N_HOME,  N_ACTION, (0|m_ctst)},
	{HAGSF, "HAGSF", "HA_get_container_stat_failed", C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAGRC, "HAGRC", "HA_get_to_read_complete",      C_RESPS,    N_HOME,  N_ACTION, (0|m_data|m_size|m_expt|m_crtt|m_wstt)},
	{HAGRF, "HAGRF", "HA_get_to_read_failed",        C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAGWC, "HAGWC", "HA_get_to_write_complete",     C_RESPS,    N_HOME,  N_ACTION, (0|m_data|m_size|m_expt|m_crtt|m_wstt)},
	{HAGWF, "HAGWF", "HA_get_to_write_failed",       C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAWAC, "HAWAC", "HA_get_to_write_all_complete", C_RESPS,    N_HOME,  N_ACTION, (0|m_expt|m_crtt|m_wstt)},
	{HAWAF, "HAWAF", "HA_get_to_write_all_failed",   C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAICC, "HAICC", "HA_inval_container_delayed_complete",  C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HAICF, "HAICF", "HA_inval_container_delayed_failed",  C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HALKC, "HALKC", "HA_lock_complete",             C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HALKF, "HALKF", "HA_lock_failed",               C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HALNF, "HALNF", "HA_lock_noblock_fail",         C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HALNS, "HALNS", "HA_lock_noblock_succeed",      C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAPAC, "HAPAC", "HA_put_all_complete",          C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAPAF, "HAPAF", "HA_put_all_failed",            C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAREC, "HAREC", "HA_replication_complete",      C_FINAL,    N_HOME,  N_ACTION, (0)},
	{HASTC, "HASTC", "HA_set_complete",              C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HASTF, "HASTF", "HA_set_failed",                C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAULC, "HAULC", "HA_unlock_complete",           C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAULF, "HAULF", "HA_unlock_failed",             C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAUPC, "HAUPC", "HA_unpin_complete",            C_RESPS,    N_HOME,  N_ACTION, (0)},
	{HAUGC, "HAUGC", "HA_upgrade_complete",          C_RESPS,    N_HOME,  N_ACTION, (0)},
	{TAAAK, "TAAAK", "TA_abort_ack",                 C_RESPS,   N_THIRD,  N_ACTION, (0|m_naks)},
	{TAATR, "TAATR", "TA_abort_trans",               C_RESPS,   N_THIRD,  N_ACTION, (0|m_naks|m_tnid)},
	{TAFRD, "TAFRD", "TA_forwarded_read_data",       C_RESPS,   N_THIRD,  N_ACTION, (0|m_data|m_size|m_wstt)},
	{TAFWD, "TAFWD", "TA_forwarded_write_data",      C_RESPS,   N_THIRD,  N_ACTION, (0|m_data|m_size|m_naks|m_wstt)},
	{TAFWA, "TAFWA", "TA_forwarded_write_all",       C_RESPS,   N_THIRD,  N_ACTION, (0|m_naks|m_wstt)},
	{TAIVA, "TAIVA", "TA_invalidation_ack",          C_RESPS,   N_THIRD,  N_ACTION, (0|m_naks)},
	{TAUGA, "TAUGA", "TA_upgrade_ack",               C_RESPS,   N_THIRD,  N_ACTION, (0|m_naks)},
	{TAWPN, "TAWPN", "TA_wait_pin",                  C_RESPS,   N_THIRD,  N_ACTION, (0|m_thrd)},
	{TAWTR, "TAWTR", "TA_wait_trans",                C_RESPS,   N_THIRD,  N_ACTION, (0|m_tnid)},
	{HTATR, "HTATR", "HT_abort_trans",               C_THIRD,    N_HOME,   N_THIRD, (0|m_tnid|m_anod|m_naks|m_tnid)},
	{HTARF, "HTARF", "HT_abort_trans_read_forward",  C_THIRD,    N_HOME,   N_THIRD, (0|m_tnid|m_cgid|m_key_|m_anod|m_naks|m_tnid)},
	{HTAWF, "HTAWF", "HT_abort_trans_write_forward", C_THIRD,    N_HOME,   N_THIRD, (0|m_tnid|m_cgid|m_key_|m_anod|m_naks|m_tnid)},
	{HTDEL, "HTDEL", "HT_delete",                    C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_naks|m_wstt|m_tran|m_tnid|m_pin_|m_thrd)},
	{HTFLS, "HTFLS", "HT_flush",                     C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_)},
	{HTFLI, "HTFLI", "HT_flush_inval",               C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_)},
	{HTFLC, "HTFLC", "HT_flush_container",           C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid)},
	{HTICD, "HTICD", "HT_inval_container_delayed",   C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_invt)},
	{HTINV, "HTINV", "HT_invalidate",                C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_naks|m_wstt|m_tran|m_tnid|m_pin_|m_thrd)},
	{HTUGR, "HTUGR", "HT_upgrade",                   C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_naks|m_tran|m_tnid|m_pin_|m_thrd)},
	{HTRDF, "HTRDF", "HT_read_forward",              C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_wstt|m_tran|m_tnid|m_pin_|m_thrd)},
	{HTWRF, "HTWRF", "HT_write_forward",             C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_naks|m_wstt|m_tran|m_tnid|m_pin_|m_thrd)},
	{HTWAF, "HTWAF", "HT_write_all_forward",         C_THIRD,    N_HOME,   N_THIRD, (0|m_cgid|m_key_|m_anod|m_naks|m_wstt|m_tran|m_tnid|m_pin_|m_thrd)},
	{AHCND, "AHCND", "AH_castout_no_data",           C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_)},
	{AHCWD, "AHCWD", "AH_castout_with_data",         C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_data|m_size|m_expt|m_crtt|m_rlst)},
	{AHCOB, "AHCOB", "AH_create_object",             C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_size|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_rlst)},
	{AHCOP, "AHCOP", "AH_create_object_put",         C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_size|m_data|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_rlst)},
	{AHDOB, "AHDOB", "AH_delete_object",             C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ncac|m_tran|m_tnid|m_rlst)},
	{AHFLS, "AHFLS", "AH_flush",                     C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_rlst)},
	{AHFLI, "AHFLI", "AH_flush_inval",               C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_rlst)},
	{AHFLD, "AHFLD", "AH_flush_with_data",           C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_data|m_size|m_rlst)},
	{AHFID, "AHFID", "AH_flush_inval_with_data",     C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_data|m_size|m_rlst)},
	{AHFLC, "AHFLC", "AH_flush_container",           C_REQST,  N_ACTION,    N_HOME, (0|m_cgid)},
	{AHFIC, "AHFIC", "AH_flush_inval_container",     C_REQST,  N_ACTION,    N_HOME, (0|m_cgid)},
	{AHGCN, "AHGCN", "AH_get_complete_no_data",      C_FINAL,  N_ACTION,    N_HOME, (0|m_slst)},
	{AHGCD, "AHGCD", "AH_get_complete_with_data",    C_FINAL,  N_ACTION,    N_HOME, (0|m_slst|m_data|m_size|m_rlst)},
	{AHGTR, "AHGTR", "AH_get_to_read",               C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_invt)},
	{AHGRR, "AHGRR", "AH_get_to_read_range",         C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ofst|m_size|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_invt)},
	{AHGTW, "AHGTW", "AH_get_to_write",              C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_invt)},
	{AHGWR, "AHGWR", "AH_get_to_write_range",        C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ofst|m_size|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_invt)},
	{AHGWN, "AHGWN", "AH_get_wait_no_data",          C_FINAL,  N_ACTION,    N_HOME, (0|m_slst)},
	{AHGWD, "AHGWD", "AH_get_wait_with_data",        C_FINAL,  N_ACTION,    N_HOME, (0|m_slst|m_data|m_size|m_rlst)},
	{AHGCS, "AHGCS", "AH_get_container_stat",        C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_stky)},
	{AHICD, "AHICD", "AH_inval_container_delayed",   C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_invt)},
	{AHLKR, "AHLKR", "AH_lock_read",                 C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHLRN, "AHLRN", "AH_lock_read_noblock",         C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHLKW, "AHLKW", "AH_lock_write",                C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHLWN, "AHLWN", "AH_lock_write_noblock",        C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHPTA, "AHPTA", "AH_put_all",                   C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_data|m_size|m_rlst)},
	{AHSOB, "AHSOB", "AH_set_object",                C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_size|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_rlst)},
	{AHSOP, "AHSOP", "AH_set_object_put",            C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_size|m_data|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_rlst)},
	{AHULR, "AHULR", "AH_unlock_read",               C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHULW, "AHULW", "AH_unlock_write",              C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_thrd)},
	{AHUNP, "AHUNP", "AH_unpin",                     C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_)},
	{AHUPG, "AHUPG", "AH_upgrade",                   C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_)},
	{AHUGC, "AHUGC", "AH_upgrade_complete",          C_FINAL,  N_ACTION,    N_HOME, (0)},
	{FHCRC, "FHCRC", "FH_create_complete",           C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHCRF, "FHCRF", "FH_create_failed",             C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{FHSTC, "FHSTC", "FH_set_complete",              C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHSTF, "FHSTF", "FH_set_failed",                C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{FHDAT, "FHDAT", "FH_data",                      C_NONE_,   N_FLASH,    N_HOME, (0|m_size|m_data)},
	{FHDEC, "FHDEC", "FH_delete_complete",           C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHDEF, "FHDEF", "FH_delete_failed",             C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{FHGTF, "FHGTF", "FH_get_failed",                C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{FHGSC, "FHGSC", "FH_get_container_stat_complete",  C_NONE_,   N_FLASH,    N_HOME, (0|m_ctst)},
	{FHGSF, "FHGSF", "FH_get_container_stat_failed", C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHICC, "FHICC", "FH_inval_container_delayed_complete",  C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHICF, "FHICF", "FH_inval_container_delayed_failed",  C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHPTC, "FHPTC", "FH_put_complete",              C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHPTF, "FHPTF", "FH_put_failed",                C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{FHXST, "FHXST", "FH_exists",                    C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHNXS, "FHNXS", "FH_not_exists",                C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHENM, "FHENM", "FH_enum",                      C_NONE_,   N_FLASH,    N_HOME, (0|m_key_|m_size|m_enpt)},
	{FHENF, "FHENF", "FH_enum_failed",               C_NONE_,   N_FLASH,    N_HOME, (0)},
	{HRCRE, "HRCRE", "HR_create",                    C_REPLI,    N_HOME, N_REPLICA, (0|m_cgid|m_key_|m_data|m_size)},
	{HRREP, "HRREP", "HR_replicate",                 C_REPLI,    N_HOME, N_REPLICA, (0|m_cgid|m_key_|m_data|m_size)},
	{HRDEL, "HRDEL", "HR_delete",                    C_REPLI,    N_HOME, N_REPLICA, (0|m_cgid|m_key_)},
	{HRSET, "HRSET", "HR_set",                       C_REPLI,    N_HOME, N_REPLICA, (0|m_cgid|m_key_|m_data|m_size)},
	{RHREC, "RHREC", "RH_replication_complete",      C_AKREP, N_REPLICA,    N_HOME, (0)},
	{THDAK, "THDAK", "TH_delete_ack",                C_RESPS,   N_THIRD,    N_HOME, (0)},
	{THFCC, "THFCC", "TH_flush_container_complete",  C_RESPS,   N_THIRD,    N_HOME, (0)},
	{THFLD, "THFLD", "TH_flush_data",                C_RESPS,   N_THIRD,    N_HOME, (0|m_data|m_size)},
	{THIDC, "THIDC", "TH_inval_container_delayed_complete",  C_RESPS,   N_THIRD,    N_HOME, (0)},
	{THWPN, "THWPN", "TH_wait_pin",                  C_RESPS,   N_THIRD,    N_HOME, (0|m_thrd)},
	{THWTR, "THWTR", "TH_wait_trans",                C_RESPS,   N_THIRD,    N_HOME, (0|m_tnid)},
	{AHGWA, "AHGWA", "AH_get_to_write_all",          C_REQST,  N_ACTION,    N_HOME, (0|m_cgid|m_key_|m_ncac|m_tran|m_tnid|m_pin_|m_thrd|m_invt)},
	{HFGFF, "HFGFF", "HF_get",                       C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{HFPTF, "HFPTF", "HF_put",                       C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_|m_size|m_data)},
	{HFCIF, "HFCIF", "HF_create",                    C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_|m_size|m_data)},
	{HFSET, "HFSET", "HF_set",                       C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_|m_size|m_data)},
	{HFCZF, "HFCZF", "HF_create_zero",               C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_|m_size)},
	{HFDFF, "HFDFF", "HF_delete",                    C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{HFXST, "HFXST", "HF_exists",                    C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{HFSEN, "HFSEN", "HF_start_enum",                C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid)},
	{HFNEN, "HFNEN", "HF_next_enum",                 C_NONE_,    N_HOME,   N_FLASH, (0|m_enpt)},
	{HFGCS, "HFGCS", "HF_get_container_stat",        C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_stky)},
	{HFICD, "HFICD", "HF_inval_container_delayed",   C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_invt)},

	// Added for writeback caching

	{HFFLS, "HFFLS", "HF_flush_obj",                 C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{FHFCC, "FHFCC", "FH_flush_obj_complete",        C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHFCF, "FHFCF", "FH_flush_obj_failed",          C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{HFFIV, "HFFIV", "HF_flush_inval_obj",           C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{FHFIC, "FHFIC", "FH_flush_inval_obj_complete",  C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHFIF, "FHFIF", "FH_flush_inval_obj_failed",    C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{HFINV, "HFINV", "HF_inval_obj",                 C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{FHINC, "FHINC", "FH_inval_obj_complete",        C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHINF, "FHINF", "FH_inval_obj_failed",          C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{HFPBD, "HFPBD", "HF_prefix_delete",             C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid|m_key_)},
	{FHPBC, "FHPBC", "FH_prefix_delete_complete",    C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHPBF, "FHPBF", "FH_prefix_delete_failed",      C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},

	{HFFLC, "HFFLC", "HF_flush_container",           C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid)},
	{FHLCC, "FHLCC", "FH_flush_ctnr_complete",       C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHLCF, "FHLCF", "FH_flush_ctnr_failed",         C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{HFFLI, "HFFLI", "HF_flush_inval_container",     C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid)},
	{FHLIC, "FHLIC", "FH_flush_inval_ctnr_complete", C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHLIF, "FHLIF", "FH_flush_inval_ctnr_failed",   C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},
	{HFINC, "HFINC", "HF_inval_container",           C_NONE_,    N_HOME,   N_FLASH, (0|m_cgid)},
	{FHCIC, "FHCIC", "FH_inval_ctnr_complete",       C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHCIF, "FHCIF", "FH_inval_ctnr_failed",         C_NONE_,   N_FLASH,    N_HOME, (0|m_stat)},

	// End of additions for writeback caching

/* the next formats are NOT automatically generated! */
        {FHCSC, "FHCSC", "FH_create_shard_complete",     C_FINAL,  N_FLASH,    N_HOME, (0)},
        {FHCSF, "FHCSF", "FH_create_shard_failed",       C_FINAL,  N_FLASH,    N_HOME, (0|m_stat)},
        {FHSSC, "FHSSC", "FH_sync_shard_complete",       C_FINAL,  N_FLASH,    N_HOME, (0)},
        {FHSSF, "FHSSF", "FH_sync_shard_failed",         C_FINAL,  N_FLASH,    N_HOME, (0|m_stat)},
        {FHDSC, "FHDSC", "FH_delete_shard_complete",     C_FINAL,  N_FLASH,    N_HOME, (0)},
        {FHDSF, "FHDSF", "FH_delete_shard_failed",       C_FINAL,  N_FLASH,    N_HOME, (0|m_stat)},
        {FHGLC, "FHGLC", "FH_get_last_seq_complete",     C_FINAL,  N_FLASH,    N_HOME, (0)},
        {FHGLF, "FHGLF", "FH_get_last_seq_failed",       C_FINAL,  N_FLASH,    N_HOME, (0|m_stat)},
        {FHSCC, "FHSCC", "HF_scan_obj_seq_complete",     C_FINAL,  N_HOME,   N_REPLICA, (0|m_stat)},
        {FHSCF, "FHSCF", "HF_scan_obj_seq_failed",       C_FINAL,  N_HOME,   N_REPLICA, (0|m_stat)},
        {FHGIC, "FHGIC", "FH_get_iter_cursors_complete", C_FINAL,  N_REPLICA,N_HOME,	(0|m_stat|m_data|m_size)},
        {FHGNC, "FHGNC", "FH_get_seqno_complete",	 C_FINAL,  N_REPLICA,N_HOME,	(0|m_stat)},
        {FHGNF, "FHGNF", "FH_get_seqno_failed", 	 C_FINAL,  N_REPLICA,N_HOME,	(0|m_stat)},
        {FHGIF, "FHGIF", "FH_get_iter_cursors_failed",	 C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHGCC, "FHGCC", "FH_get_by_cursor_complete",	 C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat|m_key_|m_data|m_size|m_seqno)},
        {FHGCF, "FHGCF", "FH_get_by_cursor_failed",	 C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHSRC, "FHSRC", "FH_start_replicating_complete",C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHSRF, "FHSRF", "FH_start_replicating_failed",	 C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHSPC, "FHSPC", "FH_stop_replicating_complete", C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHSPF, "FHSPF", "FH_stop_replicating_failed",   C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHFLC, "FHFLC", "FH_flush_all_complete",        C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHFLF, "FHFLF", "FH_flush_all_failed",          C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHRVC, "FHRVC", "FH_release_vip_complete",      C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHRVF, "FHRVF", "FH_release_vip_failed",        C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        {FHNPC, "FHNPC", "FH_noop_complete"       ,      C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat|m_crtt|m_data|m_size)},
        {FHNPF, "FHNPF", "FH_noop_failed",               C_FINAL,  N_REPLICA,N_HOME, 	(0|m_stat)},
        /* Data: SDF_shard_meta_t */
        {HFCSH, "HFCSH", "HF_create_shard",              C_REQST,  N_HOME,   N_REPLICA,  (0|m_cgid|m_data)},
        {HFSSH, "HFSSH", "HF_sync_shard",                C_REQST,  N_HOME,   N_REPLICA,  (0|m_shard)},
        {HFDSH, "HFDSH", "HF_delete_shard",              C_REQST,  N_HOME,   N_REPLICA, (0|m_shard)},
        {HFGLS, "HFGLS", "HF_get_last_seq_num",          C_REQST,  N_HOME,   N_REPLICA, (0|m_shard)},
        {HFGSN, "HFGSN", "HF_get_seq_num",               C_REQST,  N_HOME,   N_REPLICA, (0|m_shard)},
        {HFSRR, "HFSRR", "HF_start_replicating",         C_REQST,  N_HOME,   N_REPLICA, (0|m_shard|m_data)},
        {HFSPR, "HFSPR", "HF_stop_replicating",          C_REQST,  N_HOME,   N_REPLICA, (0|m_shard)},
        {HFGIC, "HFGIC", "HF_get_iter_cursors",          C_REQST,  N_HOME,   N_REPLICA, (0|m_shard|m_seqno|m_seqno_len|m_seqno_max|m_data|m_size)},
        {HFGBC, "HFGBC", "HF_get_by_cursor", 	         C_REQST,  N_HOME,   N_REPLICA, (0|m_shard|m_key_|m_data|m_size)},
        {HFSCN, "HFSCN", "HF_scan_obj_seq",              C_REQST,  N_HOME,   N_REPLICA, (0|m_shard|m_key_|m_data|m_size)},
        {HFFLA, "HFFLA", "HF_flush_all",                 C_REQST,  N_HOME,   N_REPLICA, (0|m_shard)},
        {HFRVG, "HFRVG", "HF_release_vip_group",         C_REQST,  N_HOME,   N_REPLICA, (0)},
        {HFNOP, "HFNOP", "HF_noop",                      C_REQST,  N_HOME,   N_REPLICA, (0)},
        /* Lamport clock */
        {HFOSH, "HFOSH", "HF_open_shard",                C_REPLI,  N_REPLICA,N_HOME,    (0|m_data)},
        {FHOSC, "FHOSC", "FH_open_shard_complete",       C_FINAL,  N_HOME,   N_REPLICA, (0|m_stat|m_data)},
        {FHOSF, "FHOSF", "FH_open_shard_failed",         C_FINAL,  N_HOME,   N_REPLICA, (0|m_stat)},
        /* For meta storage */
        {MMCSM, "MMCSM", "MM_createshard_meta",            C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard|m_data)},
        {MMGSM, "MMGSM", "MM_get_shard_meta",            C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard)},
        {MMPSM, "MMPSM", "MM_put_shard_meta",            C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard|m_data)},
        {MMDSM, "MMDSM", "MM_delete_shard_meta",         C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard|m_data)},
        {MMRSM, "MMRSM", "MM_return_shard_meta",         C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard|m_data)},
        {MMSMC, "MMSMC", "MM_shard_meta_changed",       C_REPLI,  N_REPLICA,N_REPLICA, (0|m_shard|m_data)},
        /* for action_new.c */
	{FHXXP, "FHXXP", "FH_exist_but_expired",         C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHGXP, "FHGXP", "FH_get_data_but_expired",      C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHGDF, "FHGDF", "FH_get_delete_failed",         C_NONE_,   N_FLASH,    N_HOME, (0)},
	{FHXDF, "FHXDF", "FH_exist_delete_failed",       C_NONE_,   N_FLASH,    N_HOME, (0)},
    	{AHSYN, "AHSYN", "AH_sync_to_flash",             C_REQST,   N_ACTION,  N_HOME,   (0|m_cgid)},
	{HASYC, "HASYC", "HA_sync_complete",             C_RESPS,   N_HOME,    N_ACTION, (0)},
	{HASYF, "HASYF", "HA_sync_failed",               C_RESPS,   N_HOME,    N_ACTION, (0)},
        /* Fast recovery */
        {HFFGB, "HFFGB", "HF_fastr_get_flash_bitmap",    C_REQST,  N_HOME,   N_REPLICA, (0)},
        {HFFGC, "HFFGC", "HF_fastr_get_cache_data",      C_REQST,  N_HOME,   N_REPLICA, (0)},
        {HFFGD, "HFFGD", "HF_fastr_get_flash_data",      C_REQST,  N_HOME,   N_REPLICA, (0)},
        {HFFSX, "HFFSX", "HF_fastr_set_indices",         C_REQST,  N_HOME,   N_REPLICA, (0)},
        {HFFRC, "HFFRC", "HF_fastr_complete",            C_REQST,  N_HOME,   N_REPLICA, (0)},
        /* Dummy */
	{ZDUMY, "ZDUMY", "dummy_msg",                    C_NONE_,    N_NONE,    N_NONE, (0)},
    };
#endif

typedef enum {
    f_trans	     = (1<<0),
    f_pin	     = (1<<1),
    f_cacheable	     = (1<<2),
    f_coherent	     = (1<<3),
    f_shared	     = (1<<4),
    f_writethru      = (1<<5),
    f_range	     = (1<<6),
    f_wstate	     = (1<<7),
    f_key_overflow   = (1<<8),
    f_lock_ctnr      = (1<<9),
    f_block_ctnr     = (1<<10),
    f_object_ctnr    = (1<<11),
    f_tombstone      = (1<<12),
    f_write_if_newer = (1<<13),
    /** @brief Perform sync operation before or after op (context dependent) */
    f_sync           = (1<<14),
    f_open_ctnr      = (1<<15),
    /** @brief Necessary locks are already held by the replication code */
    f_internal_lock  = (1<<16)
} SDF_protocol_msg_flags_t;

#define N_SDF_PROTOCOL_MSG_FLAGS  17

#ifndef _PROTOCOL_COMMON_C
    extern SDF_protocol_fmt_info_t SDF_Protocol_Msg_Flags_Info[];
#else
    SDF_protocol_fmt_info_t SDF_Protocol_Msg_Flags_Info[N_SDF_PROTOCOL_MSG_FLAGS] =
    {
        {f_trans,           "trans"},
        {f_pin,             "pin"},
        {f_cacheable,       "cacheable"},
        {f_coherent,        "coherent"},
        {f_shared,          "shared"},
        {f_writethru,       "writethru"},
        {f_range,           "range"},
        {f_wstate,          "wstate"},
        {f_key_overflow,    "key_overflow"},
        {f_lock_ctnr,       "lock_ctnr"},
        {f_block_ctnr,      "block_ctnr"},
        {f_object_ctnr,     "object_ctnr"},
        {f_tombstone,       "tombstone"},
        {f_write_if_newer,  "write_if_newer"},
        {f_sync,            "sync"},
        {f_open_ctnr,       "open_ctnr"},
        {f_internal_lock,   "internal_lock"}
    };
#endif

#define SDF_PROTOCOL_MAX_REPLICAS   5

/*  Cache state enums are here in protocol_common.h because they are needed
 *  in the statelist returned via some protocol messages.
 */

typedef enum {CACHE_M=0, CACHE_O, CACHE_S, CACHE_I,
              CACHE_C, CACHE_D} CacheDirStates;
typedef enum {CACHE_E=0, CACHE_N, CACHE_NO_EN} CacheENState;
typedef enum {CACHE_P=0, CACHE_NO_P} CachePState;
typedef enum {CACHE_T=0, CACHE_NO_T} CacheTState;
typedef enum {CACHE_W=0, CACHE_NO_W} CacheWState;

#ifndef _PROTOCOL_COMMON_C
    extern char CacheDirStateNames[];
#else
    char CacheDirStateNames[] = {'M', 'O', 'S', 'I', 'C', 'D', ' ' };
#endif

typedef struct {
    SDF_vnode_t             node;
    SDF_cacheid_t           cache;
    SDF_thrdid_t            thrd;
    SDF_transid_t           transid;
    CacheDirStates          state;
} SDF_protocol_statelist_t;

#define PROTOCOL_MSG_VERSION 1

/** @brief SDF protocol message common to action, home, flash, replication */
typedef struct SDF_protocol_msg {
    uint16_t                     current_version;
    uint16_t		         supported_version;
    SDF_protocol_msg_type_t      msgtype;
    uint32_t			 flags;
    SDF_status_t           	 status;
    /** @brief Tag originally assigned to request in action thread code */
    SDF_tag_t                    tag;
    SDF_vnode_t                  node_from;
    SDF_vnode_t                  node_to;
    SDF_vnode_t                  action_node;
    SDF_cacheid_t                cache;
    /** @brief Action node thread */
    SDF_context_t                thrd;
    SDF_transid_t		 transid;
    SDF_cguid_t			 cguid;
    SDF_simple_key_t             key __attribute__ ((aligned (FLASH_ALIGNMENT_LEN)));
    uint32_t			 n_acks;
    uint32_t                     req_status;
    uint64_t			 flash_ptr;
    uint64_t			 enum_ptr;
    uint32_t                     n_replicas;
    SDF_vnode_t                  replicas[SDF_PROTOCOL_MAX_REPLICAS];
    uint32_t			 statelist_len;
    SDF_size_t			 overflow_key_size;
    SDF_size_t			 data_offset;
    SDF_size_t			 data_size;
    SDF_shardid_t                shard;
    uint32_t                     shard_count; // num shards for this container
    SDF_time_t                   flushtime;
    SDF_time_t                   exptime;
    SDF_time_t                   createtime;
    SDF_time_t                   curtime;
    uint64_t                     seqno;
    uint64_t                     seqno_len;
    uint64_t                     seqno_max;
    uint32_t                     stat_key;
    uint64_t                     stat;
    uint64_t                     cookie1;
    uint64_t                     cookie2;
    struct sdf_replication_op_meta op_meta;
    char                         cname[MAX_CNAME_SIZE + 1];
} SDF_protocol_msg_t;


/*
 * A tiny version of the SDF_protocol_msg header.  We only keep as much
 * information as necessary to include the type.
 */
typedef struct {
    uint16_t                cur_ver;    /* Current version */
    uint16_t                sup_ver;    /* Supported version */
    SDF_protocol_msg_type_t type;       /* Message type */
} SDF_protocol_msg_tiny_t;


extern void initCommonProtocolStuff();
extern void SDFPrintProtocolMsg(FILE *f, SDF_protocol_msg_t *pm);
extern char *SDFsPrintProtocolMsg(char *sout, SDF_protocol_msg_t *pm, int strSize);
extern char *SDFsPrintAppReq(char *sout, SDF_appreq_t *pm, int strSize, SDF_boolean_t show_retvals);
extern SDF_Protocol_Msg_Info_t *getProtocolMsgInfo();
extern char *SDFStatusString(SDF_status_t status);
extern void SDFBlknumToKey(SDF_simple_key_t *pkey, uint64_t blknum);
extern SDF_status_t SDFObjnameToKey(SDF_simple_key_t *pkey, char *objname, uint32_t keylen);

#ifdef	__cplusplus
}
#endif

#endif /* _PROTOCOL_COMMON_H */
