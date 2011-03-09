/*
 * File:   sdf_sm_msg.h
 * Author: Darpan Dinker
 *
 * Created on February 24, 2008, 8:21 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_sm_msg.h 12490 2010-03-24 21:07:22Z briano $
 */

#ifndef _SDF_SM_MSG_H
#define _SDF_SM_MSG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "container_props.h"
#include "container.h"
#include "sdfappcommon/XMbox.h"

#define MAXCONTAINERPATHSIZE 256 // TODO defined somewhere else too

/**
 * Module identifier for protocol handler
 * @see agent/protocol_handler.h
 */
typedef enum {
    MODULE_CONTAINER_API = 1, MODULE_OBJECT_API, MODULE_STATS_API
} SDF_SM_module_t;

/**
 * Method identifier for container message handler
 * @see agent/message_handler_container.h
 */
typedef enum {
    CONTAINER_CREATE = 1, CONTAINER_OPEN, CONTAINER_CLOSE, CONTAINER_DELETE,
    CONTAINER_RENAME, CONTAINER_COPY, CONTAINER_MOVE, CONTAINER_LIST,
    CONTAINER_IMPORT, CONTAINER_EXPORT, CONTAINER_ENUMERATE, CONTAINER_EXISTS,
    CONTAINER_STAT, CONTAINER_PING
} SDF_SM_container_msg_t;

/**
 * @brief Arguments for method SDFCreateContainer()
 *
 * Used for all types of containers.
 * @see SDF_container_props_t
 */
typedef struct {
    char path[MAXCONTAINERPATHSIZE];
    SDF_container_props_t properties;
} SDF_ARG_container_create;

/**
 * @brief Arguments for method SDFOpenContainer()
 *
 * Used for all types of containers.
 */
typedef struct {
    char path[MAXCONTAINERPATHSIZE];
    SDF_container_mode_t mode;
    SDF_CONTAINER retContainerPtr;
} SDF_ARG_container_open;

/**
 * @brief Arguments for method SDFCloseContainer()
 *
 * Used for all types of containers.
 */
typedef struct {
    SDF_CONTAINER container;
} SDF_ARG_container_close;

/**
 * @brief Arguments for method SDFDeleteContainer()
 *
 * Used for all types of containers.
 */
typedef struct {
    char path[MAXCONTAINERPATHSIZE];
} SDF_ARG_container_delete;

/**
 * @brief Arguments for method SDFEnumerateObjects()
 *
 * Used for object containers.
 * @see SDF_object_info_t
 */
typedef struct {
    SDF_CONTAINER container;
    char_sp_t info_blob;            // Serialized set of objects (count, info0, info1, ...)
    uint32_t info_len;
} SDF_ARG_enumerate;

/**
 * @brief Arguments for method SDFContainerExists()
 *
 * Used for all types of containers.
 */
typedef struct {
    char path[MAXCONTAINERPATHSIZE];
} SDF_ARG_container_exists;

/**
 * @brief Arguments for method SDFContainerStat()
 *
 * Used for all types of containers.
 */
typedef struct {
    SDF_CONTAINER container;
    int key;
    uint64_t stat;
} SDF_ARG_container_stat;

typedef enum {
    APATR=0, APCST, APCTR, APCOB, APCOP, APDOB, APFLS, APFLI, APFCA, APFCO,
    APFIC,   APGRD, APGRR, APGWT, APGWR, APPTR, APPTA, APLRD, APLRN, APLWR,
    APLWN,   APSTR, APSYN, APSYA, APSAP, APSYP, APSYR, APSRA, APSRO, APSRP,
    APULR,   APULW, APUPN, APGWI, APGRP, APGWP, APGRT, APDCT, APNCT, APCOE,
    APPAE,   APSOE, APGRE, APICD, APGCB, APFCB, APGCS, APRDD, APWRD, APGSS,
    APGFS,   APGNC, APSCT, APCPI, APSPE, APGWE, APFCI, APSOB, APSOP, APCPE,
    APCCC,   APCCE, APGRX, APGWX, APCOS, APGRS, APPTS, APSOS, APICO, APICA,
    APINV,   APICH, APDBE, APSYC, APGIT, APRIV, APRUP, APRFL, APRFI, APDUM,

    N_SDF_APP_REQS
} SDF_app_request_type_t;

typedef uint32_t  SDF_protocol_req_fmt_t;

typedef enum {
    a_ctnr 	   = (1<<0), 
    a_key_	   = (1<<1), 
    a_size 	   = (1<<2), 
    a_asiz 	   = (1<<3), 
    a_ofst	   = (1<<4), 
    a_msiz    	   = (1<<5),
    a_pdat     	   = (1<<6), 
    a_opid         = (1<<7), 
    a_nops         = (1<<8), 
    a_pbfi         = (1<<9), 
    a_pbfo         = (1<<10), 
    a_pctx         = (1<<11), 
    a_ctxt         = (1<<12), 
    a_popd         = (1<<13), 
    a_succ         = (1<<14), 
    a_ppbf         = (1<<15), 
    a_nbyt         = (1<<16), 
    a_expt         = (1<<17), 
    a_curt         = (1<<18), 
    a_pexp         = (1<<19), 
    a_invt         = (1<<20), 
    a_pcbf         = (1<<21), 
    a_ppcb         = (1<<22), 
    a_skey         = (1<<23), 
    a_pstt         = (1<<24), 
    a_psbo         = (1<<25), 
    a_ppsi         = (1<<26), 
    a_psbi         = (1<<27), 
    a_vnod         = (1<<28), 
    a_pden         = (1<<29), 

} SDF_protocol_req_fields_t;

#define N_SDF_PROTOCOL_REQ_FMT_FIELDS  30

typedef struct {
   uint64_t                  field;
   char		             *name;
} SDF_protocol_fmt_info_t;

#ifndef _INSTANTIATE_REQ_FMT_INFO
    extern SDF_protocol_fmt_info_t SDF_Protocol_Req_Fmt_Info[];
#else
    SDF_protocol_fmt_info_t SDF_Protocol_Req_Fmt_Info[] = 
    {
        {a_ctnr,    "ctnr"},
        {a_key_,    "key"},
        {a_size,    "size"},
        {a_asiz,    "actual_size"},
        {a_ofst,    "offset"},
        {a_msiz,    "max_size"},
        {a_pdat,    "ppdata"},
        {a_opid,    "opids[]"},
        {a_nops,    "n_opids"},
        {a_pbfi,    "pbuf_in"},
        {a_pbfo,    "pbuf_out"},
	{a_pctx,    "p_ctxt"},
	{a_ctxt,    "ctxt"},
	{a_popd,    "p_opid"},
	{a_succ,    "p_success"},
	{a_ppbf,    "ppbuf_in"},
	{a_nbyt,    "nbytes"},
	{a_expt,    "exptime"},
	{a_curt,    "curtime"},
	{a_pexp,    "pexptme"},
	{a_invt,    "invtime"},
	{a_pcbf,    "pcbuf"},
	{a_ppcb,    "ppcbf"},
	{a_skey,    "stat_Key"},
	{a_pstt,    "pstat"},
	{a_psbo,    "pshbuf_out"},
	{a_ppsi,    "ppshbuf_in"},
	{a_psbi,    "pshbf_in"},
	{a_vnod,    "vnode"},
	{a_pden,    "prefix_del"},
    };
#endif

typedef struct {
   int     	           msgtype;
   char		          *shortname;
   char		          *name;
   int                     handopt;
   int                     trans;
   int                     pin;
   int                     bypass_cache;
   int                     check_expiry;
   int                     set_expiry;
   SDF_protocol_req_fmt_t  format;
} SDF_App_Request_Info_t;

#ifndef _PROTOCOL_COMMON_C
    extern SDF_App_Request_Info_t SDF_App_Request_Info[];
#else
    SDF_App_Request_Info_t SDF_App_Request_Info[N_SDF_APP_REQS] = {
	{APATR, "APATR", "abort_transaction",            0, 0, 0, 1, 0, 0, (0)},
	{APCST, "APCST", "castout",                      0, 0, 0, 0, 1, 0, (0|a_ctnr|a_key_|a_curt)},
	{APCTR, "APCTR", "commit_transaction",           0, 0, 0, 1, 0, 0, (0)},
	{APCCE, "APCCE", "count_cache_entries",          0, 0, 0, 1, 0, 0, (0|a_pstt)},
	{APCCC, "APCCC", "count_ctnr_cache_entries",     0, 0, 0, 1, 0, 0, (0|a_ctnr|a_pstt)},
	{APCOB, "APCOB", "create_object",                0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_size|a_popd)},
	{APCOP, "APCOP", "create_object_put",            1, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_size|a_pbfo|a_popd)},
	{APCOS, "APCOS", "create_object_put_shmem",      0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_size|a_psbo|a_popd)},
	{APCOE, "APCOE", "create_object_put_exp",        1, 0, 0, 0, 1, 1, (0|a_ctnr|a_key_|a_size|a_pbfo|a_curt|a_expt|a_popd)},
	{APCPI, "APCPI", "create_object_pin",            0, 0, 1, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_size|a_popd)},
	{APCPE, "APCPE", "create_object_pin_exp",        1, 0, 1, 0, 1, 1, (0|a_ctnr|a_key_|a_pdat|a_size|a_curt|a_expt|a_popd)},
	{APDOB, "APDOB", "delete_object",                1, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APDBE, "APDBE", "delete_object_exp",            1, 0, 0, 0, 1, 0, (0|a_ctnr|a_key_|a_curt|a_popd|a_pden)},
	{APFLS, "APFLS", "flush",                        0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APFLI, "APFLI", "flush_inval",                  0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APFCA, "APFCA", "flush_cache",                  0, 0, 0, 1, 0, 0, (0)},
	{APFCO, "APFCO", "flush_container",              0, 0, 0, 1, 0, 0, (0|a_ctnr)},
	{APFIC, "APFIC", "flush_inval_cache",            0, 0, 0, 1, 0, 0, (0)},
	{APFCI, "APFCI", "flush_inval_container",        0, 0, 0, 1, 0, 0, (0|a_ctnr)},
	{APINV, "APINV", "inval",                        0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APRIV, "APRIV", "remote_inval",                 0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APRFL, "APRFL", "remote_flush",                 0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APRFI, "APRFI", "remote_flush_inval",           0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APRUP, "APRUP", "remote_update",                1, 0, 0, 0, 0, 1, (0|a_ctnr|a_key_|a_size|a_pbfo|a_curt|a_expt|a_popd)},
	{APICA, "APICA", "inval_cache",                  0, 0, 0, 1, 0, 0, (0)},
	{APICH, "APICH", "inval_cache_home",             0, 0, 0, 1, 0, 0, (0|a_vnod)},
	{APICO, "APICO", "inval_container",              1, 0, 0, 1, 0, 0, (0|a_ctnr)},
	{APGIT, "APGIT", "get_inval_time",               1, 0, 0, 1, 0, 0, (0|a_ctnr|a_pexp)},
	{APGCB, "APGCB", "get_cacheable_buf",            0, 0, 0, 1, 0, 0, (0|a_ppcb|a_size)},
	{APFCB, "APFCB", "free_cacheable_buf",           0, 0, 0, 1, 0, 0, (0|a_pcbf)},
	{APGCS, "APGCS", "get_container_stat",           0, 0, 0, 1, 0, 0, (0|a_ctnr|a_skey|a_pstt|a_popd)},
	{APGRD, "APGRD", "get_to_read",                  1, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfi|a_msiz|a_asiz|a_popd)},
	{APGRS, "APGRS", "get_to_read_shmem",            0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_psbi|a_msiz|a_asiz|a_popd)},
	{APGRX, "APGRX", "get_to_read_exp",              1, 0, 0, 0, 1, 0, (0|a_ctnr|a_key_|a_ppbf|a_asiz|a_curt|a_pexp|a_popd)},
	{APGRP, "APGRP", "get_to_read_pin",              1, 0, 1, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_asiz|a_popd)},
	{APGRE, "APGRE", "get_to_read_pin_exp",          1, 0, 1, 0, 1, 0, (0|a_ctnr|a_key_|a_pdat|a_asiz|a_curt|a_pexp|a_popd)},
	{APGRT, "APGRT", "get_to_read_trans",            0, 1, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_asiz|a_popd)},
	{APGRR, "APGRR", "get_to_read_range",            0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfi|a_ofst|a_size|a_popd)},
	{APGWI, "APGWI", "get_to_write",                 0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfi|a_msiz|a_asiz|a_popd)},
	{APGWX, "APGWX", "get_to_write_exp",             1, 0, 0, 0, 1, 0, (0|a_ctnr|a_key_|a_ppbf|a_asiz|a_curt|a_pexp|a_popd)},
	{APGWP, "APGWP", "get_to_write_pin",             0, 0, 1, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_asiz|a_popd)},
	{APGWE, "APGWE", "get_to_write_pin_exp",         1, 0, 1, 0, 1, 0, (0|a_ctnr|a_key_|a_pdat|a_size|a_curt|a_pexp|a_popd)},
	{APGWT, "APGWT", "get_to_write_trans",           0, 1, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_asiz|a_popd)},
	{APGWR, "APGWR", "get_to_write_range",           0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfi|a_ofst|a_size|a_popd)},
	{APICD, "APICD", "inval_container_delayed",      1, 0, 0, 1, 0, 0, (0|a_ctnr|a_curt|a_invt)},
	{APPTR, "APPTR", "put_range",                    0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfo|a_ofst|a_size|a_popd)},
	{APPTA, "APPTA", "put_all",                      1, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfo|a_size|a_popd)},
	{APPTS, "APPTS", "put_all_shmem",                0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_psbo|a_size|a_popd)},
	{APPAE, "APPAE", "put_all_exp",                  1, 0, 0, 0, 1, 1, (0|a_ctnr|a_key_|a_size|a_pbfo|a_curt|a_expt|a_popd)},
	{APLRD, "APLRD", "lock_read",                    0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APLRN, "APLRN", "lock_read_nonblock",           0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd|a_succ)},
	{APLWR, "APLWR", "lock_write",                   0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APLWN, "APLWN", "lock_write_nonblock",          0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd|a_succ)},
	{APSOB, "APSOB", "set_object",                   0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_pbfo|a_size|a_popd)},
	{APSOS, "APSOS", "set_object_shmem",             0, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_psbo|a_size|a_popd)},
	{APSOP, "APSOP", "set_object_pin",               0, 0, 1, 0, 0, 0, (0|a_ctnr|a_key_|a_pdat|a_size|a_popd)},
	{APSOE, "APSOE", "set_object_exp",               1, 0, 0, 0, 1, 1, (0|a_ctnr|a_key_|a_size|a_pbfo|a_curt|a_expt|a_popd)},
	{APSPE, "APSPE", "set_object_pin_exp",           1, 0, 1, 0, 1, 1, (0|a_ctnr|a_key_|a_pdat|a_size|a_curt|a_expt|a_popd)},
	{APSTR, "APSTR", "start_transaction",            0, 0, 0, 1, 0, 0, (0)},
	{APSYN, "APSYN", "sync",                         0, 0, 0, 1, 0, 0, (0|a_opid|a_nops)},
	{APSYA, "APSYA", "sync_all",                     0, 0, 0, 1, 0, 0, (0)},
	{APSAP, "APSAP", "sync_all_poll",                0, 0, 0, 1, 0, 0, (0|a_succ)},
	{APSYP, "APSYP", "sync_poll",                    0, 0, 0, 1, 0, 0, (0|a_opid|a_nops|a_succ)},
	{APSYR, "APSYR", "sync_replicas",                0, 0, 0, 1, 0, 0, (0|a_opid|a_nops)},
	{APSRA, "APSRA", "sync_replicas_all",            0, 0, 0, 1, 0, 0, (0)},
	{APSRO, "APSRO", "sync_replicas_all_poll",       0, 0, 0, 1, 0, 0, (0|a_succ)},
	{APSRP, "APSRP", "sync_replicas_poll",           0, 0, 0, 1, 0, 0, (0|a_opid|a_nops|a_succ)},
	{APULR, "APULR", "unlock_read",                  0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APULW, "APULW", "unlock_write",                 0, 0, 0, 1, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APUPN, "APUPN", "unpin",                        1, 0, 0, 0, 0, 0, (0|a_ctnr|a_key_|a_popd)},
	{APNCT, "APNCT", "new_context",                  0, 0, 0, 1, 0, 0, (0|a_pctx)},
	{APDCT, "APDCT", "delete_context",               0, 0, 0, 1, 0, 0, (0|a_ctxt)},

    /* these request types are NOT automatically generated! */
	{APRDD, "APRDD", "read data",                    0, 0, 0, 1, 0, 0, (0|a_ctxt|a_ctnr)},
	{APWRD, "APWRD", "write data",                   0, 0, 0, 1, 0, 0, (0|a_ctxt|a_ctnr)},
	{APGSS, "APGSS", "get container \"file\" stats", 0, 0, 0, 1, 0, 0, (0|a_ctxt|a_ctnr)},
	{APGFS, "APGFS", "get flash stats",              0, 0, 0, 1, 0, 0, (0|a_ctxt|a_ctnr)},
	{APGNC, "APGNC", "get next container",           0, 0, 0, 1, 0, 0, (0|a_ctxt)},
	{APSCT, "APSCT", "set container times",          0, 0, 0, 1, 0, 0, (0|a_ctxt|a_ctnr)},
	{APSYC, "APSYC", "sync_container",               0, 0, 0, 1, 0, 0, (0|a_ctnr)},
	{APDUM, "APDUM", "dummy message",                0, 0, 0, 0, 0, 0, (0)},
    };
#endif

typedef struct arq {
    SDF_app_request_type_t reqtype;
    SDF_context_t          ctxt;
    SDF_cguid_t            ctnr;
    SDF_simple_key_t       key;
    SDF_size_t             sze;
    SDF_size_t             offset;
    SDF_size_t             max_size;
    SDF_size_t             nbytes;
    SDF_opid_t             opid[SDF_MAX_OPIDS];
    uint32_t               n_opids;
    void *                 pbuf_in;
    void **                ppbuf_in;
    void *                 pbuf_out;
    SDF_size_t *           pactual_size;
    void **                ppdata;
    SDF_context_t *        pctxt;
    SDF_context_t          contxt;
    SDF_opid_t *           p_opid;
    SDF_boolean_t *        p_success;
    SDF_time_t             exptime;
    SDF_time_t             curtime;
    SDF_time_t *           pexptme;
    SDF_time_t             invtime;
    void *                 pcbuf;
    void **                ppcbf;
    uint32_t               stat_key;
    uint64_t *             pstat;
    uint64_t               stat_val;
    SDFCacheObj_sp_t       dest;
    SDFCacheObj_sp_t       pshbf_in;
    SDF_vnode_t            vnode;
    SDF_size_t             destLen;
    SDF_container_type_t   ctnr_type;
    ptofMbox_sp_t          mbox_buf_handshake;
    SDF_boolean_t          internal_request;
    fthThread_t           *internal_thread;
    SDF_tag_t              tag;
    SDF_boolean_t          success;
    SDF_status_t           respStatus;
    SDF_context_t          context_returned;
    struct arq            *next;
    SDF_boolean_t          prefix_delete;
} SDF_appreq_t;

#ifdef __cplusplus
}
#endif

#endif /* _SDF_SM_MSG_H */
