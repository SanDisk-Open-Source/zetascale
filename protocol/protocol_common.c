/*
 * File:   protocol_common.c
 * Author: Brian O'Krafka
 *
 * Created on April 2, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: protocol_common.c 802 2008-03-29 00:44:48Z darpan $
 */

#define _PROTOCOL_COMMON_C
#define _INSTANTIATE_REQ_FMT_INFO

/* XXX: This belongs someplace else, but that creates link order issues */
#define _INSTANTIATE_SDF_STATUS_STRINGS

#include <stdint.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

/* #include "protocol/sdf_proto_msg_types.h" */
#include "sdfmsg/sdf_msg_types.h"
/* #include "shared/container.h" xxxzzz */
/* #include "protocol/home/init_home.h" xxxzzz */

#include "platform/stdlib.h"
#include "shared/sdf_sm_msg.h"
#include "shared/name_service.h"
#include "protocol_utils.h"
#include "protocol_common.h"
#include "flash/flash.h"
/* #include "protocol/home/homedir.h" */
/* #include "sdf_lex_buf.h" */
/* #include "home_thread.h" */
/* #include "action_appreq_tbl.h" */

static int app_request_sortfn(const void *e1_in, const void *e2_in);
static int nodes_sortfn(const void *e1_in, const void *e2_in);
static int msg_sortfn(const void *e1_in, const void *e2_in);
static int print_msg_field(char *sout, int n, SDF_protocol_msg_t *pm, int strSize);
static int print_req_field(char *sout, int n, SDF_appreq_t *par, int strSize, SDF_boolean_t show_reqvals);

void initCommonProtocolStuff() __attribute__((constructor));

void initCommonProtocolStuff()
{
   int             i;
   SDF_boolean_t   quit;
   static int initialized = 0;

   if (initialized) {
       return;
   }

   quit = SDF_FALSE;

   /* sort protocol info stuff */

   qsort((void *) SDF_App_Request_Info, (size_t) N_SDF_APP_REQS, 
         (size_t) sizeof(SDF_App_Request_Info_t), app_request_sortfn);

   qsort((void *) SDF_Protocol_Nodes_Info, (size_t) N_SDF_PROTOCOL_NODES, 
         (size_t) sizeof(SDF_protocol_fmt_info_t), nodes_sortfn);

   qsort((void *) SDF_Protocol_Msg_Info, (size_t) N_SDF_PROTOCOL_MSGS, 
         (size_t) sizeof(SDF_Protocol_Msg_Info_t), msg_sortfn);

    /* check consistency of info stuff */

   for (i=0; i<N_SDF_APP_REQS; i++) {
       if (i != SDF_App_Request_Info[i].msgtype) {
           UTWarning(21337,"inconsistency in SDF_App_Request_Info: %d != [%d].msgtype", i, i); 
	   quit = SDF_TRUE;
       }
   }

   for (i=0; i<N_SDF_PROTOCOL_NODES; i++) {
       if (i != SDF_Protocol_Nodes_Info[i].field) {
           UTWarning(21338,"inconsistency in SDF_Protocol_Nodes_Info: %d != [%d].field", i, i); 
	   quit = SDF_TRUE;
       }
   }

   for (i=0; i<N_SDF_PROTOCOL_MSGS; i++) {
       if (i != SDF_Protocol_Msg_Info[i].msgtype) {
           UTWarning(21339,"inconsistency in SDF_Protocol_Msg_Info: %d != [%d].msgtype", i, i); 
	   quit = SDF_TRUE;
       }
   }
   if (quit) {
       UTError("Protocol initialization failed!");
   }

   initialized = 1;

   /* initialize common messaging stuff */
   /* xxxzzz */
}

SDF_Protocol_Msg_Info_t *getProtocolMsgInfo()
{
    return(SDF_Protocol_Msg_Info);
}

void SDFPrintProtocolMsg(FILE *f, SDF_protocol_msg_t *pm)
{
    char  s[1024];

    SDFsPrintProtocolMsg(s, pm, 1024);
    (void) fprintf(f, "%s", s);
}

char *SDFsPrintProtocolMsg(char *sout, SDF_protocol_msg_t *pm, int strSize)
{
    int                       i, j, len=0;
    SDF_Protocol_Msg_Info_t  *pmi;
    SDF_boolean_t	      first;

    pmi = &(SDF_Protocol_Msg_Info[pm->msgtype]);

    len += snprintf(sout + len, strSize-len, "Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d, Seq:%"PRIu64", ",
                   pmi->name, pmi->shortname, pm->node_from, 
		   SDF_Protocol_Nodes_Info[pmi->src].name, pm->node_to, 
		   SDF_Protocol_Nodes_Info[pmi->dest].name, pm->tag,
		   pm->seqno);

    first = SDF_TRUE;
    for (i=0; i<N_SDF_PROTOCOL_MSG_FMT_FIELDS; i++) {
        if (pmi->format & SDF_Protocol_Msg_Fmt_Info[i].field) {
	    if (!first) {
		//(void) strcat(sout, ", ");
                len += snprintf(sout + len, strSize-len, ", ");
	    }
	    len += print_msg_field(sout + len, i, pm, strSize-len);
	    first = SDF_FALSE;
	}
    }
    //(void) strcat(sout, ", flags:(");
    len += snprintf(sout + len, strSize-len, ", flags:(");

    first = SDF_TRUE;
    for (j=0; j<N_SDF_PROTOCOL_MSG_FLAGS; j++) {
        if (pm->flags & SDF_Protocol_Msg_Flags_Info[j].field) {
	    if (!first) {
		//strcat(sout, "|");
                len += snprintf(sout + len, strSize-len, "|");
	    }
	    //strcat(sout, SDF_Protocol_Msg_Flags_Info[j].name);
            len += snprintf(sout + len, strSize-len, "%s", SDF_Protocol_Msg_Flags_Info[j].name);
	    first = SDF_FALSE;
	}
    }
    //strcat(sout, ")");
    len += snprintf(sout + len, strSize-len, ")");
    return(sout);
}

#define MAX_TMP_KEY_STRING_LEN  257  // Max key size + 1 for null terminator

char *protoKeyToString(int block_key, char *sout, int len, char *key)
{
    int   len2;

    if (block_key) {
        (void) sprintf(sout, "%lld", *((uint64_t *) key));
    } else {
	len2 = (len >= MAX_TMP_KEY_STRING_LEN) ? MAX_TMP_KEY_STRING_LEN - 1: len;
	(void) strncpy(sout, (char *) key, len2);
	sout[len2] = '\0';
    }
    return(sout);
}

static int print_msg_field(char *sout, int n, SDF_protocol_msg_t *pm, int strSize)
{
    int       i, len = 0;
    char     *s = sout;

    //len = strlen(sout);
    //s = &(sout[len]);
    switch (SDF_Protocol_Msg_Fmt_Info[n].field) {
	case m_mtyp:
	case m_tag_:
	case m_fnod:
	case m_tnod:
	case m_pin_:  /* this is covered by flags */
	case m_tran:  /* this is covered by flags */
	case m_wstt:  /* this is covered by flags */
	    /* purposefully empty */
	    break;

	case m_thrd: len += snprintf(s+len, strSize-len, "thrd:%"PRIu64, pm->thrd); break;
	case m_ncac: len += snprintf(s+len, strSize-len, "cache:%d", pm->cache); break;
	case m_tnid: len += snprintf(s+len, strSize-len, "transID:%"PRIu64, pm->transid); break;
	case m_cgid: len += snprintf(s+len, strSize-len, "cguid:%"PRIu64, pm->cguid); break;
	case m_key_: len += snprintf(s+len, strSize-len, "key:'%s'", pm->key.key); break;
	case m_size: len += snprintf(s+len, strSize-len, "size:%"PRIu64, pm->data_size); break;
	case m_ofst: len += snprintf(s+len, strSize-len, "offset:%"PRIu64, pm->data_offset); break;
	case m_naks: len += snprintf(s+len, strSize-len, "n_acks:%d", pm->n_acks); break;
	case m_fmpt: len += snprintf(s+len, strSize-len, "flashptr:%"PRIu64, pm->flash_ptr); break;
	case m_anod: len += snprintf(s+len, strSize-len, "action_node:%d", pm->action_node); break;
	case m_stat: len += snprintf(s+len, strSize-len, "status:%d", pm->status); break;
	case m_rstt: len += snprintf(s+len, strSize-len, "req_status:%d", pm->req_status); break;
	case m_enpt: len += snprintf(s+len, strSize-len, "enum_ptr:%"PRIu64, pm->enum_ptr); break;

	case m_seqno:     len += snprintf(s+len, strSize-len, "seqno:%"PRIu64, pm->seqno); break;
	case m_seqno_len: len += snprintf(s+len, strSize-len, "seqno_len:%"PRIu64, pm->seqno_len); break;
	case m_seqno_max: len += snprintf(s+len, strSize-len, "seqno_max:%"PRIu64, pm->seqno_max); break;

	case m_rlst: 
	    if (pm->n_replicas > 0) {
		len += snprintf(s+len, strSize-len, "replicas:{"); 
		for (i=0; i<pm->n_replicas; i++) {
		    if (i != 0) {
			len += snprintf(s+len, strSize-len, ","); 
		    }
		    len += snprintf(s+len, strSize-len, "%d", pm->replicas[i]); 
		}
		len += snprintf(s+len, strSize-len, "}"); 
	    }
	    break;

	case m_slst: 
	    len += snprintf(s+len, strSize-len, "statelist: TODO xxxzzz"); 
	    break;

	case m_data: 
   	    len += snprintf(s+len, strSize-len, "data: TODO xxxzzz"); 
	    break;

	case m_shard: 
   	    len += snprintf(s+len, strSize-len, "shard: %lu", pm->shard); 
	    break;

	case m_expt: 
   	    len += snprintf(s+len, strSize-len, "exptime: %lld", (unsigned long long int) pm->exptime); 
	    break;

	case m_invt: 
   	    len += snprintf(s+len, strSize-len, "invtime: %lld", (unsigned long long int) pm->flushtime); 
	    break;

	case m_stky: 
   	    len += snprintf(s+len, strSize-len, "stat_key: %d", pm->stat_key); 
	    break;

	case m_ctst: 
   	    len += snprintf(s+len, strSize-len, "ctnr_stat: %lld", (unsigned long long int) pm->stat); 
	    break;

	case m_crtt: 
   	    len += snprintf(s+len, strSize-len, "create_time: %lld", (unsigned long long int) pm->createtime); 
	    break;

       case m_shardcnt:
   	    len += snprintf(s+len, strSize-len, "shard_count: %d", (uint32_t) pm->shard_count); 
           break;

       case m_opmeta:
   	    len += snprintf(s+len, strSize-len, "op_meta-guid: %llu", (unsigned long long int) pm->op_meta.guid); 
           break;

	default:
	    UTError("Inconsistency in print_msg_field");
	    break;
    }

    return (len);
}

static int app_request_sortfn(const void *e1_in, const void *e2_in)
{
    SDF_App_Request_Info_t *e1, *e2;

    e1 = (SDF_App_Request_Info_t *) e1_in;
    e2 = (SDF_App_Request_Info_t *) e2_in;
    if (e1->msgtype < e2->msgtype) {
        return(-1);
    } else if (e1->msgtype > e2->msgtype) {
        return(1);
    } else {
        return(0);
    }
}

static int nodes_sortfn(const void *e1_in, const void *e2_in)
{
    SDF_protocol_fmt_info_t *e1, *e2;

    e1 = (SDF_protocol_fmt_info_t *) e1_in;
    e2 = (SDF_protocol_fmt_info_t *) e2_in;
    if (e1->field < e2->field ) {
        return(-1);
    } else if (e1->field > e2->field) {
        return(1);
    } else {
        return(0);
    }
}

static int msg_sortfn(const void *e1_in, const void *e2_in)
{
    SDF_Protocol_Msg_Info_t *e1, *e2;

    e1 = (SDF_Protocol_Msg_Info_t *) e1_in;
    e2 = (SDF_Protocol_Msg_Info_t *) e2_in;
    if (e1->msgtype < e2->msgtype) {
        return(-1);
    } else if (e1->msgtype > e2->msgtype) {
        return(1);
    } else {
        return(0);
    }
}

char *SDFsPrintAppReq(char *sout, SDF_appreq_t *par, int strSize, SDF_boolean_t show_retvals)
{
    int                       i, len = 0;
    SDF_App_Request_Info_t   *pmi;
    SDF_boolean_t	      first;

    #ifdef notdef
	UTStartDebugger("/home/okrafka/src/trunk/src/branches/m327/sdf/agent/build/sdfagent");
    #endif

    pmi = &(SDF_App_Request_Info[par->reqtype]);

    len += snprintf(sout+len, strSize-len, "%s (%s), ctxt:%"PRIu64" ",
                   pmi->name, pmi->shortname, par->ctxt);

    first = SDF_TRUE;
    for (i=0; i<N_SDF_PROTOCOL_REQ_FMT_FIELDS; i++) {
        if (pmi->format & SDF_Protocol_Req_Fmt_Info[i].field) {
	    if (!first) {
		len += snprintf(sout+len, strSize-len, ", ");
	    }
	    len += print_req_field(sout+len, i, par, strSize-len, show_retvals);
	    first = SDF_FALSE;
	}
    }

    strcat(sout, ")");
    return(sout);
}

static int print_req_field(char *sout, int n, SDF_appreq_t *par, int strSize, SDF_boolean_t show_retvals)
{
    int                        i, len=0;
    char                      *s = sout;
    SDF_container_type_t       ctype;

    switch (SDF_Protocol_Req_Fmt_Info[n].field) {

	case a_ctxt:    len += snprintf(s+len, strSize-len, "ctxt:%"PRIu64, par->ctxt); break;
        case a_ctnr:    len += snprintf(s+len, strSize-len, "cguid:%"PRIu64, par->ctnr); break;
        case a_size:    len += snprintf(s+len, strSize-len, "size:%"PRIu64, par->sze); break;
        case a_ofst:    len += snprintf(s+len, strSize-len, "offset:%"PRIu64, par->offset); break;
        case a_msiz:    len += snprintf(s+len, strSize-len, "max_size:%"PRIu64, par->max_size); break;
        case a_nops:    len += snprintf(s+len, strSize-len, "n_opids:%d", par->n_opids); break;
        case a_pbfo:    len += snprintf(s+len, strSize-len, "pbuf_out:%p", par->pbuf_out); break;
	case a_succ:    len += snprintf(s+len, strSize-len, "p_success:%p", par->p_success); break;
	case a_nbyt:    len += snprintf(s+len, strSize-len, "nbytes:%"PRIu64, par->nbytes); break;
	case a_expt:    len += snprintf(s+len, strSize-len, "exptime:%"PRIu32, par->exptime); break;
	case a_curt:    len += snprintf(s+len, strSize-len, "curtime:%"PRIu32, par->curtime); break;
	case a_invt:    len += snprintf(s+len, strSize-len, "invtime:%"PRIu32, par->invtime); break;
	case a_skey:    len += snprintf(s+len, strSize-len, "stat_key:%d", par->stat_key); break;
        case a_pbfi:    len += snprintf(s+len, strSize-len, "pbuf_in:%p", par->pbuf_in); break;
	case a_psbo:    len += snprintf(s+len, strSize-len, "pshbuf_out:" PLAT_SP_FMT, PLAT_SP_FMT_ARG(par->dest)); break;
	case a_psbi:    len += snprintf(s+len, strSize-len, "pshbf_in:" PLAT_SP_FMT, PLAT_SP_FMT_ARG(par->pshbf_in)); break;
	case a_pcbf:    len += snprintf(s+len, strSize-len, "pcbuf:%p", par->pcbuf); break;
	case a_pden:    len += snprintf(s+len, strSize-len, "prefix_del:%d", par->prefix_delete); break;

        case a_asiz:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*pactual_size:%"PRIu64"", par->destLen); 
	    } else {
		len += snprintf(s+len, strSize-len, "pactual_size:%p", par->pactual_size); 
	    }
	    break;

	case a_pexp:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*pexptme:%"PRIu32"", par->exptime); 
	    } else {
		len += snprintf(s+len, strSize-len, "pexptme:%p", par->pexptme); 
	    }
	    break;

	case a_ppbf:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*ppbuf_in:%p", *(par->ppbuf_in)); 
	    } else {
		len += snprintf(s+len, strSize-len, "ppbuf_in:%p", par->ppbuf_in); 
	    }
	    break;

        case a_pdat:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*ppdata:%p", *(par->ppdata)); 
	    } else {
		len += snprintf(s+len, strSize-len, "ppdata:%p", par->ppdata); 
	    }
	    break;

	case a_pctx:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*p_ctxt:%"PRIu64"", *(par->pctxt)); 
	    } else {
		len += snprintf(s+len, strSize-len, "p_ctxt:%p", par->pctxt); 
	    }
	    break;

	case a_popd:    
	    len += snprintf(s+len, strSize-len, "p_opid:%p", par->p_opid); 
	    #ifdef notdef
	    // p_opid is not currently used and may have a bogus value
	    if (show_retvals && (par->p_opid != NULL)) {
		len += snprintf(s+len, strSize-len, "p_opid->opid:%d", par->p_opid->opid); 
	    } else {
		len += snprintf(s+len, strSize-len, "p_opid:%p", par->p_opid); 
	    }
	    #endif
	    break;

	case a_pstt:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*pstat:%"PRIu64"", *(par->pstat)); 
	    } else {
		len += snprintf(s+len, strSize-len, "pstat:%p", par->pstat); 
	    }
	    break;



	case a_ppcb:    
	    if (show_retvals) {
		len += snprintf(s+len, strSize-len, "*ppcbf:%p", *(par->ppcbf)); 
	    } else {
		len += snprintf(s+len, strSize-len, "ppcbf:%p", par->ppcbf); 
	    }
	    break;

        case a_opid:
            len += snprintf(s+len, strSize-len, "opids[]:");
	    // sprintf(s, "opids[]:");
	    for (i=0; i<par->n_opids; i++) {
	        if (i != 0) {
                    len += snprintf(s+len, strSize-len, ",");
		    // strcat(s, ",");
		}
		// len = strlen(sout);
		// s = &(sout[len]);
		//sprintf(s, "%d", par->opid[i]);
                len += snprintf(s+len, strSize-len, "%"PRIu32, par->opid[i].opid);
	    }
	    break;

        case a_key_:

	    ctype   = par->ctnr_type;
	    switch (ctype) {
	        case SDF_BLOCK_CONTAINER:
	        case SDF_OBJECT_CONTAINER:
		    len += snprintf(s+len, strSize-len, "key:%.20s", par->key.key);
		    break;
		default:
		    len += snprintf(s+len, strSize-len, "key:%s", "Unknown ctype");
		    plat_log_msg(21340, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR, "Unknown container type=%u", ctype);
		    break;
	    }
	    break;

	default:
	    UTError("Inconsistency in print_req_field");
	    break;
    }
    
    return (len);
}

char *SDFStatusString(SDF_status_t status)
{
    char *sret;

    if (status >= N_SDF_STATUS_STRINGS)
    {
        sret = "UNKNOWN_STATUS";
    } else {
	sret = SDF_Status_Strings[status];
    }

    return (sret);
}

static char HexMap[] = "0123456789abcdef";

void SDFBlknumToKey(SDF_simple_key_t *pkey, uint64_t blknum)
{
    int       i;
    uint64_t  x;
    char     *sto;

    sto = &(pkey->key[16]);
    x   = blknum;
    *sto-- = '\0';
    for (i=0; i<16; i++) {
        *sto-- = HexMap[x & 0xf];
	x >>= 4;
    }
    pkey->len = 17;
}

SDF_status_t SDFObjnameToKey(SDF_simple_key_t *pkey, char *objname, uint32_t keylen)
{
    int i;
    char  *sfrom = NULL;
	char  *sto   = NULL;

	if (keylen >= (SDF_SIMPLE_KEY_SIZE - 1)) {
		plat_log_msg(30606, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR, "Object name is too long");
		return(SDF_KEY_TOO_LONG);
	}

	if (0 >= keylen) {
		plat_log_msg(160143,
		             PLAT_LOG_CAT_SDF_PROT,
					 PLAT_LOG_LEVEL_ERROR,
					 "Zero or negative size key is provided");

		return SDF_FAILURE_INVALID_KEY_SIZE;
	}

    sto   = pkey->key;
    sfrom = objname;
    for (i=0; i<keylen; i++) {
        *sto++ = *sfrom++;
    }
    *sto = '\0';

    sto[keylen + 1] = '\0';

    pkey->len = keylen + 1;

    return(SDF_SUCCESS);
}
