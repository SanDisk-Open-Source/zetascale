/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#define TABLES_SKIP_INTERNAL_BLK_OBJ_API_H
#include "shared/object.h"
#include "shared/container_meta.h"

#include "protocol/protocol_common.h"
#include "protocol/action/action_internal_ctxt.h"
#include "protocol/action/action_thread.h"
#include "ssd/fifo/mcd_osd.h"

static SDF_context_t SDF_Next_Context = SDF_RESERVED_CONTEXTS + 1;

int SDFSimpleReplicationEnabled()
{
    return(SDFSimpleReplication);
}

SDF_status_t
SDF_I_NewContext( void * pai, SDF_context_t * context )
{
	//context initialization is done correctly. They would conflict.
	plat_assert(0);
    *context = __sync_fetch_and_add( &SDF_Next_Context, 1 );
    return SDF_SUCCESS;
}


SDF_status_t
SDF_I_Delete_Context( void * pai, SDF_context_t context )
{
    return SDF_SUCCESS;
}

SDF_status_t SDFAutoDelete(SDF_internal_ctxt_t *pai)
{
    mcd_osd_auto_delete(((SDF_action_init_t *) pai)->paio_ctxt);
    return(SDF_SUCCESS);
}

#ifndef SDFAPI
SDF_status_t SDFGetContainers(SDF_internal_ctxt_t *pai, struct mcd_container **pcontainers, int *pn_containers)
{
#ifdef notdef
    mcd_osd_get_containers(((SDF_action_init_t *) pai)->paio_ctxt, pcontainers, pn_containers);
#endif /* notdef */
    return(SDF_FAILURE);
}
#else
SDF_status_t SDFGetContainersPtrs(SDF_internal_ctxt_t *pai, struct mcd_container **pcontainers, int *pn_containers)
{
#ifdef notdef
    mcd_osd_get_containers(((SDF_action_init_t *) pai)->paio_ctxt, pcontainers, pn_containers);
#endif /* notdef */
    return(SDF_FAILURE);
}
#endif /* SDFAPI */

SDF_status_t SDF_I_CreatePutBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APCOP;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_PutBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_out, SDF_size_t sze)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APPTA;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForReadPinnedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size)
{
    SDF_appreq_t            ar;
    SDF_action_init_t      *pac;
    local_SDF_CACHE_OBJ     lo = NULL;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGRP;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppdata == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);
    *ppdata = getLocalCacheObject(&lo, ar.dest, ar.destLen);

    if (pactual_size == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *pactual_size = ar.destLen;

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForReadBufferedObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void * pbuf_in, SDF_size_t max_size, SDF_size_t * pactual_size)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGRD;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.pbuf_in = pbuf_in;
    if (pbuf_in == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }
    ar.max_size = max_size;
    ar.pactual_size = pactual_size;

    ActionProtocolAgentNew(pac, &ar);

    if (pactual_size == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *pactual_size = ar.destLen;

    return(ar.respStatus);
}

SDF_status_t SDF_I_UnpinObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APUPN;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoveObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APDOB;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoveObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_time_t curtime, SDF_boolean_t pref_del)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APDBE;
    ar.prefix_delete = pref_del;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_CreatePinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t            ar;
    SDF_action_init_t      *pac;
    local_SDF_CACHE_OBJ     lo = NULL;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APCPE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    ar.sze     = sze;
    ar.exptime = exptime;
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppdata == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);
    *ppdata = getLocalCacheObject(&lo, ar.dest, ar.destLen);

    return(ar.respStatus);
}

SDF_status_t SDF_I_SetPinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t            ar;
    SDF_action_init_t      *pac;
    local_SDF_CACHE_OBJ     lo = NULL;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APSPE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    ar.sze     = sze;
    ar.exptime = exptime;
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppdata == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);
    *ppdata = getLocalCacheObject(&lo, ar.dest, ar.destLen);

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForReadPinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme)
{
    SDF_appreq_t            ar;
    SDF_action_init_t      *pac;
    local_SDF_CACHE_OBJ     lo = NULL;
    SDF_status_t            status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGRE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppdata == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);
    *ppdata = getLocalCacheObject(&lo, ar.dest, ar.destLen);

    if (pactual_size == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *pactual_size = ar.destLen;

    if (pexptme == NULL) {
        return(SDF_BAD_PEXPTIME_POINTER);
    }
    *pexptme     = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForWritePinnedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void ** ppdata, SDF_size_t sze, SDF_time_t curtime, SDF_time_t * pexptme)
{
    SDF_appreq_t            ar;
    SDF_action_init_t      *pac;
    local_SDF_CACHE_OBJ     lo = NULL;
    SDF_status_t            status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGWE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    ar.sze  = sze;
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppdata == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);
    *ppdata = getLocalCacheObject(&lo, ar.dest, ar.destLen);

    if (pexptme == NULL) {
        return(SDF_BAD_PEXPTIME_POINTER);
    }
    *pexptme     = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDF_I_CreatePutBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APCOE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    ar.exptime = exptime;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_PutBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void *pbuf_out, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APPAE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    ar.exptime = exptime;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_SetBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void *pbuf_out, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APSOE;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    ar.exptime = exptime;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForReadBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void **ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGRX;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppbf_in == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }
    ar.ppbuf_in = ppbf_in;

    ActionProtocolAgentNew(pac, &ar);

    if (pactual_size == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *pactual_size = ar.destLen;

    if (pexptme == NULL) {
        return(SDF_BAD_PEXPTIME_POINTER);
    }
    *pexptme     = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetForWriteBufferedObjectWithExpiry(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, void **ppbf_in, SDF_size_t * pactual_size, SDF_time_t curtime, SDF_time_t * pexptme)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGWX;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    if (ppbf_in == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }
    ar.ppbuf_in = ppbf_in;

    ActionProtocolAgentNew(pac, &ar);

    if (pactual_size == NULL) {
        return(SDF_BAD_SIZE_POINTER);
    }
    *pactual_size = ar.destLen;


    if (pexptme == NULL) {
        return(SDF_BAD_PEXPTIME_POINTER);
    }
    *pexptme     = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDF_I_FlushObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APFLS;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_FlushInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APFLI;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_InvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APINV;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoteInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APRIV;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoteFlushObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APRFL;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoteFlushInvalObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APRFI;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_RemoteUpdateObject(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, const char *objkey, uint32_t keylen, SDF_size_t sze, void * pbuf_out, SDF_time_t curtime, SDF_time_t exptime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;
    SDF_status_t        status;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APRUP;
    ar.curtime = curtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();
    if ((status=SDFObjnameToKey(&(ar.key), (char *) objkey, keylen)) != SDF_SUCCESS) {
        return(status);
    }
    ar.sze = sze;
    ar.pbuf_out = pbuf_out;
    ar.exptime = exptime;
    if (pbuf_out == NULL) {
        return(SDF_BAD_PBUF_POINTER);
    }

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_FlushContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APFCO;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_FlushInvalContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APFCI;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_InvalContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APICO;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_SyncContainer(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APSYC;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}

SDF_status_t SDF_I_GetContainerInvalidationTime(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, SDF_time_t *pinvtime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APGIT;
    ar.curtime = 0;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    if (pinvtime == NULL) {
        return(SDF_BAD_PINVTIME_POINTER);
    }
    *pinvtime = ar.exptime;

    return(ar.respStatus);
}

SDF_status_t SDF_I_InvalidateContainerObjects(SDF_internal_ctxt_t *pai, SDF_cguid_t ctnr, SDF_time_t curtime, SDF_time_t invtime)
{
    SDF_appreq_t        ar;
    SDF_action_init_t  *pac;

    pac = (SDF_action_init_t *) pai;
    
    ar.reqtype = APICD;
    ar.curtime = curtime;
    ar.invtime = invtime;
    ar.ctxt = pac->ctxt;
    ar.ctnr = ctnr;
    ar.ctnr_type = SDF_OBJECT_CONTAINER;
    ar.internal_request = SDF_TRUE;
    ar.internal_thread = fthSelf();

    ActionProtocolAgentNew(pac, &ar);

    return(ar.respStatus);
}


