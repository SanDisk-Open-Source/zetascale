/*
 * File:   sdf_msg_types.h
 * Author: Tom Riddle
 *
 * Created on February 25, 2008, 3:45 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_types.h,v 1.1 2008/05/22 09:47:59 drew Exp $
 */

#ifndef _SDF_MSG_TYPES_H
#define _SDF_MSG_TYPES_H


#include "sdf_msg.h"
// #include "sdfappcommon/XList.h"
#include "fth/fth.h"
#include "fth/fthMbox.h"
// #include "fth/fthXMbox.h"
#include "platform/closure.h"
// #include "applib/XMbox.h"
// #include "sdfappcommon/XMbox.h"

/*
 * The default number of bins is static and initialized according to
 * SDF_msg_protocol below. You must set this in DEF_NUM_BINS
 * The default number of preposted receive buffers are statically
 * created and fixed. They can be increased by setting the size
 * of DEF_NUM_BUFFS below.
 */
#define MAX_NUM_SCH_NODES 8
#define MSG_DFLT_KEY "deadbeeffeedcafe"



typedef vnode_t bin_t;
typedef unsigned char cbuff;
typedef struct sdf_msg_bin_list bin_list;




/*
 * Note that the constant MAX_NUM_BINS must reflect the number of items in this
 * list.
 */
#define SDF_MSG_PROTOCOL_ITEMS()                                               \
    item(SDF_DEBUG)                                                            \
    item(SDF_SYSTEM)                                                           \
    item(SDF_CONSISTENCY)                                                      \
    item(SDF_MANAGEMENT)                                                       \
    item(SDF_MEMBERSHIP)                                                       \
    /** @brief Requests to flash */                                            \
    item(SDF_FLSH)                                                             \
    item(SDF_METADATA)                                                         \
    /** @brief Requests to replication service, typically home node code */    \
    item(SDF_REPLICATION)                                                      \
    /** @brief Internal requests to replication service from peers */          \
    item(SDF_REPLICATION_PEER)                                                 \
    /** @brief Internal requests for supernode metea-data implementatino */    \
    item(SDF_REPLICATION_PEER_META_SUPER)                                      \
    /** @brief Internal requests for Paxos meta-data implementatino */         \
    item(SDF_REPLICATION_PEER_META_CONSENSUS)                                  \
    item(SDF_SHMEM)                                                            \
    item(SDF_RESPONSES)                                                        \
    item(SDF_3RDPARTY)                                                         \
    item(SDF_FINALACK)                                                         \
    item(GOODBYE)                                                              \
    item(SDF_UNEXPECT)                                                         \
    /** @brief Internal message from SDF_MSG */                                \
    item(SDF_SDFMSG)                                                           \
    /** @brief Message which originates from test code */                      \
    item(SDF_TEST)                                                             \
    /* Used for syncing between nodes */                                       \
    item(SDF_SYNC)


typedef enum SDF_msg_protocol {
#define item(caps) caps,
    SDF_MSG_PROTOCOL_ITEMS()
    item(SDF_PROTOCOL_COUNT)
#undef item
} SDF_msg_protocol;

const char *SDF_msg_protocol_to_string(enum SDF_msg_protocol msg_protocol);

/**
 * @brief legal values for sdf_msg msg_type field
 *
 * At the sdf_msg layer, all messages have unique types so that synthetic
 * error resposnes can be generated.
 *
 * XXX: drew 2009-01-03 Should either merge the SDF_MSG error types into one
 * and provide a payload with specifics, or provide a macro
 * sdf_msg_is_error() which does the right thing so we don't have
 * maintenance problems as things are added.
 */
#define SDF_MSG_TYPE_ITEMS() \
    item(FLSH_REQUEST)                                                         \
    item(FLSH_RESPOND)                                                         \
    /** @brief Meta storage request */                                         \
    item(META_REQUEST)                                                         \
    item(META_RESPOND)                                                         \
    item(REPL_REQUEST)                                                         \
    item(LOCK_RESP)                                                            \
    item(REQ_FLUSH)                                                            \
    item(REQ_MISS)                                                             \
    item(RESP_ONE)                                                             \
    item(RESP_TWO)                                                             \
    item(MDAT_REQUEST)                                                         \
    item(MGMT_REQUEST)                                                         \
    item(HEARTBEAT_REQ)                                                        \
    item(SYS_REQUEST)                                                          \
    item(SYS_PP_REQ)                                                           \
    item(SYS_PP_ACK)                                                           \
    item(SYS_SHUTDOWN_SELF)                                                    \
    item(SYS_SHUTDOWN_ALL)                                                     \
    item(SYS_SHUTDOWN_ERR)                                                     \
    /** @brief Internally generated error message */                           \
    item(SDF_MSG_ERROR)

enum SDF_msg_type {
#define item(caps) caps,
    SDF_MSG_TYPE_ITEMS()
#undef item
};

const char *SDF_msg_type_to_string(enum SDF_msg_type msg_type);

/** @brief Payload for SDF_MSG_ERROR type */
typedef struct sdf_msg_error_payload {
    /** @brief Specific failure */
    SDF_status_t error;
} sdf_msg_error_payload_t;

#define SDF_MSG_SACK_HOW_ITEMS() \
    item(SACK_HOW_NONE)                                                        \
    item(SACK_HOW_FTH_MBOX_TIME)                                               \
    item(SACK_HOW_FTH_MBOX_MSG)                                                \
    item(SACK_HOW_CLOSURE_MSG_WRAPPER)

typedef enum SDF_msg_SACK_how {
#define item(caps) caps,
    SDF_MSG_SACK_HOW_ITEMS()
#undef item
} SDF_msg_SACK_how;

const char *SDF_msg_SACK_how_to_string(enum SDF_msg_SACK_how how);

typedef enum SDF_msg_SACK_rel {
    SACK_REL_NO = 0,
    SACK_REL_YES = 1
} SDF_msg_SACK_rel;

typedef enum SDF_msg_SACK_phase {
    SACK_PHASE_ACK = 1,
    SACK_PHASE_RESP
} SDF_msg_SACK_phase;

/* item(caps, val, ack, resp, rel, modern) */
#define SDF_SACK_ITEMS() \
    item(SACK_ONLY_FTH, = 1, SACK_HOW_FTH_MBOX_TIME, SACK_HOW_NONE,            \
         SACK_REL_YES, 0)                                                      \
    item(SACK_RESP_ONLY_FTH, /* */, SACK_HOW_NONE, SACK_HOW_FTH_MBOX_MSG,      \
         SACK_REL_YES, 0)                                                      \
    item(SACK_BOTH_FTH, /* */, SACK_HOW_FTH_MBOX_TIME, SACK_HOW_FTH_MBOX_MSG,  \
         SACK_REL_YES, 0)                                                      \
    item(SACK_NONE_FTH, /* */, SACK_HOW_NONE, SACK_HOW_NONE, SACK_REL_YES, 0)  \
    item(SACK_MODERN, /* */, SACK_HOW_NONE, SACK_HOW_NONE, SACK_REL_NO, 1)

typedef enum SDF_msg_SACK {
#define item(caps, val, ack, resp, rel, modern) caps val,
  SDF_SACK_ITEMS()
#undef item
} SDF_msg_SACK;


/* Helper functions to make sense of enumeration */
static __inline__ enum SDF_msg_SACK_how
sdf_msg_sack_resp(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (resp);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_HOW_NONE);
    }
}

static __inline__ enum SDF_msg_SACK_how
sdf_msg_sack_ack(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (ack);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_HOW_NONE);
    }
}

static __inline__ enum SDF_msg_SACK_rel
sdf_msg_sack_rel(SDF_msg_SACK sack) {
    switch (sack) {
#define item(caps, val, ack, resp, rel, modern) \
    case caps: return (rel);
    SDF_SACK_ITEMS();
#undef item
    default:
        return (SACK_REL_NO);
    }
}

/**
 * @brief Bit fielded flags for #sdf_msg flags field.
 */
typedef enum {
    /** @brief A response is expected */
    SDF_MSG_FLAG_MBX_RESP_EXPECTED = 1 << 0,

    /** @brief Response is being delivered */
    SDF_MSG_FLAG_MBX_RESP_INCLUDED = 1 << 1,
} SDF_msg_flags;

#endif /* _SDF_MSG_TYPES_H */
