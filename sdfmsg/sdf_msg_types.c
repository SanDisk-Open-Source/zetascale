/*
 * File:   sdf/sdfmsg/sdf_msg_types.c
 *
 * Author: drew
 *
 * Created on December 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: sdf_msg_types.c 5531 2009-01-12 07:38:47Z drew $
 */

#include "sdf_msg_types.h"

const char *
SDF_msg_SACK_how_to_string(enum SDF_msg_SACK_how how) {
#define item(caps) case caps: return (#caps);
    switch (how) {
    SDF_MSG_SACK_HOW_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}

const char *
SDF_msg_type_to_string(enum SDF_msg_type msg_type) {
#define item(caps) case caps: return (#caps);
    switch (msg_type) {
    SDF_MSG_TYPE_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}

const char *
SDF_msg_protocol_to_string(enum SDF_msg_protocol msg_protocol) {
#define item(caps) case caps: return (#caps);
    switch (msg_protocol) {
    SDF_MSG_PROTOCOL_ITEMS()
    default:
        return ("Invalid");
    }
#undef item
}
