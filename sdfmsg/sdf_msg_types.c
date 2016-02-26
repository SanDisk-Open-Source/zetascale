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
