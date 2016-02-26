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

#ifndef HOME_UTIL_H
#define HOME_UTIL_H 1

#include "platform/defs.h"
#include "fth/fth.h"

#include "common/sdftypes.h"
#include "protocol/protocol_common.h"
#include "protocol/protocol_utils.h"
#include "sdfmsg/sdf_msg.h"

__BEGIN_DECLS

/**
 * @brief Create message
 *
 * The sdf_msg envelope fields are uninitialized.
 *
 * @param pm_old <IN> request to which the return is a reply
 *
 * @param pmsize <OUT> when non-null, *pmsize gets the message payload size
 *
 * @return dynamically allocated sdf_msg which must be released with 
 * sdf_msg_free.
 */
struct sdf_msg *
home_load_msg(SDF_vnode_t node_from, SDF_vnode_t node_to,
              SDF_protocol_msg_t *pm_old, SDF_protocol_msg_type_t msg_type,
              void *data, SDF_size_t data_size, 
              SDF_time_t exptime, SDF_time_t createtime, uint64_t sequence,
              SDF_status_t status, SDF_size_t *pmsize, char * key, int key_len,
              uint32_t flags);

__END_DECLS

#endif /* ndef HOME_UTIL_H */
