/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
