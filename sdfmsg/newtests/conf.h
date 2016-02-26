/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * Configuration structure for messaging.
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */

#ifndef CONF_H
#define CONF_H 1

typedef struct plat_opts_config_msg {
    char     property_file[PATH_MAX];   /* Property file */
    struct   plat_shmem_config shmem;   /* Used for shmem parameters */
} msg_config_t;

int plat_opts_parse_msg(msg_config_t *config, int argc, char **argv);

#endif /* CONF_H */
