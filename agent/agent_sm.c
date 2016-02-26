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
 * File:   agent_sm.c
 * Author: Darpan Dinker (in the form of agent_helper.c)
 *
 * Created on March 11, 2008, 5:32 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: agent_sm.c 11701 2010-02-13 03:14:38Z drew $
 */

#include "platform/logging.h"
#include "platform/platform.h"
#include "platform/shmem.h"
#include "platform/stat.h"
#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/string.h"
#include "platform/types.h"
#include "platform/unistd.h"
#include "utils/properties.h"

#include <stdlib.h>
#include <stdio.h>


#include "agent_helper.h"

extern int zs_instance_id;

/**
 * @brief Initialize and attach shared memory
 *
 * Prefer command line, then use file per-node values from
 * NODE[index].SHMEM.BASEDIR which gets a user name/id subdirectory
 * and sdf_shmem<index>, then try a temporary directory.
 *
 * For historical reasons when the configuration comes from the
 * property file or default the subdirectories are created
 * when they don't exist.
 *
 * @param shmem_config <INOUT>  shared memory configuration which
 * comes from command line.  It's updated to reflect any changes
 * here.
 *
 * @param index <IN> node rank used to index NODE[%d] properties
 *
 * @return SDF_TRUE on success, SDF_FALSE on failure.
 */
SDF_boolean_t
init_agent_sm_config(struct plat_shmem_config *shmem_config, int index) {
    SDF_boolean_t ret = SDF_TRUE;
    char *propName;
    int status;
    const char *path;
    const char *basedir;
    int64_t sm_size;
    char *user;
    char *backing_dir;
    char *sm_name;

    /*
     * If the shared memory file and size are still the default because they
     * weren't replaced via a command line option use the ones from the
     * property file.
     *
     * For historical reasons we make the base directory and its
     * immediate subdirectory.
     */

    if (SDF_TRUE == ret &&
        plat_shmem_config_path_is_default(shmem_config)) {

        backing_dir = NULL;

        status = plat_asprintf(&propName, "NODE[%u].SHMEM.BASEDIR", index);
        plat_assert(status != -1);
        basedir = getProperty_String(propName, plat_get_tmp_path());
        plat_log_msg(20858, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_TRACE,
                     "PROP: %s=%s", propName, basedir);
        plat_free(propName);

        status = mkdir(basedir, 0777);
        if (0 != status && EEXIST != errno) {
            plat_log_msg(21795, PLAT_LOG_CAT_SDF_CLIENT, PLAT_LOG_LEVEL_FATAL,
                         "Error creating path (%s), returned (%s)",
                         basedir, plat_strerror(errno));
            ret = SDF_FALSE;
        }
        status = plat_asprintf(&backing_dir, "%s", basedir);

        user = getenv("USER");
        if (user != NULL) {
            if (NULL == strstr(basedir, user)) {
                status = plat_asprintf(&backing_dir, "%s/%s", basedir, user);
            }
        } else {
            char tmpstr[32];
            sprintf(tmpstr, "%d", getuid());
            if (NULL == strstr(basedir, tmpstr)) {
                status = plat_asprintf(&backing_dir, "%s/%d", basedir, getuid());
            }
        }
        plat_assert(status != -1);

        // create backing file sub directory
        if (ret == SDF_TRUE) {
            status = mkdir(backing_dir, 0777);
            if (0 != status && EEXIST != errno) {
                plat_log_msg(21795, PLAT_LOG_CAT_SDF_CLIENT,
                             PLAT_LOG_LEVEL_FATAL,
                             "Error creating path (%s), returned (%s)",
                             backing_dir, plat_strerror(errno));
                ret = SDF_FALSE;
            }
        }

        if (ret == SDF_TRUE) {
			if(zs_instance_id)
                status = plat_asprintf(&sm_name, "%s/sdf_shmem%u.%d", backing_dir, index, zs_instance_id);
			else
                status = plat_asprintf(&sm_name, "%s/sdf_shmem%u.%d", backing_dir, index, getpid());
                //status = plat_asprintf(&sm_name, "%s/sdf_shmem%u", backing_dir, index);
            plat_assert(status != -1);

            plat_asprintf(&propName, "NODE[%u].SHMEM.SIZE", index);
            sm_size = getProperty_uLongLong(propName, 16 * 1024 * 1024);
            plat_log_msg(20863, PLAT_LOG_CAT_PRINT_ARGS, PLAT_LOG_LEVEL_TRACE,
                         "PROP: %s=%"PRIu64, propName, sm_size);
            plat_free(propName);

            if (plat_shmem_config_add_backing_file(shmem_config, sm_name,
                                                   sm_size)) {
                ret = SDF_FALSE;
            }

            plat_free(sm_name);
        }
        plat_free(backing_dir);
    }

    path = plat_shmem_config_get_path(shmem_config);

    if (SDF_TRUE == ret) {
        int fake = 0;
        if ((fake = getProperty_Int("SHMEM_FAKE", 0))) {
            plat_shmem_config_set_flags(shmem_config,
                                        PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC);
        }
        plat_log_msg(20864, PLAT_LOG_CAT_PRINT_ARGS,
                     PLAT_LOG_LEVEL_TRACE, "PROP: SHMEM_FAKE=%d", fake);

        status = plat_shmem_prototype_init(shmem_config);
        if (status) {
            plat_log_msg(20865, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                         PLAT_LOG_LEVEL_FATAL,
                         "shmem_init(%s) failed: %s", path,
                         plat_strerror(-status));
            ret = SDF_FALSE;
        }
    }

    if (SDF_TRUE == ret) {
        status = plat_shmem_attach(path);
        if (status) {
            plat_log_msg(20073, PLAT_LOG_CAT_PLATFORM_TEST_SHMEM,
                         PLAT_LOG_LEVEL_FATAL, "shmem_attach(%s) failed: %s",
                         path, plat_strerror(-status));
            ret = SDF_FALSE;
        }
    }

    return (ret);
}

/**
 * @brief Initialize agent side of shmem
 *
 * XXX: drew 2008-07-11 A few things use this to setup the "server" side
 * of things for testing but probably shouldn't.
 */
SDF_boolean_t
init_agent_sm(uint32_t index)
{
    SDF_boolean_t ret = SDF_TRUE;
    struct plat_shmem_config shmem_config;

    plat_shmem_config_init(&shmem_config);

    if (SDF_TRUE == ret) {
        ret = init_agent_sm_config(&shmem_config, index);
    }

    plat_shmem_config_destroy(&shmem_config);

    return (ret);
}
