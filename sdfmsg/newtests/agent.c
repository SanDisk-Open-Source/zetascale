/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/*
 * File: agent.c
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * A simulated agent.
 */
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>
#include "platform/opts.h"
#include "utils/properties.h"
#include "sdfmsg/sdf_msg_types.h"
#include "sdftcp/tools.h"
#include "sdftcp/trace.h"
#include "sdftcp/msg_cat.h"
#include "sdftcp/msg_map.h"
#include "conf.h"
#include "agent.h"


/*
 * Configurable parameters.
 */
#define DEF_SHMEM_SIZE  (16*1024*1024)
#define STR_SIZE        1024


/*
 * Useful definitions.
 */
#define streq(a, b)  (strcmp(a, b) == 0)


/*
 * Function prototypes.
 */
static void  init_shmem(uint32_t me);
static void  call_plat(msg_config_t *, int argc, char *argv[]);
static char *y_arg(char *arg);


/*
 * Initialize.
 */
void
agent_init(int argc, char *argv[])
{
    msg_config_t platconf;

    memset(&platconf, 0, sizeof(platconf));
    call_plat(&platconf, argc, argv);
    if (platconf.property_file[0])
        loadProperties(platconf.property_file);

    sdf_msg_init(argc, argv);
    init_shmem(sdf_msg_myrank());
    sdf_msg_alive();
}


/*
 * Finish.
 */
void
agent_exit(void)
{
    sdf_msg_exit();
    plat_shmem_detach();
    plat_exit(0);
}


/*
 * Call plat_opts_parse_msg adding an argument if we do not want noisy output.
 */
static void
call_plat(msg_config_t *msg_init, int argc, char *argv[])
{
    int y;
    char **old;
    char **new;
    char **args;

    if (argc < 1)
        fatal("call_plat received no arguments");

    y = 0;
    old = argv;
    for (;;) {
        char *arg = *old++;
        if (!arg)
            break;
        if (strncmp(arg, "-y", 2) != 0)
            continue;
        y++;
    }
    
    /* 
     * We need argc slots for the original arguments.  Every -y may convert
     * into an extra argument.  If we have none of them, we will need an extra
     * 2.  And 1 for the NULL at the end.
     */
    args = m_malloc((argc+y+2+1) * sizeof(*args), "agent:N*char*");

    old = argv;
    new = args;
    for (;;) {
        char *arg = *old++;

        if (!arg)
            break;
        if (strncmp(arg, "-y", 2) == 0) {
            if (strlen(arg) > 2) {
                *new++ = "--log";
                *new++ = y_arg(arg);
            }
            continue;
        }
        *new++ = arg;
    }
    if (y == 0) {
        *new++ = "--log";
        *new++ = "default=fatal";
    }
    *new = NULL;

    if (plat_opts_parse_msg(msg_init, new-args, args) < 0)
        fatal("plat_opts_parse_msg failed");
    m_free(args);
}


/*
 * Convert a -y parameter into an argument to --log.
 */
static char *
y_arg(char *arg)
{
    if (streq(arg, "-yt"))
        return "default=trace";
    if (streq(arg, "-yd"))
        return "default=debug";
    if (streq(arg, "-yi"))
        return "default=info";
    if (streq(arg, "-yw"))
        return "default=warn";
    if (streq(arg, "-ye"))
        return "default=error";
    if (streq(arg, "-yf"))
        return "default=fatal";
    if (streq(arg, "-yst"))
        return "sdf=trace";
    if (streq(arg, "-ysd"))
        return "sdf=debug";
    if (streq(arg, "-ysi"))
        return "sdf=info";
    if (streq(arg, "-ysw"))
        return "sdf=warn";
    if (streq(arg, "-yse"))
        return "sdf=error";
    if (streq(arg, "-ysf"))
        return "sdf=fatal";
    fatal("bad parameter: %s", arg);
}


/*
 * Initialize shared memory subsystem.
 */
static void
init_shmem(uint32_t me)
{
    int s;
    const char *base;
    char *shmem_path;
    char name[STR_SIZE];
    struct plat_shmem_config config;

    /* Get configuration */
    plat_shmem_config_init(&config);

    /* Ensure base directory exists */
    snprintf(name, sizeof(name), "NODE[%u].SHMEM.BASEDIR", me);
    base = getProperty_String(name, plat_get_tmp_path());
    if (mkdir(base, 0777) < 0) {
        if (errno != EEXIST)
            fatal("error creating %s for shmem", base);
    }

    /* Add backing file */
    s = plat_asprintf(&shmem_path, "%s/sdf_shmem%u", base, me);
    if (s < 0)
        fatal("plat_asprintf failed");

    /* Set the size */
    if (plat_shmem_config_path_is_default(&config)) {
        char *prop;
        int64_t size;

        s = plat_asprintf(&prop, "NODE[%u].SHMEM.SIZE", me);
        if (s < 0)
            fatal("plat_asprintf failed");
        size = getProperty_uLongLong(prop, DEF_SHMEM_SIZE);
        s = plat_shmem_config_add_backing_file(&config, shmem_path, size);
        plat_free(prop);
        if (s < 0)
            fatal("plat_shmem_config_add_backing_file failed");
    }

    /* Handle SHMEM_FAKE */
    if (!getProperty_Int("SHMEM_FAKE", 0)) {
        plat_shmem_config_set_flags(&config,
                    PLAT_SHMEM_CONFIG_DEBUG_LOCAL_ALLOC);
    }

    /* Init the prototype */
    s = plat_shmem_prototype_init(&config);
    if (s < 0) {
        const char *path = plat_shmem_config_get_path(&config);
        fatal("shmem_init(%s) failed: %s", path, plat_strerror(-s));
    }

    /* Attach */
    s = plat_shmem_attach(plat_shmem_config_get_path(&config));
    if (s < 0)
        fatal("shmem_attach(%s) failed: %s", shmem_path, plat_strerror(-s));

    /* Free configuration */
    plat_shmem_config_destroy(&config);
    plat_free(shmem_path);
}
