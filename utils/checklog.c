/*
 * File:   checklog.c
 * Author: Darryl Ouye
 *
 * Created on August 28, 2014
 *
 * SanDisk Proprietary Material, © Copyright 2014 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include "platform/logging.h"
#include "properties.h"
#include "checklog.h"

static char *logfile = NULL;
static FILE *fp = NULL;

int zscheck_init_log(char *file)
{
    if (file)
        logfile = file;
    else
        logfile = ZSCHECK_LOG_DEFAULT;

    fp = fopen(logfile, "a");

    if (fp)
        return 0;
    else {
        perror("Failed to open logfile: ");
        return -1;
    }
}
    
int zscheck_close_log()
{
    if (fp) {
      fflush(fp);
      return fclose(fp);
    }
    else
      return -1;
}

void zscheck_log_msg(
    ZS_check_entity_t entity,
    uint64_t id,
    ZS_check_error_t error,
    char *msg
    )
{
    struct timeval now;
    char *time_str;

    gettimeofday(&now, NULL);
    time_str = plat_log_timeval_to_string(&now);

    if (fp) {
        fprintf(fp, "%s %u, %lu, %u, %s\n", time_str, entity, id, error, msg);
        fflush(fp);
    }
}
