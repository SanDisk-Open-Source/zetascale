/*
 * File: msg_sdfincl.h
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Define functions to help interface to SDF.
 */

#ifndef MSG_SDFINCL_H
#define MSG_SDFINCL_H

#include "platform/stdio.h"
#include "sdfmsg/sdf_msg_types.h"

#define fdf_logd(id, fmt, args...)     _plm(id, DEBUG, "LOGD", 0, fmt, ##args)
#define fdf_loge(id, fmt, args...)     _plm(id, ERROR, "LOGE", 0, fmt, ##args)
#define fdf_logf(id, fmt, args...)     _plm(id, FATAL, "LOGF", 0, fmt, ##args)
#define fdf_logi(id, fmt, args...)     _plm(id, INFO,  "LOGI", 0, fmt, ##args)
#define fdf_logt(id, fmt, args...)     _plm(id, TRACE, "LOGT", 0, fmt, ##args)
#define fdf_logw(id, fmt, args...)     _plm(id, WARN,  "LOGW", 0, fmt, ##args)

#define fdf_logd_sys(id, fmt, args...) _plm(id, DEBUG, "LOGD", 1, fmt, ##args)
#define fdf_loge_sys(id, fmt, args...) _plm(id, ERROR, "LOGE", 1, fmt, ##args)
#define fdf_logf_sys(id, fmt, args...) _plm(id, FATAL, "LOGF", 1, fmt, ##args)
#define fdf_logi_sys(id, fmt, args...) _plm(id, INFO,  "LOGI", 1, fmt, ##args)
#define fdf_logt_sys(id, fmt, args...) _plm(id, TRACE, "LOGT", 1, fmt, ##args)
#define fdf_logw_sys(id, fmt, args...) _plm(id, WARN,  "LOGW", 1, fmt, ##args)

#define _plm(id, level, type, sys, fmt, args...)                    \
    do {                                                            \
        if (id)                                                     \
            ffdc_log(__LINE__, id, LOG_CAT,                         \
                     PLAT_LOG_LEVEL_ ## level, fmt, ##args);        \
        if (t_on(LOGS) ||                                           \
            plat_log_enabled(LOG_CAT, PLAT_LOG_LEVEL_ ## level)) {  \
            trace_print(basename(__FILE__), __LINE__, __PRETTY_FUNCTION__,    \
                        type, id, LOG_CAT, sys,                     \
                        PLAT_LOG_LEVEL_ ## level, fmt, ##args);     \
        }                                                           \
    } while (0)

#define fatal(fmt, args...)         \
    do {                            \
        fdf_logf(0, fmt, ##args);   \
        plat_exit(1);               \
    } while (0) 

#define fatal_sys(fmt, args...)         \
    do {                                \
        fdf_logf_sys(0, fmt, ##args);   \
        plat_exit(1);                   \
    } while (0) 

#define ignore(s)   \
    do {            \
        if (s);     \
    } while(0)

#endif /* MSG_SDFINCL_H */
