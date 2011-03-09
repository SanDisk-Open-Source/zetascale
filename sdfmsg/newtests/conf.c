/*
 * Messaging interface to platform code.
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 */
#include <stdio.h>
#include "sdfmsg/sdf_msg.h"
#include "conf.h"

#define PLAT_OPTS_NAME(name) name ## _msg

#include "platform/opts.h"

#define PLAT_OPTS_ITEMS_msg()                                                  \
    PLAT_OPTS_SHMEM(shmem)                                                     \
    item("msg_affinity", "messaging: thread affinity", MSG_AFFINITY,           \
         ({ sdf_msg_setinitn("msg_affinity", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_alias", "messaging: node alias", MSG_ALIAS,                      \
         ({ sdf_msg_setinitn("msg_alias", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_class", "messaging: class", MSG_CLASS,                           \
         ({ sdf_msg_setinitn("msg_class", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_dead", "messaging: node minimum dead time", MSG_DEAD,            \
         ({ sdf_msg_setinitn("msg_dead", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_debug", "messaging: debug parameters", MSG_DEBUG,                \
         ({ sdf_msg_setinitn("msg_debug", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_flags", "messaging: flags", MSG_FLAGS,                           \
         ({ sdf_msg_setinitn("msg_flags", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_iface", "messaging: interfaces", MSG_IFACE,                      \
         ({ sdf_msg_setinitn("msg_iface", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_linklive", "messaging: link liveness time", MSG_LINKLIVE,        \
         ({ sdf_msg_setinitn("msg_linklive", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_live", "messaging: node liveness time", MSG_LIVE,                \
         ({ sdf_msg_setinitn("msg_live", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_mustlive", "messaging: liveness gets own thread", MSG_MUSTLIVE,  \
         ({ sdf_msg_setinitn("msg_mustlive", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nconn", "messaging: default number of connections", MSG_NCONN,   \
         ({ sdf_msg_setinitn("msg_nconn", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nhold", "messaging: default number of hold buffers", MSG_NHOLD,  \
         ({ sdf_msg_setinitn("msg_nhold", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nobcast", "messaging: turn of broadcasting", MSG_NOBCAST,        \
         ({ sdf_msg_setinitn("msg_nobcast", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nodes", "messaging: expected node count", MSG_NODES,             \
         ({ sdf_msg_setinitn("msg_nodes", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_nthreads", "messaging: number of threads", MSG_NTHREADS,         \
         ({ sdf_msg_setinitn("msg_nthreads", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_ntunique", "messaging: unique affinity threads", MSG_NTUNIQUE,   \
         ({ sdf_msg_setinitn("msg_ntunique", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_ping", "messaging: ping interval for liveness", MSG_PING,        \
         ({ sdf_msg_setinitn("msg_ping", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_rank", "messaging: rank", MSG_RANK,                              \
         ({ sdf_msg_setinitn("msg_rank", optarg), 0;}),                        \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_statfreq", "messaging: stats frequency shown", MSG_STATFREQ,     \
         ({ sdf_msg_setinitn("msg_statfreq", optarg), 0;}),                    \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_stats", "messaging: statistic options", MSG_STATS,               \
         ({ sdf_msg_setinitn("msg_stats", optarg), 0;}),                       \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_tcpport", "messaging: TCP port", MSG_TCPPORT,                    \
         ({ sdf_msg_setinitn("msg_tcpport", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_timeout", "messaging: timeout for responses", MSG_TIMEOUT,       \
         ({ sdf_msg_setinitn("msg_timeout", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("msg_udpport", "messaging: UDP port", MSG_UDPPORT,                    \
         ({ sdf_msg_setinitn("msg_udpport", optarg), 0;}),                     \
         PLAT_OPTS_ARG_REQUIRED)                                               \
    item("property_file", "property file name",                                \
         PROPERTY_FILE, parse_string(config->property_file, optarg),           \
         PLAT_OPTS_ARG_REQUIRED)

#include "platform/opts_c.h"
