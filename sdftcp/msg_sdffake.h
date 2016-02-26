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
 * Author: Johann George.
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 *
 * Attempt to run without SDF.
 */

#ifndef MSG_SDFFAKE_H
#define MSG_SDFFAKE_H

/*
 * Logging routines.
 */
#define zs_logd(fmt, args...)      t_logs(fmt, ##args)
#define zs_loge(fmt, args...)      t_logs(fmt, ##args)
#define zs_logf(fmt, args...)      t_logs(fmt, ##args)
#define zs_logi(fmt, args...)      t_logs(fmt, ##args)
#define zs_logt(fmt, args...)      t_logs(fmt, ##args)
#define zs_logw(fmt, args...)      t_logs(fmt, ##args)

#define zs_logd_sys(fmt, args...)  t_logs(fmt, ##args)
#define zs_loge_sys(fmt, args...)  t_logs(fmt, ##args)
#define zs_logf_sys(fmt, args...)  t_logs(fmt, ##args)
#define zs_logi_sys(fmt, args...)  t_logs(fmt, ##args)
#define zs_logt_sys(fmt, args...)  t_logs(fmt, ##args)
#define zs_logw_sys(fmt, args...)  t_logs(fmt, ##args)

#define fatal(fmt, args...)     panic(fmt, ##args)
#define fatal_sys(fmt, args...) panic(fmt, ##args)

/*
 * Undo the plat_ versions of these.
 */
#define plat_exit(a)            exit(a)
#define plat_free(a)            free(a)
#define plat_assert(a)          assert(a)
#define plat_malloc(a)          malloc(a)
#define plat_strdup(a)          strdup(a)
#define plat_realloc(a, s)      realloc(a, s)
#define plat_asprintf(p, a...)  asprintf(p, ##a)

#define msg_map_recv(a)         (-1)
#define msg_map_send(a)         (-1)
#define msg_sdf_myrank()        (-1)

/*
 * Program must provide these.
 */
void panic(char *fmt, ...);
void printd(char *fmt, ...);
void printm(char *fmt, ...);

#endif /* MSG_SDFFAKE_H */
