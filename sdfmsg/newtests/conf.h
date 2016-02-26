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
