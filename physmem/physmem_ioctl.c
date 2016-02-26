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

/**
 * File:   physmem_ioctl.c
 *
 * Author: drew
 *
 * Created on July 29, 2008
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: physmem_ioctl.c 2477 2008-07-29 17:17:40Z drew $
 */

/**
 * ioctl physmem device
 */

#include <sys/ioctl.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "physmem.h"

static int region_ioctl(int fd, struct physmem_regions *regions);

int
main(int argc, char **argv) {
    int ret = 0;
    int fd = -1;
    int status;
    struct physmem_regions *regions = NULL;

    if (argc != 2) {
        fprintf(stderr, "usage: physmem_ioctl <device>\n");
        ret = 2;
    }

    if (!ret) {
        fd = open(argv[1], O_RDONLY);
        if (fd == -1) {
            perror("open");
            ret = 2;
        }
    }

    if (!ret) {
        regions = calloc(1, sizeof (*regions));
        ret = region_ioctl(fd, regions);
    }
    if (!ret) {
        regions->nregion = regions->total_region;
        regions = realloc(regions, sizeof (*regions) +
                          regions->nregion * sizeof (regions->region[0]));
        ret = region_ioctl(fd, regions);
    }

    if (regions) {
        free(regions);
    }

    if (fd != -1) {
        status = close(fd);
        if (status) {
            perror("close");
            if (!ret) {
                ret = 1;
            }
        }
    }

    return (ret);
}

static int
region_ioctl(int fd, struct physmem_regions *regions) {
    int ret;
    int i;

    if (ioctl(fd, PHYSMEMGETREGIONS, regions) == -1) {
        perror("first ioctl"); 
        ret = 2;
    } else {
        ret = 0;
    }

    if (!ret) {
        printf("%u segments total size %llx\n",
               (unsigned)regions->total_region,
               (unsigned long long)regions->total_len);
        for (i = 0; i < regions->nregion; ++i) {
            printf("phys %llx len 0x%llx\n",
                   (unsigned long long)regions->region[i].paddr,
                   (unsigned long long)regions->region[i].len);
        }
    }

    return (ret);
}
