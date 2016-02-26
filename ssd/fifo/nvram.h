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
 * NVRAM related utility functions.
 *
 * Author: Ramesh Chander.
 * Created on March, 2015.
 * (c) Sandisk Inc.
 */
#ifndef __NVRAM_H__
#define __NVRAM_H__

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <malloc.h>


#define MAX_FILE_NAME 1000
typedef struct nv_fd {
	int fd;
	uint64_t offset_in_dev; //offset in the main dev file 
	uint64_t size;
	off_t offset; //relative offset set by seek or read write
	char name[MAX_FILE_NAME];
	char *buf;
} nv_fd_t;


int nv_get_dev_fd();
void nv_set_dev_fd(int fd);

int nv_check_min_size(int fd);

int nv_get_block_size();

void nv_reformat_flog();	;
bool nv_init(uint64_t block_size, int num_recs, uint64_t dev_offset, int fd);


int
nv_open(char *name, int flags, mode_t mode);


ssize_t 
nv_read(int fd, void *buf, size_t nbytes);


ssize_t 
nv_write(int fd, const void *buf, size_t nbytes, off_t offset);

void inline
nv_flush(char *buf, size_t len);

void
nv_close(int fd);
long 
nv_ftell(int fd);
int 
nv_fseek(int fd, long offset, int whence);
void
nv_fdatasync(int fd);

//end of file
#endif 
