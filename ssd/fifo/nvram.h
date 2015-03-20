
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

//end of file
#endif 
