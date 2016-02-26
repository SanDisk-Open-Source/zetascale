/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

#ifndef PLATFORM_UTIL_TRACE_H
#define PLATFORM_UTIL_TRACE_H 1

/*
* File:   $HeadURL: svn://s002.schoonerinfotech.net/schooner-trunk/trunk/sdf/platform/util_shmem.h $
* Author: Wei,Li
*
* Created on July 31, 2008
*
* (c) Copyright 2008, Schooner Information Technology, Inc.
* http://www.schoonerinfotech.com/
*
*/
#define SHME_KEY_A  8888
#define SHME_KEY_B  8889
#define SHME_FLAG_SIZE sizeof(int)
#define SHME_LEN_SIZE sizeof(unsigned int)
#define SHME_LEN_REAL_SIZE sizeof(unsigned int)
#define SHME_LEN_TOTAL_SIZE sizeof(unsigned int)
#define SHME_PID_SIZE sizeof(pid_t)
#define SHME_HEADER_SIZE (SHME_FLAG_SIZE+SHME_LEN_SIZE+SHME_PID_SIZE+\
            SHME_LEN_REAL_SIZE+SHME_LEN_TOTAL_SIZE)
#define SHME_MAX_SIZE (SHME_CONTENT_SIZE+SHME_HEADER_SIZE)
#define SHME_FULL   51
#define SHME_FULL_A   51
#define SHME_FULL_B   52
#define SHME_WARNING 53
#define SHME_REDAY  54
#define SHME_ENABLE 1

#define TRACE_CONFIG_FILE "/opt/schooner/config/bin-trace.prop"
//#define SYNC_ALL 1
/*the deep sync all could NOT be opened!*/
//#define DEEP_SYNC_ALL 1

/*
* Thin wrappers for sdf logging system to fast flush the log information to a  share memmory
*/

__BEGIN_DECLS
/**
* This is a trace content that is used for fast trace
**/
    typedef struct trace_content
{
    /** @brief id: the 64 bits id for different trace content, system would generate the id automaticlly, so just init it to zero when tracing */
    unsigned long long id;
    /** @brief time_stamp: the time stamp of this trace content */
    uint32_t time_stamp;
    /** @brief cntr_id: id of the container */
    uint32_t cntr_id;
    /** @brief connection: the connection of this trace */
    uint32_t connection;
    /** @brief exptime: the exptimei of the key */
    uint32_t exptime;
    /** @brief key_syndrome: the key's syndrome */
    unsigned long long key_syndrome;
    /** @brief key_size: the key's size */
    uint32_t key_size:8;
    /** @brief bytes: the bytes of the value */
    uint32_t bytes:24;
    /** @brief cmd: the cmd|noreply|muti of this trace */
    uint8_t cmd;
    /** @brief return_code: return_code of this operation */
    uint8_t return_code;
    /** @brief reserved: padding to make it 40 bytes total */
    uint8_t reserved[2];
} trace_content_t;

/**
* The struct used for storing the shared tracing data.
**/
struct shmem_buffer
{
    /** @brief flag: the flag that only specific if this buffer is active */
    int *flag;

    /**
     *  @brief shmid: the share memory id
     *  (allocated with shmget)
     */
    int shmid;
    /**
     *  @brief shm: the buffer poiter used to written data.
     *  (allocated with shmat)
     */
    char *shm;

    /** @brief size: size of buffer */
    unsigned int *size;

    /** @brief real_size: real size of buffer */
    unsigned int *real_size;

    /** @brief total_size: the total size allocted*/
    unsigned int *total_size;

    /** @brief pid: the pid for the log infomation collector*/
    pid_t *pid;
};

/**
*the method is used for memcached only and also the infomation here is fixed
*formate.
*This is for the beta release, only a work around.
*If the trace need to be dumped to the file system, start the
*"util_trace_collector" in the tool folder before using this interface.
**/
unsigned int printf_fast(trace_content_t * trace);

/**
 *a external interface to get the share memory pointer.index=0 for Buffer A and
 *1 for Buffer B.
 */
struct shmem_buffer *get_shmem_info(int index);

/**
 *init the config parameter from a config file
 */
#define INIT_CONFIG_PARAMETER(file,name,value,is_str) {\
    FILE *fd=fopen(file,"r");\
    char buffer[1024];\
    char *index=0;\
    int int_value=0;\
    int len=strlen(name);\
    if(fd)\
    {\
        while(!feof(fd))\
        {\
            if (fgets(buffer,1024,fd)) {}\
            index=strstr(buffer,name);\
            if(index)\
            {\
                index+=len;\
                if(is_str)\
                {\
                    memset(&value,0,sizeof(value));\
                    memcpy(&value,index,strlen(buffer)-len-1);\
                }\
                else\
                {\
                    int_value=atoi(index);\
                    memcpy(&value,&int_value,sizeof(int));\
                }\
                break;\
            }\
        }\
        fclose(fd);\
    }\
}
__END_DECLS
#endif /* ndef PLATFORM_UTIL_TRACE_H */
