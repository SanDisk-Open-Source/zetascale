/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

/* 
* File:   sdf/platform/util_shmem_viewer.c
* Author: Wei,Li
*
* Created on July 31, 2008
*
* (c) Copyright 2008, Schooner Information Technology, Inc.
* http://www.schoonerinfotech.com/
*
*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <memory.h>

#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/spin_rw.h"
#include "platform/util_trace.h"
#include "util_trace_viewer.h"

#define MAX_FILE_NAME 256

/*the end file used for last dump*/
static char END_FILE[MAX_FILE_NAME] = "end";
/*adjust the MAX_FILE_NUMBER value if you want to adjust the number of file that
keep the trace log,default is 256 files*/
static uint32_t MAX_FILE_NUMBER = 256;
/*the buffer size, default is 50M*/
static unsigned long long SHME_CONTENT_SIZE = 50;

/*variable need to be re init for each file*/
/*the array counter*/
ulong64_t command_type_counter[TOTAL_COMMAND_TPYES] = { 0 };
ulong64_t cmd_code_counter[TOTAL_COMMAND_TPYES * STATUS_MAX_NUMBER] = { 0 };
ulong64_t key_size_counter[KEY_MAX_LENGTH] = { 0 };
ulong64_t data_size_counter[DATA_MAX_SIZE] = { 0 };

/*the hasp map counter*/
key_state_t *key_state_counter[BUCKET_MAX_NUM] = { 0 };
connection_state_t *connection_state_counter[MAX_CONNECTION] = { 0 };
uint32_t connection_interval_otime[MAX_CONNECTION]={0};
interval_state_t *interval_state_counter[MAX_INTERVAL] = { 0 };

/*the single conter*/
ulong64_t total_counter = 0;
ulong64_t total_miss = 0;
ulong64_t total_objects = 0;
ulong64_t total_object_sizes = 0;
ulong64_t total_errors = 0;
ulong64_t total_connections = 0;
ulong64_t total_intervals = 0;
ulong64_t total_gets = 0;
ulong64_t total_fail_gets = 0;
ulong64_t rev_total = 0;
ulong64_t send_total = 0;
uint32_t start_time = 0;
uint32_t end_time = 0;
/*variable need not to be re init for each file*/
const char **file_names = NULL;
int *last_file_number = NULL;
int *last_file_index = NULL;
int file_index = 0;
ulong64_t filter = 0;
int header_printed = 0;
/*the functions to show summary or dump to excel formate*/
void show_summary();
void show_dump();
/*add a cmd item to the key state stuct object*/
void
add_cmd_to_key_state(uint8_t cmd, uint8_t code, key_state_t * key)
{
    cmd_item_t *item = plat_alloc(sizeof(key_state_t));
    item->cmd = cmd & (~MCD_TRACE_NOREPLY);
    item->cmd &= (~MCD_TRACE_MULTIKEY);
    item->no_reply = cmd & (MCD_TRACE_NOREPLY);
    item->multi_key = cmd & (MCD_TRACE_MULTIKEY);
    item->code = code;
    item->next = NULL;
    if (!key->cmd_head) {
        key->cmd_head = item;
        key->cmd_tail = item;
    } else {
        key->cmd_tail->next = item;
        key->cmd_tail = item;
    }
}

/*get the key state*/
Get_Hash_Value(key_state_t, key, key_syndrome, ulong64_t,
               BUCKET_MAX_NUM, total_objects)
/*get connection state, same as key*/
Get_Hash_Value(connection_state_t, connection, connection, uint32_t,
               MAX_CONNECTION, total_connections)
/*get the interval state, same as key*/
Get_Hash_Value(interval_state_t, interval, interval, uint32_t, MAX_INTERVAL,
               total_intervals)

/*FIXME: the rev and send bandwidth may not be corroct, need to be updated
later on*/
     void add_rev_send(uint8_t cmd, uint32_t size,
                       const trace_content_t * trace)
{
    rev_total += trace->key_size;
    switch (cmd) {
    case MCD_TRACE_CMD_GET:
        send_total += size;
        break;
    case MCD_TRACE_CMD_SET:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_ADD:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_REPLACE:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_APPEND:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_PREPEND:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_CAS:
        rev_total += size;
        break;
    case MCD_TRACE_CMD_SYNC:
        break;
    case MCD_TRACE_CMD_DELETE:
        break;
    case MCD_TRACE_CMD_ARITHMETIC:
        rev_total += size;
        break;
    default:
        break;
    }
}

/*the main routine to show the content of the memory*/
void show_content(const char *addr, int offset, int size, ulong64_t
             index_total, int is_mem)
{
    trace_content_t trace;
    uint32_t bytes = 0;
    uint8_t cmd = 0;
    uint8_t no_reply = 0;
    uint8_t multi_key = 0;
    key_state_t *key = NULL;
    connection_state_t *con = NULL;
    interval_state_t *interval = NULL;
    interval_state_t *interval_connection = NULL;
    uint32_t old_time = 0;
    if (!filter) {
        printf("              id:    timestamp  conn cid cmd key         syndrome"
               "      exptime data_len  rc\n");
    }
    int hit=0;
    int diff=0;
    while (offset <= (size-sizeof(trace))) {
        memcpy(&trace, addr + offset, sizeof(trace));
        offset += sizeof(trace);
        if (1|| is_mem) {
            if(trace.id / (SHME_CONTENT_SIZE / sizeof(trace)) == index_total)
                hit=1;
            else
                diff=1;
            key = get_key_state(key_state_counter, trace.key_syndrome);
            con =
                get_connection_state(connection_state_counter,
                                     trace.connection);
            if (old_time != 0) {
                interval = get_interval_state(interval_state_counter,
                                              (trace.time_stamp - old_time));
                /*record the interval for this connection*/
                if(connection_interval_otime[trace.connection] != 0)
                    interval_connection =
                        get_interval_state(con->interval_counter,
                                           (trace.time_stamp - connection_interval_otime[trace.connection]));
            }
            old_time = trace.time_stamp;
            connection_interval_otime[trace.connection]=trace.time_stamp;
            bytes = trace.bytes;
            if (!filter) {
                printf("%.16llu: %12u %5u %.3u %.3u %.3u %.16llx %12u %8u %3u\n",
                       trace.id,
                       trace.time_stamp,
                       trace.connection,
                       trace.cntr_id,
                       trace.cmd,
                       trace.key_size,
                       trace.key_syndrome,
                       trace.exptime, bytes, trace.return_code);
            }
            total_counter++;
            /*get the real cmd type */
            cmd = trace.cmd & (~MCD_TRACE_NOREPLY);
            cmd &= (~MCD_TRACE_MULTIKEY);
            /*get if this command is no_reply or multi_key */
            no_reply = ((trace.cmd & MCD_TRACE_NOREPLY) == 0 ? 0 : 1);
            multi_key = ((trace.cmd & MCD_TRACE_MULTIKEY) == 0 ? 0 : 1);
            /*add the rev and send total for bandwidth calculation */
            add_rev_send(cmd, bytes, &trace);
            /*add the get counter and get the miss rate */
            if (cmd == MCD_TRACE_CMD_GET) {
                if (trace.return_code != SDF_SUCCESS)
                    total_fail_gets++;
                total_gets++;
            }
            /*add the cmd with code counter */
            cmd_code_counter[(cmd - 1) * STATUS_MAX_NUMBER +
                             trace.return_code]++;
            /*add the type counter */
            if (trace.cmd <= TOTAL_COMMAND_TPYES)
                command_type_counter[(trace.cmd - 1)]++;
            /*add the key size counter */
            // if (trace.key_size < KEY_MAX_LENGTH)
                key_size_counter[trace.key_size]++;
            /*add the data size counter */
            if (bytes < DATA_MAX_SIZE) {
                data_size_counter[bytes]++;
                if (1 == data_size_counter[bytes])
                    total_object_sizes++;
            }
            /*add the error counter */
            if (trace.return_code != SDF_SUCCESS)
                total_errors++;
            /*add the request number of this key */
            key->request_number++;
            /*add the request number of this connection */
            con->request_number++;
            /*add the interval numbers of this interval */
            if (interval)
                interval->request_number++;
            if (interval_connection)
                interval_connection->request_number++;
            add_cmd_to_key_state(trace.cmd, trace.return_code, key);
            /*record the end time */
            if (trace.time_stamp >= end_time)
                end_time = trace.time_stamp;
            /*record the start time */
            if (trace.time_stamp < start_time || start_time == 0)
                start_time = trace.time_stamp;
        } else {
            total_miss++;
        }
    }
    if(diff&&hit)
        printf("Notice: this file may be contributed with different trace buffers\n");
    if (filter) {
        /*show dump infor */
        if (filter & SHOW_DUMP_DIS) {
            show_dump();
        }
        /*show summary infor */
        else {
            show_summary();
        }
    }
}

/*show dump, for excel file import, use "," as seperater*/
void
show_dump()
{
    uint32_t time = 0;
    if (!header_printed) {
        printf("records,miss,time,"
               "gets,sets,adds,replace,append,prepend,cas,sync,delete,arithmetic,"
               "objs number,sizes number,connections,get_miss,tbs,recv bandwidth,"
               "send bandwidth,errors\n");
        header_printed = 1;
    }
    time = end_time - start_time;
    printf("%llu,%llu,%u,%llu,%llu,%llu,%llu,%llu,"
           "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu"
           ",%.4f,%.2f,%.2f,%.2f,%llu\n",
           total_counter, total_miss, time,
           command_type_counter[MCD_TRACE_CMD_GET - 1],
           command_type_counter[MCD_TRACE_CMD_SET - 1],
           command_type_counter[MCD_TRACE_CMD_ADD - 1],
           command_type_counter[MCD_TRACE_CMD_REPLACE - 1],
           command_type_counter[MCD_TRACE_CMD_APPEND - 1],
           command_type_counter[MCD_TRACE_CMD_PREPEND - 1],
           command_type_counter[MCD_TRACE_CMD_CAS - 1],
           command_type_counter[MCD_TRACE_CMD_SYNC - 1],
           command_type_counter[MCD_TRACE_CMD_DELETE - 1],
           command_type_counter[MCD_TRACE_CMD_ARITHMETIC - 1],
           total_objects, total_object_sizes, total_connections,
           ((double)(total_fail_gets)) / total_gets,
           (total_counter * 0.001f) / time, (rev_total) / (1024.0f * time),
           (send_total) / (1024.0f * time)
           , total_errors);

}

/*show the summary at last*/
void
show_summary()
{
    int i = 0, j = 0;
    key_state_t *key = NULL;
    cmd_item_t *item = NULL;
    connection_state_t *con = NULL;
    interval_state_t *interval = NULL;
    ulong64_t create_counter = 0;
    ulong64_t update_counter = 0;
    uint8_t has_delete = 0;
    uint8_t cmd = 0;
    uint32_t time = 0;
    printf("<<---Summary--->>\n");
    if (filter & SHOW_STATUS) {
        time = end_time - start_time;
        printf(" <---Command        States--->\n");
        printf("  Total       : %llu\n", total_counter);
        printf("  Miss        : %llu\n", total_miss);
        printf("  Time        : %u\n", time);
        printf("  Objects     : %llu\n", total_objects);
        printf("  Obj Sizes   : %llu\n", total_object_sizes);
        printf("  Connections : %llu\n", total_connections);
        printf("  Miss Rate   : %.4f\n",
               ((double)(total_fail_gets)) / total_gets);
        printf("  TBS         : %.2fKB/s\n", (total_counter * 0.001f) / time);
        printf("  RevBanwidth : %.2fKB/s\n", (rev_total) / (1024.0f * time));
        printf("  SendBanwidth: %.2fKB/s\n", (send_total) / (1024.0f * time));
        printf("  Errors      : %llu\n", total_errors);
    }
    /*command distribution */
    if (filter & SHOW_COMAMND_DIS) {
        printf(" <---Command   Distribution--->\n");
        for (i = 0; i < TOTAL_COMMAND_TPYES; i++) {
            if (command_type_counter[i])
                printf("  %s: %llu\n", command_str[i],
                       command_type_counter[i]);
        }
    }
    /*key size distribustion */
    if (filter & SHOW_KEYSIZE_DIS) {
        printf(" <---KeySize   Distribution--->\n");
        for (i = 0; i < KEY_MAX_LENGTH; i++) {
            if (key_size_counter[i])
                printf("  %d: %llu\n", i, key_size_counter[i]);
        }
    }
    /*data size distribustion */
    if (filter & SHOW_DATASIZE_DIS) {
        printf(" <---DataSize  Distribution--->\n");
        for (i = 0; i < DATA_MAX_SIZE; i++) {
            if (data_size_counter[i])
                printf("  %d: %llu\n", i, data_size_counter[i]);
        }
    }
    /*key request distribustion */
    if ((filter & SHOW_REQUEST_DIS) || (filter & SHOW_CVSU_DIS)) {
        printf(" <---Request   Distribution--->\n");
        for (i = 0; i < BUCKET_MAX_NUM; i++) {
            key = key_state_counter[i];
            while (key) {
                printf("  %llx:%llu\n", key->key_syndrome,
                       key->request_number);
                item = key->cmd_head;
                create_counter = update_counter = 0;
                has_delete = 0;
                /*print cmd list of this object */
                printf("  cmd list: ");
                while (item) {
                    if (item->code == SDF_SUCCESS) {
                        if (item->cmd == MCD_TRACE_CMD_DELETE) {
                            /*record the delete, so any new add or set would be
                               create later on */
                            has_delete = 1;
                        } else if (item->cmd != MCD_TRACE_CMD_GET) {
                            /*first set/add or the add/set after the delete */
                            if (!create_counter || has_delete) {
                                create_counter++;
                                has_delete = 0;
                            }
                            /*all others are update */
                            else
                                update_counter++;
                        }
                    }
                    printf("%s", command_str[item->cmd - 1]);
                    item = item->next;
                    if (item)
                        printf("=>");
                    else
                        printf("\n");
                }
                /*key request create vs update */
                if (create_counter && filter & SHOW_CVSU_DIS) {
                    printf("  create vs update: ");
                    printf("%llu: %llu\n", create_counter, update_counter);
                }
                key = key->next;
            }
        }
    }
    /*command with code number */
    if (filter & SHOW_CMDCODE_DIS) {
        printf(" <---CMD CODE  Distribution--->\n");
        for (i = 0; i < STATUS_MAX_NUMBER * TOTAL_COMMAND_TPYES; i++) {
            if (cmd_code_counter[i] != 0) {
                cmd = i / STATUS_MAX_NUMBER + 1;
                if (MCD_TRACE_CMD_APPEND == cmd
                    || MCD_TRACE_CMD_PREPEND == cmd
                    || MCD_TRACE_CMD_CAS == cmd
                    || MCD_TRACE_CMD_ARITHMETIC == cmd)
                    printf("  %s<->%s: %llu\n", command_str[cmd - 1],
                           store_str[i % 5],
                           cmd_code_counter[i]);
                else
                    printf("  %s<->%s: %llu\n", command_str[cmd - 1],
                           status_str[i % SDF_META_DATA_INVALID],
                           cmd_code_counter[i]);
            }
        }
    }
    /*connection's requests number */
    if (filter & SHOW_CONNECTION_DIS) {
        printf(" <---Connction Distribution--->\n");
        for (i = 0; i < MAX_CONNECTION; i++) {
            con = connection_state_counter[i];
            while (con) {
                printf("  %u: %llu\n", con->connection, con->request_number);
                if (filter & SHOW_INTERVAL_DIS) {
                    printf("   <---Interval  Distribution Per "
                           "Connection--->\n");
                    for (j = 0; j < MAX_INTERVAL; j++) {
                        interval = con->interval_counter[j];
                        while (interval) {
                            printf("   %llu: %llu\n",
                                   interval->interval,
                                   interval->request_number);
                            interval = interval->next;
                        }
                    }
                }
                con = con->next;
            }
        }
    }
    /*interval's distribution */
    if (filter & SHOW_INTERVAL_DIS) {
        printf(" <---Interval  Distribution--->\n");
        for (i = 0; i < MAX_INTERVAL; i++) {
            interval = interval_state_counter[i];
            while (interval) {
                printf("  %llu: %llu\n",
                       interval->interval, interval->request_number);
                interval = interval->next;
            }
        }
    }
    printf("<<---Summary--->>\n");
}

/*show the binary trace content of the log file*/
void
show_file(const char *file_name)
{
    int last_file = 0;
    char *index;
    ulong64_t index_total = 0;
    int fd = open(file_name, O_RDONLY);
    if (fd != -1) {
        char *addr = NULL;
        int offset = 0;
        struct stat stat_buf;
        /*get the file stat, mainly for size */
        if (!fstat(fd, &stat_buf) && (stat_buf.st_size!=0)) {
            /*mmap the file to the memory to speed up the read */
            addr = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0);
            if (addr) {
                /*only the end file need to know the last file name */
                if (strstr(file_name, END_FILE) &&
                    memcpy(&last_file_index[file_index], addr, sizeof(int))) {
                    offset += sizeof(int);
                    printf("Last file is: %d\n",
                           last_file_index[file_index++]);
                } else {
		    index = rindex(file_name, '/');
		    if(index==0)
                        last_file = atoi(file_name);
		    else
		        last_file = atoi(index+1);
                    last_file_index[file_index++] = (last_file == 0 ? 255 :
                                                     last_file - 1);
                }
                /*get the index total */
                memcpy(&index_total, addr + offset, sizeof(ulong64_t));
                offset += sizeof(ulong64_t);
                /*call the show trace content routine */
                show_content(addr, offset, stat_buf.st_size, index_total, 0);
                munmap(addr, SHME_CONTENT_SIZE);
            }
        }
        close(fd);
    }

}

/*show the binary trace content of the share mem*/
void
show_shmem(struct shmem_buffer *shmem_info)
{
    if (shmem_info->shm != (char *)(-1)) {
        int offset = 0;
        /*convert to correct type */
        const int *flag = (int *)(shmem_info->shm - SHME_HEADER_SIZE);
        unsigned int size = *(unsigned int *)(shmem_info->shm - SHME_LEN_SIZE
                                              - SHME_LEN_REAL_SIZE -
                                              SHME_LEN_TOTAL_SIZE -
                                              SHME_PID_SIZE);
        unsigned int real_size =
            *(unsigned int *)(shmem_info->shm - SHME_LEN_REAL_SIZE -
                              SHME_LEN_TOTAL_SIZE - SHME_PID_SIZE);
        unsigned int total_size =
            *(unsigned int *)(shmem_info->shm - SHME_LEN_TOTAL_SIZE -
                              SHME_PID_SIZE);
        const pid_t *pid = (pid_t *) (shmem_info->shm - SHME_PID_SIZE);
        const char *shm = shmem_info->shm;
        /*just show them */
        printf("SHM:FLAG=%d\n", *flag);
        printf("SHM:SIZE=%d\n", size);
        printf("SHM:REAL_SIZE=%d\n", real_size);
        printf("SHM:TOTAL_SIZE=%d\n", total_size);
        printf("SHM:Collector PID=%d\n", *pid);
        printf("Message: \n");
        /*call the show trace content routine */
        if(size>0)
            show_content(shm, offset, size, 0, 1);
    }
}

/*clean the resourse*/
void
clean()
{
    int i, j;
    key_state_t *key = NULL;
    key_state_t *old_key = NULL;
    connection_state_t *con = NULL;
    connection_state_t *old_con = NULL;
    interval_state_t *interval = NULL;
    interval_state_t *old_interval = NULL;
    cmd_item_t *item = NULL;
    cmd_item_t *old_item = NULL;
    for (i = 0; i < BUCKET_MAX_NUM; i++) {
        key = key_state_counter[i];
        while (key) {
            old_key = key;
            item = old_key->cmd_head;
            while (item) {
                old_item = item;
                item = item->next;
                SAFE_FREE(old_item)
            }
            key = key->next;
            SAFE_FREE(old_key)
        }
    }
    for (i = 0; i < MAX_CONNECTION; i++) {
        con = connection_state_counter[i];
        while (con) {
            old_con = con;
            for (j = 0; j < MAX_INTERVAL; j++) {
                interval = con->interval_counter[j];
                while (interval) {
                    old_interval = interval;
                    interval = interval->next;
                    SAFE_FREE(old_interval)
                }
            }
            con = con->next;
            SAFE_FREE(old_con)
        }
    }
    for (i = 0; i < MAX_INTERVAL; i++) {
        interval = interval_state_counter[i];
        while (interval) {
            old_interval = interval;
            interval = interval->next;
            SAFE_FREE(old_interval)
        }
    }
    memset(command_type_counter, 0, sizeof(ulong64_t) * TOTAL_COMMAND_TPYES);
    memset(cmd_code_counter, 0, sizeof(ulong64_t) * TOTAL_COMMAND_TPYES
           * STATUS_MAX_NUMBER);
    memset(key_size_counter, 0, sizeof(ulong64_t) * KEY_MAX_LENGTH);
    memset(data_size_counter, 0, sizeof(ulong64_t) * DATA_MAX_SIZE);
    memset(key_state_counter, 0, sizeof(key_state_t *) * BUCKET_MAX_NUM);
    memset(connection_state_counter, 0, sizeof(connection_state_t *)
           * MAX_CONNECTION);
    memset(connection_interval_otime,0,sizeof(uint32_t) * MAX_CONNECTION);
    memset(interval_state_counter, 0, sizeof(interval_state_t *)
           * MAX_INTERVAL);

    total_counter = 0;
    total_miss = 0;
    total_objects = 0;
    total_object_sizes = 0;
    total_errors = 0;
    total_connections = 0;
    total_intervals = 0;
    total_gets = 0;
    total_fail_gets = 0;
    rev_total = 0;
    send_total = 0;
    start_time = 0;
    end_time = 0;
}

/*split the string*/
void
str_split(char *str, const char *delim, char *values[], int size)
{
    char *token;
    char *saved_str = NULL;
    int i = 0;
    token = strtok_r(str, delim, &saved_str);
    for (i = 0; i < size && token; i++) {
        values[i] = token;
        token = strtok_r(NULL, delim, &saved_str);
    }
}

/*get the last file of this file*/
void
get_last_file(const char *file_base_name, char *file_name, int file)
{
    char *index;
    strcpy(file_name, file_base_name);
    index = rindex(file_name, '/');
    if(index!=0)
    	sprintf(index + 1, "%d", file);
    else
	sprintf(file_name, "%d", file);
}

/*prepare files array for file open*/
void
init_files()
{
    file_names = plat_alloc(sizeof(char *) * MAX_FILE_NUMBER);
    memset(file_names, 0, MAX_FILE_NUMBER);
    last_file_number = plat_alloc(sizeof(int) * MAX_FILE_NUMBER);
    memset(last_file_number, 0, MAX_FILE_NUMBER);
    last_file_index = plat_alloc(sizeof(int) * MAX_FILE_NUMBER);
    memset(last_file_index, 0, MAX_FILE_NUMBER);
}

/*clean the file array*/
void
clear_files()
{
    SAFE_FREE(file_names);
    SAFE_FREE(last_file_number);
    SAFE_FREE(last_file_index);
}

/*just view the share content*/
int
main(int argc, char **argv)
{
    /*get the all information to see */
    int cret;
    int i = 0, j = 0;
    char *values[2] = { 0 };
    char file_opt[MAX_FILE_OPT] = "";
    char file_opt_name[MAX_FILE_OPT] = "";
    memset(file_opt_name, 0, MAX_FILE_OPT);
    int index = 0;
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "END_FILE=", END_FILE, 1);
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "MAX_FILE_NUMBER=",
                          MAX_FILE_NUMBER, 0);
    /*init the buffer size from config file */
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "CONTENT_SIZE=",
                          SHME_CONTENT_SIZE, 0);
    SHME_CONTENT_SIZE = SHME_CONTENT_SIZE << 20;
    init_files();
    while ((cret = getopt(argc, argv, "abcdef:ghigklmnopqrstuvwxyz")) != EOF) {
        switch (cret) {
        case 'm':
            {
                struct shmem_buffer *shmem_buffer_ptr = get_shmem_info(0);
                if (*(int *)(shmem_buffer_ptr->shm - SHME_HEADER_SIZE) == 0) {
                    printf("Buffer A:\n");
                    show_shmem(shmem_buffer_ptr);
                    printf("Buffer B:\n");
                    show_shmem(get_shmem_info(1));
                } else {
                    printf("Buffer B:\n");
                    show_shmem(get_shmem_info(1));
                    printf("Buffer A:\n");
                    show_shmem(get_shmem_info(0));
                }
            }
            break;
        case 'f':
            strcpy(file_opt, optarg);
            str_split(optarg, ",", values, 2);
            file_names[index] = values[0];
            if (values[1]) {
                last_file_number[index] = atoi(values[1]);
                if (last_file_number[index] > 256) {
                    printf("Max value of the last files is %d, so %d is "
                           "not supported!\n", MAX_FILE_NUMBER,
                           last_file_number[index]);
                    return 1;
                }
            }
            index++;
            break;
        case 'a':
            filter |= SHOW_ALL;
            break;
        case 's':
            filter |= SHOW_STATUS;
            break;
        case 'c':
            filter |= SHOW_COMAMND_DIS;
            break;
        case 'k':
            filter |= SHOW_KEYSIZE_DIS;
            break;
        case 'd':
            filter |= SHOW_DATASIZE_DIS;
            break;
        case 'r':
            filter |= SHOW_REQUEST_DIS;
            break;
        case 'u':
            filter |= SHOW_CVSU_DIS;
            break;
        case 'w':
            filter |= SHOW_CMDCODE_DIS;
            break;
        case 'o':
            filter |= SHOW_CONNECTION_DIS;
            break;
        case 'i':
            filter |= SHOW_INTERVAL_DIS;
            break;
        case 'x':
            filter |= SHOW_DUMP_DIS;
            break;
        case 'h':
            {
                printf(HELP_STRING);
            }
            break;
        default:
            break;
        }
    }
    for (i = 0; i < index; i++) {
        if (file_names[i]) {
            show_file(file_names[i]);
        }
        /*remember do clean */
        clean();
        /*if is a "end,10" format, show the last last 10 files too */
        if (last_file_number[i] != 0) {
            for (j = 0; j < last_file_number[i]; j++) {
                /*get the last file */
                get_last_file(file_names[i], file_opt_name,
                              (last_file_index[i] - j +
                               MAX_FILE_NUMBER) % MAX_FILE_NUMBER);
                /*show the file */
                show_file(file_opt_name);
                /*remember do clean */
                clean();
            }
        }
    }
    clear_files();
    return 0;
}
