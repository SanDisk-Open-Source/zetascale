/*
* File:   sdf/platform/util_shmem_collector.c
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
#include <sys/ipc.h>
#include <sys/shm.h>
#include <pwd.h>
#include <unistd.h>
#include <memory.h>
#include <signal.h>
#include <malloc.h>
#include <pthread.h>

#include "platform/stdio.h"
#include "platform/stdlib.h"
#include "platform/spin_rw.h"
#include "platform/util_trace.h"
#include "platform/unistd.h"
#include "platform/platexits.h"

#define MAX_FILE_NAME 256
#define CONFIG_FILE "/opt/schooner/memcached/config/bin-trace.prop"

/*if the buffer could be over write*/
static uint32_t CIRCLE_TRACE = 1;
/*the log dir of the trace*/
static char LOG_DIR[MAX_FILE_NAME] = "/tmp/log/";
/*the end file used for last dump*/
static char END_FILE[MAX_FILE_NAME] = "end";
/*adjust the MAX_FILE_NUMBER value if you want to adjust the number of file that
keep the trace log,default is 256 files*/
static uint32_t MAX_FILE_NUMBER = 256;
/*the buffer size, default is 50M*/
static unsigned long long SHME_CONTENT_SIZE = 50;
/*the shmem buffer A*/
static struct shmem_buffer *shmem_info_A = 0;
/*the shmem buffer B*/
static struct shmem_buffer *shmem_info_B = 0;


/*the file struct to dump the log content*/
struct file_dump
{
    char file_name[MAX_FILE_NAME];
    FILE *fp;
};
/*a large file array for file dump*/
static struct file_dump *files = NULL;
/*the file index for each dump*/
static unsigned long long file_index = 0;
/*the mask for signals*/
static sigset_t mask, oldmask;
/**
*The collector dump the share memroy to the local file.
*/
void
collector(int value, struct shmem_buffer *shmem_info)
{
    unsigned long long index = 0;
    unsigned long long index_total = 0;
    /*make sure the adding of file_index is thread safe */
    index_total = __sync_fetch_and_add(&file_index, 1);
    /*if did not allow circle trace,will return if all the files is full */
    if (!CIRCLE_TRACE && index_total >= MAX_FILE_NUMBER)
        return;
    /*make a local copy, in case the collector would be interrupted by other
       collectors */
    index = index_total % MAX_FILE_NUMBER;
    /*if not sync all, we need to protect the buffer if we copy to local global buffer, but for perf reason, we would not do that, so the old data may not be right one, but at least the latest one is right. */
    /*no need to copy to local buffer, just copy the shmem, as copy to a unprotected local buffer would not ensure the data is right. maybe, a local buffer is better, but that would make the memory usage out of control, so just skip the local copy. */
    //memcpy(buffer,shmem_info->shm,value);
#ifdef DEEP_SYNC_ALL
    /*wait the real size if the right one */
    while (*(shmem_info->real_size) != value);
#endif
    if (files[index].fp) {
        /*sometimes, seek would fail, so this is more safe */
        //while(fseek(files[index].fp,0,SEEK_SET)!=0);
        /*seek to the begining */
        fseek(files[index].fp, 0, SEEK_SET);
        //printf("total size %d : %d\n",value,index);
        /*do the fwrite */
        fwrite(&index_total, sizeof(unsigned long long), 1, files[index].fp);
        fwrite(shmem_info->shm, value, 1, files[index].fp);
    } else {
        printf("can not open file, skip %s\n", files[index].file_name);
    }
    printf("total size of file %lld is %d\n", index, value);
#ifdef SYNC_ALL
    *(shmem_info->real_size) = 0;
    *(shmem_info->size) = 0;
#endif
}

/**
* The collector of buffer A
**/
void
collector_A(int sig, siginfo_t * info, void *act)
{
    collector(info->si_value.sival_int, shmem_info_A);
}

/**
* The collector of buffer B
**/
void
collector_B(int sig, siginfo_t * info, void *act)
{
    collector(info->si_value.sival_int, shmem_info_B);
}

/**
*The cleaner to clean out the content when exit or Ctlr+C
*/
void
interrupt()
{
    printf("exit...\n");
    int i = 0;
    /*close all file */
    for (i = 0; i < MAX_FILE_NUMBER; i++) {
        fclose(files[i].fp);
    }
    /*clean the pid flag */
    *(shmem_info_A->pid) = 0;
    *(shmem_info_B->pid) = 0;
    /*free the buffer */
    //free(buffer);
    /*get the buffer that is not full */
    struct shmem_buffer *shmem_info = shmem_info_A;
    /*even number for buffer A, and odd number for buffer B*/
    if ( file_index%2 == 1)
        shmem_info = shmem_info_B;
    /*flush the left content */
    printf("buffer total size %d\n", (*shmem_info->size));
    /*clear the share memory to the file LOG_END_FILE */
    char file_name[MAX_FILE_NAME];
    sprintf (file_name, "%s/%s", LOG_DIR, END_FILE);
    FILE *fp = fopen(file_name, "wb+");
    unsigned long long last_index = file_index % MAX_FILE_NUMBER - 1;
    if (fp) {
        fwrite(&(last_index), sizeof(int), sizeof(char), fp);
        fwrite(&file_index, sizeof(unsigned long long), 1, fp);
        fwrite(shmem_info->shm, (*shmem_info->size), sizeof(char), fp);
        fflush(fp);
        fclose(fp);
    } else {
        printf("flush the end file failed\n!");
    }
    /*clean the shmem */
    //memset(shmem_info_A->shm - SHME_HEADER_SIZE, '\0', SHME_MAX_SIZE);
    //memset(shmem_info_B->shm - SHME_HEADER_SIZE, '\0', SHME_MAX_SIZE);
    /*restore the sig mask */
    sigprocmask(SIG_UNBLOCK, &mask, NULL);
    /*free the files */
    plat_free(files);
    /*do exit */
    plat__exit(PLAT_EXIT_OK);
}
/**
* make the collector to be another user
*/
static void become_user(const char* username)
{
    int rval;
    struct passwd *pw;
 
    pw = getpwnam(username);
    if ( pw == NULL )
    {
        printf("user '%s' does not exist\n", username);
        plat__exit(PLAT_EXIT_FAILED_RESTART);
    }

    rval = getuid();
    if ( rval != pw->pw_uid )  
    {
        if ( rval != 0 )
        {
            printf("Must be root to setuid to \"%s\"\n", username);
            plat__exit(PLAT_EXIT_FAILED_RESTART);
        }
        rval = setuid(pw->pw_uid); 
        if ( rval < 0 )
        {
            printf("exiting. setuid '%s' error\n", username);
            plat__exit(PLAT_EXIT_FAILED_RESTART);
        }
        else
        {
            printf("switch to user : %s\n",username);
        }    
    }
}
/**
* the main entrance of the colloctor process
*/
int
main(int argc, char **argv)
{
    struct sigaction act_full_A, act_full_B;
    /*change the user if we need*/    
    if(argc>=3&&0==strcmp(argv[1],"-u"))
    {
        become_user(argv[2]);
    }
    /*get the two buffers */
    shmem_info_A = get_shmem_info(0);
    shmem_info_B = get_shmem_info(1);
    /*get the config parameter */
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "LOG_DIR=", LOG_DIR, 1);
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "END_FILE=", END_FILE, 1);
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "CIRCLE_TRACE=", CIRCLE_TRACE,
                          0);
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "MAX_FILE_NUMBER=",
                          MAX_FILE_NUMBER, 0);
    /*init the buffer size from config file */
    INIT_CONFIG_PARAMETER(TRACE_CONFIG_FILE, "CONTENT_SIZE=",
                          SHME_CONTENT_SIZE, 0);
    SHME_CONTENT_SIZE = SHME_CONTENT_SIZE << 20;
    /*prepare the log file */
    if (shmem_info_A->shm != (char *)(-1)) {
        /*mk the dir if not exsits,493==0755 */
        mkdir(LOG_DIR, 493);
        /*ini the size, real_size and pid */
        //*(shmem_info_A->size) = 0;
        //*(shmem_info_A->real_size) = 0;
        *(shmem_info_A->pid) = plat_getpid();
        //*(shmem_info_B->size) = 0;
        //*(shmem_info_B->real_size) = 0;
        *(shmem_info_B->pid) = plat_getpid();
        //buffer=malloc(SHME_MAX_SIZE*sizeof(char));
        /*open all files here */
        int i = 0;
        files = plat_alloc(sizeof(struct file_dump) * MAX_FILE_NUMBER);
        for (i = 0; i < MAX_FILE_NUMBER; i++) {
            sprintf(files[i].file_name, "%s/%d", LOG_DIR, i);
            files[i].fp = fopen(files[i].file_name, "wb+");
        }
        /*catch the int and term */
        signal(SIGINT, interrupt);
        signal(SIGTERM, interrupt);
        /*set the handler of two singnal */
        act_full_A.sa_sigaction = collector_A;
        act_full_A.sa_flags = SA_SIGINFO;
        act_full_B.sa_sigaction = collector_B;
        act_full_B.sa_flags = SA_SIGINFO;
        sigaction(SHME_FULL_A, &act_full_A, NULL);
        sigaction(SHME_FULL_B, &act_full_B, NULL);
        /*only the SHME_FULL_A and SHME_FULL_B would be catched */
        sigemptyset(&mask);
        sigaddset(&mask, SHME_FULL_A);
        sigaddset(&mask, SHME_FULL_B);
        sigprocmask(SIG_BLOCK, &mask, &oldmask);
        /*block to wait the two singals */
        while (1) {
            sigsuspend(&oldmask);
        }
        interrupt();
    }
    return 0;
}
