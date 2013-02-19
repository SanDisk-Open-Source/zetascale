/*
 * FDF Admin Interface.
 *
 * Author: Manavalan Krishnan
 *
 * Copyright (c) 2013 SanDisk Corporation.  All rights reserved.
 */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h> 
#include "fdf.h"
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "sdf.h"
#include "sdf_internal.h"
#include "fdf.h"
#include "utils/properties.h"
#include "protocol/protocol_utils.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/async_puts.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/replicator_adapter.h"
#include "ssd/fifo/mcd_ipf.h"
#include "ssd/fifo/mcd_osd.h"
#include "ssd/fifo/mcd_bak.h"
#include "shared/init_sdf.h"
#include "shared/private.h"
#include "shared/open_container_mgr.h"
#include "shared/container_meta.h"
#include "shared/name_service.h"
#include "shared/shard_compute.h"
#include "shared/internal_blk_obj_api.h"
#include "agent/agent_common.h"
#include "agent/agent_helper.h"
#include "fdf_internal.h"

/*
 * Functions defined in fdf.c
 */

static admin_config_t admin_config;


typedef struct {
    STATS_PRINT_TYPE type;
    char cname[128];
    int state; /* 0 - not running, 1 - running */
}stats_dump_cfg_t;
static stats_dump_cfg_t dump_thd_cfg;


#define MAX_CMD_TOKENS 8
#define STATS_BUFFER_SIZE 1024 

#define LOG_ID PLAT_LOG_ID_INITIAL
#define LOG_CAT PLAT_LOG_CAT_SDF_NAMING
#define LOG_DBG PLAT_LOG_LEVEL_DEBUG
#define LOG_INFO PLAT_LOG_LEVEL_INFO
#define LOG_ERR PLAT_LOG_LEVEL_ERROR
#define LOG_WARN PLAT_LOG_LEVEL_WARN
#define LOG_FATAL PLAT_LOG_LEVEL_FATAL

FDF_status_t print_container_stats_by_name(struct FDF_thread_state *thd_state,
                              int fd, char *cname, int stats_type);
FDF_status_t print_container_stats_by_cguid( struct FDF_thread_state *thd_state,
                                   int fd, FDF_cguid_t cguid, int stats_type);
void *FDFStatsDumpThread(void *arg);



/* Admin Commands 
 * container stats <detailed|mini> <cname>
 * container stats_dump <detailed|mini> <cname|all>
 * container list
 */

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_adm_cmd(char *command, cmd_token_t *tokens, 
                                          const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }
            
            break; /* string end */
        }
    }
    
    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;
    return ntokens;
}

FDF_status_t write_socket(int conn_fd, char *data, int len ) {
    int bytes_written, rc;
    int err = 0;

    bytes_written = 0;
    while( bytes_written != len ) {
        rc = write(conn_fd,data+bytes_written, (len - bytes_written));
        if ( rc < 0 ) {
            if ( err > 3 ) {
                plat_log_msg(160053, LOG_CAT, LOG_ERR,
                     "Unable to write string %s",data+bytes_written);
                return FDF_FAILURE;
            }
            err++;
            continue;
        }
        bytes_written = bytes_written + rc;
        err = 0;
    }
    return FDF_SUCCESS;
}

char *get_stats_catogory_desc_str(FDF_STATS_TYPE type) {
    if( type == FDF_STATS_TYPE_APP_REQ ) {
        return "Application requests";
    }
    else if( type == FDF_STATS_TYPE_FLASH ) {
        return "Flash statistics";
    }
    else if( type == FDF_STATS_TYPE_OVERWRITES )  {
        return "Overwrite and write-through statistics";
    }
    else if( type == FDF_STATS_TYPE_CACHE_TO_FLASH )  {
        return "Cache to Flash Manager requests";
    }
    else if( type == FDF_STATS_TYPE_FLASH_TO_CACHE )  {
        return "Flash Manager responses to cache";
    }
    else if( type == FDF_STATS_TYPE_FLASH_RC )  {
        return "Flash layer return codes";
    }
    else if( type == FDF_STATS_TYPE_PER_CACHE )  {
        return "Cache statistics";
    }
    else if( type == FDF_STATS_TYPE_FLASH_MANAGER )  {
        return "Flash Manager requests/responses";;
    }
    return "Unknown type:";
}

void print_stats(int fd, FDF_stats_t *stats) {
    int i, len, stats_type, category;
    char buf[STATS_BUFFER_SIZE * 4];
  
    //fprintf(stderr,"Returnng from print_stats\n");
    len = sprintf(buf," %s:\n",
                           get_stats_catogory_desc_str(FDF_STATS_TYPE_APP_REQ));
    write_socket(fd,buf,len);
    for (i = 0; i < FDF_N_ACCESS_TYPES; i++ ) {
        if( stats->n_accesses[i] == 0 ) {
            continue;
        }
        len = sprintf(buf,"   %s = %lu\n",get_access_type_stats_desc(i),
                                                       stats->n_accesses[i]);
        write_socket(fd, buf, len);
    }
    stats_type = FDF_STATS_TYPE_OVERWRITES; 
    len = sprintf(buf," %s:\n",
                      get_stats_catogory_desc_str(FDF_STATS_TYPE_OVERWRITES));
    write_socket(fd,buf,len);
    for (i = 0; i < FDF_N_CACHE_STATS; i++ ) {
        if( stats->cache_stats[i] == 0 ) {
            continue;
        }
        category = get_cache_type_stats_category(i); 
        if ( category != stats_type )  {
            len = sprintf(buf," %s:\n",
                                   get_stats_catogory_desc_str(category));
            write_socket(fd,buf,len);
            stats_type = category;
        }
        len = sprintf(buf,"   %s = %lu\n",get_cache_type_stats_desc(i),
                                                  stats->cache_stats[i]);
        write_socket(fd, buf, len);
    }
#if 0
    len = sprintf(buf,"[%s]\n",
                           get_stats_catogory_desc_str(FDF_STATS_TYPE_FLASH));
    write_socket(fd,buf,len);
 
    for (i = 0; i < FDF_N_FLASH_STATS; i++ ) {
        if( stats->flash_stats[i] == 0 ) {
            continue;
        }
        //fprintf(stderr,"Assiging stats\n");
        len = sprintf(buf,"   %s = %lu\n",get_flash_type_stats_desc(i),
                                          stats->flash_stats[i]);
        //fprintf(stderr,"Writing to socket\n");
        write_socket(fd, buf, len);
    }
#endif
}

int open_stats_dump_file() {
    int fd;
    const char *file_name;
    /* Open the file */
    file_name = getProperty_String("FDF_STATS_FILE","/var/log/fdfstats.log");
    fd = open( file_name, O_APPEND|O_CREAT|O_RDWR);
    if( fd < 0 ) {
        plat_log_msg(160054, LOG_CAT, LOG_ERR,
                     "Unable to open stats file %s",file_name);
        return fd;

    }
    return fd;
}

FDF_status_t dump_container_stats_by_name(struct FDF_thread_state *thd_state,
                                                 char *cname, int stats_type) {
    int fd,rc;
      
    /* Open the file */
    fd = open_stats_dump_file();
    if( fd < 0 ) {
        return FDF_FAILURE;
    }
    write_socket(fd,"Test\n",4);
    rc = print_container_stats_by_name(thd_state,fd,cname,stats_type);
    close(fd);
    return rc;
}

FDF_status_t dump_container_stats_by_cguid(struct FDF_thread_state *thd_state,
                                           FDF_cguid_t cguid, int stats_type) {
    int fd,rc;
        
    /* Open the file */
    fd = open_stats_dump_file();
    if( fd < 0 ) {
        return FDF_FAILURE;
    }
    rc = print_container_stats_by_cguid(thd_state,fd,cguid,stats_type);
    close(fd);
    return rc;
}       

FDF_status_t dump_all_container_stats(struct FDF_thread_state *thd_state,
                                                               int stats_type) {
    int i,fd;
    uint32_t n_cguids;
    FDF_cguid_t cguids[MCD_MAX_NUM_CNTRS];

    FDFGetContainers(thd_state,cguids,&n_cguids);
    if( n_cguids <= 0 ) {
        plat_log_msg(160055, LOG_CAT, LOG_ERR,
                           "No container exists");
        return FDF_FAILURE;
    }
    fd = open_stats_dump_file();
    if( fd < 0 ) {
        return FDF_FAILURE;
    }
    for ( i = 0; i < n_cguids; i++ ) {
        print_container_stats_by_cguid(thd_state,fd,cguids[i],stats_type);
    }
    write_socket(fd,"--------\n",sizeof("--------\n"));
    return FDF_SUCCESS;
}

FDF_status_t print_container_stats_by_name(struct FDF_thread_state *thd_state,
                              int fd, char *cname, int stats_type) {
    int len;
    FDF_cguid_t cguid;
    char stats_buffer[STATS_BUFFER_SIZE];

    /* Find cgiud for given container name */
    cguid = FDFGetCguid(cname);
    if ( cguid == SDF_NULL_CGUID ) {
        len = sprintf(stats_buffer,"Container %s not found", cname);
        write_socket(fd,stats_buffer,len);
        return FDF_FAILURE;
    }
    //fprintf(stderr,"Calling print_container_stats_by_cguid...\n");
    return print_container_stats_by_cguid(thd_state,fd,cguid,stats_type);
}

FDF_status_t print_container_stats_by_cguid( struct FDF_thread_state *thd_state,
                                   int fd, FDF_cguid_t cguid, int stats_type) {
    int len,i;
    time_t t;
    FDF_status_t rc;
    FDF_stats_t stats;
    char stats_buffer[STATS_BUFFER_SIZE], *cname;
    FDF_container_props_t props;
    uint64_t num_objs = 0;
    uint64_t used_space = 0;   

    /* Get container name */
    cname = FDFGetContainerName(cguid);
    if( cname == NULL ) {
        len = sprintf(stats_buffer,"Unable to container name for cguid %lu",
                                                                       cguid);
         write_socket(fd,stats_buffer,len);
         return FDF_FAILURE;
    }

    /* Get container properties and print */
    rc = FDFGetContainerProps(thd_state,cguid,&props);
    if ( rc != FDF_SUCCESS ) {
         len = sprintf(stats_buffer,"Unable to get container properties for %s" 
                                "(error:%u)",cname,rc);
         write_socket(fd,stats_buffer,len);
         return FDF_FAILURE;
    }
    time(&t);
    /* Print the container properties */
    get_cntr_info(cguid,NULL, 0, &num_objs, &used_space, NULL);
    len = sprintf(stats_buffer,"Timestamp:%s[Per Container Statistics]\n"
                          " Container Properties:\n"
                          "   Name        :%s\n"
                          "   Cguid       :%lu\n"
                          "   Size        :%lukb\n"
                          "   Persistence :%d\n"
                          "   Eviction    :%d\n"
                          "   Writethrough:%d\n"
                          "   FIFO        :%d\n"
                          "   Asyncwrites :%d\n"
                          "   Durability  :%u\n"
                          "   Num_objs    :%lu\n"
                          "   Used_space  :%lu\n",ctime(&t),
                          cname,cguid, props.size_kb, props.persistent,
                          props.evicting,props.writethru,props.fifo_mode,
                          props.async_writes, props.durability_level,
                          num_objs, used_space);
    write_socket(fd,stats_buffer,len);
    /* Get Per container stats */
    rc = FDFGetContainerStats(thd_state,cguid,&stats); 
    if ( rc != FDF_SUCCESS ) {
        len = sprintf(stats_buffer,"Stats failed for %s(error:%u)", 
                                             cname,rc);
        write_socket(fd,stats_buffer,len);    
        return FDF_FAILURE;
    }
    print_stats(fd,&stats);
    
    if( stats_type != STATS_PRINT_TYPE_DETAILED ) {   
        return FDF_SUCCESS;
    }
    /* Print Flash layer statistics */
    len = sprintf(stats_buffer,"[Overall FDF Statistics]\n");
    write_socket(fd,stats_buffer,len);
    len = sprintf(stats_buffer," %s:\n",
                           get_stats_catogory_desc_str(FDF_STATS_TYPE_FLASH));
    write_socket(fd,stats_buffer,len);
    for (i = 0; i < FDF_N_FLASH_STATS; i++ ) {
        if( stats.flash_stats[i] == 0 ) {
            continue;
        }
        //fprintf(stderr,"Assiging stats\n");
        len = sprintf(stats_buffer,"   %s = %lu\n",get_flash_type_stats_desc(i),
                                          stats.flash_stats[i]);
        //fprintf(stderr,"Writing to socket\n");
        write_socket(fd, stats_buffer, len);
    }
    write_socket(fd," Flash layout:\n",sizeof("Flash layout:\n"));
    fdf_get_flash_map(thd_state,cguid,stats_buffer,&len);        
    write_socket(fd,stats_buffer,len);

    /* Get the Total Flash stats */
    rc = FDFGetStats(thd_state,&stats);
    if ( rc != FDF_SUCCESS ) {
        len = sprintf(stats_buffer,"Stats failed for %s(error:%u)",
                                             cname,rc);
        write_socket(fd,stats_buffer,len);    
        return FDF_FAILURE;
    }
    print_stats(fd,&stats);
    return FDF_SUCCESS;
}

static void process_container_cmd(struct FDF_thread_state *thd_state, 
                       int conn_fd, cmd_token_t *tokens, size_t ntokens){
    int len,rc,i;
    int stats_type = 0;
    char stats_buffer[STATS_BUFFER_SIZE], *cname;
    pthread_attr_t attr;
    pthread_t thread;
    //fprintf(stderr,"Number of tokens:%d\n",(int)ntokens);
    if ( ntokens < 2 ) {
        len = sprintf(stats_buffer,"Invalid argument:"
                                                 " Type help for more info\n");
        write_socket(conn_fd,stats_buffer,len);
        return;
    }
    if( strcmp(tokens[1].value,"stats" ) == 0 ){
        if( ntokens < 3 ) {
            len = sprintf(stats_buffer,"Invalid arguments:" 
                                                 " Type help for more info\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        if( ntokens >= 4 ) { 
            if( strcmp(tokens[3].value,"v" ) == 0) {
                stats_type = STATS_PRINT_TYPE_DETAILED;
            }
        }
        print_container_stats_by_name(thd_state, conn_fd, 
                                                  tokens[2].value, stats_type);
        return;
    }
    else if( strcmp(tokens[1].value,"stats_dump" ) == 0 ){
        if( is_auto_dump_enabled() ) {
           len = sprintf(stats_buffer,"Periodic stats dump has been enabled."
                                             " Can not dump container stats\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        if( ntokens < 3 ) {
            len = sprintf(stats_buffer,"Invalid arguments for stats_dump:" 
                                                 " Type help for more info\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        /* Check if dump thread is already running */
        if( dump_thd_cfg.state == 1 ) {
             len = sprintf(stats_buffer,"A dump is already in progress."  
                                                           "Pls try later\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        if( dump_thd_cfg.state == 1 ) {
             len = sprintf(stats_buffer,"A dump is already in progress."
                                                           "Pls try later\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        if( ntokens >= 4 ) { 
            if( strcmp(tokens[3].value,"v" ) == 0 ) {
                stats_type = STATS_PRINT_TYPE_DETAILED;
            }
        }
        dump_thd_cfg.state = 1;
        strcpy(dump_thd_cfg.cname,tokens[2].value);
        dump_thd_cfg.type = stats_type;

        /* Create dump thread */
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        rc = pthread_create(&thread, &attr,FDFStatsDumpThread, NULL);
        if( rc ) {
            len = sprintf(stats_buffer,"Unable to create dump thread\n");
            write_socket(conn_fd,stats_buffer,len);
            dump_thd_cfg.state = 0;
        }
    }
    else if( strcmp(tokens[1].value,"autodump" ) == 0 ) {
        if( ntokens < 3 ) {
            len = sprintf(stats_buffer,"Invalid arguments for autodump"
                                                 " Type help for more info\n");
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
        if( strcmp(tokens[2].value,"enable") == 0 ) {
            enable_stats_auto_dump();
        }
        else if( strcmp(tokens[2].value,"disable") == 0 ) {
            disable_stats_auto_dump();
        }
        else {
            len = sprintf(stats_buffer,"Invalid subcommand %s\n",
                                           tokens[2].value);
            write_socket(conn_fd,stats_buffer,len);
            return;
        }
    }
    else if( strcmp(tokens[1].value,"list" ) == 0 ) {
        for( i = 4; i < MCD_MAX_NUM_CNTRS; i++ ) {
            cname = FDFGetContainerName((FDF_cguid_t)i);
            if ( strcmp(cname,"") != 0) {     
                len = sprintf(stats_buffer,"%s\n",cname);
                write_socket(conn_fd,stats_buffer,len);
            }
        }
    }
    else {
        //fprintf(stderr,"Command:(%s)\n",tokens[1].value);
        len = sprintf(stats_buffer,"Invalid command:(%s). Type help for list"
                         " of commands\n",tokens[1].value);
        write_socket(conn_fd,stats_buffer,len);
    }
}

static void print_admin_command_usage(int conn_fd) {
    int len;
    char stats_buffer[STATS_BUFFER_SIZE * 2];
    len = sprintf(stats_buffer,"\nSupported commands:\n" 
                   "container stats <container name> [v]\n"
                   "container stats_dump <container name|all> [v]\n"
                   "container autodump   <enable/disable>\n"
                   "container list\nhelp\nquit\n\n");
 
    write_socket(conn_fd,stats_buffer,len);
}

static FDF_status_t process_admin_cmd( struct FDF_thread_state *thd_state, 
                                                      int conn_fd, char *cmd ) {
    int len;
    size_t ntokens;
    cmd_token_t tokens[MAX_CMD_TOKENS];
    char stats_buffer[STATS_BUFFER_SIZE];

    /* Parse the command */
    ntokens = tokenize_adm_cmd(cmd, tokens, MAX_CMD_TOKENS);
    ntokens--;
    //fprintf(stderr,"Input command:(%s) ntokens:%u\n",cmd,(int)ntokens);
    if ( ntokens <= 0 ) {
        len = sprintf(stats_buffer,"Please specify a command." 
                                           "Type help for list of commands\n");
        write_socket(conn_fd,stats_buffer,len);
        return FDF_FAILURE;
    }  
    /*
    for(len = 0; len < ntokens; len++ ) {
       fprintf(stderr,"token[%d]:%s\n",len,tokens[len].value);
    } */
    if( strcmp(tokens[0].value,"container") == 0 ) {
        process_container_cmd(thd_state, conn_fd,tokens,ntokens);
    }
    else if( strcmp(tokens[0].value,"help") == 0 ) {
        print_admin_command_usage(conn_fd);
    }
    else if( strcmp(tokens[0].value,"quit") == 0 ) {
         close(conn_fd);
    }
    else {
        len = sprintf(stats_buffer,"Invalid command:(%s)." 
                          "Type help for list of commands\n",cmd);
        write_socket(conn_fd,stats_buffer,len);
        return FDF_FAILURE;
    }
    return FDF_SUCCESS;
}

#define CMD_BUFFER_SIZE 256

void *FDFStatsDumpThread(void *arg) {
    int ret;
    struct FDF_thread_state *thd_state;
    //fprintf(stderr,"Starting Dump thread...\n");

    ret = FDFInitPerThreadState(admin_config.fdf_state,
                                 (struct FDF_thread_state **) &thd_state );
    if (ret != FDF_SUCCESS) {
        plat_log_msg(160056,LOG_CAT, LOG_ERR,
                    "Unable to create thread state(%d)\n", ret);
        dump_thd_cfg.state = 0;
        return NULL;
    }
    if( strcmp(dump_thd_cfg.cname,"all") == 0 ) {
        dump_all_container_stats(thd_state,dump_thd_cfg.type);
    }
    else {
        dump_container_stats_by_name(thd_state,dump_thd_cfg.cname,
                                                    dump_thd_cfg.type);
    }
    dump_thd_cfg.state = 0;
    return NULL;
}


void *FDFAdminThread(void *arg) {
    int server_fd, conn_fd, ret,i;
    struct sockaddr_in srv_addr;
    admin_config_t *adm_cfg;
    char buffer[CMD_BUFFER_SIZE];
    struct FDF_thread_state *thd_state;
    

    /* Create server socket and listen on it for commands */
    adm_cfg = (admin_config_t *)arg;

    ret = FDFInitPerThreadState( adm_cfg->fdf_state, 
                                 (struct FDF_thread_state **) &thd_state );
    if (ret != FDF_SUCCESS) {
        plat_log_msg(160056,LOG_CAT, LOG_ERR,
                            "Unable to create thread state(%d)\n", ret);
        return NULL;
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if( server_fd < 0 ) {
        plat_log_msg(160057, LOG_CAT, LOG_ERR, 
                                   "Unable to open socket for admin port");
        return 0;    
    }
    memset(&srv_addr, '0', sizeof(srv_addr));

    ret = 1;
    ret = setsockopt( server_fd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(ret) );

    srv_addr.sin_family      = AF_INET;
    srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    srv_addr.sin_port        = htons(adm_cfg->admin_port); 

    ret = bind(server_fd, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    if ( ret < 0 ) {
         plat_log_msg(160058, LOG_CAT, LOG_ERR, 
                     "Unable to bind admin port %u",adm_cfg->admin_port);
         return 0;
    }
    listen(server_fd, 5); 
    while(1)
    {
        conn_fd = accept(server_fd, (struct sockaddr*)NULL, NULL); 
        if( conn_fd < 0 ) {
            plat_log_msg(160059, LOG_CAT, LOG_ERR,
                         "Unable to accept new connections");
            continue;
        }
        /* Process the connection and respond*/
        while (1) {
            ret = read(conn_fd,buffer,CMD_BUFFER_SIZE); 
            if( ret < 0 ) {
                break;
            }
            if( ret == 0 ) {
                /*Client closed the connectio. Break */
                break;
            }
            /* Remove the \r\n char. at the end */
            for( i = 0; i < ret; i++ ) {
                if( (buffer[i] == '\r') || (buffer[i] == '\n')) {
                    buffer[i] = 0; 
                }
            } 
            process_admin_cmd(thd_state, conn_fd,buffer);
        }
        /*close the connection*/ 
        close(conn_fd);
    }
    plat_log_msg(160060, LOG_CAT, LOG_INFO,"Admin thread exiting...");
    return 0;
}
FDF_status_t fdf_start_admin_thread( struct FDF_state *fdf_state ) {
    pthread_t thd;
    int rc;

    admin_config.admin_port  = getProperty_Int( "FDF_ADMIN_PORT", 51350 );
    admin_config.num_threads = 1;
    admin_config.fdf_state   = fdf_state;

    /* Create Admin thread */    
    rc = pthread_create(&thd,NULL,FDFAdminThread,(void *)&admin_config);
    if( rc != 0 ) {
        plat_log_msg(170003,LOG_CAT, LOG_ERR,
                                         "Unable to start the stats thread");
    }
    return FDF_SUCCESS; 
}

FDF_status_t fdf_stop_admin_thread(uint16_t admin_port, uint16_t num_thds ) {
    /* Create Admin thread */  
    return FDF_SUCCESS;
}
