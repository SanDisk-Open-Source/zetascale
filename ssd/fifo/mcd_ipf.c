/*
 *  Copyright (c) Schooner Information Technology, Inc. 2009
 */
#ifndef IPF_TEST
//#include "memcached.h"
//#include "command.h"
#include "mcd_ipf.h"
#include "protocol/replication/replicator.h"
#include "protocol/replication/copy_replicator.h"
#include "protocol/replication/copy_replicator_internal.h"
#include "protocol/action/simple_replication.h"
#include "protocol/replication/sdf_vips.h"
#include "platform/logging.h"
#include "shared/private.h"
#include "shared/internal_blk_obj_api.h"
#include "protocol/protocol_common.h"
#include "protocol/action/action_thread.h"
#include "protocol/action/action_internal_ctxt.h"
#include "protocol/home/home_flash.h"
#include "protocol/action/async_puts.h"
#include "agent/agent_common.h"
#include "utils/properties.h"
#ifdef SDFAPI
#include "api/sdf.h"
#else
#include "common/sdf_properties.h"
#endif /* SDFAPI */
#include "shared/shard_meta.h"
#endif

#include <stdio.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <ctype.h>
#include <netinet/in.h>
#include <netdb.h>

#ifndef tprintf
#define tprintf(...)
#endif

#ifndef dprintf
#define dprintf(...)
#endif

#define false  0
#define true   1
#define null   ((void *) 0)

#define min(a, b)       ((a) < (b) ? (a) : (b))
#define array_size(a)   (sizeof(a) / sizeof((a)[0]))

#define if_command      "/usr/bin/sudo /sbin/ifconfig"
#define arping_command  "/usr/bin/sudo /opt/schooner/bin/arping"
#define ip_command      "/usr/bin/sudo /sbin/ip"
#define ip_tab_command  "/usr/bin/sudo /sbin/iptables"
#define route_command  "/usr/bin/sudo /sbin/route"
#define MAX_PORT_FWD_VIPS 4
typedef struct ipf_pfw_entry {
    qrep_ctnr_iface_t vips[MAX_PORT_FWD_VIPS];
    int num_vips;
    int rport;
    int vport;
}ipf_pfw_entry_t;

#ifdef VIPGROUP_SUPPORT
#define NUM_RULE_TABLES 9
#define NUM_PFW_ENTRIES 8
#define MAX_NUM_RULE_TABLES 255
#define RULE_TABLE_OFFSET   51

struct settings settings;
struct sdf_agent_state Mcd_agent_state;

unsigned char rule_table_list[MAX_NUM_RULE_TABLES]; 

unsigned int viptbl_ids[MAX_VIP_GROUPS_PER_NODE][NUM_RULE_TABLES]=
               {{211,212,213,214,215,216,217,218,219},
               {221,222,223,224,225,226,227,228,229}};
unsigned int vnode_viptbl_ids[MAX_VIP_GROUPS_PER_NODE][NUM_RULE_TABLES]=
               {{151,152,153,154,155,156,157,158,159},
               {161,162,163,164,165,166,167,168,169}};
int configure_vip(const struct sdf_vip_config *config, int vip_group_id);
int remove_vip(struct sdf_vip_config *config, int vip_group_id);
static void ipf_send ( int, uint32_t, char *, int, struct timeval);
static void ipf_send_vgroup( int,int, int, qrep_ctnr_iface_t *, int, ipf_pfw_entry_t *, struct timeval );
void ipf_handle_vip_manage_laptop(int add, int intra_node_vip_group_id);
extern int (*sdf_remove_vip)(struct sdf_vip_config *, int);
extern int msg_sdf_myrank();
extern int *msg_sdf_ranks(int *np);
#endif
#ifndef IPF_TEST
extern struct SDF_shared_state sdf_shared_state;
SDF_action_init_t *get_action_init_state();
extern int mcd_format_container_internal( void *, int);
extern int mcd_start_container_internal( void * , int  );
extern int mcd_stop_container_internal( void * , int );
extern int mcd_format_container_byname_internal( void *, char *);
extern int mcd_start_container_byname_internal( void * , char *);
extern int mcd_stop_container_byname_internal( void * , char *);
extern int mcd_get_tcp_port_by_cguid( SDF_cguid_t , int * );
extern int mcd_get_cname_by_cguid( SDF_cguid_t , char * );
extern int mcd_is_container_running(int);
extern int mcd_is_container_running_byname(char *);
extern int mcd_processing_container_cmds( void );
fthLock_t  rule_table_lock;
#endif

void put_rule_table_id();
int get_rule_table_id();

int execute_system_cmd(char *);
void ipf_signal_handler(int signal);
/*
 *  ipf_entry_t - the structure for the list of ip interfaces
 */

typedef struct ipf_entry ipf_entry_t;


struct ipf_entry {
    ipf_entry_t *   next;
/*  int             ref_count;   currently not used */
    uint32_t        address;
    char            if_1[32];
    int16_t         if_2;
    struct timeval  expiration;
#ifdef VIPGROUP_SUPPORT
    int num_vips; 
    qrep_ctnr_iface_t vips[QREP_MAX_IFS_PER_NODE];
    int num_pfws;
    ipf_pfw_entry_t pfw_entries[NUM_PFW_ENTRIES];
#endif
};

/*
 *  ipf_msg_t - structure for a command
 *
 *     Commands are passed from the memcache server process to an ip failover
 *  process.  The failover process invokes ifconfig to remove vips when the
 *  server crashes.
 */

enum {
    ipf_type_fail = 20,
    ipf_type_exit,
    ipf_type_remember,
    ipf_type_forget
};

typedef struct {
    int             command;
    uint32_t        address;
    char            if_1[32];
    int16_t         if_2;
    struct timeval  expiration;
#ifdef VIPGROUP_SUPPORT
    int num_vips; 
    qrep_ctnr_iface_t vips[QREP_MAX_IFS_PER_NODE];
    int num_pfws;
    ipf_pfw_entry_t pfw_entries[NUM_PFW_ENTRIES];
#endif
} ipf_msg_t;

static ipf_entry_t *    ipf_head;
static pthread_mutex_t  ipf_lock     = PTHREAD_MUTEX_INITIALIZER;
static int              ipf_active   = false;
static int              ipf_pipe_fd  = -1;
static int              ipf_next_vip = 1;
static int              ipf_bind_any;
static volatile int     ipf_schedule;
static stack_t          ipf_alt_stack;
static char             ipf_stack_region[256 * 1024];
static struct timeval   null_expiration = { 0 };

static struct sdf_replicator *         ipf_replicator;

/*
 *  ipf_create_if - fork a process to ifconfig a vip interface
 */

static void
ipf_create_if(uint32_t address, uint32_t netmask, char *if_1, int16_t if_2)
{
    char  command[8192];

    tprintf("ipf_create_if\n");
    snprintf(command, sizeof(command), "%s %s:%d down",
        if_command, if_1, if_2);

    system(command);

    snprintf(command, sizeof(command),
        "%s %s:%d %d.%d.%d.%d netmask %d.%d.%d.%d up",
        if_command, if_1, (int) if_2,

        (address >> 24) & 0xff,
        (address >> 16) & 0xff,
        (address >>  8) & 0xff,
        (address >>  0) & 0xff,

        (netmask >> 24) & 0xff,
        (netmask >> 16) & 0xff,
        (netmask >>  8) & 0xff,
        (netmask >>  0) & 0xff);

    system(command);

    snprintf(command, sizeof(command),
        "%s -q -c 3 -U -I %s %d.%d.%d.%d",
        arping_command,
        if_1,
        (address >> 24) & 0xff,
        (address >> 16) & 0xff,
        (address >>  8) & 0xff,
        (address >>  0) & 0xff);

    system(command); 
  
    return;
}

void configure_single_vip(qrep_ctnr_iface_t *vip) {
    int maskbits, rc;
    char system_cmd[512];
    unsigned int md[4],ip[4],nt[4];
    unsigned char bd[4];
    char netaddr[32],bcast_addr[32];
    unsigned char bitarr[16]={0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4};

    plat_log_msg(20195,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                "Configuring single vip %s ruletable:%d subnet type:%d",
                 vip->ip, vip->rule_table_id, vip->vip_subnet_type);
    /* Reset the existing route and address table */
    sprintf(system_cmd,"%s rule del table %d 2>/dev/null 1>/dev/null",
                                        ip_command, vip->rule_table_id);
    execute_system_cmd(system_cmd);
    sprintf(system_cmd,"%s route flush table %d 2>/dev/null 1>/dev/null",ip_command,
                                                                    vip->rule_table_id);
    execute_system_cmd(system_cmd);
    sprintf(system_cmd,"%s rule del from %s table main 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->ip);
    execute_system_cmd(system_cmd);

    sprintf(system_cmd,"%s rule del from %s table %srtable 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->ip, vip->name);
    execute_system_cmd(system_cmd);
    /* Configure New Address */
    sscanf(vip->ip,"%d.%d.%d.%d",&ip[0],&ip[1],&ip[2],&ip[3]);
    sscanf(vip->mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
    maskbits = bitarr[md[0] & 0xF] + bitarr[(md[0] >>4) & 0xF]+
               bitarr[md[1] & 0xF] + bitarr[(md[1] >>4) & 0xF]+
               bitarr[md[2] & 0xF] + bitarr[(md[2] >>4) & 0xF]+
               bitarr[md[3] & 0xF] + bitarr[(md[3] >>4) & 0xF] ;
    /* Network Address */
    nt[0]=ip[0] & md[0];
    nt[1]=ip[1] & md[1];
    nt[2]=ip[2] & md[2];
    nt[3]=ip[3] & md[3]; 
    sprintf(netaddr,"%u.%u.%u.%u", nt[0],nt[1],nt[2],nt[3]);

    bd[0] = ~md[0];
    bd[1] = ~md[1];
    bd[2] = ~md[2];
    bd[3] = ~md[3];

    /* Broadcast Address */
    bd[0] = bd[0] | nt[0];
    bd[1] = bd[1] | nt[1];
    bd[2] = bd[2] | nt[2];
    bd[3] = bd[3] | nt[3];
    sprintf(bcast_addr,"%u.%u.%u.%u", bd[0],bd[1],bd[2],bd[3]);

    sprintf(system_cmd,"%s %s:%d %s netmask %s broadcast %s up 1>/dev/null",if_command, vip->name,
                                              vip->rule_table_id, vip->ip,"255.255.255.255",bcast_addr);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20196, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Configure IP:%s on interface %s failed\n", vip->ip,vip->name);
    }
    /* Send Gratituos Arps*/
    sprintf(system_cmd,"%s -q -U -I %s %s -c3 1>/dev/null &",arping_command,
                                                                vip->name, vip->ip);
    execute_system_cmd(system_cmd);
    /* Create Rule/route table */
    sprintf(system_cmd,"%s rule add from %s table %d 1>/dev/null",ip_command,vip->ip,
                                                                               vip->rule_table_id );
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20197, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Adding source route table %d for %s failed\n",vip->rule_table_id, vip->ip);
    }

    /* Create Rule for static routes */
    sprintf(system_cmd,"%s rule add from %s table main 1>/dev/null",ip_command,vip->ip);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20197, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Adding source route table %d for %s failed\n",vip->rule_table_id, vip->ip);
    }
    
     /* Create Rule for static routes */
    sprintf(system_cmd,"%s rule add from %s table %srtable 1>/dev/null",ip_command,vip->ip,vip->name);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(80000, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Adding source route to table %srtable for %s failed\n",vip->name, vip->ip);
    }

    if( vip->vip_subnet_type == QREP_VIP_SUBNET_TYPE_OWN  ) {
        /*Set the local route on the default table. We set this always. This may fail
          for subsequent sets. But it is ok*/
        plat_log_msg(20198, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                    "VIP %s not part of interface subnet. Add local route %s/%d",
                                                    vip->ip,netaddr,maskbits);
        sprintf(system_cmd,"%s add -net %s netmask %s dev %s 1>/dev/null 2>/dev/null",route_command,
                                                                      netaddr,vip->mask,vip->name);
        execute_system_cmd(system_cmd);

        plat_log_msg(80002, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                    "Add local route %s/%d for %s to %srtable", netaddr,maskbits,vip->ip,vip->name);
        sprintf(system_cmd,"%s route add %s/%d dev %s table  %srtable 1>/dev/null 2>/dev/null",ip_command,
                                                                      netaddr,maskbits,vip->name,vip->name);
        execute_system_cmd(system_cmd);
    }

    sprintf(system_cmd,"%s route add %s/%d dev %s table %d src %s 1>/dev/null",ip_command,
                        netaddr,maskbits,vip->name,vip->rule_table_id,vip->ip);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20199, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Adding Route %s/%d to table %d on %s for src %s failed \n",
                    netaddr,maskbits, vip->rule_table_id, vip->name, vip->ip);
    }
    if( strcmp(vip->gw,"") ) {
        sprintf(system_cmd,"%s route add default via %s dev %s table %d 1>/dev/null",ip_command,
                     vip->gw,vip->name,vip->rule_table_id);
        rc = execute_system_cmd(system_cmd);
        if( rc != 0 ) {
            plat_log_msg(20200, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
               "Adding default Route via %s on %s  to table %d failed \n",
                    vip->gw, vip->name, vip->rule_table_id);
        }
    }
}
#define MAX_THREAD_COUNT 10


void *vip_config_thread(void *arg)
{
    int i;
    qrep_ctnr_iface_t *iface = (qrep_ctnr_iface_t *)arg;
    for( i = 0; i < iface->temp; i++ ) {
        configure_single_vip(&iface[i]);
    }
    pthread_exit(NULL);
}

void configure_vip_group_async(ipf_entry_t *entry) {
    int i,j;
    char system_cmd[512];
#ifndef IPF_TEST
    int vip_cnt,th_cnt=0, padding=0,thread_vips,vips_per_thread,vip_arry_index,status;
    pthread_t thread[MAX_THREAD_COUNT];
    pthread_attr_t attr;

    plat_log_msg(20201, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG, "+++++++++\nconfigvip: numvips:%d numpfws:%d\n",
                                         entry->num_vips,entry->num_pfws);
    plat_log_msg(20202, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,"VIP configure start: number of vips:%d\n",entry->num_vips);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    vip_cnt = entry->num_vips;
    vips_per_thread = vip_cnt/MAX_THREAD_COUNT;
    padding = vip_cnt%MAX_THREAD_COUNT;
    vip_arry_index = 0;
    /* Killall Previos arpings if any one hanging around from previous configuration*/
    sprintf(system_cmd,"sudo /usr/bin/killall arping 2>/dev/null");
    execute_system_cmd(system_cmd);

    for(i = 0; i < MAX_THREAD_COUNT; i++ ) {
        thread_vips = vips_per_thread;
        if( padding > 0 ) {
            thread_vips++;
            padding--;
        }
        if( thread_vips == 0 ) {
            break;
        }
        th_cnt++;
        plat_log_msg(20203, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                                                "VIPS for thread %d  start: %d  num vips: %d\n",i,vip_arry_index,thread_vips);
        entry->vips[vip_arry_index].temp = thread_vips;
        pthread_create(&thread[i], &attr, vip_config_thread, (void *) &(entry->vips[vip_arry_index]));
        vip_arry_index = vip_arry_index + thread_vips;
    }
    pthread_attr_destroy(&attr);
    for(i = 0; i < th_cnt; i++ ) {
       pthread_join(thread[i],(void **) &status);
    }
    plat_log_msg(20204, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,"VIP config done\n");
#endif

    #if 0
    for( i = 0; i < entry->num_vips; i++ ) {
        if( entry->vips[i].rule_table_id != 0 ) {
            configure_single_vip(&(entry->vips[i]));
        }
        else {
            plat_log_msg(20205, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, 
                         "NULL ROUTE TABLE ID:%d ip:%s BUG?\n", entry->vips[i].rule_table_id, entry->vips[i].ip);    
        }
    }
    #endif

    /* set the IP TABLE RULES */
    for( i = 0; i < entry->num_pfws; i++ ) {
        for( j = 0; j < entry->pfw_entries[i].num_vips; j++ ) {
            sprintf(system_cmd,"%s -t nat -A OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,entry->pfw_entries[i].vips[j].ip, entry->pfw_entries[i].rport, 
                                                                         entry->pfw_entries[i].vport);
            execute_system_cmd(system_cmd);
            sprintf(system_cmd,"%s -t nat -A OUTPUT -p udp -d %s --dport %d"
                                       " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,entry->pfw_entries[i].vips[j].ip, entry->pfw_entries[i].rport, 
                                                                         entry->pfw_entries[i].vport);
            execute_system_cmd(system_cmd);
        }
    }
}

/*
 *  ipf_delete_if - fork a process to remove a vip interface
 */

static void ipf_delete_single_vip(qrep_ctnr_iface_t *vip) {
    char system_cmd[512];
    int maskbits,rc;
    unsigned int md[4];
    unsigned char bitarr[16]={0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4};

    plat_log_msg(20206,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                "Deleting single vip %s ruletable:%d type:%d",
                 vip->ip, vip->rule_table_id, vip->vip_subnet_type);

    sscanf(vip->mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
    maskbits = bitarr[md[0] & 0xF] + bitarr[(md[0] >>4) & 0xF]+
               bitarr[md[1] & 0xF] + bitarr[(md[1] >>4) & 0xF]+
               bitarr[md[2] & 0xF] + bitarr[(md[2] >>4) & 0xF]+
               bitarr[md[3] & 0xF] + bitarr[(md[3] >>4) & 0xF] ;

    sprintf(system_cmd,"%s addr del %s/%d dev %s 2>/dev/null 1>/dev/null",ip_command,
                                                         vip->ip,32,vip->name);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20207, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
               "Deleting vip(%s) failed",system_cmd);
    }
    if( vip->rule_table_id != 0 ) {
        sprintf(system_cmd,"%s rule del table %d 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->rule_table_id);
        execute_system_cmd(system_cmd);
        sprintf(system_cmd,"%s route flush table %d 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->rule_table_id);
        execute_system_cmd(system_cmd);
        sprintf(system_cmd,"%s rule del from %s table main 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->ip);
        execute_system_cmd(system_cmd);

        sprintf(system_cmd,"%s rule del from %s table %srtable 2>/dev/null 1>/dev/null",ip_command,
                                                          vip->ip, vip->name);
        execute_system_cmd(system_cmd);

    }
    else {
        plat_log_msg(20205, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
           "NULL ROUTE TABLE ID:%d ip:%s BUG?\n", vip->rule_table_id, vip->ip);
   }
}

void *vip_delete_thread(void *arg)
{
    int i;
    qrep_ctnr_iface_t *iface = (qrep_ctnr_iface_t *)arg;
    for( i = 0; i < iface->temp; i++ ) {
        ipf_delete_single_vip(&iface[i]);
    }
    pthread_exit(NULL); 
}

static void
ipf_delete_if(ipf_entry_t *entry)
{
#ifdef VIPGROUP_SUPPORT
    int i, j;
    char system_cmd[512];

    #ifndef IPF_TEST
    int vip_cnt,th_cnt=0, padding=0,thread_vips,vips_per_thread,vip_arry_index,status;
    pthread_t thread[MAX_THREAD_COUNT];
    pthread_attr_t attr;

    plat_log_msg(20208,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                            "++++++++++\nDelete VIPs: num_vips:%d",entry->num_vips);
    /* Killall Previos arpings if any one hangig around */
    sprintf(system_cmd,"sudo /usr/bin/killall arping 2>/dev/null");
    execute_system_cmd(system_cmd);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    vip_cnt = entry->num_vips;
    vips_per_thread = vip_cnt/MAX_THREAD_COUNT;
    padding = vip_cnt%MAX_THREAD_COUNT;
    vip_arry_index = 0;

    for(i = 0; i < MAX_THREAD_COUNT; i++ ) {
        thread_vips = vips_per_thread;
        if( padding > 0 ) {
            thread_vips++;
            padding--;
        }
        if( thread_vips == 0 ) {
            break;
        }
        th_cnt++;
        plat_log_msg(20203, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                                                "VIPS for thread %d  start: %d  num vips: %d\n",i,vip_arry_index,thread_vips);
        entry->vips[vip_arry_index].temp = thread_vips;
        pthread_create(&thread[i], &attr, vip_delete_thread, (void *) &(entry->vips[vip_arry_index]));
        vip_arry_index = vip_arry_index + thread_vips;
    }
    pthread_attr_destroy(&attr);
    for(i = 0; i < th_cnt; i++ ) {
       pthread_join(thread[i],(void **) &status);
    }
    plat_log_msg(20209, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,"VIP delete done\n");
#endif

#if 0
    /* Killall Previos arpings if any one hangig around */
    sprintf(system_cmd,"sudo /usr/bin/killall arping 2>/dev/null");
    execute_system_cmd(system_cmd);
    for( i = 0; i < entry->num_vips; i++ ) {
        ipf_delete_single_vip( &(entry->vips[i]));
        
    }
#endif
    /* Flush the NAT Table while we die. This function is currently called only when the process dies
     * And flash only if the dying node is virtual node because virtual node only creates NAT rules
     */
    /* set the IP TABLE RULES */
    for( i = 0; i < entry->num_pfws; i++ ) {
        for( j = 0; j < entry->pfw_entries[i].num_vips; j++ ) {
            sprintf(system_cmd,"%s -t nat -D OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,entry->pfw_entries[i].vips[j].ip, entry->pfw_entries[i].rport, 
                                                                         entry->pfw_entries[i].vport);
            execute_system_cmd(system_cmd);
            sprintf(system_cmd,"%s -t nat -D OUTPUT -p udp -d %s --dport %d"
                                       " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,entry->pfw_entries[i].vips[j].ip, entry->pfw_entries[i].rport, 
                                                                         entry->pfw_entries[i].vport);
            execute_system_cmd(system_cmd);
        }
    }
#else
    char command[8192];
    tprintf("ipf_delete_if\n");
    snprintf(command, sizeof(command), "%s %s:%d down",
        if_command, entry->if_1, entry->if_2);

    system(command);
#endif
    return;
}

/*
 *  ipf_ge - compare for time a >= time b
 */

static int
ipf_ge(struct timeval a, struct timeval b)
{
    int  result;

    result =
            a.tv_sec > b.tv_sec
        ||  (
                a.tv_sec == b.tv_sec
            &&  a.tv_usec >= b.tv_usec
            );

    return result;
}

/*
 *  ipf_gt - compare for time a > time b
 */

static int
ipf_gt(struct timeval a, struct timeval b)
{
    int  result;

    result =
            a.tv_sec > b.tv_sec
        ||  (
                a.tv_sec == b.tv_sec
            &&  a.tv_usec > b.tv_usec
            );

    return result;
}
#ifdef VIPGROUP_SUPPORT
int get_if_ip_param( char *ifname, char *ip, char *mask, char *gw) {
    FILE *fptr;
    char line[80], ifcfg_file[64];
    struct in_addr inp;

    sprintf(ifcfg_file,"/etc/sysconfig/network-scripts/ifcfg-%s",ifname);
    strcpy(ip,"");
    strcpy(mask,"");
    strcpy(gw,"");

    fptr = fopen(ifcfg_file, "r");
    if( fptr == NULL ) {
        plat_log_msg(20210,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                "Getting IF Parameters for iface  %s Failed\n", ifname);
        return 1;
    }
    while( fgets(line,80,fptr) != NULL ) {
       if( strncmp(line,"IPADDR=",strlen("IPADDR=")) == 0)  {
          strncpy(ip,line+strlen("IPADDR="),strlen(line)-strlen("IPADDR="));
          if( inet_aton(ip, &inp) == 0 ) {
              plat_log_msg(20211,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                "Unable to parse IP Address(%s) from %s\n",ip,ifcfg_file);
              fclose(fptr);
              return 1;
          }
          strcpy(ip,inet_ntoa(inp));
       }
       else if( strncmp(line,"NETMASK=",strlen("NETMASK=")) == 0)  {
          strncpy(mask,line+strlen("NETMASK="),strlen(line)-strlen("NETMASK="));
          if( inet_aton(mask, &inp) == 0 ) {
              plat_log_msg(20212,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                "Unable to parse Mask(%s) from %s\n",ip,ifcfg_file);
              fclose(fptr);
              return 1;
          }
          strcpy(mask,inet_ntoa(inp));
       }
       else if( strncmp(line,"GATEWAY=",strlen("GATEWAY=")) == 0)  {
          strncpy(gw,line+strlen("GATEWAY="),strlen(line)-strlen("GATEWAY="));
          if( inet_aton(gw, &inp) == 0 ) {
              plat_log_msg(20213,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                "Unable to parse GW(%s) from %s\n",gw,ifcfg_file);
              fclose(fptr);
              return 1;
          }
          strcpy(gw,inet_ntoa(inp));
       }
    }
    fclose(fptr);
    plat_log_msg(20214,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                "Interface %s parameters(ip:%s mask:%s gw:%s)", ifname,ip,mask,gw);
    return 0;
}

void get_net_address(char *ipaddr, char *mask, char *netaddr) {
    unsigned int md[4];
    unsigned int ip[4];
    sscanf(ipaddr,"%d.%d.%d.%d",&ip[0],&ip[1],&ip[2],&ip[3]);
    sscanf(mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
    sprintf(netaddr,"%d.%d.%d.%d", ip[0] & md[0], ip[1] & md[1],ip[2] & md[2],ip[3] & md[3]);
}

int find_vip_subnet_type_from_vip_list(qrep_ctnr_iface_t *vip_list, int num_vips, qrep_ctnr_iface_t *vip) {
    int i;
    char netaddr0[32],netaddr1[32];

    plat_log_msg(20215,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                "Getting vip subnet type for %s mask:%s ifname:%s type:%d", vip->ip,vip->mask,vip->name,vip->vip_subnet_type);
    for( i = 0; i < num_vips; i++ ) {
        if((strcmp(vip->ip,vip_list[i].ip) != 0) && (vip_list[i].vip_subnet_type != QREP_VIP_SUBNET_TYPE_UNKNOWN)
                                             && (strcmp(vip->name,vip_list[i].name) == 0)){
            get_net_address(vip_list[i].ip, vip_list[i].mask,netaddr0);
            get_net_address(vip->ip, vip->mask,netaddr1);
            if( strcmp(netaddr0,netaddr1) == 0 ) {
                /* We found an entry in the list. So this vip should be secondary always*/
                plat_log_msg(20216,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                "Entry found for %s on the vip entry %s", vip->ip, vip_list[i].ip);
                return vip_list[i].vip_subnet_type;
            }
        }
    }
    plat_log_msg(20217,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                 "VIP subnet type not found on the given list for %s", vip->ip);
    return QREP_VIP_SUBNET_TYPE_UNKNOWN;
}


/* THIS FUNCTION MUST BE CALLED after acquiring ipf_lock */
void remove_vip_local_route_if_needed(qrep_ctnr_iface_t *vip) {
    int i,rc,maskbits;
    int ip[4],md[4],nt[4];
    ipf_entry_t *current;
    char netaddr0[32],netaddr1[32],system_cmd[128];
    unsigned char bitarr[16]={0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4};

    plat_log_msg(20218,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "Remove local route if no other VIP in same subnet(ip: %s mask:%s iface:%s)",vip->ip,vip->mask,vip->name); 
    if( vip->vip_subnet_type != QREP_VIP_SUBNET_TYPE_OWN ) {
        plat_log_msg(20219,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                     "VIP Type for %s is not QREP_VIP_SUBNET_TYPE_OWN. Just return\n",vip->ip);
        return;
    }
    get_net_address(vip->ip, vip->mask,netaddr0);
    current = ipf_head;
    while( current != NULL ) {
        for( i = 0; i < current->num_vips; i++ ) {
            if( (current->vips[i].vip_subnet_type != QREP_VIP_SUBNET_TYPE_OWN) || (strcmp(vip->ip,current->vips[i].ip) == 0) 
                                                     || (strcmp(vip->name,current->vips[i].name) != 0))  {
                continue;
            }
            get_net_address(current->vips[i].ip, current->vips[i].mask,netaddr1);
            if( strcmp(netaddr0,netaddr1) == 0 ) {
                /* We found an entry in the list. So do not need to delete local route*/
                plat_log_msg(20220, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                         "Found other vip %s in same subnet. Just return",current->vips[i].ip);
                return;
            }
        }
        current = current->next;
    }
    plat_log_msg(20221,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "No VIP exists in same subnet(ip: %s mask:%s iface:%s)just remove local route",vip->ip,vip->mask,vip->name); 

    sscanf(vip->ip,"%d.%d.%d.%d",&ip[0],&ip[1],&ip[2],&ip[3]);
    sscanf(vip->mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
    maskbits = bitarr[md[0] & 0xF] + bitarr[(md[0] >>4) & 0xF]+
               bitarr[md[1] & 0xF] + bitarr[(md[1] >>4) & 0xF]+
               bitarr[md[2] & 0xF] + bitarr[(md[2] >>4) & 0xF]+
               bitarr[md[3] & 0xF] + bitarr[(md[3] >>4) & 0xF] ;

    nt[0]=ip[0] & md[0];
    nt[1]=ip[1] & md[1];
    nt[2]=ip[2] & md[2];
    nt[3]=ip[3] & md[3]; 
    sprintf(netaddr0,"%d.%d.%d.%d", nt[0],nt[1],nt[2],nt[3]);

    sprintf(system_cmd,"%s route del %s/%d dev %s 1>/dev/null 2>/dev/null",ip_command,
                                                           netaddr0,maskbits,vip->name);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20222,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
            "Failed to remove localroute(%s)\n",system_cmd);
    }
    /*Delete from interface table*/
    sprintf(system_cmd,"%s route del %s/%d dev %s table %srtable 1>/dev/null 2>/dev/null",ip_command,
                                                           netaddr0,maskbits,vip->name,vip->name);
    rc = execute_system_cmd(system_cmd);
    if( rc != 0 ) {
        plat_log_msg(20222,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
            "Failed to remove localroute(%s)\n",system_cmd);
    }
}

int is_vip_partof_iface_subnet( qrep_ctnr_iface_t *vip ) {
    char netaddr0[32],netaddr1[32],ifip[32],ifmask[32],ifgw[32];

    if( get_if_ip_param(vip->name,ifip,ifmask,ifgw) != 0 ) {
        plat_log_msg(20223, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
           "ERROR:Unable to find whether vip %s in interface %s subnet. Just return YES", vip->ip,vip->name);
        return 1;
    }
    get_net_address(ifip, ifmask,netaddr0);
    get_net_address(vip->ip, vip->mask,netaddr1);
    if( strcmp(netaddr0,netaddr1) == 0 ) {
        return 1;
    }
    plat_log_msg(20224,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                   "Vip: %s mask:%s not part of iface:%s subnet",vip->ip,vip->mask,vip->name); 
    return 0;
}

void find_vip_subnet_type(qrep_ctnr_iface_t *vips,int num_vips) {
    int i, viptype;
    ipf_entry_t *current;
    char ifip[32],ifmask[32],ifgw[32];

    plat_log_msg(20225,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "Set VIP type num vips %d",num_vips);
    for( i = 0; i < num_vips; i++ ) {
        current = ipf_head;
        viptype = QREP_VIP_SUBNET_TYPE_UNKNOWN;
        if( strcmp(vips[i].mask,"") == 0 ) {
            if( get_if_ip_param(vips[i].name,ifip,ifmask,ifgw) != 0 ) {
                plat_log_msg(20226, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL,
                    "ERROR:Unable to find IP params for interface %s. Just set mask to 255.255.0.0",vips[i].name);
                strcpy(vips[i].mask,"255.255.0.0");
            }
            else {
                 plat_log_msg(20227,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                        "Mask Field for %s is empty. Adding interface's mask(%s)",vips[i].ip,ifmask);
                strcpy(vips[i].mask,ifmask);
            }
        }
        while( current != NULL ) {
            viptype = find_vip_subnet_type_from_vip_list(current->vips,current->num_vips, &(vips[i]));
            if( viptype != QREP_VIP_SUBNET_TYPE_UNKNOWN ) {
                plat_log_msg(20228,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "VIP subnet type found from old list for %s is set to %d",vips[i].ip, viptype);
                vips[i].vip_subnet_type = viptype;
                break;
            }
            current = current->next;
        }
        if( viptype == QREP_VIP_SUBNET_TYPE_UNKNOWN ) {
            /* try to find from the current list */
            viptype = find_vip_subnet_type_from_vip_list(vips,num_vips, &(vips[i]));
            if( viptype != QREP_VIP_SUBNET_TYPE_UNKNOWN ) {
                plat_log_msg(20229,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "VIP subnet type found from current list for %s and is set to %d",vips[i].ip, viptype);
                vips[i].vip_subnet_type = viptype;
                continue;
            }
            /* VIP type is still unknown Try to find from ifcfg file*/
            if( is_vip_partof_iface_subnet(&(vips[i])) == 1 ) {
                plat_log_msg(20230,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "VIP subnet type found from iface subnet for %s and vip subtype set to %d",vips[i].ip, QREP_VIP_SUBNET_TYPE_IFACE);
                vips[i].vip_subnet_type = QREP_VIP_SUBNET_TYPE_IFACE;
            }
            else {
                plat_log_msg(20231,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "VIP subnet type can not be found from iface subnet for %s. subtyp set to %d",vips[i].ip, QREP_VIP_SUBNET_TYPE_OWN);
                vips[i].vip_subnet_type = QREP_VIP_SUBNET_TYPE_OWN;
            }
        }
    }
}

void print_vip_info(char *str, qrep_ctnr_iface_t *vips, int num_vips) {
    int i; 
    for( i = 0; i < num_vips; i++ ) {
        plat_log_msg(20232,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "VIP %s Mask %s GW %s subnet type %d iface %s",vips[i].ip,vips[i].mask,vips[i].gw,vips[i].vip_subnet_type,vips[i].name);
    }
}
static int ipf_add_vgrp_to_list( uint32_t vipgroupid, int num_vips, qrep_ctnr_iface_t *vips, 
                                                      int num_pfws, ipf_pfw_entry_t *pfw_entries,
                                                      struct timeval expiration ) {
    ipf_entry_t *  current;
    ipf_entry_t *  new;
    int            lease_extended;

    pthread_mutex_lock(&ipf_lock);
    lease_extended = false;
    current = ipf_head;

    plat_log_msg(20233,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                      "=============\nCONFIGURE VIP REQUEST: NUM VIPS %d Group:%d\n",num_vips,vipgroupid);
    //set_vip_type(vips,num_vips);
    find_vip_subnet_type(vips,num_vips);
    while (current != null) {
        if (current->address == vipgroupid) {
            if (ipf_gt(expiration, current->expiration)) {
                current->expiration = expiration;
                lease_extended = true;
            }
            plat_log_msg(20234,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "Adding Dynamic VIP %s to group %d",vips[0].ip,vipgroupid); 
            /*Add the new list to current list*/
            memcpy( &(current->vips[current->num_vips]),vips,sizeof(qrep_ctnr_iface_t) * num_vips);
            current->num_vips = current->num_vips + num_vips;
            /* configure the Virtual IP*/
            for( int i = 0; i < num_vips; i++ ) {
               configure_single_vip(&(vips[i]));
            }
            print_vip_info("ipf_add_vgrp_to_list",current->vips,current->num_vips);
            pthread_mutex_unlock(&ipf_lock);
            return lease_extended;
        }
        current = current->next;
    }

    new = plat_alloc(sizeof(*new));
    if (new == null) {
        pthread_mutex_unlock(&ipf_lock);
        return 0;
    }

    new->next = ipf_head;
    ipf_head = new;
    new->address    = vipgroupid;
    new->expiration = expiration;
    new->num_vips = num_vips;
    memcpy(new->vips,vips, sizeof(qrep_ctnr_iface_t) * num_vips );
    new->num_pfws = num_pfws;
    if( new->num_pfws > 0 ) {
        memcpy( new->pfw_entries, pfw_entries, sizeof(ipf_pfw_entry_t) * num_pfws);
    }
    configure_vip_group_async(new);
    pthread_mutex_unlock(&ipf_lock);
    return true;
}
#endif


/*
 *  ipf_add_to_list - add an entry to the list of vip interfaces
 */
static int
ipf_add_to_list
(
    uint32_t address, char *if_1, int16_t *if_2, struct timeval expiration
)
{
    ipf_entry_t *  current;
    ipf_entry_t *  new;
    int            lease_extended;

    pthread_mutex_lock(&ipf_lock);
    lease_extended = false;
    current = ipf_head;

    while (current != null) {
        if (current->address == address) {
/*          current->ref_count++; must check for existing ref */

            if (ipf_gt(expiration, current->expiration)) {
                current->expiration = expiration;
                lease_extended = true;
            }

            tprintf("ip_add_to_list:  found 0x%x (%s:%d)\n",
                (unsigned) address, current->if_1, (int) current->if_2);
            pthread_mutex_unlock(&ipf_lock);
            return lease_extended;
        }

        current = current->next;
    }

    new = plat_alloc(sizeof(*new));

    if (new == null) {
        pthread_mutex_unlock(&ipf_lock);
        return 0;
    }
    if (*if_2 < 0) {
        *if_2 = ipf_next_vip++;
    }

    tprintf("ip_add_to_list:  insert 0x%x (%s:%d)\n",
        (unsigned) address, if_1, (int) *if_2);

    new->next = ipf_head;
    ipf_head = new;

    new->address    = address;
/*  new->ref_count  = 1; */
    new->if_2       = *if_2;
    new->expiration = expiration;
    memset(new->if_1,0,sizeof(new->if_1));
    strncpy(new->if_1, if_1,sizeof(new->if_1)-1);
    pthread_mutex_unlock(&ipf_lock);
    return true;
}

/*
 *  ipf_remove_from_list_if - remove an entry from the list of vip interfaces
 */

static void
ipf_remove_from_list(uint32_t address, qrep_ctnr_iface_t *vip )
{
    ipf_entry_t *  current;
    ipf_entry_t *  previous;

    pthread_mutex_lock(&ipf_lock);
    current = ipf_head;
    previous = null;

    while (current != null) {
        if (current->address == address) {
            if( vip == NULL ) { 
                plat_log_msg(20235,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "==========\nDelete All VIPs from group %d",address);
                /* Delete all IPs in the group */
                ipf_delete_if(current);
                for( int i = 0; i < current->num_vips; i++ ) {
                    remove_vip_local_route_if_needed(&(current->vips[i]));
                    /*Just set the type of this to UnKnown so that the remove_vip_local_route_if_needed
                      function will not pickup this entry for next iteration*/
                    current->vips[i].vip_subnet_type = QREP_VIP_SUBNET_TYPE_UNKNOWN;
                }
                /* Entries got deleted. Just check whether we need to 
                   remove any local route*/
                if (previous == null) {
                    ipf_head = current->next;
                } else {
                    previous->next = current->next;
                }
                plat_free(current);
                break;
            }
            else {
                int entry_found = 0;
                plat_log_msg(20236,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "===========\nTrying to Delete Dynamic VIP %s from group %d",vip->ip,address);
                /* Search the given VIP from list*/
                for( int i = 0; i < current->num_vips; i++ ) {
                    if( (entry_found == 0) && (strcmp(current->vips[i].ip, vip->ip) == 0)
                                           && (strcmp(current->vips[i].name, vip->name) == 0)) {
                        plat_log_msg(20237,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                            "Entry found Deleting Dynamic VIP %s from group %d",vip->ip,address);
                        ipf_delete_single_vip( &(current->vips[i]));
                        /* If  vip is primary and then find whether there is another interface in the same subnet.
                           and then mark that is the primary and change the subnet using ifconfig*/
                        remove_vip_local_route_if_needed(&(current->vips[i]));
                        current->num_vips--;
                        entry_found = 1;
                        if( i == current->num_vips ) {
                            /*This is last entry. Just comeout*/
                            plat_log_msg(20238,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                            "This is last entry. Do not need to adjust list");
                            break;
                        }
                    }
                    if( entry_found == 1 ) {
                        memcpy( &(current->vips[i]), &(current->vips[i+1]), sizeof(qrep_ctnr_iface_t));
                    }
                }
                if( entry_found == 0 ) {
                      plat_log_msg(20239,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                            "Entry NOT found for VIP %s from group %d. BUG?",vip->ip,address);
                }
                else {
                    if( current->num_vips == 0 ) {
                        /*All Vips are removed. Just delete the current entry */ 
                          if (previous == null) {
                              ipf_head = current->next;
                          } else {
                              previous->next = current->next;
                          }
                          plat_log_msg(20240,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                            "Number of VIPs in group %d is 0. So delete current entry",address);
                          plat_free(current);
                          break;
                    }
                }
                /*Debug*/
                print_vip_info("ipf_remove_from_list",current->vips,current->num_vips);
            }
            break;
        }

        previous = current;
        current = current->next;
    }

    pthread_mutex_unlock(&ipf_lock);
    return;
}

static void
ipf_remove_all(void)
{
    ipf_entry_t *  current;
    ipf_entry_t *  next;

    dprintf(" ==== ipf_remove_all\n");
    current = ipf_head;
    plat_log_msg(20241,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                   "Remove all VIPS");
    pthread_mutex_lock(&ipf_lock);
    while (current != null) {
        next = current->next;
        ipf_delete_if(current);
        for( int i = 0; i < current->num_vips; i++ ) {
            remove_vip_local_route_if_needed(&(current->vips[i]));
            /*Just set the type of this to SUBNET so that the remove_vip_local_route_if_needed
              function will not pickup this entry for next iteration*/
            current->vips[i].vip_subnet_type = QREP_VIP_SUBNET_TYPE_UNKNOWN;
        }
        plat_free(current);
        current = next;
        ipf_head = current;
    }

    ipf_head = null;
    pthread_mutex_unlock(&ipf_lock);
    return;
}

static struct timeval
ipf_check_list(void)
{
    struct timeval   now;
    struct timezone  time_zone;
    struct timeval   next_time;

    ipf_entry_t *  current;
    ipf_entry_t *  next;
    ipf_entry_t *  prev;

    gettimeofday(&now, &time_zone);
    memset(&next_time, 0, sizeof(next_time));
    next_time.tv_sec = ((unsigned long) -1) >> 1;

    current = ipf_head;
    prev = null;
    tprintf("ipf_check_list:  enter\n");
    plat_log_msg(20242,PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                   "IPF Checklist");
    pthread_mutex_lock(&ipf_lock);
    while (current != null) {
        next = current->next;

        if (ipf_ge(now, current->expiration)) {
            ipf_delete_if(current);
            for( int i = 0; i < current->num_vips; i++ ) {
                remove_vip_local_route_if_needed(&(current->vips[i]));
                /*Just set the type of this to SUBNET so that the remove_vip_local_route_if_needed
                  function will not pickup this entry for next iteration*/
                current->vips[i].vip_subnet_type = QREP_VIP_SUBNET_TYPE_UNKNOWN;
            }
            if (prev == null) {
                ipf_head = next;
            } else {
                prev->next = next;
            }

            plat_free(current);
        } else {
            if (next_time.tv_sec > current->expiration.tv_sec) {
               next_time.tv_sec = current->expiration.tv_sec;
               tprintf("ipf_check_list:  set next\n");
            }

            prev = current;
        }

        current = next;
    }
    pthread_mutex_unlock(&ipf_lock);

    tprintf("ipf_check_list:  exit\n");
    return next_time;
}

static void
ipf_schedule_alarm(struct timeval next_time)
{
    int  error;
    struct itimerval  itimer;
    struct timeval    now;
    struct timezone   timezone;

    gettimeofday(&now, &timezone);
    memset(&itimer, 0, sizeof(itimer));

    if (now.tv_sec < next_time.tv_sec) {
       itimer.it_value.tv_sec = next_time.tv_sec - now.tv_sec + 1;
    } else {
       tprintf("ipf_schedule_alarm:  match\n");
       itimer.it_value.tv_sec = 1;
    }

    next_time.tv_usec = min(next_time.tv_usec, 999999);
    tprintf("ipf_schedule_alarm:  { %ld, %ld }\n",
        (long) itimer.it_value.tv_sec,
        (long) itimer.it_value.tv_usec);

    error = setitimer(ITIMER_REAL, &itimer, null);

    if (error) {
        perror("setitimer");
    }

    return;
}

static void
ipf_alarm(int signal)
{
    int  busy;
    struct timeval   next_time;
    struct timezone  time_zone;

    busy = pthread_mutex_trylock(&ipf_lock);

    if (busy) {
        tprintf("ipf_alarm:  busy\n");
        ipf_schedule = 1;
        gettimeofday(&next_time, &time_zone);
        next_time.tv_sec++;
        ipf_schedule_alarm(next_time);
        return;
    }

    tprintf("ipf_alarm:  check\n");
    next_time = ipf_check_list();
    ipf_schedule_alarm(next_time);
    pthread_mutex_unlock(&ipf_lock);
    return;
}

static void
ipf_sigpipe(int signal)
{
    ipf_remove_all();
    plat_exit(0);
}

/*
 *  ipf_handler - the main routine for ip failover process
 */

void
ipf_handler(void)
{
    ipf_msg_t  message;
    int        count;
    int        result;
    int        done;
    sigset_t   block_set;
    sigset_t   open_set;

    struct sigaction  action;
    struct timeval    next_time;

    setpgid(0, 0);
    ipf_head = null;
    done = false;

    action.sa_handler  = ipf_sigpipe;
    action.sa_flags    = SA_ONSTACK;
    action.sa_restorer = NULL;

    sigemptyset(&action.sa_mask);
    sigaction(SIGPIPE, &action, NULL);

    action.sa_handler  = ipf_alarm;
    action.sa_flags    = 0;
    action.sa_restorer = NULL;

    sigemptyset(&action.sa_mask);
    sigaddset(&action.sa_mask, SIGALRM);
    sigaction(SIGALRM, &action, NULL);

    sigemptyset(&block_set);
    sigaddset(&block_set, SIGALRM);

    /*
     *  Loop reading commands from the fd until the pipe closes or we're told to
     *  exit.
     */

    do {
        count = 0;

        do {
            errno = EINVAL;

            result =
              read(ipf_pipe_fd, (void *) &message + count,
                  sizeof(message) - count);

            dprintf(" ==== pipe read got %d (%d)\n", (int) result, (int) errno);

            /*
             *  Ignore EAGAIN and EINTR.  Kill all the interfaces for any other
             *  type of read error.
             */

            if (result <= 0 && (errno == EAGAIN || errno == EINTR)) {
                result = 0;
                sleep(1);
            } else if (result <= 0) {
                message.command = ipf_type_fail;
                break;
            }

            count += result;
        } while (count < sizeof(message));

        /*
         *  Now that a command has been read, process it.
         */

        sigprocmask(SIG_BLOCK, &block_set, &open_set);

        switch (message.command) {
        case ipf_type_fail:
            ipf_remove_all();
            done = true;
            break;

        case ipf_type_exit:
            dprintf(" ==== exit!\n");
            while (ipf_head != null) {
               ipf_remove_from_list(ipf_head->address, NULL);
            }

            done = true;
            break;

        case ipf_type_remember:
	    dprintf(" ==== remember 0x%x, %d:%d\n",
               (unsigned) message.address,
               (int) message.if_1, (int) message.if_2);
#ifdef VIPGROUP_SUPPORT
           ipf_add_vgrp_to_list(message.address, message.num_vips, &(message.vips[0]),
                                                 message.num_pfws, &(message.pfw_entries[0]),
                                                 message.expiration);
#else
            ipf_add_to_list(message.address, message.if_1, &message.if_2,
                message.expiration);
#endif
            break;

        case ipf_type_forget:
            if( message.num_vips > 0 ) {
                ipf_remove_from_list(message.address,&(message.vips[0]));
            }
            else {
                ipf_remove_from_list(message.address, NULL);
            }
            break;
        }

        if (ipf_schedule) {
            pthread_mutex_lock(&ipf_lock);
            next_time = ipf_check_list();
            ipf_schedule_alarm(next_time);
            ipf_schedule = 0;
            pthread_mutex_unlock(&ipf_lock);
        }

        sigprocmask(SIG_SETMASK, &open_set, null);
    } while (! done);

    return;
}

static int
ipf_find_settings(uint64_t cguid)
{
    int  i;

    for (i = 0; i < array_size(settings.vips); i++) {
        if (settings.vips[i].cguid == cguid) {
            return i;
        }
    }

    return -1;
}

static uint32_t
ipf_get_host(char *name)
{
    uint32_t  address;
    int       count;
    int       dq1;
    int       dq2;
    int       dq3;
    int       dq4;

    count = sscanf(name, "%d.%d.%d.%d", &dq1, &dq2, &dq3, &dq4);

    if (count != 4) {
        plat_log_msg(20243,
            PLAT_LOG_CAT_SDF_APP_MEMCACHED,
            PLAT_LOG_LEVEL_FATAL,
            "The vip address is not valid!");
        return 0;
    }

    address = (dq1 << 24) | (dq2 << 16) | (dq3 << 8) | dq4;
    address = htonl(address);
    return address;
}
#ifndef IPF_TEST
extern void send_recovery_start_event(int32_t pnode, QREP_RECOVERY_TYPE rtype);

int is_node_live(int node) {
    int *node_list, num_nodes, i;
    node_list = msg_sdf_ranks(&num_nodes);
    for (i = 0; i < num_nodes; i++) {
        if (node == node_list[i]) {
            plat_free(node_list);
            return 1;
        }
    }
    plat_free(node_list);
    return 0;
}
#endif

static void
ipf_notify
(
    plat_closure_scheduler_t *  context,
    void *                      env,
    int                         events,
    struct cr_shard_meta *      shard_meta,
    enum sdf_replicator_access  access,
    struct timeval              expiration,
    sdf_replicator_notification_complete_cb_t  completion
)
{
    int       i;
#ifndef VIPGROUP_SUPPORT
    uint32_t  address;
    uint32_t  mask;
    char *    if_id;
#endif

#ifdef SIMPLE_REPLICATION
#   ifdef VIPGROUP_SUPPORT
    struct sdf_agent_state *mcd_state;
    qrep_state_t *ps;
    int my_node_id ; /* , new_lease; */
    int16_t my_grp_id,intra_node_vip_group_id;

    mcd_state = &(Mcd_agent_state);
    SDF_action_state_t *pas = mcd_state->ActionInitState.pcs;
    ps = &(pas->qrep_state);
    my_node_id = msg_sdf_myrank();
    int num_pfws = 0;
    ipf_pfw_entry_t pfw_entries[NUM_PFW_ENTRIES];
#   endif
#endif

#ifndef VIPGROUP_SUPPORT
    i = ipf_find_settings(shard_meta->persistent.cguid);

    if (i < 0) {
	#ifdef SIMPLE_REPLICATION
	    if (!SDFSimpleReplicationEnabled()) {
		plat_closure_apply(sdf_replicator_notification_complete_cb,
		    &completion);
	    }
	#else
	    plat_closure_apply(sdf_replicator_notification_complete_cb,
		&completion);
	#endif
        return;
    }

    if (events & SDF_REPLICATOR_EVENT_LTIME) {
        SDF_I_InvalContainer(&Mcd_agent_state.ActionInitState, settings.vips[i].cguid);
        /* XXX tbd("close any connections to this container"); */
    }
#endif // ndef VIPGROUP_SUPPORT
    plat_log_msg(20244, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_DEBUG,
                            "IPF NOTIFY: Entering...");

    switch (access) {
    case SDF_REPLICATOR_ACCESS_RO:
        plat_log_msg(20245,
            PLAT_LOG_CAT_SDF_APP_MEMCACHED,
            PLAT_LOG_LEVEL_FATAL,
            "The read-only replication mode is not supported");
        plat_exit(1);
        break;

    case SDF_REPLICATOR_ACCESS_RW:
#ifdef VIPGROUP_SUPPORT
        intra_node_vip_group_id = shard_meta->persistent.intra_node_vip_group_id;
        my_grp_id =  ps->node_state[my_node_id].group_id;
        plat_log_msg(20246, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_DEBUG,
                            "STM: SDF_REPLICATOR_ACCESS_RW: Vgrp:%d mygrp:%d mynode:%d\n",
                                   intra_node_vip_group_id,my_grp_id,my_node_id); 
        if( my_grp_id !=  ps->node_state[intra_node_vip_group_id].group_id ) {
            plat_log_msg(20247, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Faile node:%d(grp:%d) not in my group:%d, ignore failure\n",intra_node_vip_group_id,
                                    intra_node_vip_group_id, my_grp_id);
            break;
        }
        if(  ps->groups[my_grp_id].type == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
             /*N+1 group type*/
             /*Ignore if the current node is active already*/
             if( ps->node_state[my_node_id].type != QREP_NODE_TYPE_STANDBY ) {
                  plat_log_msg(20248, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Current node %d is already active. Ignore failure\n",my_node_id);
                  break;
             }
             if( ps->node_state[my_node_id].num_vgrps_being_serviced >= 1 ) {
                  plat_log_msg(20249, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Node is already handling 1 VIP group. ignore vipgroup:%d \n",intra_node_vip_group_id);
                  break;
             }
        }
        else if( ps->groups[my_grp_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED) {
            /* Check Whether the specified VIP group is being serviced if so, then it is just lease renewal*/
            for( i = 0; i < ps->node_state[my_node_id].num_vgrps_being_serviced; i++ ) {
                if( ps->node_state[my_node_id].serviced_vgrp_ids[i] == intra_node_vip_group_id ) {
                  plat_log_msg(20250, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_TRACE,
                               "New Lease for VIP Group: %d just ignore\n",intra_node_vip_group_id);
                  break;
                }
            }

            if( i < ps->node_state[my_node_id].num_vgrps_being_serviced ) {
                plat_log_msg(20251, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_TRACE,
                                         "Ignoring the RW notification for :%d\n",intra_node_vip_group_id);
                break;
            }

            if( ps->node_state[my_node_id].num_vgrps_being_serviced >= MAX_VIP_GROUPS_PER_NODE){
                  plat_log_msg(20252, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Current node %d is already handling %d VIP groups. Ignore failure\n",my_node_id,
                                        ps->node_state[my_node_id].num_vgrps_being_serviced);
                  break;
            }

         }
         /*req->stm_test_arg holds the Virtual IP group that needs to be configured to the 
           current node*/
        configure_vip(ps->vip_config, intra_node_vip_group_id);
        ps->node_state[my_node_id].type = QREP_NODE_TYPE_ACTIVE;
        plat_log_msg(20253, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                            "STM: Vgrp:%d configured at node:%d",intra_node_vip_group_id,my_node_id);
#if 0
         /*Remember the information so that, when process dies, we can clear
           old opf functions require address, if name .. for remembering
           we need to remember only the vip group id. So some dummy values are passed */
        new_lease = ipf_add_vgrp_to_list(intra_node_vip_group_id, ps->node_state[intra_node_vip_group_id].num_vips, 
                                                      &(ps->node_state[intra_node_vip_group_id].vipgroup[0]), expiration);
        if (new_lease) {
            ipf_send_vgroup( ipf_type_remember,intra_node_vip_group_id, ps->node_state[intra_node_vip_group_id].num_vips,
                                                      &(ps->node_state[intra_node_vip_group_id].vipgroup[0]), expiration );
        }
#endif
         ipf_send_vgroup( ipf_type_remember,intra_node_vip_group_id, ps->node_state[intra_node_vip_group_id].num_vips,
                                                  &(ps->node_state[intra_node_vip_group_id].vipgroup[0]), 
                                                  num_pfws, &(pfw_entries[0]),expiration );
#ifndef IPF_TEST
         ipf_handle_vip_manage_laptop(1,intra_node_vip_group_id);
#endif
        #ifndef IPF_TEST
        /* There may be a case where both the nodes 
           started simulatenously. In that case liveness events may come in RECOVERY peroid and both are ignored
           Then one node will have both the VIP groups though the other node is UP,
           Just check whether the other node is UP after we aceuire both the groups. If so, then start the recovery */
        if( ps->groups[my_grp_id].type == SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
            if( intra_node_vip_group_id == my_node_id ) {
                int i=0;
                fthWaitEl_t * wait;
                wait = fthLock(&(ps->node_state[my_node_id].lock), 1, NULL);
                for( i = 0; i < ps->node_state[my_node_id].nctnrs_node; i++ ) {
                    if( ps->node_state[my_node_id].cntrs[i].flags & qr_persist ){
                        break;
                    }
                }
                if ( i >= ps->node_state[my_node_id].nctnrs_node ) {
                     fthUnlock(wait);
                     plat_log_msg(160025, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_INFO,
                                                     "Node %d does not have any persistent container\n",my_node_id);
                     plat_log_msg(160026, PLAT_LOG_CAT_SDF_SIMPLE_REPLICATION, PLAT_LOG_LEVEL_INFO,
                                                     "Node becomes authoritative for persistent containers\n");
                }
                else {
                     fthUnlock(wait);
                }
            }
            if( ps->node_state[my_node_id].num_vgrps_being_serviced == 1 ) {
                /* Check whether we have only non persistent container. If so, enable authority for future
                   persistent recovery */
                int i;
                fthWaitEl_t * wait_list;
                wait_list = fthLock(&(ps->node_state[my_node_id].lock), 1, NULL);
                for( i = 0; i < ps->node_state[my_node_id].nctnrs_node; i++ ) {
                    if( ps->node_state[my_node_id].cntrs[i].flags & qr_persist ){
                        break;
                    }
                }
                if( i >= ps->node_state[my_node_id].nctnrs_node ) {
                    plat_log_msg(20254, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                    "No persistent containers exist. Enabling this node for authority for future persistent recovery\n");
                    ps->node_state[my_node_id].persistent_auth = 1;
                }
                fthUnlock(wait_list);
            }
            if( ps->node_state[my_node_id].num_vgrps_being_serviced >= MAX_VIP_GROUPS_PER_NODE ) {
                /* This node owns both the groups . Find other nodes ID*/
                for( int i = 0; i < ps->node_state[my_node_id].num_vgrps_being_serviced; i++ ) {
                    if( my_node_id !=  ps->node_state[my_node_id].serviced_vgrp_ids[i] ) {
                        /*check whether this node is alive */
                        //if( is_node_live(ps->node_state[my_node_id].serviced_vgrp_ids[i]) == 1 ) {
                        if (ps->node_state[ps->node_state[my_node_id].serviced_vgrp_ids[i]].live == 1) {
                            /*check whether recovery is already completed
                              Somtimes the state machine can give the VIP group access for a very short period 
                              even after it allocated the same VIP  group to other node due to the following reasons
                              Recovering node took some time to ack the VIP  group ownership but by the time
                              the surviving node thought that other node is not ready and got vipgroup
                              In this case, the statemachine later figures it out that other node actually 
                              has ownership and removes the assigned vip group at the surving node*/
                            if( ps->node_state[ps->node_state[my_node_id].serviced_vgrp_ids[i]].rec_flag == 0 ) {
                                fthWaitEl_t * wait_list;
                                QREP_RECOVERY_TYPE rtype;
                                plat_log_msg(20255, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                                "Simultaneous Start!!. Send recovery event to %d",ps->node_state[my_node_id].serviced_vgrp_ids[i]);
                                wait_list = fthLock(&(ps->node_state[my_node_id].lock), 1, NULL);
                                if( ps->node_state[my_node_id].persistent_auth == 1 ) {
                                    rtype = QREP_RECOVERY_TYPE_ALL;
                                }
                                else {
                                    rtype = QREP_RECOVERY_TYPE_NON_PERSISTENT_ONLY;
                                }
                                fthUnlock(wait_list);
                                send_recovery_start_event(ps->node_state[my_node_id].serviced_vgrp_ids[i],rtype);
                            }
                            else {
                                plat_log_msg(20256, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                                          "Recovery Already completed. Ignore this VipGroup access for recovery\n");
                            }
                        }
                    }
                }
            }
        }
        #endif
#else
        address = ipf_get_host(settings.vips[i].address);
        mask    = settings.vips[i].mask;
        if_id   = settings.vips[i].if_id;

        if (address == 0) {
            break;
        }
#ifdef SDFREPLICATION
        if (settings.vips[i].tcp_port != 0) {
            ipf_remember(address, mask, if_id, -1, expiration); /* for tcp */

            if (! ipf_bind_any && (events & SDF_REPLICATOR_EVENT_LTIME)) {
                mcd_vip_server_socket(&settings.vips[i], PORT_TYPE_TCP);
            }
        }

        if (settings.vips[i].udp_port != 0) {
            ipf_remember(address, mask, if_id, -1, expiration); /* for udp */

            if (! ipf_bind_any && (events & SDF_REPLICATOR_EVENT_LTIME)) {
                mcd_vip_server_socket(&settings.vips[i], PORT_TYPE_UDP);
            }
        }
#endif /* SDFREPLICATION */
#endif
        break;

    case SDF_REPLICATOR_ACCESS_NONE:
#ifdef VIPGROUP_SUPPORT
        intra_node_vip_group_id = shard_meta->persistent.intra_node_vip_group_id;
        plat_log_msg(20257, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_DEBUG,
                            "STM: SDF_REPLICATOR_ACCESS_NONE: Vgrp:%d mynode:%d\n",
                                   intra_node_vip_group_id,my_node_id); 

        for( i = 0; i <  ps->node_state[my_node_id].num_vgrps_being_serviced ; i++ ) {
            if (ps->node_state[my_node_id].serviced_vgrp_ids[i] == intra_node_vip_group_id){
                /*Current node is serving the given vip_group_id.
                  if the current node's group group is not mirrored node, then it is bug. just skip*/
                if( ps->groups[ps->node_state[my_node_id].group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
                   plat_log_msg(20258, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                        "Current node %d is not in mirrored cluster, BUG!\n",my_node_id);
                   break; 
                }
                plat_log_msg(20259, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                        "Virtual IP group %d removed from node %d\n",intra_node_vip_group_id,my_node_id);
                remove_vip(ps->vip_config,intra_node_vip_group_id);
                ipf_send_vgroup( ipf_type_forget,intra_node_vip_group_id, 0,NULL,0,NULL,expiration );
#ifndef IPF_TEST
                ipf_handle_vip_manage_laptop(0,intra_node_vip_group_id);
#endif
            }
        }
        plat_log_msg(20260, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_DEBUG,
                             "VIP Group %d Not in my list. Ignore\n",intra_node_vip_group_id);
#else
        address = ipf_get_host(settings.vips[i].address);

        if (address == 0) {
            break;
        }

        if (settings.vips[i].tcp_port != 0) {
            ipf_forget(address); /* for tcp */
        }

        if (settings.vips[i].udp_port != 0) {
            ipf_forget(address); /* for udp */
        }

        /*
         *  This cache flush should not be needed, but I am keeping it as a
         *  defensive measure.  The SDF_REPLICATOR_EVENT_LTIME bit should be
         *  passed when a cache flush is needed.
         */

        SDF_I_InvalContainer(&Mcd_agent_state.ActionInitState, settings.vips[i].cguid);
#endif
        break;
    }
    plat_closure_apply(sdf_replicator_notification_complete_cb, &completion);
#if 0

    #ifdef SIMPLE_REPLICATION
    if (!SDFSimpleReplicationEnabled()) {
         plat_log_msg(20261, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                            "Applying call back completion\n");
        plat_closure_apply(sdf_replicator_notification_complete_cb, &completion);
    }
    #else
	plat_closure_apply(sdf_replicator_notification_complete_cb, &completion);
    #endif
#endif
}

static void
ipf_notify_simple(uint64_t cguid, int active)
{
    int       i;
    uint32_t  address;
    uint32_t  mask;
    char *    if_id;
    struct timeval   expiration;

    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    i = ipf_find_settings(cguid);

    if (i < 0) {
        return;
    }

    SDF_I_InvalContainer(&Mcd_agent_state.ActionInitState, cguid);

    if (active) {
        address = ipf_get_host(settings.vips[i].address);
        mask    = settings.vips[i].mask;
        if_id   = settings.vips[i].if_id;
#ifdef SDFREPLICATION
        if (address != 0) {
            if (settings.vips[i].tcp_port != 0) {
                ipf_remember(address, mask, if_id, -1, expiration);

                if (! ipf_bind_any) {
                    mcd_vip_server_socket(&settings.vips[i], PORT_TYPE_TCP);
                }
            }

            if (settings.vips[i].udp_port != 0) {
                ipf_remember(address, mask, if_id, -1, expiration);
    
                if (! ipf_bind_any) {
                    mcd_vip_server_socket(&settings.vips[i], PORT_TYPE_UDP);
                }
           }
        }
#endif /* SDFREPLICATION */
    } else {
        address = ipf_get_host(settings.vips[i].address);

        if (address != 0) {
            if (settings.vips[i].tcp_port != 0) {
                ipf_forget(address); /* for tcp */
            }

            if (settings.vips[i].udp_port != 0) {
                ipf_forget(address); /* for udp */
            }
        }
    }

    return;
}

void
ipf_simple_dead(int my_node_id, int dead_node)
{
#ifdef VIPGROUP_SUPPORT
    ipf_start_simple(dead_node);
#else
    for (int i = 0; i < MCD_MAX_NUM_CNTRS; i++) {
        if (settings.vips[i].is_standby) {
            ipf_notify_simple(settings.vips[i].cguid, 1);
        }
    }
#endif

    return;
}

/*
 *  ipf_set_signals - set signal handling routine
 *
 *     Most signals are routed to a cleanup routine when running.
 *
 *     The main memcached process is set to catch SIGPIPE and abort when that
 *  signal is received.  Thus, if the background failover process dies, the
 *  memcached instance can be restarted.
 */

static void
ipf_set_signals(void (*routine)(int))
{
    int  i;
    int  result;
    struct sigaction  action;

    action.sa_handler = routine;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_ONSTACK;
    action.sa_restorer = NULL;

    for (i = 1; i < NSIG; i++) {
        switch (i) {
        case SIGINT:
        case SIGALRM:
        case SIGPOLL:
        case SIGCONT:
        case SIGCHLD:
        case SIGSTOP:
        case SIGTSTP:
        case SIGTTIN:
        case SIGTTOU:
        case SIGTRAP:
        case SIGKILL:
        case SIGPIPE:
        case SIGWINCH:
            break;

/*      case SIGPIPE: XXX */
        case SIGABRT:
        default:
            if (i < SIGRTMIN) {
               sigaction(i, &action, NULL);
            }

            break;
        }
    }

    ipf_alt_stack.ss_sp    = ipf_stack_region;
    ipf_alt_stack.ss_size  = sizeof(ipf_stack_region);
    ipf_alt_stack.ss_flags = 0;

    result = sigaltstack(&ipf_alt_stack, NULL);

    if (result < 0) {
        dprintf(" ==== sigaltstack failed\n");
    }

    return;
}

static void
ipf_start_replication(struct sdf_replicator *replicator)
{
    sdf_replicator_notification_cb_t my_closure;

    ipf_replicator = replicator;

    if (ipf_replicator == null) {
        plat_log_msg(20262,
            PLAT_LOG_CAT_SDF_APP_MEMCACHED,
            PLAT_LOG_LEVEL_FATAL,
            "The replicator doesn't exist");
        plat_exit(1);
    }

    my_closure = sdf_replicator_notification_cb_create
                    (PLAT_CLOSURE_SCHEDULER_ANY_OR_SYNCHRONOUS,
                                  ipf_notify, null);
    if (ipf_replicator->add_notifier != NULL) {
        ipf_replicator->add_notifier(ipf_replicator, my_closure);
    }

    return;
}


/* 
 * ipf_is_node_ready
 * This function returns whether the node is ready.
 * The node becomes ready after it gets one virtual group atleast 
 */
#ifdef SIMPLE_REPLICATION
SDF_boolean_t ipf_is_node_ready() {
   struct qrep_state *repstate;
   struct sdf_agent_state *mcd_state;
   qrep_node_state_t *node;
   SDF_action_state_t *pas;
   int my_rank = msg_sdf_myrank();

   mcd_state = &(Mcd_agent_state);
   pas = mcd_state->ActionInitState.pcs;
   repstate = &(pas->qrep_state);
   node =  &(repstate->node_state[my_rank]); 
   if( node->num_vgrps_being_serviced == 1 ) {
       return SDF_TRUE;
   }
   return SDF_FALSE;
}

SDF_boolean_t ipf_is_node_dominant() {
   struct qrep_state *repstate;
   struct sdf_agent_state *mcd_state;
   qrep_node_state_t *node;
   SDF_action_state_t *pas;
   int my_rank = msg_sdf_myrank();

   mcd_state = &(Mcd_agent_state);
   pas = mcd_state->ActionInitState.pcs;
   repstate = &(pas->qrep_state);
   node =  &(repstate->node_state[my_rank]);

   if( node->num_vgrps_being_serviced == 2 ) {
       return SDF_TRUE;
   }
   return SDF_FALSE;
}

SDF_boolean_t ipf_is_node_started_first_time() {
    struct sdf_agent_state *mcd_state;
    mcd_state = &(Mcd_agent_state);
    if( mcd_state->config.system_recovery == SYS_FLASH_REFORMAT ) {
        return SDF_TRUE;
    }
    return SDF_FALSE;
}

SDF_boolean_t ipf_is_node_started_in_auth_mode() {
    struct sdf_agent_state *mcd_state;
    mcd_state = &(Mcd_agent_state);
    if( mcd_state->config.auth_mode == 1 ) {
        return SDF_TRUE;
    }
    return SDF_FALSE;
}


#ifndef IPF_TEST
SDF_boolean_t ipf_is_node_independent() {
    char prop_name[128];
    int group_id, mynode;
    char *grp_type;

    mynode = msg_sdf_myrank();

    /* Check whether the node is in indpenedent group */
    sprintf( prop_name, "NODE[%d].GROUP_ID", mynode);
    group_id = getProperty_uLongInt(prop_name, -1);
    if( group_id == -1 ) {
        /* Group ID is not specified. It could be because of old prop file */
        return SDF_TRUE;
    }

    sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].TYPE",group_id);
    grp_type = (char *)getProperty_String(prop_name, "");
    if( strcmp(grp_type,"MIRRORED") == 0 ) {
        return SDF_FALSE;
    }

    return SDF_TRUE;
}

SDF_boolean_t ipf_is_node_in_mirrored() {
    char prop_name[128];
    int group_id, mynode;
    char *grp_type;

    mynode = msg_sdf_myrank();

    /* Check whether the node is in indpenedent group */
    sprintf( prop_name, "NODE[%d].GROUP_ID", mynode);
    group_id = getProperty_uLongInt(prop_name, -1);
    if( group_id == -1 ) {
        /* Group ID is not specified. It could be because of old prop file */
        return SDF_FALSE;
    }

    sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].TYPE",group_id);
    grp_type = (char *)getProperty_String(prop_name, "");
    if( strcmp(grp_type,"MIRRORED") == 0 ) {
        return SDF_TRUE;
    }

    return SDF_FALSE;
}
#endif

#endif

#ifndef IPF_TEST
int is_ipf_enabling_required() {
    char prop_name[128];
    int group_id, mynode;
    char *grp_type;

    mynode = msg_sdf_myrank();
    /* My node should be in independent group to clone */
    /* Check whether the node is in indpenedent group */
   
    sprintf( prop_name, "NODE[%d].GROUP_ID", mynode);
    group_id = getProperty_uLongInt(prop_name, -1);
    if( group_id == -1 ) {
        return 0;
    }

    sprintf( prop_name, "SDF_CLUSTER_GROUP[%d].TYPE",group_id);
    grp_type = (char *)getProperty_String(prop_name, "");
    if( (strcmp(grp_type,"MIRRORED") == 0) || (strcmp(grp_type,"N+1") == 0)  ) {
        return 1;
    }
    return 0;
}
#endif

/*
 *  ipf_start - initialize the ipf interface
 *
 *     This procedure starts the ipf failover process after creating a socket
 *  pair for communication.  Currently, communication is one way, but we might
 *  need to ack commands eventually.
 */

void
ipf_set_active(int bind_any)
{
    int    result;
    int    sockets[2];
    pid_t  pid;

    ipf_bind_any = bind_any;

    pthread_mutex_lock(&ipf_lock);

    if (ipf_active) {
        pthread_mutex_unlock(&ipf_lock);
        return;
    }

    ipf_active = true;
    pthread_mutex_unlock(&ipf_lock);

    sdf_agent_start_cb = ipf_start;
    sdf_start_simple   = ipf_start_simple;
    sdf_notify_simple  = ipf_notify_simple;
    sdf_simple_dead    = ipf_simple_dead;

    cmc_settings       = &settings;
#ifdef SIMPLE_REPLICATION
    sdf_is_node_started_first_time = ipf_is_node_started_first_time;
    sdf_is_node_started_in_auth_mode = ipf_is_node_started_in_auth_mode;
#   ifndef IPF_TEST
#ifdef SDFREPLICATION
    sdf_mcd_format_container_internal = mcd_format_container_internal;
#endif /* SDFREPLICATION */
    sdf_mcd_start_container_internal  = mcd_start_container_internal;
    sdf_mcd_stop_container_internal   = mcd_stop_container_internal;
#ifdef SDFREPLICATION
    sdf_mcd_format_container_byname_internal = mcd_format_container_byname_internal;
#endif /* SDFREPLICATION */
    sdf_mcd_start_container_byname_internal  = mcd_start_container_byname_internal;
    sdf_mcd_stop_container_byname_internal   = mcd_stop_container_byname_internal;
#ifdef SDFREPLICATION
    sdf_mcd_get_tcp_port_by_cguid     = mcd_get_tcp_port_by_cguid;
    sdf_mcd_get_cname_by_cguid        = mcd_get_cname_by_cguid;
    sdf_mcd_is_container_running      = mcd_is_container_running;
    sdf_mcd_is_container_running_byname = mcd_is_container_running_byname;
    sdf_mcd_processing_container_commands = mcd_processing_container_cmds;
#endif /* SDFREPLICATION */
    sdf_action_init_ptr = get_action_init_state;
    fthLockInit(&rule_table_lock);
#   endif
#endif
#ifdef VIPGROUP_SUPPORT
    sdf_remove_vip = remove_vip;
#endif

#ifndef IPF_TEST
    if( is_ipf_enabling_required() != 1 ) {
        return ;
    }
#endif

#if 0
    result = socketpair(AF_UNIX, SOCK_STREAM, 0, sockets);
#endif
    result = pipe(sockets);

    if (result < 0) {
        return;
    }

    /*
     * XXX: drew 2010-01-12 fork instead of plat fork since we've yet
     * to make ffdc work with multiple heavy-weight processes sharing
     * the same buffer (that will mean versioning ffdc) and this isn't
     * a replacement as in the daemonize mode.
     */
    pid = fork();

    if (pid != 0) {
        ipf_pipe_fd = sockets[1];
        close(sockets[0]);

#if 0
        /*
         * XXX: drew 2010-04-14 Removed because ipf_signal_handler
         * incorrectly translates all failures (SIGSEGV, SIGBUS) into 
         * exit calls with no core dumps
	 *
         * Calls to exit(3) are also improper since that causes
         * atexit(3) and destructor code to run potentially 
         */

        ipf_set_signals(ipf_signal_handler);
#endif

        if (settings.start_vips) {
            for (int i = 0; i < MCD_MAX_NUM_CNTRS; i++) {
                if
                (
                    settings.vips[i].tcp_port == 0
                && settings.vips[i].udp_port == 0
                ) {
                    continue;
                }

                unsigned address = ipf_get_host(settings.vips[i].address);
                unsigned mask    = settings.vips[i].mask;
                char *   if_id   = settings.vips[i].if_id;
                struct timeval expiration;

                expiration.tv_sec = ((unsigned long) -1) >> 1;
                expiration.tv_usec = 0;

                if (settings.vips[i].tcp_port != 0) {
                    ipf_remember(address, mask, if_id, -1, expiration);
                }

                if (settings.vips[i].udp_port != 0) {
                    ipf_remember(address, mask, if_id, -1, expiration);
                }
            }
        }

        return;
    }

    ipf_pipe_fd = sockets[0];
    close(sockets[1]);
    ipf_handler();
    plat_exit(0);
}

int
ipf_start(struct sdf_replicator *replicator)
{
    if (! ipf_active) {
        return 0;
    }

    ipf_start_replication(replicator);
    /*
    if (! settings.start_vips) {
        ipf_start_replication(replicator);
    }
    */ 

    return 1;
}

#ifdef VIPGROUP_SUPPORT
int ipf_start_simple(int vipgroup)
{
    struct sdf_agent_state *mcd_state;
    qrep_state_t *ps;
    int new_lease, my_node_id;
    struct timeval   expiration;
    int16_t intra_node_vip_group_id=vipgroup;
    plat_log_msg(20263, PLAT_LOG_CAT_SDF_PROT, 
                 PLAT_LOG_LEVEL_INFO, "Skipping start simple\n");
    return 0;

    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    mcd_state = &(Mcd_agent_state);
    SDF_action_state_t *pas = mcd_state->ActionInitState.pcs;
    ps = &(pas->qrep_state);
    my_node_id = msg_sdf_myrank();
    configure_vip(ps->vip_config, intra_node_vip_group_id);

    ps->node_state[my_node_id].type = QREP_NODE_TYPE_ACTIVE;
    /*Remember the information so that, when process dies, we can clear
      old opf functions require address, if name .. for remembering
      we need to remember only the vip group id. So some dummy values are passed */
    new_lease = ipf_add_vgrp_to_list(intra_node_vip_group_id, ps->node_state[vipgroup].num_vips,
                                               &(ps->node_state[vipgroup].vipgroup[0]), 0,NULL,expiration);
    if (new_lease) {
        ipf_send_vgroup(ipf_type_remember, intra_node_vip_group_id,ps->node_state[vipgroup].num_vips,
                                               &(ps->node_state[vipgroup].vipgroup[0]), 0,NULL,expiration);
    }
    return 0;
}
#else
int ipf_start_simple(int mynode)
{
    unsigned address;
    unsigned mask;
    char *   if_id;
    struct timeval expiration;

    if (! ipf_active) {
        return 0;
    }

    for (int i = 0; i < MCD_MAX_NUM_CNTRS; i++) {
        if
        (
            settings.vips[i].tcp_port == 0
        && settings.vips[i].udp_port == 0
        ) {
            continue;
        }

        if (settings.vips[i].is_standby != 0) {
            continue;
        }

        address = ipf_get_host(settings.vips[i].address);
        mask    = settings.vips[i].mask;
        if_id   = settings.vips[i].if_id;

        expiration.tv_sec = ((unsigned long) -1) >> 1;
        expiration.tv_usec = 0;

        if (settings.vips[i].tcp_port != 0) {
            ipf_remember(address, mask, if_id, -1, expiration);
        }
        
        if (settings.vips[i].udp_port != 0) {
            ipf_remember(address, mask, if_id, -1, expiration);
        }
    }

    return 0;
}
#endif

#ifdef VIPGROUP_SUPPORT
static void ipf_send_vgroup( int command, int vgrp,int num_vips, qrep_ctnr_iface_t *vips, int num_pfws, 
                                                  ipf_pfw_entry_t *pfw_entries, struct timeval expiration ) {
    ipf_msg_t  message;
    void *     buffer;
    ssize_t    result;
    int        count;

    if (ipf_pipe_fd < 0) {
#ifdef IPF_TEST
    plat_abort();
#else
    plat_assert(ipf_pipe_fd >  0 );
#endif
    }

    memset(&message, 0, sizeof(message));
    message.command    = command;
    message.address    = vgrp;
    message.expiration = expiration;
    message.num_vips   = num_vips;
    if( num_vips > 0 ) {
        memcpy(message.vips, vips, sizeof( qrep_ctnr_iface_t ) * num_vips );
    }
    message.num_pfws = num_pfws;
    if( num_pfws > 0 ) {
        memcpy(message.pfw_entries, pfw_entries,sizeof(ipf_pfw_entry_t) * num_pfws);
    }

    buffer = &message;
    count = 0;
    do {
        errno = EINVAL;
        result = write(ipf_pipe_fd, buffer + count, sizeof(message) - count);
        if (result <= 0 && (errno == EINTR || errno == EAGAIN)) {
            sleep(1);
            result = 0;
        } else if (result <= 0) {
            /* ipf_head should have the full list of interfaces. */
            ipf_pipe_fd = -1; /* don't try to send anymore */
            ipf_fail(0);
        }
        count += result;
    } while (count < sizeof(message));
}
#endif

/*
 *  ipf_send - send a message to the ipf process
 */

static void
ipf_send
(
    int command, uint32_t address, char *if_1, int if_2, struct timeval expiration
)
{
    ipf_msg_t  message;
    void *     buffer;
    ssize_t    result;
    int        count;

    if (ipf_pipe_fd < 0) {
#ifdef IPF_TEST
       plat_abort();
#else
       plat_assert( ipf_pipe_fd > 0 ) ;
#endif
    }

    tprintf("ipf_send:  0x%x, (%s:%d) command 0x%d\n",
        (unsigned) address, if_1, if_2, command);
 

    memset(&message, 0, sizeof(message));

    message.command    = command;
    message.address    = address;
    message.if_2       = if_2;
    message.expiration = expiration;

    if (if_1 != null) {
        memset(message.if_1,0,sizeof(message.if_1));
        strncpy(message.if_1, if_1, sizeof(message.if_1)-1);
    }

    buffer = &message;

    count = 0;

    do {
        errno = EINVAL;

        result = write(ipf_pipe_fd, buffer + count, sizeof(message) - count);

        if (result <= 0 && (errno == EINTR || errno == EAGAIN)) {
            sleep(1);
            result = 0;
        } else if (result <= 0) {
            /* ipf_head should have the full list of interfaces. */
            ipf_pipe_fd = -1; /* don't try to send anymore */
            ipf_fail(0);
        }

        count += result;
    } while (count < sizeof(message));

    return;
}

void
ipf_remember
(
    uint32_t  address,
    uint32_t  netmask,
    char *    if_1,
    int16_t   if_2,
    struct timeval expiration
)
{
    int  new_lease;

    address = ntohl(address);
    new_lease = ipf_add_to_list(address, if_1, &if_2, expiration);

    if (new_lease) {
        ipf_send(ipf_type_remember, address, if_1, if_2, expiration);
        ipf_create_if(address, netmask, if_1, if_2);
    }

    return;
}

void
ipf_forget(uint32_t address)
{
    char     if_1[64];
    int16_t  if_2;

    if_2 = 0; /* keep gcc happy */

    ipf_remove_from_list(address, NULL);
    ipf_send(ipf_type_forget, address, if_1, if_2, null_expiration);
    return;
}

static void
ipf_ignore_sigpipe(void)
{
    struct sigaction  action;

    action.sa_handler  = SIG_IGN;
    action.sa_flags    = SA_ONSTACK;
    action.sa_restorer = NULL;
    sigemptyset(&action.sa_mask);

    sigaction(SIGPIPE, &action, NULL);
    return;
}

void
ipf_fail(int signal)
{
    ipf_set_signals(SIG_DFL);

    if (ipf_pipe_fd >= 0) {
        ipf_ignore_sigpipe();
        ipf_send(ipf_type_fail, 0, 0, 0, null_expiration);
        sleep(1);
        close(ipf_pipe_fd);
    }

    plat_abort();
}

void
ipf_signal_handler(int signal)
{
    ipf_set_signals(SIG_DFL);
    if (ipf_pipe_fd >= 0) {
        ipf_ignore_sigpipe();
        ipf_send(ipf_type_fail, 0, 0, 0, null_expiration);
        sleep(1);
        close(ipf_pipe_fd);
    }
    plat_log_msg(80019, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG, "Got signal %d, exiting",signal); 
    plat_exit(0);
}


void
ipf_exit(void)
{

    if (ipf_pipe_fd >= 0) {
        ipf_ignore_sigpipe();
        ipf_send(ipf_type_exit, 0, 0, 0, null_expiration);
        sleep(1);
        close(ipf_pipe_fd);
    }

    ipf_remove_all();
    return;
}

int execute_system_cmd(char *cmd) {
    plat_log_msg(20264, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG, "Cmd:%s",cmd);
    return system(cmd);
}

#ifdef VIPGROUP_SUPPORT

SDF_action_init_t *get_action_init_state() {
    struct sdf_agent_state *mcd_state;
    mcd_state = &(Mcd_agent_state);
    return &(mcd_state->ActionInitState);
}

void process_state_machine_command( struct mcd_conn * c, mcd_request_t * req ) {
     SDF_action_init_t * pai;
     qrep_state_t * ps;
     int my_node_id, my_grp_id,failed_node_grp_id;

     pai = (SDF_action_init_t *)c->pai;
     ps = &(pai->pcs->qrep_state);
     my_node_id = msg_sdf_myrank();

     if( req->stm_testcmd == 1 ) {
         /*Activate the node. It configures the local virtual IP group*/
         if( (req->stm_test_arg >= ps->nnodes) || 
             (req->stm_test_arg < 0 ) ) {
              plat_log_msg(20265, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Invalid Node(vipgrp) :%d specified command:%d\n",req->stm_test_arg,
                                                                           req->stm_testcmd );
              return;
         }
         if( ps->node_state[my_node_id].type != QREP_NODE_TYPE_STANDBY ) {
              plat_log_msg(20266, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "The Node %d is already active\n",my_node_id);
              return;
         }
         /*req->stm_test_arg holds the Virtual IP group that needs to be configured to the 
           current node*/
         ps->node_state[my_node_id].type = QREP_NODE_TYPE_ACTIVE;
         configure_vip(ps->vip_config, req->stm_test_arg);
     }
     else if( req->stm_testcmd == 2 ) {
         /*node down event. if local node is standby or miirornode, it 
           migrates the VIP group of the specfied failed node*/
         if( (req->stm_test_arg >= ps->nnodes) || 
             (req->stm_test_arg < 0 ) ) {
              plat_log_msg(20265, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Invalid Node(vipgrp) :%d specified command:%d\n",req->stm_test_arg,
                                                                           req->stm_testcmd );
              return;
         }
         my_grp_id = ps->node_state[my_node_id].group_id;
         failed_node_grp_id = ps->node_state[req->stm_test_arg].group_id;
         if( my_grp_id != failed_node_grp_id ) {
              plat_log_msg(20247, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Faile node:%d(grp:%d) not in my group:%d, ignore failure\n",req->stm_test_arg,
                                    failed_node_grp_id,my_grp_id);
              return;
         }

         if(  my_grp_id == SDF_CLUSTER_GRP_TYPE_NPLUS1 ) {
             /*N+1 group type*/
             /*Ignore if the current node is active already*/
             if( ps->node_state[my_node_id].type != QREP_NODE_TYPE_STANDBY ) {
                  plat_log_msg(20248, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Current node %d is already active. Ignore failure\n",my_node_id);
                  return;
             }
         }
         else if( my_grp_id == SDF_CLUSTER_GRP_TYPE_MIRRORED) {
            if( ps->node_state[my_node_id].num_vgrps_being_serviced >= MAX_VIP_GROUPS_PER_NODE){
                  plat_log_msg(20252, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Current node %d is already handling %d VIP groups. Ignore failure\n",my_node_id,
                                        ps->node_state[my_node_id].num_vgrps_being_serviced);
                  return;
            }
         }
         /*req->stm_test_arg holds the Virtual IP group that needs to be configured to the 
           current node*/
         configure_vip(ps->vip_config, req->stm_test_arg);
     }
     else if( req->stm_testcmd == 3 ) {
         /*Giveup VIP group event. if local node miirornode, it 
           gives up the VIP group of the recovering node*/
         if( (req->stm_test_arg >= ps->nnodes) ||
             (req->stm_test_arg < 0 ) ) {
              plat_log_msg(20265, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                            "Invalid Node(vipgrp) :%d specified command:%d\n",req->stm_test_arg,
                                                                           req->stm_testcmd );
              return;
         }
         /*req->stm_test_arg holds the Virtual IP group that needs to be removed from the 
           current node*/
         remove_vip(ps->vip_config, req->stm_test_arg);
         if( ps->node_state[my_node_id].num_vgrps_being_serviced < 1 ) {
             ps->node_state[my_node_id].type =  QREP_NODE_TYPE_STANDBY;
         }
     }
}

#ifndef IPF_TEST
int get_rule_table_id() {
    int i,rule_table_offset,my_rank;
    struct sdf_agent_state *mcd_state;
    SDF_action_state_t *pas;
    struct qrep_state *repstate;
    fthWaitEl_t *wait_list;

    my_rank = msg_sdf_myrank();
    mcd_state = &(Mcd_agent_state);
    pas = mcd_state->ActionInitState.pcs;
    repstate = &(pas->qrep_state);

    rule_table_offset = RULE_TABLE_OFFSET; 
    if( repstate->node_state[my_rank].is_virtual == 1 ) {
        rule_table_offset = RULE_TABLE_OFFSET+100; 
    }
    wait_list = fthLock(&rule_table_lock, 1, NULL);
    for( i = rule_table_offset; i < MAX_NUM_RULE_TABLES; i++ ) {
        if( rule_table_list[i] == 0 ) {
            rule_table_list[i] = 1;
            break;
        }
    }
    fthUnlock(wait_list);
    if ( i >= MAX_NUM_RULE_TABLES ) {
        return -1;
    } 
    return i;
}

void ipf_handle_vip_manage_laptop(int add, int intra_node_vip_group_id) {
    struct sdf_agent_state *mcd_state;
    qrep_state_t *ps;
    int my_node_id, vport, rport, vnode, rnode, i,num_pfws = 0;
    ipf_pfw_entry_t pfw_entries[NUM_PFW_ENTRIES];

    mcd_state = &(Mcd_agent_state);
    SDF_action_state_t *pas = mcd_state->ActionInitState.pcs;
    ps = &(pas->qrep_state);
    my_node_id = msg_sdf_myrank();

    if( ps->node_state[my_node_id].is_virtual != 1 ) {
        return;
    }
    plat_log_msg(20267, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_DEBUG,
                                        "NODE IS VIRTUAL  NUM_PFWS:%d \n",num_pfws);
    vnode = my_node_id;
    rnode = my_node_id - 1;
    num_pfws = ps->node_state[my_node_id].nctnrs_node;
    for( i = 0; i < num_pfws; i++ ) {
#ifdef SDFREPLICATION 
        mcd_get_tcp_port_by_cguid(ps->node_state[vnode].cntrs[i].cguid, &vport);
#endif /* SDFREPLICATION */
        rport = vport - 1000;
        pfw_entries[i].vport = vport;
        pfw_entries[i].rport = rport;
#ifndef IPF_TEST
        if( add == 1 ) {
            ipf_handle_container_add(vport,rport,intra_node_vip_group_id);
        }
        else {
            ipf_handle_container_delete(vport,rport,intra_node_vip_group_id);
        }
#endif
    }
}


void ipf_handle_container_add(int vport, int rport, int node){
    int my_rank, num_vips,num_pfws;
    SDF_action_state_t *pas;
    struct sdf_agent_state *mcd_state;
    struct qrep_state *repstate;
    struct timeval   expiration;
    ipf_pfw_entry_t pfw_entries[1];

    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    mcd_state = &(Mcd_agent_state);
    pas = mcd_state->ActionInitState.pcs;
    repstate = &(pas->qrep_state);
    my_rank = msg_sdf_myrank();

    plat_log_msg(20268, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                      "container add rport: %d vport:%d node:%d\n",rport,vport,node);
    if( repstate->node_state[my_rank].is_virtual != 1 ) {
        plat_log_msg(20269, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                     "The current node is not virtual. Ignore");
        return;
    }
    num_vips = repstate->node_state[node].num_vips;
    if( num_vips > MAX_PORT_FWD_VIPS ) {
        num_vips = MAX_PORT_FWD_VIPS;
    }
    memcpy(pfw_entries[0].vips,repstate->node_state[node].vipgroup,num_vips * sizeof(qrep_ctnr_iface_t));
    num_pfws = 1;
    pfw_entries[0].num_vips = num_vips;
    pfw_entries[0].vport = vport;
    pfw_entries[0].rport = rport;
    
    ipf_send_vgroup( ipf_type_remember,vport+node, 0, NULL,1, pfw_entries,expiration);
    return;
}

void ipf_handle_container_delete(int vport, int rport, int node){
    int my_rank;
    SDF_action_state_t *pas;
    struct sdf_agent_state *mcd_state;
    struct qrep_state *repstate;
    struct timeval   expiration;

    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    mcd_state = &(Mcd_agent_state);
    pas = mcd_state->ActionInitState.pcs;
    repstate = &(pas->qrep_state);

    my_rank = msg_sdf_myrank();
    if( repstate->node_state[my_rank].is_virtual != 1 ) {
        plat_log_msg(20269, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_INFO,
                     "The current node is not virtual. Ignore");
        return;
    }
    ipf_send_vgroup( ipf_type_forget,vport+node, 0,NULL,0,NULL,expiration );
    return;
}

void put_rule_table_id( int id ) {
    fthWaitEl_t *wait_list;

    if( (id < RULE_TABLE_OFFSET) || (id > MAX_NUM_RULE_TABLES) ) {
        plat_log_msg(20270, PLAT_LOG_CAT_SDF_APP_MEMCACHED, PLAT_LOG_LEVEL_FATAL,
                     "Invalid rule table id %d\n",id);
        return;
    }
    wait_list = fthLock(&rule_table_lock, 1, NULL);
    rule_table_list[id]=0;
    fthUnlock(wait_list);
}

int ipf_add_vip(char *node_name, char *ifname, char *ip, char *mask, char *gw, char *err_str ) {
    int  node_rank,myrank,peerrank,i,vip_index;
    qrep_node_state_t *mynode,*peernode,*givennode;
    struct sdf_agent_state *mcd_state;
    SDF_action_state_t *pas;
    struct qrep_state *repstate;
    fthWaitEl_t *wait_list;
    struct timeval   expiration;

    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    myrank = msg_sdf_myrank();
    mcd_state = &(Mcd_agent_state);
    pas = mcd_state->ActionInitState.pcs;
    repstate = &(pas->qrep_state); 

    plat_log_msg(20271, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                                          "Add VIP:%s mask:%s gw:%s iface:%s node:%s",
                                           ip,mask,gw,ifname,node_name);

    /* Check My Node Mode */
    mynode = &(repstate->node_state[myrank]);
    if ( repstate->groups[mynode->group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        plat_log_msg(20272, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                                          "Current node is not in Mirrored Mode");
        sprintf(err_str,"Current node is not in Mirrored Mode");
        return 1;
    }

    /* Find Node ID of the given node */
    node_rank = -1;
    for( i = 0 ; i < repstate->nnodes; i++ ) {
        if( strcmp(repstate->node_state[i].host_name,node_name) == 0) {         
            node_rank = i;
        }
    }
    if( node_rank < 0 ) {
        plat_log_msg(20273, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR, 
                                          "Unable to find node ID for %s",node_name);
        sprintf(err_str,"Unable to find node ID for %s",node_name);
        return 1;
    }

    /* Find the Peer Node */
    peerrank=repstate->groups[mynode->group_id].nodes[0];
    if( repstate->groups[mynode->group_id].nodes[0] == myrank ){
        peerrank=repstate->groups[mynode->group_id].nodes[1];
    }
    peernode = &(repstate->node_state[peerrank]);

    /* Check whether the given node is either local node or peer node */
    if( (node_rank != myrank) && (node_rank != peerrank) ) {
        plat_log_msg(20274, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR, 
                   "The node %s(%d) not in local mirror group",node_name,node_rank);
        sprintf(err_str,"The node %s(%d) not in local mirror group",node_name,node_rank);
        return 1;
    }
    givennode = mynode;
    if( node_rank == peerrank ) {
        givennode = peernode;    
    }
    wait_list = fthLock(&(givennode->lock), 1, NULL);
    for( i = 0; i < givennode->num_vips; i++ ) {
        if( (strcmp(givennode->vipgroup[i].ip, ip) == 0 ) &&
                                 (strcmp(givennode->vipgroup[i].name, ifname) == 0)) {
            plat_log_msg(20275, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                   "The VIP %s on %s already exists",ip,ifname);
            sprintf(err_str,"The VIP %s on %s already exists",ip,ifname);
            fthUnlock(wait_list);
            return 1;
        }
    }

    if( givennode->num_vips >= QREP_MAX_IFS_PER_NODE ) {
        plat_log_msg(20276, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                    "Maximum allowed number of VIPs(%d) already configured",QREP_MAX_IFS_PER_NODE);
        sprintf(err_str,"Maximum allowed number of VIPs(%d) already configured",QREP_MAX_IFS_PER_NODE);
        fthUnlock(wait_list);
        return 1;
    }
    strcpy(givennode->vipgroup[givennode->num_vips].ip,ip);
    strcpy(givennode->vipgroup[givennode->num_vips].name,ifname);
    strcpy(givennode->vipgroup[givennode->num_vips].mask,mask);
    strcpy(givennode->vipgroup[givennode->num_vips].gw,gw);
    givennode->vipgroup[givennode->num_vips].rule_table_id = 0;
    vip_index = givennode->num_vips;
    givennode->num_vips++;
    fthUnlock(wait_list);
    //print_vip_info("ipf_add_vip:",givennode->vipgroup,givennode->num_vips);

    /* Add the new VIP to the VIP group and then configure it if current node 
       services the vip group*/
    wait_list = fthLock(&(mynode->lock), 1, NULL);
    for( i = 0; i < mynode->num_vgrps_being_serviced; i++ ) {
        if(mynode->serviced_vgrp_ids[i] == node_rank) {
            plat_log_msg(20277, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                   "The Current node servicing %s. Adding VIP %s on %s",node_name,ip,ifname);
            givennode->vipgroup[vip_index].rule_table_id = get_rule_table_id();
            ipf_send_vgroup( ipf_type_remember,node_rank, 1, 
                         &(givennode->vipgroup[vip_index]), 0, NULL,expiration);
            break;
        }
    }
    fthUnlock(wait_list);
    return 0;
}

int ipf_delete_vip(char *node_name, char *ifname, char *ip, char *err_str) {
    qrep_ctnr_iface_t delete_if;
    int  node_rank,myrank,peerrank,i,vip_index;
    qrep_node_state_t *mynode,*peernode,*givennode;
    struct sdf_agent_state *mcd_state;
    SDF_action_state_t *pas;
    struct qrep_state *repstate;
    fthWaitEl_t *wait_list;
    struct timeval   expiration;

    plat_log_msg(20278, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_DEBUG,
                                          "Delete VIP:%s iface:%s node:%s",
                                           ip,ifname,node_name);
    expiration.tv_sec = ((unsigned long) -1) >> 1;
    expiration.tv_usec = 0;

    myrank = msg_sdf_myrank();
    mcd_state = &(Mcd_agent_state);
    pas = mcd_state->ActionInitState.pcs;
    repstate = &(pas->qrep_state);

    /* Check My Node Mode */
    mynode = &(repstate->node_state[myrank]);
    if ( repstate->groups[mynode->group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
        sprintf(err_str,"Current node is not in Mirrored Mode");
        return 1;
    }

    /* Find Node ID of the given node */
    node_rank = -1;
    for( i = 0 ; i < repstate->nnodes; i++ ) {
        if( strcmp(repstate->node_state[i].host_name,node_name) == 0) {
            node_rank = i;
        }
    }
    if( node_rank < 0 ) {
        plat_log_msg(20273, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                                          "Unable to find node ID for %s",node_name);
        sprintf(err_str,"Unable to find node ID for %s",node_name);
        return 1;
    }

    /* Find the Peer Node */
    peerrank=repstate->groups[mynode->group_id].nodes[0];
    if( repstate->groups[mynode->group_id].nodes[0] == myrank ){
        peerrank=repstate->groups[mynode->group_id].nodes[1];
    }
    peernode = &(repstate->node_state[peerrank]);

    /* Check whether the given node is either local node or peer node */
    if( (node_rank != myrank) && (node_rank != peerrank) ) {
        plat_log_msg(20274, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                   "The node %s(%d) not in local mirror group",node_name,node_rank);
        sprintf(err_str,"The node %s(%d) not in local mirror group",node_name,node_rank);
        return 1;
    }
    givennode = mynode;
    if( node_rank == peerrank ) {
        givennode = peernode;
    }
    vip_index = -1;
    wait_list = fthLock(&(givennode->lock), 1, NULL);
    for( i = 0; i < givennode->num_vips; i++ ) {
        if( (strcmp(givennode->vipgroup[i].ip, ip) == 0 ) &&
                                 (strcmp(givennode->vipgroup[i].name, ifname) == 0)) {
            vip_index = i;
            memcpy(&(delete_if), &(givennode->vipgroup[i]), sizeof(qrep_ctnr_iface_t));
            break;
        }
    }
    if( vip_index < 0 ) {
        plat_log_msg(20279, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_ERROR,
                   "The VIP %s on %s does not exist",ip,ifname);
        sprintf(err_str,"The VIP %s on %s does not exist",ip,ifname);
        fthUnlock(wait_list);
        return 1;
    }

    for( i = vip_index; i < (givennode->num_vips -1); i++ ) {
        memcpy(&(givennode->vipgroup[i]), &(givennode->vipgroup[i+1]), sizeof(qrep_ctnr_iface_t));
    }
    givennode->num_vips--;
    fthUnlock(wait_list);
    //print_vip_info("ipf_delete_vip:",givennode->vipgroup,givennode->num_vips);

    /* Add the new VIP to the VIP group and then configure it if current node 
       services the vip group*/
    wait_list = fthLock(&(mynode->lock), 1, NULL);
    for( i = 0; i < mynode->num_vgrps_being_serviced; i++ ) {
        if(mynode->serviced_vgrp_ids[i] == node_rank) {
            plat_log_msg(20280, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                   "The Current node servicing %s. Removing VIP %s from %s",node_name,ip,ifname);
            put_rule_table_id(delete_if.rule_table_id);
            ipf_send_vgroup( ipf_type_forget,node_rank, 1,&(delete_if),0,NULL,expiration );
            break;
        }
    }
    fthUnlock(wait_list);
    return 0;
}
#endif


/**
 * @brief removes given VIP group configuration from local node
 * @return 0 on success, 1 on failure
 */
int remove_vip( struct sdf_vip_config *config, int vip_group_id) {
#ifdef IPF_TEST_VIP
   int i, maskbits,j;
   unsigned char bitarr[16]={0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4};
   unsigned int md[4];
   unsigned int ip[4];
   char netaddr[32];
   char system_cmd[512];
#else
   int i;
#endif
   sdf_vip_group_t *vgrp;
   struct qrep_state *repstate;
   qrep_node_state_t *node;
   struct sdf_agent_state *mcd_state;
   SDF_action_state_t *pas;
   int my_rank = msg_sdf_myrank();
   
   /*Get IF List*/
   vgrp = sdf_vip_config_get_vip_group(config, vip_group_id);
   if( vgrp == NULL ) {
       plat_log_msg(20281, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, "Invalid VIP group\n");
       return -1;
   }
   mcd_state = &(Mcd_agent_state);
   pas = mcd_state->ActionInitState.pcs;
   repstate = &(pas->qrep_state);
   node = &( repstate->node_state[vip_group_id]);

   for( i = 0; i < repstate->node_state[my_rank].num_vgrps_being_serviced; i++ ) {
       if( repstate->node_state[my_rank].serviced_vgrp_ids[i] == vip_group_id ) {
           break;
       }
   }
   if( i >= repstate->node_state[my_rank].num_vgrps_being_serviced ) {
       plat_log_msg(20282, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO, "Current Node not servicing vipgroup:%d\n",vip_group_id);
       return 0;
   }
   
#ifdef IPF_TEST_VIP
   if( repstate->node_state[my_rank].serviced_vgrp_ids[0] == vip_group_id) {
       repstate->node_state[my_rank].serviced_vgrp_ids[0] = repstate->node_state[my_rank].serviced_vgrp_ids[1];
   }
   repstate->node_state[my_rank].num_vgrps_being_serviced--;
#else // def IPF_TEST_VIP
   fthWaitEl_t * wait_list;
   wait_list = fthLock(&(repstate->node_state[my_rank].lock), 1, NULL);
   if( repstate->node_state[my_rank].serviced_vgrp_ids[0] == vip_group_id) {
       repstate->node_state[my_rank].serviced_vgrp_ids[0] = repstate->node_state[my_rank].serviced_vgrp_ids[1];
   }
   repstate->node_state[my_rank].num_vgrps_being_serviced--;
   fthUnlock(wait_list);

   wait_list = fthLock(&(node->lock), 1, NULL);
   /* Lock the node before updating the VIP */
   for( i = 0 ; i < node->num_vips; i++ ) {
       put_rule_table_id(node->vipgroup[i].rule_table_id);
   }
   fthUnlock(wait_list);
#endif // else def IPF_TEST_VIP

   /*Get IF List*/
#ifdef IPF_TEST_VIP
   for( i = 0 ; i < node->num_vips; i++ ) {
       sscanf(node->vipgroup[i].ip,"%d.%d.%d.%d",&ip[0],&ip[1],&ip[2],&ip[3]);
       sscanf(node->vipgroup[i].mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
       maskbits = bitarr[md[0] & 0xF] + bitarr[(md[0] >>4) & 0xF]+
                  bitarr[md[1] & 0xF] + bitarr[(md[1] >>4) & 0xF]+
                  bitarr[md[2] & 0xF] + bitarr[(md[2] >>4) & 0xF]+
                  bitarr[md[3] & 0xF] + bitarr[(md[3] >>4) & 0xF] ;
       sprintf(netaddr,"%d.%d.%d.%d", ip[0] & md[0], ip[1] & md[1],ip[2] & md[2],ip[3] & md[3]);
       /*Remove old rule tables*/
       sprintf(system_cmd,"%s rule del table %d 2>/dev/null 1>/dev/null",ip_command, node->vipgroup[i].rule_table_id);
       if( node->vipgroup[i].rule_table_id == 0 ) {
           /*Invalid configuratiob. This is BUG*/
           plat_log_msg(20283, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, "NULL ROUTE TABLE ID:%d\n",vip_group_id);
#ifdef IPF_TEST
           plat_abort();
#else
           plat_assert(node->vipgroup[i].rule_table_id != 0);
#endif
       }
       execute_system_cmd(system_cmd);
       sprintf(system_cmd,"%s route flush table %d 2>/dev/null 1>/dev/null",ip_command, node->vipgroup[i].rule_table_id);
       execute_system_cmd(system_cmd);
       /*Add IP address First*/
       sprintf(system_cmd,"%s addr del %s/%d dev %s 2>/dev/null 1>/dev/null",ip_command, node->vipgroup[i].ip,32,node->vipgroup[i].name);
       execute_system_cmd(system_cmd);
   }

   if( repstate->node_state[my_rank].is_virtual == 1 ) {
       int vport, rport, vnode, rnode;
       plat_log_msg(20284, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "VIPGROUP:REMOVAL My Node(%d) is virtual node\n",my_rank);
       vnode = my_rank;
       rnode = my_rank - 1;
       /* My Node is Virtual Node */
       if ( my_rank == vip_group_id ) {
           plat_log_msg(20285, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "This is a Weird case that should never happen: vnode:%d rnode:%d\n",vnode, rnode);
           for( i = 0; i < node->nctnrs_node; i++ ) {
               vport = repstate->node_state[vnode].cntrs[i].vip_tcp_port;
               rport = repstate->node_state[rnode].cntrs[i].vip_tcp_port;
               for( j = 0 ; j < node->num_vips; j++ ) {
                   sprintf(system_cmd,"%s -t nat -D OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip, rport, vport);
                   execute_system_cmd(system_cmd);
                   sprintf(system_cmd,"%s -t nat -D OUTPUT -p udp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip, rport, vport);
                   execute_system_cmd(system_cmd);
               }
           }
       }
       else {
           /*It is a recovery The recovering node got the VIP and it is real node*/
           plat_log_msg(20286, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "VIP Group is Real Node's group. vnode:%d rnode:%d\n",vnode, rnode);
           for( i = 0; i < node->nctnrs_node; i++ ) {
               vport = repstate->node_state[vnode].cntrs[i].vip_tcp_port;
               rport = repstate->node_state[rnode].cntrs[i].vip_tcp_port;
               for( j = 0 ; j < node->num_vips; j++ ) {
                   sprintf(system_cmd,"%s -t nat -D OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip,rport, vport);
                   execute_system_cmd(system_cmd);
                   sprintf(system_cmd,"%s -t nat -D OUTPUT -p udp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip,rport, vport);
                   execute_system_cmd(system_cmd);
               }
           }
       }
   }
   else {
       plat_log_msg(20287, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO,
                     "My Node:%d is not virtual node. Just DO Nothing on virtual node recovery\n",my_rank);
   } 
#endif // def IPF_TEST_VIP

   return 0;
}


/**
 * @brief configures VIP group to the local node
 * @return 0 on success, 1 on failure
 */
int configure_vip(const struct sdf_vip_config *config, int vip_group_id) {
#ifdef IPF_TEST_VIP
   int i, maskbits,j;
   unsigned char bitarr[16]={0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4};
   unsigned int md[4];
   unsigned int ip[4];
   char netaddr[32];
   char system_cmd[512];
#else
   int i;
#endif
   sdf_vip_group_t *vgrp;
   struct qrep_state *repstate;
   qrep_node_state_t *node;
   struct sdf_agent_state *mcd_state;
   SDF_action_state_t *pas;
   int my_rank = msg_sdf_myrank();
   int vip_rule_tbl_index = 0;

   /*Get IF List*/
   vgrp = sdf_vip_config_get_vip_group(config, vip_group_id);
   if( vgrp == NULL ) {
       plat_log_msg(20281, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, "Invalid VIP group\n");
       return -1;
   }
   mcd_state = &(Mcd_agent_state);
   pas = mcd_state->ActionInitState.pcs;
   repstate = &(pas->qrep_state);

   node = &( repstate->node_state[vip_group_id]);
   if( repstate->node_state[my_rank].num_vgrps_being_serviced >= MAX_VIP_GROUPS_PER_NODE ) {
       plat_log_msg(20288, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_FATAL, "The node already srvs:%d grps\n",
                                                repstate->node_state[my_rank].num_vgrps_being_serviced);
       return -1;
   }
#if 0
   if( repstate->node_state[my_rank].is_virtual == 1 ) {
       /* This means that we are running two instanced on same machine 
        * Use different rule table ID so that it will node collide with 
        * Other nodes rule tables */
      memcpy(viptbl_ids, vnode_viptbl_ids, (sizeof(int) * MAX_VIP_GROUPS_PER_NODE * NUM_RULE_TABLES));
   }
#endif // 0
#ifdef IPF_TEST_VIP
   vip_rule_tbl_index = repstate->node_state[my_rank].num_vgrps_being_serviced;
   repstate->node_state[my_rank].serviced_vgrp_ids[repstate->node_state[my_rank].num_vgrps_being_serviced] = vip_group_id;
   repstate->node_state[my_rank].num_vgrps_being_serviced++;
   for( i = 0 ; i < node->num_vips; i++ ) {
       node->vipgroup[i].rule_table_id = viptbl_ids[vip_rule_tbl_index][i];
   }
#else
   fthWaitEl_t * wait_list;
   wait_list = fthLock(&(repstate->node_state[my_rank].lock), 1, NULL);
   vip_rule_tbl_index = repstate->node_state[my_rank].num_vgrps_being_serviced;
   repstate->node_state[my_rank].serviced_vgrp_ids[repstate->node_state[my_rank].num_vgrps_being_serviced] = vip_group_id;
   repstate->node_state[my_rank].num_vgrps_being_serviced++;
   fthUnlock(wait_list);

   wait_list = fthLock(&(node->lock), 1, NULL);
   /* Lock the node before updating the VIP */
   for( i = 0 ; i < node->num_vips; i++ ) {
       node->vipgroup[i].rule_table_id = get_rule_table_id();
   }
   fthUnlock(wait_list);
#endif // else def IPF_TEST_VIP

#ifdef IPF_TEST_VIP
   /*Remove old rule tables*/
   for( j = 0; j < NUM_RULE_TABLES; j++ ) {
       sprintf(system_cmd,"%s rule del table %d 2>/dev/null 1>/dev/null",ip_command,viptbl_ids[vip_rule_tbl_index][j]);
       execute_system_cmd(system_cmd);
       sprintf(system_cmd,"%s route flush table %d 2>/dev/null 1>/dev/null",ip_command,viptbl_ids[vip_rule_tbl_index][j]);
       execute_system_cmd(system_cmd);
   }

   for( i = 0 ; i < node->num_vips; i++ ) {
       sscanf(node->vipgroup[i].ip,"%d.%d.%d.%d",&ip[0],&ip[1],&ip[2],&ip[3]);
       sscanf(node->vipgroup[i].mask,"%d.%d.%d.%d",&md[0],&md[1],&md[2],&md[3]);
       maskbits = bitarr[md[0] & 0xF] + bitarr[(md[0] >>4) & 0xF]+
                  bitarr[md[1] & 0xF] + bitarr[(md[1] >>4) & 0xF]+
                  bitarr[md[2] & 0xF] + bitarr[(md[2] >>4) & 0xF]+
                  bitarr[md[3] & 0xF] + bitarr[(md[3] >>4) & 0xF] ;
       sprintf(netaddr,"%d.%d.%d.%d", ip[0] & md[0], ip[1] & md[1],ip[2] & md[2],ip[3] & md[3]);
       node->vipgroup[i].rule_table_id = viptbl_ids[vip_rule_tbl_index][i];
       /*Add IP address First*/
       sprintf(system_cmd,"%s %s:%d %s netmask %s up 2>/dev/null 1>/dev/null",if_command, node->vipgroup[i].name,vip_group_id, node->vipgroup[i].ip,"255.255.255.255"); 
       /*
       sprintf(system_cmd,"%s addr add %s/%d dev %s 2>/dev/null 1>/dev/null",ip_command,node->vipgroup[i].ip,maskbits,node->vipgroup[i].name); */
       execute_system_cmd(system_cmd);
       /*Send Gratituos Arps*/
       sprintf(system_cmd,"%s -q -U -I %s %s -c3 2>/dev/null 1>/dev/null &",arping_command, node->vipgroup[i].name, node->vipgroup[i].ip);
       execute_system_cmd(system_cmd);
       /*Configure the routes*/
       /*Create Rule table*/
       sprintf(system_cmd,"%s rule add from %s table %d 2>/dev/null 1>/dev/null",ip_command,node->vipgroup[i].ip,viptbl_ids[vip_rule_tbl_index][i]);
       execute_system_cmd(system_cmd);
       sprintf(system_cmd,"%s route add %s/%d dev %s table %d src %s 2>/dev/null 1>/dev/null",ip_command,
                                     netaddr,maskbits,node->vipgroup[i].name,viptbl_ids[vip_rule_tbl_index][i],node->vipgroup[i].ip);
       execute_system_cmd(system_cmd);
       if( strcmp(node->vipgroup[i].gw,"") ) {
           sprintf(system_cmd,"%s route add default via %s dev %s table %d 2>/dev/null 1>/dev/null",ip_command,
                                     node->vipgroup[i].gw,node->vipgroup[i].name,viptbl_ids[vip_rule_tbl_index][i]);
           execute_system_cmd(system_cmd);
       }
   }

   if( repstate->groups[vgrp->group_group_id].type != SDF_CLUSTER_GRP_TYPE_MIRRORED ) {
       return 0;
   }
   if( repstate->node_state[my_rank].is_virtual == 1 ) {
       int vport, rport, vnode, rnode;
       plat_log_msg(20289, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO, 
                     "VIPGrp Config: My Node(%d) is virtual node\n",my_rank);
       vnode = my_rank;
       rnode = my_rank - 1; /* Second node in the 2way mirror is always virtual) */
       /* My Node is Virtual Node */
       if ( my_rank == vip_group_id ) {
           /* We just configured our own virtual group. We have to create port forwarding rules as below
              if fromVIP == our VIP, dest port = real port, then Map it to our Port */
           plat_log_msg(20290, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO, 
                     "VIP Group is My own Vip Group. Virtual Node startup vnode:%d rnode:%d\n",vnode, rnode);
           for( i = 0; i < repstate->node_state[my_rank].nctnrs_node; i++ ) {
               vport = repstate->node_state[vnode].cntrs[i].vip_tcp_port;
               rport = repstate->node_state[rnode].cntrs[i].vip_tcp_port;
               for( j = 0 ; j < node->num_vips; j++ ) {
                   sprintf(system_cmd,"%s -t nat -A OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip, rport, vport);
                   execute_system_cmd(system_cmd);
                   sprintf(system_cmd,"%s -t nat -A OUTPUT -p udp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip, rport, vport);
                   execute_system_cmd(system_cmd);

               }
           }
       }
       else {
           /*It is a fail over. Other node is Real Node*/
           plat_log_msg(20286, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO, 
                     "VIP Group is Real Node's group. vnode:%d rnode:%d\n",vnode, rnode);
           for( i = 0; i < node->nctnrs_node; i++ ) {
               vport = repstate->node_state[vnode].cntrs[i].vip_tcp_port;
               rport = repstate->node_state[rnode].cntrs[i].vip_tcp_port;
               for( j = 0 ; j < node->num_vips; j++ ) {
                   sprintf(system_cmd,"%s -t nat -A OUTPUT -p tcp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip,rport, vport);
                   execute_system_cmd(system_cmd);
                   sprintf(system_cmd,"%s -t nat -A OUTPUT -p udp -d %s --dport %d"
                                        " -j REDIRECT --to-ports %d 2>/dev/null 1>/dev/null",
                                       ip_tab_command,node->vipgroup[j].ip,rport, vport);
                   execute_system_cmd(system_cmd);
               }
           }
       }
   }
   else {
       plat_log_msg(20291, PLAT_LOG_CAT_SDF_PROT, PLAT_LOG_LEVEL_INFO, 
                     "My Node:%d is not virtual node. Just DO Nothing\n",my_rank);
   } 
#endif // def IPF_TEST_VIP
   return 0;
}
#endif

#ifndef IPF_TEST
/*
 * Process memcached command
 */
void process_vip_command(struct mcd_conn * c, mcd_request_t * req)
{
    SDF_action_init_t * pai;
    qrep_state_t * ps;

    // If needed for stub
    pai = (SDF_action_init_t *)c->pai;
    ps = &(pai->pcs->qrep_state);

    switch (req->cmd) {
    case MCD_CMD_ACTIVATE_VIP:
        // stub
        break;
    case MCD_CMD_DEACTIVATE_VIP:
        // stub
        break;
    default:
        plat_assert(0);
    }

}
#endif

int mcd_new_socket(struct addrinfo *ai) {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        plat_log_msg( 20757,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                      PLAT_LOG_LEVEL_ERROR, "socket(), errno=%d", errno );
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        plat_log_msg( 20744,
                      PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                      PLAT_LOG_LEVEL_ERROR,
                      "setting O_NONBLOCK, errno=%d", errno );
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}

#if 0
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size; 
    
    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0) { 
            plat_log_msg( 20166,
                          PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                          PLAT_LOG_LEVEL_ERROR,
                          "getsockopt(SO_SNDBUF), errno=%d", errno);
            perror("getsockopt(SO_SNDBUF)");
        }   
        return;
    }   
    
    /* Binary-search for the real maximum. */
    min = old_size;
    max = MCD_MAX_SENDBUF_SIZE;
    
    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }   
    }   
    
    plat_log_msg( 20758,
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                  PLAT_LOG_LEVEL_TRACE,
                  "<%d send buffer was %d, now %d", sfd, old_size, last_good );
}

static void maximize_rcvbuf( const int sfd )
{
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_RCVBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0) {
            plat_log_msg( 20759,
                          PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                          PLAT_LOG_LEVEL_ERROR,
                          "getsockopt(SO_RCVBUF), errno=%d", errno);
            perror("getsockopt(SO_RCVBUF)");
        }
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MCD_MAX_RECVBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if ( 0 ==
             setsockopt(sfd, SOL_SOCKET, SO_RCVBUF, (void *)&avg, intsize) ) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }
    plat_log_msg( 20760,
                  PLAT_LOG_CAT_SDF_APP_MEMCACHED,
                  PLAT_LOG_LEVEL_INFO,
                  "socket %d rcvbuf size was %d, now %d",
                  sfd, old_size, last_good );
}
      
int
mcd_vip_server_socket(struct vip *vip, Network_Port_Type port_type)
{
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints;
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags = 1;
    int udp;
    int port;

    /*
     * FIXME_BINARY: time to remove this function
     */
    plat_abort();

    udp = port_type == PORT_TYPE_UDP;
    port = udp ? vip->udp_port : vip->tcp_port;

    /*
     * the memset call clears nonstandard fields in some impementations
     * that otherwise mess things up.
     */
    memset(&hints, 0, sizeof (hints));
    hints.ai_flags = AI_PASSIVE|AI_ADDRCONFIG;

    if (udp) {
        hints.ai_protocol = IPPROTO_UDP;
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_family = AF_INET; /* This left here because of issues with OSX 10.5 */
    } else {
        hints.ai_family = AF_UNSPEC;
        hints.ai_protocol = IPPROTO_TCP;
        hints.ai_socktype = SOCK_STREAM;
    }

    snprintf(port_buf, NI_MAXSERV, "%d", port);

    error = getaddrinfo(vip->address, port_buf, &hints, &ai);

    if (error != 0) {
        if (error != EAI_SYSTEM) {
            plat_log_msg( 20764,
                          PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                          PLAT_LOG_LEVEL_ERROR,
                          "getaddrinfo(): %s", gai_strerror(error) );
        } else {
            plat_log_msg( 20765,
                          PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                          PLAT_LOG_LEVEL_ERROR,
                          "getaddrinfo(): error=%d", error );
            perror("getaddrinfo()");
        }

        return 1;
    }

    for (next = ai; next; next= next->ai_next) {
        mcd_conn *listen_conn_add;
        if ((sfd = mcd_new_socket(next)) == -1) {
            freeaddrinfo(ai);
            return 1;
        }

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (udp) {
            maximize_rcvbuf(sfd);
            maximize_sndbuf(sfd);
        } else {
            setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                plat_log_msg( 20766,
                              PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                              PLAT_LOG_LEVEL_ERROR,
                              "bind(): error=%d", errno );
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            close(sfd);
            continue;
        } else {
          success++;
          if (port_type != PORT_TYPE_UDP && listen(sfd, 1024) == -1) {
              plat_log_msg( 20767,
                            PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                            PLAT_LOG_LEVEL_ERROR,
                            "listen(): error=%d", errno );
              perror("listen()");
              close(sfd);
              freeaddrinfo(ai);
              return 1;
          }
      }

      if (udp) {
        int c;

        for (c = 0; c < settings.num_threads; c++) {
            /* this is guaranteed to hit all threads because we round-robin */
#ifdef  MCD_USE_FTHREAD
            mcd_fth_conn_new( sfd, conn_read, EV_READ | EV_PERSIST,
                              UDP_READ_BUFFER_SIZE, 1, main_base, NULL );
#else
            dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
                              UDP_READ_BUFFER_SIZE, 1);
#endif
        }
      } else {
        if (!(listen_conn_add = conn_new(sfd, conn_listening,
                                         EV_READ | EV_PERSIST, 1, false, main_base))) {
            plat_log_msg( 20768,
                          PLAT_LOG_CAT_SDF_APP_MEMCACHED_TRACE,
                          PLAT_LOG_LEVEL_FATAL,
                          "failed to create listening connection" );
            plat_exit(EXIT_FAILURE);
        }

        listen_conn_add->next = listen_conn;
        listen_conn = listen_conn_add;
      }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}
#endif
