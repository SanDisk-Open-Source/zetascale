#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <getopt.h>
#include <zs.h>

static char *logfile = NULL;

static struct ZS_state* zs_state;
static __thread struct ZS_thread_state *_zs_thd_state;
static int label,superblock,shard_prop,shard_desc,seg_list,class_desc,chkpoint,flog,pot,potbitmap,slabbitmap;
static int container=-255;
//extern int mcd_corrupt_pot();
//
#define CMC_SHARD_ID 1099511627777
#define VMC_SHARD_ID 2199023255553
#define VDC_SHARD_ID 3298534883329
#define FLUSH_LOG_PREFIX    "mbfl_"

static struct option long_options[] = { 
    {"label",        no_argument,  0, 'l'}, 
    {"superblock",   no_argument,  0, 's'}, 
    {"shard_prop",   no_argument,  0, 'p'}, 
    {"shard_desc",   no_argument,  0, 'd'}, 
    {"seg_list",     no_argument,  0, 'S'}, 
    {"class_desc",   no_argument,  0, 'c'}, 
    {"chkpoint",     no_argument,  0, 'C'}, 
    {"flog",         no_argument,  0, 'f'}, 
    {"pot",          no_argument,  0, 'P'}, 
    {"potbitmap",    no_argument,  0, 'b'}, 
    {"slabbitmap",    no_argument,  0, 'B'}, 
    {"help",         no_argument,  0, 'h'}, 
    {"container",    required_argument, 0, 'N'}, 
    {0, 0, 0, 0} 
};


void print_usage(char *pname) {
    printf("\n%s corrupts the specified region(s) in the ZetaScale storage by writing some random data\n\n",pname);
    printf("Usage:\n%s  --container=<container ID:0(cmc), 1(vmc), 2(vdc) or -1(all containers)> [--label] [--superblock]\n"
                     "              [--shard_prop] [--shard_desc] [--seg_list] [--class_desc] [--chkpoint] [--flog] [--pot] [--potbitmap] [--slabbitmap]\n",pname);
    printf("Options:\n");
    printf("--container  :Specifies the ID of the container to be corrupted. Container IDs: 0(cmc), 1(vmc), 2(vdc), -1(all containers)\n");
    printf("--label      :Corrupts the label region in the ZetaScale storage\n");
    printf("--superblock :Corrupts the superblock in the ZetaScale storage\n");
    printf("--shard_prop :Corrupts the shard properties of the specified container in the ZetaScale storage\n");
    printf("--shard_desc :Corrupts the shard descriptor of the specified container in the ZetaScale storage\n");
    printf("--seg_list   :Corrupts the segment mapping of the specified container in the ZetaScale storage\n");
    printf("--class_desc :Corrupts the class description of the specified container in the ZetaScale storage\n");
    printf("--chkpoint   :Corrupts the check point records of the specified container in the ZetaScale storage\n");
    printf("--flog       :Corrupts the flog of the specified container in the ZetaScale storage\n");
    printf("--pot        :Corrupts the persistent object table of the specified container in the ZetaScale storage\n");
    printf("--potbitmap  :Corrupts the persistent object table bitmap of the specified container in the ZetaScale storage (Storm only)\n");
    printf("--slabbitmap  :Corrupts the segment bitmap table of the specified container in the ZetaScale storage (Storm only)\n");
            
}
#define FLUSH_LOG_MAX_PATH 1024
int flog_corrupt(uint64_t shard_id) {
    int rc;
    char path[FLUSH_LOG_MAX_PATH];
    FILE *fp = NULL;
    char *log_flush_dir = (char *) ZSGetProperty("ZS_LOG_FLUSH_DIR", "/tmp");
    int zs_instance_id = atoi(ZSGetProperty("ZS_INSTANCE_ID", "0"));

    if (log_flush_dir == NULL)
        return 1;

    if(zs_instance_id){
        snprintf(path, sizeof(path), "%s/zs_%d/%s%lu",
        log_flush_dir, zs_instance_id, FLUSH_LOG_PREFIX, shard_id);
    }
    else{
        snprintf(path, sizeof(path), "%s/%s%lu",
        log_flush_dir, FLUSH_LOG_PREFIX, shard_id);
    }
    fprintf(stderr,"Corrupting flog file:%s\n",path);
    fp = fopen(path, "w+");
    if (!fp) {
        fprintf(stderr,"Unable to open flog:%s\n",path);
        return -1;
    }
    rc = fprintf(fp,"%1024s\n", "corrupting flog");
    fclose(fp);
    return rc;

}

int get_options(int argc, char *argv[])
{
    int option_index = 0;
    int c;

    label=0;
    superblock=0;
    shard_prop=0;
    shard_desc=0;
    seg_list=0;
    class_desc=0;
    chkpoint=0;
    class_desc=0;
    flog=0;
    pot=0;
    potbitmap=0;
    slabbitmap=0;

    while (1) { 
        c = getopt_long (argc, argv, "lspdScCfPbBhN:", long_options, &option_index); 

        if (c == -1) 
            break;
     
        switch (c) { 
        case 'l': 
            label=1;
            break;
        case 's': 
            superblock=1;
            break;
        case 'p': 
            shard_prop=1;
            break;
        case 'd': 
            shard_desc=1;
            break;
        case 'S': 
            seg_list=1;
            break;
        case 'c': 
            class_desc=1;
            break;
        case 'C': 
            chkpoint=1;
            break;
        case 'f': 
            flog=1;
            break;
        case 'P': 
            pot=1;
            break;
        case 'b': 
            potbitmap=1;
            break;
        case 'B': 
            slabbitmap=1;
            break;
        case 'N': 
            container = atoi(optarg); 
            break;

        case 'h': 
            print_usage(argv[0]);
            return -1;

        default:
            print_usage(argv[0]);
            return -1;
        }
    }
    return 0;
}

int init_zs() {
    ZS_status_t status = ZS_FAILURE;

    if ( ZS_SUCCESS != ( status = ZSInit( &zs_state ) ) ) {
        fprintf(stderr, "Failed to initialize ZS API!\n");
        return status;
    }

    if ( ZS_SUCCESS != ( status = ZSInitPerThreadState(zs_state, &_zs_thd_state ) ) ) {
        fprintf(stderr, "Failed to initialize ZS API!\n");
        return status;
    }

    return status;
}

int close_zs()
{
    ZS_status_t status = ZS_FAILURE;

    ZSCheckClose();

    if ( ZS_SUCCESS != ( status = ZSReleasePerThreadState(&_zs_thd_state ) ) ) {
        fprintf(stderr, "Failed to release ZS thread state!\n");
        return status;
    }

    if ( ZS_SUCCESS != (status = ZSShutdown( zs_state ) ) ) {
        fprintf(stderr, "Failed to shutdown ZS API!\n");
        return status;
    }

    return status;
}

ZS_status_t check_meta()
{
    return ZSCheckMeta();
}

ZS_status_t check_btree()
{
    return ZSCheck( _zs_thd_state, 0 );
}

ZS_status_t check_flog()
{
    return ZSCheckFlog( );
}

ZS_status_t check_pot()
{
    return ZSCheckPOT( );
}

int main(int argc, char *argv[])
{
    char *prop_file = NULL;

    if ( get_options( argc, argv) ) {
        return -1;
    }

    prop_file = getenv("ZS_PROPERTY_FILE");
    if (prop_file) {
        ZSLoadProperties(prop_file);
    }

    if( label == 1 ) {
        ZSSetProperty("ZS_FAULT_LABEL_CORRUPTION","1");
    }
    if( superblock == 1 ) {
        ZSSetProperty("ZS_FAULT_SBLOCK_CORRUPTION","1");
    }
    if( shard_prop == 1 ) {
        ZSSetProperty("ZS_FAULT_SHARD_PROP_CORRUPTION","1");
    }
    if( shard_desc == 1 ) {
        ZSSetProperty("ZS_FAULT_SHARD_DESC_CORRUPTION","1");
    }
    if( seg_list == 1 ) {
        ZSSetProperty("ZS_FAULT_SEGLIST_CORRUPTION","1");
    }
    if( class_desc == 1 ) {
        ZSSetProperty("ZS_FAULT_CLASS_DESC_CORRUPTION","1");
    }
    if( chkpoint == 1 ) {
        ZSSetProperty("ZS_FAULT_CHKPOINT_CORRUPTION","1");
    }
    if( flog == 1 ) {
        ZSSetProperty("ZS_FAULT_FLOG_CORRUPTION","1");
    }
    if( pot == 1 ) {
        ZSSetProperty("ZS_FAULT_POT_CORRUPTION","1");
    }
    if( potbitmap == 1 ) {
        ZSSetProperty("ZS_FAULT_POTBITMAP_CORRUPTION","1");
    }
    if( slabbitmap == 1 ) {
        ZSSetProperty("ZS_FAULT_SLABBITMAP_CORRUPTION","1");
    }
    if( container == 0 ) {
        ZSSetProperty("ZS_FAULT_CONTAINER_CMC","1");
    }
    if( container == 1 ) {
        ZSSetProperty("ZS_FAULT_CONTAINER_VMC","1");
    }
    if( container == 2 ) {
        ZSSetProperty("ZS_FAULT_CONTAINER_VDC","1");
    }
    if( container == -1 ) {
        ZSSetProperty("ZS_FAULT_CONTAINER_CMC","1");
        ZSSetProperty("ZS_FAULT_CONTAINER_VMC","1");
        ZSSetProperty("ZS_FAULT_CONTAINER_VDC","1");
    }

    if( container == -255 ) {
        fprintf(stderr,"\nUsage error: Container ID not specified\n");
        print_usage(argv[0]);
        exit(1);
    }

    if( flog == 1 ) {
        if( atoi(ZSGetProperty("ZS_FAULT_CONTAINER_CMC", "0")) == 1 ) {
            flog_corrupt(CMC_SHARD_ID);
        }
        if( atoi(ZSGetProperty("ZS_FAULT_CONTAINER_VMC", "0")) == 1 ) {
            flog_corrupt(VMC_SHARD_ID);
        }
        if( atoi(ZSGetProperty("ZS_FAULT_CONTAINER_VDC", "0")) == 1 ) {
            flog_corrupt(VDC_SHARD_ID);
        }
    }

    unsetenv("ZS_PROPERTY_FILE");
    ZSSetProperty("ZS_META_FAULT_INJECTION", "1");
    ZSSetProperty("ZS_REFORMAT", "0");

    if ((potbitmap == 1 || slabbitmap == 1) && !atoi(ZSGetProperty("ZS_STORM_MODE",  "1"))) {
        fprintf(stderr, "POT/SLAB bitmap check only allowed in Storm mode\n");
        print_usage(argv[0]);
        return -1;
    }

    ZSCheckInit(logfile);
    if (ZS_SUCCESS != check_meta()) {
        fprintf(stderr, "meta check failed, unable to continue\n");
        return -1;
    } else {
        fprintf(stderr, "meta corruption succeeded\n");
    }
if (0) {
    if( pot != 1 ) {
         exit(0);
    }
    if( (label == 1) || (superblock ==1 ) ||  (shard_prop == 1) || (shard_desc == 1) ||
        (seg_list == 1) || ( class_desc == 1 ) || ( class_desc == 1 ) || (chkpoint == 1 ) ||
        (flog == 1 ) || (potbitmap == 1) || (slabbitmap == 1) ) {
        fprintf(stderr, "POT corruption can not be done when one of the following option is enabled\n"
                        "label,superblock,shard_prop,shard_desc,seg_list,class_desc,chkpoint\n");
        exit(0);
    }
}

    return 0;
}
