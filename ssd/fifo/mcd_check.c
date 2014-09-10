/*
 * File:   mcd_check_meta.c
 * Author: Darryl Ouye
 *
 * Created on Sep 02, 2014
 *
 * SanDisk Proprietary Material, © Copyright 2012 SanDisk, all rights reserved.
 * http://www.sandisk.com
 *
 */

#include <aio.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <signal.h>
#include <sys/stat.h>

#include "platform/stdio.h"
#include "platform/unistd.h"
#include "utils/properties.h"
#include "utils/hash.h"
#include "fth/fthSem.h"
#include "ssd/ssd_local.h"
#include "ssd/fifo/fifo.h"
#include "ssd/ssd_aio.h"
#include "mcd_aio.h"
#include "mcd_osd.h"
#include "mcd_rec.h"
#include "mcd_rep.h"
#include "mcd_bak.h"
#include "hash.h"
#include "fdf_internal.h"
#include "container_meta_blob.h"
#include "utils/checklog.h"
#include "protocol/action/recovery.h"
#include "ssd/fifo/slab_gc.h"

#define SUPER_BUF 0
#define CMC_PROP_BUF 1
#define VMC_PROP_BUF 2
#define VDC_PROP_BUF 3
#define CMC_DESC_BUF 4
#define VMC_DESC_BUF 5
#define VDC_DESC_BUF 6
#define NUM_META_BLOCKS 7

#define CMC_SHARD_ID 1099511627777
#define VMC_SHARD_ID 2199023255553
#define VDC_SHARD_ID 3298534883329

#define CMC_SHARD_IDX 0
#define VMC_SHARD_IDX 1
#define VDC_SHARD_IDX 2

#define SHARD_DESC_BLK_SIZE   ( MCD_OSD_SEG0_BLK_SIZE * 16 )

// global
char                  buf[NUM_META_BLOCKS][SHARD_DESC_BLK_SIZE];
int                   cmc_properties_ok = 0;
int                   vmc_properties_ok = 0;
int                   vdc_properties_ok = 0;

extern int __zs_check_mode_on;

int
mcd_check_label(int fd)
{
    int                         bytes;
    int                         label_num;
    char                        label[MCD_OSD_SEG0_BLK_SIZE];

    // read label
    bytes = pread( fd, label, MCD_OSD_SEG0_BLK_SIZE, 0);
    if ( MCD_OSD_SEG0_BLK_SIZE != bytes ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_READ_ERROR, "failed to read flash label");
        return -1;
    }

    // validate label
    label_num = atoi( label + strlen( "Schooner" ) );
    if ( 0 != memcmp( label, "Schooner", strlen( "Schooner" ) ) ||
         Mcd_aio_num_files < label_num ||
         0 > label_num ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR, "flash label invalid");
        return -1;
    } 

    zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_SUCCESS, "flash label valid");

    return 0;
}

int
mcd_check_superblock(int fd)
{
    int                 bytes = -1;
    uint64_t            checksum = 0;
    mcd_rec_flash_t    *superblock = NULL;

    // read superblock 
    bytes = pread( fd, buf[SUPER_BUF], MCD_OSD_SEG0_BLK_SIZE, MCD_REC_LABEL_BLKS * MCD_OSD_SEG0_BLK_SIZE);
    if ( MCD_OSD_SEG0_BLK_SIZE != bytes ) {
        zscheck_log_msg(ZSCHECK_SUPERBLOCK, 0, ZSCHECK_READ_ERROR, "failed to read superblock");
        return -1;
    }   

    superblock = (mcd_rec_flash_t *)buf[SUPER_BUF];

    // validate superblock magic number
    if ( superblock->eye_catcher != MCD_REC_FLASH_EYE_CATCHER ) {
        zscheck_log_msg(ZSCHECK_SUPERBLOCK, 0, ZSCHECK_MAGIC_ERROR, "superblock magic number invald");
        return -1;
    }
    zscheck_log_msg(ZSCHECK_SUPERBLOCK, 0, ZSCHECK_SUCCESS, "superblock magic number valid");

    // validate superblock checksum
    checksum = superblock->checksum;
    superblock->checksum = 0;
    superblock->checksum = hashb((unsigned char *)superblock,
                                 MCD_OSD_SEG0_BLK_SIZE,
                                 MCD_REC_FLASH_EYE_CATCHER);
    if ( superblock->checksum != checksum ) {
        zscheck_log_msg(ZSCHECK_SUPERBLOCK, 0, ZSCHECK_CHECKSUM_ERROR, "superblock checksum invalid");
        return -1;
    }

    zscheck_log_msg(ZSCHECK_SUPERBLOCK, 0, ZSCHECK_SUCCESS, "superblock checksum valid");
    return 0;
}

int
mcd_check_shard_properties(int fd, int shard_idx)
{   
    int                   status = -1;
    int                   bytes = -1;
    int                   buf_idx = -1;
    uint64_t              shard_id = 0;
    uint64_t              checksum = 0;
    mcd_rec_properties_t *properties = NULL;
    
    switch ( shard_idx ) {

    case CMC_SHARD_IDX:
        buf_idx = CMC_PROP_BUF;
        shard_id = CMC_SHARD_ID;
        break;

    case VMC_SHARD_IDX:
        buf_idx = VMC_PROP_BUF;
        shard_id = VMC_SHARD_ID;
        break;

    case VDC_SHARD_IDX:
        buf_idx = VDC_PROP_BUF;
        shard_id = VDC_SHARD_ID;
        break;
    }

    // read shard properties
    bytes = pread( fd, buf[buf_idx], MCD_OSD_SEG0_BLK_SIZE, ((shard_idx + 2 + MCD_REC_LABEL_BLKS) * MCD_OSD_SEG0_BLK_SIZE));

    if ( MCD_OSD_SEG0_BLK_SIZE == bytes ) {

        properties = (mcd_rec_properties_t *)buf[buf_idx]; 
        
        // check the shard properties magic number
        if ( properties->eye_catcher != MCD_REC_PROP_EYE_CATCHER ) {
            zscheck_log_msg(ZSCHECK_SHARD_PROPERTIES, shard_id, ZSCHECK_MAGIC_ERROR, "shard properties magic number invalid");
        } else {
            zscheck_log_msg(ZSCHECK_SHARD_PROPERTIES, shard_id, ZSCHECK_SUCCESS, "shard properties magic number valid");
            
            // check the shard properties checksum
            checksum = properties->checksum;
            properties->checksum = 0;    
            properties->checksum = hashb((unsigned char *)properties,
                                         MCD_OSD_SEG0_BLK_SIZE,
                                         MCD_REC_PROP_EYE_CATCHER);
                
            if ( properties->checksum != checksum ) {
                zscheck_log_msg(ZSCHECK_SHARD_PROPERTIES, shard_id, ZSCHECK_CHECKSUM_ERROR, "shard properties checksum invalid");
            } else {
                zscheck_log_msg(ZSCHECK_SHARD_PROPERTIES, shard_id, ZSCHECK_SUCCESS, "shard properties checksum valid");
                status = 0;
            }
        }
    } else {
        zscheck_log_msg(ZSCHECK_SHARD_PROPERTIES, shard_id, ZSCHECK_READ_ERROR, "failed to read shard properties");
    }

    return status;
}

int
mcd_check_shard_descriptor(int fd, int shard_idx)
{
    int                   status = -1;
    ssize_t               bytes = 0;
    int                   prop_buf_idx = -1;
    int                   buf_idx = -1;
    uint64_t              shard_id = 0;
    uint64_t              checksum = 0;
    mcd_rec_properties_t *properties = NULL;
    mcd_rec_shard_t      *shard = NULL;

    switch ( shard_idx ) {

    case CMC_SHARD_IDX:
        prop_buf_idx = CMC_PROP_BUF;
        buf_idx = CMC_DESC_BUF;
        shard_id = CMC_SHARD_ID;
        break;

    case VMC_SHARD_IDX:
        prop_buf_idx = VMC_PROP_BUF;
        buf_idx = VMC_DESC_BUF;
        shard_id = VMC_SHARD_ID;
        break;

    case VDC_SHARD_IDX:
        prop_buf_idx = VDC_PROP_BUF;
        buf_idx = VDC_DESC_BUF;
        shard_id = VDC_SHARD_ID;
        break;
    }

    // shard properties point to shard descriptor block offset
    properties = (mcd_rec_properties_t *)buf[prop_buf_idx];

    // read descriptor
    bytes = pread( fd, buf[buf_idx], SHARD_DESC_BLK_SIZE, (properties->blk_offset * SHARD_DESC_BLK_SIZE));

    if ( SHARD_DESC_BLK_SIZE == bytes ) {

        shard = (mcd_rec_shard_t *)buf[buf_idx];

        // check the shard descriptor magic number
        if ( shard->eye_catcher != MCD_REC_SHARD_EYE_CATCHER ) {
            zscheck_log_msg(ZSCHECK_SHARD_DESCRIPTOR, shard_id, ZSCHECK_MAGIC_ERROR, "shard descriptor magic number invalid");
        } else {
            zscheck_log_msg(ZSCHECK_SHARD_DESCRIPTOR, shard_id, ZSCHECK_SUCCESS, "shard descriptor magic number valid");

            // check the shard descriptor checksum
            checksum = shard->checksum;
            shard->checksum = 0;
            shard->checksum = hashb((unsigned char *)shard,
                                    SHARD_DESC_BLK_SIZE,
                                    MCD_REC_SHARD_EYE_CATCHER);

            if ( shard->checksum != checksum ) {
                zscheck_log_msg(ZSCHECK_SHARD_DESCRIPTOR, shard_id, ZSCHECK_CHECKSUM_ERROR, "shard descriptor checksum invalid");
            } else {
                zscheck_log_msg(ZSCHECK_SHARD_DESCRIPTOR, shard_id, ZSCHECK_SUCCESS, "shard descriptor checksum valid");
                status = 0;
            }
        }
    } else {
        zscheck_log_msg(ZSCHECK_SHARD_DESCRIPTOR, shard_id, ZSCHECK_READ_ERROR, "failed to read shard descriptor");
    }

    return status;
}

int
mcd_check_all_shard_properties(int fd)
{
    // validate cmc shard properties
    if ( 0 == mcd_check_shard_properties(fd, CMC_SHARD_IDX) ) {
        cmc_properties_ok = 1;
    }

    // validate vmc shard properties
    if ( 0 == mcd_check_shard_properties(fd, VMC_SHARD_IDX) ) {
        vmc_properties_ok = 1;
    }

    // validate vdc shard properties
    if ( 0 == mcd_check_shard_properties(fd, VDC_SHARD_IDX) ) {
        vdc_properties_ok = 1;
    }

    if ( (cmc_properties_ok + vmc_properties_ok + vdc_properties_ok) < 3 )
        return -1;
    else
        return 0;
}

int
mcd_check_all_shard_descriptors(int fd)
{
    int count = 0;

    // validate cmc shard properties
    if ( cmc_properties_ok && 0 == mcd_check_shard_descriptor(fd, CMC_SHARD_IDX) ) {
        ++count;
    }   
    
    // validate vmc shard properties
    if ( vmc_properties_ok && 0 == mcd_check_shard_descriptor(fd, VMC_SHARD_IDX) ) {
        ++count;;
    }
    
    // validate vdc shard properties
    if ( vdc_properties_ok && 0 == mcd_check_shard_descriptor(fd, VDC_SHARD_IDX) ) {
        ++count;;
    }   
        
    if ( count < 3 )
        return -1;
    else    
        return 0;
}           

int
mcd_check_segment_list(int fd, mcd_rec_shard_t *shard)
{   
    int status = -1;
    int i = 0;
    ssize_t bytes = 0;
    uint64_t checksum = 0;
    char *buf = NULL;
    mcd_rec_list_block_t *seg_list = NULL;
    char msg[1024];

    // allocate a buffer
    buf = (char *) malloc( shard->map_blks * SHARD_DESC_BLK_SIZE );

    // read the segment table for this shard
    bytes = pread( fd,
                   buf,
                   ( shard->map_blks * SHARD_DESC_BLK_SIZE ),
                   ( shard->blk_offset + shard->seg_list_offset) * SHARD_DESC_BLK_SIZE );
        
    if ( ( shard->map_blks * SHARD_DESC_BLK_SIZE ) == bytes ) {

        for ( i = 0; i < shard->map_blks; i++ ) {
    
            seg_list = (mcd_rec_list_block_t *)(buf + (i * SHARD_DESC_BLK_SIZE));
    
            // verify class segment block checksum
            checksum = seg_list->checksum;
            seg_list->checksum = 0; 
            seg_list->checksum = hashb((unsigned char *)seg_list, SHARD_DESC_BLK_SIZE, 0);
    
            if ( seg_list->checksum != checksum ) {
                sprintf( msg, "segment list %d checksum invalid", i );
                zscheck_log_msg( ZSCHECK_SEGMENT_LIST, shard->shard_id, ZSCHECK_CHECKSUM_ERROR, msg );
            } else {
                sprintf( msg, "segment list %d checksum valid", i );
                zscheck_log_msg( ZSCHECK_SHARD_DESCRIPTOR, shard->shard_id, ZSCHECK_SUCCESS, msg );
                status = 0;
            }
        }
    } else {
        zscheck_log_msg( ZSCHECK_SEGMENT_LIST, shard->shard_id, ZSCHECK_READ_ERROR, "failed to read segment list" );
    }

    free(buf);

    return status;
}

int
mcd_check_all_segment_lists(int fd)
{   
    int count = 0; 
    mcd_rec_shard_t *shard = NULL;
                   
    // validate cmc shard segment lists
    shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];

    if ( cmc_properties_ok && 0 == mcd_check_segment_list(fd, shard) ) {
        ++count;
    }    

    // validate vmc shard properties
    shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];

    if ( vmc_properties_ok && 0 == mcd_check_segment_list(fd, shard) ) {
        ++count;;
    }
            
    // validate vdc shard properties
    shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];

    if ( vdc_properties_ok && 0 == mcd_check_segment_list(fd, shard) ) {
        ++count;;
    }
            
    if ( count < 3 )
        return -1; 
    else        
        return 0;
}           

int
mcd_check_class_descriptor(int fd, mcd_rec_shard_t *shard)
{
    int c = 0;
    int status = -1;
    uint64_t checksum = 0;    
    ssize_t bytes = 0;
    uint32_t blk_size = 0;
    mcd_rec_class_t *pclass = NULL;
    char desc[SHARD_DESC_BLK_SIZE];
    char msg[1024];
    mcd_rec_flash_t *superblock = NULL;

    superblock = (mcd_rec_flash_t *)buf[SUPER_BUF];
    blk_size = superblock->blk_size;

    for ( c = 0; c < MCD_OSD_MAX_NCLASSES; c++ ) {

        // read class desc + segment table for each class
        bytes = pread( fd, 
                       desc,
                       blk_size,
                       (shard->blk_offset + shard->class_offset[ c ] ) * blk_size );

        if ( blk_size == bytes ) {

            pclass = (mcd_rec_class_t *)desc;

            // check the class list magic number
            if ( pclass->eye_catcher != MCD_REC_CLASS_EYE_CATCHER ) {
                sprintf( msg, "class descriptor %d magic number invalid", c);
                zscheck_log_msg( ZSCHECK_CLASS_DESCRIPTOR, shard->shard_id, ZSCHECK_MAGIC_ERROR, msg );
            } else {
                sprintf( msg, "class descriptor %d magic number valid", c);
                zscheck_log_msg( ZSCHECK_CLASS_DESCRIPTOR, shard->shard_id, ZSCHECK_SUCCESS, msg );

                // verify class descriptor checksum
                checksum  = pclass->checksum;
                pclass->checksum = 0;
                pclass->checksum = hashb((unsigned char *)pclass,
                                         blk_size,
                                         MCD_REC_CLASS_EYE_CATCHER);

                if ( pclass->checksum != checksum ) {
                    sprintf( msg, "class descriptor %d checksum invalid", c );
                    zscheck_log_msg( ZSCHECK_CLASS_DESCRIPTOR, shard->shard_id, ZSCHECK_CHECKSUM_ERROR, msg );
                } else {
                    sprintf( msg, "class descriptor %d checksum valid", c );
                    zscheck_log_msg( ZSCHECK_CLASS_DESCRIPTOR, shard->shard_id, ZSCHECK_SUCCESS, msg );
                    status = 0;
                }
            }
        } else {
            sprintf( msg, "failed to read class descriptor %d", c );
            zscheck_log_msg( ZSCHECK_CLASS_DESCRIPTOR, shard->shard_id, ZSCHECK_READ_ERROR, msg );
        }
    }

    return status;
}

int
mcd_check_all_class_descriptors(int fd)
{
    int count = 0;
    mcd_rec_shard_t *shard = NULL;

    // validate cmc shard class descriptor
    shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];

    if ( cmc_properties_ok && 0 == mcd_check_class_descriptor(fd, shard) ) {
        ++count;
    }

    // validate vmc shard class descriptor
    shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];

    if ( vmc_properties_ok && 0 == mcd_check_class_descriptor(fd, shard) ) {
        ++count;;
    }

    // validate vdc shard class descriptor
    shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];

    if ( vdc_properties_ok && 0 == mcd_check_class_descriptor(fd, shard) ) {
        ++count;;
    }

    if ( count < 3 )
        return -1;
    else
        return 0;
}

int
mcd_check_ckpt_descriptor(int fd, mcd_rec_shard_t *shard)
{
    int                   status = -1;
    ssize_t               bytes = 0;
    uint64_t              checksum = 0;
    uint64_t              shard_id = 0;
    uint32_t              blk_size = 0;
    mcd_rec_ckpt_t       *ckpt = NULL;
    mcd_rec_flash_t      *superblock = NULL;
    char                  ckpt_buf[SHARD_DESC_BLK_SIZE];

    shard_id = shard->shard_id;

    superblock = (mcd_rec_flash_t *)buf[SUPER_BUF];
    blk_size = superblock->blk_size;

    // read descriptor
    bytes = pread( fd, ckpt_buf, blk_size, (shard->blk_offset + shard->rec_md_blks - 1) * blk_size);

    if ( blk_size == bytes ) {

        ckpt = (mcd_rec_ckpt_t *)ckpt_buf;

        // check the ckpt descriptor magic number
        if ( ckpt->eye_catcher != MCD_REC_CKPT_EYE_CATCHER ) {
            zscheck_log_msg(ZSCHECK_CKPT_DESCRIPTOR, shard_id, ZSCHECK_MAGIC_ERROR, "ckpt descriptor magic number invalid");
        } else {
            zscheck_log_msg(ZSCHECK_CKPT_DESCRIPTOR, shard_id, ZSCHECK_SUCCESS, "ckpt descriptor magic number valid");

            // check the shard descriptor checksum
            checksum = ckpt->checksum;
            ckpt->checksum = 0;
            ckpt->checksum = hashb((unsigned char *)ckpt,
                                   blk_size,
                                   MCD_REC_CKPT_EYE_CATCHER);

            if ( ckpt->checksum != checksum ) {
                zscheck_log_msg(ZSCHECK_CKPT_DESCRIPTOR, shard_id, ZSCHECK_CHECKSUM_ERROR, "ckpt descriptor checksum invalid");
            } else {
                zscheck_log_msg(ZSCHECK_CKPT_DESCRIPTOR, shard_id, ZSCHECK_SUCCESS, "ckpt descriptor checksum valid");
                status = 0;
            }
        }
    } else {
        zscheck_log_msg(ZSCHECK_CKPT_DESCRIPTOR, shard_id, ZSCHECK_READ_ERROR, "failed to read ckpt descriptor");
    }

    return status;
}

int
mcd_check_all_ckpt_descriptors(int fd)
{
    int count = 0;
    mcd_rec_shard_t *shard = NULL;

    // validate cmc shard ckpt descriptor
    shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];

    if ( cmc_properties_ok && 0 == mcd_check_ckpt_descriptor(fd, shard) ) {
        ++count;
    }

    // validate vmc shard ckpt descriptor
    shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];

    if ( vmc_properties_ok && 0 == mcd_check_ckpt_descriptor(fd, shard) ) {
        ++count;;
    }

    // validate vdc shard ckpt descriptor
    shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];

    if ( vdc_properties_ok && 0 == mcd_check_ckpt_descriptor(fd, shard) ) {
        ++count;;
    }

    if ( count < 3 )
        return -1;
    else
        return 0;
}

int
mcd_check_meta()
{
    int fd = -1;
    int open_flags = O_RDWR;
    int status = -1;
    char fname[PATH_MAX + 1];

    // Open the flash device
    strcpy(fname, getProperty_String("ZS_FLASH_FILENAME", "/tmp/schooner0"));

    fd = open( fname, open_flags, 00600 );
    if (fd < 0) {
        perror(fname);
        return -1;
    }

    // Check the flash label - non-fatal if corrupt
    status = mcd_check_label(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_label failed. non-fatal error\n");
    }
 
    // Check the superblock - fatal if corrupt
    status = mcd_check_superblock(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_superblock failed. fatal error\n");
        goto out;
    }

    status = mcd_check_all_shard_properties(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_shard_properties failed. non-fatal error\n");
    }

    status = mcd_check_all_shard_descriptors(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_shard_descriptors failed. non-fatal error\n");
    }

    status = mcd_check_all_segment_lists(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_segment_lists failed. non-fatal error\n");
    }

    status = mcd_check_all_class_descriptors(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_class_descriptors failed. non-fatal error\n");
    }

    status = mcd_check_all_ckpt_descriptors(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_ckpt_descriptors failed. non-fatal error\n");
    }

#if 0
    mcd_check_log_page( NULL, NULL, 1, 0);
#endif

out:
    close(fd);
    return status;
}

extern mcd_osd_shard_t * Mcd_osd_slab_shards[];

static mcd_osd_shard_t *
mcd_check_get_osd_shard( uint64_t shard_id )
{
    int i = 0;
    mcd_osd_shard_t * osd_shard = NULL;

    for ( i = 0; i < MCD_OSD_MAX_NUM_SHARDS; i++ ) { 
        if ( NULL == Mcd_osd_slab_shards[i] ) { 
            continue; 
        } 

        if ( Mcd_osd_slab_shards[i]->mos_shard.shardID == shard_id ) { 
            osd_shard = Mcd_osd_slab_shards[i]; 
            plat_assert_always( 1 == osd_shard->opened );
            break;
        }
    } 

    return osd_shard;
}

int
mcd_check_flog()
{
    int status = -1;
    int errors = 0;
    osd_state_t * context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_RCVR );
    mcd_rec_shard_t * shard = NULL;
    mcd_osd_shard_t * osd_shard = NULL;

    // check cmc 
    shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = flog_check( osd_shard, context );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_flog failed for cmc.\n");
        ++errors;
    }

    // check vmc 
    shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = flog_check( osd_shard, context );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_flog failed for vmc.\n");
        ++errors;
    }

    // check vdc 
    shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = flog_check( osd_shard, context );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_flog failed for vdc.\n");
        ++errors;
    }

    return ( errors == 0 ) ? 0 : -1;
}

int
mcd_check_pot()
{
    int status = -1;
    int errors = 0;
    osd_state_t * context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_RCVR );
    mcd_rec_shard_t * shard = NULL;
    mcd_osd_shard_t * osd_shard = NULL;

    // check cmc
    shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = check_object_table( context, osd_shard );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_pot failed for cmc.\n");
        ++errors;
    }

    // check vmc
    shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = check_object_table( context, osd_shard );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_pot failed for vmc.\n");
        ++errors;
    }

    // check vdc
    shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];
    osd_shard = mcd_check_get_osd_shard( shard->shard_id );
    status = check_object_table( context, osd_shard );
    if ( status ) {
        fprintf(stderr,">>>mcd_check_pot failed for vdc.\n");
        ++errors;
    }

    return ( errors == 0 ) ? 0 : -1;
}

// Is check mode turned on?
// 0 - disabled
// 1 - enabled
int 
mcd_check_is_enabled()
{
    return __zs_check_mode_on;
}
