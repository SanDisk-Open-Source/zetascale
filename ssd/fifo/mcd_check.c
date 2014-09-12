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
#include "mcd_check.h"
#include "hash.h"
#include "fdf_internal.h"
#include "container_meta_blob.h"
#include "utils/checklog.h"
#include "protocol/action/recovery.h"
#include "ssd/fifo/slab_gc.h"
#include "malloc.h"

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
#define dprintf fprintf

#include "utils/rico.h"
#include "ssd/fifo/mcd_rec2.h"
#define	bytes_per_device_block		(1uL << 13)
bool match_potbm_checksum( potbm_t *potbm, uint n);
bool match_slabbm_checksum( slabbm_t *, uint);
bool empty( void *, uint);
ulong potbm_base( mcd_rec_shard_t *);
ulong slabbm_base( mcd_rec_shard_t *p);

// global
char                  buf[NUM_META_BLOCKS][SHARD_DESC_BLK_SIZE];
int                   cmc_properties_ok = 0;
int                   vmc_properties_ok = 0;
int                   vdc_properties_ok = 0;
char                  tmp_buf[SHARD_DESC_BLK_SIZE];

extern int __zs_check_mode_on;

mcd_osd_shard_t * mcd_check_get_osd_shard( uint64_t shard_id );
int mcd_corrupt_meta();
int mcd_corrupt_pot();

int
mcd_check_label(int fd)
{
    int                         bytes;
    int                         label_num;
    char                        label[MCD_OSD_SEG0_BLK_SIZE];

    // read label
    dprintf(stderr,"%s Reading label at offset:%d\n",__FUNCTION__,0);
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
    dprintf(stderr,"%s Reading at offset:%d\n",__FUNCTION__,MCD_REC_LABEL_BLKS * MCD_OSD_SEG0_BLK_SIZE);
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
    dprintf(stderr,"%s Reading at offset:%lu\n",__FUNCTION__,(uint64_t)((shard_idx + 2 + MCD_REC_LABEL_BLKS) * MCD_OSD_SEG0_BLK_SIZE));
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
    dprintf(stderr,"%s Reading at offset:%lu\n",__FUNCTION__,(properties->blk_offset * SHARD_DESC_BLK_SIZE));
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
mcd_check_potbm(int fd, int buf_idx, uint64_t shard_id)
{
    int                   status = 0;
    ssize_t               bytes;

	mcd_rec_shard_t *p = (mcd_rec_shard_t *)buf[buf_idx];

	size_t n = p->rec_potbm_blks * bytes_per_device_block;
	potbm_t* potbm = (potbm_t*)memalign( MCD_OSD_META_BLK_SIZE, n);
	if(!potbm)
		return -1;

	bytes = pread(fd, potbm, n, potbm_base(p));

	if (bytes !=n || ((potbm->eye_catcher || empty(potbm, n)) &&
			match_potbm_checksum( potbm, n) &&
			potbm->eye_catcher != MCD_REC_POTBM_EYE_CATCHER)) {
		zscheck_log_msg(ZSCHECK_POT_BITMAP, shard_id, ZSCHECK_CHECKSUM_ERROR, "POT bitmap checksum invalid");
		status = -1;
	}

	free(potbm);

    return status;
}

int
mcd_check_slabbm(int fd, int buf_idx, uint64_t shard_id)
{
    int                   status = 0;
    ssize_t               bytes;

	mcd_rec_shard_t *p = (mcd_rec_shard_t *)buf[buf_idx];

	size_t n = p->rec_slabbm_blks * bytes_per_device_block;
	slabbm_t* slabbm = (slabbm_t*)memalign( MCD_OSD_META_BLK_SIZE, n);
	if(!slabbm)
		return -1;

	bytes = pread(fd, slabbm, n, slabbm_base(p));

	if (bytes != n || ((slabbm->eye_catcher || empty(slabbm, n)) &&
			match_slabbm_checksum( slabbm, n) &&
			slabbm->eye_catcher != MCD_REC_SLABBM_EYE_CATCHER)) {
		zscheck_log_msg(ZSCHECK_SLAB_BITMAP, shard_id, ZSCHECK_CHECKSUM_ERROR, "SLAB bitmap checksum invalid");
		status = -1;
	}

	free(slabbm);

    return status;
}

int
mcd_check_all_potbm(int fd)
{
    int count = 0;

    if ( cmc_properties_ok && !mcd_check_potbm(fd, CMC_DESC_BUF, CMC_SHARD_ID) )
        ++count;

    if ( vmc_properties_ok && !mcd_check_potbm(fd, VMC_DESC_BUF, VMC_SHARD_ID) )
        ++count;

    if ( vdc_properties_ok && !mcd_check_potbm(fd, VDC_DESC_BUF, VDC_SHARD_ID) )
        ++count;

    return count < 3 ? -1 : 0;
}

int
mcd_check_all_slabbm(int fd)
{
    int count = 0;

    if ( cmc_properties_ok && !mcd_check_slabbm(fd, CMC_DESC_BUF, CMC_SHARD_ID) )
        ++count;

    if ( vmc_properties_ok && !mcd_check_slabbm(fd, VMC_DESC_BUF, VMC_SHARD_ID) )
        ++count;

    if ( vdc_properties_ok && !mcd_check_slabbm(fd, VDC_DESC_BUF, VDC_SHARD_ID) )
        ++count;

    return count < 3 ? -1 : 0;
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
    dprintf(stderr,"%s Reading at offset:%lu\n",__FUNCTION__,( shard->blk_offset + shard->seg_list_offset) * SHARD_DESC_BLK_SIZE);
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
        dprintf(stderr,"%s Reading at offset:%lu\n",__FUNCTION__,(shard->blk_offset + shard->class_offset[ c ] ) * blk_size );
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
    dprintf(stderr,"%s Reading at offset:%lu\n",__FUNCTION__,(shard->blk_offset + shard->rec_md_blks - 1) * blk_size);
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
        fprintf(stderr,">>>mcd_check_shard_properties failed. fatal error\n");
        goto out;
    }

    status = mcd_check_all_shard_descriptors(fd);
    if ( 0 != status ) {
        fprintf(stderr,">>>mcd_check_shard_descriptors failed. fatal error\n");
        goto out;
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

    fprintf(stderr,"potbm check: ");
    status = mcd_check_all_potbm(fd);
    fprintf(stderr,"%s\n", status ? "failed" : "succeeded");

    fprintf(stderr,"slabbm check: ");
    status = mcd_check_all_slabbm(fd);
    fprintf(stderr,"%s\n", status ? "failed" : "succeeded");

out:
    close(fd);
    if( atoi(getProperty_String("ZS_META_FAULT_INJECTION", "0"))!=0) {
        return mcd_corrupt_meta();        
    }
    return status;
}

extern mcd_osd_shard_t * Mcd_osd_slab_shards[];

mcd_osd_shard_t *
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

    if( atoi(getProperty_String("ZS_META_FAULT_INJECTION", "0"))!=0) {
        return mcd_corrupt_pot();
    }

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
// 1 - enabled, no init zs
// 2 - enabled, init zs
int 
mcd_check_level()
{
    return __zs_check_mode_on;
}

int
mcd_corrupt_pot()
{
    int status = -1;
    int errors = 0;
    osd_state_t * context = mcd_fth_init_aio_ctxt( SSD_AIO_CTXT_MCD_REC_RCVR );
    mcd_rec_shard_t * shard = NULL;
    mcd_osd_shard_t * osd_shard = NULL;

    if( atoi(getProperty_String("ZS_FAULT_POT_CORRUPTION", "0")) == 0) {
        return 0;
    }

    if( atoi(getProperty_String("ZS_FAULT_CONTAINER_CMC", "0")) == 1) {
        // check cmc
        shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];
        osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        status = mcd_corrupt_object_table( context, osd_shard );
        if ( status ) {
            fprintf(stderr,"Corrupt object table for cmc failed\n");
            ++errors;
        }
    }

    // check vmc
    if( atoi(getProperty_String("ZS_FAULT_CONTAINER_VMC", "0")) == 1) {
        shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];
        osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        status = mcd_corrupt_object_table( context, osd_shard );
        if ( status ) {
            fprintf(stderr,"Corrupt object table for vmc failed\n");
            ++errors;
        }
    }

    // check vdc
    if( atoi(getProperty_String("ZS_FAULT_CONTAINER_VDC", "0")) == 1) {
        shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];
        osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        status = mcd_corrupt_object_table( context, osd_shard );
        if ( status ) {
            fprintf(stderr,"Corrupt object table for vdc failed\n");
            ++errors;
        }
    }

    return ( errors == 0 ) ? 0 : -1;
}




int mcd_corrupt_label(int fd) {
    char  label[MCD_OSD_SEG0_BLK_SIZE];
    /* Corrupt some bytes of label */
    fprintf(stderr,"Corrupting label at offset :%lu\n",(uint64_t)MCD_OSD_SEG0_BLK_SIZE/2);
    if( pwrite(fd,label,MCD_OSD_SEG0_BLK_SIZE/2, 0) < 0 ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write label field");
        return -1;
    }
    return -1;
}

int mcd_corrupt_superblock(int fd) {
    /* Corrupt half of the bytes superblock */
    fprintf(stderr,"Corrupting superblock at offset :%lu\n",(uint64_t)(MCD_REC_LABEL_BLKS * MCD_OSD_SEG0_BLK_SIZE));
    memset(tmp_buf,0x3F,SUPER_BUF);
    if( pwrite(fd,tmp_buf,MCD_OSD_SEG0_BLK_SIZE/2, MCD_REC_LABEL_BLKS * MCD_OSD_SEG0_BLK_SIZE) < 0 ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to super block");
        return -1;
    } 
    return 0;
}

int mcd_corrupt_shard_properties (int fd, int shard_idx) {
    int                   buf_idx = -1;
    uint64_t              shard_id = 0;
    char tmp_buf[MCD_OSD_SEG0_BLK_SIZE];

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
    fprintf(stderr,"Corrupting shard properties at offset :%lu for phy container:%lu\n",(uint64_t)((shard_idx + 2 + MCD_REC_LABEL_BLKS) * MCD_OSD_SEG0_BLK_SIZE),shard_id);
    memset(tmp_buf,0x3F,MCD_OSD_SEG0_BLK_SIZE/2); 
    /* Write half of the properties with junk value */
    if( pwrite(fd,tmp_buf,MCD_OSD_SEG0_BLK_SIZE/2, ((shard_idx + 2 + MCD_REC_LABEL_BLKS) * MCD_OSD_SEG0_BLK_SIZE)) < 0 ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to shard properties");
        return -1;
    }
    return 0;
}

int
mcd_corrupt_shard_descriptor(int fd, int shard_idx)
{
    int                   prop_buf_idx = -1;
    int                   buf_idx = -1;
    uint64_t              shard_id = 0;
    mcd_rec_properties_t *properties = NULL;

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
    fprintf(stderr,"Corrupting shard descripters at offset :%lu for phy container:%lu\n",(properties->blk_offset * SHARD_DESC_BLK_SIZE),shard_id);
    memset(tmp_buf,0x3f,SHARD_DESC_BLK_SIZE/2);
    /* Write half of the properties with junk value */
    if( pwrite(fd,tmp_buf,SHARD_DESC_BLK_SIZE/2, (properties->blk_offset * SHARD_DESC_BLK_SIZE)) < 0 ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to shard descriptor");
        return -1;
    }
    return 0;
}

int mcd_corrupt_segment_list(int fd, mcd_rec_shard_t *shard) {
    char *buf = NULL;

    // allocate a buffer
    buf = (char *) malloc( shard->map_blks * SHARD_DESC_BLK_SIZE );
    memset(buf,0x3F,shard->map_blks * SHARD_DESC_BLK_SIZE); 
    fprintf(stderr,"Corrupting segment list at offset :%lu\n",( shard->blk_offset + shard->seg_list_offset) * SHARD_DESC_BLK_SIZE );
    /* Write half of the properties with junk value */
    if( pwrite(fd,buf,( shard->map_blks * SHARD_DESC_BLK_SIZE ), ( shard->blk_offset + shard->seg_list_offset) * SHARD_DESC_BLK_SIZE ) < 0 ) {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to segment list");
        return -1;
    }
    free(buf);
    return 0;
}

int
mcd_corrupt_class_descriptor(int fd, mcd_rec_shard_t *shard)
{
    int c = 0;
    uint32_t blk_size = 0;
    mcd_rec_flash_t *superblock = NULL;

    superblock = (mcd_rec_flash_t *)buf[SUPER_BUF];
    blk_size = superblock->blk_size;

    memset(tmp_buf,0x3F,SHARD_DESC_BLK_SIZE); 
    for ( c = 0; c < MCD_OSD_MAX_NCLASSES; c++ ) {
        // read class desc + segment table for each class
        fprintf(stderr,"Corrupting class desc at offset :%lu\n",(shard->blk_offset + shard->class_offset[ c ] ) * blk_size) ;
        if( pwrite(fd,tmp_buf, blk_size, (shard->blk_offset + shard->class_offset[ c ] ) * blk_size ) < 0 ) {
            zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to class descriptor");
            return -1;
        }
    }
    return 0;
}


int
mcd_corrupt_ckpt_descriptor(int fd, mcd_rec_shard_t *shard)
{
    uint64_t              shard_id = 0;
    uint32_t              blk_size = 0;
    mcd_rec_flash_t      *superblock = NULL;

    shard_id = shard->shard_id;

    superblock = (mcd_rec_flash_t *)buf[SUPER_BUF];
    blk_size = superblock->blk_size;
    fprintf(stderr,"Corrupting check point record :%lu\n",(shard->blk_offset + shard->rec_md_blks - 1) * blk_size) ;

    memset(tmp_buf,0x3f,SHARD_DESC_BLK_SIZE);
    if( pwrite(fd,tmp_buf, blk_size, (shard->blk_offset + shard->rec_md_blks - 1) * blk_size ) < 0 )     {
        zscheck_log_msg(ZSCHECK_LABEL, 0, ZSCHECK_LABEL_ERROR,"Unable to write in to check point record");
        return -1;
    }
    return 0;
}



int mcd_corrupt_meta() {
    /* Faults:
     * ZS_FAULT_LABEL_CORRUPTION
     * ZS_FAULT_SBLOCK_CORRUPTION
     * ZS_FAULT_SHARD_PROP_CORRUPTION
     * ZS_FAULT_SHARD_DESC_CORRUPTION
     * ZS_FAULT_SEGLIST_CORRUPTION
     * ZS_FAULT_CLASS_DESC_CORRUPTION
     * ZS_FAULT_CHKPOINT_CORRUPTION
     * ZS_FAULT_FLOG_CORRUPTION
     * ZS_FAULT_POT_CORRUPTION
     * ZS_FAULT_SEG_BITMAP_CORRUPTION
     * ZS_FAULT_POT_CORRUPTION
     * ZS_FAULT_CONTAINER_CMC
     * ZS_FAULT_CONTAINER_VMC
     * ZS_FAULT_CONTAINER_VDC
     */
    int open_flags = O_RDWR, rc,fd,i;
    char fname[PATH_MAX + 1];
    mcd_rec_shard_t *shard = NULL;
    mcd_osd_shard_t * osd_shard = NULL;

    fprintf(stderr,"Injecting faults\n");

    strcpy(fname, getProperty_String("ZS_FLASH_FILENAME", "/tmp/schooner0"));
    fd = open( fname, open_flags, 00600 );
    if (fd < 0) {
        perror(fname);
        return -1;
    }

    if( atoi(getProperty_String("ZS_FAULT_LABEL_CORRUPTION", "0")) != 0 ) {
        rc = mcd_corrupt_label(fd);
    }
    if( atoi(getProperty_String("ZS_FAULT_SBLOCK_CORRUPTION", "0")) != 0 ) {
        rc = mcd_corrupt_superblock(fd);
    }
    for( i = CMC_SHARD_IDX; i <= VDC_SHARD_IDX; i++ ) {
        if( i == CMC_SHARD_IDX ) {
            if( atoi(getProperty_String("ZS_FAULT_CONTAINER_CMC","0")) == 0 ) {
                continue;
            }
            shard = (mcd_rec_shard_t *)buf[CMC_DESC_BUF];
            osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        }
        else if( i == VMC_SHARD_IDX ) {
            if( atoi(getProperty_String("ZS_FAULT_CONTAINER_VMC","0")) == 0 ) {
                continue;
            }
            shard = (mcd_rec_shard_t *)buf[VMC_DESC_BUF];
            osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        }
        else if( i == VDC_SHARD_IDX ) {
            if( atoi(getProperty_String("ZS_FAULT_CONTAINER_VDC","0")) == 0 ) {
                continue;
            }
            shard = (mcd_rec_shard_t *)buf[VDC_DESC_BUF];
            osd_shard = mcd_check_get_osd_shard( shard->shard_id );
        }
        if( atoi(getProperty_String("ZS_FAULT_SHARD_PROP_CORRUPTION", "0"))!=0) {
            rc = mcd_corrupt_shard_properties(fd,i);
            if( rc < 0 ) {
                return -1;
            }
        }
        if( atoi(getProperty_String("ZS_FAULT_SHARD_DESC_CORRUPTION", "0"))!=0) {
            rc = mcd_corrupt_shard_descriptor(fd,i);
            if( rc < 0 ) {
                return -1;
            }
        }
        if( atoi(getProperty_String("ZS_FAULT_SEGLIST_CORRUPTION", "0"))!=0) {
            rc = mcd_corrupt_segment_list(fd,shard);
            if( rc < 0 ) {
                return -1;
            }
        }
        if( atoi(getProperty_String("ZS_FAULT_CLASS_DESC_CORRUPTION", "0"))!=0) {
            rc = mcd_corrupt_class_descriptor(fd,shard);
            if( rc < 0 ) {
                return -1;
            }
        }
        if( atoi(getProperty_String("ZS_FAULT_CHKPOINT_CORRUPTION", "0"))!=0) {
            rc = mcd_corrupt_ckpt_descriptor(fd,shard);
            if( rc < 0 ) {
                return -1;
            }
        }
        if( atoi(getProperty_String("ZS_FAULT_FLOG_CORRUPTION", "0"))!=0) {
#if 0
            rc = flog_corrupt(osd_shard);
            if( rc < 0 ) {
                return -1;
            }
#endif
        }
    }
    return 0;
}


