#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <stdio.h>
#include <linux/hdreg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <scsi/scsi.h>
#include <scsi/sg.h>

#include "mcd_sgio.h"

#define ATA_OP_FLUSHCACHE 0xe7
#define	ATA_USING_LBA  (1 << 6)
#define SG_ATA_16_LEN		16
//#define ATA_OP_FLUSHCACHE 0xea

enum {
	SG_CDB2_TLEN_NODATA	= 0 << 0,
	SG_CDB2_TLEN_FEAT	= 1 << 0,
	SG_CDB2_TLEN_NSECT	= 2 << 0,

	SG_CDB2_TLEN_BYTES	= 0 << 2,
	SG_CDB2_TLEN_SECTORS	= 1 << 2,

	SG_CDB2_TDIR_TO_DEV	= 0 << 3,
	SG_CDB2_TDIR_FROM_DEV	= 1 << 3,

	SG_CDB2_CHECK_COND	= 1 << 5,
};

static inline int needs_lba48 (__u8 ata_op, __u64 lba, unsigned int nsect)
{
	const __u64 lba28_limit = (1<<28) - 1;

	switch (ata_op) {
		case ATA_OP_READ_PIO_EXT:
		case ATA_OP_READ_DMA_EXT:
		case ATA_OP_WRITE_PIO_EXT:
		case ATA_OP_WRITE_DMA_EXT:
		case ATA_OP_READ_VERIFY_EXT:
		case ATA_OP_WRITE_UNC_EXT:
		case ATA_OP_READ_NATIVE_MAX_EXT:
		case ATA_OP_SET_MAX_EXT:
		case ATA_OP_FLUSHCACHE_EXT:
			return 1;
	}
	if (lba >= lba28_limit)
		return 1;
	if (nsect) {
		if (nsect > 0xff)
			return 1;
		if ((lba + nsect - 1) >= lba28_limit)
			return 1;
	}
	return 0;
}

__u64 tf_to_lba (struct ata_tf *tf)
{
	__u32 lba24, lbah;
	__u64 lba64;

	lba24 = (tf->lob.lbah << 16) | (tf->lob.lbam << 8) | (tf->lob.lbal);
	if (tf->is_lba48)
		lbah = (tf->hob.lbah << 16) | (tf->hob.lbam << 8) | (tf->hob.lbal);
	else
		lbah = (tf->dev & 0x0f);
	lba64 = (((__u64)lbah) << 24) | (__u64)lba24;
	return lba64;
}

void tf_init (struct ata_tf *tf, __u8 ata_op, __u64 lba, unsigned int nsect)
{
	memset(tf, 0, sizeof(*tf));
	tf->command  = ata_op;
	tf->dev      = ATA_USING_LBA;
	tf->lob.lbal = lba;
	tf->lob.lbam = lba >>  8;
	tf->lob.lbah = lba >> 16;
	tf->lob.nsect = nsect;
	if (needs_lba48(ata_op, lba, nsect)) {
		tf->is_lba48 = 1;
		tf->hob.nsect = nsect >> 8;
		tf->hob.lbal = lba >> 24;
		tf->hob.lbam = lba >> 32;
		tf->hob.lbah = lba >> 40;
	} else {
		tf->dev |= (lba >> 24) & 0x0f;
	}
}

int sg16 (int fd, int rw, int dma, struct ata_tf *tf,
	void *data, unsigned int data_bytes, unsigned int timeout_secs)
{
	unsigned char cdb[SG_ATA_16_LEN];
	unsigned char sb[32], *desc;
	struct scsi_sg_io_hdr io_hdr;

	memset(&cdb, 0, sizeof(cdb));
	memset(&sb,     0, sizeof(sb));
	memset(&io_hdr, 0, sizeof(struct scsi_sg_io_hdr));

	if (dma) {
		//cdb[1] = data ? (rw ? SG_ATA_PROTO_UDMA_OUT : SG_ATA_PROTO_UDMA_IN) : SG_ATA_PROTO_NON_DATA;
		cdb[1] = data ? SG_ATA_PROTO_DMA : SG_ATA_PROTO_NON_DATA;
	} else {
		cdb[1] = data ? (rw ? SG_ATA_PROTO_PIO_OUT : SG_ATA_PROTO_PIO_IN) : SG_ATA_PROTO_NON_DATA;
	}
	cdb[ 2] = SG_CDB2_CHECK_COND;
	if (data) {
		cdb[2] |= SG_CDB2_TLEN_NSECT | SG_CDB2_TLEN_SECTORS;
		cdb[2] |= rw ? SG_CDB2_TDIR_TO_DEV : SG_CDB2_TDIR_FROM_DEV;
	}

	cdb[ 0] = SG_ATA_16;
	cdb[ 4] = tf->lob.feat;
	cdb[ 6] = tf->lob.nsect;
	cdb[ 8] = tf->lob.lbal;
	cdb[10] = tf->lob.lbam;
	cdb[12] = tf->lob.lbah;
	cdb[13] = tf->dev;
	cdb[14] = tf->command;
	if (tf->is_lba48) {
		cdb[ 1] |= SG_ATA_LBA48;
		cdb[ 3]  = tf->hob.feat;
		cdb[ 5]  = tf->hob.nsect;
		cdb[ 7]  = tf->hob.lbal;
		cdb[ 9]  = tf->hob.lbam;
		cdb[11]  = tf->hob.lbah;
	}
	io_hdr.cmd_len = SG_ATA_16_LEN;

	io_hdr.interface_id	= 'S';
	io_hdr.mx_sb_len	= sizeof(sb);
	io_hdr.dxfer_direction	= data ? (rw ? SG_DXFER_TO_DEV : SG_DXFER_FROM_DEV) : SG_DXFER_NONE;
	io_hdr.dxfer_len	= data ? data_bytes : 0;
	io_hdr.dxferp		= data;
	io_hdr.cmdp		= cdb;
	io_hdr.sbp		= sb;
	io_hdr.pack_id		= tf_to_lba(tf);
	io_hdr.timeout		= (timeout_secs ? timeout_secs : 5) * 1000; /* msecs */

	if (ioctl(fd, SG_IO, &io_hdr) == -1) {
		return -1;	/* SG_IO not supported */
	}

	if (io_hdr.host_status || io_hdr.driver_status != SG_DRIVER_SENSE
	 || (io_hdr.status && io_hdr.status != SG_CHECK_CONDITION))
	{
	  	errno = EBADE;
		return -1;
	}

	desc = sb + 8;
	if (sb[0] != 0x72 || sb[7] < 14 || desc[0] != 0x09 || desc[1] < 0x0c) {
		errno = EBADE;
		return -1;
	}

	tf->is_lba48  = desc[ 2] & 1;
	tf->error     = desc[ 3];
	tf->lob.nsect = desc[ 5];
	tf->lob.lbal  = desc[ 7];
	tf->lob.lbam  = desc[ 9];
	tf->lob.lbah  = desc[11];
	tf->dev       = desc[12];
	tf->status    = desc[13];
	tf->hob.feat  = 0;
	if (tf->is_lba48) {
		tf->hob.nsect = desc[ 4];
		tf->hob.lbal  = desc[ 6];
		tf->hob.lbam  = desc[ 8];
		tf->hob.lbah  = desc[10];
	} else {
		tf->hob.nsect = 0;
		tf->hob.lbal  = 0;
		tf->hob.lbam  = 0;
		tf->hob.lbah  = 0;
	}

	if (tf->status & (ATA_STAT_ERR | ATA_STAT_DRQ)) {
		errno = EIO;
		return -1;
	}
	return 0;
}

int do_drive_cmd (int fd, unsigned char *args)
{

	struct ata_tf tf;
	void *data = NULL;
	unsigned int data_bytes = 0;
	int rc;

	if (args == NULL)
		goto use_legacy_ioctl;
	/*
	 * Reformat and try to issue via SG_IO:
	 */
	if (args[3]) {
		data_bytes = args[3] * 512;
		data       = args + 4;
	}
	tf_init(&tf, args[0], 0, args[1]);
	tf.lob.feat = args[2];
	if (tf.command == ATA_OP_SMART) {
		tf.lob.nsect = args[3];
		tf.lob.lbal  = args[1];
		tf.lob.lbam  = 0x4f;
		tf.lob.lbah  = 0xc2;
	}

	rc = sg16(fd, SG_READ, SG_PIO, &tf, data, data_bytes, 0);
	if (rc == -1) {
		if (errno == EINVAL || errno == ENODEV)
			goto use_legacy_ioctl;
	}

	if (rc == 0 || errno == EIO) {
		args[0] = tf.status;
		args[1] = tf.error;
		args[2] = tf.lob.nsect;
	}
	return rc;

use_legacy_ioctl:

	return ioctl(fd, HDIO_DRIVE_CMD, args);
}

int
do_drive_sync( int fd )
{
	int rc;

	__u8 args[4] = {ATA_OP_FLUSHCACHE,0,0,0};

	rc = do_drive_cmd(fd, args);

        return rc;
}
