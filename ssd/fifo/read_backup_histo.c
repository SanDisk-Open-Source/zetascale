//----------------------------------------------------------------------------
// ZetaScale
// Copyright (c) 2016, SanDisk Corp. and/or all its affiliates.
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License version 2.1 as published by the Free
// Software Foundation;
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License v2.1 for more details.
//
// A copy of the GNU Lesser General Public License v2.1 is provided with this package and
// can also be found at: http://opensource.org/licenses/LGPL-2.1
// You should have received a copy of the GNU Lesser General Public License along with
// this program; if not, write to the Free Software Foundation, Inc., 59 Temple
// Place, Suite 330, Boston, MA 02111-1307 USA.
//----------------------------------------------------------------------------

/*
 * File:   apps/memcached/server/memcached-1.2.5-schooner/read_backup_stream.c
 * Author: Wayne Hineman
 *
 * Created on Oct 30, 2009
 *
 * (c) Copyright 2009, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: read_backup_stream.c 14056 2010-06-07 23:07:13Z hiney $
 *
 * To compile: gcc -g read_backup_stream.c -o read_backup_stream
 *
 */

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

#include <getopt.h>
#include <string.h>

#include <endian.h>
#include <byteswap.h>

#define PROGRAM_NAME "read_backup_stream"
#define PROGRAM_VERSION "1.0"

#define HISTO_MAX_BINS  100000
#define HISTO_MAX_BYTES 100000

static uint64_t *sortarray[HISTO_MAX_BINS];

typedef struct histo {
    uint32_t    t_offset;
    uint64_t    counts[HISTO_MAX_BINS];
    uint64_t    binbytes[HISTO_MAX_BINS];
    uint64_t    binblocks[HISTO_MAX_BINS];
    uint64_t    bytes[HISTO_MAX_BYTES];
    uint64_t    blocks[HISTO_MAX_BYTES/512 + 1];
    uint64_t    overflows;
    uint64_t    size_overflows;
} histo_t;

static void histo_init(histo_t *ph);
static void histo_add(histo_t *ph, uint32_t tc_in, uint32_t bytes);
static void histo_dump(FILE *f, histo_t *ph, int sort_by_time);

static histo_t  histo_u;
static histo_t  histo_m;

typedef enum {
    OPT_VERSION  = 'v',
    OPT_OBJ      = 'o',
    OPT_META     = 'm',
    OPT_KEY      = 'k',
    OPT_HEX_KEY  = 'x',
    OPT_DATA     = 'd',
    OPT_HEX_DATA = 'e',
    OPT_HELP     = 'h',
    OPT_FILE     = 'f',
    OPT_SHORT    = 's',
} options_t;

/* options */
static struct option long_options[] =
{
    {"file", required_argument, NULL, OPT_FILE},
    {"obj", no_argument, NULL, OPT_OBJ},
    {"meta", no_argument, NULL, OPT_META},
    {"key", no_argument, NULL, OPT_KEY},
    {"hexkey", no_argument, NULL, OPT_HEX_KEY},
    {"data", no_argument, NULL, OPT_DATA},
    {"hexdata", no_argument, NULL, OPT_HEX_DATA},
    {"help", no_argument, NULL, OPT_HELP},
    {"version", no_argument, NULL, OPT_VERSION},
    {"short", no_argument, NULL, OPT_SHORT},
    {0, 0, 0, 0},
};

/* Need to keep this whole thing in sync with struct mcd_osd_meta */
#if __BYTE_ORDER == __BIG_ENDIAN
/* The host byte order is the same as network byte order,
   so these functions are all just identity.  */

# define mcd_ntohll(x)      (x)
# define mcd_ntohl(x)       (x)
# define mcd_ntohs(x)       (x)
# define mcd_htonll(x)      (x)
# define mcd_htonl(x)       (x)
# define mcd_htons(x)       (x)

# define mcd_meta_hton(m)
# define mcd_meta_ntoh(m)

#elif __BYTE_ORDER == __LITTLE_ENDIAN

# define mcd_ntohll(x)    __bswap_64 (x)
# define mcd_ntohl(x)     __bswap_32 (x)
# define mcd_ntohs(x)     __bswap_16 (x)
# define mcd_htonll(x)    __bswap_64 (x)
# define mcd_htonl(x)     __bswap_32 (x)
# define mcd_htons(x)     __bswap_16 (x)

# define mcd_meta_hton(m)                               \
        m->magic       = mcd_htonl(m->magic);           \
        m->version     = mcd_htons(m->version);         \
        m->key_len     = mcd_htons(m->key_len);         \
        m->data_len    = mcd_htonl(m->data_len);        \
        m->blk1_chksum = mcd_htonl(m->blk1_chksum);     \
        m->create_time = mcd_htonl(m->create_time);     \
        m->expiry_time = mcd_htonl(m->expiry_time);     \
        m->seqno       = mcd_htonll(m->seqno);          \
        m->checksum    = mcd_htonll(m->checksum);

# define mcd_meta_ntoh(m)                               \
        m->magic       = mcd_ntohl(m->magic);           \
        m->version     = mcd_ntohs(m->version);         \
        m->key_len     = mcd_ntohs(m->key_len);         \
        m->data_len    = mcd_ntohl(m->data_len);        \
        m->blk1_chksum = mcd_ntohl(m->blk1_chksum);     \
        m->create_time = mcd_ntohl(m->create_time);     \
        m->expiry_time = mcd_ntohl(m->expiry_time);     \
        m->seqno       = mcd_ntohll(m->seqno);          \
        m->checksum    = mcd_ntohll(m->checksum);

#endif

/* little endian */
#define BACKUP_HEADER_MAGIC (0x50554b42)    /* "BKUP" */
#define BACKUP_TRAILER_MAGIC (0x444e4b42)   /* "BKND" */
#define BACKUP_META_MAGIC (0x4154454d)      /* "META" */
#define BACKUP_OBJ_MAGIC (0x544a424f)       /* "OBJT" */

/**
*  backup header from server
   +-------------------+---------+---------+
  0|     magic         | version | #streams|
   +-------------------+---------+---------+
  8|      full         |  incr   | stream# |
   +-------------------+---------+---------+
 16|         previous backup seqno         |
   +---------------------------------------+
 24|         current backup seqno          |
   +---------------------------------------+
 32|           backup time stamp           |
   +---------------------------------------+
 40|                                       |
   +                                       +
 48|                                       |
   +                                       +
 56|                                       |
   +                                       +
 64|                                       |
   +             cluster name              +
 72|                                       |
   +                                       +
 80|                                       |
   +                                       +
 88|                                       |
   +                                       +
 96|                                       |
   +---------------------------------------+
104|                                       |
   +                                       +
112|                                       |
   +                                       +
120|                                       |
   +                                       +
128|                                       |
   +           container name              +
136|                                       |
   +                                       +
144|                                       |
   +                                       +
152|                                       |
   +                                       +
160|                                       |
   +---------+---------+-------------------+
168|tcp port | udp port|   container id    |
   +---------+---------+-------------------+
176|              size quota               |
   +-------------------+---------+----+----+
184|   object quota    |protovers|Evic|Pers|
   +-------------------+---------+----+----+
192
*/
typedef struct bkup_header {
    uint32_t    magic;              // magic number
    uint16_t    version;            // struct version number
    uint16_t    streams;            // number of output streams
    uint32_t    full_nth;           // full backup number (series)
    uint16_t    incr_nth;           // incr backup number within series
    uint16_t    stream_idx;         // stream number
    uint64_t    pre_seq;            // previous backup sequence number
    uint64_t    cur_seq;            // this backup sequence number
    uint64_t    bk_time;            // backup time
    char        cluster_name[64];   // cluster name
    char        ctr_name[64];       // container name
    uint16_t    tcp_port;           // TCP port number
    uint16_t    udp_port;           // UDP port number
    uint32_t    ctr_id;             // container ID
    uint64_t    size_quota;         // container size
    uint32_t    obj_quota;          // object quota
    uint16_t    protocol_version;   // backup protocol version
    uint8_t     evict;              // eviction=1, no eviction=0
    uint8_t     persist;            // persistent=1, non-persistent=0
} ms_bkup_hdr_t;

/**
* backup trailer
  +-------------------+---------+---------+
 0|     magic         | version | #streams|
  +-------------------+---------+---------+
 8|      full         |  incr   | stream# |
  +-------------------+---------+---------+
16|         previous backup seqno         |
  +---------------------------------------+
24|         current backup seqno          |
  +---------------------------------------+
32
 */
typedef struct bkup_trailer {
    uint32_t    magic;              // magic number
    uint16_t    version;            // struct version number
    uint16_t    streams;            // number of output streams
    uint32_t    full_nth;           // full backup number (series)
    uint16_t    incr_nth;           // incr backup number within series
    uint16_t    stream_idx;         // stream number
    uint64_t    pre_seq;            // previous backup sequence number
    uint64_t    cur_seq;            // this backup sequence number
} ms_bkup_trailer_t;

/**
* object meta from server
  +-------------------+---------+---------+
 0|     magic         | version | key len |
  +-------------------+---------+---------+
 8|    data length    | 1st block chksum  |
  +-------------------+-------------------+
16|    create time    |   expiry time     |
  +-------------------+-------------------+
24|                seqno                  |
  +---------------------------------------+
32|              checksum                 |
  +---------------------------------------+
40
 */
typedef struct mcd_osd_meta {
    uint32_t    magic;              // magic number
    uint16_t    version;            // struct version number
    uint16_t    key_len;            // key length
    uint32_t    data_len;           // data length (includes \r\n)
    uint32_t    blk1_chksum;        // checksum of first 512-byte block
    uint32_t    create_time;        // object create time
    uint32_t    expiry_time;        // object expiration time
    uint64_t    seqno;              // objcet sequence number
    uint64_t    checksum;           // full object checksum (md+key+data)
} ms_obj_meta_t;

/**
* object header
  +-------------------+---------+---------+
 0|     magic         | version |    0    |
  +-------------------+---------+---------+
 8|     length        |      address      |
  +-------------------+-------------------+
16
 */
/* local stored object header */
typedef struct obj_hdr {
    uint32_t    magic;              // magic number
    uint16_t    version;            // struct version number
    uint16_t    reserved;           // not used
    uint32_t    length;             // full object length (md+key+data)
    uint32_t    addr;               // logical block address in flash
} ms_obj_hdr_t;

// embedded metadata within the data portion of each object
typedef struct {
    uint8_t         version;        // nonvolatile version number
    uint8_t         internal_flags; // internal flags
    uint16_t        reserved;       // alignment
    uint32_t        client_flags;   // opaque flags from client
    uint64_t        cas_id;         // the CAS identifier
} object_data_t;
// Data follows immediately

inline void
print_version( void )
{
    printf( "%s v%s\n\n", PROGRAM_NAME, PROGRAM_VERSION );
}

inline void
print_description( void )
{
    printf( "    A utility to print data from backup stream files.\n\n"
            "    By default, print backup header and backup trailer,\n"
            "    and give object stats at the end.\n\n"
            "    Optionally print object headers, object metadata,\n"
            "    object key and object data.\n\n" );
}

void
print_usage( void )
{
    printf( "Usage: %s -hv | -f file [-o -m -k -x -d -e -s]\n\n"
            "Options:\n"
            "    -f, --file      backup stream file\n"
            "    -o, --obj       print object headers, including headers for\n"
            "                    deleted objects\n"
            "    -m, --meta      print object metadata\n"
            "    -k, --key       print object key\n"
            "    -x, --hexkey    print object key in hex\n"
            "    -d, --data      print object data\n"
            "    -e, --hexdata   print object data in hex\n"
            "    -v, --version   displays program version\n"
            "    -s, --short     only process first 10000 objects\n"
            "    -h, --help      displays help\n\n",
            PROGRAM_NAME );
    return;
}

int
main( int argc, char **argv )
{
    int                       print_ohdr = 0;
    int                       print_meta = 0;
    int                       print_key = 0;
    int                       print_hexkey = 0;
    int                       print_data = 0;
    int                       print_hexdata = 0;
    int                       do_short = 0;
    int                       rc;
    int                       i;
    int                       size = (1024 * 1024) + 512;
    int                       key_len;
    int                       data_len;
    int                       fd;
    int                       opt;
    int                       option_index = 0;
    uint32_t                  magic;
    uint64_t                  obj_count = 0;
    uint64_t                  obj_deleted = 0;
    uint64_t                  obj_expired = 0;
    uint64_t                  total_count = 0;
    uint64_t                  total_deleted = 0;
    uint64_t                  total_expired = 0;
    FILE                    * stream;
    FILE                    * fout;
    FILE                    * fu;
    FILE                    * fm;
    char                    * file = NULL;
    char                    * buf;
    char                    * buf2;
    ms_bkup_hdr_t           * hdr;
    ms_bkup_trailer_t       * trl;
    ms_obj_hdr_t            * ohdr;
    ms_obj_meta_t           * meta;
    object_data_t           * obj_meta;
    char                      ufname[1024];
    char                      mfname[1024];
    char                      fname[1024];
    uint32_t                  t_backup;
    int                       expired_flag;

    while ((opt = getopt_long(argc, argv, "ehomkdvxsf:",
                              long_options, &option_index)) != -1) {

        switch (opt) {
        case 0:
            break;

        case OPT_FILE:
            file = strdup(optarg);
            break;

        case OPT_SHORT:      // --short or -s 
            do_short = 1;
            break;

        case OPT_OBJ:      // --obj or -o 
            print_ohdr = 1;
            break;

        case OPT_META:     // --meta or -m
            print_meta = 1;
            break;

        case OPT_KEY:      // --key or -k
            print_key = 1;
            break;

        case OPT_HEX_KEY:  // --hexkey or -x
            print_hexkey = 1;
            break;

        case OPT_DATA:     // --data or -d
            print_data = 1;
            break;

        case OPT_HEX_DATA: // --hexdata or -e
            print_hexdata = 1;
            break;

        case OPT_HELP:     // --help or -h
            print_version();
            print_description();
            print_usage();
            return 0;

        case OPT_VERSION:  // --version or -v
            print_version();
            return 0;
        }
    }

    // have to have an input file
    if ( file == NULL ) {
        printf( "Error: must specify '-f file'\n\n" );
        print_usage();
        return 1;
    }

    // get a buffer
    buf = malloc( size );
    if ( buf == NULL ) {
        printf( "Unable to malloc %d-sized buffer\n", size );
        free( file );
        return 1;
    }

    // get another buffer
    buf2 = malloc( size );
    if ( buf2 == NULL ) {
        printf( "Unable to malloc second %d-sized buffer\n", size );
        free( file );
        return 1;
    }

    #ifdef notdef
    // open output files
    sprintf(ufname, "%s_u_meta", file);
    fu = fopen(ufname, "w+");
    if (fu == NULL) {
        printf( "Unable to open file '%s'\n", ufname);
        return 1;
    }
    sprintf(mfname, "%s_m_meta", file);
    fm = fopen(mfname, "w+");
    if (fm == NULL) {
        printf( "Unable to open file '%s'\n", mfname);
        return 1;
    }
    #endif

    histo_init(&histo_u);
    histo_init(&histo_m);

    for (i=1; i<=8; i++) {

        sprintf(fname, "8.%d", i);

	// open backup file
	fd = open( fname, O_RDONLY );
	if ( fd < 0 ) {
	    printf( "Unable to open file '%s'\n", file );
	    free( file );
	    free( buf );
	    return 1;
	}

	obj_count = 0;
	obj_deleted = 0;
	obj_expired = 0;

	// read backup header
	size = read( fd, buf, sizeof( ms_bkup_hdr_t ) );
	if ( size < sizeof( ms_bkup_hdr_t ) ) {
	    printf( "Error reading backup header\n" );
	    free( file );
	    free( buf );
	    return 1;
	}

	hdr   = (ms_bkup_hdr_t *) buf;
	magic = mcd_ntohl( hdr->magic );
	t_backup = mcd_ntohll( hdr->bk_time );

	// print backup header
	printf( "\nBackup Header:\n" );
	printf( "Magic:           %.8x  %.*s\n", mcd_ntohl( hdr->magic ),
						 sizeof( magic ),
						 (char *)&magic );
	printf( "version:         %u\n", mcd_ntohs( hdr->version ) );
	printf( "streams:         %u\n", mcd_ntohs( hdr->streams ) );
	printf( "full:            %u\n", mcd_ntohl( hdr->full_nth ) );
	printf( "incr:            %u\n", mcd_ntohs( hdr->incr_nth ) );
	printf( "stream:          %u\n", mcd_ntohs( hdr->stream_idx ) );
	printf( "previous seq:    %lu\n", mcd_ntohll( hdr->pre_seq ) );
	printf( "current seq:     %lu\n", mcd_ntohll( hdr->cur_seq ) );
	printf( "backup time:     %lu\n", mcd_ntohll( hdr->bk_time ) );
	printf( "cluster name:    %.*s\n", sizeof( hdr->cluster_name ),
					   hdr->cluster_name );
	printf( "container name:  %.*s\n", sizeof( hdr->ctr_name ),
					   hdr->ctr_name );
	printf( "tcp port:        %u\n", mcd_ntohs( hdr->tcp_port ) );
	printf( "udp port:        %u\n", mcd_ntohs( hdr->udp_port ) );
	printf( "container id:    %u\n", mcd_ntohl( hdr->ctr_id ) );
	printf( "size quota:      %lu\n", mcd_ntohll( hdr->size_quota ) );
	printf( "object quota:    %u\n", mcd_ntohl( hdr->obj_quota ) );
	printf( "protocol vers:   %u\n", mcd_ntohs( hdr->protocol_version ) );
	printf( "eviction:        %u\n", hdr->evict );
	printf( "persistent:      %u\n", hdr->persist );

	if ( mcd_ntohl( hdr->magic ) != BACKUP_HEADER_MAGIC ) {
	    printf( "\nUnknown backup header magic number %.8x\n", hdr->magic );
	    goto out;
	}

	while ( 1 ) {

	    // read object header
	    size = read( fd, buf, sizeof( ms_obj_hdr_t ) );
	    if ( size < sizeof( ms_obj_hdr_t ) ) {
		printf( "\nError reading object header\n" );
		goto out;
	    }
	    ohdr = (ms_obj_hdr_t *) buf;

	    // backup trailer marks the end
	    if ( mcd_ntohl( ohdr->magic ) == BACKUP_TRAILER_MAGIC ) {
		break;
	    }

	    // verify object head magic number
	    if ( mcd_ntohl( ohdr->magic ) != BACKUP_OBJ_MAGIC ) {
		printf( "\nUnknown object header magic number %.8x\n",
			ohdr->magic );
		goto out;
	    }
	    obj_count++;
	    total_count++;

	    if (do_short && (obj_count > 10000)) {
		goto out;
	    }

	    // print object header
	    if ( print_ohdr ) {
		magic = mcd_ntohl( ohdr->magic );
		printf( "\nObject Header:\n" );
		printf( "Magic:           %.8x  %.*s\n", mcd_ntohl( ohdr->magic ),
			sizeof( magic ), (char *)&magic );
		printf( "version:         %u\n", mcd_ntohs( ohdr->version ) );
		printf( "reserved:        %u\n", ohdr->reserved );
		printf( "length:          %u\n", mcd_ntohl( ohdr->length ) );
		printf( "address:         %u\n", mcd_ntohl( ohdr->addr ) );
	    }

	    // check for deleted object
	    if ( ohdr->length == 0 ) {
		printf( "\nObject at address %u deleted\n",
			mcd_ntohl( ohdr->addr ) );
		obj_deleted++;
		total_deleted++;
		continue;
	    }

	    // read metadata
	    size = read( fd, buf2, sizeof( ms_obj_meta_t ) );
	    if ( size < sizeof( ms_obj_meta_t ) ) {
		printf( "\nError reading metadata\n" );
		goto out;
	    }
	    meta = (ms_obj_meta_t *) buf2;

	    // verify metadata magic number
	    if ( mcd_ntohl( meta->magic ) != BACKUP_META_MAGIC ) {
		printf( "\nUnknown metadata magic number %.8x\n", meta->magic );
		goto out;
	    }

	    // convert network format
	    mcd_meta_ntoh( meta );
	    key_len = meta->key_len;
	    data_len = meta->data_len;

	    // read key and data
	    size = read( fd, buf, key_len + data_len );
	    if ( size < key_len + data_len ) {
		printf( "\nError reading data\n" );
		goto out;
	    }

	    obj_meta = (object_data_t *)(buf + key_len);

            // check for expired objects
	    expired_flag = 0;
	    if ((meta->expiry_time != 0) &&
		(meta->expiry_time <= t_backup))
	    {
	        // object is expired
		expired_flag = 1;
		obj_expired++;
		total_expired++;
	    }

            // check for flushed objects
	    #ifdef notdef
	    if ((xxxzzz->flush_time != 0) &&
		(meta->create_time >= )   &&
		(meta->create_time >= ))
	    {
	        // object is flushed
	    }
	    #endif

	    if (buf[0] == 'U') {
		fout = fu;
		if (!expired_flag) {
		    histo_add(&histo_u, meta->create_time, meta->key_len + meta->data_len);
		}
	    } else {
		fout = fm;
		if (!expired_flag) {
		    histo_add(&histo_m, meta->create_time, meta->key_len + meta->data_len);
		}
	    }

	    // print metadata
	    if ( print_meta ) {
		fprintf(fout, "\nMetadata:\n" );
		fprintf(fout, "Magic:           %.8x  %.*s\n", meta->magic,
			sizeof( meta->magic ), (char *)&meta->magic );
		fprintf(fout, "version:         %u\n", meta->version );
		fprintf(fout, "key length:      %u\n", meta->key_len );
		fprintf(fout, "data length:     %u\n", meta->data_len );
		fprintf(fout, "1st chksum:      %u\n", meta->blk1_chksum );
		fprintf(fout, "create time      %u\n", meta->create_time );
		fprintf(fout, "expiry time      %u\n", meta->expiry_time );
		fprintf(fout, "seqno:           %u\n", meta->seqno );
		fprintf(fout, "checksum:        %u\n", meta->checksum );
	    }

	    // print key
	    if ( print_key ) {
		fprintf(fout, "\nKey: '%.*s'\n", key_len, buf );
	    }

	    // print key in hex
	    if ( print_hexkey ) {
		fprintf(fout, "Hex Key: 0x" );
		for ( i = 0; i < key_len; i++ ) {
		    fprintf(fout, "%.2x", *(char *)(buf + i) );
		}
		fprintf(fout, "\n" );
	    }

	    // print data
	    if ( print_data ) {
		fprintf(fout, "\nData: meta={ver=%d, iflg=%x, cflg=%x, cas=%lu}, "
			"len=%d, '%.*s'\n",
			obj_meta->version,
			obj_meta->internal_flags,
			obj_meta->client_flags,
			obj_meta->cas_id,
			data_len - sizeof(object_data_t),
			data_len - sizeof(object_data_t),
			buf + key_len + sizeof(object_data_t) );
	    }

	    // print data in hex
	    if ( print_hexdata ) {
		fprintf(fout, "Hex Data: 0x" );
		for ( i = 0; i < data_len - sizeof(object_data_t); i++ ) {
		    fprintf(fout, "%.2x",
			    *(char *)(buf + key_len + sizeof(object_data_t) + i) );
		}
		fprintf(fout, "\n" );
	    }
	}

	// read the remainder of the trailer
	size = read( fd, buf + sizeof( ms_obj_hdr_t ),
		     sizeof( ms_bkup_trailer_t ) - sizeof( ms_obj_hdr_t ) );

	if ( size < sizeof( ms_bkup_trailer_t ) - sizeof( ms_obj_hdr_t )  ) {
	    printf( "Error reading backup trailer\n" );
	    free( file );
	    free( buf );
	    return 1;
	}

	trl   = (ms_bkup_trailer_t *) buf;
	magic = mcd_ntohl( trl->magic );

	// print backup trailer
	printf( "\nBackup Trailer:\n" );
	printf( "Magic:           %.8x  %.*s\n", mcd_ntohl( trl->magic ),
						 sizeof( magic ),
						 (char *)&magic );
	printf( "version:         %u\n", mcd_ntohs( trl->version ) );
	printf( "streams:         %u\n", mcd_ntohs( trl->streams ) );
	printf( "full:            %u\n", mcd_ntohl( trl->full_nth ) );
	printf( "incr:            %u\n", mcd_ntohs( trl->incr_nth ) );
	printf( "stream:          %u\n", mcd_ntohs( trl->stream_idx ) );
	printf( "previous seq:    %lu\n", mcd_ntohll( trl->pre_seq ) );
	printf( "current seq:     %lu\n", mcd_ntohll( trl->cur_seq ) );

	size = read( fd, buf, 1 );
	if ( size != 0 ) {
	    printf( "Error: extra data after backup trailer\n" );
	}

     out:
	printf( "\nTotal objects read:   %lu\n", obj_count );
	printf( "Existing objects:     %lu\n", obj_count - obj_deleted - obj_expired );
	printf( "Deleted objects:      %lu\n", obj_deleted );
	printf( "Expired objects:      %lu\n", obj_expired);

	//rc = fclose( stream );
	rc = close( fd );
	if ( rc != 0 ) {
	    printf( "\nError %d (%s) closing file '%s'\n",
		    errno, strerror(errno), file );
	}
    }

    printf("\n");
    printf("=======================================================================\n");
    printf("Creation Time Histogram for Mapping Data (Sorted by Object Count)\n");
    printf("=======================================================================\n");
    histo_dump(stdout, &histo_m, 0);
    printf("=======================================================================\n");
    printf("Creation Time Histogram for User Data (Sorted by Object Count)\n");
    printf("=======================================================================\n");
    histo_dump(stdout, &histo_u, 0);

    printf("\n");
    printf("=======================================================================\n");
    printf("Creation Time Histogram for Mapping Data (Sorted by Creation Time)\n");
    printf("=======================================================================\n");
    histo_dump(stdout, &histo_m, 1);
    printf("=======================================================================\n");
    printf("Creation Time Histogram for User Data (Sorted by Creation Time)\n");
    printf("=======================================================================\n");
    histo_dump(stdout, &histo_u, 1);

    printf( "\nTotal objects read:   %lu\n", total_count );
    printf( "Existing objects:     %lu\n", total_count - total_deleted - total_expired);
    printf( "Deleted objects:      %lu\n", total_deleted );
    printf( "Expired objects:      %lu\n", total_expired );

    free( file );
    free( buf );

    return 0;
}

static void histo_init(histo_t *ph)
{
    int     i;
    struct  timeval tv;

    if (gettimeofday(&tv, NULL) != 0) {
        printf("gettimeofday failed!");
	exit(1);
    }
    ph->t_offset = tv.tv_sec;

    ph->overflows      = 0;
    ph->size_overflows = 0;

    for (i=0; i<HISTO_MAX_BINS; i++) {
        ph->counts[i]    = 0;
        ph->binbytes[i]  = 0;
        ph->binblocks[i] = 0;
    }
    for (i=0; i<HISTO_MAX_BYTES; i++) {
        ph->bytes[i]  = 0;
    }
    for (i=0; i<(HISTO_MAX_BYTES/512 + 1); i++) {
        ph->blocks[i]  = 0;
    }
}

static void histo_add(histo_t *ph, uint32_t tc_in, uint32_t bytes)
{
    uint32_t    blocks;
    uint32_t    tc;

    assert(tc_in < ph->t_offset);
    tc = ph->t_offset - tc_in;
    tc /= 3600;

    if (bytes <= 512) {
        blocks = 1;
    } else if (bytes <= 1024) {
        blocks = 2;
    } else if (bytes <= 2048) {
        blocks = 3;
    } else if (bytes <= 4096) {
        blocks = 4;
    } else if (bytes <= 8192) {
        blocks = 5;
    } else {
        blocks = bytes/512;
	if (bytes % 512) {
	    blocks++;
	}
    }

    if (tc > HISTO_MAX_BINS) {
        ph->overflows++;
    } else {
        ph->counts[tc]++;
        ph->binbytes[tc]  += bytes;
        ph->binblocks[tc] += blocks;
    }

    if (bytes > HISTO_MAX_BYTES) {
        ph->size_overflows++;
    } else {
        ph->bytes[bytes]++;
        ph->blocks[blocks]++;
    }
}

static char *format_count(char *sout, double x)
{
    if (x > 1e9) {
        sprintf(sout, "%8.2fbi", x/1e9);
    } else if (x > 1e6) {
        sprintf(sout, "%8.2fmi", x/1e6);
    } else if (x > 1e3) {
        sprintf(sout, "%8.2fki", x/1e3);
    } else {
        sprintf(sout, "%8.2f  ", x);
    }
    return(sout);
}

static char *format_bytes(char *sout, double x)
{
    if (x > (1024.*1024.*1024.)) {
        sprintf(sout, "%8.2fb", x/(1024.*1024.*1024.));
    } else if (x > (1024.*1024.)) {
        sprintf(sout, "%8.2fm", x/(1024.*1024.));
    } else if (x > 1024.) {
        sprintf(sout, "%8.2fk", x/1024.);
    } else {
        sprintf(sout, "%8.2f ", x);
    }
    return(sout);
}

static int sortcmp(const void *e1, const void *e2)
{
    uint64_t  c1, c2;

    c1 = **((uint64_t **) e1);
    c2 = **((uint64_t **) e2);
    if (c1 > c2) {
        return(-1);
    } else if (c1 < c2) {
        return(1);
    } else {
        return(0);
    }
}

static void histo_dump(FILE *f, histo_t *ph, int sort_by_date)
{
    int       i, hours;
    double    count,  ccount;
    double    bytes,  cbytes;
    double    blocks, cblocks;
    double    total_count;
    time_t    t;
    char     *stime;
    char      s1[1024];
    char      s2[1024];
    char      s3[1024];
    char      s4[1024];
    char      s5[1024];
    char      s6[1024];

    ccount = cbytes = cblocks = 0;

    #ifdef DO_BLOCK_DATA
	fprintf(f, " Hours           Time                               Count                         Size                    Blocks\n");
	fprintf(f, "                                           Bin (%)   Cummulative (%)         Bin   Cummulative      Bin    Cummulative\n");
	fprintf(f, "==========================================================================================================================\n");
    #else
	fprintf(f, " Hours           Time                               Count           \n");
	fprintf(f, "                                           Bin (%)   Cummulative (%)\n");
	fprintf(f, "===============================================================================\n");
    #endif

    total_count = 0;
    for (i=0; i<HISTO_MAX_BINS; i++) {
	total_count  += ph->counts[i];
    }

    if (sort_by_date) {
	//  Sort in ascending order of age
	//  Nothing special has to be done here
    } else {
	//  Sort in descending order of counts

	for (i=0; i<HISTO_MAX_BINS; i++) {
	    sortarray[i] = &(ph->counts[i]);
	}
	qsort(sortarray, HISTO_MAX_BINS, sizeof(uint64_t *), sortcmp);
    }

    for (i=0; i<HISTO_MAX_BINS; i++) {
        if (sort_by_date) {
	    hours = i;
	} else {
	    hours = (int) (sortarray[i] - ph->counts);
	}
        if (ph->counts[hours] == 0) {
            continue;
        }
	t        = ph->t_offset - (hours*3600);
        stime    = ctime(&t);
        stime[strlen(stime)-1] = '\0';
	count    = ph->counts[hours];
	bytes    = ph->binbytes[hours];
	blocks   = ph->binblocks[hours];
	ccount  += count;
	cbytes  += bytes;
	cblocks += blocks;
	#ifdef DO_BLOCK_DATA
	    fprintf(f, "%5d  %20s  %8g (%5.2f%%)  %8s (%5.2f%%)   %8s  %8s   %8s  %8s\n",
		    hours, stime,
		    count, 100.0*count/total_count, format_count(s2, ccount), 100.0*ccount/total_count,
		    format_bytes(s3, bytes), format_bytes(s4, cbytes),
		    format_bytes(s5, blocks*512.), format_bytes(s6, cblocks*512.));
	#else
	    fprintf(f, "%5d  %20s  %8g (%5.2f%%)  %8s (%5.2f%%)\n",
		    hours, stime,
		    count, 100.0*count/total_count, format_count(s2, ccount), 100.0*ccount/total_count);
	#endif
    }

    #ifdef DO_BLOCK_DATA
	fprintf(f, "==========================================================================================================================\n");
	fprintf(f, "\n");
	fprintf(f, "%g overflows, %g size_overflows\n", (double) ph->overflows, (double) ph->size_overflows);
    #else
	fprintf(f, "===============================================================================\n");
	fprintf(f, "\n");
	fprintf(f, "%g overflows\n", (double) ph->overflows);
    #endif
    fprintf(f, "\n");
}


