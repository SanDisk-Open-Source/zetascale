/*
 * Copyright (c) 2009-2016, SanDisk Corporation. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with SanDisk Corp.  Please
 * refer to the included End User License Agreement (EULA), "License" or "License.txt" file
 * for terms and conditions regarding the use and redistribution of this software.
 */

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
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <getopt.h>
#include <string.h>

#include <endian.h>
#include <byteswap.h>

#define PROGRAM_NAME "read_backup_stream"
#define PROGRAM_VERSION "1.0"

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
    printf( "Usage: %s -hv | -f file [-o -m -k -x -d -e]\n\n"
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

    while ((opt = getopt_long(argc, argv, "ehomkdvxf:",
                              long_options, &option_index)) != -1) {

        switch (opt) {
        case 0:
            break;

        case OPT_FILE:
            file = strdup(optarg);
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

    // open backup file
    fd = open( file, O_RDONLY );
    if ( fd < 0 ) {
        printf( "Unable to open file '%s'\n", file );
        free( file );
        free( buf );
        return 1;
    }

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

        if (buf[0] == 'U') {
	    fout = fu;
	} else {
	    fout = fm;
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
    printf( "Existing objects:     %lu\n", obj_count - obj_deleted );
    printf( "Deleted objects:      %lu\n", obj_deleted );

    //rc = fclose( stream );
    rc = close( fd );
    if ( rc != 0 ) {
        printf( "\nError %d (%s) closing file '%s'\n",
                errno, strerror(errno), file );
    }

    free( file );
    free( buf );

    return 0;
}

