/*
 * ZS Logging Container
 *
 * Copyright 2015 SanDisk Corporation.  All rights reserved.
 */

#include	<stdio.h>
#include	"lc.h"
#include	"platform/logging.h"
#include	"utils/rico.h"


/* resource calculations for one ZS instance
 */
#define bytes_per_nvram				(1L << 24)
#define bytes_per_streaming_object		(1L << 16)
#define placement_groups_per_flash_array	(1L << 13)
#define bytes_per_stream_buffer			(1L << 12)
#define streams_per_flash_array			(bytes_per_nvram / bytes_per_streaming_object)
#define placement_groups_per_stream		(placement_groups_per_flash_array / streams_per_flash_array)
#define bytes_of_stream_buffers			(streams_per_flash_array * bytes_per_stream_buffer)


#define	DIAGNOSTIC		PLAT_LOG_LEVEL_DIAGNOSTIC
#define	INFO			PLAT_LOG_LEVEL_INFO
#define	ERROR			PLAT_LOG_LEVEL_ERROR
#define	FATAL			PLAT_LOG_LEVEL_FATAL
#define	INITIAL			PLAT_LOG_ID_INITIAL
#define	msg( id, lev, ...)	plat_log_msg( id, PLAT_LOG_CAT_SDF_APP_MEMCACHED_RECOVERY, lev, __VA_ARGS__)


static void	dump_params( );
static char	*prettynumber( ulong);


void
lc_init( )
{

	dump_params( );
}


static void
dump_params( )
{

	msg( INITIAL, INFO, "Nominal sizes for Logging Container subsystem:");
	msg( INITIAL, INFO, "bytes_per_nvram = %s", prettynumber( bytes_per_nvram));
	msg( INITIAL, INFO, "bytes_per_streaming_object = %s", prettynumber( bytes_per_streaming_object));
	msg( INITIAL, INFO, "placement_groups_per_flash_array = %s", prettynumber( placement_groups_per_flash_array));
	msg( INITIAL, INFO, "bytes_per_stream_buffer = %s", prettynumber( bytes_per_stream_buffer));
	msg( INITIAL, INFO, "streams_per_flash_array = %s", prettynumber( streams_per_flash_array));
	msg( INITIAL, INFO, "placement_groups_per_stream = %s", prettynumber( placement_groups_per_stream));
	msg( INITIAL, INFO, "bytes_of_stream_buffers = %s", prettynumber( bytes_of_stream_buffers));
}


/*
 * convert number to a pretty string
 *
 * String is returned in a static buffer.  Powers with base 1024 are defined
 * by the International Electrotechnical Commission (IEC).
 */
static char	*
prettynumber( ulong n)
{
	static char	nbuf[100];

	unless (n)
		return ("0");
	uint i = 0;
	until (n & 1L<<i)
		++i;
	i /= 10;
	sprintf( nbuf, "%lu%.2s", n/(1L<<i*10), &"\0\0KiMiGiTiPiEi"[2*i]);
	return (nbuf);
}
