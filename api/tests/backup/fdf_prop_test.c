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

#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "api/sdf.h"
#include "api/sdf_internal.h"
#include "api/zs.h"

static struct ZS_state *zs_state;
static int iterations = 10;
static int size = 1024 * 1024;

ZS_status_t fdf.create_container (
	struct ZS_thread_state *_zs_thd_state,
	char                    *cname,
	ZS_cguid_t             *cguid,
	ZS_boolean_t			fifo,
	ZS_boolean_t			persistent,
	ZS_boolean_t			evicting
	)
{
    ZS_status_t            ret;
    ZS_container_props_t   props;
    uint32_t                flags		= ZS_CTNR_CREATE;

    props.size_kb                       = size;
    props.fifo_mode                     = fifo;
    props.persistent                    = persistent;
    props.evicting                      = evicting;
    props.writethru                     = SDF_TRUE;
    props.durability_level              = SDF_FULL_DURABILITY;
    props.cguid                         = SDF_NULL_CGUID;
    props.num_shards                    = 1;
    props.cid                    		= SDF_NULL_CID;

    ret = ZSOpenContainer (
			_zs_thd_state, 
			cname, 
			&props,
			flags,
			cguid
			);

	fprintf( stderr, 
			 "\n>>>ZSOpenContainer: fifo=%d, persistent=%d, evicting=%d, %lu - %s\n", 
			 fifo,
			 persistent,
			 evicting,
			 *cguid, 
			 SDF_Status_Strings[ret] );

    return ret;
}

ZS_status_t zs_get_container_props(
    struct ZS_thread_state *_zs_thd_state,
    ZS_cguid_t              cguid,
	ZS_container_props_t	*pprops
    )
{  
    ZS_status_t     ret;

    ret = ZSGetContainerProps(
            _zs_thd_state,
            cguid,
			pprops
            );
   
	if ( ZS_SUCCESS != ret ) {
    	fprintf( stderr, ">>>container_props: %lu - %s\n", cguid, SDF_Status_Strings[ret] );
	}
   
    return ret;
}

int prop_test()
{

    struct ZS_thread_state 	*_zs_thd_state;

    ZS_cguid_t  				 cguid;
    ZS_container_props_t		 props;
    char 						 cname[32];
    
    if ( ZS_SUCCESS != ZSInitPerThreadState( zs_state, &_zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        plat_assert( 0 );
	}

	for (int i = 0; i < iterations; i++ ) {
		sprintf( cname, "container-%02d", i );
		plat_assert( fdf.create_container( _zs_thd_state, cname, &cguid, SDF_TRUE, SDF_TRUE, SDF_FALSE ) == SDF_SUCCESS);
		plat_assert( zs_get_container_props( _zs_thd_state, cguid, &props ) == SDF_SUCCESS);

		fprintf( stderr, "\n>>>cname					            = %s\n", cname );
		fprintf( stderr, ">>>container_props: size_kb           = %lu\n", props.size_kb );
		fprintf( stderr, ">>>container_props: fifo_mode         = %u\n", props.fifo_mode );
		fprintf( stderr, ">>>container_props: persistent        = %u\n", props.persistent );
		fprintf( stderr, ">>>container_props: evicting          = %u\n", props.evicting );
		fprintf( stderr, ">>>container_props: writethru         = %u\n", props.writethru );
		fprintf( stderr, ">>>container_props: cguid             = %lu\n", props.cguid );
		fprintf( stderr, ">>>container_props: cid               = %lu\n", props.cid );
		fprintf( stderr, ">>>container_props: num_shards        = %u\n", props.num_shards );
	}

    return 0;
}

int param_test()
{

    struct ZS_thread_state     *_zs_thd_state;

    ZS_cguid_t                  cguid;
    char                         cname[32];

    if ( ZS_SUCCESS != ZSInitPerThreadState( zs_state, &_zs_thd_state ) ) {
        fprintf( stderr, "ZS thread initialization failed!\n" );
        plat_assert( 0 );
    }

	// CACHE: FIFO, non-persistent, evicting
	sprintf( cname, "1-CACHE:FIFO-NP-EVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_TRUE, ZS_FALSE, ZS_TRUE );

	// STORE: SLAB, persistent, non-evicting
	sprintf( cname, "2-STORE:SLAB-P-NONEVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_FALSE, ZS_TRUE, ZS_FALSE );

	// P.CACHE: SLAB, persistent, evicting
	sprintf( cname, "3-P.CACHE:SLAB-P-EVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_FALSE, ZS_TRUE, ZS_TRUE );

	// NE.CACHE: SLAB, non-persistent, non-evicting
	sprintf( cname, "4-NE.CACHE:SLAB-NP-NONEVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_FALSE, ZS_FALSE, ZS_FALSE );

	// FAIL: FIFO, non-persistent, non-evicting
	sprintf( cname, "5-FAIL:FIFO-NP-NONEVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_TRUE, ZS_FALSE, ZS_FALSE );

	// FAIL: FIFO, persistent, non-evicting
	sprintf( cname, "6-FAIL:FIFO-P-NONEVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_TRUE, ZS_TRUE, ZS_FALSE );

	// CACHE.SLAB: SLAB, non-persistent, evicting
	sprintf( cname, "7-CACHE.SLAB:SLAB-NP-EVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_FALSE, ZS_FALSE, ZS_TRUE );

	// FAIL: FIFO, persistent, evicting
	sprintf( cname, "8-FAIL:FIFO-P-EVICT" );
    fdf.create_container( _zs_thd_state, cname, &cguid, ZS_TRUE, ZS_TRUE, ZS_TRUE );
    
    return 0;
}           

int main(int argc, char *argv[])
{
    ZSSetProperty("SDF_FLASH_FILENAME", "/schooner/data/schooner%d");
    ZSSetProperty("SDF_FLASH_SIZE", "12");
    ZSSetProperty("SDF_CC_MAXCACHESIZE", "1000000000");

    if ( ZSInit( &zs_state ) != ZS_SUCCESS ) {
        fprintf( stderr, "ZS initialization failed!\n" );
        plat_assert( 0 );
    }

    fprintf( stderr, "ZS was initialized successfully!\n" );

	param_test();

    fprintf(stderr, "DONE\n");

    return(0);
}
