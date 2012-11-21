#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include "api/sdf.h"
#include "api/sdf_internal.h"
#include "api/fdf.h"

static struct FDF_state *fdf_state;
static int iterations = 10;
static int size = 1024 * 1024;

FDF_status_t fdf_create_container (
	struct FDF_thread_state *_fdf_thd_state,
	char                    *cname,
	FDF_cguid_t             *cguid,
	FDF_boolean_t			fifo,
	FDF_boolean_t			persistent,
	FDF_boolean_t			evicting
	)
{
    FDF_status_t            ret;
    FDF_container_props_t   props;
    uint32_t                flags		= FDF_CTNR_CREATE;

    props.size_kb                       = size;
    props.fifo_mode                     = fifo;
    props.persistent                    = persistent;
    props.evicting                      = evicting;
    props.writethru                     = SDF_TRUE;
    props.durability_level              = SDF_FULL_DURABILITY;
    props.cguid                         = SDF_NULL_CGUID;
    props.num_shards                    = 1;
    props.cid                    		= SDF_NULL_CID;

    ret = FDFOpenContainer (
			_fdf_thd_state, 
			cname, 
			&props,
			flags,
			cguid
			);

	fprintf( stderr, 
			 "\n>>>FDFOpenContainer: fifo=%d, persistent=%d, evicting=%d, %lu - %s\n", 
			 fifo,
			 persistent,
			 evicting,
			 *cguid, 
			 SDF_Status_Strings[ret] );

    return ret;
}

FDF_status_t fdf_get_container_props(
    struct FDF_thread_state *_fdf_thd_state,
    FDF_cguid_t              cguid,
	FDF_container_props_t	*pprops
    )
{  
    FDF_status_t     ret;

    ret = FDFGetContainerProps(
            _fdf_thd_state,
            cguid,
			pprops
            );
   
	if ( FDF_SUCCESS != ret ) {
    	fprintf( stderr, ">>>container_props: %lu - %s\n", cguid, SDF_Status_Strings[ret] );
	}
   
    return ret;
}

int prop_test()
{

    struct FDF_thread_state 	*_fdf_thd_state;

    FDF_cguid_t  				 cguid;
    FDF_container_props_t		 props;
    char 						 cname[32];
    
    if ( FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
        fprintf( stderr, "FDF thread initialization failed!\n" );
        plat_assert( 0 );
	}

	for (int i = 0; i < iterations; i++ ) {
		sprintf( cname, "container-%02d", i );
		plat_assert( fdf_create_container( _fdf_thd_state, cname, &cguid, SDF_TRUE, SDF_TRUE, SDF_FALSE ) == SDF_SUCCESS);
		plat_assert( fdf_get_container_props( _fdf_thd_state, cguid, &props ) == SDF_SUCCESS);

		fprintf( stderr, "\n>>>cname					            = %s\n", cname );
		fprintf( stderr, ">>>container_props: size_kb           = %u\n", props.size_kb );
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

    struct FDF_thread_state     *_fdf_thd_state;

    FDF_cguid_t                  cguid;
    char                         cname[32];

    if ( FDF_SUCCESS != FDFInitPerThreadState( fdf_state, &_fdf_thd_state ) ) {
        fprintf( stderr, "FDF thread initialization failed!\n" );
        plat_assert( 0 );
    }

	// CACHE: FIFO, non-persistent, evicting
	sprintf( cname, "1-CACHE:FIFO-NP-EVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_TRUE, FDF_FALSE, FDF_TRUE );

	// STORE: SLAB, persistent, non-evicting
	sprintf( cname, "2-STORE:SLAB-P-NONEVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_FALSE, FDF_TRUE, FDF_FALSE );

	// P.CACHE: SLAB, persistent, evicting
	sprintf( cname, "3-P.CACHE:SLAB-P-EVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_FALSE, FDF_TRUE, FDF_TRUE );

	// NE.CACHE: SLAB, non-persistent, non-evicting
	sprintf( cname, "4-NE.CACHE:SLAB-NP-NONEVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_FALSE, FDF_FALSE, FDF_FALSE );

	// FAIL: FIFO, non-persistent, non-evicting
	sprintf( cname, "5-FAIL:FIFO-NP-NONEVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_TRUE, FDF_FALSE, FDF_FALSE );

	// FAIL: FIFO, persistent, non-evicting
	sprintf( cname, "6-FAIL:FIFO-P-NONEVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_TRUE, FDF_TRUE, FDF_FALSE );

	// CACHE.SLAB: SLAB, non-persistent, evicting
	sprintf( cname, "7-CACHE.SLAB:SLAB-NP-EVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_FALSE, FDF_FALSE, FDF_TRUE );

	// FAIL: FIFO, persistent, evicting
	sprintf( cname, "8-FAIL:FIFO-P-EVICT" );
    fdf_create_container( _fdf_thd_state, cname, &cguid, FDF_TRUE, FDF_TRUE, FDF_TRUE );
    
    return 0;
}           

int main(int argc, char *argv[])
{
    FDFSetProperty("SDF_FLASH_FILENAME", "/schooner/data/schooner%d");
    FDFSetProperty("SDF_FLASH_SIZE", "12");
    FDFSetProperty("SDF_CC_MAXCACHESIZE", "1000000000");

    if ( FDFInit( &fdf_state ) != FDF_SUCCESS ) {
        fprintf( stderr, "FDF initialization failed!\n" );
        plat_assert( 0 );
    }

    fprintf( stderr, "FDF was initialized successfully!\n" );

	param_test();

    fprintf(stderr, "DONE\n");

    return(0);
}
