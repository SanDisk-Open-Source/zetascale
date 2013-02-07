#ifndef __FDF_TEST__
#define __FDF_TEST__

FDF_status_t
fdf_init();

FDF_status_t
fdf_init_thread();

FDF_status_t fdf_transaction_start();
FDF_status_t fdf_transaction_commit();

FDF_status_t fdf_create_container (
	char                    *cname,
	uint64_t				size,
	FDF_cguid_t             *cguid
	);

FDF_status_t fdf_get (
	FDF_cguid_t                cguid,
	char                      *key,
	uint32_t                   keylen,
	char                     **data,
	uint64_t                  *datalen
	   );

FDF_status_t fdf_free_buffer(
	char 					*data
	);

FDF_status_t fdf_enumerate (
	FDF_cguid_t 			  cguid,
	struct FDF_iterator		**_fdf_iterator
	);

FDF_status_t fdf_next_enumeration(
	FDF_cguid_t               cguid,
	struct FDF_iterator		 *_fdf_iterator,
	char                     **key,
	uint32_t                  *keylen,
	char                     **data,
	uint64_t                  *datalen
	);

FDF_status_t fdf_finish_enumeration (
	FDF_cguid_t  			 cguid,
	struct FDF_iterator		*_fdf_iterator
	);

FDF_status_t fdf_set (
	FDF_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen,
	char					*data,
	uint64_t				 datalen
	);

FDF_status_t fdf_delete(
	FDF_cguid_t              cguid,
	char					*key,
	uint32_t				 keylen
	);

void
advance_spinner();

#define t(func, res) ({ \
	FDF_status_t r = func; \
	fprintf(stderr, "%x %s:%d %s %s=%s - Expected: %s - %s\n", \
		(int)pthread_self(), basename(__FILE__), __LINE__, __FUNCTION__, \
		#func, FDFStrError(r), #res, r == res ? "OK": "FAILED"); \
	if(r != res) \
		exit(1); \
	r; })

#endif /* __FDF_TEST__ */

