#ifndef __FDF_TRANS_H__
#define __FDF_TRANS_H__

typedef struct {
	uint64_t id;
} trx_t;

trx_t* fdf_get_trx_id();

FDF_status_t fdf_trx_start(trx_t* trx);
FDF_status_t fdf_trx_commit(trx_t* trx);

#endif /* __FDF_TRANS_H__ */

