#include "mcd_rec.h"
#include "mcd_osd.h"

int
mcd_check_label(int fd);

int
mcd_check_superblock(int fd);

int
mcd_check_shard_properties(int fd, int shard_idx);

int
mcd_check_shard_descriptor(int fd, int shard_idx);

int     
mcd_check_all_shard_properties(int fd);

int
mcd_check_all_shard_descriptors(int fd);

int
mcd_check_segment_list(int fd, mcd_rec_shard_t *shard);

int
mcd_check_all_segment_lists(int fd);

int 
mcd_check_class_descriptor(int fd, mcd_rec_shard_t *shard);

int         
mcd_check_all_class_descriptors(int fd);

int 
mcd_check_ckpt_descriptor(int fd, mcd_rec_shard_t *shard);

int
mcd_check_all_ckpt_descriptors(int fd);

int 
mcd_check_meta();

mcd_osd_shard_t *
mcd_check_get_osd_shard( uint64_t shard_id );

int
mcd_check_flog();

int
mcd_check_pot();

int 
mcd_check_is_enabled();

