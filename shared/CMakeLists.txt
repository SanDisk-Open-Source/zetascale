add_definitions(-std=gnu99 -fgnu89-inline)
set(srcs cmc.c container.c container_meta.c container_props.c init_sdf.c internal_blk_obj_api.c name_service.c object.c open_container_map.c 	open_container_mgr.c 	shard_compute.c)
add_library(shared OBJECT ${srcs})
