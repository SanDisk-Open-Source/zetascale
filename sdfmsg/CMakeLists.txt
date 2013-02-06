add_library(sdfmsg OBJECT sdf_fth_mbx.c 
        sdf_msg_new.c 
        sdf_msg_sync.c 
	sdf_msg_shmem.c 
	sdf_msg_types.c 
	sdf_msg_action.c 
	sdf_msg_wrapper.c 
	sdf_msg_binding.c)
add_dependencies(sdfmsg ffdc)
