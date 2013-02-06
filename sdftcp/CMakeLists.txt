add_library(sdftcp OBJECT msg_int.c 
	msg_map.c 
	msg_msg.c 
	locks.c 
	stats.c 
	tools.c 
	trace.c)
add_dependencies(sdftcp ffdc)
