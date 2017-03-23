add_library(ws OBJECT ws.c 
	lat.c
	wshash.c
	threadpool.c
        wsstats.c
	mbox.c)
add_dependencies(ws ffdc)
