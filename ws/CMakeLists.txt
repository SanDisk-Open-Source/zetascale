add_library(ws OBJECT ws.c 
        destage.c
	lat.c
	latd.c
	wshash.c
	threadpool.c
        wsstats.c
	mbox.c)
add_dependencies(ws ffdc)
