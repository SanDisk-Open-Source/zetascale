add_library(fifo OBJECT fifo.c 
	mcd_aio.c
	mcd_bak.c
	mcd_pfx.c
	mcd_rec.c
	mcd_rec2.c
	mcd_rep.c
	mcd_sync.c
	mcd_hash.c
 	mcd_osd.c
# 	mcd_ipf.c
	recovery.c
	slab_gc.c
	hash.c
    mcd_check.c
    nvram.c
    scavenger.c )
add_dependencies(fifo ffdc)
add_definitions(-Wno-switch -Wno-parentheses)
