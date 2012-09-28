add_definitions(-fno-strict-aliasing -DYY_NO_INPUT)
add_library(sdfaction tlmap.c fastcc_new.c action_new.c tlmap4.c simple_replication.c async_puts.c appbuf_pool.c response_tbl.c)
