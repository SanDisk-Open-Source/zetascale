add_definitions(-DYY_NO_INPUT -std=gnu99 -fgnu89-inline)
add_library(sdfaction tlmap.c fastcc_new.c action_new.c tlmap4.c simple_replication.c async_puts.c appbuf_pool.c response_tbl.c)
