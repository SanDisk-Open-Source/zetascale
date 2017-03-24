add_definitions(-Wno-switch -std=gnu99 -fgnu89-inline -Wno-unused-variable -Wno-parentheses)
add_library(api OBJECT fdf.c enumerate.c stats.c async_commands.c
		licensed.c fdf_hash.c fdf_tlmap.c cmap_hash.c cmap.c
		fdf_cmap.c lc.c nvr_svc.c fdf_ws.c fdf_destage.c)
add_dependencies(api ffdc snappy)
#add_executable(sdftest main.c)
#target_link_libraries(sdftest ecc sdftest sdf sdfagent sdfshared sdfaction sdfhome
#sdfreplication sdfprotocol sdfssd clipper fifo sdfssd utils sdfmsgqueue
#sdfmtp hotkey misc fth platform pthread rt nsl util m aio event)
