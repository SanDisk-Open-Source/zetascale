add_definitions(-std=gnu99 -fgnu89-inline)
add_library(api OBJECT sdf.c fdf.c enumerate.c stats.c)
add_dependencies(api ffdc)
#add_executable(sdftest main.c)
#target_link_libraries(sdftest ecc sdftest sdf sdfagent sdfshared sdfaction sdfhome
#sdfreplication sdfprotocol sdfssd clipper fifo sdfssd utils sdfmsgqueue
#sdfmtp hotkey misc fth platform pthread rt nsl util m aio event)
