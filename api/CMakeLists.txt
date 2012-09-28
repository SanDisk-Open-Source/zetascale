add_definitions(-std=gnu99 -fgnu89-inline)
add_library(sdf sdf.c)
add_executable(sdftest main.c)
target_link_libraries(sdftest sdf sdfagent sdfshared sdfaction sdfhome
sdfreplication sdfprotocol sdfssd clipper fifo sdfssd ecc utils sdfmsgqueue
sdfmtp hotkey misc fth platform pthread rt nsl util m aio event)
