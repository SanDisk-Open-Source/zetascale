add_definitions(-fno-builtin-printf)
add_library(fth OBJECT fth.c 
	fthLock.c 
	fthMbox.c 
        fthSem.c 
        fthOpt.c 
	fthSignal.c)
add_dependencies(fth ffdc)
