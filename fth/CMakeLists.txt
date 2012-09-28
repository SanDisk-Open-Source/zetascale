add_definitions(-fno-builtin-printf)
add_library(fth fth.c 
	fthLock.c 
	fthMbox.c 
        fthSem.c 
        fthOpt.c 
	fthSignal.c)