enable_language(ASM)
add_library(utils OBJECT linkedlist.c 
	crc32.s
	hash.c 
	hashmap.c 
	properties.c 
	rtlog.c 
	stats.c 
	stats2.c 
	caller.c 
	sdfkey.c 
	platform_check.c)
add_dependencies(utils ffdc)
