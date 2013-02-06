add_library(hotkey OBJECT ht_report.c ht_snapshot.c ht_alloc.c)
add_dependencies(hotkey ffdc)
