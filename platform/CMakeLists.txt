set(n 0)
while(n LESS ${NCPU})
	MATH( EXPR n "${n} + 1" )
	set(ffdc_log_files ${ffdc_log_files} ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.${n}.c)
endwhile(n LESS ${NCPU})

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.c ${ffdc_log_files} ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.h ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.dbg ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_reader.c
    COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && /usr/bin/perl ffdc_parser.pl ${NCPU} $(CMAKE_SOURCE_DIR)
	DEPENDS ffdc_log.mc
)

ADD_LIBRARY(platform OBJECT aio_api.c
	aio_libaio.c
	aio_error_bdb.c
	aio_error_control.c
	aio_wc.c
	alloc_stack.c
	aoset.c
	attr.c
	coredump_filter.c
	cpu_peers.c
	event.c
	fd_dispatcher.c
	ffdc_log.c
	fth_scheduler.c
	get_exe.c
	logging.c
	mbox_scheduler.c
	mem_debug.cc
	memory_fault.c
	memory_size.c
	mprobe.c
	msg.c
	prng.c
	alloc.c
	shmem_alloc.c
	shmem.c
	shmem_debug.c
	shmem_global.c
	shmem_ptrs.c
	shmem_test.c
	stats.c
	strarray_alloc.c
	string.c
	timer_dispatcher.c
	tmp_path.c
  	util_trace.c
  	assert.c
	epoll.c
	fcntl.c
	fd.c
	file.c
	fork.c
	mman.c
	mutex.c
	printf.c
	process.c
	random.c
	rwlock.c
	socket.c
	sysvipc.c
	thread.c
	time.c
	${ffdc_log_files}
        )
add_subdirectory(tool/hotkey)
add_custom_target(ffdc DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.c
${ffdc_log_files} ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.h
${CMAKE_CURRENT_SOURCE_DIR}/ffdc_log.dbg ${CMAKE_CURRENT_SOURCE_DIR}/ffdc_reader.c)
add_dependencies(platform ffdc)
