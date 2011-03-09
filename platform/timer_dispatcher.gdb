define print_dispatcher_heap
    set $heap = $arg0.heap
    
    set variable $i = 0
    print $heap.count
    while ($i < $heap.count)
	print *($heap.data[$i])
	set $i = $i + 1
    end
end

document print_dispatcher_heap
    print_dispatcher_heap <plat_timer_dispatcher>
end
