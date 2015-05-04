# Macros needs for core ZS.
# Author: Ramesh Chander 


define find_container 
set $map = cmap_cguid_hash
if ($argc < 1)
    printf "usage: ./find_container cguid\n"
end
set $cguid = $arg0
set $nbuckets = $map->nbuckets 
set $buckets = $map->buckets
set $found = 0
printf "Searching in %d buckets.\n",  $nbuckets
while ($nbuckets > 0) 
    set $bucket = &$buckets[$nbuckets - 1]
    printf "Searching in %d bucket.\r", $nbuckets
    if ($bucket != 0) 
	    set $entry = $buckets[$nbuckets - 1]->entry
	    while ($entry != 0) 
		set $cont = (cntr_map_t *) $entry->contents
		if ($cont->cguid == $cguid) 
		    printf "Found entry\n"
	            p $cont
		    p *$cont
		    set $found = 1
		    loop_break; 
		end
		set $entry = $entry->next
	    end
    end

    if($found == 1)
	loop_break
    end

    set $nbuckets--
end
if ($found != 1)
   printf "Sorry, did not find your cguid %d  in the container map.\n", $cguid
end
