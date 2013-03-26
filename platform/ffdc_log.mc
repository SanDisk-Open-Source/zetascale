#
# File: $HeadURL$
# Last Changed By: $LastChangedBy$
#
# Copyright (c) 2009-2013, SanDisk Corporation.  All rights reserved.
#
# $Id$
#

#------------------------------------------------------------------------
# To add a new message ID range for a user:
# 1. Take the range specified as "always_last" and add the new user with 
#    that range
# 2. Increment the range for "always_last" by specifying the next range
#    in the series for "always_last"
#------------------------------------------------------------------------

[RANGES]
0 reserved
10000 dkhoe
20000 drew
30000 briano
40000 hiney
50000 xiaonan
60000 jmoilanen
70000 johann
80000 mkrishnan
90000 kcai
100000 xmao
110000 gxu
120000 wli
130000 tomr
140000 build
150000 darryl
160000 root
170000 rico
180000 efirsov
190000 always_last


[MESSAGES]


# reserved


# dkhoe
10000 The Node is in CLONE Mode.\n
10001 Enabled FFDC logging through admin port
10002 Disabled FFDC logging through admin port
10003 Initialized FFDC logging (max buffers=%d, thread bufsize=0x%x)
10004 Unable to initialize FFDC logging (max buffers=%d, thread bufsize=0x%x)
10005 FFDC logging disabled
10011 aio_base lacks %%d, %%x, or %%o
10012 aio_base has too many %% entries
10013 Flush throttle set to %d%% through admin port
10014 Modified data threshold set to %d%% through admin port
10015 SDF Autoflush set to %d%%, with %d sleep msec.
10016 SDF flush throttle set to %d%%.
10017 SDF modified data threshold set to %d%%.
10018 aio_base has %%%c not  %%d, %%x, or %%o
10019 PROP: MEMCACHED_PREFIX_DEL_DELIMITER=%c
10020 This is a FFDC speed test %lu


# drew
20000 ENTERING
20001 failed to allocate memory
20002 allocated buf=%p, aligned buf=%p
20003 failed to read blocks, rc=%d
20004 offset=%lu real_len=%lu
20005 key length mismatch, req %d aio %d
20006 key mismatch, req %s
20007 data size too large
20008 failed to write blocks, rc=%d
20009 hash=%lu offset=%lu
20010 ENTERING, offset=%lu nbytes=%d
20011 buffer/offset not aligned, buf=%p offset=%lu
20012 read beyond limit, offset=%lu nbytes=%d limit=%lu
20013 sub_fds[%d][%x]=%d aio_offset=%lu
20014 request size beyond strip limit, off=%lu nbytes=%d
20015 read iocb ready: fd=%d nbytes=%lu offset=%lu
20016 failed to submit all requests, submitted=%d
20017 read submitted, off=%lu bytes=%d fd=%d a_off=%lu a_bytes=%lu pending=%u
20018 got aio mail
20019 write beyond limit, offset=%lu nbytes=%d limit=%lu
20020 write submitted, off=%lu bytes=%d fd=%d a_off=%lu a_bytes=%lu pending=%u
20021 couldn't open '%s': %s
20022 thread %lu: stat failed for, '%s%s': %s
20023 thread %lu: couldn't open device '%s': %s
20024 thread %lu: name=%s, st_dev=0x%x, st_rdev=0x%x
20025 couldn't close '%s': %s
20026 thread %lu: couldn't find sync device for 0x%x
20027 readlink failed for thread %lu, '%s': %s
20028 thread %lu: closing old fd=%d, devname='%s'
20029 thread %lu, cannot find device: '%s' -> '%s'
20030 thread %lu: opening raid block device '%s'
20031 couldn't open device '%s': %s
20032 thread %lu: devname='%s', old fd=%d, new fd=%d
20033 ENTERING, thread=%lu
20034 fstat failed for thread %lu: %s
20035 thread %lu: filesystem on block device, st_dev=0x%x, st_rdev=0x%x
20036 thread %lu: raw block device st_dev=0x%x, st_rdev=0x%x
20037 thread %lu: raid array on block device, st_dev=0x%x, st_rdev=0x%x
20038 device sync thread %lu, devname=%s waiting
20039 device sync failed, thread %lu, devname=%s, rc=%d, errno=%d
20040 thread %lu: device sync on dead device
20041 thread %lu: device sync fd=%d, devname=%s, rc=%d
20042 sync failed, thread %lu, devname=%s: %s
20043 device sync thread %lu, devname=%s sending reply, rc=%d
20044 device sync thread %lu, devname=%s halting
20045 all devices synced
20046 an error occurred syncing %d of %d devices
20047 sync disabled
20048 %d device(s) synced
20049 an error occurred syncing %d of %d device(s)
20050 ENTERING, dname=%s
20051 Please specify total flash size with the property file or --aio_flash_size
20052 failed to initialize the aio context
20053 aio context initialized
20054 too many aio files, max is %d
20055 too many sub files, max is %d
20056 failed to open file %s: %s
20057 fstat failed for file %s: %s
20058 file %s is too small
20059 aio file %s opened successfully
20060 aio path too long
20061 cannot sync devices, try running with --no_sync
20062 mcd_aio ops registered
20063 failed to alloc memory for backup desc
20064 ENTERING, shardID=%lu, off=%u, del=%d
20065 ENTERING, shardID=%lu
20066 plat_alloc failed
20067 key=%s
20068 sdf shim initialized, log_lvl=%d
20069 container %s already exists
20070 failed to create container %s
20071 failed to open container %s
20072 container=%s opened
20073 shmem_attach(%s) failed: %s
20074 strdup failed
20075 bucket_head=%p
20076 key=%s key_len=%d hash=%lu
20077 cache hit, data=%p len=%d ref_count=%d
20078 object %s not found
20079 flashGet() failed, rc=%d
20080 flash hit
20081 failed to allocate cache entry
20082 new bucket_head=%p
20083 old entry found, data=%p len=%d ref_count=%d
20084 ENTERING, ctxt=%lu
20085 ENTERING, ctxt=%lu exp_time=%lu cur_time=%lu
20086 failed to open container
20087 failed to create container %s, status=%s
20088 failed to open container %s, status=%s
20089 failed to delete container %s, status=%s
20090 container %s opened
20091 for NO_OP run, please use memcached-fthread-shim instead
20092 agent_engine_pre_init() failed
20093 agent_engine_pre_init() succeeded
20094 Triggering simulated flash power-save
20095 Schooner Flash board power-save not supported!
20096 ENTERING, i=%d
20097 ENTERING, bucket=%d
20098 ENTERING, num_buckets=%d
20099 failed to alloc bucket locks
20100 ENTERING, c=%p
20101 failed to alloc new req
20102 ENTERING, c=%p, req=%p
20103 time to enlarge freelist, c=%p count=%d
20104 ENTERING, stat key=%d
20105 ENTERING, init_state=%d is_fthread=%d
20106 conn_new failed, fd=%d base=%p
20107 new connection, c=%p sfd=%d state=%d fth_count=%d
20108 new conn, conn=%p c=%p sfd=%d state=%d fth_count=%d pai=%p
20109 worker mail posted, c=%p
20110 conn closed, c=%p sfd=%d
20111 close mail posted, c=%p
20112 drove machine, c=%p, c->state=%d c->fth_count=%d
20113 connection closed, c=%p
20114 event fd doesn't match conn fd!
20115 pending events reached pm=%lu, count=%lu
20116 count=%d, c=%p
20117 auto-delete mail posted, c=%p
20118 Unable to find GrpId(NODE[%d].GROUP_ID) for node:%d
20119 failed to create and open container %s, status=%s
20120 failed to obtain invalidation time for container %s
20121 failed to obtain shard for container %s
20122 container %s opened, id=%ld size=%lluKB num_objs=%d eviction=%d persistence=%d
20123 container prop found, port=%d id=%d name=%s
20124 persisted container state %d
20125 recovered container state not found
20126 invalid recovered container state %d
20127 container error during initialization, aborting...
20128 got cluster status, node_id=%u num_nodes=%d
20129 failed to listen on TCP port %d
20130 failed to listen on UDP port %d
20131 next container id %lu
20132 CMC Shard: %lx Name:%s cguid:%d\n
20133 failed to get events
20134 got io events, num_events=%d
20135 too many aio errors (> %d), time to abort
20136 aio helper terminated
20137 c=%p freed
20138 got event
20139 got auto-delete mail
20140 got fth mail, c=%p pai=%p
20141 container stopping mail posted
20142 ENTERING %s
20143 ENTERING, num_fthreads=%d, num_sched=%d
20144 aio init helper fthread spawned
20145 failed to alloc pseudo conn for auto delete
20146 auto-delete conn created
20147 agent_engine_post_init() failed
20148 poller fthread spawned, use context %d
20149 maximum number of fthreads is %d
20150 %d fthread workers created
20151 writer fthread spawned, use context %d
20152 aio poller fthread spawned
20153 Mcd_is_fthread set to %d, pth_id=%d
20154 poller fthread spawned
20155 writer fthread spawned
20156 Setting CPU affinity %d for multiple schedulers
20157 Setting scheduler affinity to CPU %d
20158 Error %d setting affinity for CPU %d
20159 Ignoring invalid CPU affinity %d (valid range is 0-%d)
20160 sem posted
20161 scheduler halted
20162 number of fth schedulers over limit, max=%d
20163 pthread_create() failed, rc=%d
20164 scheduler %d created
20165 schedulers terminated
20166 getsockopt(SO_SNDBUF), errno=%d
20167 socket %d send buffer was %d, now %d
20168 %s\n
20169 failed to create socket:%s
20170 failed to bind socket:%s
20171 udp conn set up for %s:%s fds=%p
20172 Please reduce the number of fthreads
20173 failed to alloc osd_buf
20174 failed to alloc udp_conn
20175 ENTERING, self=%p
20176 self found, use context %d
20177 new fthread, use context %u
20178 ENTERING, port=%d
20179 SDFStartContainer() failed, status=%s
20180 SDFStopContainer() failed, status=%s
20181 Please link with the ET(Edge Triggering)-enabled libevent
20182 CONN mail posted, c=%p
20183 drove machine, c=%p, c->state=%d
20184 got INIT req, ctnr_name=%s
20185 failed to create container
20186 container=%ld
20187 got CONN req, connection=%p
20188 this should never happen
20189 Mcd_is_fthread set to %d
20190 failed to alloc xmbox shmem
20191 thread %d created
20192 agent_engine_init() failed, rc=%d
20193 agent_engine_init() succeeded
20194 home_flash_start() failed, rc=%d
20195 Configuring single vip %s ruletable:%d subnet type:%d
20196 Configure IP:%s on interface %s failed\n
20197 Adding source route table %d for %s failed\n
20198 VIP %s not part of interface subnet. Add local route %s/%d
20199 Adding Route %s/%d to table %d on %s for src %s failed \n
20200 Adding default Route via %s on %s  to table %d failed \n
20201 +++++++++\nconfigvip: numvips:%d numpfws:%d\n
20202 VIP configure start: number of vips:%d\n
20203 VIPS for thread %d  start: %d  num vips: %d\n
20204 VIP config done\n
20205 NULL ROUTE TABLE ID:%d ip:%s BUG?\n
20206 Deleting single vip %s ruletable:%d type:%d
20207 Deleting vip(%s) failed
20208 ++++++++++\nDelete VIPs: num_vips:%d
20209 VIP delete done\n
20210 Getting IF Parameters for iface  %s Failed\n
20211 Unable to parse IP Address(%s) from %s\n
20212 Unable to parse Mask(%s) from %s\n
20213 Unable to parse GW(%s) from %s\n
20214 Interface %s parameters(ip:%s mask:%s gw:%s)
20215 Getting vip subnet type for %s mask:%s ifname:%s type:%d
20216 Entry found for %s on the vip entry %s
20217 VIP subnet type not found on the given list for %s
20218 Remove local route if no other VIP in same subnet(ip: %s mask:%s iface:%s)
20219 VIP Type for %s is not QREP_VIP_SUBNET_TYPE_OWN. Just return\n
20220 Found other vip %s in same subnet. Just return
20221 No VIP exists in same subnet(ip: %s mask:%s iface:%s)just remove local route
20222 Failed to remove localroute(%s)\n
20223 ERROR:Unable to find whether vip %s in interface %s subnet. Just return YES
20224 Vip: %s mask:%s not part of iface:%s subnet
20225 Set VIP type num vips %d
20226 ERROR:Unable to find IP params for interface %s. Just set mask to 255.255.0.0
20227 Mask Field for %s is empty. Adding interface's mask(%s)
20228 VIP subnet type found from old list for %s is set to %d
20229 VIP subnet type found from current list for %s and is set to %d
20230 VIP subnet type found from iface subnet for %s and vip subtype set to %d
20231 VIP subnet type can not be found from iface subnet for %s. subtyp set to %d
20232 VIP %s Mask %s GW %s subnet type %d iface %s
20233 =============\nCONFIGURE VIP REQUEST: NUM VIPS %d Group:%d\n
20234 Adding Dynamic VIP %s to group %d
20235 ==========\nDelete All VIPs from group %d
20236 ===========\nTrying to Delete Dynamic VIP %s from group %d
20237 Entry found Deleting Dynamic VIP %s from group %d
20238 This is last entry. Do not need to adjust list
20239 Entry NOT found for VIP %s from group %d. BUG?
20240 Number of VIPs in group %d is 0. So delete current entry
20241 Remove all VIPS
20242 IPF Checklist
20243 The vip address is not valid!
20244 IPF NOTIFY: Entering...
20245 The read-only replication mode is not supported
20246 STM: SDF_REPLICATOR_ACCESS_RW: Vgrp:%d mygrp:%d mynode:%d\n
20247 Faile node:%d(grp:%d) not in my group:%d, ignore failure\n
20248 Current node %d is already active. Ignore failure\n
20249 Node is already handling 1 VIP group. ignore vipgroup:%d \n
20250 New Lease for VIP Group: %d just ignore\n
20251 Ignoring the RW notification for :%d\n
20252 Current node %d is already handling %d VIP groups. Ignore failure\n
20253 STM: Vgrp:%d configured at node:%d
20254 No persistent containers exist. Enabling this node for authority for future persistent recovery\n
20255 Simultaneous Start!!. Send recovery event to %d
20256 Recovery Already completed. Ignore this VipGroup access for recovery\n
20257 STM: SDF_REPLICATOR_ACCESS_NONE: Vgrp:%d mynode:%d\n
20258 Current node %d is not in mirrored cluster, BUG!\n
20259 Virtual IP group %d removed from node %d\n
20260 VIP Group %d Not in my list. Ignore\n
20261 Applying call back completion\n
20262 The replicator doesn't exist
20263 Skipping start simple\n
20264 Cmd:%s
20265 Invalid Node(vipgrp) :%d specified command:%d\n
20266 The Node %d is already active\n
20267 NODE IS VIRTUAL  NUM_PFWS:%d \n
20268 container add rport: %d vport:%d node:%d\n
20269 The current node is not virtual. Ignore
20270 Invalid rule table id %d\n
20271 Add VIP:%s mask:%s gw:%s iface:%s node:%s
20272 Current node is not in Mirrored Mode
20273 Unable to find node ID for %s
20274 The node %s(%d) not in local mirror group
20275 The VIP %s on %s already exists
20276 Maximum allowed number of VIPs(%d) already configured
20277 The Current node servicing %s. Adding VIP %s on %s
20278 Delete VIP:%s iface:%s node:%s
20279 The VIP %s on %s does not exist
20280 The Current node servicing %s. Removing VIP %s from %s
20281 Invalid VIP group\n
20282 Current Node not servicing vipgroup:%d\n
20283 NULL ROUTE TABLE ID:%d\n
20284 VIPGROUP:REMOVAL My Node(%d) is virtual node\n
20285 This is a Weird case that should never happen: vnode:%d rnode:%d\n
20286 VIP Group is Real Node's group. vnode:%d rnode:%d\n
20287 My Node:%d is not virtual node. Just DO Nothing on virtual node recovery\n
20288 The node already srvs:%d grps\n
20289 VIPGrp Config: My Node(%d) is virtual node\n
20290 VIP Group is My own Vip Group. Virtual Node startup vnode:%d rnode:%d\n
20291 My Node:%d is not virtual node. Just DO Nothing\n
20292 FLASH ERROR: code is %u
20293 ENTERING, shard=%p shard_id=%lu flags=0x%x quota=%lu max_nobjs=%u
20294 shard size greater than 2TB, not supported yet
20295 failed to allocate random table
20296 rand_table[%d]=%u, segments[%d]=%u
20297 remapping table initialized, size=%lu
20298 failed to allocate hash buckets
20299 hash buckets allocated, size=%lu
20300 lock_buckets=%lu lock_bktsize=%d, total lock size=%lu
20301 failed to allocate hash table
20302 hash table initialized, size=%lu
20303 failed to alloc overflow table
20304 overflow table initialized, size=%lu
20305 failed to alloc overflow index
20306 overflow index initialized, size=%lu
20307 address table initialized, size=%lu
20308 failed to allocate write buffer
20309 shard initialized, total allocated bytes=%lu
20310 nothing to reclaim
20311 not enough magic, blk_offset=%lu
20312 key_len=%d data_len=%u
20313 reclaiming item: syndrome=%lx syn=%x addr=%u blocks=%u
20314 space reclaimed, blk_offset=%lu blocks=%d
20315 failed to allocate read buffer
20316 got writer mail, blk_committed=%lu
20317 this should not happen
20318 writer mail arrived out of order
20319 failed to commit buffer, rc=%d
20320 buffer still referenced, count=%u
20321 wbuf %d committed, rsvd=%lu alloc=%lu cmtd=%lu next=%lu
20322 rbuf to read, blk_offset=%lu
20323 failed to read data, rc=%d
20324 sleeper waked up, blocks=%d, total=%d
20325 not enough magic!
20326 key length mismatch, req %d osd %d
20327 %lu get cmds
20328 failed to allocate data buffer
20329 %lu sets, b_alloc=%lu overwrites=%lu evictions=%lu soft_of=%lu hard_of=%lu p_id=%d 
20330 object size beyond limit, raw_len=%lu
20331 object exists
20332 block offset about to wrap
20333 mail posted, ready to sleep
20334 waked up, mail=%lu
20335 over allocation detected, rsvd=%lu alloc=%lu next=%lu offset=%lu blks=%d
20336 wbuf %d full, writer mail posted off=%lu rsvd=%lu alloc=%lu cmtd=%lu next=%lu
20337 store object [%ld][%d]: syndrome: %lx key_len: %d
20338 blocks allocated, key_len=%d data_len=%lu blk_offset=%lu addr=%lu blocks=%d filled=%u
20339 failed to allocate slab segments
20340 failed to allocate segment bitmaps[%d]
20341 failed to allocate free lists
20342 slab class inited, size=%d bitmap_size=%d
20343 failed to allocate segment lookup table
20344 segments initialized, %d segments, %d classes size=%lu
20345 failed to alloc shard cache
20346 shard cache allocated, size=%lu miss_interval=%d
20347 ENTERING, total=%lu blks=%lu free=%lu
20348 maximum storage size supported is 2TB
20349 rand_blksize set to %d
20350 number of ssd files needs to be power of 2
20351 aio strip size set to %lu
20352 collision detected, needs a larger table
20353 mask=%.16lx index=%lu value=%u
20354 segment free list set up, totally %d segments
20355 out of free segments
20356 error updating class
20357 segment %d allocated for shard %lu class %ld, blk_offset=%lu used_slabs=%lu total_slabs=%lu
20358 free slab found, map_off=%d blk_off=%lu
20359 free slab, map_off=%d blk_off=%lu
20360 unused slab found, seg_off=%lu map_off=%u
20361 evicting, pth_id=%d hand=%u address=%u index=%lu
20362 segment not ready yet
20363 race detected
20364 cls=%ld addr=%u off=%d map=%.16lx curr=%d slabs=%lu tid=%d
20365 cls=%ld addr=%u off=%d map=%.16lx slabs=%lu, pend=%lu
20366 syndrome collision, key_len=%d data_len=%u
20367 failed to allocate slab
20368 blocks allocated, key_len=%d data_len=%lu blk_offset=%lu blocks=%d
20369 store object [%ld][%d]: syndrome: %lx
20370 bucket=%lu index=%hu
20371 ENTERING, shard=%p
20372 req=%p hash=%lu bucket=%lu
20373 local write: key: %s len: %d data: 0x%lx syndrom: %lx
20374 ENTERING, initializing mcd osd
20375 failed to init mcd osd
20376 failed to format flash rec area
20377 failed to init mcd rec
20378 failed to init replication
20379 ENTERING, shard_id=%lu flags=0x%x quota=%lu max_nobjs=%u
20380 shard found, id=%lu
20381 not enough space
20382 failed to allocate mcd_shard
20383 not enough space, needed %lu available %d
20384 failed to allocated segment list
20385 %dth segment allocated, blk_offset=%u
20386 %lu segments allcated to shard %lu, free_seg_curr=%d
20387 too many shards created
20388 shard_format() failed, rc=%d
20389 seqno cache init failed, rc=%d
20390 slab class dealloc, blksize=%d, bitmap_size=%lu
20391 segments deallocated, %lu segments, %d classes size=%lu
20392 overflow index deallocated, size=%lu
20393 hash table deallocated, size=%lu
20394 overflow table deallocated, size=%lu
20395 lock_buckets deallocated, size=%lu
20396 hash buckets deallocated, size=%lu
20397 remapping table deallocated, size=%lu
20398 address table deallocated, size=%lu
20399 shardID=%lu, total deallocated bytes=%lu
20400 ENTERING, shard_id=%lu
20401 could not find shard, id=%lu
20402 shard init failed, id=%lu
20403 shard recover failed, id=%lu
20404 shard backup init failed, shardID=%lu
20405 ENTERING, shardID=%lu, seqno=%lu
20406 ENTERING, context=%p shardID=%lu
20407 ENTERING, buf=%p
20408 ENTERING, shardID=%lu, full=%d, can=%d, com=%d
20409 ENTERING, shardID=%lu, pseq=%lu, cseq=%lu, can=%d, com=%d
20410 invalid mail, expecting %d received %lu
20411 admin mail received
20412 ENTERING, index=%d
20413 mcd_osd ops registered
20414 not supported yet
20415 invalid raw data size, len=%lu
20416 key/raw_data size mismatch, mkey_len=%d rkey_len=%d md_len=%u rd_len=%lu
20417 failed to allocate raw read buffer
20418 reading raw segment, blk_offset=%lu
20419 failed to read meta, rc=%d
20420 segment read, blk_offset=%lu
20421 not enough magic! blk_off=%lu map_off=%lu key_len=%d seqno=%lu
20422 object deleted, blk_offset=%lu map_offset=%lu key_len=%d seqno=%lu
20423 object found, blk_offset=%lu map_offset=%lu key_len=%d seqno=%lu
20424 ENTERING, addr=%lu prev_seq=%lu curr_seq=%lu
20425 raw object read, addr=%lu len=%lu
20426 mcd_osd_raw_get() failed, status=%s
20427 ENTERING, next_addr=%lu bytes=%lu scanned=%lu expired=%lu
20428 expired obj, key=%s key_len=%d data_len=%d
20429 expired object deleted, key=%s
20430 found slab mode container, tcp_port=%d
20431 invalid length %d (max=%d)
20432 couldn't find shardID=%lu
20433 non-persistent shard, shardID=%lu
20434 failed to write blob, shardID=%lu, offset=%lu, rc=%d
20435 ENTERING, slots=%d
20436 global blob verification failure, offset=%lu, copy=%d
20437 global blob read failure, offset=%lu, copy=%d, rc=%d
20438 blob verification failure, shardID=%lu, shard_offset=%lu, offset=%lu
20439 blob read failure, shardID=%lu, shard_offset=%lu, offset=%lu, rc=%d
20440 superblock data copies (%d) can't be more than the number of files (%d)
20441 failed to write label, blks=%d, blk_offset=%lu, ssd=%d, rc=%d
20442 failed to write superblock data, blks=%d, blk_offset=%lu, ssd=%d, rc=%d
20443 failed to write shard props shardID=%lu, slot=%d, rc=%d
20444 found %d of %d %.*s (offset=%lu) copies ok
20445 attempting to fix %d of %d bad/mismatched copies
20446 failed to write superblock data, try=%d, offset=%lu, count=%d, copy=%d, rc=%d
20447 giving up trying to fix superblock data, offset=%lu, count=%d, copy=%d
20448 successfully re-wrote superblock data, offset=%lu, count=%d, copy=%d
20449 error syncing superblock, rc=%d
20450 failed to allocate shard props
20451 failed to read flash desc, offset=%lu, copy=%d, rc=%d
20452 invalid flash desc checksum, offset=%lu, copy=%d
20453 invalid flash desc, offset=%lu, copy=%d
20454 error updating superblock, rc=%d
20455 failed to read shard props, offset=%ld, ssd=%d, rc=%d
20456 Invalid property checksum, offset=%lu, ssd=%d, slot=%d
20457 invalid property desc, offset=%lu, copy=%d
20458 failed to read shard desc, rc=%d
20459 invalid shard checksum, blk_offset=%lu
20460 failed to allocate shard
20461 failed to allocate pshard
20462 failed to read shard metadata, shrad_offset=%lu, offset=%lu, rc=%d
20463 failed to allocate segment list
20464 invalid class seglist checksum, shard_offset=%lu, offset=%lu
20465 found persistent shard, id=%lu, blk_offset=%lu, md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, pad=%lu (%lu entries)
20466 container %s.%d.%d property changed from persistent to non-persistent - leaving as persistent
20467 can't find shard, shardID=%lu
20468 removing %s shard, shardID=%lu name=%s, tcpport=%d, id=%d (not in current configuration)
20469 write property failed, rc=%d
20470 failed to read seg list, shardID=%lu, blk_offset=%lu, blks=%d, rc=%d
20471 invalid class checksum, shardID=%lu, blk_offset=%lu (blksize=%d)
20472 invalid class seglist checksum, blk_offset=%lu (blksize=%d, slo=%lu, offset=%d)
20473 failed to write seg list blk, shardID=%lu, blk_offset=%lu (blksize=%d), off/slot=%d/%d, rc=%d
20474 ENTERING, shardID=%lu, curr=%d, free=%d, xtra=%d
20475 shardID=%lu, no more tombstones available, curr=%d, free=%d, xtra=%d
20476 failed to alloc tombstone list anchor
20477 failed to alloc tombstone list
20478 Recovering persistent shard, id=%lu, blk_offset=%lu, md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, pad=%lu (%lu entries)
20479 failed to read class metadata, shardID=%lu, blk_offset=%lu, blks=%d, rc=%d
20480 invalid class checksum, shardID=%lu, blk_offset=%lu
20481 invalid class seglist checksum, shardID=%lu, blk_offset=%lu
20482 failed to read ckpt, shardID=%lu, blk_offset=%lu, rc=%d
20483 failed to alloc shard ckpt, shardID=%lu
20484 failed to init tombstones, shardID=%lu
20485 failed to pre-alloc log, shardID=%lu
20486 Recovering persistent shard, id=%lu
20487 failed to read log 0, shardID=%lu, rel_offset=%lu, blk_offset=%lu, rc=%d
20488 failed to read log 1, shardID=%lu, rel_offset=%lu, blk_offset=%lu, rc=%d
20489 failed to init log, rc=%d
20490 Recovered persistent shard, id=%lu, objects=%lu, seqno=%lu, cas_id=%lu, bytes allocated=%lu
20491 writing properties for shardID=%lu, slot=%d
20492 failed to set shard props, rc=%d
20493 persisting new state '%s' for shardID=%lu, slot=%d
20494 no pshard for shard shardID=%lu
20495 pshard deleted, allocated=%lu
20496 recovery overflow for store mode shard!
20497 recovery overflow for cache mode shard!
20498 <<<< upd_HT: syn=%u, blocks=%u, del=%u, bucket=%u, addr=%lu
20499 <<<< skipping offset=%u, start=%lu, end=%lu
20500 <<<< apply_log_rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%u, ooff=%u, seq=%lu, tseq=%lu, obj: syn=%u, ts=%u, blocks=%u, bucket=%u, toff=%lu, seq=%lu
20501 rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%u, ooff=%u, seq=%lu, tseq=%lu, obj: syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, toff=%lu, seq=%lu,  hwm_seqno=%lu
20502 orig_rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%u, ooff=%u, seq=%lu, tseq=%lu
20503 rec: syn=%u, blocks=%u, del=%u, bucket=%u, boff=%u, ooff=%u, seq=%lu, tseq=%lu, obj: syn=%u, ts=%u, blocks=%u, del=%u, bucket=%u, toff=%lu, seq=%lu, hwm_seqno=%lu
20504 ENTERING, log_offset=%lu, log_blks=%lu, start_obj=%lu, num_objs=%lu, last_LSN=%lu, high_offset=%lu, in_rec=%d
20505 reading log buffer blk_off=%lu, blks=%lu
20506 failed to read log, shardID=%lu, blk_offset=%lu, blks=%lu, rc=%d
20507 Invalid log page checksum, found=%lu, calc=%lu, boff=%lu, poff=%d
20508 Invalid log page header, shardID=%lu, magic=0x%x, version=%d, boff=%lu, poff=%d
20509 LSN fell off, prevLSN=%lu, pageLSN=%lu, lastLSN=%lu
20510 Skipping log, offset=%lu, prevLSN=%lu, lastLSN=%lu
20511 ENTERING, %s start=%lu, count=%lu
20512 failed to %s table, start=%lu, count=%lu, blk_offset=%lu, count=%lu, rc=%d
20513 %s %lu blocks (start=%lu, num=%lu)
20514 can't allocate %luM buffer
20515 allocated %lu byte buffer
20516 Object table updater no-op for shardID=%lu
20517 >>>> Object table updater running, shardID=%lu, log=%d, in_rec=%d, update=%lu
20518 >>>> Stopping table update, shardID=%lu, start=%lu, high=%lu
20519 >>>> Reading table chunk %d of %d, shardID=%lu, startBlk=%lu, count=%lu (startObj=%lu, count=%lu, highOff=%lu)
20520 >>>> Found %lu objects, %lu tombstones in table chunk %d of %d, shardID=%lu
20521 >>>> Skip process old log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu
20522 >>>> Process old log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu
20523 >>>> End process old log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu, highLSN=%lu, applied=%d
20524 >>>> Skip process curr log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu
20525 >>>> Process curr log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu
20526 >>>> End process curr log %u, shardID=%lu, start=%lu, highOff=%lu, end=%lu, ckptLSN=%lu, highLSN=%lu, applied=%d
20527 Recovering object table, part %d of %d
20528 >>>> Recovered chunk %d, objects=%lu, seqno=%lu
20529 >>>> Skip writing table chunk %d of %d, shardID=%lu, start=%lu, count=%lu (startObj=%lu, count=%lu, highOff=%lu, old=%d, curr=%d)
20530 >>>> Writing table chunk %d of %d, shardID=%lu, startBlk=%lu, count=%lu (startObj=%lu, count=%lu, highOff=%lu, old=%d, curr=%d)
20531 >>>> Updating ckptLSN=%lu, shardID=%lu
20532 failed to write ckpt, shardID=%lu, offset=%lu, rc=%d
20533 >>>> Updated shard_id=%lu, ckptLSN=%lu, old_log=%d, in_recovery=%d, recovered_objs=%lu, high_seqno=%lu
20534 updater thread halting
20535 failed to allocate log desc
20536 failed to allocate log buffer
20537 shardId=%lu, limiting SYNC_UPDATES to %d
20538 failed to allocate PP logbuf
20539 failed to read end of log, rc=%d
20540 failed to read page after end of log, rc=%d
20541 log initialized, shardID=%lu, curr_log=%d, log_offset=%lu/%lu, log_blks=%lu, curr_LSN=%lu, wbs=%lu, nextfill=%lu, lbs0=%lu, lbs1=%lu
20542 <<<< log_write: shardID=%lu, syn=%u, blocks=%u, del=%u, bucket=%u, blk_offset=%u, old_offset=%u
20543 <<<< log_write: shardID=%lu, logbuf %d filled
20544 <<<< sync: shardID=%lu, logbuf=%d, blks=%u
20545 >>>> sync complete: shardID=%lu, logbuf=%d, blks=%u
20546 ENTERING: shardID=%lu
20547 ENTERING: shardID=%lu, state=%s
20548 ENTERING: shardID=%lu, ppbuf_seqno=%lu, pp_slots=%u
20549 shardId=%lu, curr_seqno=%lu, rec_seqno=%lu, count=%d
20550 shardId=%lu, snap_seqno=%lu, pp_seqno=%lu, apply %d pending updates
20551 shardID=%lu, ppbuf_seqno=%lu, prev=%u, curr=%u, left=%lu, hiseq=%lu
20552 failed to commit log buffer, shardID=%lu, blk_offset=%lu, count=%lu, rc=%d
20553 shardID=%lu, pp_recs=%d, fill_count=%u, seqno=%lu -> slot[%d]: seqno=%lu, slots=%u, prev_count=%u, fill_count=%u
20554 SYNC: shardID=%lu, pp_recs=%d, sync_recs=%d, last_slot=%d; slot[%d]: seqno=%lu, slots=%u, prev_count=%u, fill_count=%u
20555 PP: shardID=%lu, last_slot=%d, slot[%d]: seqno=%lu, slots=%u, prev_count=%u, fill_count=%u
20556 signaling update thread, shardID=%lu, log=%d, hwm_seqno=%lu
20557 shardID=%lu, logbuf %d written, sync=%s, log=%d, rel_off=%lu, blk_off=%lu, blk_cnt=%lu
20558 log writer thread halting, shard_id=%lu
20559 shardID=%lu reference count is %u
20560 %screasing log size from %lu to %u blocks
20561 failed to allocate buffer
20562 formatting shard id=%lu segment %d of %lu, offset=%lu
20563 failed to format shard, shardID=%lu, blk_offset=%lu, b=%d, count=%d, rc=%d
20564 failed to format shard metadata, shardID=%lu, blk_offset=%lu, count=%lu, rc=%d
20565 cannot alloc pshard
20566 formatted persistent shard, id=%lu, blk_offset=%lu, md_blks=%lu, table_blks=%lu, pad=%lu, log_blks=%lu, pad=%lu (%lu entries)
20567 can't find empty prop slot shardID=%lu
20568 failed to write shard props, rc=%d
20569 formatting reserved areas, offset=%lu, blks=%d
20570 failed to format reserved areas, rc=%d
20571 failed to write label, rc=%d
20572 Not enough memory, rep_get_iteration_cursor() failed.
20573 failed to alloc buffer
20574 failed to read buffer, rc=%d
20575 add cursor[%ld][%ld]: syndrome: %x
20576 data copy send: stale: no longer on flash: %d syndrome requested: 0x%x
20577 data copy send: stale: invalid keylen: %d syndrome requested: 0x%x
20578 data copy send: stale: syndrome found: 0x%lx syndrome requested: 0x%x
20579 data copy send: stale: expired/flushed syndrome: 0x%x flush_time: %d create_time: %d
20580 data copy send: key: %s len: %d data: 0x%lx tombstone: %d syndrome: 0x%x
20581 failed to alloc seqno cache
20582 SDF returned NULL object on success
20583 invalid object size, size=%lu
20584 mcd_release_object_data() failed, status=%s
20585 object created, key=%s
20586 object expired, key=%s
20587 object exists, key=%s
20588 sdf_create_put_buffered_obj() failed, status=%s
20589 failed to free data buffer, ret=%d
20590 object set, key=%s
20591 sdf_set_buffered_obj() failed, status=%s
20592 object updated, key=%s
20593 sdf_put_buffered_obj() failed, status=%s
20594 object replaced, key=%s
20595 object unknown, key=%s
20596 ENTERING, key=%s
20597 object pinned for read, key=%s len=%lu
20598 >%d sending key %s
20599 >%d sending suffix%.*s
20600 >%d sending value %.*s
20601 object not found, key=%s
20602 mcd_get_object_data() failed, status=%s
20603 this should not happen, cmd=%d
20604 object pinned for read, key=%s len=%d
20605 failed to free cache buffer, status=%s
20606 object released, key=%s
20607 mcd_set_object_data() failed, status=%s
20608 object written, key=%s
20609 object removed, key=%s
20610 mcd_remove_object() failed, status=%s
20611 %u set cmds processed
20612 >%d %.*s
20613 object already deleted, key=%s
20614 object unpinned, key=%s
20615 cas_id mismatch, key=%s
20616 failed to alloc buf, status=%s
20617 object synced, key=%s
20618 sdf_sync_object() failed, status=%s
20619 object sync_inval'ed, key=%s
20620 sdf_sync_inval_object() failed, status=%s
20621 container synced, key=%s
20622 sdf_sync_container() failed, status=%s
20623 container sync_inval'ed, key=%s
20624 sdf_sync_inval_container() failed, status=%s
20625 container inval'ed, key=%s
20626 sdf_inval_container() failed, status=%s
20627 container (%d) backup started: '%s'
20628 container (%d) backup or restore already running
20629 container (%d) backup requires full backup
20630 container (%d) backup client protocol version %u incompatible with server version %u
20631 container (%d) backup not started, status %s
20632 container (%d) backup status: %s
20633 container (%d) restore in progress
20634 container (%d) backup not running, status %s
20635 container (%d) restore couldn't set flush time
20636 container (%d) restore started: '%s'
20637 container (%d) restore sequence number %lu last restored
20638 container (%d) restore container not empty
20639 container (%d) restore client protocol version %u incompatible with server version %u
20640 container restore not started, status %s
20641 container (%d) restore status: %s
20642 container (%d) backup in progress
20643 container (%d) restore not running, status %s
20644 SUCCESS: Object container create cname='%s', status=%s
20645 SUCCESS: Open Container cname='%s', status=%s
20646 FAILURE: Open Container cname='%s', status=%s
20647 FAILURE: Object container create cname='%s', status=%s
20648 sdf stat %s parsing failure
20649 invalid stat key %d
20650 invalid sdf cache stats, rd=%lu rx=%lu tr=%lu
20651 svn build version does not match build version in mcd_compatibility.h!
20652 hotkey version does not match version in mcd_compatibility.h!
20653 backup version does not match version in mcd_compatibility.h!
20654 CMC version does not match version in mcd_compatibility.h!
20655 blob version does not match version in mcd_compatibility.h!
20656 property file version does not match version in mcd_compatibility.h!
20657 replication protocol version does not match version in mcd_compatibility.h!
20658 sdfmsg version does not match version in mcd_compatibility.h!
20659 msgtcp version does not match version in mcd_compatibility.h!
20660 flash_descriptor version does not match version in mcd_compatibility.h!
20661 shard_descriptor version does not match version in mcd_compatibility.h!
20662 class_descriptor version does not match version in mcd_compatibility.h!
20663 props_descriptor version does not match version in mcd_compatibility.h!
20664 ckpt_record version does not match version in mcd_compatibility.h!
20665 popen failed!
20666 fgets failed!
20667 Could not parse version line!
20668 pclose during version check failed!
20669 Error %d on release_data for key '%s'
20670 Error %d on delete context for key '%s'
20671 Adding msghdr, msgused=%d, iovused=%d
20672 malloc()
20673 calloc()
20674 getpeername failed, ret=%d errno=%d
20675 homeless connection encountered, port=%d
20676 homeless conn encountered, port=%d
20677 c=%p port=%d container=%ld
20678 <%d server listening
20679 <%d server listening (udp), port %d
20680 <%d new client connection, port %d
20681 event_add, errno=%d c=%p fd=%d %lu
20682 <%d connection closed.
20683 Adding iov, len=%d
20684 Added iov, msgused=%d, msgbytes=%d, iovused=%d
20685 >%d NOREPLY %s
20686 >%d %s
20687 req cleanup failed, status=%s
20688 No VIP group group for:%d\n
20689 Printing Vip Group Group:%d
20690 No VIP group group for vid:%d\n
20691 No VIP group for vid:%d\n
20692 VID Array:%s\n
20693 pref list:%s\n
20694 /proc/stat opened, fd=%d
20695 failed to read from proc, rc=%d errno=%d
20696 stats output len=%lu left=%lu
20697 stats output too long, truncated
20698 binary tracing on
20699 binary tracing off
20700 token=%s
20701 failed to alloc hot_key memory, size=%d
20702 failed to initiaize hot key support.
20703 hot key support inited, size=%d nbuckets=%d hotkeys=%d flag=%d
20704 hot key stats on
20705 hot key stats off
20706 failed to reset hot key stats, rc=%d
20707 latency stats on
20708 latency stats off
20709 log level set, %s
20710 >%d END
20711 Number of tokens < 3 : %d ignoring\n
20712 <%d %s
20713 Received shutdown command
20714 starting container, port=%d
20715 failed to start container, status=%s
20716 failed to set container state, rc=%d
20717 container started, port=%d
20718 stopping container, port=%d
20719 failed to stop container, status=%s
20720 container stopped, port=%d
20721 failed to delete container, status=%s
20722 %d tcp conn closed, port=%d
20723 %d udp conn closed, port=%d
20724 failed to cleanup hot_key reporter
20725 deleting container, port=%d
20726 tcp_port=%d udp_port=%d capacity=%ldMB eviction=%d persistent=%d id=%lu cname=%s
20727 This node is VIRTUAL\n
20728 formatting container, port=%d
20729 defunct container encountered, tcp_port=%d
20730 failed to start the container, rc=%d
20731 failed to stop the container, rc=%d
20732 invalid prev_state %d
20733 failed to start persistent recovery, rc=%d
20734 received shutdown command, exiting
20735 read buffer not empty
20736 Couldn't realloc input buffer
20737 listen, errno=%d
20738 c=%p, msgcurr=%d, msgused=%d
20739 c=%p, res=%d
20740 Couldn't update event
20741 Failed to write, and not due to blocking; errno=%d, sfd=%d
20742 Too many open connections
20743 accept(), errno=%d
20744 setting O_NONBLOCK, errno=%d
20745 too many connections, count=%lu
20746 Failed to read, and not due to blocking
20747 Couldn't build response
20748 Transmit complete, c=%p, state=%d
20749 Mwrite complete, c=%p, req_cleanup=%d
20750 Write complete, c=%p, req_cleanup=%d
20751 Unexpected state %d, sdf=%d, c=%p
20752 Transmit incomplete, c=%p
20753 Transmit hard error, c=%p
20754 Transmit soft error, c=%p
20755 cleanup failed, c=%p, status=%s
20756 Catastrophic: event fd doesn't match conn fd!
20757 socket(), errno=%d
20758 <%d send buffer was %d, now %d
20759 getsockopt(SO_RCVBUF), errno=%d
20760 socket %d rcvbuf size was %d, now %d
20761 any bind(): error=%d
20762 any listen(): error=%d
20763 failed to create listening connection (any)
20764 getaddrinfo(): %s
20765 getaddrinfo(): error=%d
20766 bind(): error=%d
20767 listen(): error=%d
20768 failed to create listening connection
20769 MULTI_INSTANCE_MCD is Set
20770 NODE[%d].GROUP_ID = %d\n
20771 SDF_CLUSTER_GROUP[%d].TYPE = %s\n
20772 second npde = %d\n
20773 Could not open the pid file %s for writing
20774 Could not close the pid file %s.
20775 Could not remove the pid file %s.
20776 SIGINT handled.
20777 SIGPWR handled.
20778 Failed to set large pages: %s
20779 Will use default page size
20780 Failed to get supported pagesizes: %s
20781 Executable: %s, Version: %s, Build: %d
20782 no container name specified, use default 'Schooner'
20783 failed to ensure corefile creation
20784 failed to getrlimit number of files
20785 failed to set rlimit for open files (%d - %d). Try running as root or requesting smaller maxconns value.
20786 failed to daemon() in order to daemonize
20787 warning: -k invalid, mlockall() failed: %s
20788 warning: -k invalid, mlockall() not supported on this platform.  proceeding without.
20789 can't run as root without the -u switch
20790 can't find the user %s to switch to
20791 failed to assume identity of user %s
20792 Passing option '%s' to agent_engine_init
20793 Cannot load properties file %s
20794 total flash size is %lu
20795 total specified size is %lu
20796 Cannot set property SDF_FTHREAD_SCHEDULERS=%s
20797 Cannot find peer node info in prop file
20798 %s not found
20799 Version Mismtach with the peer %s
20800 Version check failed. Peer may be down
20801 Version check failed. Peer Node not reachable 
20802 Peer version check SUCCESS
20803 failed to ignore SIGPIPE; sigaction, errno=%d
20804 start in independent clone mode
20805 running in independent mode
20806 running in replicated mode
20807 Node %d is Virtual
20808 Node %d is Virtual. Changing Port from %d to %d
20809 Node %d is Virtual. VIP is not null Changing Port from %d to %d
20810 no container specified in the property file\n
20811 admin port %d opened successfully
20812 No Vip group group for vid:%d\n
20813 Unable to allocate mem for sdf_vip_config\n
20814 Unable to allocate mem for vip_group groups\n
20815 Unable to allocate mem for vip_groups for group:%d\n
20816 Unable to allocate mem for pref List for vip group :%d\n
20817 ID:%d name:%s cguid:%d flags:%d nreplicas:%d num_vgrps:%d numvips:%d\nvips:%s\n
20818 GroupId:%d\nName:%s\nNumCnts:%d\nNumIfs:%d\nIfs:%s\n
20819 %s
20820 Container List...\n
20821 -------------------------\n
20822 GroupId:%d\nType:%d\nNumNodes:%d\nNodes:%s\n
20823 Number Of Nodes:%d\nStatus Port:%d\nClusterId:%d\nClusterName:%s\nNumGroups:%d\n
20824 --------------------\nNode List...\n
20825 Still PFD entry for tcp %s:%d to %d exists\n
20826 Still PFD entry for udp %s:%d to %d exists\n
20827 Could not find PFD entry for tcp %s:%d to %d\n
20828 Could not find PFD entry for udp %s:%d to %d\n
20829 No IP config info for:%s on dev %s\n
20830 Vip group config: rule table %d does not exist\n
20831 ViP group config failure: No route for %s/%d on table:%d\n
20832 ViP group config failure: No default route via %s on table:%d\n
20833 IP config for:%s on dev %s exists after removal\n
20834 Rule table:%d exists after removal\n
20835 Route for %s/%d on table:%d exists after removal\n
20836 Default route via %s on table:%d exists after removal\n
20837 =====================================================================\n
20838 Missing version in property file  (Add 'SDF_PROP_FILE_VERSION = %"PRIu64"')
20839 Inconsistent version of property file: %"PRIu64" (only version %"PRIu64" is supported)
20840 Version of property file: %"PRIu64"
20841 PROP: SDF_CLUSTER_NUMBER_NODES=%u
20842 PROP: SDF_ACTION_NODE_THREADS=%u
20843 PROP: SDF_FLASH_PROTOCOL_THREADS=%u
20844 PROP: SDF_REPLICATION_THREADS=%u
20845 PROP: SDF_SHARD_MAX_OBJECTS=%u
20846 PROP: SDF_MSG_ENGINE_START=%u
20847 PROP: SDF_NUM_FLASH_DEVS=%u
20848 PROP: SDF_DEFAULT_SHARD_COUNT=%u
20849 plat_opts_parse_sdf_agent SUCCESS = %u
20850 set properties SUCCESS = %u
20851  Replication StateMachine Turned ON\n
20852 init_flash  = %u
20853 init_action_home = %u
20854 home_flash_start = %u
20855 sdf_replicator_adapter_start = %u
20856 async_puts_start = %u
20857 init_containers  = %u
20858 PROP: %s=%s
20859 Error creating path (%s), returned (%d)
20860 %s=%s. Defaults will be used.\n
20861 mmap required\n
20862 SM not initialized\n
20863 PROP: %s=%"PRIu64"
20864 PROP: SHMEM_FAKE=%d
20865 shmem_init(%s) failed: %s
20866 Not enough shared memory, plat_shmem_alloc() failed.
20867 PROP: SDF_SHARD_MAX_OBJECTS=%d
20868 %s:%u: %s: Assertion `%s (%d) == %s (%d)' failed.
20869 Can't get CPU affinity: %s
20870 %d CPUs available not all usable with FTH_MAX_SCHEDS %d
20871 fth using up to %d cores from set 0x%x
20872 Failed to set scheduler %d affinity: %s
20873 fth scheduler %d affinity to core %d partners %x
20874 Too few cores to set affinity on sched >= %d
20875 Trying to start scheduler %d >= FTH_MAX_SCHEDS
20876 shmem init failure: %s
20877 shmem attach failure: %s
20878 %lld nsec/switch
20879 %lld usec idle
20880 shmem detach failure: %s
20881 Received signal %d
20882 Failed to get scheduler %d affinity: %s
20883 clock_gettime() went backwards by %3.1g seconds
20884 tsc went backwards by %3.1g seconds
20885 fthIdleConrol %p at %p allocated
20886 fthIdleControlAlloc failed
20887 fthIdleConrol %p at %p free
20888 fthIdleConrol at %p attach attached %d ceiling %d
20889 fthIdleConrol at %p detach attached %d ceiling %d
20890 fthIdleControl at %p no thread, total sleep %ld
20891 fthIdleConrol at %p wait sleeping attached %d ceiling %d sleeping %d
20892 fthIdleConrol at %p wait awake attached %d ceiling %d sleeping %d
20893 fthIdleConrol at %p wait returning attached %d ceiling %d sleeping %d
20894 fthIdleConrol at %p poke numEvents %d attached %d ceiling %d sleeping %d
20895 fth %ld usec %s with %ld allowed
20896 plat_alloc(%llu) returned %p
20897 plat_alloc_arena(%llu, %s) returned %p
20898 plat_calloc(%llu, %llu) returned %p
20899 plat_realloc(%p, %llu) returned %p
20900 plat_free(%p)
20901 plat_alloc_steal_from_heap(%llu) returned %p
20902 can't mprotect %p len %lu: %d
20903 alloc stack red %p data %p to %p
20904 free stack red %p data %p
20905 %s:%u: %s: Assertion `%s' failed.
20906 do closure type %s bound to fn %p env %p
20907 create closure type %s bound to fn %p env %p from %p
20908 apply closure type %s %s bound to fn %p env %p from %p
20909 coredump_filter not set (try a newer kernel)
20910 Failed to %s coredump_filter: %s
20911 Core dump filter %s val 0x%x
20912 can't find any cache levels in %s
20913 glob %s failed: GLOB_ABORTED
20914 glob %s failed: GLOB_NOMATCH
20915 glob %s failed: unknown result %d
20916 expected comma delimited list in %s/shard_cpu_map
20917 can't parse %s from %s/shard_cpu_map
20918 can't open %s: %s
20919 error parsing %s
20920 event %p name '%s' alloc free count %d
20921 event %p name '%s' free free_called_count now %d
20922 event %p name '%s'p scheduling impl_free ref_count now %d
20923 event %p name '%s' fire fire_count %d (incremented) ref_count %d (incremented)
20924 event %p name '%s' fire count %d (incremented)
20925 event %p name '%s' reset ref_count %d (incremented)
20926 event %p name '%s' reset nop
20927 event %p name '%s' impl_fire
20928 event %p name '%s' fired
20929 event %p name '%s' fired done
20930 event %p name '%s' fire done
20931 event %p name '%s' fire count %d refire
20932 event %p name '%s' impl_reset
20933 event %p name '%s' impl_free
20934 event %p name '%s', impl_free_done
20935 event %p name '%s' state %s->%s
20936 event %p name '%s' state %s ref_count 0
20937 event %p name '%s' event_do_free
20938 epoll(%d) failed: %d
20939 polled nfds %d timeout %d ret %d
20940 plat_fd_event %p state %s->%s
20941 plat_fork_execve(filename = %s, args = %s) failed: %d
20942 plat_fork_execve(filename = %s, args = %s)
20943 plat_fth_scheduler %p start called
20944 plat_fth_scheduler %p started
20945 plat_fth_scheduler %p failed to start 
20946 plat_fth_sched %p pts %p main starting
20947 plat_fth_sched %p pts %p main stopped
20948 plat_fth_scheduler %p shutdown complete
20949 plat_fth_scheduler %p shutdown called
20950 log gettime function changed
20951 Faulted %p len 0x%lx, %ld pages in %ld secs %ld usecs %.2g usecs per maj_flt %ld min_flt %ld
20952 Couldn't find MemTotal in %s
20953 Error reading %s: %s
20954 buffer %p page %lx not readable
20955 buffer %p page %lx readable
20956 buffer %p page %lx not writable
20957 buffer %p page %lx writable
20958 plat_send_msg write error %d
20959 plat_recv_msg short header %d of %u bytes
20960 plat_recv_msg bad magix %x
20961 plat_recv_msg short payload %d of %u bytes
20962 exe argument required
20963 shmem header %p maps to NULL
20964 shmem header magic %x not %x
20965 shmem admin magic %x not %x
20966 Reattached shmem_alloc at %p
20967 end_of_data >= end_of_segment
20968 Physmem disabled - not all segments have mapping
20969 Physmem allocation enabled
20970 Created shmem_alloc at %p
20971 Unable to initialize shared memory statistics
20972 Detaching shared memory with existing local arenas threads must call #plat_shmem_pthread_done()
20973 switched to segment %d of %d skipping %llu bytes in last segment
20974 Unable to create arena %s
20975 %ld bytes becoming unusable
20976 Arena corruption  (probable buffer overrun) at %p magic number is %x
20977 arena %p bucket %d grow %d objects parent remaining %llu
20978 arena %p bucket %d grow %lu objects from heap 
20979 arena %p bucket %d shrink %lu objects remain %lu parent %lu
20980 Not initializing due to -DPLAT_SHMEM_FAKE
20981 Ignoring address space request 0x%lx != PLAT_SHMEM_ADDRESS_SPACE = 0x%lx
20982 Can't determine address space size: %s
20983 Ignoring base address request 0x%lx != PLAT_SHMEM_MAP = 0x%lx
20984 Simulating physical memory starting at 0x%lx
20985 aligning address map by %lu bytes
20986 munmap of start alignment failed: %s
20987 munmap of end alignment failed: %s
20988 munmap /dev/zero failed : %s
20989 Not attaching due to -DPLAT_SHMEM_FAKE
20990 open(%s) failed: %s
20991 read %s failed: %s
20992 read %s too short
20993 plat_shmem_attach first segment bad magic %x
20994 plat_shmem_attach first segment no first flag
20995 plat_shmem_attach virt_map at %p not compile time %p
20996 plat_shmem_attach phys_map at %p not compile time %p
20997 plat_shmem_attach phys address space of %llx not compile time %llx
20998 plat_shmem_attach header map %p not existing %p
20999 plat_shmem_attach header len 0x%lx not existing 0x%lx
21000 plat_shmem_attach failed to alloc addr space: %s
21001 plat_shmem_attach admin segment bad magic %x
21002 plat_shmem_attach alloc method %s
21003 Address space 0x%lx-0x%lx not available: %s
21004 open(/dev/zero) failed: %s
21005 mmap /dev/zero hint %p len 0x%lx failed: %s
21006 mmap() placed 0x%lx bytes pages at %p not %p
21007 can't open /proc/self/maps: %s
21008 invalid line reading /proc/self/maps: %s
21009 request 0x%lx-0x%lx existing map 0x%lx-0x%lx %s
21010 cannot attach segment due to unsupported descriptor type %d
21011 init file %s len %llu
21012 ulink(%s) failed: %s
21013 truncate(%s, %lld) failed: %s
21014 shared memory file %s len %lu initialized
21015 shared memory file %s initialization failed: %s
21016 ioctl(%s) failed: %s
21017 second ioctl(%s) failed: %s
21018 shared memory physmem device %s len %llu initialized
21019 shared memory physmem device  %s initialization failed: %s
21020 adding %u regions from device %s total size %llu
21021 skipping region from device %s offset %llu paddr %llu len %llu - too small
21022 skipping region from device %s offset %llu paddr %llu len %llu - paddr too high
21023 initializing region from device %s offset %llu paddr %llu len %llu
21024 init mmap %s offset %llu len %llu paddr %llx
21025 mmap() failed: %s
21026 Error writing pages to %s, probably out of memory
21027 munmap %s failed: %s
21028 cannot attach segment due to invalid descriptor type %d
21029 attach mmap %s offset %llu len %llu paddr %llx
21030 device %s serial %lx does not match %lx
21031 mmap(%s) failed for phys: %s
21032 mmap(%s) failed for virt: %s
21033 May be unable to core dump contents of %s
21034 shmat(%d) failed: %s
21035 shmem_attach_handshake unexpected response_to_seqno %llu not %llu
21036 shmem_attach_handshake unexpected type  %x not %x
21037 shmem_attach_handshake error %s
21038 mmap argument required
21039 sigaction(SIGTERM) failed: %s
21040 sigaction(SIGTINT) failed: %s
21041 init failed
21042 %llu objects totaling %llu bytes in-use
21043 creat(%s) failed: %s
21044 plat_shmem_trivial_test_end not called
21045 %s %ld
21046 temp directory already set to %s not %s
21047 name  %s too long
21048 Cannot create temp directory: %s
21049 No enough memory for hotkey, need %d avaiable %d
21050 alloc: start=%p, end=%p, offset=%d, used=%d\n
21051 Enter:
21052 unexpected command : %d
21053 slab=%d / %d, winners=%d\n
21054 entry=%p: key=%s syndrome=%"PRIu64"refcount=%"PRIu64"
21055 bucket=%d/%d, buckets=%d
21056 entry=%p: syndrome=%"PRIu64" refcount=%"PRIu64"
21057 snapshot=%d/%d, entries=%d
21058 entry=%p : key=%s refcount=%"PRIu64"
21059 dumping winner lists
21060 dumping candidate lists
21061 dumping snapshot lists
21062 dumping all
21063 max_list_head is NULL, list=%p
21064 ntop is larger than nbuckets: ntop=%d,nbuckets=%"PRIu64"
21065 Receive buffer is too small %d, need %lu
21066 ntop is larger than maxtop: ntop=%d, maxtop=%d
21067 sdf_create_queue_pair failed in get_queue_pair!
21068 PROP: SDF_REPLICATION=%s
21069 PROP: SDF_LRU=%s
21070 PROP: SDF_SIMPLE_REPLICATION=%s
21071 PROP: SDF_MAX_OBJ_SIZE=%"PRIu64"
21072 PROP: SDF_CC_MAXCACHESIZE=%"PRIu64"
21073 PROP: SDF_CC_AVG_OBJSIZE=%d
21074 PROP: SDF_CC_BUCKETS=%"PRIu64"
21075 PROP: SDF_CC_NSLABS=%"PRIu64"
21076 PROP: SDF_LRU_THRESHOLD must be <= 0.2; forcing it to 0.2
21077 PROP: SDF_LRU_THRESHOLD=%g
21078 Bad value for SDF_PER_THREAD_NONOBJECT_ARENA
21079 Bad value for SDF_PER_THREAD_OBJECT_ARENA
21080 Cache arenas object %s nonobject %s
21081 plat_alloc failed!
21082 PROP: SDF_N_SYNC_CONTAINER_THREADS=%d
21083 PROP: SDF_N_SYNC_CONTAINER_CURSORS=%d
21084 Existence check for flash_ctrput_wb failed! (flash return code = %d)
21085 Flash get to check expiry for a create-put (wb) failed! (flash return code = %d)
21086 Flash get to check expiry for a put (wb) failed! (flash return code = %d)
21087 Flash delete for an expired put failed! (flash return code = %d)
21088 Existence check for flash_set_wb failed! (flash return code = %d)
21089 deletion for a castout (eviction) failed!
21090 Flash get to check expiry for a create-put (wt) failed! (flash return code = %d)
21091 Flash get to check expiry for a put (wt) failed! (flash return code = %d)
21092 Flash delete for an expired get failed! (flash return code = %d)
21093 Flash get to check expiry for a delete failed! (flash return code = %d)
21094 Flash delete of an expired object in flash failed! (flash return code = %d)
21095 Flash delete operation failed! (flash return code = %d)
21096 deletion for a castout (eviction mode) failed!
21097 deletion for a castout (store mode) failed (flash_retcode=%d)!
21098 Failed to get container metadata
21099 name_service_put_meta failed!
21100 Failed to get container metadata for CMC
21101 sdf_msg_send to replication failed
21102 sdf_msg_send to replication succeed\n
21103 sdf_msg_send to failed: Node (%d) died\n
21104 sdf_msg_send to failed: Message timeout to node %d\n
21105 sdf_msg_send to failed: Unknown error (%d) to node %d\n
21106 Not enough memory, plat_alloc() failed.
21107 Exceeded max number of containers (%d).
21108 Could not find flash_dev for cguid %llu shardid %lu
21109 flash returned no pshard for cguid %llu shardid %lu
21110 ===========  name_service_get_meta(cguid=%"PRIu64"): meta.flush_time=%d meta.flush_set_time=%d ===========
21111 name_service_get_meta failed!
21112 Used up all SDF_RESERVED_CONTEXTS!
21113 [tag %d, node %d, pid %d, thrd %d] request: %s
21114 [tag %d, node %d, pid %d, thrd %d] final status: %s (%d), %s
21115 Unsupported command: %s (%s)!
21116 Failed to get a directory entry
21117 Invalid container type in flush_stuff: %d
21118 Invalid container type in inval_stuff: %d
21119 sdf_get_preloaded_ctnr_meta failed for cguid %"PRIu64"!
21120 SDFUnPreloadContainerMetaData failed!
21121 delete_home_shard_map_entry failed!
21122 init_app_buf_pool failed!
21123 Inconsistency in number of requests in flight!
21124 Too many requests in flight (%d)!
21125 [tag %d, node %d, pid %d, thrd %d] %s (ret=%s[%d]): pshard: %p, key:%s, keyLen:%d, t_exp:%d, t_create:%d, pdata:%p, dataLen:%d, sequence:%"PRIu64"
21126 ===========  START async put (cguid=%"PRIu64",key='%s'):  ===========
21127 ===========  END async put (cguid=%"PRIu64",key='%s'):  ===========
21128 data local store no partner: key: %s data: 0x%lx ret: %d
21129 Invalid value for flags
21130 data forwarding sending: key: %s len: %d data: 0x%lx
21131 data local store partner: key: %s len: %d data: 0x%lx ret: %d
21132 Invalid combined_action: %d (%s) for operation xxxzzz(cguid=%"PRIu64", key='%s')
21133 Not enough memory, sdf_msg_alloc() failed.
21134 async puts thread pool %p allocated
21135 async_puts_alloc failed 
21136 shutdown async puts state %p NOT IMPLEMENTED!
21137 async puts %p starting
21138 home flash %p started
21139 home flash %p failed to start 
21140 Invalid asynchronous put service request type (%d)
21141 async puts thread stopping pts %p
21142 Not enough memory for cache (%"PRIu64"), plat_alloc() failed.
21143 Not enough memory for cache.
21144 Not enough memory for cache: must have at least enough memory for two maximum sized objects per slab (%"PRIu64" required, %"PRIu64" available). Try increasing SDF_CC_MAXCACHESIZE or reducing the number of slabs.
21145 Maximum object size must be a multiple of 8B
21146 Maximum object size is too large! (it must be < %d)
21147 lru_threshold must be <= 1
21148 lru_threshold must be >= 0
21149 LRU bytes = %"PRIu64"
21150 ================>   pslab:%p, key: '%s'
21151 Container type is neither OBJECT or BLOCK.
21152 Internal inconsistency: entry to be removed is not in bucket list.
21153 ================>   tail: %p head: %p
21154 ================>   lru_head: %p lru_tail: %p
21155 Object is larger than specified max. Check SDF_MAX_OBJ_SIZE property is large enough
21156 ================>   size: %d, tail: %p head: %p
21157 Object is larger than specified max. Check that SDF_MAX_OBJ_SIZE property is large enough
21158 ================>  Transient SDF cache entry was not reclaimed!
21159 Bad combined response in replication response tableRespTbl[%s][%s][%s][%s]
21160 Bad action in replication response tableRespTbl[%s][%s][%s][%s]
21161 ID:%d name:%s cguid:%d flags:%d nreplicas:%d num_vgrps:%d numvips:%d vips:%s\n
21162 GroupId:%d\nhName:%s\nNumCnts:%d\nNumIfs:%d\nIfs:%s
21163 GroupId:%d\nType:%d\nNumNodes:%d\nNodes:%s
21164 VIPGROUP:%s
21165 VIPGROUPGROUP:%s
21166 VIP CONFIG NULL:%s
21167 VIP CONFIG:%s
21168 SDF_CLUSTER_NUMBER_NODES > 0\n
21169 Number of nodes in property file (%d) is not the same as the number of nodes reported by the messaging system (%d)!
21170 Could not allocate per-node replication state
21171 SDF_CLUSTER_ID should  be >= 0 
21172 SDF_CLUSTER_NAME should not be empty
21173 SDF_CLUSTER_NUMBER_OF_GROUPS must be > 0
21174 Could not allocate per group structure
21175 SDF_CLUSTER_GROUP[%d].NUM_NODES must be >= 2 for 2way or nplus1\n
21176 SDF_CLUSTER_GROUP[%d].GROUP_ID must be >= 0\n
21177 Could not allocate per group per node stcructure\n
21178 SDF_CLUSTER_GROUP[%d].NODE[%d] must be configured \n
21179 NODE[%d].GROUP_ID must be >= 0 for 2way or nplus1\n
21180 NODE[%d].NAME must not be empty\n
21181 IS NODE STARTED FIRST TIME: %d\n
21182 MEMCACHED_NCONTAINERS[%d] must be > 0\n
21183 Could not allocate per-node container state array
21184 Inconsistency in cluster configuration: Invalid replica node (%d) for MEMCACHED_CONTAINER[%d][%d]
21185 Inconsistency in cluster configuration: NREPLICAS must be 1 or 2 for MEMCACHED_CONTAINER[%d][%d]
21186 Inconsistency in cluster configuration: Invalid standby node (%d) for MEMCACHED_CONTAINER[%d][%d]
21187 Inconsistency in cluster configuration: Unreplicated containers with a standby must allow eviction (MEMCACHED_CONTAINER[%d][%d])
21188 Inconsistency in cluster configuration: Invalid standby container (%d) for MEMCACHED_CONTAINER[%d][%d]
21189 Inconsistency in cluster configuration: Unreplicated containers with a standby must allow eviction (MEMCACHED_CONTAINER[%d][%d].STANDBY_CONTAINER)
21190 Inconsistency in cluster configuration: Standby containers must have a standby node (ie need to know what node the container is on) Invalid standby node (%d) for MEMCACHED_CONTAINER[%d][%d]
21191 Inconsistency in cluster configuration: MEMCACHED_CONTAINER[%d][%d].NUM_IFACES must be > 0 for 2way or n+1 group\n
21192 Inconsistency in cluster configuration: for the node:%d cont:%d, ip(%s),mask(%s),ifname(:%s) should not be null\n
21193 Inconsistency in cluster configuration: No mask was given for vip %s (MEMCACHED_CONTAINER[%d][%d])
21194 The vip mask \"%s\" is not valid for MEMCACHED_CONTAINER[%d][%d]
21195 Could not allocate per-container replication state
21196 Fatal inconsistency in cluster configuration
21197 Inconsistency in cluster configuration: mismatched container ID's for MEMCACHED_CONTAINER[%d][%d]
21198 Inconsistency in cluster configuration: mismatched global indices for MEMCACHED_CONTAINER[%d][%d]
21199 Inconsistency in cluster configuration: mismatched GB's for MEMCACHED_CONTAINER[%d][%d]
21200 Inconsistency in cluster configuration: mismatched MAX_OBJS's for MEMCACHED_CONTAINER[%d][%d]
21201 Inconsistency in cluster configuration: mismatched STANDBY_NODE's for MEMCACHED_CONTAINER[%d][%d]
21202 Inconsistency in cluster configuration: mismatched NREPLICAS for MEMCACHED_CONTAINER[%d][%d]
21203 Inconsistency in cluster configuration: unreplicated container on more than one node for MEMCACHED_CONTAINER[%d][%d]
21204 Inconsistency in cluster configuration: mismached REPLICA_NODE's for MEMCACHED_CONTAINER[%d][%d]
21205 Inconsistency in cluster configuration: invalid NREPLICAS for MEMCACHED_CONTAINER[%d][%d]
21206 Inconsistency in cluster configuration: EVICTION mode conflict for MEMCACHED_CONTAINER[%d][%d]
21207 Inconsistency in cluster configuration: PERSISTENT mode conflict for MEMCACHED_CONTAINER[%d][%d]
21208 Inconsistency in cluster configuration: an IS_STANDBY container can only reside on a single node (MEMCACHED_CONTAINER[%d][%d])
21209 Inconsistency in cluster configuration: VIP mode conflict for MEMCACHED_CONTAINER[%d][%d]
21210 Inconsistency in cluster configuration: mismatched VIP's for MEMCACHED_CONTAINER[%d][%d]
21211 Inconsistency in cluster configuration: mismatched container VIP_MASK's for MEMCACHED_CONTAINER[%d][%d]
21212 Inconsistency in cluster configuration: mismatched container VIP_TCP_PORT's for MEMCACHED_CONTAINER[%d][%d]
21213 Inconsistency in cluster configuration: mismatched container VIP_UDP_PORT's for MEMCACHED_CONTAINER[%d][%d]
21214 Inconsistency in cluster configuration: mismatched container VIP_IF's for MEMCACHED_CONTAINER[%d][%d]
21215 Setting Node: %d as Virtual
21216 Starting in CLONE mode \n
21217 Added a container with container id: %d (cguid:%d)to RepStruture peer:%d\n
21218 Enable replication to node %d\n
21219 Could not start replication of container %d to node %d\n
21220 Node %d does not have any container\n
21221 container %d not found in list\n
21222 Invalid Node Id:%d NumNodes:%d\n
21223 Group Type:%s for node:%d\n
21224 Group ID:%d for node:%d\n
21225 Setting container status %d to cid :%d\n
21226 Setting container status %d to cid :%d on replica\n
21227 Number of Nodes:%d in node:%d\n
21228 SDFGetClusterGroupType CID:%d GroupType:%d \n
21229 Unable to get Group Group ID for container id:%d\n
21230 UPDATE META: CID:%d GroupId:%d type:%d\n
21231 Unable to open socket\n
21232 Unable to resolve local host\n
21233 Connection to split brain handler failed\n
21234 Write to split brain handler failed\n
21235 Read from split brain handler failed\n
21236 Node %d is live!
21237 Live Node is my node. Ignore the event\n
21238 The Live node is not part of the the local group(id:%d). So Ignore the event\n
21239 Both the local node %d  and new live node %d are in same group %d\n
21240 The Live node %d is not serviced by the local node %d !. Ignore the event for now\n
21241 My local node %d is not yet active just ignore\n
21242 The Recovering node is part of nplus 1. Just ignore \n
21243 The Recovering node is in group %d Just ignore \n
21244 Unable to find Action Init state. Skipping recovery...\n
21245 Node %d is dead!
21246 CMC Shard: %lx cguid:%d\n
21247 Sending RECOVERED command to STM\n
21248 Stoping cloning\n
21249 Failed to get the container port for :%d
21250 Restarting the container %d failed before starting recovery
21251 Starting cloning threads\n
21252 Could not allocate memory for container list
21253 Recovery without memcached restart due to possible splitbrian condition. Exiting...
21254 SLAVE Skipping container %d\n
21255 Recovery type is only Non Persitent: Ignore recoverying container %d
21256 Recovery type is only Persitent: Ignore recoverying container %d
21257 Formating the container %d
21258 Stopping the container %d failed
21259 Failed to format the container port:%d
21260 Container %d Not Found
21261 Starting the container %d Failed
21262 Failed to start replicating from node: %d rc: %d
21263 %s: Failed to start replication
21264 Stoping the container %d after recovery failed
21265 Data recovery done for the container %d
21266 Restarting the container %d
21267 Failed to start replication: sync old node %d failed: %d
21268 SLAVE CNTRID: %d status %d\n
21269 Data recovery done
21270 rpc_start_replicating failed: %d
21271 Waiting to start recovery to %d\n
21272 Number of VGRP serviced %d. Ignore Recovery to node %d\n
21273 Recovery to node %d already completed. Ignore\n
21274 Waiting for completion of container manipulation command to start recovery
21275 recovery request ID  %d remote node %d rec_type %d\n
21276 There is new recovery request waiting. Ignore the currrent one %d\n
21277 Remote Node %d died. Ignore the recovery\n
21278 My Node Hosts 2 VIP Groups.  Start the recovery to node %d\n
21279 flash_put failed: key: %s len: %d data: 0x%lx
21280 data copy recv: key: %s len: %d data: 0x%lx rc: %d
21281 Failed to get iteration cursors node:%d rc: %d\n
21282 get iteration complete
21283 get iteration cursors returned a negativecursor count: %d
21284 cursor_mbx_done returned a NULL value
21285 Failed to get iteration cursors node: %d rc: %d\n
21286 Failed to get cursors\n
21287 rpc_get_by_cursor recv: key: %s status: %d
21288 Could not allocate thread-local map buckets.
21289 Could not allocate a thread-local map entry.
21290 Could not allocate a thread-local key.
21291 Can't create queue pair src node %u service %u dest node %u service %u
21292 home flash %p allocated
21293 home_flash_alloc failed 
21294 shutdown flash state %p ref_count %d
21295 home flash %p starting
21296 shutdown flash state %p complete
21297 flash protocol thread %d starting pts %p
21298 STALE LTIME - recv_pm->op_meta.shard_ltime: %u - pfs->ltime: %u
21299 received Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d shard 0x%lx
21300 Unsupported message type %s
21301 update ltime: %u
21302 sending message msgtype = %s
21303 Home flash drain I/O not complete - waiting - ref count: %d - wait_count: %d
21304 Home flash drain I/O wait limit exceeded (%d) - shutting down!
21305 Home flash drain I/O complete
21306 flash protocol thread stopping pts %p, %d remain
21307 no shardmap entry for shardid 0x%lx
21308 Could not create shardmap entry for shardid 0x%lx; Entry already exists or there is insufficient memory
21309 home_flash ran out of shard map structuresfor shardid 0x%lx
21310 home_flash operation to a stopped containerfor cwguid %llu shardid 0x%lx
21311 Adjust time current_time_in: %d current_time_local: %d clock_skew: %d
21312 data forwarding recv: key: %s len: %d data: 0x%lx
21313 Object invalidation failed with status %s(%d)
21314 [tag %d, node %d, pid %d, thrd %d] Slow path flash call: %s '%s':%"PRIu64", pshard=%p, ret=%s(%d), status=%s(%d)
21315 home_flash_shard_wrapper operation to a stopped containerfor shardid 0x%lx
21316 Create shard sguid 0x%lx flags %X quota %llu numObjs %d
21317 HFCSH container name: %s
21318 shardOpen failed for shardid 0x%lx
21319 create shard sguid 0x%lx failed, can't create flags
21320 flash returned no pshard for shardid 0x%lx
21321 sync shard for shardid %lu
21322 flash returned no pshard for cwguid %llu key %s shardid %lx
21323 get_last_sequence sguid 0x%lx quota %llu numObjs %d
21324 got sequence num %ld for  sguid 0x%lx
21325 flash returned no pshard for cwguid %llu key %s shardid 0x%lx
21326 Home Flash Get by Cursor allocation failure\n
21327 get_sequence sguid 0x%lx quota %llu numObjs %d
21328 flashDelete failed for sguid 0x%lx
21329 flashDelete succeeded for sguid 0x%lx
21330 HFFLA: current_time_in: %d flush_time_in: %d current_time_local: %d clock_skew: %d flush_time_local: %d
21331 No error mapping for %s\n
21332 PROP: SDF_HOME_DIR_BUCKETS=%"PRIu64"
21333 PROP: SDF_HOME_DIR_LOCKTYPE=%"PRIu32"
21334 \nHash Table Delete ppme %p pb %p h %lu\n
21335 \nHash Table Delete ppme %p key %"PRIu64" contents %p\n
21336 sdf/PROTOCOL is INITIALIZED \n
21337 inconsistency in SDF_App_Request_Info: %d != [%d].msgtype
21338 inconsistency in SDF_Protocol_Nodes_Info: %d != [%d].field
21339 inconsistency in SDF_Protocol_Msg_Info: %d != [%d].msgtype
21340 Unknown container type=%u
21341 Object name is too long: '%s'
21342 Could not open UTMalloc trace file
21343 Opened UTMalloc trace file: %s
21344 copy_replicator %p allocated
21345 copy_replicator alloc failed
21346 copy_replicator %p starting
21347 copy_replicator %p shard 0x%lx recovered
21348 copy_replicator %p shard 0x%lx recovered status %s output '%s'
21349 copy_replicator %p shutdown
21350 copy_replicator %p node %u shutdown with %d ref count
21351 copy_replicator %p node %u node %u live
21352 copy_replicator %p node %u unknown node %u live cluster may be misconfigured
21353 copy_replicator %p node %u node %u dead
21354 copy_replicator %p node %u unknown node %u dead cluster may be misconfigured
21355 copy_replicator %p node %u refcount 0
21356 copy_replicator %p node %u shard shutdown count 0
21357 copy_replicator %p node %u rms stopped
21358 copy_replicator %p node %u scheduler stopped
21359 cr_shard %p node %u shard 0x%lx vip group %d state from %s to %s
21360 cr_shard %p node %u shard 0x%lx vip group %d fast start preferred replica %d on node %d
21361 cr_shard %p node %u shard 0x%lx vip group %d fast start after_restart %d
21362 cr_shard %p node %u shard 0x%lx vip group %d home %d lease len %3.1f expires %s
21363 cr_shard %p node %u shard 0x%lx vip group %d home %d lease len %3.1f expires %s with local shard state %s
21364 cr_shard %p node %u shard 0x%lx vip group %d mutual redo %s replica %d local to node %u in-core state %s last range type %s start %lld len %lld
21365 cr_shard %p node %u shard 0x%lx vip group %d create failed state %s
21366 cr_shard %p node %u shard 0x%lx vip group %d current seqno %lld proposed %s
21367 cr_shard %p node %u shard 0x%lx vip group %d state %s putting meta lease len %3.1f
21368 cr_shard %p node %u shard 0x%lx vip group %d put meta complete: %s state %s flags 0x%x do not include CRSSF_META_SOURCE_SELF
21369 cr_shard %p node %u shard 0x%lx vip group %d put meta complete: %s
21370 cr_shard %p node %u shard 0x%lx vip group %d missed meta update last seqno %lld received %lld
21371 cr_shard %p node %u shard 0x%lx vip group %d lease expires %s meta now %s
21372 cr_shard %p node %u shard 0x%lx vip group %d stale meta received  current seqno %lld received %lld
21373 cr_shard %p node %u shard 0x%lx vip group %d seqno %lld notify start
21374 cr_shard %p node %u shard 0x%lx vip group %d seqno %lld notify events {%s} access %s expires %s
21375 cr_shard %p node %u shard 0x%lx vip group %d notification ref count 0 after %ld usec
21376 cr_shard %p node %u shard 0x%lx vip group %d refcount 0
21377 cr_shard %p node %u shard 0x%lx vip group %d state %s scheduling renewal callback at %s
21378 cr_shard %p node %u shard 0x%lx vip group %d state %s renewal callback
21379 cr_shard %p node %u shard 0x%lx vip group %d state %s lease renew meta_seqno %u lease len %lu seconds
21380 cr_shard %p node %u shard 0x%lx vip group %d recovered
21381 cr_shard %p node %u shard 0x%lx vip group %d notify complete
21382 cr_shard %p node %u shard 0x%lx vip group %d put_meta complete: %s
21383 cr_shard %p node %u shard 0x%lx vip group %d recovered status %s output '%s'
21384 cr_key_lock %p node %u shard 0x%lx vip group %d %s lock key %*.*s %s
21385 cr_shard_key_lock %p node %u shard 0x%lx vip group %d %s unlock key %*.*s
21386 cr_shard_key_lock %p node %u shard 0x%lx vip group %d free
21387 cr_shard_key_lock %p node %u shard 0x%lx vip group %d %s lock key %*.*s granted
21388 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u state from %s to %s
21389 node %u shard 0x%lx vip group %d replica %d local to node %u set to RECOVERED state
21390 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u mark_failed failed: %s
21391 c_replicar %p node %u shard 0x%lx vip group %d replica %d local to node %u get by cursor from node %u
21392 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u get iteration cursors from node %u
21393 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u eof
21394 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u %d cursors
21395 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u status %s
21396 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u get by cursor returned in non-iterating, non-shutdown state %s
21397 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u shard state %s replica state %s get by cursor failed status %s
21398 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u get by cursor returned seqno %llu key %*.*s %sdata len %u status %s
21399 cr_replica %p node %u shard 0x%lx vip group %d replica %d local to node %u get by cursor returned in unhandled state %s
21400 cr_replica %p node %u shard 0x%lx vip group %d redo replica at node %u to node %u op %s seqno %llu key %*.*s data len %u
21401 cr_replica %p node %u shard 0x%lx vip group %d redo replica at node %u to cr_replica %p node %u op %s seqno %llu key %*.*s data len %u status %s
21402 cr_recovery_op %p node %u shard 0x%lx vip group %d undo replica at node %u get by cursor results  msg %s seqno %llu key %*.*s data len %u allocated
21403 cr_recovery_op %p node %u shard 0x%lx vip group %d undo replica at node %u seqno %llu tombstone %d key %*.*s lock %s
21404 cr_recovery_op %p node %u shard 0x%lx vip group %d undo replica at node %u authoritative get results  msg %s seqno %llu tombstone %d key %*.*s data len %u status %s
21405 cr_recovery_op %p node %u shard 0x%lx vip group %d undo replica at node %d put  msg %s key %*.*s data len %u
21406 cr_recovery_op %p node %u shard 0x%lx vip group %d undo replica at node %u put results  msg %s seqno %llu key %*.*s data len %u status %s
21407 cr_recovery_op %p node %u shard 0x%lx vip group %d undo  replica at node %d complete  status %s
21408 cr_replica %p node %u shard 0x%lx vip group %d refcount 0
21409 Unsupported message type (msgtype = %s)
21410 cr_op %p node %u shard 0x%lx start Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d sending to %s
21411 copy_replicator %p op %p got meta
21412 copy_replicator %p op %p %d replicas
21413 copy_replicator %p op %p flash response received Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d
21414 copy_replicator %p node %u op %p seqno %llu request done status %s
21415 copy_replicator %p op %p flash_multi
21416 copy_replicator %p op %p flash request to node %lu failed: %s
21417 copy_replicator %p op %p flash request %s to node %lu start
21418 copy_replicator %p op %p flash request %s node %d SDF_MSG_ERROR received status %s
21419 copy_replicator %p op %p flash request %s node %d response received Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d status %s
21420 ===============> RESPONSE node_id %"PRIu32" response=%s, mkey=%s
21421 copy_replicator %p op %p create shard nreplica = %d
21422 copy_replicator %p op %p create shard stage %s -> %s
21423 error reading meta_data %s: %s %s
21424 error reading vip meta: %s %s
21425 rms %p allocated
21426 rms alloc failed
21427 rms %p node %u start
21428 rms %p node %u shutdown
21429 rms %p node %u create shard 0x%lx vip group %d
21430 rms %p node %u get shard 0x%lx
21431 rms %p node %u put shard 0x%lx seqno %ld
21432 rms %p node %u delete shard 0x%lx seqno %ld
21433 rms %p node %u shard 0x%lx op %p request to node %u Msg: %s (%s)
21434 rms %p node %u reput shard 0x%lx vip group %d
21435 rms %p node %u reput shard 0x%lx vip group %d: %s
21436 rms %p node %u shutdown shard 0x%lx
21437 rms %p node %u shard 0x%lx ref count zero state=%s
21438 rms %p node %u shard 0x%lx op %p flash request Msg: %s (%s) shard 0x%lx key %*.*s data_size %lu
21439 rms %p node %u shard 0x%lx op %p flash response received error status %s
21440 rms %p node %u shard 0x%lx op %p flash response received Msg: %s (%s) status %s
21441 rms  %p shard 0x%lx get failed to unmarshal status %s
21442 rms %p node %u shard 0x%lx notify node %u Msg: %s (%s)
21443 rms %p node %u shard 0x%lx timeout event exists but not lease, waiting for event delivery at %s
21444 rms %p node %u shard 0x%lx vip group %d lease expires in %3.1f seconds at %s
21445 rms %p node %u shard 0x%lx vip group %d lease expired home was %d
21446 rms %p node %u shard 0x%lx vip group %d lease expired home was %d but exists with %ld usecs remain
21447 rms_shard %p node %u node %u dead
21448 rms %p node %u shard 0x%lx pilot beacon
21449 rms %p node %u shard 0x%lx put with no lease but lease exists for node %u
21450 rms %p node %u shard 0x%lx put lease exists request home %d current %d
21451 rms %p node %u shard 0x%lx put non-causal seqno %lld current %lld
21452 rms %p node %u shard 0x%lx put new home bad ltime  request %lu current %lu
21453 rms %p node %u op %p %s complete status %s shard 0x%lx vip group %d home %d lease sec %3.1f elapsed %ld usec meta seqno %lld ltime %lld
21454 rms %p node %u op %p %s complete status %s
21455 rms %p node %u shard 0x%lx respond to node %u Msg: %s (%s) response %s (%s) status %s
21456 rms %p node %u shard 0x%lx op %p meta response from node %u SDF_MSG_ERROR status %s
21457 rms %p node %u shard 0x%lx op %p meta response from node %u Msg: %s (%s) status %s current home node %d meta seqno %lld ltime %lld
21458 rms %p node %u shard 0x%lx op %p meta response from node %u Msg: %s (%s) status %s
21459 Can't bind bind node %u service %u
21460 replicator_adapter %p allocated
21461 sdf_replicator_adapter_alloc failed
21462 replicator adapter %p starting
21463 replicator adapter %p scheduler failed to start
21464 replicator adapter %p replicator failed to start
21465 replicator adapter %p started
21466 replicator adapter %p failed to start
21467 replicator_adapter %p shutdown called
21468 replicator_adapter %p replicator stopped
21469 replicator_adapter %p scheduler stopped
21470 replication_adapter %p send Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d
21471 replication_adapter %p node %u send failed
21472 failed to allocate queue pair local %x:%x remote %x:%x
21473 get last sequence number failed
21474 get cursors failed
21475 get op_meta failed
21476 get by cursor failed
21477 send completed to last_seqno sync
21478 send completed to get cursors
21479 send completed to get by cursor
21480 get sequence number failed
21481 rr iteration test send Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d
21482 Unable to allocate mem for vip_group \n
21483 Unable to allocate mem pref nodes\n
21484 Unable to alloccate memory for vip_group_id_array\n
21485 \nNode %d: what action-how %s mbox %p\n
21486 \nNode %d: Alias replymsg %p req wrapper %p rep dn %d rep ds %d ret sn %d ret ss %d\n        ret response_mkx %p ret mkeyflags %lx\n        mkey %lx\n
21487 \nNode %d: Request MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n        mkey %lx\n
21488 \nNode %d: Response MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n        mkey %lx\n
21489 \nNode %d: One-way MSG %p wrapper %p dn %d ds %d sn %d ss %d type %d flags 0x%x\n        mkey %lx\n
21490 \nNode %d: MSG Freeing %p dn %d ds %d sn %d ss %d type %d len %d\n
21491 %p - %s
21492 PROP: SDF_CMC_BUCKETS=%d
21493 Failed to update meta data map list
21494 %p - TL Update: %s
21495 %p - map %p - key %s - CACHED: %p
21496 ********CMC RECOVER********
21497 ********CMC INITIALIZE********
21498 Node: %d
21499 PROP: SDF_META_TYPE=%d
21500 \nNode : Failed status = %d...\n
21501 Node: %d - %s\n
21502 Incompatible version of SDF_container_meta_blob_t\n
21503 Incompatible version of SDF_container_meta_t\n
21504 Node: %d - %s
21505 %s - cguid: %llu\n
21506 %llu - %s
21507 %llu
21508 Failed to get %s
21509 CACHED - %llu - %s
21510 Invalid CMC_TYPE!
21511 %s - %s
21512 remove from TL %llu
21513 remove from global map %llu
21514 remove from flash %llu
21515 Invalid cmc_type!
21516 %llu - %s - %s
21517 entry
21518 Unsupported operation!
21519 Failed to allocate memory for hash key
21520 Failed to replace meta hash entry
21521 %p - REPLACE: %s - %p
21522 %p - INSERT: %s - %p
21523 Status: %s
21524 Shared Memory Stats:\n Initial: allocated_bytes=%"PRIu64", allocated_count=%"PRIu64", used_bytes=%"PRIu64"
21525 \n Ending : allocated_bytes=%"PRIu64", allocated_count=%"PRIu64", used_bytes=%"PRIu64"
21526 \n Diff   : allocated_bytes=%"PRIu64", allocated_count=%"PRIu64", used_bytes=%"PRIu64"\n
21527 Container: %s - Multi Devs: %d
21528 Container: %s - Single Dev
21529 Container: %s - Num Shards: %d
21530 Container: %s - Num Objs: %d
21531 Container: %s - DEBUG_MULTI_SHARD_INDEX: %d
21532 failed to lock %s
21533 failed to unlock %s
21534 failed to map cguid: %s
21535 cname=%s, build_shard failed
21536 cname=%s, createParentContainer() failed
21537 cname=%s, build_meta failed
21538 cname=%s, function returned status = %u
21539 name_service_put_meta failed for cguid %"PRIu64"
21540 %s - failed to flush and invalidate container
21541 %s - flush and invalidate container succeed
21542 %s - failed to delete action thread container state
21543 %s - action thread delete container state succeeded
21544 %s - failed to delete container shards
21545 %s - delete container shards succeeded
21546 %s - failed to remove metadata
21547 %s - remove metadata succeeded
21548 %s - failed to unlock metadata
21549 %s - unlock metadata succeeded
21550 %s - failed to remove cguid map
21551 %s - remove cguid map succeeded
21552 Delete pending for %s
21553 set cid %d (%d) to cguid %d\n
21554 SDFActionOpenContainer failed for container %s
21555 Opened %s
21556 Failed to find %s
21557 Enumerate: %s - %u
21558 FAILURE: SDFCreateContainer - build meta
21559 create_put_meta: %s - %lu
21560 FAILURE: create_put_meta - create %s
21561 SUCCESS: create_put_meta - put %s
21562 create_put_meta: %d\n
21563 Container parent descriptor [blockSize=%u, container_type=%u, dir=%s, cguid=%lu, name=%s, num_open_descr=%u, ptr_open_descr=TODO]\n
21564 Container descriptor [mode=%u, ptr_next=TODO, ptr_parent=TODO, ptr_void=TODO]
21565 create shard %lu on node %u
21566 create shard %lu on node %u failed to send message
21567 create shard %lu on node %u received error %s
21568 create shard %lu on node %u succeeded
21569 create shard %lu on node %u failed
21570 delete shard %lu on node %u
21571 delete shard %lu on node %u failed to send message
21572 delete shard %lu on node %u received error %s
21573 delete shard %lu on node %u succeeded
21574 delete shard %lu on node %u failed
21575 STMDBG: Create META shard id:%lx  metasguid:%lx CGUID:%lu\n
21576 STMDBG: Sh. Create id:%lx metasguid:%lx CGUID:%lu FAILED\n
21577 build_shard container_type.type %d num_objs %d num_replicas %d
21578 FAILURE: SDFCreateContainer - %d shard
21579 %s - %lu
21580 generate_cguid: %llu
21581 generated shardids 0x%lx (+ %u reserved) for  cguid %lu
21582 FAILED: container name exceeds max
21583 metadata created for %s
21584 could not allocate memory
21585 invalid parameter
21586 SUCCESS: container_meta_destroy
21587 FAILED: container_meta_destroy
21588 FAILURE: get_object - flashget
21589 SUCCESS: get_object - flashget
21590 FAILURE: put_object - memory allocation
21591 FAILURE: put_object - flashPut
21592 SUCCESS: put_object - flashPut
21593 FAILURE: get_block - flashBlockRead
21594 SUCCESS: get_block - flashBlockRead
21595 FAILURE: put_block - flashBlockWrite
21596 SUCCESS: put_block - flashBlockWrite
21597 FAILURE: object_exists - failed to get shard
21598 FAILURE: put - invalid parm
21599 FAILURE: put - could not find shard
21600 FAILURE: put - unknown container type
21601 FAILURE: SDFGet - invalid parm
21602 FAILURE: SDFGet - could not find shard
21603 FAILURE: SDFGet - unknown container type
21604 CMC create succeeded
21605 CMC create failed
21606 Did not find cguid counter state - re-initializing counter
21607 Failed to write cguid state
21608 Failure: Node %u - init_set_cmc_cguid - failed to map cguid %s - status %u
21609 Success: Node %u - init_set_cmc_cguid
21610 %s - %llu - %s
21611 cguid: %lu (0x%lx) - shard: %lu (0x%lx)\n objkey: %s  status: %s
21612 %lu - %lu
21613 %lu - %lu - %s
21614 %s - %llu
21615 %llu - %u - %s
21616 %llu - NULL - %s
21617 Container %s exists in backend
21618 Container %s does not exist in backend
21619 failed to get meta - %s
21620 delContainerDescriptor: invalid path
21621 delContainerDescriptor: failed to find parent for %s
21622 createParentContainer: container exist for %s in map
21623 NODE/GUID for %s = %lu/%llu
21624 failed to add map for %s
21625 successfully added map for %s
21626 closeParentContainer: failed to close %s
21627 closeParentContainer: closed parent
21628 closeParentContainer: failed to remove parent for %s
21629 closeParentContainer: closed descriptor for %s
21630 %lu
21631 SUCCESS: testCreateBlockContainer() #1
21632 FAILED: testCreateBlockContainer() #1
21633 SUCCESS: testCreateBlockContainer() #2
21634 FAILED: testCreateBlockContainer() #2
21635 FAILED: testCreateBlockContainer() #3
21636 SUCCESS: testCreateBlockContainer() #3
21637 SUCCESS: delete block container
21638 FAILURE: delete block container
21639 FAILED: testOpenBlockContainer() #1
21640 SUCCESS: testOpenBlockContainer() #1
21641 SUCCESS: testGetPutBlockContainer - put block %d
21642 FAILED: testGetPutBlockContainer - get block %d
21643 SUCCESS: testGetPutBlockContainer - get block %d
21644 SUCCESS: test container create
21645 FAILURE: test container create
21646 SUCCESS: delete container
21647 FAILURE: delete container
21648 FAILURE: Open Container - %s
21649 FAILURE: Close Container - %s
21650 SUCCESS: Open/Close Container - %s
21651 FAILURE: SDFCreate
21652 FAILURE: SDFPut
21653 FAILURE: SDFGet
21654 SUCCESS: SDF init
21655 FAILED: SDF init
21656 FAILURE: flashPut
21657 FAILURE: flashGet
21658 Could not open SSD file: %s - %s
21659 flash_offset is not an even multiple of the page size!
21660 pages per slab (%"PRIu64") is too large--forcing to %"PRIu64"!
21661 adjusted_size > required_size!
21662 Problem computing buckets_per_slab
21663 n_bucket_bits is too large!
21664 Number of buckets is not a power of 2.
21665 n_bucket_bits is too large (%d is max)!
21666 plat_alloc failed in clipperInit for bucket allocation!
21667 plat_alloc failed in clipperInit for slab allocation!
21668 plat_alloc failed in clipperInit for entry allocation!
21669 pagesize=%d, npages=%"PRIu64", pages/slab=%"PRIu64", nslabs=%"PRIu64", slabsize=%"PRIu64", nbuckets=%"PRIu64"
21670 n_syndrome_bits=%d, n_bucket_bits=%d
21671 requested ssd size = %"PRIu64", adjusted ssd size = %"PRIu64"
21672 Shard configured as DATA STORE.
21673 Shard configured for EVICTION.
21674 plat_alloc failed in flashOpen!
21675 Failure initializing aio subsystem: devName='%s', rc=%d
21676 Insufficient space on flash in shardCreate!  Reducing quota from %"PRIu64" to %"PRIu64"
21677 plat_alloc failed in shardCreate!
21678 object is not entirely contiguous!
21679 key + metadata take up more than one page!
21680 Object requires too many pages (%d)!
21681 Could not find a page entry in the bucket it refers to!
21682 Contiguous region with too many pages (%"PRIu64")!
21683 read_flash failed (rc=%d).
21684 CLIPPER get_object: key='%s', ice=%"PRIu64", npages=%d, pmeta->databytes=%d, pmeta->npages_used=%d, pmeta->npages_actual=%d
21685 CLIPPER put_object: key='%s', ice=%"PRIu64", npages_new=%d, pmeta->databytes=%d, pmeta->npages_used=%d, pmeta->npages_actual=%d
21686 write_flash failed (rc=%d).
21687 ClipperStartEnumeration is not currently supported!
21688 ClipperEndEnumeration is not currently supported!
21689 ClipperNextEnumeration is not currently supported!
21690 ClipperRemove is not currently supported!
21691 ENTERING, devName=%s
21692 failed to alloc dev
21693 failed to alloc aio state
21694 failed to init aio
21695 dev size is %lu
21696 fifo_flashOpen not implemented!
21697 ENTERING, shardID=%lu max_nobjs=%u
21698 fifo_shardCreate not implemented!
21699 fifo_shardOpen not implemented!
21700 fifo_flashGet not implemented!
21701 fifo_flashPut not implemented!
21702 fifo_flashFreeBuf not implemented!
21703 fifo_flashStats not implemented!
21704 fifo_shardSync not implemented!
21705 fifo_shardDelete not implemented!
21706 fifo_shardStart not implemented!
21707 fifo_shardStop not implemented!
21708 fifo_flashGetHighSequence not implemented!
21709 fifo_flashSetSyncedSequence not implemented!
21710 fifo_flashGetIterationCursrors not implemented!
21711 fifo_flashGetByCursor not implemented!
21712 fifo_flashGetRetainedTombstoneGuarantee not implemented!
21713 fifo_flashRegisterSetRRTGCallback not implemented!
21714 htf_flashOpen not implemented!
21715 htf_shardCreate not implemented!
21716 htf_flashGet not implemented!
21717 htf_flashPut not implemented!
21718 aio_init not implemented!
21719 aio_init_context not implemented!
21720 aio_blk_read not implemented!
21721 aio_blk_write not implemented!
21722 Invalid SSD scheme (%s)!
21723 Invalid SSD scheme (%d)!
21724 flashGet is not yet implemented!
21725 flashPut is not yet implemented!
21726 flashEnumerate is not yet implemented!
21727 flashOpen is not yet implemented!
21728 shardCreate is not yet implemented!
21729 shardOpen is not yet implemented!
21730 shardDelete is not yet implemented!
21731 shardSync is not yet implemented!
21732 shardStart is not yet implemented!
21733 shardStop is not yet implemented!
21734 flashClose is not yet implemented!
21735 shardFree is not yet implemented!
21736 getNextShard is not yet implemented!
21737 setLRUCallback is not yet implemented!
21738 flashRegisterSetRTGCallback is not yet implemented!
21739 In HashMap_create()
21740 Incorrect arguments in sdf/shared/HashMap_create(numBuckets=%u, lockType=%u)
21741 In HashMap_destroy()
21742 In HashMap_put(%s)
21743 argument map=%p
21744 In LinkedList_destroy()
21745 In LinkedList_printElements()
21746 In LinkedList_put(%s)\n
21747 LinkedList_put() - key[%s] already present\n
21748 In LinkedList_remove(), key is NULL *****\n
21749 In LinkedList_remove(%s), list is empty *****\n
21750 In LinkedList_remove(%s), could not find, sz of list =%d *****\n
21751 In LinkedList_get(), key is NULL *****\n
21752 In LinkedList_get(%s), list is empty *****\n
21753 In LinkedList_get(%s), could not find *****\n
21754 unloaded properties
21755 Reading properties file '%s'
21756 Reading properties file '%s' has an error!\n
21757 Parsed property error (ret:%d)('%s', '%s')
21758 Parsed property ('%s', '%s')
21759 cr_shard %p node %u shard 0x%lx vip group %d home node %u found
21760 cr_shard %p node %u shard 0x%lx vip group %d preferred node %u not found
21761 cr_shard %p node %u shard 0x%lx vip group %d preferred node %u dead
21762 cr_shard %p node %u shard 0x%lx vip group %d preferred node %u live but home still %u
21763 supressed - default
21764 Diagnostic %d %d %d
21765 Debug %s
21766 test_count val=%d
21767 test_done
21768 fth scheduler starting
21769 fth scheduler stopped
21770 Parse options failed.
21771 Initialization of Test Registry failed.
21772 Tests completed with return value %d.
21773 can't determine exe path for starting children
21774 fork, execve failed: %s
21775 Child %d started
21776 Child %d exited with status %d
21777 Child %d terminated with signal %s%s
21778 %d children terminated
21779 timer fired at %d seconds
21780 timer_dispatcher_main out of loop
21781 ref count 0
21782 test_closure_scheduler_shutdown
21783 freeing dispatcher
21784 dispatcher free
21785 timer %p time %d now %d created
21786 timer %p time %d now %d free
21787 timer %p time %d now %d fired
21788 Initialized FFDC logging (max buffers=%d, thread bufsize=0x%lx)
21789 Unable to initialize FFDC logging (max buffers=%d, thread bufsize=0x%lx)
21790 short string %s long string %"FFDC_LONG_STRING(100)"
21791 cr_shard %p node %u shard 0x%lx vip group %d current seqno %lld proposed %"FFDC_LONG_STRING(512)"
21792 cr_shard %p node %u shard 0x%lx vip group %d lease expires %s meta now %"FFDC_LONG_STRING(512)"
21793 in child process
21794 plat/shmem/file path %s does not match header %s
21795 Error creating path (%s), returned (%s)
21796 replicator_key_lock %p node %u shard 0x%lx vip group %d %s unlock key %*.*s
21797 replicator_key_lock %p node %u shard 0x%lx vip group %d %s lock key %*.*s granted
21798 replicator_key_lock %p node %u shard 0x%lx vip group %d free
21799 cr_shard %p node %u shard 0x%lx vip group %d lease not allowed due to %d others hosted
21800 cr_shard %p node %u shard 0x%lx vip group %d lease not allowed since preferred group not hosted
21801 cr_shard %p node %u shard 0x%lx vip group %d lease allowed
21802 unlink(%s) failed: %s
21803 mmap(%s) failed: %s
21804 setup context
21805 op 1 write offset 0 len 4096
21806 op 1 write offset 0 len 4096 started
21807 op 1 write offset 0 len 4096 complete
21808 op 2 read offset 0 len 4096
21809 op 2 read offset 0 len 4096 started
21810 op 2 read offset 0 len 4096 complete
21811 destroy context
21812 paio_api %p destroy
21813 paio_api %p setup paio_context %p maxevents %d = %d
21814 paio_context %p destroy
21815 paio_context %p submit %d of %ld iocb %p op %d fd %d buf %p nbytes %ld offset %lld
21816 paio_context %p submit %d of %ld iocb %p op %d fd %d
21817 paio_context %p submit nr %ld write ios %ld bytes %ld = %d
21818 paio_context %p cancel iocb %p op %d fd %d  = %d
21819 paio_context %p getevents min_nr %ld nr %ld write ios %ld bytes %ld = %ld
21820 paio_context %p complete iocb %p op %d fd %d buf %p nbytes %ld offset %lld res %ld res2 %ld
21821 paio_context %p complete iocb %p op %d fd %d res %ld res2 %ld
21822 write_read op 1 write offset 0 len 4096
21823 write_read op 1 write offset 0 len 4096 started
21824 write_read op 1 write offset 0 len 4096 complete
21825 write_read op 2 read offset 0 len 4096
21826 write_read op 2 read offset 0 len 4096 started
21827 write_read op 2 read offset 0 len 4096 complete
21828 %s total batch %d count %d start %ld op %s current batch %d count %d offset %ld len %ld
21829 total batch %d count %d start %ld op %s started current batch %d count %d offset %ld len %ld
21830 Requested op kind %s offset %d len %d conflicts with existing kind %s offset %d len %d seqno %d
21831 Requested op kind %s offset %d len %d failed %s
21832 paio_wc_context %p submit write fd %d buf %p nbytes %ld offset %lld from %d user ios inflight ios %d bytes %ld = %d %s
21833 paio_wc_context %p user iocb %p short write %lu of %lu
21834 paio_setup failed: %s
21835 event %p name '%s' scheduling impl_free ref_count now %d
21836 rms %p node %u shard 0x%lx vip group %d mmsmc get from node %u not meta write node %u
21837 rms %p node %u shard 0x%lx skip notify node %u (no meta)
21838 rms %p node %u shard 0x%lx skip notify node %u (local version is reput)
21839 cr_shard %p node %u shard 0x%lx vip group %d lease not allowed since sguid not assigned
21840 cr_shard %p node %u shard 0x%lx vip group %d lease not allowed since shard_meta is unset
21841 cr_shard %p node %u shard 0x%lx vip group %d lease not allowed since current home is %u
21842 cr_shard %p node %u shard 0x%lx vip group %d lease allowed with preference %d preffered hosted %d
21843 cr_shard %p node %u shard 0x%lx vip group %d delay %ld secs %lu usecs before lease request  with preference %d of %d
21844 cr_shard %p node %u shard 0x%lx vip group %d initial preference %d of %d
21845 cr_shard %p node %u shard 0x%lx vip group %d normal preference %d of %d
21846 cr_shard %p node %u shard 0x%lx non-vip group preference %d of %d
21847 Failed to trim phys_map: %s
21848 Failed to trim virt_map: %s
21849 replicator_key_lock %p node %u shard 0x%lx vip group %d %s lock key %*.*s %s
21850 plat_free(%p) from %p
21851 plat_alloc(%llu) returned %p from %p
21852 rms %p node %u shard 0x%lx op %s lease exists request home %d current %d converting to put with no lease
21853 cr_shard %p node %u shard 0x%lx vip group %d home %d lease len %3.1f expires %s ignored from %d pending puts
21854 rms %p node %u shard 0x%lx op %s allowed shard meta current home %d new new home %d lease sec %3.1f meta seqno %lld ltime %lld
21855 rms %p node %u shard 0x%lx op %s allowed  reason '%s' shard meta current home %d new new home %d lease sec %3.1f meta seqno %lld ltime %lld
21856 rms %p node %u shard 0x%lx op %s allowed  reason '%s' shard meta current home %d new home %d lease sec %3.1f meta seqno %lld ltime %lld
21857 rms %p node %u shard 0x%lx op %s allowed  reason '%s' shard meta current home %d write node %d last home %d new home %d lease sec %3.1f meta seqno %lld ltime %lld
21858 rms %p node %u shard 0x%lx put with no home when no  home exists
21859 rms %p node %u shard 0x%lx put with no home when no home exists
21860 rms %p node %u shard 0x%lx put with no home when home exists
21861 rms %p node %u shard 0x%lx op %s allowed reason '%s' shard meta current home %d write node %d last home %d new home %d lease sec %3.1f meta seqno %lld ltime %lld
21862 split brain detected - this node thinks home is %d remote node %d thinks home is %d
21863 cannot truncate %s: %s
21864 test_error_injection op 1 read offset 0 len 4096
21865 test_error_injection op 1 read offset 0 len 4096 started
21866 test_error_injection op 1 read offset 0 len 4096 complete
21867 test_error_injection op 2 read offset 0 len 4096
21868 test_error_injection op 2 read offset 0 len 4096 started
21869 test_error_injection op 2 read offset 0 len 4096 complete
21870 Not enough memory for cache: must have at least enough memory for a single maximum sized object per slab (%"PRIu64" required, %"PRIu64" available). Try increasing SDF_CC_MAXCACHESIZE or reducing SDF_CC_NSLABS
21871 bdb failed: %d


# briano
30000 slabsize is too large!
30001 plat_alloc failed in ClipperInit for bucket allocation!
30002 plat_alloc failed in ClipperInit for slab allocation!
30003 plat_alloc failed in ClipperInit for entry allocation!
30004 pagesize=%d, npages=%"PRIu64", pages/slab=%d, nslabs=%"PRIu64", slabsize=%d, nbuckets=%"PRIu64"
30005 n_syndrome_bits=%d, n_slabsize_bits=%d, n_bucket_bits=%d
30006 ClipperDestroy is not yet implemented.
30007 ClipperPut is not currently supported!
30008 init_ssd_map: pages per slot must be 256!
30009 init_ssd_map: nsets is too large!
30010 init_ssd_map: not enough bits in a 64 bit hash!
30011 init_ssd_map: nsets = %"PRIu64", assoc=%d, page_size=%d, pages/slot=%d
30012 init_ssd_map: requested ssd size = %"PRIu64", adjusted ssd size = %"PRIu64"
30013 plat_alloc failed in init_ssd_map!
30014 Insufficient space on flash in shardCreate!
30015 MTMapDestroy is not yet implemented.
30016 SDFCacheDestroy is not yet implemented.
30017 err=%d %p
30018 %s err=%d
30019 sync_failed ret=%s\n
30020 flush_failed ret=%s\n
30021 flush_ctnr_finished, cur=%ld-%ld, next to sync_ctnr ret=%s\n
30022 flushctnr failed ret=%s\n
30023 flush_ctnr finished, cur=%ld-%ld, sync_ctnr finished, ret=%s\n
30024 flushinv_failed ret=%s\n
30025 fscanf_err:%s\n
30026 err_ret=%s\n
30027 recovering key_value set\n
30028 set: nobj=%d, nloop=%d, pre_set=%d\n
30029 test: loop=%d, pre_set=%d\n
30030 __ERRORS__\n
30031 trace_updated: ctnr=%d, obj=%d, len=%d\n
30032 done thread=%d\n
30033 sync_key=%s, value=%s\n
30034 ret=%s
30035 key:[%s][%ld]\nvalue NOT match: \n[%s]\n[%s]\nret=%s
30036 exptime NOT match: \n[%"PRIu64"]\n[%"PRIu64"]\nret=%s
30037 max_expiry_delay err=%d %p
30038 min_expiry_delay err=%d %p
30039 err=%d
30040 1. init-check-all: err=%d
30041 2. init-check-all: err=%d
30042 3. init-check-all: err=%d
30043 4. init-check-all: err=%d
30044 5. init-check-all: err=%d
30045 6-1. init-check-all: err=%d
30046 6-2. init-check-all: err=%d
30047 7. init-check-all: err=%d
30048 f1.err=%d %p
30049 f2.err=%d %p
30050 event_add, errno=%d c=%p fd=%d
30051 cleanup failed, c=%p, status=%d
30052 UDP port %d opened
30053 incoming ctx is %ld \n
30054 send_to_client: sending back response to socket path %s\n
30055 rendezvouz_with_client sdf_agent thread running... \n
30056  sdf_msg_engine socket bind done...\n
30057 sdfagent: incoming socket path is %s\n
30058 sdfagent: recvd %d bytes\n
30059 Sent %d Bytes to daemon
30060 Sent %d bytes to daemon
30061 Received shmem queue ptr (%p)\n
30062 Calling rendezvous_with_client() \n ... 
30063 Sending shared-queue address (%p) to client.
30064 ... starting queue_handling threads\n
30065 PROP: SDF_HOME_NODE_THREADS=%u
30066 Node %d: Single Process mode -- No MPI INIT, Number of procs %d
30067 Node %d: MPI init complete, Number of procs %d
30068 Node %d: SINGLE PROCESS MODE with MPI  - Number of procs %d
30069 init_agent_sm SUCCESS = %u
30070 sdf_msg_init = %u
30071 disabled startup messaging engine
30072 PROP: SDF_CC_LOCKTYPE=%"PRIu32"
30073 PROP: SDF_CC_AVG_OBJSIZE=%"PRIu64"
30074 PROP: SDF_CC_MINCACHESIZE=%"PRIu64"
30075 PROP: SDF_ACTION_HOMEMSG_LEX_BUCKETS=%d
30076 get_container_shards was unsuccessful! (sdf_status=%d, shard_count=%d, MAX_SHARD_IDs=%d
30077 flash returned no pshard for shardid %lu
30078 Not enough memory: SDFCacheCreateCacheObject returned NULL.
30079 Not enough memory: createCacheObject returned NULL.
30080 Not enough memory, plat_alloc() failed to allocate stats buffer.
30081 Not enough memory for cache: must have at least enough memory for a single maximum sized object per slab (%"PRIu64" required, %"PRIu64" available). Try increasing SDF_CC_MAXCACHESIZE.
30082 PROP: SDF_ALLOCATOR_MODE=%s
30083 PROP: SDF_FASTCC_PAGE_SIZE=%"PRIu64"
30084 ===========  name_service_get_meta(cguid=%"PRIu64"): meta.flush_time=%"PRIu64" meta.flush_set_time=%"PRIu64" ===========
30085 Replication is not currently supported!
30086 sdf_replicator_get_op_meta failed!
30087 [tag %d, node %d, pid %d, thrd %d] CASTOUT status: %s (%d), %s
30088 [tag %d, node %d, pid %d, thrd %d] CASTOUT: %s
30089 Failed to get container metadata for a castout
30090 Failed to get a directory entry for a castout
30091 Not enough memory for cache, plat_alloc() failed.
30092 Not enough memory for cache: must have at least enough memory for a single maximum sized object per slab (%"PRIu64" required, %"PRIu64" available).
30093 SDF_bufPoolInit failed!
30094 Could not allocation an SDF buffer pool!
30095 plat_alloc returned NULL!
30096 In LinkedDirList_destroy()
30097 In LinkedDirList_print()
30098 In LinkedDirList_printElements()
30099 In LinkedDirList_put()\n
30100 LinkedDirList_put() - key[] already present\n
30101 In LinkedDirList_remove(), key is NULL *****\n
30102 In LinkedDirList_remove(), list is empty *****\n
30103 In LinkedDirList_remove() [1], ret==NULL!\n
30104 In LinkedDirList_remove() [2], ret==NULL!\n
30105 In LinkedDirList_remove(), could not find, sz of list =%d *****\n
30106 In LinkedDirList_get(), key is NULL *****\n
30107 In LinkedDirList_get(), list is empty *****\n
30108 In LinkedDirList_get(), could not find *****\n
30109 Incorrect arguments in sdf/shared/HomeDir_create(numBuckets=%u, lockType=%u)
30110 In HomeDir_create()
30111 In HomeDir_destroy()
30112 In HomeDir_print()
30113 In HomeDir_put()
30114 In HomeDir_put()\n
30115 In HomeDir_put() failed *****\n
30116 In HomeDir_remove()
30117 When time came to remove object from the HomeDir [1], its not found!
30118 When time came to remove object from the HomeDir[2], its not found!
30119 HomeDir_remove()\n
30120 HomeDir_remove() failed *****\n
30121 In internal_HomeDir_get()
30122 HomeDir_get()\n
30123 HomeDir_get() failed *****\n
30124 In HomeDir_get_create()
30125 In HomeDir_get()
30126 name_service returned no shard for cguid %llu key %s
30127  invalid shard 0x%llx for cguid %llu key %s
30128 node: %d - cguid - 0x%llx - shard 0x%lx\n
30129 plat_alloc returned NULL
30130 flash returned no pshard for cwguid %llu key %s shardid %lu
30131 get_last_sequence sguid 0x%lx flags %X quota %llu numObjs %d
30132 PROP: SDF_HOME_ACTIONMSG_LEX_BUCKETS=%d
30133 received Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d
30134 flash returned no pshard for cguid %llu key %s shardid %lu
30135 Invalid message type: %d
30136 No room in flash for a flush! (flash return code = %d)
30137 No room in flash for a flush (deleting object instead)! (flash return code = %d)
30138 Flash delete failed for a failed flush! (flash return code = %d)
30139 No room in flash for a castout! (flash return code = %d)
30140 No room in flash for a castout (deleting object instead)! (flash return code = %d)
30141 We are now toast: flash delete failed for a failed castout! (flash return code = %d)
30142 homedir_actions Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d, Ptodo Actions:%s Msg: %s
30143 sdf_msg_alloc returned NULL
30144 Warning: 
30145 copy_replicator %p refcount 0
30146 copy_replicator %p closure scheduler stopped
30147 copy_replicator %p op %p start Msg: %s (%s), V%d(%s) -> V%d(%s), Tag:%d sending to %s
30148 copy_replicator %p op %p request done
30149 error reading meta_data %s: %s
30150 request_state:mbox:%p, sync_box:%p, cr_shard_meta:%p\n
30151 alloc test_node failed!
30152 alloc framework %p successfully
30153 test_framework %p node enumerate===
30154 test_node:%p, mailbox:%p
30155 alloc framework failed
30156 Cannot find node:%d
30157 get node %u from nodelist of test_framework
30158 get node %u error
30159 header node of test_framework null!
30160 start all nodes successfully
30161 start all nodes failed
30162 got a reponse_msg %p
30163 mbx:%p
30164 ALLOCATE wrapper :%p
30165 Cannot find node %d from framework
30166 write a key:%.*s, pm_key:%.*s, key_len:%d, strlen(key):%d
30167 get container meta failed
30168 Can't find node node_id:%d
30169 get container_meta failed
30170 illegal or crashed node:%d
30171 sync read failed
30172 container meta null
30173 rtfw receive wrapper:%p
30174 illegal src_vnode:%d, dest_vnode:%d, nnode:%d
30175 No destination node: %d
30176 create shard_meta ok
30177 request_state:mbox:%p, sync_box:%p
30178 shard :%"PRIu64"exist!
30179 create container meta success
30180 memory allocate error
30181 del container meta:%p, shard_id:%"PRIu64"
30182 del container meta success
30183 time now %ld secs %ld usecs
30184 send completed to sync
30185 async user op completed
30186 rtfw shutdown async completed
30187 rtfw shutdown sync completed
30188 Do nothing
30189 rtfw block timer fired
30190 rtfw at timer fired
30191 node_id %lld allocated at %p
30192 node_id %lld alloc failed
30193 test_node alloc failed, temporary scheduler to shutdown
30194 flash %p started
30195 flash %p start failed
30196 failed to allocate replicator test_node %p id %"PRIu32"
30197 replicator started %p
30198 node %lld replicator start failed
30199 node_id %lld crash completed with status %s
30200 node_id %lld shutdown
30201 NOW:usec:%lu, sec:%lu, nw_ltcy:%"PRIu32"
30202 node_id %lld %s msg type %s src %lld:%s dest %lld:%s mkey %s
30203 node_id %lld error %s
30204 node_id %lld protocol msgtype %s
30205 node_id %lld key %s
30206 node_id %lld invalid protocol message type %d
30207 \n************************************************************\n                  Test framework shutdown                       \n************************************************************
30208 data:%s, data_len:%d
30209 \n***************************************************\n                  delete object async                 \n***************************************************
30210 write completed
30211 \n**************************************************\n                  read object async                  \n**************************************************
30212 \n**************************************************\n                  create shard sync                 \n**************************************************
30213 \n**************************************************\n                 write object async                  \n**************************************************
30214 test_framework %p allocated\n
30215 start test_framework
30216 test_framework started\n
30217 smt get response
30218 JOIN
30219 \n**************************************************\n                 write object sync                  \n**************************************************
30220 write key:%s, key_len:%u, data:%s, data_len:%u
30221 \n**************************************************\n                  read object sync                  \n**************************************************
30222 KEY:%s, key_len:%d
30223 read data:%s, data_len:%d
30224 \n**************************************************\n                  crash node sync                  \n**************************************************
30225 crash node:%"PRIu32" complete
30226 \n***************************************************\n                  delete object sync                 \n***************************************************
30227 \n**************************************************\n                   delete shard sync                \n**************************************************
30228 \n************************************************************\n                  Test framework sync summary                 \n************************************************************
30229 \ncreat_shard :%d, Success:%d\nwrite_sync  :%d, Success:%d\nread_sync   :%d, Success:%d\ndelete_sync :%d, Success:%d\ndelete_shard:%d, Success:%d\n
30230 ./%s --timeout us_sec
30231 remaining_us:%d, target timeout:%d
30232 smt_send_msg_api_cb, send msg callback function
30233 allocate sdf_msg_timeout_test error
30234 request_state:%p
30235 timeout usecs specified:%d
30236 send message closure out successfully
30237 timeout expired!  Kill scheduler pthread
30238 count:%"PRIu64"
30239 got usecs %d
30240 KILLED
30241 put shard meta failed on node:%"PRIu32"
30242 put shard meta success on node:%"PRIu32"
30243 get shard meta failed on node:%"PRIu32"
30244 get shard meta success on node:%"PRIu32"
30245 rank:%d
30246 start sdf_msg sucessfully!
30247 start sdf_msg failed!
30248 node %p %"PRIu32" allocated
30249 framework %p allocated
30250 send a mbox to node %"PRIu32"
30251 \n output sdfmsg:\t %d %s
30252 node fthread worker %p allocated
30253 node %"PRIu32"started
30254 test mode should be \"async\" or \"sync
30255 redundant input: %s
30256 invalid input: %s
30257 no input file
30258 can not open file %s.
30259 reading %s
30260 duplicated op id %d found in %s:%d
30261 a complete %d without start found in %s:%d
30262 unexpected complete %d in %s:%d
30263 [PASS]
30264 [FAILED]
30265 invalid line %d: %s
30266 [ABORTED]
30267 node_id %"PRIu32" replication test flash %p allocated.
30268 node_id %"PRIu32" replication test flash %p failed.
30269 node_id:%"PRIu32" shutdown flash impli
30270 node_id %"PRIu32" crash completed
30271 node_id %"PRIu32" shutdown flash successfully!
30272 node %lld rtn_start_impl
30273 node %lld skipping start: state %s not RTN_STATE_DEAD
30274 node_id %"PRIu32" crash flash
30275 node_id:%"PRIu32" crash flash impl
30276 node_id %"PRIu32" fake Func:%s()  
30277 node_id %"PRIu32" Hashmap get key %.*s not exist
30278 node_id %"PRIu32" Hashmap put key %.*s ok
30279 node_id %"PRIu32" Hashmap put key %.*s exists
30280 node_id %"PRIu32" Hashmap replace key %.*s okpm->data_size:%d, f_entry->data_size:%d
30281 node_id %"PRIu32" Hashmap delete key:%s 
30282 node_id %"PRIu32" Hashmap delete key %s 
30283 node_id %"PRIu32" Hashmap delete key %.*s not exist
30284 node_id %"PRIu32" shard not existed, shard_id:%"PRIu64"
30285 node_id %"PRIu32" shard %"PRIu64" existed 
30286 node_id %"PRIu32" create shard %"PRIu64" successfully
30287 node_id %"PRIu32" shard %"PRIu64" not existed 
30288 node_id %"PRIu32" delete shard %"PRIu64"sucsessfully
30289 node_id %"PRIu32" delete shard %"PRIu64" failed
30290 node_id %lld NOW:usec:%lu, sec:%lu, nw_ltcy:%"PRIu32"
30291 node_id %"PRIu32" send msg
30292 inbound queue empty in node:%"PRIu32" chip:%"PRIu32"
30293 send msg response to %"PRIu32"
30294 node_id %"PRIu32" Unsupported meesage type
30295 node_id %"PRIu32" shard_id:%"PRIu64", temp_shard:%p
30296 node_id %"PRIu32" create shard:%"PRIu64"
30297 node_id %"PRIu32" allocate flash shard error.
30298 reply wrapper failure
30299 allocate wrapper:%p
30300 node_id %"PRIu32" shard:%p, shard_id:%"PRIu64"
30301 node_id %"PRIu32" mbox:%p
30302 alloc framework %p successfully type %s
30303 start node %d successfully
30304 start node %d failed
30305 get node:%d shard_id:%d container_meta failed 
30306 msg_wraper:%p illegal src_vnode:%d, dest_vnode:%d, nnode:%d
30307 Illegal service type:%d
30308 free closure scheduler
30309 node_id:%"PRIu32" crashed!
30310 Probability error
30311 max number of keys: %d
30312 Test generator allocated
30313 Test generator destroyed
30314 Test generator configure initialized, mode = %s, seed = %d, max_parallel = %d, iterations = %d, work_set_size = %d
30315 loading operation %d, ruuning %d, remaining %d, generator %p
30316 calling rtfw_create_shard_async shard_id %d op_id %d over
30317 calling rtfw_delete_shard_async shard_id %d op_id %d over
30318 calling rtfw_crash_node_async op_id %d
30319 calling rtfw_read_async op_id %d over
30320 shard_id %d, node_id %d, key %.*s
30321 rtfw_read_async returned
30322 calling rtfw_write_async op_id %d over
30323 key:%.*s, key_len:%d
30324 data: %.*s
30325 calling rtfw_delete_async op_id %d over
30326 shutdown test framework in ASYNC mode
30327 shutdown complete
30328 Starting test framework in ASYNC mode
30329 pre-seeding calling rtfw_create_shard_sync
30330 calling rtfw_create_shard_sync shard_id %d
30331 Starting test framework in SYNC mode
30332 op type: %s
30333 calling rtfw_delete_shard_sync shard_id %d
30334 calling rtfw_crash_node_sync
30335 calling rtfw_read_sync shard_id %d, node_id %d, key %s, key_len %d
30336 rtfw_read_sync returned
30337 calling rtfw_write_sync
30338 key:%s, key_len:%d, strlen(key):%d
30339 rtfw_write_sync returned
30340 calling rtfw_delete_sync
30341 internal error occurred
30342 return status = %s
30343 shutdown test framework in SYNC mode
30344 start random running in SYNC mode
30345 start random running in ASYNC mode
30346 continue
30347 before: %"PRIu64"
30348 after: %"PRIu64"
30349 %"PRIu64" ops remaining at this load
30350 %d ops remaining at this run
30351 calling back env: %p, op type %s, op_id %d over
30352 reached the end of history but stillhave not find a match
30353 value not possible
30354 shard already exists when create start,node:%"PRIu32" shard_id %d.
30355 shard not found when create complete, shard_id %d.
30356 shard not found when delete start,node:%"PRIu32" shard_id %d.
30357 shard not found when delete start,shard_id %d.
30358 shard not found when write start,node:%"PRIu32" shard_id %d.
30359 shard not found when read start,node:%"PRIu32" shard_id %d.
30360 read verification key:%s data:%s successful!
30361 shard not found when delete start, shard_id %d.
30362 node_id %lld failed to allocate closure_scheduler
30363 node_id %lld failed to allocate msg_requests.mmap
30364 node_id %lld failed to allocate flash
30365 node_id %"PRIu32" alloc failed
30366 node_id %lld rtn_shutdown_impl
30367 node_id %lld rtn_start_impl
30368 node_id %lld skipping start: state %s not RTN_STATE_DEAD
30369 node_id %lld failed to start flash: %s
30370 node_id %lld started
30371 node_id %lld failed to allocate replicator
30372 node_id %lld failed to start replicator
30373 node_id %lld failed to allocate meta storage
30374 node_id %lld rtn_crash_impl
30375 node_id %"PRIu32" crash completed with status %s
30376 send msg response to %p
30377 node_id %"PRIu32" shutdown
30378 \n\n WTF  msg_flags 0x%x\n
30379 \nNode %d: dn %d ss %d ds %d msg_flags 0x%x akrpmbx %p akrpmbx_from_req %p mkey %s\n
30380 wrapper:%p, node_id %lld %s msg seqno 0x%llx type %s src %lld:%s dest %lld:%s mkey %s
30381 node_id %lld key %*.*s
30382 In reqq_create()
30383 In reqq_init()
30384 In reqq_destroy()
30385 In reqq_lock()
30386 In reqq_unlock()
30387 In reqq_peek()
30388 In reqq_enqueue()
30389 In reqq_dequeue()
30390 \nHTable Debug pm %p pkey %s keylen %d nbuckets %lld\n
30391 \nHTable Debug pme %p pkey %s \n
30392 \nHash Table Delete ppme %p mkey %s contents %p numentries %d\n
30393 \nHash Table Delete ppme %p mkey %s contents %p remaining numentries %d\n
30394 \nNode %d: Setup HASH TABLE with num_bkts %d dummy key - %s\n
30395 \nNode %d: number of entries != 1... FAILED\n
30396 \nNode %d: entry %p contents %p has mkey %s = to the one we sent with len %d -> %s
30397 \nNode %d: entry %p created and verifed
30398 \nNode %d: entry created FAILED ret = %d
30399 \nNode %d: Hash Table Next Enum entry %p has ts %lu does it =  testit ts %lu\n
30400 \nNode %d: Next Hash Table Enum checkit %p should be NULL\n
30401 \nNode %d: Delete key %s was successful ret %d\n
30402 \nNode %d: Delete key %s failed ret %d\n
30403 \nNode %d: hash entry was deleted but enum check in not NULL\n
30404 \nNode %d: msg %p hresp %p dn %d mkey %s klen %d ar_mbx %p\n        actual rbox %p hseqnum %lu msg_timeout %li ptimemkr %lu timemkr %lu ntimemkr %lu\n        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n
30405 \nNode %d: HASH Retrive msg %p mhash %p hrmbx %p msg_timeout %lu hseqnum %lu\n        har_mbx %p = mar_mbx %p hmbx_req %p = mmbx_req %p diff time %lu ms\n        mkey %s ptimemkr %lu timemkr %lu ntimemkr %lu\n        PC_CNT %d, PA_CNT %d, PN_CNT %d, C_CNT %d, A_CNT %d, N_CNT %d\n
30406 \nNode %d: HASH ERROR hrmbx %p != respmbox %p\n
30407 \nNode %d: hresp_seqnum %lu tmout %d\n        timemkr %lu ntimemkr %lu ptimemkr %lu diff %lu\n        msg_timeout %lu actual tm %lu usec %lu\n        PC_CNT %d PA_CNT %d PN_CNT %d C_CNT %d A_CNT %d N_CNT %d\n
30408 Failed to remove open container handle
30409 %s - Failed to remove cguid map
30410 SUCCESS: SDFOpenContainer - %s
30411 FAILURE: SDFOpenContainer - cannot find %s
30412 create shard %lu on node %u received error response
30413 SUCCESS: container_meta_create
30414 FAILED: container_meta_create - set
30415 FAILED: container_meta_create - parms
30416 Failed to retrieve container guid counter state - cannot initialize CMC
30417 SDFClientAPI(%s)
30418 %u - %llu
30419 %u - %llu - %s
30420 %u
30421 closeParentContainer: closed parent for %s
30422 = sdfshared_createQueue(%u).
30423 sdfshared_freeQueue(%p).
30424 %s, SDF_QUEUE{Id=%s, QSIZE=%u, size=%u, head=%u, tail=%u}
30425 sdfshared_enqueue(%p, %p).
30426 Before dequeue(%p, %p).
30427  = sdfshared_dequeue(%p).
30428 PreXMboxWait  numMbx = %d Index = %d; thread_id = %ul;  mbox_sh_ptr = (%p)
30429 PostXMboxWait mboxIndex = %u;thread_id = %ul; mbox_sh_ptr = (%p) : mbox_ptr = %p  SB-entry = (%p)
30430 TEST FAILURES: %d
30431 // test get read pinned =%lu=======================
30432 Create Put Buffered Object: %s - %llu - %lu - %s
30433 Get Read Pinned Object: %s - %llu - %lu - %s
30434 Get Read Pinned Object: %s - %llu - %lu
30435 UnpinObject: %s - %llu - %lu - %s
30436 UnpinObject: %s - %llu - %lu
30437 RemoveObject: %s - %llu - %lu - %s
30438 RemoveObject: %s - %llu - %lu
30439 // test get read buffered =%lu=======================
30440 Get Read Buffered Object: %s - %llu - %lu - %s
30441 Get Read Buffered Object: %s - %llu - %lu
30442 // test create and get read buffered =%lu=======================
30443 Create Put Buffered Object with Expiry: %s - %llu - %lu - %s
30444 Create Put Buffered Object with Expiry: %s - %u - %s
30445 Get Read Buffered Object with Expiry: %s - %llu - %lu - %s
30446 Get Read Buffered Object with Expiry: %s - %llu - %lu
30447 GetBufferedObjectWithExpiry exptime mismatch! Key=%s, Expected=%u, Read=%u
30448 GetBufferedObjectWithExpiry data mismatch at offset=%d, key=%s!
30449 Count Cache Entries: %lu - %s
30450 Count Container Cache Entries: %llu, created=%llu
30451 // test SDF__InvalidateContainerObjects=%lu=======================
30452 Flush Inval Cache: %lu - %s
30453 Flush Inval Cache: %lu - %s - %s
30454 // test get read buffered before flushall takes place (thrd=%lu) =======================
30455 // test get read buffered after flushall takes place (thrd=%lu) =======================
30456 // test cmc cguid =%lu================================
30457 CMC put cguid failed: %s - %llu - %lu - %s
30458 CMC get cguid failed: %s - %llu - %lu - %s
30459 CMC get cguid: %s - %llu - %lu
30460 // =================================
30461 start %lu
30462 FAILED: %d errors
30463 done %lu
30464 init_container...
30465 init_container: FAILED to create container: %s - %s
30466 init_container: FAILED to get metadata: %s - %s
30467 init_container: FAILED to open container: %s - %s
30468 init_container done
30469 : %lu
30470 threads_complete %d vs threads %d
30471 ALL %d FTHREADS DONE - %d errors....
30472 : done - %lu
30473   Scheduler %lu started\n
30474   Scheduler %lu halted\n
30475   Spawning fthread: %d
30476 Max threads is %d
30477 Test Complete: %d ops
30478 // test shared nothing: %d - %lu===============================
30479 // test fth shmem: %d - %lu================================n
30480 // test fth lock: %d - %lu================================n
30481 // test fth sync add: %d - %lu===============================
30482 // test fth malloc: %d - %lu================================n
30483 api_tests done %lu
30484   Scheduler %llu halted\n
30485 // =====================================
30486 // test_create_object_container
30487 SDFCreateContainer(path=%s) is SUCCESS :) 
30488 SDFCreateContainer(path=%s) FAILED, as CONTAINER EXISTS ;-) 
30489 SDFCreateContainer(path=%s) is FAILURE - %s:( 
30490 // test_delete_container
30491 SDFDeleteContainer(path=%s) is SUCCESS :) 
30492 SDFDeleteContainer(path=%s) is FAILURE - %s :( 
30493 // test_open_close_object_container
30494 SDFOpenContainer(path=%s) is FAILURE - %s :( 
30495 SDFCloseContainer(path=%s) is FAILURE - %s :( 
30496 SDFOpenContainer(path=%s) is SUCCESS :) 
30497 // memslap: %s - %u - %u =====================================
30498 SDFNewContext ..!
30499 SDFNewContext FAILED!
30500 // open_container
30501 SDFOpenContainer(path=%s) is FAILURE - %s:( 
30502 // create object =================================
30503 SDFCreatePutBufferedObject(%s) FAILED, as OBJECT_EXISTS ;-) 
30504 SDFCreatePutBufferedObject(%s) is FAILURE - %s :( 
30505 SDFCreatePutBufferedObject(%s) is SUCCESS - %u:) 
30506 create_obj = %u
30507 total = %u
30508 // get object read pinned ===========================
30509 SDFGetForReadPinnedObject(%s-%s) is FAILURE - %s
30510 SDFGetForReadPinnedObject(%s-%s) is SUCCESS: SDFGet 
30511 %d - len = %u - gdata2-'%*s'
30512 iter %d: data mismatch for index %d
30513 iter %d: data matched!
30514 // unpin object ====================================
30515 SDFUnpinObject(%s-%s) is FAILURE - %s
30516 SDFUnpinObject(%s-%s) is SUCCESS: SDFUnpinObject
30517 // put object =================================
30518 SDFPutBufferedObject(%s) FAILED, as OBJECT_UNKNOWN ;-) 
30519 SDFPutBufferedObject(%s) is FAILURE - %s :( 
30520 SDFPutBufferedObject(%s) is SUCCESS - %u:) 
30521 // remove object=================================
30522 SDFRemoveObject(%s-%s) is FAILURE - %s
30523 SDFRemoveObject(%s-%s) is SUCCESS: SDFGet 
30524 Num objects in %s is %lu
30525 Failed to get num objects for %s
30526 SDFCloseObjectContainer(path=%s) is FAILURE - %s
30527 SDFCloseObjectContainer(path=%s) is SUCCESS: Open/Close Container
30528 SDFDeleteContext FAILED!
30529 CMC put cguid failed: %s - %llu - %s
30530 CMC get cguid map failed: %s - %llu - %s
30531 CMC got map: %s - %llu
30532 testObjectContainer start %d
30533 testObjectContainer done - %d
30534 %s -%lu
30535 done - %llu
30536 : core - %llu
30537 Start fthread %lu
30538 Start scheduler %lu
30539 Scheduler %lu halted
30540 Create pthread %lu
30541 test done
30542 %s=NULL. Defaults will be used.\n
30543 NULL container name - %s
30544 NULL path - %s
30545 restore version does not match version in mcd_compatibility.h!
30546 stop home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d before call)
30547 start home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d before call)
30548 create home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d)
30549 delete home shard map entry (shard=%"PRIu64", pshard=%p, stopflag=%d)
30550 home_flash operation (%d) to a valid containerfor cwguid %llu shardid 0x%lx (pshard=%p, stopflag=%d)
30551 home_flash operation (%d) to a non-existent containerfor cwguid %llu shardid 0x%lx
30552 home_flash operation (%d) to a stopped containerfor cwguid %llu shardid 0x%lx
30553 Home to Flash Start Replication (I am master): cguid=%"PRIu64", node_from=%d
30554 Home to Flash Start Replication (I am slave): cntr_id=%d, cntr_status=%d
30555 replicating_to_node = SDF_ILLEGAL_VNODE
30556 replicating_to_node = %d
30557 [tag %d, node %d, pid %d, thrd %d] request: %"FFDC_LONG_STRING(100)"
30558 [tag %d, node %d, pid %d, thrd %d] final status: %s (%d)
30559 Not enough memory for cache: must have at least enough memory for a single maximum sized object per slab (%"PRIu64" required, %"PRIu64" available). Try increasing SDF_CC_MAXCACHESIZE or reducing the number of slabs.
30560 nslabs had to be reduced from %"PRIu64" to %"PRIu64" so that each slab could hold at least one max sized object
30561 malloc failed!
30562 Cannot enable writeback caching for container %"PRIu64" because writeback caching is disabled.
30563 ===========  START async wrbk (cguid=%"PRIu64",key='%s'):  ===========
30564 ===========  END async writeback (cguid=%"PRIu64",key='%s'):  ===========
30565 Could not compute a value for nslabs!
30566 init_state_fn must be non-NULL!
30567 update_state_fn must be non-NULL!
30568 print_fn must be non-NULL!
30569 is_dirty_fn must be non-NULL!
30570 Unknown request type '%d' found on remote request queue!
30571 Object update failed with status %s(%d)
30572 Writeback caching can only be enabled for eviction mode containers
30573 Cannot enable writeback caching for container 
30574 wrbk_fn must be non-NULL!
30575 Cannot enable writeback caching for container '%s' because writeback caching is disabled.
30576 There is insufficient memory for even a single slab!  There must be at least enough memory to hold one max sized object.  Try increasing the cache size (SDF_CC_MAXCACHESIZE).
30577 Could not allocate SDF cache slab temporary entry
30578 SDF_MAX_PARALLEL_FLUSHES = %d
30579 Invalid container type in flush_inval_stuff: %d
30580 ===========  END async flush (cguid=%"PRIu64",key='%s'):  ===========
30581 PROP: SDF_MAX_FLUSHES_PER_MOD_CHECK=%d
30582 PROP: SDF_MODIFIED_FRACTION is out of range; using default of %g
30583 PROP: SDF_MODIFIED_FRACTION=%g
30584 flush_fn must be non-NULL!
30585 SDF_MAX_PARALLEL_FLUSHES must be non-zero; using default of %d
30586 Invalid request type: %d (%s)
30587 Invalid combined_action: %d (%s) for operation(cguid=%"PRIu64", key='%s')
30588 SDFChangeContainerWritebackMode() failed, status=%s
30589 changing container to writeback, port=%d
30590 changing container to writethru, port=%d
30591 SDFSetAutoflush() failed, status=%s
30592 SDFSetFlushThrottle() failed, status=%s
30593 SDFSetModThresh() failed, status=%s
30594 SDFSelfTest() failed, status=%s
30595 Enabled SDF cache autoflushing through admin port
30596 Disabled SDF cache autoflushing through admin port
30597 Cannot turn on autoflush because writeback caching is disabled.
30598 SDF Autoflush set to %d.
30599 Percent parameter (%d) must be between 1 and 100.
30600 'ntest' parameter (%d) must be between 1 and 10.
30601 SDF self test %d started.
30602 PROP: SDF_MAX_OUTSTANDING_FLUSHES=%d
30603 SDF_MAX_OUTSTANDING_FLUSHES must be non-zero; using default of %d
30604 SDF_MAX_OUTSTANDING_FLUSHES must be less than or equal to the number of async put threads; setting to %d
30605 SDF_MAX_OUTSTANDING_FLUSHES = %d
30606 Object name is too long
30607 SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES must be non-zero; using default of %d
30608 SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES must be less than or equal to the number of async put threads; setting to %d
30609 SDF_MAX_OUTSTANDING_BACKGROUND_FLUSHES = %d
30610 SDF_BACKGROUND_FLUSH_SLEEP_MSEC must be >= %d; defaulting to minimum value
30611 SDF_BACKGROUND_FLUSH_SLEEP_MSEC = %d
30612 sleep_msec parameter (%d) must be >= %d.
30613 Cannot configure autoflush because writeback caching is disabled.
30614 Cannot set flush throttle because writeback caching is disabled.
30615 Cannot set modified data threshold because writeback caching is disabled.
30616 SDF self test started with args '%s'.
30617 async put thread pool %p started
30618 async put thread pool %p failed to start 
30619 background_flusher could not be started
30620 number of async put threads must be > 0
30621 Percent parameter (%d) must be between 0 and 100.
30622 Wait
30623 Post
30624 Cannot enable writeback caching for store mode container %"PRIu64".
30625 Cannot start background flusher because writeback caching is disabled.
30626 failed to lock %"PRIu64"
30627 failed to unlock %"PRIu64"
30628 PROP: MEMCACHED_PREFIX_BASED_DELETION=%s
30629 unknown request type (%d)!
30630 Version Mismatch with the peer %s
30634 XXXZZZ(%d) alloc_steal %"PRIu64"B: 0x%p
30635 XXXZZZ(%d) alloc %"PRIu64"B: 0x%p
30636 XXXZZZ(%d) free: 0x%p
30637 flash returned pshard %p for cguid %llu shardid %lu
30638 ===========  name_service_get_meta(cguid=%"PRIu64"): meta.flush_time=%d meta.flush_set_time=%d ctnr_meta[%d].pshard=%p &(ctnr_meta[%d])=%p ===========
30639 (cguid=%"PRIu64"): pas->ctnr_meta[%d].pshard=%p, pctnr_md=%p
30640 Duplicate container for cguid %"PRIu64".
30641 Could not open license status file at '%s'.\n
30642 Could not open license file at '%s'.\nContact Schooner support at www.schoonerinfotech.com to procure a license.\n
30643 Out of memory!\n
30644 Problem reading license file at '%s'.\n
30645 License at '%s' is not valid.\nContact Schooner support at www.schoonerinfotech.com.\n
30646 License at '%s' is valid.\n
30647 mcd_check_license() called.  CheckCounter=%"PRIu64"...\n
30648 Could not close license status file at '%s'.\n
30649 Could not close license file at '%s'.\n
30650 Check License: %s\n
30651 License check failed.  You have %d hours to get a production license.  Contact Schooner support at www.schoonerinfotech.com.\n
30652 License check failed too many times.  You must get a production license to restart the server.  Contact Schooner support at www.schoonerinfotech.com.\n
30653 pth_id=%d exceeds maximum of %d
30654 Check License: %s %s\n
30655 Ran out of free log segments, shardID=%lu
30656 failed to enable core dump for user %s
30657 Setrlimit failed! Error: %s\n
30658 fth using up to %d cores
30660 failed to initialize the aio context (rc=%d '%s')
30664 container backup sync failed for enum, status=%s
30665 process_raw_get_command_enum() failed


# hiney
40000 container (%d) skipped expired objects=%lu, blocks=%lu
40001 container (%d) skipped flushed objects=%lu, blocks=%lu
40002 object skipped (expired), blk_offset=%lu map_offset=%lu key_len=%d
40003 object skipped (flush), blk_offset=%lu map_offset=%lu key_len=%d
40004 flash desc version change, offset=%lu, copy=%d, old=%d, new=%d
40005 invalid property desc, offset=%lu, copy=%d, slot=%d
40006 property desc version change, offset=%lu, copy=%d, slot=%d, old=%d, new=%d
40007 updating properties, offset=%lu, slot=%d
40008 failed to update shard props, offset=%lu, slot=%d, rc=%d
40009 updating superblock, offset=%lu
40010 failed to update superblock, offset=%lu, rc=%d
40011 failed to read label, blk_offset=%lu, ssd=%d, rc=%d
40012 Invalid signature '%s' read from fd %d
40013 Unexpected signature '%s' read from fd %d
40014 container (%s) backup sync failed, status=%s
40015 container (%s) backup started: '%s'
40016 container (%s) backup or restore already running
40017 container (%s) backup requires full backup
40018 container (%s) backup client protocol version %u incompatible with server version %u
40019 container (%s) backup not started, status %s
40020 container (%s) backup status: %s
40021 container (%s) restore in progress
40022 container (%s) backup not running, status %s
40023 container (%s) restore couldn't set flush time
40024 container (%s) restore started: '%s'
40025 container (%s) restore sequence number %lu last restored
40026 container (%s) restore container not empty
40027 container (%s) restore client protocol version %u incompatible with server version %u
40028 container (%s) restore status: %s
40029 container (%s) backup in progress
40030 container (%s) restore not running, status %s
40031 failed to read log %d, shardID=%lu, rel_offset=%lu, blk_offset=%lu, rc=%d
40032 failed to allocate log segment list
40033 failed to alloc shard log segments, shardID=%lu
40034 SKIPPING, shardID=%lu, pass %d of %d, log %d: obj: start=%lu, end=%lu; high=%lu, low=%lu
40035 ENTERING, shardID=%lu, pass %d of %d, log %d: start=%lu, blks=%lu; obj: start=%lu, end=%lu; high=%lu, low=%lu
40036 Invalid log page checksum, shardID=%lu, found=%lu, calc=%lu, boff=%lu, poff=%d
40037 LSN fell off, shardID=%lu, prevLSN=%lu, pageLSN=%lu, ckptLSN=%lu
40038 Skipping log page, shardID=%lu, boff=%lu, pageLSN=%lu, ckptLSN=%lu
40039 processed shardID=%lu, pass %d of %d, log %u: high=%lu, low=%lu; highLSN=%lu; applied=%lu
40040 Skip table read, shardID=%lu: pass %d of %d, chunk %d of %d; blk: start=%lu, count=%d; obj: start=%lu, count=%lu; log: high=%lu, low=%lu
40041 Reading table, shardID=%lu: pass %d of %d, chunk %d of %d; blk: start=%lu, count=%d; obj: start=%lu, count=%lu, log: high=%lu, low=%lu
40042 Writing table, shardID=%lu: pass %d of %d, chunk %d of %d; blk: start=%lu, count=%d; obj: start=%lu, count=%lu, log: high=%lu, low=%lu
40043 updater for shardID=%lu can't allocate %luM buffer
40044 updater for shardID=%lu allocated %lu byte buffer
40045 Recovering object table, shardID=%lu, pass %d of %d
40046 Log %d already merged, shardID=%lu, ckpt_LSN=%lu, log_pages=%lu, last_log=%d, last_page=%lu, last_LSN=%lu, next_LSN=%lu
40047 Skip table write, shardID=%lu, pass %d of %d, chunk %d of %d,
40048 Table update complete, shardID=%lu, pass %d of %d, chunk %d of %d
40049 Skip table write,shardID=%lu, pass %d of %d, chunk %d of %d
40050 Skipped object table read, shardID=%lu, pass %d of %d; chunk %d of %d
40051 Recovered shardID=%lu, chunk %d of %d, obj=%lu, seq=%lu
40052 updater thread halting, shardID=%lu
40053 ENTERING: shardID=%lu, count=%d, fill_count=%u
40054 ENTERING: shardID=%lu, seqno=%lu, count=%d, fill_count=%u
40055 shardId=%lu, snap_seqno=%lu, logbuf_seqno=%lu, apply %d pending updates
40056 shardID=%lu, seqno=%lu, prev=%u, curr=%u, left=%lu, hiseq=%lu
40057 SYNC: shardID=%lu, pp_recs=%d, sync_recs=%d, seqno=%lu, sync=%s
40058 container (%s) skipped expired objects=%lu, blocks=%lu
40059 container (%s) skipped flushed objects=%lu, blocks=%lu
40060 snap_dump from address %p:
40061 invalid ckpt record, shardID=%lu, blk_offset=%lu
40062 allocated %lu byte buffer for %lu log segments
40063 allocated %d log segments to shardID=%lu, remaining=%d
40064 recovered segment[%lu]: blk_offset=%u
40065 class[%d], blksize=%d: new_seg[%d], blk_offset=%lu
40066 recovered class[%d], blksize=%d, segment[%d]: blk_offset=%lu, seg_table[%lu]
40067 log0_LSN=%lu, log1_LSN=%lu, rec_log_blks=%lulog0_lastLSN=%lu, log1_lastLSN=%lu
40068 ENTERING, shardID=%lu, pass %d of %d, log %d: start=%lu, blks=%lu, cached=%u; obj: start=%lu, end=%lu; high=%lu, low=%lu
40069 mail_log=%d, last_log=%d, last_page=%lu, last_LSN=%lu, ckpt_LSN=%lu, log_blks=%lu
40070 <<<< log_write: shardID=%lu, syn=%u, blocks=%u, del=%u, bucket=%u, blk_offset=%u, seqno=%lu, old_offset=%u, tseq=%lu
40071 updater thread %lu can't allocate %lu byte buffer
40072 updater thread %lu allocated %lu byte buffer
40073 Updater thread %lu waiting
40074 Updater thread %lu exiting
40075 Updater thread %lu assigned to shardID=%lu
40076 Updater thread %lu halting, shardID=%lu
40077 Log writer thread %lu waiting
40078 Log writer thread %lu exiting
40079 Log writer thread %lu assigned to shardID=%lu
40080 Log writer thread %lu halting, shardID=%lu
40081 Recovering object table, shardID=%lu, pass %d of %d: %d percent complete
40082 log0_LSN=%lu, log1_LSN=%lu, rec_log_blks=%lu, log0_lastLSN=%lu, log1_lastLSN=%lu
40083 Recovering object table, shardID=%lu, pass %d of %d: complete, highLSN=%lu, ckptLSN=%lu
40084 Skip process log, shardID=%lu: pass %d of %d, log %d; obj: start=%lu, end=%lu; high=%lu, low=%lu
40085 Skip process log, shardID=%lu: pass %d of %d, log %d; obj: start=%lu, end=%lu; highOff=%lu, lowOff=%lu, highLSN=%lu, ckptLSN=%lu
40086 Processing log, shardID=%lu, pass %d of %d, log %d: start=%lu, blks=%lu, cached=%u; obj: start=%lu, end=%lu; highOff=%lu, lowOff=%lu, highLSN=%lu, ckptLSN=%lu
40087 ckptLSN=%lu, next_LSN=%lu, log_blks=%lu, LSN[0]=%lu, lastLSN[0]=%lu, LSN[1]=%lu, lastLSN[1]=%lu
40088 Old log %d merge %s, shardID=%lu, ckpt_LSN=%lu, log_pages=%lu, ckpt_log=%d, ckpt_page=%lu, ckpt_page_LSN=%lu
40089 %s object table, shardID=%lu, pass %d of %d
40090 %s object table, shardID=%lu, pass %d of %d: %d percent complete
40091 %s object table, shardID=%lu, pass %d of %d: complete, highLSN=%lu, ckptLSN=%lu
40092 failed to allocate %lu byte buffer for %lu log segments
40093 failed to allocate segment array, size=%lu
40094 failed to allocate %lu byte buffer for %lu update segments
40095 allocated %lu byte buffer for %lu update segments
40096 deallocated %d log segments from shardID=%lu, remaining=%d
40097 Reading log segment %d: shardID=%lu, start=%lu, buf=%p
40098 reading log buffer blk_off=%lu, blks=%lu, buf=%p
40099 Processing log, shardID=%lu, pass %d of %d, log %d: start=%lu, blks=%lu, segments=%d, cached=%u; obj: start=%lu, end=%lu; highOff=%lu, lowOff=%lu, highLSN=%lu, ckptLSN=%lu
40100 %s blocks, start=%lu, count=%lu, buf=%p
40101 Reading table, shardID=%lu: pass %d of %d, chunk %d of %d; segments=%d;  blk: start=%lu, count=%d; obj: start=%lu, count=%lu, log: high=%lu, low=%lu
40102 Reading segment %d: shardID=%lu, start=%lu, buf=%p
40103 Writing table, shardID=%lu: pass %d of %d, chunk %d of %d; segments=%d, blk: start=%lu, count=%d; obj: start=%lu, count=%lu, log: high=%lu, low=%lu
40104 Writing segment %d: shardID=%lu, start=%lu, buf=%p
40105 reserving %d update segments for shardID=%lu
40106 attached %d update segments to shardID=%lu, remaining=%d
40107 detached %d update segments from shardID=%lu, free=%d
40108 failed to allocate %lu byte array for object table recovery, shardID=%lu
40109 %screasing log size from %lu to %lu blocks
40110 Unexpected LSN, shardID=%lu, LSN=%lu, prev_LSN=%lu; seg=%d, page=%d
40111 base segment table initialized, size=%lu
40112 bitmaps initialized, size=%d, bitmap_size=%d, total=%d
40113 slab class inited, blksize=%d, free_slabs=%lu, segments=%lu
40114 slab class dealloc, blksize=%d, free_slabs=%lu, segments=%lu
40115 LSN fell off, shardID=%lu, prevLSN=%lu, pageLSN=%lu, ckptLSN=%lu, seg=%d, page=%d
40116 Reading segment %d: shardID=%lu, start=%lu, count=%d, buf=%p
40117 Writing segment %d: shardID=%lu, start=%lu, count=%d, buf=%p
40118 Read less than a full log segment: buf=%p, blk_count=%d, logblks=%lu, soff=%lu, io=%lu, boff=%lu, LSN1=%lu, LSN2=%lu
40119 container (%s) backup: %lu of %lu allocated segments, empty=%lu, error=%lu, total=%lu; objects=%lu, blks=%lu; deleted=%lu, blks=%lu; expired=%lu, blks=%lu; flushed=%lu, blks=%lu; error_obj=%lu, error_blks=%lu
40120 ENTERING, shardID=%lu, full=%d, vers=%d
40121 invalid key offset %lu
40122 Error reading raw segment, blk_offset=%lu, slab_blksize=%d, next_slab=%u, rc=%d
40123 Dealloc[%d]: %d
40124 Pending dealloc[%d]: %d
40125 Reading bad raw segment (%lu), blksize=%d, seg_offset=%lu, num_blks=%d, blks=%d, rec=%s
40126 Error reading bad raw segment (%lu), seg_offset=%lu, blk_offset=%lu, blks=%d, rc=%d
40127 Bad block(s) found in raw segment (%lu), seg_offset=%lu, count=%d
40128 object skipped (error), blk_offset=%lu map_offset=%lu


# xiaonan
50000 failed to listen on admin port %d
50001 failed to parse container ip, j=%d
50002 ref_count reduced, port=%d ref=%d
50003 IPADDR_ANY detected
50004 got ip_addr %s
50005 tcp_port=%d udp_port=%d capacity=%ldMB eviction=%d persistent=%d id=%lu cname=%s num_ips=%d ips=%s
50006 formatting container, name=%s
50007 starting container, name=%s
50008 stopping container, name=%s
50009 ip %s added to container %s
50010 ip %s removed from container %s
50011 failed to retrieve UDP request dst IP, port=%d
50012 homeless UDP request received, port=%d ip=%s
50013 TCP listening socket found for port %d ref=%d
50014 UDP listening socket found for port %d ref=%d
50015 ref_count=%d
50016 got ips: %s
50017 failed to dup container IP list
50018 invalid ip_addr %s encounterd
50019 too many IPs specified in the property file
50020 IPADDR_ANY cannot be specified with any other IPs
50021 total number of ips is %d, ips=%s
50022 failed to upgrade mcd_container
50023 failed to dup cname %s
50024 unknown version number %d
50025 aio_free_context not implemented!
50026 ENTERNING
50027 failed to initialize prefix-based deleter for container %s
50028 failed to persist prefix-based deletion for container %s
50029 ENTERING, exptime=%lu
50030 failed to get SDF properties, status=%s
50031 failed to cleanup prefix deleter
50032 SASL enabled for container %s
50033 SASL disabled for container %s
50034 System is not a valid Schooner platform
50035 Schooner platform check passed
50036 invalid delimiter %c, please specify a value between ' ' and '~'
50037 prefix-based deletion delimiter set to %c
50038 buf=%p aligned=%p offset=%hu
50039 not enough magic in buffer header
50040 freeing iobuf, buf=%p orig=%p
50041 writer resume mail posted, shard=%p
50042 wbuf %d full, writer notified off=%lu rsvd=%lu alloc=%lu cmtd=%lu next=%lu
50043 max slab_blksize=%d
50044 last entry, i=%d blksize=%d
50045 too many invalid evictions, cnt=%u moff=%d boff=%lu hind=%u
50046 too many deleted evictions, cnt=%u
50047 ENTERING key=%s len=%d
50048 prefix-based deletion failed, prefix=%s status=%s
50049 prefix-based delete, key=%s
50050 prefix object deleted, key=%s
50051 failed to update prefix deleter, rc=%s
50052 ENTERING container=%s prefix=%s
50053 prefix registered, key=%s time=%lu addr=%lu
50054 failed to register prefix, key=%s
50055 ENTERING container=%s
50056 failed to get prefix list
50057 number of prefixes is %d
50058 ENTERING, c=%p c->cas=%lu
50059 unknown rc, this should not happen
50060 ENTERING, sub_cmd=%s
50061 ENTERING, binary flush_all for container %s
50062 Failed to initialize SASL conn, c=%p cname=%s
50063 sasl_conn initialized, c=%p
50064 sasl mechs: %s
50065 mech:  ``%s'' with %d bytes of data
50066 uname=%s
50067 cmd 0x%02x, rc is %s\n
50068 ENTERING, c=%p cmd=%d substate=%d
50069 Not handling substate %d\n
50070 ENTERING, c=%p container=%s
50071 prefix-delete mail posted, c=%p
50072 failed to set shard flush_time, rc=%d
50073 failed to set sdf flush_time, rc=%d
50074 failed to bind socket:%s port=%d
50075 aio context category %s count=%d
50076 new aio context allocated, category=%s count=%d
50077 invalid aio context category %d
50078 ENTERING category=%d
50079 failed to remove object, status=%s
50080 failed to get prefix stats, status=%s
50081 received prefix-based delete command, key=%s
50082 Error initializing sasl
50083 Initialized SASL
50084 failed to write sasl pipe
50085 sasl mail posted, c=%p mails=%lu
50086 error writing sasl pipe, rc=%d
50087 error reading sasl pipe, rc=%d
50088 sasl_helper terminated
50089 failed to create sasl helper pipe
50090 received msg, c=%p mails=%lu
50091 object size doesn't match hash entry!!! klen=%d dlen=%u nbybes=%d syn=%lu hsyn=%hu ctime=%u stime=%lu


# jmoilanen


# johann
70000 too many invalid evictions, shard=%p cnt=%u moff=%d boff=%lu hind=%u
70001 using fast recovery
70002 recovery: mrep_bmap: bitmap too small: nsegs=%d
70003 recovery: mrep_sect: bad size: %ld != %d
70004 %s: msg size too small: %d
70005 %s: wrong type: %d != %d
70006 %s: error: %d
70007 %s: wrong msg size: %d < %d
70008 bad mreq_fbmap msg: bad length
70009 bad mreq_fbmap msg: bad shard
70010 bad HFFSX message: num_setx=%d len=%d
70011 bad mreq_sect msg: bad length
70012 bad mreq_sect msg: bad shard
70013 mreq_sect: recovering node died
70014 recovery: mrep_sect: invalid magic %x
70015 recovery: mrep_sect: invalid num_obj %d
70016 recovery: mrep_sect: invalid num_obj (%d) should be %d
70017 recovery: mrep_sect: bad object list
70018 recovery write %ld:%ld (%ld)
70019 write failed bo=%ld nb=%ld
70020 bad recovery meta data
70021 mkmsg_flash: read error
70022 mkmsg_flash: recovering node died
70023 recovery read %ld:%ld (%ld)
70024 mcd_fth_aio_blk_read failed: st=%d blkno=%ld mapblk=%ld nb=%ld
70025 bad set index: %ld head=%ld tail=%ld
70026 null set index: %ld head=%ld tail=%ld
70027 recovery: found buffer, size=%ld
70028 recovery: waiting for buffer, size=%ld
70029 sdfmsg version %d
70030 found rank of %d in property file
70031 bad messaging parameter: %s = %s
70032 parm_set: bad size: type=%c size=%d
70033 parm_set: bad parameter %s = %s
70034 %s = %ld
70035 %s = %g
70036 %s = %s
70037 sdf_msg_send to dead node: %d
70038 received message from unknown node m%d
70039 received truncated message from node n%d
70040 received message from node n%d: bad sdfmsg version: %d
70041 stray message: addr=(%d:%d<=%d:%d) id=%ld len=%d
70042 bad data from n%d(%d); dropped
70043 creating queue=(%d:%d=>%d:%d) but (%d:%d=>%d:%d) already exists
70044 %s: addr=(%d:%d=>%d:%d)
70045 %s node=m%d state=%s rank=%d
70046 node m%d drop attempt, non-existent
70047 msgtcp version %d
70048 possible clock skew %.1fs
70049 UDP receive failed
70050 UDP send failed: %s to %s
70051 UDP send succeeded: %s to %s
70052 node drop rn=%s: %s
70053 recv n%d: bad metadata type=%d
70054 resending %d messages from %ld
70055 getaddrinfo %s:%s failed: %s
70056 getaddrinfo %s:%s failed: no valid entries
70057 failing back to %s
70058 conn err rn=%s if=%s fd=%d rip=%s: %s
70059 failing over to %s
70061 watchdog: R: %s
70062 watchdog: W: %s
70063 watchdog: connect failed: %s
70064 if=%s con=%d pri=%d lip=%s bip=%s
70065 send_talk node=m%d state=%s rank=%d
70066 node n%d is live
70067 node n%d is dead
70068 node %s is %s
70069 node new rn=%s uip=%s ver=%d
70070 path %s rn=%s if=%s rip=%s nc=%d
70071 conn %s rn=%s if=%s fd=%d rip=%s
70072 path %s rn=%s if=%s rip=%s nc=%d lip=%s
70073 UDP socket failed
70074 UDP bind failed
70075 failed to add contact IP: %s
70076 attempting to add duplicate contact IP: %s
70077 path %s rn=%s if=%s lip=%s rip=%s nc=%d
70078 Syncing data (SYNC_DATA set)
70080 Flushing logs to %s
70081 Syncing logs (DATA_SYNC set)
70092 Flush log recovery: failed to allocate fbio
70093 Flush log recovery: failed to allocate sector
70094 Flush log recovery: write failed: blk=%ld err=%d
70095 Flush log recovery: read failed: blk=%ld err=%d
70096 Flush log recovery: LSN mismatch old=%ld new=%ld
70097 Flush log recovery: patched %d log records for shard %lu
70098 Flushing logs in place (LOG_FLUSH_IN_PLACE set)
70100 Flush log recovery: cannot open sync log file %s flags=%x error=%d
70101 Flush log recovery: log flush write failed seek=%ld errno=%d
70102 Syncing logs (SYNC_DATA set)
70103 Flush log sync: failed to allocate fbio
70104 Flush log sync: write failed: blk=%ld err=%d
70105 Flush log sync: read failed: blk=%ld err=%d
70106 Flush log sync: failed to allocate sector
70107 Flush log sync: patched %d log records for shard %lu
70108 Flush log sync: cannot open sync log file %s flags=%x error=%d
70109 Flush log sync: log flush write failed seek=%ld errno=%d size=%ld
70110 message
70111 enumeration started for container %ld
70112 enumeration error %ld >= %ld
70113 Unable to open file descriptor 
70114 bad container: %d
70115 container %d would have %ld objects
70116 container %d would have a size of %ld bytes
70117 enumeration ended for container %ld
70118 Failed on get_cntr_info for container %ld
70119 Container %s: id=%ld objs=%ld used=%ld size=%ld full=%.1f%%
70120 Container %s: id=%ld objs=%ld used=%ld
70121 enumeration internal error %ld >= %ld
70122 Async thread started...
70123 Initializing the async threads...
70124 Read from properties file '%s'
70125 Bad setting of FDF_LOG_LEVEL: %s
70126 PROP: SDF_MSG_ON=%u
70127 Programming error: Numbers of stats defined in FDF_access_types_t(%d) does not match array fdf_stats_access_type(%d)
70128 Programming error: Numbers of stats defined in FDF_cache_stat_t(%d) does not match array fdf_stats_cache(%d)
70129 Programming error: Numbers of stats defined in fdf_stats_flash(%d) does not match array fdf_stats_flash(%d)


# mkrishnan
80000 Adding source route to table %srtable for %s failed\n
80002 Add local route %s/%d for %s to %srtable
80003 SDF_CLUSTER_NUMBER_NODES should be > 0\n
80004 SDFGetNumNodesInClusterFromConfig: Number of Nodes  %d \n
80005 Failed to get the container name for :%d
80006 Formating the container %s
80007 Stopping the container %s failed
80008 Failed to format the container port:%s
80009 Container %s Not Found
80010 Starting the container %s Failed
80011 Stoping the container %s after recovery failed
80012 sdf_vip_group_get_node_preference for %d = %d\n
80013 sdf_vip_group_get_node_preference FOR %d = %d\n
80014 sdf_vip_group_get_node_rank for %d = %d\n
80015 sdf_vip_group_get_node_by_preference for %d(grp:%d) = %d\n
80016 sdf_vip_group_get_node_preference for %d(grpid:%d) = %d\n
80017 sdf_vip_group_get_node_preference FOR %d(grpid:%d) = %d\n
80018 IPF fail called :%d
80019 Got signal %d, exiting
80020 Initializing Flash Data Fabric (Rev:%s)
80021 Waiting for %d container deletes to finish
80022 Shutdown in Progress. Operation not allowed 
80023 Delete already under progress for container %lu
80024 Failed to get stats for container:%lu (%s)
80025 %s Virtual Metadata Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %sasync_writes = %s,durability = %s)
80026 %s Virtual Data Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %sasync_writes = %s,durability = %s)
80027 Starting FDF admin on TCP Port:%u
80028 Unable to start admin on TCP Port:%u
80029 Staring asynchronous command handler
80030 FDF Configuration: Storage size = %d GB,Reformat the storage = %s,Cache size = %llu,Maximum object size = %llu
80031 FDF Testmode enabled
80032 Property file: %s
80033 Shutdown completed
80034 Container %s does not exist
80035 %s Virtual Metadata Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %s,async_writes = %s,durability = %s)
80036 Unsupported size(%lu bytes) for VDC. Maximum supported size is 2TB
80037 %s Virtual Data Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %s,async_writes = %s,durability = %s)
80038 Small DRAM Cache: # of slabs had to be reduced from %"PRIu64" to %"PRIu64" so that each slab could hold at least one max sized object. Possible performance impact
80039 Creating Container Metadata Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %s,async_writes = %s,durability = %s)
80040 Opening Container Metadata Container (name = %s,size = %lu kbytes,persistence = %s,eviction = %s,writethrough = %s,fifo = %s,async_writes = %s,durability = %s)
80041 Flash file %s opened successfully
80042 No space left for hash entry
80043 Slab class empty
80044 hash table overflow area full. num_hard_overflows=%lu
80045 compact_class(async) before\n
80046 compact_class(async) after\n


# kcai


# xmao
100000 object removed, not found key=%s
100001 mcd_remove_object() failed, status=%d
100002 >%d UNHANDLED ERROR: %d
100003 %d: Failed to grow buffer.. closing connection
100004 Failed to initialize SASL conn.
100005 Failed to list SASL mechanisms.
100006 Unhandled command %d with challenge %s
100007 Unknown sasl response:  %d\n
100008 %d: Client using the %s protocol
100009 Invalid magic:  %x
100010 Recieve data with unexpected protocol
100011 Failed to build UDP headers
100012 ntop is larger than statop: ntop=%d, statop=%d
100013 Allocate memory for copy of winner head failed.
100014 Allocate memory for copy of winners failed.
100015 Allocate memory for copy of key table failed.
100016 Allocate memory for sorted client failed.
100017 Allocate memory for copy of snapshot sorted winners failed.
100018 Allocate memory for copy of ref sorted winners failed.
100019 Fail to allocate memory for copy of winner head.
100020 Fail to allocate memory for copy of winners.
100021 Fail to allocate memory for copy of key table.
100022 Fail to allocate memory for snapshot sorted winners.
100023 Fail to allocate memory for ref sorted winners.


# gxu
110001 hotkey assertion failed: %s %d
110002 flag is incorrect, flag=%d
110003 unexpected command type: %d for 1 instant
110005 unexpected command type:%d for 2 instants
110007 winner list tail is not null: nlist=%d


# wli


# tomr


# build


# darryl
150000 ENTERING, init_state=%d
150001 pth_id=%d
150002 writer fthread spawned, use context %p
150003 object size doesn't match hash entry!!! klen=%d dlen=%u nbybes=%d syn=%lu hsyn=%hu ctime=%u
150004 Mcd_aio_states[%d] = %p
150005 mcd_osd_container_generation: %d\n
150006 mcd_osd_container_generation: ENTER\n
150007 SDFGetContainerProps: %p - %lu - %p
150008 SDFGetContainerProps: 2
150009 SDFGetContainerProps: 3
150010 SDFGetContainerProps: 4
150011 SDFGetContainerProps: status=%s
150012 mcd_osd_container_generation: pai=%p, cguid=%lu\n
150013 SDFGetContainerProps: eviction=%s
150014 SERVER_ERROR failed to obtain SDF properties
150015 MCD_FTH_SDF_INIT
150016 XSDFGetContainerProps: %p - %lu - %p
150017 XSDFGetContainerProps: status=%s
150018 XSDFGetForReadBufferedObjectWithExpiry
150019 SDFGenerateCguid: %llu
150020 Invalid cguid - %s
150021 %lu - %s
150022 scheduler startup process created
150023 SDFCreateContainer failed for container %s because 128 containers have already been created.
150024 NULL - %s
150025 pread failed!
150026 Failed to find shard for %s
150027 not enough space: quota=%lu - free_seg=%lu - seg_size=%lu
150028 segment free list set up, %d segments, %lu bytes/segment
150029 failed to allocate hash table: %lu
150031 pread failed - %s
150032 Failed to find meta data for %lu -  %s
150033 FDFCreateContainer failed for container %s because 128 containers have already been created.
150034 Failed to save cguid state: %s
150035 Open container structure is NULL - %s
150036 Set to %s
150037 done
150038 ENTERING, buf=%p offset=%lu nbytes=%d
150039 %s, size=%u bytes
150040 %s, size=%lu bytes
150041 Container is not deleted (busy or error): cguid=%lu, status=%s
150042 FIFO mode is only allowed for non-evicting, non-persistent containers
150043 FIFO mode is only allowed for evicting, non-persistent containers
150044 shard delete failed due to NULL shard pointer
150055 Failed to open support containers
150056 Failed to create support containers
150057 Failed to create VMC container - %s\n
150058 Failed to find metadata container for %lu
150059 Failed to create CMC container - %s\n
150060 Failed status = %d...\n
150063 Failed to find metadata container for %s
150076 Failed to open support containers: %s
150077 Container full
150078 Cannnot change container size
150079 Cannnot reduce container size
150080 Cannnot read container metadata for %lu
150081 Cannnot write container metadata for %lu
150082 Cannnot find container id %lu
150083 %lu - %s\n
150084 Failed to generate container id for %s
150085 Could not read metadata for %lu\n
150086 Could not mark delete in progress for container %lu\n
150087 Could not clear delete in progress for container %lu\n
150088 Unable to initialize FDF thread state, exiting
150089 Unable to start the virtual container initialization thread.
150090 Incorrect value(%d) for Mcd_aio_num_files. It must be set to 1
150091 Cannot open container %lu to delete it
150092 Failed to delete container objects
150093 Cannot close container %lu to delete it
150094 Failed to resize %lu - %s
150095 Failed to delete object while deleting container: %s
150096 Failed to end enumeration while deleting container: %s
150097 Failed to close container during delete - attempting delete
150098 Failed to delete container %lu during recovery - %s
150099 Container does not exist
150100 %s, container size=%lu bytes is less then minimum container size, which is 1KB
150101 %s, container size=%lu KB is less then minimum container size, which is 1KB
150102 i_ctnr=%d
150103 stop=%d, i=%d
150105 Failed to end enumeration while evicting objects: %s
150107 Failed to initiate container eviction for %u\n
150108 Could not determine eviction type for container %u\n


# root
160000 failed to get events; errno=%d (%s)
160001 failed to get events; num_events=%d, errno=%d (%s), use_paio=%d, io_ctxt=%p
160002 io_ctxt == 0! (%p)
160003 not enough space, needed %lu available %lu
160004 %lu segments allcated to shard %lu, free_seg_curr=%lu
160005 failed to read flash desc, offset=%lu, copy=%lu, rc=%d
160006 invalid flash desc checksum, offset=%lu, copy=%lu
160007 invalid flash desc, offset=%lu, copy=%lu
160008 flash desc version change, offset=%lu, copy=%lu, old=%d, new=%d
160009 failed to read shard props, offset=%ld, ssd=%lu, rc=%d
160010 Invalid property checksum, offset=%lu, ssd=%lu, slot=%lu
160011 invalid property desc, offset=%lu, copy=%lu, slot=%lu
160012 property desc version change, offset=%lu, copy=%lu, slot=%lu, old=%d, new=%d
160013 updating properties, offset=%lu, slot=%lu
160014 failed to update shard props, offset=%lu, slot=%lu, rc=%d
160015 deallocated %d log segments from shardID=%lu, remaining=%lu
160016 attached %d update segments to shardID=%lu, remaining=%lu
160017 detached %d update segments from shardID=%lu, free=%lu
160018 allocated %d log segments to shardID=%lu, remaining=%lu
160019 Segment count (%d) exceeded number of recovery update buffer segments (%lu)!
160020 Node becomes authoritative for persistent containers
160021 IS NODE STARTED FIRST TIME: %d, Is node started in authoritative mode:%d\n
160022 container is peristent, but started in authoritative mode
160023 Instance iteration local tag %d:%u
160024 Node becomes authoritative for persistent containers %d
160025 Node %d does not have any persistent container\n
160026 Node becomes authoritative for persistent containers\n
160027 pwrite failed!
160028 error updating class(shrink)
160029 Failed to find shard for cguid %"PRIu64"
160030 Container is not deleted (busy or error): cguid=%lu(%d), status=%s
160031 Delete request pending. Deleting... cguid=%lu
160032 Already opened or error: %s - %s
160033 %s, size=%ld bytes
160034 %s(cguid=%lu) - %s
160035 %s, size=%d bytes
160036 %s, container size=%d bytes is less then minimum container size, which is 1Gb
160038 Flash Data Fabric:%s
160039 Container must be open to execute a read object
160040 Container must be open to execute a write object
160041 Container must be open to execute a delete object
160042 Container must be open to execute a container enumeration
160043 Container must be open to execute a flush object
160044 Container must be open to execute a flush container
160045 fifo_shardClose not implemented!
160046 ENTERING, shard_id=%lu durability_level=%u
160047 Failed to get container metadata for VMC
160048 Metadata for cguid %"PRIu64" has already been loaded!
160050 Container %ld has only %d hash bits while the FDF cache uses %d hash bits!
160051 hash_fn must be non-NULL!
160052 failed to allocate key buffer
160053 Unable to write string %s
160054 Unable to open stats file %s
160055 No container exists
160056 Unable to create thread state(%d)\n
160057 Unable to open socket for admin port
160058 Unable to bind admin port %u
160059 Unable to accept new connections
160060 Admin thread exiting...
160061 Using writeback caching with store mode containers can result in lost data if the system crashes
160062 FDFCreateContainer failed for container %s because %d containers have already been created.
160063 PROP: FDF_CACHE_ALWAYS_MISS=%s
160064 PROP: FDF_STRICT_WRITEBACK=%s
160065 Using writeback caching for store mode container %"PRIu64" may result in data loss if system crashes.
160066 %ld asynchronous writes have failed!  This message is displayed only every %d failures.
160067 %ld asynchronous writebacks have failed!  This message is displayed only every %d failures.
160068 %ld asynchronous flushes have failed!  This message is displayed only every %d failures.
160069 %p
160070 Invalid command(%d) received
160071 Memory allocation failed
160072 Invalid FDF state
160073 Unable to initialize thread state\n
160074 Container does not exist. Delete can not proceed
160075 Unable to open container %lu for deleting
160076 Deleting all objects in container %lu failed
160077 Closing container %lu after deleting objects failed
160078 Could not read metadata for %lu. Delete can not proceed\n
160079 Could not clear Metadata for %lu after delete
160080 Container %lu is not cleanedup completlt
160081 Null container Id. Delete can not proceed.
160082 Failed to close container during delete
160083 Delete already under progress fpr %lu
160084 Could not mark delete in progress for container %lu. Delete can not proceed
160085 Failed to initiate the asynchronous container delete
160086 Unsupported command(%d) received
160087 Could not read metadata for %lu. skipping this container from list
160088 Container %lu is being deleted. So not included in the list
160089 Container %lu does not exist
160090 Container %lu is not cleanedup completly
160091 Deleting container %lu
160092 Unable to remove cguid map for container %lu. Delete can not proceed
160093 Could not mark delete in progress for container %lu. 
160094 Unable to create cguid map for container %lu.
160095 Failed to get container metadata for cguid:%lu
160096 %s serialize:%d
160097 Operation denied: Shutdown in progress %s
160098 Thread state is null
160099 Waiting for %d containers deletes to finish
160100 Unable to create thread context %s
160101 Total containers = %d
160102 Error getting container properties for index=%d cguid=%ld: %s
160103 Got container properties for index=%d cguid=%ld: %s
160104 Error closing container ID: %ld with %s
160105 Closed container id %ld with %s
160106 Closed %d containers
160107 Shutdown phase 1 returns :%s
160108 is_fdf_operation_allowed:%s
160109 Unable to get stats for container:%lu
160110 Container does not exist. Can not rename
160111 Could not read metadata for %lu. Can not rename\n
160112 Container %lu is already renamed. \n
160113 Renaming container %s to %s\n
160114 Unable to remove cguid map for container %lu. Can not rename
160115 Unable to write metadata for %lu. Can not rename 
160116 Unable to create cguid map for container %lu.Can not rename
160117 Failed to delete the container:%lu
160118 Start: Waiting for %d containers deletes to finish
160119 End: Waiting for %d containers deletes to finish
160120 Containers closed=%d
160121 Signal completion of pending deletes\n
160122 Waiting for completion of pending deletes\n
160123 Base file name of flash files not set
160124 Device size is less than minimum required
160125 Failed due to an illegal container ID:%s
160129 Shutdown in Progress. Operation not allowed
160136 Opening Container Matadata Container
160137 Creating Container Matadata Container
160138 Container %lu is not found
160139 Container %lu deletion is in progress
160140 Container %lu does not exist:%s
160141 Failed to make room for new object\n
160142 Failed to evict object: %s - %s, continuing to search for victims
160143 Zero or negative size key is provided


# rico
170001 1st error message by Rico (%s)
170002 2nd error message by Rico (%s)
170003 Unable to start the stats thread
170007 TRX cannot be applied due to insufficient memory
170008 TRX too big to apply
170009 TRX sequence anomaly


# efirsov
180000 Removing log file %s
180001 Flush log sync: cannot unlink sync log file %s error=%d
180002 pwrite failed!(%s)
180003 PROP: FDF_SLAB_GC=%s
180004 shard free segments list init failed, shardID=%lu
180005 shard=%p GC threshold adjusted from %d %% to %d %%
180006 ENTERING, shardID=%p
180007 failed to allocate temporary segments map
180008 shard=%p free_segments found %lu, blk_allocated %lu
180009 failed to allocate free segments list
180010 Couldn't create flush log directory %s: %s
180011 shard->id=%ld class->blk_size=%d class->num_segments=%d shard->free_segments_count=%ld
180012 Shard %ld, class->blksize %d, %seligible for%s%s%s GC\n
180013 Shard %ld class->blksize %d, total_slabs=%ld, free_slabs=%d used_slabs=%d average_slabs=%d gc->threshold=%d\n
180014 shard->id=%ld. GC of class->blksize=%d requested
180015 shard->id=%ld. GC freed segment from class->blksize=%d. Shard free segments %ld
180016 shard->id=%ld. signalling gc thread to gc class->slab_blksize=%d class->total_slabs=%ld class->used_slabs=%ld
180017 Couldn't allocate space for shard->id=%ld class=%p blocks=%d
180018 ENTERING, shard->id=%ld, gc threshold=%d
180019 FDF_LOG_O_DIRECT is set
180020 FDF_O_SYNC is set
180021 FDFSetProperty ('%s', '%s'). Old value: %s
