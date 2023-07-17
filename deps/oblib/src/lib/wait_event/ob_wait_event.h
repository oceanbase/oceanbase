/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef WAIT_EVENT_DEF
WAIT_EVENT_DEF(NULL_EVENT, 10000, "", "", "", "", OTHER, "", true)
WAIT_EVENT_DEF(DB_FILE_DATA_READ, 10001, "db file data read", "fd", "offset", "size", USER_IO, "db file data read", true)
WAIT_EVENT_DEF(DB_FILE_DATA_INDEX_READ, 10002, "db file data index read", "fd", "offset", "size", USER_IO, "db file data index read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_READ, 11001, "db file compact read", "fd", "offset", "size", SYSTEM_IO, "db file compact read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_WRITE, 11002, "db file compact write", "fd", "offset", "size", SYSTEM_IO, "db file compact write", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_READ, 11003, "db file index build read", "fd", "offset", "size", SYSTEM_IO, "db file index build read", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_WRITE, 11004, "db file index build write", "fd", "offset", "size", SYSTEM_IO, "db file index build write", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_READ, 11005, "db file migrate read", "fd", "offset", "size", SYSTEM_IO, "db file migrate read", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_WRITE, 11006, "db file migrate write", "fd", "offset", "size", SYSTEM_IO, "db file migrate write", true)
WAIT_EVENT_DEF(BLOOM_FILTER_BUILD_READ, 11007, "bloomfilter build read", "fd", "offset", "size", SYSTEM_IO, "bloomfilter build read", true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_WRITE, 11008, "interm result disk write", "fd", "offset", "size", USER_IO, "interm result disk write", true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_READ, 11009, "interm result disk read", "fd", "offset", "size", USER_IO, "interm result disk read", true)
WAIT_EVENT_DEF(ROW_STORE_DISK_WRITE, 11010, "row store disk write", "fd", "offset", "size", USER_IO, "row store disk write", true)
WAIT_EVENT_DEF(ROW_STORE_DISK_READ, 11011, "row store disk read", "fd", "offset", "size", USER_IO, "row store disk read", true)
WAIT_EVENT_DEF(CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK_WAIT, 11012, "ObCacheLineSegregatedArrayBase alloc mutex", "fd", "offset", "size", USER_IO, "ObCacheLineSegregatedArrayBase alloc mutex", true)
WAIT_EVENT_DEF(SERVER_OBJECT_POOL_ARENA_LOCK_WAIT, 11013, "server object pool arena lock wait", "fd", "offset", "size", USER_IO, "row store disk read", true)
WAIT_EVENT_DEF(MEMSTORE_MEM_PAGE_ALLOC_INFO, 11014, "memstore memory page alloc info", "cur_mem_hold", "sleep_interval", "cur_ts", SYSTEM_IO, "memstore memory page alloc info", true)
WAIT_EVENT_DEF(MEMSTORE_MEM_PAGE_ALLOC_WAIT, 11015, "memstore memory page alloc wait", "cur_mem_hold", "sleep_interval", "cur_ts", SYSTEM_IO, "memstore memory page alloc wait", true)
//scheduler
WAIT_EVENT_DEF(OMT_WAIT, 12001, "sched wait", "req type", "req start timestamp", "wait start timestamp", SCHEDULER, "sched wait", true)
WAIT_EVENT_DEF(OMT_IDLE, 12002, "sched idle", "wait start timestamp", "", "", IDLE, "sched idle", true)

//network
WAIT_EVENT_DEF(SYNC_RPC, 13000, "sync rpc", "pcode", "size", "", NETWORK, "sync rpc", true)
WAIT_EVENT_DEF(MYSQL_RESPONSE_WAIT_CLIENT, 13001, "mysql response wait client", "", "", "", NETWORK, "mysql response wait client", true)
WAIT_EVENT_DEF(DAS_ASYNC_RPC_LOCK_WAIT, 13002, "das wait remote response", "", "", "", NETWORK, "das wait remote response", true)
WAIT_EVENT_DEF(ASYNC_EXTERNAL_TABLE_LOCK_WAIT, 13003, "external table wait remote response", "", "", "", NETWORK, "external table wait remote response", true)

//application
WAIT_EVENT_DEF(MT_READ_LOCK_WAIT,14001,"memstore read lock wait","lock","waiter","owner",APPLICATION,"memstore read lock wait", false)
WAIT_EVENT_DEF(MT_WRITE_LOCK_WAIT,14002,"memstore write lock wait","lock","waiter","owner",APPLICATION,"memstore write lock wait", false)
WAIT_EVENT_DEF(ROW_LOCK_WAIT,14003,"row lock wait","lock","waiter","owner",APPLICATION,"row lock wait", false)

//concurrency
WAIT_EVENT_DEF(PT_LOCATION_CACHE_LOCK_WAIT, 15001, "partition location cache lock wait", "table_id", "partition_id", "", CONCURRENCY, "partition location cache lock wait", true)
WAIT_EVENT_DEF(KV_CACHE_BUCKET_LOCK_WAIT, 15002, "latch: kvcache bucket wait", "address", "number", "tries", CONCURRENCY, "latch: kvcache bucket wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_LOCK_WAIT, 15003, "latch: default spin lock wait", "address", "number", "tries", CONCURRENCY, "latch: default spin lock wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_RWLOCK_WAIT, 15004, "latch: default spin rwlock wait", "address", "number", "tries", CONCURRENCY, "latch: default spin rwlock wait", true)
WAIT_EVENT_DEF(DEFAULT_MUTEX_WAIT, 15005, "latch: default mutex wait", "address", "number", "tries", CONCURRENCY, "latch: default mutex wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_TASK_LOCK_WAIT, 15006, "latch: time wheel task lock wait", "address", "number", "tries", CONCURRENCY, "latch: time wheel task lock wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_BUCKET_LOCK_WAIT, 15007, "latch: time wheel bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: time wheel bucket lock wait", true)
WAIT_EVENT_DEF(ELECTION_LOCK_WAIT, 15008, "spinlock: election lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: election lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_LOCK_WAIT, 15009, "latch: trans ctx lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans ctx lock wait", true)
WAIT_EVENT_DEF(PARTITION_LOG_LOCK_WAIT, 15010, "latch: partition log lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition log lock wait", true)
WAIT_EVENT_DEF(PCV_SET_LOCK_WAIT, 15011, "latch: plan cache pcv set lock wait", "address", "number", "tries", CONCURRENCY, "latch: plan cache pcv set lock wait", true)
WAIT_EVENT_DEF(REWRITE_RULE_ITEM_LOCK_WAIT, 15093, "latch: rewrite rule item lock wait", "address", "number", "tries", CONCURRENCY, "latch: rewrite rule item lock wait", true)

WAIT_EVENT_DEF(CLOG_HISTORY_REPORTER_LOCK_WAIT, 15012, "latch: clog history reporter lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog history reporter lock wait", true)
WAIT_EVENT_DEF(CLOG_EXTERNAL_EXEC_LOCK_WAIT, 15013, "latch: clog external executor lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog external executor lock wait", true)
WAIT_EVENT_DEF(CLOG_MEMBERSHIP_MGR_LOCK_WAIT, 15014, "latch: clog membership mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog membership mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_RECONFIRM_LOCK_WAIT, 15015, "latch: clog reconfirm lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog reconfirm lock wait", true)
WAIT_EVENT_DEF(CLOG_SLIDING_WINDOW_LOCK_WAIT, 15016, "latch: clog sliding window lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog sliding window lock wait", true)
WAIT_EVENT_DEF(CLOG_STAT_MGR_LOCK_WAIT, 15017, "latch: clog stat mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog stat mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_TASK_LOCK_WAIT, 15018, "latch: clog task lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog task lock wait", true)
WAIT_EVENT_DEF(CLOG_IDMGR_LOCK_WAIT, 15019, "latch: clog id mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog id mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_CACHE_LOCK_WAIT, 15020, "latch: clog cache lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog cache lock wait", true)
WAIT_EVENT_DEF(ELECTION_MSG_LOCK_WAIT, 15021, "latch: election msg lock wait", "address", "number", "tries", CONCURRENCY, "latch: election msg lock wait", true)
WAIT_EVENT_DEF(PLAN_SET_LOCK_WAIT, 15022, "latch: plan set lock wait", "address", "number", "tries", CONCURRENCY, "latch: plan set lock wait", true)
WAIT_EVENT_DEF(PS_STORE_LOCK_WAIT, 15023, "latch: ps store lock wait", "address", "number", "tries", CONCURRENCY, "latch: ps store lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_MGR_LOCK_WAIT, 15024, "latch: trans context mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans context mgr lock wait", true)
WAIT_EVENT_DEF(DEFAULT_RECURSIVE_MUTEX_WAIT, 15025, "latch: default recursive mutex wait", "address", "number", "tries", CONCURRENCY, "latch: default recursive mutex wait", true)
WAIT_EVENT_DEF(DEFAULT_DRW_LOCK_WAIT, 15026, "latch: default drw lock wait", "address", "number", "tries", CONCURRENCY, "latch: default drw lock wait", true)
WAIT_EVENT_DEF(DEFAULT_BUCKET_LOCK_WAIT, 15027, "latch: default bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: default bucket lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_BUCKET_LOCK_WAIT, 15028, "latch: trans ctx bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans ctx bucket lock wait", true)
WAIT_EVENT_DEF(MACRO_WRITER_LOCK_WAIT, 15029, "latch: macro writer lock wait", "address", "number", "tries", CONCURRENCY, "latch: macro writer lock wait", true)
WAIT_EVENT_DEF(TOKEN_BUCKET_LOCK_WAIT, 15030, "latch: token bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: token bucket lock wait", true)
WAIT_EVENT_DEF(LIGHTY_HASHMAP_BUCKET_LOCK_WAIT, 15031, "latch: lighty hashmap bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: lighty hashmap bucket lock wait", true)
WAIT_EVENT_DEF(ROW_CALLBACK_LOCK_WAIT, 15032, "latch: row callback lock wait", "address", "number", "tries", CONCURRENCY, "latch: row callback lock wait", true)
WAIT_EVENT_DEF(LS_LOCK_WAIT, 15033, "latch: ls lock wait", "address", "number", "tries", CONCURRENCY, "latch: ls lock wait", true)
WAIT_EVENT_DEF(SWITCH_STAGGER_MERGE_FLOW_WAIT, 15034, "latch: switch stagger merge flow wait", "address", "number", "tries", CONCURRENCY, "latch: switch stagger merge flow wait", true)
WAIT_EVENT_DEF(SWITCH_LEADER_WAIT, 15035, "latch: switch leader wait", "address", "number", "tries", CONCURRENCY, "latch: switch leader wait", true)
WAIT_EVENT_DEF(PARTITION_FREEZE_WAIT, 15036, "latch: partition freeze wait", "address", "number", "tries", CONCURRENCY, "latch: partition freeze wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_LOCK_WAIT, 15037, "latch: schema service wait", "address", "number", "tries", CONCURRENCY, "latch: schema service wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_STATS_LOCK_WAIT, 15038, "latch: schema service stats wait", "address", "number", "tries", CONCURRENCY, "latch: schema service stats wait", true)
WAIT_EVENT_DEF(TENANT_LOCK_WAIT, 15039, "latch: tenant lock wait", "address", "number", "tries", CONCURRENCY, "latch: tenant lock wait", true)
WAIT_EVENT_DEF(CONFIG_LOCK_WAIT, 15040, "latch: config lock wait", "address", "number", "tries", CONCURRENCY, "latch: config lock wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_LOCK_WAIT, 15041, "latch: major_freeze lock wait", "address", "number", "tries", CONCURRENCY, "latch: major_freeze lock wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_LOCK_WAIT, 15042, "latch: partition table updater lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition table updater wait", true)
WAIT_EVENT_DEF(MULTI_TENANT_LOCK_WAIT, 15043, "latch: multi tenant lock wait", "address", "number", "tries", CONCURRENCY, "latch: multi tenant lock wait", true)
WAIT_EVENT_DEF(LEADER_COORDINATOR_LOCK_WAIT, 15044, "latch: leader coordinator lock wait", "address", "number", "tries", CONCURRENCY, "latch: leader coordinator lock wait", true)
WAIT_EVENT_DEF(LEADER_STAT_LOCK_WAIT, 15045, "latch: leader stat lock wait", "address", "number", "tries", CONCURRENCY, "latch: leader stat lock wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_SERVICE_LOCK_WAIT, 15046, "latch: major_freeze service lock wait", "address", "number", "tries", CONCURRENCY, "latch: major_freeze service lock wait", true)
WAIT_EVENT_DEF(RS_BOOTSTRAP_LOCK_WAIT, 15047, "latch: rs bootstrap lock wait", "address", "number", "tries", CONCURRENCY, "latch: rs bootstap lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_ITEM_LOCK_WAIT, 15048, "latch: schema mgr item lock wait", "address", "number", "tries", CONCURRENCY, "latch: schema mgr item lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_LOCK_WAIT, 15049, "latch: schema mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: schema mgr lock wait", true)
WAIT_EVENT_DEF(SUPER_BLOCK_LOCK_WAIT, 15050, "latch: super block lock wait", "address", "number", "tries", CONCURRENCY, "latch: super block lock wait", true)
WAIT_EVENT_DEF(FROZEN_VERSION_LOCK_WAIT, 15051, "latch: frozen version lock wait", "address", "number", "tries", CONCURRENCY, "latch: frozen version lock wait", true)
WAIT_EVENT_DEF(RS_BROADCAST_LOCK_WAIT, 15052, "latch: rs broadcast lock wait", "address", "number", "tries", CONCURRENCY, "latch: rs broadcast lock wait", true)
WAIT_EVENT_DEF(SERVER_STATUS_LOCK_WAIT, 15053, "latch: server status lock wait", "address", "number", "tries", CONCURRENCY, "latch: server status lock wait", true)
WAIT_EVENT_DEF(SERVER_MAINTAINCE_LOCK_WAIT, 15054, "latch: server maintaince lock wait", "address", "number", "tries", CONCURRENCY, "latch: server maintaince lock wait", true)
WAIT_EVENT_DEF(UNIT_MANAGER_LOCK_WAIT, 15055, "latch: unit manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: unit manager lock wait", true)
WAIT_EVENT_DEF(ZONE_MANAGER_LOCK_WAIT, 15056, "latch: zone manager maintaince lock wait", "address", "number", "tries", CONCURRENCY, "latch: zone manager maintaince lock wait", true)
WAIT_EVENT_DEF(ALLOC_OBJECT_LOCK_WAIT, 15057, "latch: alloc object lock wait", "address", "number", "tries", CONCURRENCY, "latch: alloc object lock wait", true)
WAIT_EVENT_DEF(ALLOC_BLOCK_LOCK_WAIT, 15058, "latch: alloc block lock wait", "address", "number", "tries", CONCURRENCY, "latch: alloc block lock wait", true)
WAIT_EVENT_DEF(TRACE_RECORDER_LOCK_WAIT, 15059, "latch: trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: normal trace recorder lock wait", true)
WAIT_EVENT_DEF(SESSION_TRACE_RECORDER_LOCK_WAIT, 15060, "latch: session trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: session trace recorder lock wait", true)
WAIT_EVENT_DEF(TRANS_TRACE_RECORDER_LOCK_WAIT, 15061, "latch: trans trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans trace recorder lock wait", true)
WAIT_EVENT_DEF(ELECT_TRACE_RECORDER_LOCK_WAIT, 15062, "latch: election trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: election trace recorder lock wait", true)
WAIT_EVENT_DEF(ALIVE_SERVER_TRACER_LOCK_WAIT, 15063, "latch: alive server tracer lock wait", "address", "number", "tries", CONCURRENCY, "latch: alive server tracer lock wait", true)
WAIT_EVENT_DEF(ALLOC_CHUNK_LOCK_WAIT, 15064, "latch: allocator chunk lock wait", "address", "number", "tries", CONCURRENCY, "latch: allocator chunk lock wait", true)
WAIT_EVENT_DEF(ALLOC_TENANT_LOCK_WAIT, 15065, "latch: allocator tenant lock wait", "address", "number", "tries", CONCURRENCY, "latch: tenant lock in allocator lock wait", true)
WAIT_EVENT_DEF(IO_QUEUE_LOCK_WAIT, 15066, "latch: io queue lock wait", "address", "number", "tries", CONCURRENCY, "latch: io queue lock wait", true)
WAIT_EVENT_DEF(ZONE_INFO_RW_LOCK_WAIT, 15067, "latch: zone infos rw lock wait", "address", "number", "tries", CONCURRENCY, "latch: zone infos rw lock wait", true)
WAIT_EVENT_DEF(MT_TRACE_RECORDER_LOCK_WAIT, 15068, "latch: memtable trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: memtable trace recorder lock wait", true)
WAIT_EVENT_DEF(BANDWIDTH_THROTTLE_LOCK_WAIT, 15069, "latch: bandwidth throttle lock wait", "address", "number", "tries", CONCURRENCY, "latch: bandwidth throttle lock wait", true)
WAIT_EVENT_DEF(RS_EVENT_TS_LOCK_WAIT, 15070, "latch: rs event table timestamp lock wait", "address", "number", "tries", CONCURRENCY, "latch: rs event table timestamp lock wait", true)
WAIT_EVENT_DEF(CLOG_FD_CACHE_LOCK_WAIT, 15071, "latch: clog fd cache lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog fd cache lock wait", true)
WAIT_EVENT_DEF(MIGRATE_LOCK_WAIT, 15072, "latch: migrate lock wait", "address", "number", "tries", CONCURRENCY, "latch: migrate lock wait", true)
WAIT_EVENT_DEF(CLOG_CASCADING_INFO_LOCK_WAIT, 15073, "latch: clog cascading info lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog cascading info lock wait", true)
WAIT_EVENT_DEF(CLOG_LOCALITY_LOCK_WAIT, 15074, "latch: clog locality lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog locality lock wait", true)
WAIT_EVENT_DEF(PRIORITY_TASK_QUEUE_LOCK_WAIT, 15075, "latch: priority task queue lock wait", "address", "number", "tries", CONCURRENCY, "latch: priority task queue lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_LOCK_WAIT, 15076, "latch: group migrate lock wait", "address", "number", "tries", CONCURRENCY, "latch: group migrate lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_TASK_LOCK_WAIT, 15077, "latch: group migrate task lock wait", "address", "number", "tries", CONCURRENCY, "latch: group migrate task lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_TASK_IDLE_WAIT, 15078, "latch: group migrate task idle wait", "address", "number", "tries", CONCURRENCY, "latch: group migrate task idle wait", true)
WAIT_EVENT_DEF(LOG_ENGINE_ENV_SWITCH_LOCK_WAIT, 15079, "latch: log engine env switch lock wait", "address", "number", "tries", CONCURRENCY, "latch: log engine env switch lock wait", true)
WAIT_EVENT_DEF(CLOG_CONSEC_INFO_MGR_LOCK_WAIT, 15081, "latch: clog consec info mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog consec info mgr lock wait", true)
WAIT_EVENT_DEF(ELECTION_GROUP_LOCK_WAIT, 15082, "latch: election group lock wait", "address", "number", "tries", CONCURRENCY, "latch: election group lock wait", true)
WAIT_EVENT_DEF(ELECTION_GROUP_TRACE_RECORDER_LOCK_WAIT, 15083, "latch: election group trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: election group trace recorder lock wait", true)
WAIT_EVENT_DEF(LATCH_WAIT_QUEUE_LOCK_WAIT, 15084, "latch: wait queue lock wait", "address", "number", "tries", CONCURRENCY, "latch: wait queue lock wait", true)
WAIT_EVENT_DEF(ID_MAP_NODE_LOCK_WAIT, 15085, "latch: id map node lock wait", "address", "number", "tries", CONCURRENCY, "latch: id map node lock wait", true)
WAIT_EVENT_DEF(BLOCK_MANAGER_LOCK_WAIT, 15086, "latch: block manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: block manager lock wait", true)
WAIT_EVENT_DEF(EXTENT_STORAGE_MANAGER_LOCK_WAIT, 15087, "latch: extent storage manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: extent storage manager lock wait", true)
WAIT_EVENT_DEF(REBUILD_RETRY_LIST_LOCK_WAIT, 15088, "latch: rebuild retry list lock wait", "address", "number", "tries", CONCURRENCY, "latch: rebuild retry list lock wait", true)
WAIT_EVENT_DEF(CLOG_SWITCH_INFO_LOCK_WAIT, 15089, "latch: clog switch info lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog switch info lock wait", true)
WAIT_EVENT_DEF(CLOG_IDC_LOCK_WAIT, 15090, "latch: clog idc lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog idc lock wait", true)
WAIT_EVENT_DEF(TABLET_BUCKET_LOCK_WAIT, 15091, "latch: tablet bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: tablet bucket lock wait", true)
WAIT_EVENT_DEF(TENANT_META_MEM_MGR_LOCK_WAIT, 15092, "latch: tenant meta memory manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: tenant meta memory manager lock wait", true)
WAIT_EVENT_DEF(SLOG_PROCESSING_MUTEX_WAIT, 15093, "latch: slog processing mutex wait", "address", "number", "tries", CONCURRENCY, "latch: slog processing mutex wait", true)
WAIT_EVENT_DEF(TABLET_TABLE_STORE_LOCK_WAIT, 15094, "latch: tablet table store lock wait", "address", "number", "tries", CONCURRENCY, "latch: tablet table store lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_LOCK_WAIT, 15095, "latch: tmp file lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_EXTENT_LOCK_WAIT, 15096, "latch: tmp file extent lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file extent lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_MGR_LOCK_WAIT, 15097, "latch: tmp file mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file mgr lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_STORE_LOCK_WAIT, 15098, "latch: tmp file store lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file store lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_MACRO_LOCK_WAIT, 15099, "latch: tmp file macro lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file macro lock wait", true)
WAIT_EVENT_DEF(INDEX_BUILDER_LOCK_WAIT, 15100, "latch: sstable index builder lock wait", "address", "number", "tries", CONCURRENCY, "latch: sstable index builder lock wait", true)
WAIT_EVENT_DEF(DEFAULT_COND_WAIT, 15101, "default condition wait", "address", "", "", CONCURRENCY, "default condition wait", true)
WAIT_EVENT_DEF(DEFAULT_SLEEP, 15102, "sleep wait", "sleep_interval", "", "", CONCURRENCY, "sleep wait", true)
WAIT_EVENT_DEF(CLOG_WRITER_COND_WAIT, 15103, "clog writer condition wait", "address", "", "", CONCURRENCY, "clog writer condition wait", true)
WAIT_EVENT_DEF(IO_CONTROLLER_COND_WAIT, 15104, "io controller condition wait", "address", "", "", CONCURRENCY, "io controller condition wait", true)
WAIT_EVENT_DEF(IO_PROCESSOR_COND_WAIT, 15105, "io processor condition wait", "address", "", "", CONCURRENCY, "io processor condition wait", true)
WAIT_EVENT_DEF(DEDUP_QUEUE_COND_WAIT, 15106, "dedup queue condition wait", "address", "", "", CONCURRENCY, "dedup queue condition wait", true)
WAIT_EVENT_DEF(SEQ_QUEUE_COND_WAIT, 15107, "seq queue condition wait", "address", "", "", CONCURRENCY, "seq queue condition wait", true)
WAIT_EVENT_DEF(INNER_CONNECTION_POOL_COND_WAIT, 15108, "inner connection pool condition wait", "address", "", "", CONCURRENCY, "inner connection pool condition wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_COND_WAIT, 15109, "partition table updater condition wait", "address", "", "", CONCURRENCY, "partition table updater condition wait", true)
WAIT_EVENT_DEF(REBALANCE_TASK_MGR_COND_WAIT, 15110, "rebalance task mgr condition wait", "address", "", "", CONCURRENCY, "rebalance task mgr condition wait", true)
WAIT_EVENT_DEF(ASYNC_RPC_PROXY_COND_WAIT, 15111, "async rpc proxy condition wait", "address", "", "", CONCURRENCY, "async rpc proxy condition wait", true)
WAIT_EVENT_DEF(THREAD_IDLING_COND_WAIT, 15112, "thread idling condition wait", "address", "", "", CONCURRENCY, "thread idling condition wait", true)
WAIT_EVENT_DEF(RPC_SESSION_HANDLER_COND_WAIT, 15113, "rpc session handler condition wait", "address", "", "", CONCURRENCY, "rpc session handler condition wait", true)
WAIT_EVENT_DEF(LOCATION_CACHE_COND_WAIT, 15114, "location cache condition wait", "address", "", "", CONCURRENCY, "location cache condition wait", true)
WAIT_EVENT_DEF(REENTRANT_THREAD_COND_WAIT, 15115, "reentrant thread condition wait", "address", "", "", CONCURRENCY, "reentrant thread condition wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_COND_WAIT, 15116, "major freeze condition wait", "address", "", "", CONCURRENCY, "major freeze condition wait", true)
WAIT_EVENT_DEF(MINOR_FREEZE_COND_WAIT, 15117, "minor freeze condition wait", "address", "", "", CONCURRENCY, "minor freeze condition wait", true)
WAIT_EVENT_DEF(TH_WORKER_COND_WAIT, 15118, "th worker condition wait", "address", "", "", CONCURRENCY, "th worker condition wait", true)
WAIT_EVENT_DEF(DEBUG_SYNC_COND_WAIT, 15119, "debug sync condition wait", "address", "", "", CONCURRENCY, "debug sync condition wait", true)
WAIT_EVENT_DEF(EMPTY_SERVER_CHECK_COND_WAIT, 15120, "empty server check condition wait", "address", "", "", CONCURRENCY, "empty server check condition wait", true)
WAIT_EVENT_DEF(SCHEDULER_COND_WAIT, 15121, "ob scheduler condition wait", "address", "", "", CONCURRENCY, "ob scheduler condition wait", true)
WAIT_EVENT_DEF(RESTORE_READER_COND_WAIT, 15122, "ob restore reader condition wait", "address", "", "", CONCURRENCY, "ob restore reader condition wait", true)
WAIT_EVENT_DEF(DYNAMIC_THREAD_POOL_COND_WAIT, 15123, "ob dynamic thread pool condition wait", "address", "", "", CONCURRENCY, "ob dynamic thread pool condition wait", true)
WAIT_EVENT_DEF(DELETE_DISK_COND_WAIT, 15124, "delete disk condition wait", "address", "", "", CONCURRENCY, "delete disk condition wait", true)
WAIT_EVENT_DEF(SLOG_FLUSH_COND_WAIT, 15125, "slog flush condition wait", "address", "", "", CONCURRENCY, "slog flush condition wait", true)
WAIT_EVENT_DEF(BUILD_INDEX_SCHEDULER_COND_WAIT, 15126, "build index scheduler condition wait", "address", "", "", CONCURRENCY, "build index scheduler condition wait", true)
WAIT_EVENT_DEF(TBALE_MGR_LOCK_WAIT, 15127, "table mgr wait", "address", "number", "tries", CONCURRENCY, "table mgr lock wait", false)
WAIT_EVENT_DEF(PARTITION_STORE_LOCK_WAIT, 15128, "partition store lock wait", "address", "number", "tries", CONCURRENCY, "partition store lock wait", false)
WAIT_EVENT_DEF(PARTITION_STORE_CHANGE_LOCK_WAIT, 15129, "partition store change lock wait", "address", "number", "tries", CONCURRENCY, "partition store change lock wait", false)
WAIT_EVENT_DEF(TABLET_MEMTABLE_LOCK_WAIT, 15130, "tablet memtable lock wait", "address", "number", "tries", CONCURRENCY, "tablet memtable lock wait", true)
WAIT_EVENT_DEF(BUILD_INDEX_COMPACT_COND_WAIT, 15131, "build index compaction condition wait", "address", "", "", CONCURRENCY, "build index compaction condition wait", false)
WAIT_EVENT_DEF(DAG_WORKER_COND_WAIT, 15132, "dag worker condition wait", "address", "", "", CONCURRENCY, "dag worker condition wait", false)
WAIT_EVENT_DEF(TBALE_MGR_MAP_WAIT, 15133, "table mgr map wait", "address", "number", "tries", CONCURRENCY, "table mgr map wait", false)
WAIT_EVENT_DEF(IO_CALLBACK_QUEUE_LOCK_WAIT, 15134, "io callback queue lock wait", "address", "number", "tries", CONCURRENCY, "io callback queue lock wait", true)
WAIT_EVENT_DEF(IO_CHANNEL_LOCK_WAIT, 15135, "io channel lock wait", "address", "number", "tries", CONCURRENCY, "io channel lock wait", true)
WAIT_EVENT_DEF(PARTITION_AUDIT_SPIN_LOCK_WAIT, 15136, "latch: partition audit spin lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition audit spin lock wait", true)
WAIT_EVENT_DEF(PARTITION_GROUP_LOCK_WAIT, 15137, "partition group lock wait", "address", "number", "tries", CONCURRENCY, "partition group lock wait", false)
WAIT_EVENT_DEF(PX_WORKER_LEADER_LOCK_WAIT, 15138, "latch: px worker leader spin lock wait", "address", "number", "tries", CONCURRENCY, "latch:  px worker leader spin lock wait", true)
WAIT_EVENT_DEF(RAID_WRITE_RECORD_LOCK_WAIT, 15139, "latch: raid write record lock wait", "address", "number", "tries", CONCURRENCY, "latch:  raid write record lock wait", true)
WAIT_EVENT_DEF(RS_GTS_TASK_MGR_COND_WAIT, 15140, "rs gts task mgr condition wait", "address", "", "", CONCURRENCY, "rs gts task mgr condition wait", true)
WAIT_EVENT_DEF(OB_ALLOCATOR_LOCK_WAIT, 15141, "latch: ob allocator lock wait", "address", "number", "tries", CONCURRENCY, "latch: ob allocator lock wait", true)
WAIT_EVENT_DEF(BLOCK_ID_GENERATOR_LOCK_WAIT, 15142, "latch: block id generator lock wait", "address", "number", "tries", CONCURRENCY, "latch: block id generator lock wait", true)
WAIT_EVENT_DEF(OB_CONTEXT_LOCK_WAIT, 15143, "ob context lock wait", "address", "", "", CONCURRENCY, "ob context lock wait", true)
WAIT_EVENT_DEF(OB_LOG_ARCHIVE_SCHEDULER_LOCK_WAIT, 15144, "ob log archive scheduler lock wait", "address", "", "", CONCURRENCY, "ob log archive scheduler lock wait", true)
WAIT_EVENT_DEF(OB_REQ_TIMEINFO_LIST_WAIT, 15145, "ob request timeinfo list wait", "address", "", "", CONCURRENCY, "ob reqyest timeinfo list wait", true)
WAIT_EVENT_DEF(BACKUP_INFO_MGR_LOCK_WAIT, 15146, "backup info wait", "address", "number", "tries", CONCURRENCY, "backup info lock wait", false)
WAIT_EVENT_DEF(HASH_MAP_LOCK_WAIT, 15147, "hashmap lock wait", "address", "number", "tries", CONCURRENCY, "hashmap lock wait", true)
WAIT_EVENT_DEF(FILE_REF_LOCK_WAIT, 15148, "file ref lock wait", "address", "number", "tries", CONCURRENCY, "file ref lock wait", true)
WAIT_EVENT_DEF(TIMEZONE_LOCK_WAIT, 15149, "latch: timezone lock wait", "address", "number", "tries", CONCURRENCY, "latch: timezone lock wait", true)
WAIT_EVENT_DEF(XA_STMT_LOCK_WAIT, 15150, "latch: xa stmt lock wait", "address", "number", "tries", CONCURRENCY, "latch: xa stmt lock wait", true)
WAIT_EVENT_DEF(XA_QUEUE_LOCK_WAIT, 15151, "latch: xa queue lock wait", "address", "number", "tries", CONCURRENCY, "latch: xa queue lock wait", true)
WAIT_EVENT_DEF(MEMTABLE_CALLBACK_LIST_LOCK_WAIT, 15152, "latch: memtable callback list lock wait", "address", "number", "tries", CONCURRENCY, "latch: memtable callback list lock wait", true)
WAIT_EVENT_DEF(MEMTABLE_CALLBACK_LIST_MGR_LOCK_WAIT, 15153, "latch: memtable callback list mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: memtable callback list mgr lock wait", true)
WAIT_EVENT_DEF(XA_CTX_LOCK_WAIT, 15154, "latch: xa ctx lock wait", "address", "number", "tries", CONCURRENCY, "latch: xa queue lock wait", true)
WAIT_EVENT_DEF(MASTER_KEY_LOCK_WAIT, 15155, "latch: master key lock wait", "address", "number", "tries", CONCURRENCY, "latch: master key lock wait", true)
WAIT_EVENT_DEF(RATELIMIT_LOCK_WAIT, 15156, "latch: ratelimit lock wait", "address", "number", "tries", CONCURRENCY, "latch: ratelimit lock wait", true)
WAIT_EVENT_DEF(MIGRATE_RETRY_WORKER_COND_WAIT, 15157, "migrate retry queue worker condition wait", "address", "", "", CONCURRENCY, "migrate retry queue worker condition wait", false)
WAIT_EVENT_DEF(BACKUP_TASK_SCHEDULER_COND_WAIT, 15158, "backup scheduler condition wait", "address", "", "", CONCURRENCY, "backup scheduler condition wait", true)
WAIT_EVENT_DEF(HA_SERVICE_COND_WAIT, 15159, "ha service condition wait", "address", "", "", CONCURRENCY, "ha service condition wait", false)
WAIT_EVENT_DEF(PX_LOOP_COND_WAIT, 15160, "px loop condition wait", "address", "", "", CONCURRENCY, "px loop condition wait", true)
WAIT_EVENT_DEF(DTL_CHANNEL_LIST_LOCK_WAIT, 15161, "latch:dtl channel list lock wait", "address", "", "", CONCURRENCY, "dtl channel list lock wait", true)
WAIT_EVENT_DEF(DTL_CACHED_BUFFER_LIST_LOCK_WAIT, 15162, "latch:dtl free buffer list lock wait", "address", "", "", CONCURRENCY, "dtl free buffer list lock wait", true)
WAIT_EVENT_DEF(PS_CACHE_EVICT_LOCK_WAIT, 15163, "latch:plan cache evict lock wait", "address", "", "", CONCURRENCY, "plan cache evict lock wait", true)
WAIT_EVENT_DEF(SQL_SHARED_HJ_LOCK_WAIT, 15164, "latch:shared hash join cond lock wait", "address", "", "", CONCURRENCY, "shared hash join cond lock wait", true)
WAIT_EVENT_DEF(SQL_SHARED_HJ_COND_WAIT, 15165, "mutex: shared hash join cond wait", "address", "", "", CONCURRENCY, "shared hash join cond wait", true)
WAIT_EVENT_DEF(SQL_WA_PROFILE_LIST_LOCK_WAIT, 15166, "latch: sql work area profile list lock wait", "address", "", "", CONCURRENCY, "sql work area profile list lock wait", true)
WAIT_EVENT_DEF(SQL_WA_STAT_MAP_LOCK_WAIT, 15167, "latch: sql work area stat map lock wait", "address", "", "", CONCURRENCY, "sql work area profile stat map lock wait", true)
WAIT_EVENT_DEF(SQL_MEMORY_MGR_MUTEX_LOCK_WAIT, 15168, "latch: sql memory manager mutex lock","address", "", "", CONCURRENCY, "sql memory manager mutex lock", true)
WAIT_EVENT_DEF(LOAD_DATA_RPC_CB_LOCK_WAIT, 15169, "latch: load data rpc asyn callback lock", "address", "", "", CONCURRENCY, "load data rpc asyn callback lock", true)
WAIT_EVENT_DEF(SQL_DYN_SAMPLE_MSG_LOCK_WAIT, 15170, "latch: merge dynamic sampling piece message lock", "address", "", "", CONCURRENCY, "merge dynamic sampling piece message lock", true)
WAIT_EVENT_DEF(SQL_GI_SHARE_POOL_LOCK_WAIT, 15171, "latch: granule iterator task queue lock","address", "", "", CONCURRENCY, "granule iterator task queue lock", true)
WAIT_EVENT_DEF(DTL_RECV_CHANNEL_PROVIDER_LOCK_WAIT, 15172, "latch: dtl receive channel provider access lock", "address", "", "", CONCURRENCY, "dtl receive channel provider access lock", true)
WAIT_EVENT_DEF(PX_TENANT_TARGET_LOCK_WAIT, 15173, "latch: parralel execution tenant target lock", "address", "", "", CONCURRENCY, "parralel execution tenant target lock", true)
WAIT_EVENT_DEF(PX_WORKER_STAT_LOCK_WAIT, 15174, "latch: parallel execution worker stat lock", "address", "", "", CONCURRENCY, "parallel execution worker stat lock", true)
WAIT_EVENT_DEF(SESSION_QUERY_LOCK_WAIT, 15175, "latch: session query lock", "address", "", "", CONCURRENCY, "session query lock", true)
WAIT_EVENT_DEF(SESSION_THREAD_DATA_LOCK_WAIT, 15176, "latch: session thread data lock", "address", "", "", CONCURRENCY, "session thread data lock", true)
WAIT_EVENT_DEF(SESSION_POOL_MAP_LOCK_WAIT, 15177, "latch: maintain session pool lock", "address", "", "", CONCURRENCY, "maintain session pool lock", true)
WAIT_EVENT_DEF(SEQUENCE_VALUE_ALLOC_LOCK_WAIT, 15178, "latch: sequence value alloc lock", "address", "", "", CONCURRENCY, "sequence value alloc lock", true)
WAIT_EVENT_DEF(SEQUENCE_CACHE_LOCK_WAIT, 15179, "latch: sequence cache lock", "address", "", "", CONCURRENCY, "sequence cache lock", true)
WAIT_EVENT_DEF(INNER_CONN_POOL_LOCK_WAIT, 15180, "latch: inner connection pool lock", "address", "", "", CONCURRENCY, "inner connection pool lock", true)
WAIT_EVENT_DEF(TENANT_RES_MGR_LIST_LOCK_WAIT, 15181, "latch: tenant resource mgr list lock",  "address", "", "", CONCURRENCY, "tenant resource mgr list lock", true)
WAIT_EVENT_DEF(TC_FREE_LIST_LOCK_WAIT, 15183, "latch: tc free list lock",  "address", "", "", CONCURRENCY, "tc free list lock", true)
WAIT_EVENT_DEF(DEDUP_QUEUE_LOCK_WAIT, 15184, "latch: dedup queue lock", "address", "", "", CONCURRENCY, "dedup queue lock", true)
WAIT_EVENT_DEF(SLOG_CKPT_LOCK_WAIT, 15185, "slog checkpoint lock wait", "address", "", "", CONCURRENCY, "slog checkpoint lock wait", true)
WAIT_EVENT_DEF(LOCAL_DEVICE_LOCK_WAIT, 15186, "local device lock wait", "address", "", "", CONCURRENCY, "local device lock wait", true)
WAIT_EVENT_DEF(FIXED_SIZE_ALLOCATOR_LOCK_WAIT, 15187, "fixed size allocator lock wait", "address", "", "", CONCURRENCY, "fixed size allocator lock wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_SWITCH_LOCK_WAIT, 15188, "latch:major_freeze switch lock wait", "address", "number", "tries", CONCURRENCY, "latch:major_freeze switch lock wait", true)
WAIT_EVENT_DEF(ZONE_MERGE_MANAGER_READ_LOCK_WAIT, 15189, "latch:zone merge manager read lock wait", "address", "number", "tries", CONCURRENCY, "latch:zone merge manager read lock wait", true)
WAIT_EVENT_DEF(ZONE_MERGE_MANAGER_WRITE_LOCK_WAIT, 15190, "latch:zone merge manager write lock wait", "address", "number", "tries", CONCURRENCY, "latch:zone merge manager write lock wait", true)
WAIT_EVENT_DEF(AUTO_INCREMENT_INIT_LOCK_WAIT, 15191, "latch: auto increment table node init lock wait", "address", "", "", CONCURRENCY, "auto increment init lock", true)
WAIT_EVENT_DEF(AUTO_INCREMENT_ALLOC_LOCK_WAIT, 15192, "latch: auto increment alloc lock wait", "address", "", "", CONCURRENCY, "auto increment alloc lock", true)
WAIT_EVENT_DEF(AUTO_INCREMENT_SYNC_LOCK_WAIT, 15193, "latch: auto increment sync lock wait", "address", "", "", CONCURRENCY, "auto increment sync lock", true)
WAIT_EVENT_DEF(AUTO_INCREMENT_GAIS_LOCK_WAIT, 15194, "latch: auto increment global service lock wait", "address", "", "", CONCURRENCY, "auto increment gais lock", true)
WAIT_EVENT_DEF(AUTO_INCREMENT_LEADER_LOCK_WAIT, 15195, "latch: auto increment get leader lock wait", "address", "", "", CONCURRENCY, "auto increment leader lock", true)
WAIT_EVENT_DEF(ALLOC_MEM_DUMP_TASK_WAIT, 15198, "latch: alloc memory dump task wait", "address", "number", "tries", CONCURRENCY, "latch: alloc memory dump task wait", true)
WAIT_EVENT_DEF(ALLOC_ESS_WAIT, 15199, "latch: alloc expand, shrink and segment wait", "address", "number", "tries", CONCURRENCY, "latch: alloc expand, shrink and segment wait", true)
WAIT_EVENT_DEF(CONCURRENT_BITSET_WAIT, 15200, "latch: concurrent bitset wait", "address", "number", "tries", CONCURRENCY, "latch: concurrent bitset wait", true)
WAIT_EVENT_DEF(OB_SEG_ARRAY_WAIT, 15201, "latch: seg array wait", "address", "number", "tries", CONCURRENCY, "latch: seg array wait", true)
WAIT_EVENT_DEF(OB_AREAN_ALLOCATOR_WAIT, 15202, "latch: arena allocator wait", "address", "number", "tries", CONCURRENCY, "latch: arena allocator wait", true)
WAIT_EVENT_DEF(OB_CACHED_ALLOCATOR_WAIT, 15203, "latch: cached allocator wait", "address", "number", "tries", CONCURRENCY, "latch: cached allocator wait", true)
WAIT_EVENT_DEF(OB_DELAY_FREE_ALLOCATOR_WAIT, 15204, "latch: delay free allocator wait", "address", "number", "tries", CONCURRENCY, "latch: delay free allocator wait", true)
WAIT_EVENT_DEF(OB_FIFO_ALLOCATOR_WAIT, 15205, "latch: FIFO allocator wait", "address", "number", "tries", CONCURRENCY, "latch: FIFO allocator wait", true)
WAIT_EVENT_DEF(PAGE_MANAGER_WAIT, 15206, "latch: page manager wait", "address", "number", "tries", CONCURRENCY, "latch: page manager wait", true)
WAIT_EVENT_DEF(SIMPLE_FIFO_ALLOCATOR_WAIT, 15207, "latch: simple FIFO allocator wait", "address", "number", "tries", CONCURRENCY, "latch: simple FIFO allocator wait", true)
WAIT_EVENT_DEF(OB_DLIST_WAIT, 15208, "latch: DList wait", "address", "number", "tries", CONCURRENCY, "latch: DList wait", true)
WAIT_EVENT_DEF(OB_GLOBAL_FREE_LIST_WAIT, 15209, "latch: global freelist wait", "address", "number", "tries", CONCURRENCY, "latch: global freelist wait", true)
WAIT_EVENT_DEF(OB_FREEZE_INFO_MANAGER_WAIT, 15210, "latch: freeze info manager wait", "address", "number", "tries", CONCURRENCY, "latch: freeze info manager wait", true)
WAIT_EVENT_DEF(CHUNK_FREE_LIST_WAIT, 15211, "latch: chunk free list wait", "address", "number", "tries", CONCURRENCY, "latch:  chunk free list wait", true)
WAIT_EVENT_DEF(CHUNK_USING_LIST_WAIT, 15212, "latch: chunk using list wait", "address", "number", "tries", CONCURRENCY, "latch: chunk using list wait", true)
WAIT_EVENT_DEF(WORK_DAG_WAIT, 15213, "latch: work dag wait", "address", "number", "tries", CONCURRENCY, "latch: work dag wait", true)
WAIT_EVENT_DEF(WORK_DAG_NET_WAIT, 15214, "latch: work dag net wait", "address", "number", "tries", CONCURRENCY, "latch: work dag net wait", true)
WAIT_EVENT_DEF(SYS_TASK_STAT_WAIT, 15215, "latch: sys task stat wait", "address", "number", "tries", CONCURRENCY, "latch: sys task stat wait", true)
WAIT_EVENT_DEF(INFO_MGR_WAIT, 15216, "latch: info mgr wait", "address", "number", "tries", CONCURRENCY, "latch: info mgr wait", true)
WAIT_EVENT_DEF(MERGER_DUMP_WAIT, 15217, "latch: merger dump wait", "address", "number", "tries", CONCURRENCY, "latch: merger dump wait", true)
WAIT_EVENT_DEF(TABLET_MERGE_INFO_WAIT, 15218, "latch: tablet merge info wait", "address", "number", "tries", CONCURRENCY, "latch: tablet merge info wait", true)
WAIT_EVENT_DEF(WASH_OUT_WAIT, 15219, "latch: wash out wait", "address", "number", "tries", CONCURRENCY, "latch: wash out wait", true)
WAIT_EVENT_DEF(KV_CACHE_INST_WAIT, 15220, "latch: kv cache inst wait", "address", "number", "tries", CONCURRENCY, "latch: kv cache inst wait", true)
WAIT_EVENT_DEF(KV_CACHE_LIST_WAIT, 15221, "latch: kv cache list wait", "address", "number", "tries", CONCURRENCY, "latch: kv cache list wait", true)
WAIT_EVENT_DEF(THREAD_STORE_WAIT, 15222, "latch: thread store wait", "address", "number", "tries", CONCURRENCY, "latch: thread store wait", true)
WAIT_EVENT_DEF(GLOBAL_KV_CACHE_CONFIG_WAIT, 15223, "latch: global kv cache config wait", "address", "number", "tries", CONCURRENCY, "latch: global kv cache config wait", true)
WAIT_EVENT_DEF(BACKUP_LOCK_WAIT, 15224, "latch: backup lock", "address", "", "", CONCURRENCY, "backup lock", true)
WAIT_EVENT_DEF(RESTORE_LOCK_WAIT, 15225, "latch: restore lock", "address", "", "", CONCURRENCY, "restore lock", true)
WAIT_EVENT_DEF(OBJECT_DEVICE_LOCK_WAIT, 15226, "latch: object device lock", "address", "", "", CONCURRENCY, "object device lock", true)
WAIT_EVENT_DEF(GLOBAL_IO_CONFIG_WAIT, 15227, "latch: global io config wait", "address", "number", "tries", CONCURRENCY, "latch: global io config wait", true)
WAIT_EVENT_DEF(TENANT_IO_MANAGE_WAIT, 15228, "rwlock: tenant io manage wait", "address", "number", "tries", CONCURRENCY, "rwlock: tenant io manage wait", true)
WAIT_EVENT_DEF(IO_FAULT_DETECTOR_WAIT, 15229, "spinlock: io fault detector wait", "address", "number", "tries", CONCURRENCY, "spinlock: io fault detector wait", true)
WAIT_EVENT_DEF(SSTABLE_INSERT_TABLE_CONTEXT_WAIT, 15230, "spinlock: direct insert table context wait", "address", "number", "tries", CONCURRENCY, "spinlock: direct insert table context wait", true)
WAIT_EVENT_DEF(SSTABLE_INSERT_TABLET_CONTEXT_WAIT, 15231, "latch: direct insert tablet context wait", "address", "number", "tries", CONCURRENCY, "latch: direct insert tablet context wait", true)
WAIT_EVENT_DEF(SSTABLE_INSERT_TABLE_MANAGER_WAIT, 15232, "latch: direct insert table manager wait", "address", "number", "tries", CONCURRENCY, "latch: direct insert manager wait", true)
WAIT_EVENT_DEF(COMPLEMENT_DATA_CONTEXT_WAIT, 15233, "spinlock: complement data context wait", "address", "number", "tries", CONCURRENCY, "spinlock: complement data context wait", true)
WAIT_EVENT_DEF(TABLET_DDL_KV_MGR_WAIT, 15234, "latch: tablet ddl kv mgr wait", "address", "number", "tries", CONCURRENCY, "latch: tablet ddl kv mgr wait", true)
WAIT_EVENT_DEF(TABLET_AUTO_INCREMENT_MGR_WAIT, 15235, "spinlock: tablet auto increment mgr wait", "address", "number", "tries", CONCURRENCY, "spinlock: tablet auto increment mgr wait", true)
WAIT_EVENT_DEF(TABLET_AUTO_INCREMENT_SERVICE_WAIT, 15236, "latch: tablet auto increment service wait", "address", "number", "tries", CONCURRENCY, "latch: tablet auto increment service wait", true)
WAIT_EVENT_DEF(ALL_SERVER_TRACER_WAIT, 15237, "spinlock: all server tracer wait", "address", "number", "tries", CONCURRENCY, "spinlock: all server tracer wait", true)
WAIT_EVENT_DEF(UPGRADE_STORAGE_FORMAT_VERSION_WAIT, 15238, "spinlock: upgrade storage format version wait", "address", "number", "tries", CONCURRENCY, "spinlock: upgrade storage format version wait", true)
WAIT_EVENT_DEF(MEMSTORE_ALLOCATOR_LOCK_WAIT, 15239, "spinlock: memstore allocator lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: memstore allocator lock wait", true)
WAIT_EVENT_DEF(THREAD_POOL_LOCK_WAIT, 15240, "spinlock: thread pool lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: thread pool lock wait", true)
WAIT_EVENT_DEF(CLOG_CKPT_LOCK_WAIT, 15241, "spinlock: clog checkpoint lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: clog checkpoint lock wait", true)
WAIT_EVENT_DEF(LS_META_LOCK_WAIT, 15242, "spinlock: ls meta lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: ls meta lock wait", true)
WAIT_EVENT_DEF(LS_CHANGE_LOCK_WAIT, 15243, "latch: ls change lock wait", "address", "number", "tries", CONCURRENCY, "latch: ls change lock wait", true)
WAIT_EVENT_DEF(TENANT_MEM_USAGE_LOCK_WAIT, 15244, "latch: tenant memory usage lock wait", "address", "number", "tries", CONCURRENCY, "latch: tenant memory usage lock wait", true)
WAIT_EVENT_DEF(TX_TABLE_LOCK_WAIT, 15245, "rwlock: tx table lock wait", "address", "number", "tries", CONCURRENCY, "rwlock: tx table lock wait", true)
WAIT_EVENT_DEF(MEMTABLE_STAT_LOCK_WAIT, 15246, "spinlock: memtable stat lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: memtable stat lock wait", true)
WAIT_EVENT_DEF(DEADLOCK_DETECT_LOCK_WAIT, 15247, "spinlock: deadlock detect lock wait", "address", "number", "tries", CONCURRENCY, "spinlock: deadlock detect lock wait", true)
WAIT_EVENT_DEF(BACKUP_DATA_SERVICE_COND_WAIT, 15248, "backup data service condition wait", "address", "", "", CONCURRENCY, "backup data service condition wait", true)
WAIT_EVENT_DEF(BACKUP_CLEAN_SERVICE_COND_WAIT, 15249, "backup clean service condition wait", "address", "", "", CONCURRENCY, "backup clean service condition wait", true)
WAIT_EVENT_DEF(BACKUP_ARCHIVE_SERVICE_COND_WAIT, 15250, "backup archive service condition wait", "address", "", "", CONCURRENCY, "backup archive service condition wait", true)
WAIT_EVENT_DEF(SRS_LOCK_WAIT, 15251, "latch: srs lock wait", "address", "number", "tries", CONCURRENCY, "latch: srs lock wait", true)
WAIT_EVENT_DEF(ARB_SERVER_CONFIG_WAIT, 15252, "arbserver config wait", "address", "number", "tries", CONCURRENCY, "arbserver config wait", true)
WAIT_EVENT_DEF(CLOG_CKPT_RWLOCK_WAIT, 15253, "rwlock: clog checkpoint rwlock wait", "address", "number", "tries", CONCURRENCY, "rwlock: clog checkpoint rwlock wait", true)
WAIT_EVENT_DEF(TENANT_IO_CONFIG_WAIT, 15254, "rwlock: tenant io config wait", "address", "number", "tries", CONCURRENCY, "rwlock: tenant io config wait", true)
WAIT_EVENT_DEF(SQL_WF_PARTICIPATOR_LOCK_WAIT, 15255, "latch: window function participator cond lock wait", "address", "", "", CONCURRENCY, "window function participator cond lock wait", true)
WAIT_EVENT_DEF(SQL_WF_PARTICIPATOR_COND_WAIT, 15256, "mutex: window function participator cond wait", "address", "", "", CONCURRENCY, "window function participator cond wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_DIAGNOSE_LOCK_WAIT, 15257, "latch: major_freeze diagnose lock wait", "address", "number", "tries", CONCURRENCY, "latch: major_freeze diagnose lock wait", true)
WAIT_EVENT_DEF(HB_RESPONSES_LOCK_WAIT, 15258, "latch: hb responses lock wait", "address", "number", "tries", CONCURRENCY, "latch: hb responses lock wait", true)
WAIT_EVENT_DEF(ALL_SERVERS_INFO_IN_TABLE_LOCK_WAIT, 15259, "latch: all servers info in table lock wait", "address", "number", "tries", CONCURRENCY, "latch: all servers info in table lock wait", true)
WAIT_EVENT_DEF(OPT_STAT_GATHER_STAT_LOCK_WAIT, 15260, "latch: optimizer stat gather stat lock wait", "address", "number", "tries", CONCURRENCY, "latch: optimizer stat gather stat lock wait", true)
WAIT_EVENT_DEF(TENANT_IO_POOL_WAIT, 15261, "rwlock: tenant io pool wait", "address", "number", "tries", CONCURRENCY, "rwlock: tenant io pool wait", true)
WAIT_EVENT_DEF(DISPLAY_TASKS_LOCK_WAIT, 15262, "latch: display tasks lock wait", "address", "number", "tries", CONCURRENCY, "latch: display tasks lock wait", true)
WAIT_EVENT_DEF(TMP_FILE_MEM_BLOCK_LOCK_WAIT, 15263, "latch: tmp file mem block lock wait", "address", "number", "tries", CONCURRENCY, "latch: tmp file mem block lock wait", true)

//transaction
WAIT_EVENT_DEF(END_TRANS_WAIT, 16001, "wait end trans", "rollback", "trans_hash_value", "participant_count", COMMIT,"wait end trans", false)
WAIT_EVENT_DEF(START_STMT_WAIT, 16002, "wait start stmt", "trans_hash_value", "physic_plan_type", "participant_count", CLUSTER, "wait start stmt", false)
WAIT_EVENT_DEF(END_STMT_WAIT, 16003, "wait end stmt", "rollback", "trans_hash_value", "physic_plan_type", CLUSTER, "wait end stmt", false)
WAIT_EVENT_DEF(REMOVE_PARTITION_WAIT, 16004, "wait remove partition", "tenant_id", "table_id", "partition_id", ADMINISTRATIVE, "wait remove partition", false)
WAIT_EVENT_DEF(TRANS_BATCH_RPC_LOCK_WAIT, 16005, "trans wait batch rpc lock", "address", "number", "tries", CONCURRENCY, "trans wait batch rpc lock", true)
WAIT_EVENT_DEF(CLOG_RENEW_MS_TASK_LOCK_WAIT, 16006, "latch: clog sw renew ms task lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog sw renew ms task lock wait", true)
WAIT_EVENT_DEF(UNDO_STATUS_LOCK_WAIT, 16007, "undo status lock wait", "", "", "", CONCURRENCY, "UNDO_STATUS_LOCK_WAIT", true)
WAIT_EVENT_DEF(FREEZE_ASYNC_WORKER_LOCK_WAIT, 16008, "freeze async worker lock wait", "", "", "", CONCURRENCY, "FREEZE_ASYNC_WORKER_LOCK_WAIT", true)
WAIT_EVENT_DEF(TRX_EXEC_CTX_LOCK_WAIT, 16009, "transaction execution ctx lock wait", "", "", "", CONCURRENCY, "TRX_EXEC_CTX_LOCK_WAIT", true)
WAIT_EVENT_DEF(TENANT_DISK_USAGE_LOCK_WAIT, 16010, "tenant disk usage lock wait", "", "", "", CONCURRENCY, "TENANT_DISK_USAGE_LOCK_WAIT", true)
WAIT_EVENT_DEF(LOG_OFFSET_ALLOC_LOCK_WAIT, 16011, "log_offset allocator lock wait", "", "", "", CONCURRENCY, "LOG_OFFSET_ALLOC_LOCK_WAIT", true)
WAIT_EVENT_DEF(GTI_SOURCE_LOCK_WAIT, 16012, "update trans id assignable interval lock wait", "", "", "", CONCURRENCY, "GTI_SOURCE_LOCK_WAIT", true)
WAIT_EVENT_DEF(APPLY_STATUS_LOCK_WAIT, 16013, "apply_status lock wait", "", "", "", CONCURRENCY, "APPLY_STATUS_LOCK_WAIT", true)
WAIT_EVENT_DEF(ID_SOURCE_LOCK_WAIT, 16014, "ID allocator updates assignable interval lock wait", "", "", "", CONCURRENCY, "ID_SOURCE_LOCK_WAIT", true)
WAIT_EVENT_DEF(TRANS_AUDIT_RECORD_LOCK_WAIT, 16015, "trans records audit information lock wait", "", "", "", CONCURRENCY, "TRANS_AUDIT_RECORD_LOCK_WAIT", true)
WAIT_EVENT_DEF(TABLET_LOCK_WAIT, 16016, "tablet lock wait", "", "", "", CONCURRENCY, "TABLET_LOCK_WAIT", true)
WAIT_EVENT_DEF(TABLET_MULTI_SOURCE_DATA_WAIT, 16017, "tablet multi source data wait", "", "", "", CONCURRENCY, "TABLET_MULTI_SOURCE_DATA_WAIT", true)
WAIT_EVENT_DEF(DTL_CHANNEL_MGR_WAIT, 16018, "latch:dtl channel mgr hashmap lock wait", "address", "number", "tries", CONCURRENCY, "latch:dtl channel mgr hashmap lock wait", true)
WAIT_EVENT_DEF(PL_DEBUG_WAIT, 16019, "latch:pl debug lock wait", "address", "number", "tries", CONCURRENCY, "latch:pl debug lock wait", true)
WAIT_EVENT_DEF(PL_SYS_BREAKPOINT_WAIT, 16020, "latch:pl sys breakpoint lock wait", "address", "number", "tries", CONCURRENCY, "latch:pl sys breakpoint lock wait", true)
WAIT_EVENT_DEF(DBMS_JOB_TASK_WAIT, 16021, "latch:dbms_job task lock wait", "address", "number", "tries", CONCURRENCY, "latch:dbms_job task lock wait", true)
WAIT_EVENT_DEF(DBMS_JOB_MASTER_WAIT, 16022, "latch:dbms_job master lock wait", "address", "number", "tries", CONCURRENCY, "latch:dbms_job master lock wait", true)
WAIT_EVENT_DEF(DBMS_SCHEDULER_TASK_WAIT, 16023, "latch:dbms_scheduler task lock wait", "address", "number", "tries", CONCURRENCY, "latch:dbms_scheduler task lock wait", true)
WAIT_EVENT_DEF(DBMS_SCHEDULER_MASTER_WAIT, 16024, "latch:dbms_scheduler master lock wait", "address", "number", "tries", CONCURRENCY, "latch:dbms_scheduler master lock wait", true)
WAIT_EVENT_DEF(TENANT_WORKER_WAIT, 16025, "tenant worker lock wait", "address", "number", "tries", CONCURRENCY, "tenant worker lock wait", true)

WAIT_EVENT_DEF(SERVER_LOCALITY_CACHE_LOCK_WAIT, 16026, "latch:server locality cache lock wait", "address", "number", "tries", CONCURRENCY, "server locality cache lock wait", true)
WAIT_EVENT_DEF(MASTER_RS_CACHE_LOCK_WAIT, 16027, "latch:master rs cache lock wait", "address", "number", "tries", CONCURRENCY, "master rs cache lock wait", true)
WAIT_EVENT_DEF(SCHEMA_REFRESH_INFO_LOCK_WAIT, 16028, "latch:schema refresh info lock wait", "address", "number", "tries", CONCURRENCY, "schema refresh info lock wait", true)
WAIT_EVENT_DEF(REFRESH_SCHEMA_LOCK_WAIT, 16029, "latch:refresh schema lock wait", "address", "number", "tries", CONCURRENCY, "refresh schema lock wait", true)
WAIT_EVENT_DEF(REFRESHED_SCHEMA_CACHE_LOCK_WAIT, 16030, "latch:refreshed schema cache lock wait", "address", "number", "tries", CONCURRENCY, "refreshed schema cache lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_CACHE_LOCK_WAIT, 16031, "latch:schema mgr cache lock wait", "address", "number", "tries", CONCURRENCY, "schema mgr cache lock wait", true)
WAIT_EVENT_DEF(RS_MASTER_KEY_RESPONSE_LOCK_WAIT, 16032, "latch:rs master key response lock wait", "address", "number", "tries", CONCURRENCY, "rs master key response lock wait", true)
WAIT_EVENT_DEF(RS_MASTER_KEY_REQUEST_LOCK_WAIT, 16033, "latch:rs master key request lock wait", "address", "number", "tries", CONCURRENCY, "rs master key request lock wait", true)
WAIT_EVENT_DEF(RS_MASTER_KEY_MGR_LOCK_WAIT, 16034, "latch:rs master key mgr lock wait", "address", "number", "tries", CONCURRENCY, "rs master key mgr lock wait", true)
WAIT_EVENT_DEF(THREAD_HANG_CHECKER_LOCK_WAIT, 16035, "latch:thread hang checker lock wait", "address", "number", "tries", CONCURRENCY, "thread hang checker lock wait", true)
WAIT_EVENT_DEF(CREATE_INNER_SCHEMA_EXECUTOR_LOCK_WAIT, 16036, "latch:create inner schema executor lock wait", "address", "number", "tries", CONCURRENCY, "create inner schema executor lock wait", true)

WAIT_EVENT_DEF(WRS_SERVER_VERSION_WAIT, 16037, "weak read server version wait ", "address", "number", "tries", CONCURRENCY, "WRS_SERVER_VERSION_WAIT", true)
WAIT_EVENT_DEF(TX_LS_LOG_WRITER_WAIT, 16038, "transaction ls log writer wait", "address", "number", "tries", CONCURRENCY, "TX_LS_LOG_WRITER_WAIT", true)
WAIT_EVENT_DEF(TX_DESC_WAIT, 16039, "transaction descriptor wait", "address", "number", "tries", CONCURRENCY, "TX_DESC_WAIT", true)
WAIT_EVENT_DEF(TX_DESC_COMMIT_WAIT, 16040, "transaction descriptor commit wait", "address", "number", "tries", CONCURRENCY, "TX_DESC_COMMIT_WAIT", true)
WAIT_EVENT_DEF(WRS_CLUSTER_SERVICE_WAIT, 16041, "weak read service cluster service wait", "address", "number", "tries", CONCURRENCY, "WRS_CLUSTER_SERVICE_WAIT", true)
WAIT_EVENT_DEF(TX_STAT_ITEM_WAIT, 16042, "transaction stat item wait", "address", "number", "tries", CONCURRENCY, "TX_STAT_ITEM_WAIT", true)
WAIT_EVENT_DEF(WRS_CLUSTER_VERSION_MGR_WAIT, 16043, "weak read service cluster version manager wait", "address", "number", "tries", CONCURRENCY, "WRS_CLUSTER_VERSION_MGR_WAIT", true)
WAIT_EVENT_DEF(TABLE_API_LOCK_WAIT, 16044, "latch: table api interface lock", "address", "number", "tries", CONCURRENCY, "TABLE_API_LOCK_WAIT", true)
WAIT_EVENT_DEF(SERVER_LOCALITY_MGR_LOCK_WAIT, 16045, "latch: server locality manager lock", "address", "number", "tries", CONCURRENCY, "SERVER_LOCALITY_MGR_LOCK_WAIT", true)
WAIT_EVENT_DEF(DDL_LOCK_WAIT, 16046, "latch: ddl task lock", "address", "number", "tries", CONCURRENCY, "DDL_LOCK_WAIT", true)
WAIT_EVENT_DEF(DEADLOCK_LOCK_WAIT, 16047, "latch: deadlock lock", "address", "number", "tries", CONCURRENCY, "DEADLOCK_LOCK_WAIT", true)
WAIT_EVENT_DEF(BG_THREAD_MONITOR_LOCK_WAIT, 16048, "latch: background thread monitor lock", "address", "number", "tries", CONCURRENCY, "BG_THREAD_MONITOR_LOCK_WAIT", true)
WAIT_EVENT_DEF(RPC_STAT_LOCK_WAIT, 16049, "latch: rpc stat lock", "address", "number", "tries", CONCURRENCY, "RPC_STAT_LOCK_WAIT", true)
WAIT_EVENT_DEF(DBLINK_LOCK_WAIT, 16050, "latch: dblink lock", "address", "number", "tries", CONCURRENCY, "DBLINK_LOCK_WAIT", true)
WAIT_EVENT_DEF(REPLAY_STATUS_WAIT, 16051, "replay status lock wait", "", "", "", CONCURRENCY, "REPLAY_STATUS_WAIT", true)
WAIT_EVENT_DEF(REPLAY_STATUS_TASK_WAIT, 16052, "replay status task lock wait", "", "", "", CONCURRENCY, "REPLAY_STATUS_TASK_WAIT", true)
WAIT_EVENT_DEF(MAX_APPLY_SCN_WAIT, 16053, "max apply scn lock wait", "", "", "", CONCURRENCY, "MAX_APPLY_SCN_WAIT", true)
WAIT_EVENT_DEF(GC_HANDLER_WAIT, 16054, "gc handler lock wait", "", "", "", CONCURRENCY, "GC_HANDLER_WAIT", true)
WAIT_EVENT_DEF(FREEZE_THREAD_POOL_WAIT, 16055, "freeze thread pool wait", "", "", "", CONCURRENCY, "FREEZE_THREAD_POOL_WAIT", true)
WAIT_EVENT_DEF(DDL_EXECUTE_LOCK_WAIT, 16056, "ddl execute lock wait", "", "", "", CONCURRENCY, "DDL_EXECUTE_LOCK_WAIT", true)
WAIT_EVENT_DEF(DUP_TABLET_LOCK_WAIT, 16057, "dup tablet lock wait", "", "", "", CONCURRENCY, "DUP_TABLET_LOCK_WAIT", true)

// WAIT_EVENT_DEF(TENANT_MGR_TENANT_BUCKET_LOCK_WAIT, 16056, "tenant mgr tenant bucket lock wait", "", "", "", CONCURRENCY, "TENANT_MGR_TENANT_BUCKET_LOCK_WAIT", true)

//replication group
WAIT_EVENT_DEF(RG_TRANSFER_LOCK_WAIT, 17000, "transfer lock wait", "src_rg", "dst_rg", "transfer_pkey", CONCURRENCY, "transfer lock wait", false)

// libobcdc
WAIT_EVENT_DEF(OBCDC_PART_MGR_SCHEMA_VERSION_WAIT, 18000, "oblog part mgr schema version wait", "", "", "", CONCURRENCY, "oblog part mgr schema version wait", true)
WAIT_EVENT_DEF(OBCDC_PROGRESS_RECYCLE_LOCK_WAIT, 18001, "latch: obcdc progress recycle lock wait", "", "", "", CONCURRENCY, "latch: obcdc progress recycle lock wait", true)
WAIT_EVENT_DEF(OBCDC_METAINFO_LOCK_WAIT, 18002, "latch: obcdc metainfo lock wait", "", "", "", CONCURRENCY, "latch: obcdc metainfo lock wait", true)
WAIT_EVENT_DEF(OBCDC_TRANS_CTX_LOCK_WAIT, 18003, "latch: obcdc trans ctx lock wait", "", "", "", CONCURRENCY, "latch: obcdc trans ctx lock wait", true)
WAIT_EVENT_DEF(OBCDC_SVR_BLACKLIST_LOCK_WAIT, 18004, "latch: obcdc svr blacklist wait", "", "", "", CONCURRENCY, "latch: obcdc svr blacklist lock wait", true)
WAIT_EVENT_DEF(OBCDC_SQLSERVER_LOCK_WAIT, 18005, "latch: obcdc sqlserver lock wait", "", "", "", CONCURRENCY, "latch: obcdc sqlserver lock wait", true)
WAIT_EVENT_DEF(OBCDC_TIMEZONE_GETTER_LOCK_WAIT, 18006, "latch: obcdc timezone getter lock wait", "", "", "", CONCURRENCY, "latch: obcdc timezone getter lock wait", true)
WAIT_EVENT_DEF(OBCDC_FETCHLOG_ARPC_LOCK_WAIT, 18007, "latch: obcdc fetchlog arpc lock wait", "", "", "", CONCURRENCY, "latch: obcdc fetchlog arpc lock wait", true)
WAIT_EVENT_DEF(OBCDC_FETCHSTREAM_CONTAINER_LOCK_WAIT, 18008, "latch: obcdc fetchstream container lock wait", "", "", "", CONCURRENCY, "latch: obcdc fetchstream container lock wait", true)
WAIT_EVENT_DEF(EXT_SVR_BLACKLIST_LOCK_WAIT, 18009, "latch: external server blacklist lock wait", "", "", "", CONCURRENCY, "latch: external server blacklist lock wait", true)
WAIT_EVENT_DEF(CDC_SERVICE_LS_CTX_LOCK_WAIT, 18010, "latch: cdcservice clientlsctx lock wait", "", "", "", CONCURRENCY, "latch: cdcservice clientlsctx lock wait", true)

// palf
WAIT_EVENT_DEF(PALF_SW_SUBMIT_INFO_WAIT, 19000, "palf sw last submit log info lock wait", "", "", "", CONCURRENCY, "PALF_SW_SUBMIT_INFO_WAIT", true)
WAIT_EVENT_DEF(PALF_SW_COMMITTED_INFO_WAIT, 19001, "palf sw committed info lock wait", "", "", "", CONCURRENCY, "PALF_SW_COMMITTED_INFO_WAIT", true)
WAIT_EVENT_DEF(PALF_SW_SLIDE_INFO_WAIT, 19002, "palf sw last slide log info lock wait", "", "", "", CONCURRENCY, "PALF_SW_SLIDE_INFO_WAIT", true)
WAIT_EVENT_DEF(PALF_SW_FETCH_INFO_WAIT, 19003, "palf sw fetch log info lock wait", "", "", "", CONCURRENCY, "PALF_SW_FETCH_INFO_WAIT", true)
WAIT_EVENT_DEF(PALF_SW_MATCH_LSN_MAP_WAIT, 19004, "palf sw match lsn map lock wait", "", "", "", CONCURRENCY, "PALF_SW_MATCH_LSN_MAP_WAIT", true)
WAIT_EVENT_DEF(PALF_SW_LOC_CB_WAIT, 19005, "palf sw location cache cb lock wait", "", "", "", CONCURRENCY, "PALF_SW_LOC_CB_WAIT", true)
WAIT_EVENT_DEF(PALF_CM_CONFIG_WAIT, 19006, "palf cm config data lock wait", "", "", "", CONCURRENCY, "PALF_CM_CONFIG_WAIT", true)
WAIT_EVENT_DEF(PALF_CM_PARENT_WAIT, 19007, "palf cm parent info lock wait", "", "", "", CONCURRENCY, "PALF_CM_PARENT_WAIT", true)
WAIT_EVENT_DEF(PALF_CM_CHILD_WAIT, 19008, "palf cm child info lock wait", "", "", "", CONCURRENCY, "PALF_CM_CHILD_WAIT", true)
WAIT_EVENT_DEF(PALF_HANDLE_IMPL_WAIT, 19009, "palf handle impl lock wait", "", "", "", CONCURRENCY, "PALF_HANDLE_IMPL_WAIT", true)
WAIT_EVENT_DEF(PALF_LOG_ENGINE_LOCK_WAIT, 19010, "latch: palf log engine lock wait", "", "", "", CONCURRENCY, "latch: palf log engine lock wait", true)
WAIT_EVENT_DEF(PALF_ENV_LOCK_WAIT, 19011, "latch: palf env lock wait", "", "", "", CONCURRENCY, "latch: palf log env lock wait", true)
WAIT_EVENT_DEF(RCS_LOCK_WAIT, 19012, "latch: role change serivce lock wait", "", "", "", CONCURRENCY, "latch: role change service lock wait", true)

// archive
WAIT_EVENT_DEF(LS_ARCHIVE_TASK_LOCK_WAIT, 19013, "latch: ls archive task lock wait", "", "", "", CONCURRENCY, "latch: ls archive task lock wait", true)
WAIT_EVENT_DEF(ARCHIVE_ROUND_MGR_LOCK_WAIT, 19014, "latch: archive round mgr lock wait", "", "", "", CONCURRENCY, "latch: archive round mgr lock wait", true)
WAIT_EVENT_DEF(ARCHIVE_PERSIST_MGR_LOCK_WAIT, 19015, "latch: archive persist mgr lock wait", "", "", "", CONCURRENCY, "latch: archive persist mgr lock wait", true)
WAIT_EVENT_DEF(ARCHIVE_TASK_QUEUE_LOCK_WAIT, 19016, "latch: archive task queue lock wait", "", "", "", CONCURRENCY, "latch: archive task queue lock wait", true)

// sleep
WAIT_EVENT_DEF(BANDWIDTH_THROTTLE_SLEEP, 20000, "sleep: bandwidth throttle sleep wait", "sleep_interval", "", "", CONCURRENCY, "sleep: bandwidth throttle sleep wait", true)
WAIT_EVENT_DEF(DTL_PROCESS_CHANNEL_SLEEP, 20001, "sleep: dtl process channel sleep wait", "sleep_interval", "", "", CONCURRENCY, "sleep: dtl process channel sleep wait", true)
WAIT_EVENT_DEF(DTL_DESTROY_CHANNEL_SLEEP, 20002, "sleep: dtl destroy channel sleep wait", "sleep_interval", "", "", CONCURRENCY, "sleep: dtl destroy channel sleep wait", true)
WAIT_EVENT_DEF(STORAGE_WRITING_THROTTLE_SLEEP, 20003, "sleep: storage writing throttle sleep", "sleep_interval", "", "", CONCURRENCY, "sleep: storage writing throttle sleep", true)
WAIT_EVENT_DEF(STORAGE_AUTOINC_FETCH_RETRY_SLEEP, 20004, "sleep: tablet autoinc fetch new range retry wait", "sleep_interval", "", "", CONCURRENCY, "sleep: tablet autoinc fetch new range retry wait", true)
WAIT_EVENT_DEF(STORAGE_AUTOINC_FETCH_CONFLICT_SLEEP, 20005, "sleep: tablet autoinc fetch new range conflict wait", "sleep_interval", "", "", CONCURRENCY, "sleep: tablet autoinc fetch new range conflict wait", true)
WAIT_EVENT_DEF(STORAGE_HA_FINISH_TRANSFER, 20006, "sleep: finish transfer sleep wait", "sleep_interval", "", "", CONCURRENCY, "sleep: finish transfer sleep wait", true)

// logservice
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_IO_TASK_WAIT, 20007, "latch: log external storage io task wait", "", "", "", CONCURRENCY, "latch: log external storage io task wait", true)
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_HANDLER_RW_WAIT, 20008, "latch: log external storage handler rw wait", "", "", "", CONCURRENCY, "latch: log external storage handler rw wait", true)
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_HANDLER_WAIT, 20009, "latch: log external storage handler spin wait", "", "", "", CONCURRENCY, "latch: log external storage handler spin wait", true)

WAIT_EVENT_DEF(WAIT_EVENT_END, 99999, "event end", "", "", "", OTHER, "event end", false)
#endif

#ifndef OB_WAIT_EVENT_DEFINE_H_
#define OB_WAIT_EVENT_DEFINE_H_

#include "lib/ob_errno.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/wait_event/ob_wait_class.h"


namespace oceanbase
{
namespace common
{
static const int64_t MAX_WAIT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_WAIT_EVENT_PARAM_LENGTH = 64;
static const int64_t SESSION_WAIT_HISTORY_NEST = 10;

struct ObWaitEventIds
{
  enum ObWaitEventIdEnum
  {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, display_name, is_phy) def,
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
  };
};

struct ObWaitEventDesc
{
  int64_t event_no_;
  uint64_t p1_;
  uint64_t p2_;
  uint64_t p3_;
  int64_t wait_begin_time_;
  int64_t wait_end_time_;
  int64_t wait_time_;
  uint64_t timeout_ms_;
  int64_t level_;
  int64_t parent_;
  bool is_phy_; // as allow nested wait event, the outer event is always logical event, the inner events, such as latch, usleep, are physical.
  ObWaitEventDesc() : event_no_(0),
                      p1_(0),
                      p2_(0),
                      p3_(0),
                      wait_begin_time_(0),
                      wait_end_time_(0),
                      wait_time_(0),
                      timeout_ms_(0),
                      level_(0),
                      parent_(0),
                      is_phy_(false) {}
  inline bool operator<(const ObWaitEventDesc &other) const;
  inline bool operator>(const ObWaitEventDesc &other) const;
  inline bool operator==(const ObWaitEventDesc &other) const;
  inline bool operator!=(const ObWaitEventDesc &other) const;
  inline int add(const ObWaitEventDesc &other);
  void reset()
  {
    event_no_ = 0;
    p1_ = 0;
    p2_ = 0;
    p3_ = 0;
    wait_begin_time_ = 0;
    wait_end_time_ = 0;
    wait_time_ = 0;
    timeout_ms_ = 0;
    level_ = 0;
    parent_ = 0;
    is_phy_ = false;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

struct ObWaitEventStat
{
  //total number of timeouts for a event
  uint32_t total_timeouts_;
  //max amount of time waited for a event
  uint32_t max_wait_;
  //total number of waits for a event
  uint64_t total_waits_;
  //total amount of time waited for a event
  uint64_t time_waited_;


  ObWaitEventStat() : total_timeouts_(0),
                      max_wait_(0),
                      total_waits_(0),
                      time_waited_(0) { }
  int add(const ObWaitEventStat &other);
  void reset()
  {
    total_timeouts_ = 0;
    max_wait_ = 0;
    total_waits_ = 0;
    time_waited_ = 0;
  }
  inline bool is_valid() const { return total_waits_ > 0; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

struct ObWaitEvent
{
  int64_t event_id_;
  char event_name_[MAX_WAIT_EVENT_NAME_LENGTH];
  char param1_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param2_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param3_[MAX_WAIT_EVENT_PARAM_LENGTH];
  int64_t wait_class_;
  char display_name_[MAX_WAIT_EVENT_NAME_LENGTH];
  bool is_phy_;
};


extern const ObWaitEvent OB_WAIT_EVENTS[];

#define EVENT_NO_TO_CLASS_ID(event_no) OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_no].wait_class_].wait_class_id_
#define EVENT_NO_TO_CLASS(event_no) OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_no].wait_class_].wait_class_

/**
 * -----------------------------------------------------Inline Methods------------------------------------------------------
 */
inline bool ObWaitEventDesc::operator <(const ObWaitEventDesc &other) const
{
  return wait_begin_time_ < other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator >(const ObWaitEventDesc &other) const
{
  return wait_begin_time_ > other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator ==(const ObWaitEventDesc &other) const
{
  return event_no_ == other.event_no_
           && p1_ == other.p1_
           && p2_ == other.p2_
           && p3_ == other.p3_
           && wait_begin_time_ == other.wait_begin_time_
           && wait_end_time_ == other.wait_end_time_
           && wait_time_ == other.wait_time_
           && timeout_ms_ == other.timeout_ms_
           && level_ == other.level_;
}

inline bool ObWaitEventDesc::operator !=(const ObWaitEventDesc &other) const
{
  return !(*this==(other));
}

inline int ObWaitEventDesc::add(const ObWaitEventDesc &other)
{
  int ret = common::OB_SUCCESS;
  if (other.wait_begin_time_ > wait_begin_time_) {
    *this = other;
  }
  return ret;
}

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
