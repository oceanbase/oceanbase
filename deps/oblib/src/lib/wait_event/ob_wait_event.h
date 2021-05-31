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
WAIT_EVENT_DEF(NULL_EVENT, 10000, "system internal wait", "", "", "", OTHER, "system internal wait", true)
WAIT_EVENT_DEF(
    DB_FILE_DATA_READ, 10001, "db file data read", "fd", "offset", "size", USER_IO, "db file data read", true)
WAIT_EVENT_DEF(DB_FILE_DATA_INDEX_READ, 10002, "db file data index read", "fd", "offset", "size", USER_IO,
    "db file data index read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_READ, 11001, "db file compact read", "fd", "offset", "size", SYSTEM_IO,
    "db file compact read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_WRITE, 11002, "db file compact write", "fd", "offset", "size", SYSTEM_IO,
    "db file compact write", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_READ, 11003, "db file index build read", "fd", "offset", "size", SYSTEM_IO,
    "db file index build read", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_WRITE, 11004, "db file index build write", "fd", "offset", "size", SYSTEM_IO,
    "db file index build write", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_READ, 11005, "db file migrate read", "fd", "offset", "size", SYSTEM_IO,
    "db file migrate read", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_WRITE, 11006, "db file migrate write", "fd", "offset", "size", SYSTEM_IO,
    "db file migrate write", true)
WAIT_EVENT_DEF(BLOOM_FILTER_BUILD_READ, 11007, "bloomfilter build read", "fd", "offset", "size", SYSTEM_IO,
    "bloomfilter build read", true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_WRITE, 11008, "interm result disk write", "fd", "offset", "size", USER_IO,
    "interm result disk write", true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_READ, 11009, "interm result disk read", "fd", "offset", "size", USER_IO,
    "interm result disk read", true)
WAIT_EVENT_DEF(
    ROW_STORE_DISK_WRITE, 11010, "row store disk write", "fd", "offset", "size", USER_IO, "row store disk write", true)
WAIT_EVENT_DEF(
    ROW_STORE_DISK_READ, 11011, "row store disk read", "fd", "offset", "size", USER_IO, "row store disk read", true)
WAIT_EVENT_DEF(CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK_WAIT, 11012, "ObCacheLineSegregatedArrayBase alloc mutex", "fd",
    "offset", "size", USER_IO, "ObCacheLineSegregatedArrayBase alloc mutex", true)
WAIT_EVENT_DEF(SERVER_OBJECT_POOL_ARENA_LOCK_WAIT, 11013, "server object pool arena lock wait", "fd", "offset", "size",
    USER_IO, "row store disk read", true)
WAIT_EVENT_DEF(MEMSTORE_MEM_PAGE_ALLOC_INFO, 11014, "memstore memory page alloc info", "cur_mem_hold", "sleep_interval",
    "cur_ts", SYSTEM_IO, "memstore memory page alloc info", true)
WAIT_EVENT_DEF(MEMSTORE_MEM_PAGE_ALLOC_WAIT, 11015, "memstore memory page alloc wait", "cur_mem_hold", "sleep_interval",
    "cur_ts", SYSTEM_IO, "memstore memory page alloc wait", true)
// scheduler
WAIT_EVENT_DEF(OMT_WAIT, 12001, "sched wait", "req type", "req start timestamp", "wait start timestamp", SCHEDULER,
    "sched wait", true)
WAIT_EVENT_DEF(OMT_IDLE, 12002, "sched idle", "wait start timestamp", "", "", IDLE, "sched idle", true)

// network
WAIT_EVENT_DEF(SYNC_RPC, 13000, "sync rpc", "pcode", "size", "", NETWORK, "sync rpc", true)
WAIT_EVENT_DEF(MYSQL_RESPONSE_WAIT_CLIENT, 13001, "mysql response wait client", "", "", "", NETWORK,
    "mysql response wait client", true)

// application
WAIT_EVENT_DEF(MT_READ_LOCK_WAIT, 14001, "memstore read lock wait", "lock", "waiter", "owner", APPLICATION,
    "memstore read lock wait", false)
WAIT_EVENT_DEF(MT_WRITE_LOCK_WAIT, 14002, "memstore write lock wait", "lock", "waiter", "owner", APPLICATION,
    "memstore write lock wait", false)
WAIT_EVENT_DEF(ROW_LOCK_WAIT, 14003, "row lock wait", "lock", "waiter", "owner", APPLICATION, "row lock wait", false)

// concurrency
WAIT_EVENT_DEF(PT_LOCATION_CACHE_LOCK_WAIT, 15001, "partition location cache lock wait", "table_id", "partition_id", "",
    CONCURRENCY, "partition location cache lock wait", true)
WAIT_EVENT_DEF(KV_CACHE_BUCKET_LOCK_WAIT, 15002, "latch: kvcache bucket wait", "address", "number", "tries",
    CONCURRENCY, "latch: kvcache bucket wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_LOCK_WAIT, 15003, "latch: default spin lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: default spin lock wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_RWLOCK_WAIT, 15004, "latch: default spin rwlock wait", "address", "number", "tries",
    CONCURRENCY, "latch: default spin rwlock wait", true)
WAIT_EVENT_DEF(DEFAULT_MUTEX_WAIT, 15005, "latch: default mutex wait", "address", "number", "tries", CONCURRENCY,
    "latch: default mutex wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_TASK_LOCK_WAIT, 15006, "latch: time wheel task lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: time wheel task lock wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_BUCKET_LOCK_WAIT, 15007, "latch: time wheel bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: time wheel bucket lock wait", true)
WAIT_EVENT_DEF(ELECTION_LOCK_WAIT, 15008, "latch: election lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: election lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_LOCK_WAIT, 15009, "latch: trans ctx lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: trans ctx lock wait", true)
WAIT_EVENT_DEF(PARTITION_LOG_LOCK_WAIT, 15010, "latch: partition log lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: partition log lock wait", true)
WAIT_EVENT_DEF(PCV_SET_LOCK_WAIT, 15011, "latch: plan cache pcv set lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: plan cache pcv set lock wait", true)

WAIT_EVENT_DEF(CLOG_HISTORY_REPORTER_LOCK_WAIT, 15012, "latch: clog history reporter lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog history reporter lock wait", true)
WAIT_EVENT_DEF(CLOG_EXTERNAL_EXEC_LOCK_WAIT, 15013, "latch: clog external executor lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog external executor lock wait", true)
WAIT_EVENT_DEF(CLOG_MEMBERSHIP_MGR_LOCK_WAIT, 15014, "latch: clog membership mgr lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog membership mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_RECONFIRM_LOCK_WAIT, 15015, "latch: clog reconfirm lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: clog reconfirm lock wait", true)
WAIT_EVENT_DEF(CLOG_SLIDING_WINDOW_LOCK_WAIT, 15016, "latch: clog sliding window lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog sliding window lock wait", true)
WAIT_EVENT_DEF(CLOG_STAT_MGR_LOCK_WAIT, 15017, "latch: clog stat mgr lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: clog stat mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_TASK_LOCK_WAIT, 15018, "latch: clog task lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: clog task lock wait", true)
WAIT_EVENT_DEF(CLOG_IDMGR_LOCK_WAIT, 15019, "latch: clog id mgr lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: clog id mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_CACHE_LOCK_WAIT, 15020, "latch: clog cache lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: clog cache lock wait", true)
WAIT_EVENT_DEF(ELECTION_MSG_LOCK_WAIT, 15021, "latch: election msg lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: election msg lock wait", true)
WAIT_EVENT_DEF(PLAN_SET_LOCK_WAIT, 15022, "latch: plan set lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: plan set lock wait", true)
WAIT_EVENT_DEF(PS_STORE_LOCK_WAIT, 15023, "latch: ps store lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: ps store lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_MGR_LOCK_WAIT, 15024, "latch: trans context mgr lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: trans context mgr lock wait", true)
WAIT_EVENT_DEF(DEFAULT_RECURSIVE_MUTEX_WAIT, 15025, "latch: default recursive mutex wait", "address", "number", "tries",
    CONCURRENCY, "latch: default recursive mutex wait", true)
WAIT_EVENT_DEF(DEFAULT_DRW_LOCK_WAIT, 15026, "latch: default drw lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: default drw lock wait", true)
WAIT_EVENT_DEF(DEFAULT_BUCKET_LOCK_WAIT, 15027, "latch: default bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: default bucket lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_BUCKET_LOCK_WAIT, 15028, "latch: trans ctx bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: trans ctx bucket lock wait", true)
WAIT_EVENT_DEF(MACRO_META_BUCKET_LOCK_WAIT, 15029, "latch: macro meta bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: macro meta bucket lock wait", true)
WAIT_EVENT_DEF(TOKEN_BUCKET_LOCK_WAIT, 15030, "latch: token bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: token bucket lock wait", true)
WAIT_EVENT_DEF(LIGHTY_HASHMAP_BUCKET_LOCK_WAIT, 15031, "latch: lighty hashmap bucket lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: lighty hashmap bucket lock wait", true)
WAIT_EVENT_DEF(ROW_CALLBACK_LOCK_WAIT, 15032, "latch: row callback lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: row callback lock wait", true)
WAIT_EVENT_DEF(PARTITION_LOCK_WAIT, 15033, "latch: partition lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: partition lock wait", true)
WAIT_EVENT_DEF(SWITCH_STAGGER_MERGE_FLOW_WAIT, 15034, "latch: switch stagger merge flow wait", "address", "number",
    "tries", CONCURRENCY, "latch: switch stagger merge flow wait", true)
WAIT_EVENT_DEF(SWITCH_LEADER_WAIT, 15035, "latch: switch leader wait", "address", "number", "tries", CONCURRENCY,
    "latch: switch leader wait", true)
WAIT_EVENT_DEF(PARTITION_FREEZE_WAIT, 15036, "latch: partition freeze wait", "address", "number", "tries", CONCURRENCY,
    "latch: partition freeze wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_LOCK_WAIT, 15037, "latch: schema service wait", "address", "number", "tries", CONCURRENCY,
    "latch: schema service wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_STATS_LOCK_WAIT, 15038, "latch: schema service stats wait", "address", "number", "tries",
    CONCURRENCY, "latch: schema service stats wait", true)
WAIT_EVENT_DEF(TENANT_LOCK_WAIT, 15039, "latch: tenant lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: tenant lock wait", true)
WAIT_EVENT_DEF(CONFIG_LOCK_WAIT, 15040, "latch: config lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: config lock wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_LOCK_WAIT, 15041, "latch: major freeze lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: major freeze lock wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_LOCK_WAIT, 15042, "latch: partition table updater lock wait", "address",
    "number", "tries", CONCURRENCY, "latch: partition table updater wait", true)
WAIT_EVENT_DEF(MULTI_TENANT_LOCK_WAIT, 15043, "latch: multi tenant lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: multi tenant lock wait", true)
WAIT_EVENT_DEF(LEADER_COORDINATOR_LOCK_WAIT, 15044, "latch: leader coordinator lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: leader coordinator lock wait", true)
WAIT_EVENT_DEF(LEADER_STAT_LOCK_WAIT, 15045, "latch: leader stat lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: leader stat lock wait", true)
WAIT_EVENT_DEF(ROOT_MAJOR_FREEZE_LOCK_WAIT, 15046, "latch: root major freeze lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: root major freeze lock wait", true)
WAIT_EVENT_DEF(RS_BOOTSTRAP_LOCK_WAIT, 15047, "latch: rs bootstrap lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: rs bootstap lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_ITEM_LOCK_WAIT, 15048, "latch: schema mgr item lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: schema mgr item lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_LOCK_WAIT, 15049, "latch: schema mgr lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: schema mgr lock wait", true)
WAIT_EVENT_DEF(SUPER_BLOCK_LOCK_WAIT, 15050, "latch: super block lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: super block lock wait", true)
WAIT_EVENT_DEF(FROZEN_VERSION_LOCK_WAIT, 15051, "latch: frozen version lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: frozen version lock wait", true)
WAIT_EVENT_DEF(RS_BROADCAST_LOCK_WAIT, 15052, "latch: rs broadcast lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: rs broadcast lock wait", true)
WAIT_EVENT_DEF(SERVER_STATUS_LOCK_WAIT, 15053, "latch: server status lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: server status lock wait", true)
WAIT_EVENT_DEF(SERVER_MAINTAINCE_LOCK_WAIT, 15054, "latch: server maintaince lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: server maintaince lock wait", true)
WAIT_EVENT_DEF(UNIT_MANAGER_LOCK_WAIT, 15055, "latch: unit manager lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: unit manager lock wait", true)
WAIT_EVENT_DEF(ZONE_MANAGER_LOCK_WAIT, 15056, "latch: zone manager maintaince lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: zone manager maintaince lock wait", true)
WAIT_EVENT_DEF(ALLOC_OBJECT_LOCK_WAIT, 15057, "latch: alloc object lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: alloc object lock wait", true)
WAIT_EVENT_DEF(ALLOC_BLOCK_LOCK_WAIT, 15058, "latch: alloc block lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: alloc block lock wait", true)
WAIT_EVENT_DEF(TRACE_RECORDER_LOCK_WAIT, 15059, "latch: trace recorder lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: normal trace recorder lock wait", true)
WAIT_EVENT_DEF(SESSION_TRACE_RECORDER_LOCK_WAIT, 15060, "latch: session trace recorder lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: session trace recorder lock wait", true)
WAIT_EVENT_DEF(TRANS_TRACE_RECORDER_LOCK_WAIT, 15061, "latch: trans trace recorder lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: trans trace recorder lock wait", true)
WAIT_EVENT_DEF(ELECT_TRACE_RECORDER_LOCK_WAIT, 15062, "latch: election trace recorder lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: election trace recorder lock wait", true)
WAIT_EVENT_DEF(ALIVE_SERVER_TRACER_LOCK_WAIT, 15063, "latch: alive server tracer lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: alive server tracer lock wait", true)
WAIT_EVENT_DEF(ALLOC_CHUNK_LOCK_WAIT, 15064, "latch: allocator chunk lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: allocator chunk lock wait", true)
WAIT_EVENT_DEF(ALLOC_TENANT_LOCK_WAIT, 15065, "latch: allocator tenant lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: tenant lock in allocator lock wait", true)
WAIT_EVENT_DEF(IO_QUEUE_LOCK_WAIT, 15066, "latch: io queue lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: io queue lock wait", true)
WAIT_EVENT_DEF(ZONE_INFO_RW_LOCK_WAIT, 15067, "latch: zone infos rw lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: zone infos rw lock wait", true)
WAIT_EVENT_DEF(MT_TRACE_RECORDER_LOCK_WAIT, 15068, "latch: memtable trace recorder lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: memtable trace recorder lock wait", true)
WAIT_EVENT_DEF(BANDWIDTH_THROTTLE_LOCK_WAIT, 15069, "latch: bandwidth throttle lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: bandwidth throttle lock wait", true)
WAIT_EVENT_DEF(RS_EVENT_TS_LOCK_WAIT, 15070, "latch: rs event table timestamp lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: rs event table timestamp lock wait", true)
WAIT_EVENT_DEF(CLOG_FD_CACHE_LOCK_WAIT, 15071, "latch: clog fd cache lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: clog fd cache lock wait", true)
WAIT_EVENT_DEF(MIGRATE_LOCK_WAIT, 15072, "latch: migrate lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: migrate lock wait", true)
WAIT_EVENT_DEF(CLOG_CASCADING_INFO_LOCK_WAIT, 15073, "latch: clog cascading info lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog cascading info lock wait", true)
WAIT_EVENT_DEF(CLOG_LOCALITY_LOCK_WAIT, 15074, "latch: clog locality lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: clog locality lock wait", true)
WAIT_EVENT_DEF(PRIORITY_TASK_QUEUE_LOCK_WAIT, 15075, "latch: priority task queue lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: priority task queue lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_LOCK_WAIT, 15076, "latch: group migrate lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: group migrate lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_TASK_LOCK_WAIT, 15077, "latch: group migrate task lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: group migrate task lock wait", true)
WAIT_EVENT_DEF(GROUP_MIGRATE_TASK_IDLE_WAIT, 15078, "latch: group migrate task idle wait", "address", "number", "tries",
    CONCURRENCY, "latch: group migrate task idle wait", true)
WAIT_EVENT_DEF(LOG_ENGINE_ENV_SWITCH_LOCK_WAIT, 15079, "latch: log engine env switch lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: log engine env switch lock wait", true)
WAIT_EVENT_DEF(CLOG_CONSEC_INFO_MGR_LOCK_WAIT, 15081, "latch: clog consec info mgr lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog consec info mgr lock wait", true)
WAIT_EVENT_DEF(ELECTION_GROUP_LOCK_WAIT, 15082, "latch: election group lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: election group lock wait", true)
WAIT_EVENT_DEF(ELECTION_GROUP_TRACE_RECORDER_LOCK_WAIT, 15083, "latch: election group trace recorder lock wait",
    "address", "number", "tries", CONCURRENCY, "latch: election group trace recorder lock wait", true)
WAIT_EVENT_DEF(LATCH_WAIT_QUEUE_LOCK_WAIT, 15084, "latch: wait queue lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: wait queue lock wait", true)
WAIT_EVENT_DEF(ID_MAP_NODE_LOCK_WAIT, 15085, "latch: id map node lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: id map node lock wait", true)
WAIT_EVENT_DEF(EXTENT_STORAGE_MANAGER_LOCK_WAIT, 15087, "latch: extent storage manager lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: extent storage manager lock wait", true)
WAIT_EVENT_DEF(REBUILD_RETRY_LIST_LOCK_WAIT, 15088, "latch: rebuild retry list lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: rebuild retry list lock wait", true)
WAIT_EVENT_DEF(CLOG_SWITCH_INFO_LOCK_WAIT, 15089, "latch: clog switch info lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: clog switch info lock wait", true)
WAIT_EVENT_DEF(CLOG_IDC_LOCK_WAIT, 15090, "latch: clog idc lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: clog idc lock wait", true)
WAIT_EVENT_DEF(PG_STORAGE_BUCKET_LOCK_WAIT, 15091, "latch: pg_storage bucket lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: pg_storage bucket lock wait", true)

WAIT_EVENT_DEF(
    DEFAULT_COND_WAIT, 15101, "default condition wait", "address", "", "", CONCURRENCY, "default condition wait", true)
WAIT_EVENT_DEF(DEFAULT_SLEEP, 15102, "default sleep", "", "", "", CONCURRENCY, "default sleep", true)
WAIT_EVENT_DEF(CLOG_WRITER_COND_WAIT, 15103, "clog writer condition wait", "address", "", "", CONCURRENCY,
    "clog writer condition wait", true)
WAIT_EVENT_DEF(IO_CONTROLLER_COND_WAIT, 15104, "io controller condition wait", "address", "", "", CONCURRENCY,
    "io controller condition wait", true)
WAIT_EVENT_DEF(IO_PROCESSOR_COND_WAIT, 15105, "io processor condition wait", "address", "", "", CONCURRENCY,
    "io processor condition wait", true)
WAIT_EVENT_DEF(DEDUP_QUEUE_COND_WAIT, 15106, "dedup queue condition wait", "address", "", "", CONCURRENCY,
    "dedup queue condition wait", true)
WAIT_EVENT_DEF(SEQ_QUEUE_COND_WAIT, 15107, "seq queue condition wait", "address", "", "", CONCURRENCY,
    "seq queue condition wait", true)
WAIT_EVENT_DEF(INNER_CONNECTION_POOL_COND_WAIT, 15108, "inner connection pool condition wait", "address", "", "",
    CONCURRENCY, "inner connection pool condition wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_COND_WAIT, 15109, "partition table updater condition wait", "address", "", "",
    CONCURRENCY, "partition table updater condition wait", true)
WAIT_EVENT_DEF(REBALANCE_TASK_MGR_COND_WAIT, 15110, "rebalance task mgr condition wait", "address", "", "", CONCURRENCY,
    "rebalance task mgr condition wait", true)
WAIT_EVENT_DEF(ASYNC_RPC_PROXY_COND_WAIT, 15111, "async rpc proxy condition wait", "address", "", "", CONCURRENCY,
    "async rpc proxy condition wait", true)
WAIT_EVENT_DEF(THREAD_IDLING_COND_WAIT, 15112, "thread idling condition wait", "address", "", "", CONCURRENCY,
    "thread idling condition wait", true)
WAIT_EVENT_DEF(RPC_SESSION_HANDLER_COND_WAIT, 15113, "rpc session handler condition wait", "address", "", "",
    CONCURRENCY, "rpc session handler condition wait", true)
WAIT_EVENT_DEF(LOCATION_CACHE_COND_WAIT, 15114, "location cache condition wait", "address", "", "", CONCURRENCY,
    "location cache condition wait", true)
WAIT_EVENT_DEF(REENTRANT_THREAD_COND_WAIT, 15115, "reentrant thread condition wait", "address", "", "", CONCURRENCY,
    "reentrant thread condition wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_COND_WAIT, 15116, "major freeze condition wait", "address", "", "", CONCURRENCY,
    "major freeze condition wait", true)
WAIT_EVENT_DEF(MINOR_FREEZE_COND_WAIT, 15117, "minor freeze condition wait", "address", "", "", CONCURRENCY,
    "minor freeze condition wait", true)
WAIT_EVENT_DEF(TH_WORKER_COND_WAIT, 15118, "th worker condition wait", "address", "", "", CONCURRENCY,
    "th worker condition wait", true)
WAIT_EVENT_DEF(DEBUG_SYNC_COND_WAIT, 15119, "debug sync condition wait", "address", "", "", CONCURRENCY,
    "debug sync condition wait", true)
WAIT_EVENT_DEF(EMPTY_SERVER_CHECK_COND_WAIT, 15120, "empty server check condition wait", "address", "", "", CONCURRENCY,
    "empty server check condition wait", true)
WAIT_EVENT_DEF(SCHEDULER_COND_WAIT, 15121, "ob scheduler condition wait", "address", "", "", CONCURRENCY,
    "ob scheduler condition wait", true)
WAIT_EVENT_DEF(RESTORE_READER_COND_WAIT, 15122, "ob restore reader condition wait", "address", "", "", CONCURRENCY,
    "ob restore reader condition wait", true)
WAIT_EVENT_DEF(DYNAMIC_THREAD_POOL_COND_WAIT, 15123, "ob dynamic thread pool condition wait", "address", "", "",
    CONCURRENCY, "ob dynamic thread pool condition wait", true)
WAIT_EVENT_DEF(DELETE_DISK_COND_WAIT, 15124, "delete disk condition wait", "address", "", "", CONCURRENCY,
    "delete disk condition wait", true)
WAIT_EVENT_DEF(SLOG_FLUSH_COND_WAIT, 15125, "slog flush condition wait", "address", "", "", CONCURRENCY,
    "slog flush condition wait", true)
WAIT_EVENT_DEF(BUILD_INDEX_SCHEDULER_COND_WAIT, 15126, "build index scheduler condition wait", "address", "", "",
    CONCURRENCY, "build index scheduler condition wait", true)
WAIT_EVENT_DEF(TBALE_MGR_LOCK_WAIT, 15127, "table mgr wait", "address", "number", "tries", CONCURRENCY,
    "table mgr lock wait", false)
WAIT_EVENT_DEF(PARTITION_STORE_LOCK_WAIT, 15128, "partition store lock wait", "address", "number", "tries", CONCURRENCY,
    "partition store lock wait", false)
WAIT_EVENT_DEF(PARTITION_STORE_CHANGE_LOCK_WAIT, 15129, "partition store change lock wait", "address", "number",
    "tries", CONCURRENCY, "partition store change lock wait", false)
WAIT_EVENT_DEF(PARTITION_STORE_NEW_MEMTABLE_LOCK_WAIT, 15130, "partition store new memtable lock wait", "address",
    "number", "tries", CONCURRENCY, "partition store new memtable lock wait", false)
WAIT_EVENT_DEF(BUILD_INDEX_COMPACT_COND_WAIT, 15131, "build index compaction condition wait", "address", "", "",
    CONCURRENCY, "build index compaction condition wait", false)
WAIT_EVENT_DEF(DAG_WORKER_COND_WAIT, 15132, "dag worker condition wait", "address", "", "", CONCURRENCY,
    "dag worker condition wait", false)
WAIT_EVENT_DEF(TBALE_MGR_MAP_WAIT, 15133, "table mgr map wait", "address", "number", "tries", CONCURRENCY,
    "table mgr map wait", false)
WAIT_EVENT_DEF(IO_CALLBACK_QUEUE_LOCK_WAIT, 15134, "io callback queue lock wait", "address", "number", "tries",
    CONCURRENCY, "io callback queue lock wait", true)
WAIT_EVENT_DEF(IO_CHANNEL_LOCK_WAIT, 15135, "io channel lock wait", "address", "number", "tries", CONCURRENCY,
    "io channel lock wait", true)
WAIT_EVENT_DEF(PARTITION_AUDIT_SPIN_LOCK_WAIT, 15136, "latch: partition audit spin lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: partition audit spin lock wait", true)
WAIT_EVENT_DEF(PARTITION_GROUP_LOCK_WAIT, 15137, "partition group lock wait", "address", "number", "tries", CONCURRENCY,
    "partition group lock wait", false)
WAIT_EVENT_DEF(PX_WORKER_LEADER_LOCK_WAIT, 15138, "latch: px worker leader spin lock wait", "address", "number",
    "tries", CONCURRENCY, "latch:  px worker leader spin lock wait", true)
WAIT_EVENT_DEF(RAID_WRITE_RECORD_LOCK_WAIT, 15139, "latch: raid write record lock wait", "address", "number", "tries",
    CONCURRENCY, "latch:  raid write record lock wait", true)
WAIT_EVENT_DEF(RS_GTS_TASK_MGR_COND_WAIT, 15140, "rs gts task mgr condition wait", "address", "", "", CONCURRENCY,
    "rs gts task mgr condition wait", true)
WAIT_EVENT_DEF(OB_ALLOCATOR_LOCK_WAIT, 15141, "latch: ob allocator lock wait", "address", "number", "tries",
    CONCURRENCY, "latch: ob allocator lock wait", true)
WAIT_EVENT_DEF(MACRO_META_MAJOR_KEY_MAP_LOCK_WAIT, 15142, "latch: macro block meta major key map lock wait", "address",
    "number", "tries", CONCURRENCY, "latch:  macro block meta major key map lock wait", true)
WAIT_EVENT_DEF(
    OB_CONTEXT_LOCK_WAIT, 15143, "ob context lock wait", "address", "", "", CONCURRENCY, "ob context lock wait", true)
WAIT_EVENT_DEF(OB_LOG_ARCHIVE_SCHEDULER_LOCK_WAIT, 15144, "ob log archive scheduler lock wait", "address", "", "",
    CONCURRENCY, "ob log archive scheduler lock wait", true)
WAIT_EVENT_DEF(OB_REQ_TIMEINFO_LIST_WAIT, 15145, "ob request timeinfo list wait", "address", "", "", CONCURRENCY,
    "ob reqyest timeinfo list wait", true)
WAIT_EVENT_DEF(BACKUP_INFO_MGR_LOCK_WAIT, 15146, "backup info wait", "address", "number", "tries", CONCURRENCY,
    "backup info lock wait", false)
WAIT_EVENT_DEF(HASH_MAP_LOCK_WAIT, 15147, "hashmap lock wait", "address", "number", "tries", CONCURRENCY,
    "hashmap lock wait", true)
WAIT_EVENT_DEF(FILE_REF_LOCK_WAIT, 15148, "file ref lock wait", "address", "number", "tries", CONCURRENCY,
    "file ref lock wait", true)
WAIT_EVENT_DEF(TIMEZONE_LOCK_WAIT, 15149, "latch: timezone lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: timezone lock wait", true)
WAIT_EVENT_DEF(ARCHIVE_RESTORER_LOCK_WAIT, 15150, "latch: archive restore queue lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: archive restore queue lock wait", true)
WAIT_EVENT_DEF(XA_STMT_LOCK_WAIT, 15151, "latch: xa stmt lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: xa stmt lock wait", true)
WAIT_EVENT_DEF(XA_QUEUE_LOCK_WAIT, 15152, "latch: xa queue lock wait", "address", "number", "tries", CONCURRENCY,
    "latch: xa queue lock wait", true)

// transaction
WAIT_EVENT_DEF(END_TRANS_WAIT, 16001, "wait end trans", "rollback", "trans_hash_value", "participant_count", COMMIT,
    "wait end trans", false)
WAIT_EVENT_DEF(START_STMT_WAIT, 16002, "wait start stmt", "trans_hash_value", "physic_plan_type", "participant_count",
    CLUSTER, "wait start stmt", false)
WAIT_EVENT_DEF(END_STMT_WAIT, 16003, "wait end stmt", "rollback", "trans_hash_value", "physic_plan_type", CLUSTER,
    "wait end stmt", false)
WAIT_EVENT_DEF(REMOVE_PARTITION_WAIT, 16004, "wait remove partition", "tenant_id", "table_id", "partition_id",
    ADMINISTRATIVE, "wait remove partition", false)
WAIT_EVENT_DEF(TRANS_BATCH_RPC_LOCK_WAIT, 16005, "trans wait batch rpc lock", "address", "number", "tries", CONCURRENCY,
    "trans wait batch rpc lock", true)
WAIT_EVENT_DEF(CLOG_RENEW_MS_TASK_LOCK_WAIT, 16006, "latch: clog sw renew ms task lock wait", "address", "number",
    "tries", CONCURRENCY, "latch: clog sw renew ms task lock wait", true)
WAIT_EVENT_DEF(
    UNDO_STATUS_LOCK_WAIT, 16007, "undo status lock wait", "", "", "", CONCURRENCY, "UNDO_STATUS_LOCK_WAIT", true)
WAIT_EVENT_DEF(FREEZE_ASYNC_WORKER_LOCK_WAIT, 16008, "freeze async worker lock wait", "", "", "", CONCURRENCY,
    "FREEZE_ASYNC_WORKER_LOCK_WAIT", true)

// replication group
WAIT_EVENT_DEF(RG_TRANSFER_LOCK_WAIT, 17000, "transfer lock wait", "src_rg", "dst_rg", "transfer_pkey", CONCURRENCY,
    "transfer lock wait", false)

// liboblog
WAIT_EVENT_DEF(OBLOG_PART_MGR_SCHEMA_VERSION_WAIT, 18000, "oblog part mgr schema version wait", "", "", "", CONCURRENCY,
    "oblog part mgr schema version wait", true)

WAIT_EVENT_DEF(WAIT_EVENT_END, 99999, "event end", "", "", "", OTHER, "event end", false)
#endif

#ifndef OB_WAIT_EVENT_DEFINE_H_
#define OB_WAIT_EVENT_DEFINE_H_

#include "lib/ob_errno.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/wait_event/ob_wait_class.h"

namespace oceanbase {
namespace common {
static const int64_t MAX_WAIT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_WAIT_EVENT_PARAM_LENGTH = 64;
static const int64_t SESSION_WAIT_HISTORY_NEST = 10;

struct ObWaitEventIds {
  enum ObWaitEventIdEnum {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, display_name, is_phy) def,
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
  };
};

struct ObWaitEventDesc {
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
  bool is_phy_;
  ObWaitEventDesc()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  inline bool operator<(const ObWaitEventDesc& other) const;
  inline bool operator>(const ObWaitEventDesc& other) const;
  inline bool operator==(const ObWaitEventDesc& other) const;
  inline bool operator!=(const ObWaitEventDesc& other) const;
  inline int add(const ObWaitEventDesc& other);
  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

struct ObWaitEventStat {
  // total number of waits for a event
  uint64_t total_waits_;
  // total number of timeouts for a event
  uint64_t total_timeouts_;
  // total amount of time waited for a event
  uint64_t time_waited_;
  // max amount of time waited for a event
  uint64_t max_wait_;
  ObWaitEventStat()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  int add(const ObWaitEventStat& other);
  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  inline bool is_valid() const
  {
    return total_waits_ > 0;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
};

struct ObWaitEvent {
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
 * -----------------------------------------------------Inline
 * Methods------------------------------------------------------
 */
inline bool ObWaitEventDesc::operator<(const ObWaitEventDesc& other) const
{
  return wait_begin_time_ < other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator>(const ObWaitEventDesc& other) const
{
  return wait_begin_time_ > other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator==(const ObWaitEventDesc& other) const
{
  return event_no_ == other.event_no_ && p1_ == other.p1_ && p2_ == other.p2_ && p3_ == other.p3_ &&
         wait_begin_time_ == other.wait_begin_time_ && wait_end_time_ == other.wait_end_time_ &&
         wait_time_ == other.wait_time_ && timeout_ms_ == other.timeout_ms_ && level_ == other.level_;
}

inline bool ObWaitEventDesc::operator!=(const ObWaitEventDesc& other) const
{
  return !(*this == (other));
}

inline int ObWaitEventDesc::add(const ObWaitEventDesc& other)
{
  int ret = common::OB_SUCCESS;
  if (other.wait_begin_time_ > wait_begin_time_) {
    *this = other;
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
