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

#ifdef LATCH_DEF
/**
 * LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, enable)
 * @param def Name of this latch
 * @param id Identifier of an latch ATTENTION: please add id placeholder on master.
 * @param name Name for this latch. Display on virtual table v$event_name
 * @param policy LATCH_READ_PREFER and LATCH_FIFO
 * @param max_spin_cnt for mutex, spin several times to try to get lock before actually perform an mutex lock.
 * @param max_yield_cnt times of call to sched_yield() instead of perform an mutex lock.
 * @param enable Indicate whether this latch is enabled. Marked it false it you merely need it as an placeholder.
 * NOTICE: No need to add wait event when adding latch
 * NOTICE: do not reuse latch id or rename latch!
*/
LATCH_DEF(LATCH_WAIT_QUEUE_LOCK, 0, "latch wait queue lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEFAULT_SPIN_LOCK, 1, "default spin lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEFAULT_SPIN_RWLOCK, 2, "default spin rwlock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEFAULT_MUTEX, 3, "default mutex", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(KV_CACHE_BUCKET_LOCK, 4, "kv cache bucket latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TIME_WHEEL_TASK_LOCK, 5, "time wheel task latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TIME_WHEEL_BUCKET_LOCK, 6, "time wheel bucket latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ELECTION_LOCK, 7, "election latch", LATCH_FIFO, 20000000L, 0, true)
LATCH_DEF(TRANS_CTX_LOCK, 8, "trans ctx latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PARTITION_LOG_LOCK, 9, "partition log latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PCV_SET_LOCK, 10, "pcv set latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_HISTORY_REPORTER_LOCK, 11, "clog history reporter latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_EXTERNAL_EXEC_LOCK, 12, "clog external executor latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_MEMBERSHIP_MGR_LOCK, 13, "clog member ship mgr latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_RECONFIRM_LOCK, 14, "clog reconfirm latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_SLIDING_WINDOW_LOCK, 15, "clog sliding window latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_STAT_MGR_LOCK, 16, "clog stat mgr latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_TASK_LOCK, 17, "clog task latch", LATCH_FIFO, 20000000L, 0, true)
LATCH_DEF(CLOG_IDMGR_LOCK, 18, "clog id mgr latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_CACHE_LOCK, 19, "clog cache latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ELECTION_MSG_LOCK, 20, "election msg latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PLAN_SET_LOCK, 21, "plan set latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PS_STORE_LOCK, 22, "ps store latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TRANS_CTX_MGR_LOCK, 23, "trans ctx mgr latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ROW_LOCK, 24, "row latch", LATCH_READ_PREFER, 200, 0, true)
LATCH_DEF(DEFAULT_RECURSIVE_MUTEX, 25, "default recursive mutex", LATCH_FIFO, 2000L, 0, false)
LATCH_DEF(DEFAULT_DRW_LOCK, 26, "default drw lock", LATCH_FIFO, 200000, 0, true)
LATCH_DEF(DEFAULT_BUCKET_LOCK, 27, "default bucket lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TRANS_CTX_BUCKET_LOCK, 28, "trans ctx bucket lock", LATCH_FIFO, 20000000L, 0, false)
LATCH_DEF(MACRO_WRITER_LOCK, 29, "macro writer lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TOKEN_BUCKET_LOCK, 30, "token bucket lock", LATCH_FIFO, 20000000L, 0, false)
LATCH_DEF(LIGHTY_HASHMAP_BUCKET_LOCK, 31, "light hashmap bucket lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ROW_CALLBACK_LOCK, 32, "row callback lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(LS_LOCK, 33, "ls latch", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(NOUSE_34, 34, "not used 34", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(SWITCH_LEADER_LOCK, 35, "switch leader lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PARTITION_FREEZE_LOCK, 36, "partition freeze lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(SCHEMA_SERVICE_LOCK, 37, "schema service lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(SCHEMA_SERVICE_STATS_LOCK, 38, "schema service stats lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(TENANT_LOCK, 39, "tenant lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(CONFIG_LOCK, 40, "config lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(MAJOR_FREEZE_LOCK, 41, "major freeze lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(PARTITION_TABLE_UPDATER_LOCK, 42, "partition table updater lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(MULTI_TENANT_LOCK, 43, "multi tenant lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(LEADER_COORDINATOR_LOCK, 44, "leader coordinator lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(LEADER_STAT_LOCK, 45, "leader stat lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(MAJOR_FREEZE_SERVICE_LOCK, 46, "major freeze service lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(RS_BOOTSTRAP_LOCK, 47, "rs bootstrap lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(SCHEMA_MGR_ITEM_LOCK, 48, "schema mgr item lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(SCHEMA_MGR_LOCK, 49, "schema mgr lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(SUPER_BLOCK_LOCK, 50, "super block lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(FROZEN_VERSION_LOCK, 51, "frozen version lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(RS_BROADCAST_LOCK, 52, "rs broadcast lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(SERVER_STATUS_LOCK, 53, "server status lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(SERVER_MAINTAINCE_LOCK, 54, "server maintaince lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(UNIT_MANAGER_LOCK, 55, "unit manager lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(ZONE_MANAGER_LOCK, 56, "zone manager maintaince lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(ALLOC_OBJECT_LOCK, 57, "object set lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(ALLOC_BLOCK_LOCK, 58, "block set lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(TRACE_RECORDER_LOCK, 59, "normal trace recorder lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SESSION_TRACE_RECORDER_LOCK, 60, "session trace recorder lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TRANS_TRACE_RECORDER_LOCK, 61, "trans trace recorder lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ELECT_TRACE_RECORDER_LOCK, 62, "election trace recorder lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ALIVE_SERVER_TRACER_LOCK, 63, "alive server tracer lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(ALLOC_CHUNK_LOCK, 64, "allocate chunk lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ALLOC_TENANT_LOCK, 65, "allocate tenant lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(IO_QUEUE_LOCK, 66, "io queue lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ZONE_INFO_RW_LOCK, 67, "zone infos rw lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(MT_TRACE_RECORDER_LOCK, 68, "memtable trace recorder lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(BANDWIDTH_THROTTLE_LOCK, 69, "bandwidth throttle lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RS_EVENT_TS_LOCK, 70, "rs event table timestamp lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_FD_CACHE_LOCK, 71, "clog fd cache lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(MIGRATE_LOCK, 72, "migrate lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_CASCADING_INFO_LOCK, 73, "clog cascading info lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_LOCALITY_LOCK, 74, "clog locality lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(DTL_CHANNEL_WAIT, 75, "DTL channel wait", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(GROUP_MIGRATE_LOCK, 76, "group migrate lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(GROUP_MIGRATE_TASK_LOCK, 77, "group migrate task lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(LOG_ENGINE_ENV_SWITCH_LOCK, 78, "log engine env switch lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_CONSEC_INFO_MGR_LOCK, 79, "clog consec log info mgr latch", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(RG_TRANSFER_LOCK, 80, "rg transfer lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TABLE_MGR_LOCK, 81, "table mgr lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PARTITION_STORE_LOCK, 82, "partition store lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PARTITION_STORE_CHANGE_LOCK, 83, "partition store change lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TABLET_MEMTABLE_LOCK, 84, "tablet memtable lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ELECTION_GROUP_LOCK, 85, "election group latch", LATCH_FIFO, 20000000L, 0, false)
LATCH_DEF(ELECTION_GROUP_TRACE_RECORDER_LOCK, 86, "election group trace recorder lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK, 87, "ObCacheLineSegregatedArrayBase alloc lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(SERVER_OBJECT_POOL_ARENA_LOCK, 88, "server object pool arena lock", LATCH_FIFO, 20000000L, 0, true)
LATCH_DEF(TABLE_MGR_MAP, 89, "table mgr map", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ID_MAP_NODE_LOCK, 90, "id map node lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(EXTENT_STORAGE_MANAGER_LOCK, 91, "extent storage manager lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(BLOCK_MANAGER_LOCK, 92, "block manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(REBUILD_RETRY_LIST_LOCK, 93, "rebuild retry list lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PARTITION_AUDIT_SPIN_LOCK, 94, "partition audit spin lock", LATCH_FIFO, 20000000L, 0, true)
LATCH_DEF(CLOG_SWITCH_INFO_LOCK, 95, "clog switch_info latch", LATCH_FIFO, 20000000L, 0, false)
LATCH_DEF(PARTITION_GROUP_LOCK, 96, "partition group lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PX_WORKER_LEADER_LOCK, 97, "px worker leader lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_IDC_LOCK, 98, "clog idc lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TABLET_BUCKET_LOCK, 99, "tablet bucket lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_ALLOCATOR_LOCK, 100, "ob allocator lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(BLOCK_ID_GENERATOR_LOCK, 101, "block id generator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_CONTEXT_LOCK, 102, "ob context lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(OB_PG_INDEX_LOCK, 103, "ob pg index lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(OB_LOG_ARCHIVE_SCHEDULER_LOCK, 104, "ob log archive scheduler lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(MEM_DUMP_ITER_LOCK, 105, "mem dump iter lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_REQ_TIMEINFO_LIST_LOCK, 106, "ob request timeinfo list lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TRANS_BATCH_RPC_LOCK, 107, "trans batch rpc lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(BACKUP_INFO_MGR_LOCK, 108, "backup info mgr lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CLOG_RENEW_MS_TASK_LOCK, 109, "clog sw renew ms task spin lock", LATCH_FIFO, 20000000L, 0, false)
LATCH_DEF(HASH_MAP_LOCK, 110, "hashmap latch lock", LATCH_FIFO, 2000L, 0, true)
LATCH_DEF(FILE_REF_LOCK, 111, "file ref latch lock", LATCH_FIFO, 2000L, 0, false)
LATCH_DEF(TIMEZONE_LOCK, 112, "timezone lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(UNDO_STATUS_LOCK, 113, "undo status lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(FREEZE_ASYNC_WORKER_LOCK, 114, "freeze async worker lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(XA_STMT_LOCK, 115, "xa stmt lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(XA_QUEUE_LOCK, 116, "xa queue lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(MEMTABLE_CALLBACK_LIST_LOCK, 117, "memtable calback list lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(MEMTABLE_CALLBACK_LIST_MGR_LOCK, 118, "memtable calback list mgr lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(XA_CTX_LOCK, 119, "xa queue lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MASTER_KEY_LOCK, 120, "master key lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RATELIMIT_LOCK, 121, "ratelimit lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(TRX_EXEC_CTX_LOCK, 122, "transaction execution ctx lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TENANT_DISK_USAGE_LOCK, 123, "tenant disk usage lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(LOG_OFFSET_ALLOC_LOCK, 124, "log_offset allocator latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_META_MEM_MGR_LOCK, 125, "tenant meta memory manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(GTI_SOURCE_LOCK, 126, "update trans id assignable interval lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(APPLY_STATUS_LOCK, 127, "apply_status latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ID_SOURCE_LOCK, 128, "ID allocator updates assignable interval lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TRANS_AUDIT_RECORD_LOCK, 129, "trans records audit information lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TABLET_MULTI_SOURCE_DATA_LOCK, 130, "tablet multi source data lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DTL_CHANNEL_MGR_LOCK, 131, "DTL channel mgr wait", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PL_DEBUG_LOCK, 132, "PL DEBUG lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PL_SYS_BREAKPOINT_LOCK, 133, "PL sys breakpoint lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DBMS_JOB_TASK_LOCK, 134, "dbms_job task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DBMS_JOB_MASTER_LOCK, 135, "dbms_job master lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DTL_CHANNEL_LIST_LOCK,136, "dtl channel list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DTL_CACHED_BUFFER_LIST_LOCK,137, "dtl free buffer list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PS_CACHE_EVICT_MUTEX_LOCK, 138, "plan cache evict lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_SHARED_HJ_COND_LOCK, 139, "shared hash join lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_WA_PROFILE_LIST_LOCK, 140, "sql work area profile list lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_WA_STAT_MAP_LOCK, 141, "sql work area stat map lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_MEMORY_MGR_MUTEX_LOCK, 142, "sql memory manager mutex lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(LOAD_DATA_RPC_CB_LOCK, 143, "load data rpc asyn callback lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_DYN_SAMPLE_MSG_LOCK, 144, "merge dynamic sampling piece message lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_GI_SHARE_POOL_LOCK, 145, "granule iterator task queue lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DTL_RECV_CHANNEL_PROVIDER_LOCK, 146, "dtl receive channel provider access lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PX_TENANT_TARGET_LOCK, 147, "parralel execution tenant target lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PX_WORKER_STAT_LOCK, 148, "parallel execution worker stat lock",LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SESSION_QUERY_LOCK, 149, "session query lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SESSION_THREAD_DATA_LOCK, 150, "session thread data lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SESSION_POOL_MAP_LOCK, 151, "maintain session pool lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(SEQUENCE_VALUE_ALLOC_LOCK, 152, "sequence value alloc lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SEQUENCE_CACHE_LOCK, 153, "sequence cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_PROGRESS_RECYCLE_LOCK, 154, "obcdc progress recycle lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_METAINFO_LOCK, 155, "obcdc metainfo lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_TRANS_CTX_LOCK, 156, "obcdc trans ctx lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_SVR_BLACKLIST_LOCK, 157, "obcdc svr blacklist lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_SQLSERVER_LOCK, 158, "obcdc sqlserver lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_TIMEZONE_GETTER_LOCK, 159, "obcdc timezone getter lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_FETCHLOG_ARPC_LOCK, 160, "obcdc fetchlog arpc lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBCDC_FETCHSREAM_CONTAINER_LOCK, 161, "obcdc fetchstream container lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(INNER_CONN_POOL_LOCK, 162, "inner connection pool lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_RES_MGR_LIST_LOCK, 163, "tenant resource mgr list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(NOUSE_164, 164, "not used 164", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TC_FREE_LIST_LOCK, 165, "tc free list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEDUP_QUEUE_LOCK, 166, "dedup queue lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SLOG_PROCESSING_MUTEX, 167, "slog processing mutex", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(TABLET_TABLE_STORE_LOCK, 168, "tablet table store lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TMP_FILE_LOCK, 169, "tmp file lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(TMP_FILE_EXTENT_LOCK, 170, "tmp file extent lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(TMP_FILE_MGR_LOCK, 171, "tmp file manager lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(TMP_FILE_STORE_LOCK, 172, "tmp file store lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TMP_FILE_MACRO_LOCK, 173, "tmp file macro lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(INDEX_BUILDER_LOCK, 174, "sstable index builder lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(SLOG_CKPT_LOCK, 175, "slog checkpoint lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(LOCAL_DEVICE_LOCK, 176, "local device lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(FIXED_SIZE_ALLOCATOR_LOCK, 177, "fixed size allocator lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(DBMS_SCHEDULER_TASK_LOCK, 178, "dbms_scheduler task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DBMS_SCHEDULER_MASTER_LOCK, 179, "dbms_scheuler master lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_WORKER_LOCK, 180, "tenant worker lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SERVER_LOCALITY_CACHE_LOCK, 181, "server locality cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MASTER_RS_CACHE_LOCK, 182, "master rs cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SCHEMA_REFRESH_INFO_LOCK, 183, "schema refresh info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(REFRESH_SCHEMA_LOCK, 184, "refresh schema lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(REFRESHED_SCHEMA_CACHE_LOCK, 185, "refreshed schema cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SCHEMA_MGR_CACHE_LOCK, 186, "schema mgr cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RS_MASTER_KEY_RESPONSE_LOCK, 187, "rs master key respone lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RS_MASTER_KEY_REQUEST_LOCK, 188, "rs master key request lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RS_MASTER_KEY_MGR_LOCK, 189, "rs master key mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(THREAD_HANG_CHECKER_LOCK, 190, "thread hang checker lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CREATE_INNER_SCHEMA_EXECUTOR_LOCK, 191, "create inner schema executor lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WRS_SERVER_VERSION_LOCK, 192, "weak read server version lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TX_LS_LOG_WRITER_LOCK, 193, "transaction ls log writer lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TX_DESC_LOCK, 194, "transaction descriptor lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(NOUSE_195, 195, "not used 195", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TX_DESC_COMMIT_LOCK, 196, "transaction descriptor commit lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WRS_CLUSTER_SERVICE_LOCK, 197, "weak read service cluster service lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TX_STAT_ITEM_LOCK, 198, "transaction stat item lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WRS_CLUSTER_VERSION_MGR_LOCK, 199, "weak read service cluster version manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MAJOR_FREEZE_SWITCH_LOCK, 200, "major freeze switch lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ZONE_MERGE_MANAGER_READ_LOCK, 201, "zone merge manager read lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ZONE_MERGE_MANAGER_WRITE_LOCK, 202, "zone merge manager write lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(AUTO_INCREMENT_INIT_LOCK, 203, "auto increment init lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(AUTO_INCREMENT_ALLOC_LOCK, 204, "auto increment alloc lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(AUTO_INCREMENT_SYNC_LOCK, 205, "auto increment sync lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(AUTO_INCREMENT_GAIS_LOCK, 206, "auto increment GAIS lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(AUTO_INCREMENT_LEADER_LOCK, 207, "auto increment leader lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(NOUSE_208, 208, "not used 208", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(NOUSE_209, 209, "not used 209", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ALLOC_MEM_DUMP_TASK_LOCK, 210, "alloc memory dump task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ALLOC_ESS_LOCK, 211, "alloc expand, shrink and segment lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CONCURRENT_BITSET_LOCK, 212, "concurrent bitset lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(OB_SEG_ARRAY_LOCK, 213, "seg array lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(OB_AREAN_ALLOCATOR_LOCK, 214, "arena allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_CACHED_ALLOCATOR_LOCK, 215, "cached allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_DELAY_FREE_ALLOCATOR_LOCK, 216, "delay free allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_FIFO_ALLOCATOR_LOCK, 217, "FIFO allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PAGE_MANAGER_LOCK, 218, "page manager lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(SIMPLE_FIFO_ALLOCATOR_LOCK, 219, "simple FIFO allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_DLIST_LOCK, 220, "DList lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_GLOBAL_FREE_LIST_LOCK, 221, "global freelist lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OB_FREEZE_INFO_MANAGER_LOCK, 222, "freeze info manager lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(CHUNK_FREE_LIST_LOCK, 223, "chunk free list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CHUNK_USING_LIST_LOCK, 224, "chunk using list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WORK_DAG_LOCK, 225, "work dag lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WORK_DAG_NET_LOCK, 226, "work dag_net lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SYS_TASK_STAT_LOCK, 227, "sys task stat lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(INFO_MGR_LOCK, 228, "info mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MERGER_DUMP_LOCK, 229, "merger dump lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLET_MERGE_INFO_LOCK, 230, "tablet merge info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(WASH_OUT_LOCK, 231, "wash out lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(KV_CACHE_INST_LOCK, 232, "kv cache inst lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(KV_CACHE_LIST_LOCK, 233, "kv cache list lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(THREAD_STORE_LOCK, 234, "thread store lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(GLOBAL_KV_CACHE_CONFIG_LOCK, 235, "global kvcache config lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(BACKUP_LOCK, 236, "backup lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RESTORE_LOCK, 237, "restore lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(OBJECT_DEVICE_LOCK, 238, "object device lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(GLOBAL_IO_CONFIG_LOCK, 239, "global io config lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_IO_MANAGE_LOCK, 240, "tenant io manage lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(IO_FAULT_DETECTOR_LOCK, 241, "io fault detector lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLE_API_LOCK, 242, "table api interface lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SERVER_LOCALITY_MGR_LOCK, 243, "server locality manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DDL_LOCK, 244, "ddl task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEADLOCK_LOCK, 245, "deadlock lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(BG_THREAD_MONITOR_LOCK, 246, "background thread monitor lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RPC_STAT_LOCK, 247, "rpc stat lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DBLINK_LOCK, 248, "dblink lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_SW_SUBMIT_INFO_LOCK, 249, "palf sw last submit log info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_SW_COMMITTED_INFO_LOCK, 250, "palf sw committed info lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PALF_SW_SLIDE_INFO_LOCK, 251, "palf sw last slide log info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_SW_FETCH_INFO_LOCK, 252, "palf sw fetch log info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_SW_MATCH_LSN_MAP_LOCK, 253, "palf sw match lsn map lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_SW_LOC_CB_LOCK, 254, "palf sw location cache cb lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(PALF_CM_CONFIG_LOCK, 255, "palf cm config data lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_CM_PARENT_LOCK, 256, "palf cm parent info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_CM_CHILD_LOCK, 257, "palf cm child info lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_HANDLE_IMPL_LOCK, 258, "palf handle impl lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_LOG_ENGINE_LOCK, 259, "log engine lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(PALF_ENV_LOCK, 260, "palf env lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RCS_LOCK, 261, "role change service lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(LS_ARCHIVE_TASK_LOCK, 262, "ls archive task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ARCHIVE_ROUND_MGR_LOCK, 263, "archive round mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ARCHIVE_PERSIST_MGR_LOCK, 264, "archive persist mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ARCHIVE_TASK_QUEUE_LOCK, 265, "archive task queue lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(EXT_SVR_BLACKLIST_LOCK, 266, "external server blacklist lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SSTABLE_INSERT_TABLE_CONTEXT_LOCK, 267, "direct insert table context lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SSTABLE_INSERT_TABLET_CONTEXT_LOCK, 268, "direct insert tablet context lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SSTABLE_INSERT_TABLE_MANAGER_LOCK, 269, "direct insert table manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(COMPLEMENT_DATA_CONTEXT_LOCK, 270, "complement data context lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLET_DDL_KV_MGR_LOCK, 271, "tablet ddl kv mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLET_AUTO_INCREMENT_MGR_LOCK, 272, "tablet auto increment mgr lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLET_AUTO_INCREMENT_SERVICE_LOCK, 273, "tablet auto increment service lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ALL_SERVER_TRACER_LOCK, 274, "all server tracer lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(UPGRADE_STORAGE_FORMAT_VERSION_LOCK, 275, "upgrade storage format version lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(REPLAY_STATUS_LOCK, 276, "replay status lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(REPLAY_STATUS_TASK_LOCK, 277, "replay status task lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MAX_APPLY_SCN_LOCK, 278, "max apply scn lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(GC_HANDLER_LOCK, 279, "gc handler lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MEMSTORE_ALLOCATOR_LOCK, 280, "memstore allocator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(THREAD_POOL_LOCK, 281, "thread pool lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_CKPT_LOCK, 282, "clog checkpoint lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(LS_META_LOCK, 283, "ls meta lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(LS_CHANGE_LOCK, 284, "ls change lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_MEM_USAGE_LOCK, 285, "tenant memory usage lock" , LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TX_TABLE_LOCK, 286, "tx table lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MEMTABLE_STAT_LOCK, 287, "metmable stat lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DEADLOCK_DETECT_LOCK, 288, "deadlock detect lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(FREEZE_THREAD_POOL_LOCK, 289, "freeze thread pool lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DAS_ASYNC_RPC_LOCK, 290, "das wait remote response lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CLOG_CKPT_RWLOCK, 291, "clog checkpoint rwlock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(REWRITE_RULE_ITEM_LOCK, 292, "rewrite rule item lock", LATCH_FIFO, 2000, 0, true)

LATCH_DEF(SRS_LOCK, 293, "srs lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(DDL_EXECUTE_LOCK, 294, "ddl execute lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TENANT_IO_CONFIG_LOCK, 295, "tenant io config lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_WF_PARTICIPATOR_COND_LOCK, 296, "window function participator lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(ARB_SERVER_CONFIG_LOCK, 297, "arbserver config lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(CDC_SERVICE_LS_CTX_LOCK, 298, "cdcservice clientlsctx lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MAJOR_FREEZE_DIAGNOSE_LOCK, 299, "major freeze diagnose lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(HB_RESPONSES_LOCK, 300, "hb responses lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(ALL_SERVERS_INFO_IN_TABLE_LOCK, 301, "all servers info in table lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(OPT_STAT_GATHER_STAT_LOCK, 302, "optimizer stat gather stat lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DUP_TABLET_LOCK, 303, "dup tablet lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TENANT_IO_CALLBACK_LOCK, 304, "support IO callback thread num managemen", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DISPLAY_TASKS_LOCK, 305, "display tasks lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(TMP_FILE_MEM_BLOCK_LOCK, 306, "tmp file mem block lock", LATCH_FIFO, INT64_MAX, 0, true)

LATCH_DEF(LOG_EXTERNAL_STORAGE_IO_TASK_LOCK, 307, "log external storage io task condition", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(LOG_EXTERNAL_STORAGE_HANDLER_RW_LOCK, 308, "log external storage handler rw lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(LOG_EXTERNAL_STORAGE_HANDLER_LOCK, 309, "log external storage handler spin lock", LATCH_FIFO, 2000, 0, true)

LATCH_DEF(PL_DEBUG_RUNTIMEINFO_LOCK, 310, "PL DEBUG RuntimeInfo lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(MDS_TABLE_HANDLER_LOCK, 311, "mds table handler lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(IND_NAME_CACHE_LOCK, 312, "index name cache lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(COMPACTION_DIAGNOSE_LOCK, 313, "compaction diagnose lock", LATCH_READ_PREFER, 2000, 0, true)
LATCH_DEF(OB_MAJOR_MERGE_INFO_MANAGER_LOCK, 314, "major merge info manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(RESOURCE_SERVICE_LOCK, 315, "resource service lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(RESOURCE_SERVICE_SWITCH_LOCK, 316, "resource service switch lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(ASH_LOCK, 317, "ash lock", LATCH_FIFO, 2000, 0, true)

LATCH_DEF(ZONE_STORAGE_MANAGER_LOCK, 318, "zone storage manager maintaince lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(ZONE_STORAGE_INFO_RW_LOCK, 319, "zone storage infos rw lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(DEVICE_MANIFEST_RW_LOCK, 320, "device manifest rw lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(MANIFEST_TASK_LOCK, 321, "manifest task lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(OB_DEVICE_CREDENTIAL_MGR_LOCK, 322, "device credential mgr rw lock", LATCH_READ_PREFER, 2000, 0, false)
LATCH_DEF(DISK_SPACE_MANAGER_LOCK, 323, "share storage disk space manager lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TIERED_SUPER_BLOCK_LOCK, 324, "tiered super block lock", LATCH_FIFO, 2000, 0, false)
LATCH_DEF(TSLOG_PROCESSING_MUTEX, 325, "tslog processing mutex", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TSLOG_CKPT_LOCK, 326, "tslog checkpoint lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(FILE_MANAGER_LOCK, 327, "ile manager lock", LATCH_FIFO, INT64_MAX, 0, false)

LATCH_DEF(TRANS_ACCESS_LOCK, 328, "trans read/write access latch", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TRANS_FLUSH_REDO_LOCK, 329, "trans flush redo log latch", LATCH_FIFO, 2000, 0, true)

LATCH_DEF(TENANT_DIRECT_LOAD_MGR_LOCK, 330, "tenant direct load manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(TABLET_DIRECT_LOAD_MGR_LOCK, 331, "tablet direct load manager lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(DIRECT_LOAD_SLICE_WRITER_LOCK, 332, "direct load slice writer lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(COLUMN_STORE_DDL_RESCAN_LOCK, 333, "column store ddl rescan lock", LATCH_FIFO, INT64_MAX, 0, true)
LATCH_DEF(TABLET_DIRECT_LOAD_MGR_SCHEMA_LOCK, 334, "tablet direct load manager schema lock", LATCH_FIFO, 2000, 0, true)
LATCH_DEF(SQL_AUDIT, 335, "sql audit release second level queue lock", LATCH_FIFO, 2000, 0, true)

LATCH_DEF(S2_PHY_BLOCK_LOCK, 336, "s2 phy block lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(S2_MEM_BLOCK_LOCK, 337, "s2 mem block lock", LATCH_FIFO, INT64_MAX, 0, false)
LATCH_DEF(TENANT_MGR_TENANT_BUCKET_LOCK, 338, "tenant mgr tenant bucket lock", LATCH_READ_PREFER, INT64_MAX, 0, false)

LATCH_DEF(LATCH_END, 339, "latch end", LATCH_FIFO, 2000, 0, true)

#endif

#ifndef OB_LATCH_DEFINE_H_
#define OB_LATCH_DEFINE_H_
#include "lib/wait_event/ob_wait_event.h"

namespace oceanbase
{
namespace common
{

#define LATCH_DEF_true(def, id, name, policy, max_spin_cnt, max_yield_cnt) def,
#define LATCH_DEF_false(def, id, name, policy, max_spin_cnt, max_yield_cnt)


struct ObLatchIds
{
  enum ObLatchIdEnum
  {
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, enable)\
LATCH_DEF_##enable(def, id, name, policy, max_spin_cnt, max_yield_cnt)
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
  };
};
#undef LATCH_DEF_true
#undef LATCH_DEF_false

struct ObLatchPolicy
{
  enum ObLatchPolicyEnum
  {
    LATCH_READ_PREFER = 0,
    LATCH_FIFO
  };
};

#define LatchWaitEventBegin 100000
struct ObLatchDesc
{
  static const int64_t MAX_LATCH_NAME_LENGTH = 64;
  int64_t latch_id_;
  char latch_name_[MAX_LATCH_NAME_LENGTH];
  int32_t policy_;
  uint64_t max_spin_cnt_;
  uint64_t max_yield_cnt_;
  // every latch has a lock wait event
  static int64_t wait_event_idx(const int64_t latch_idx) { return latch_idx + ObWaitEventIds::WAIT_EVENT_DEF_END; }
  static int64_t wait_event_id(const int64_t latch_id) { return LatchWaitEventBegin + latch_id; /*explicit defined latch id is always less than 100000*/}
};

extern const ObLatchDesc OB_LATCHES[];

static constexpr int32_t WAIT_EVENTS_TOTAL = ObWaitEventIds::WAIT_EVENT_DEF_END + ObLatchIds::LATCH_END;
}//common
}//oceanbase
#endif /* OB_LATCH_DEFINE_H_ */
