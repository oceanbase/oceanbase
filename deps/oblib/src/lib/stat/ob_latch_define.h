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
LATCH_DEF(LATCH_WAIT_QUEUE_LOCK, 0, "latch wait queue lock", LATCH_FIFO, 2000, 0, LATCH_WAIT_QUEUE_LOCK_WAIT,
    "latch wait queue lock")
LATCH_DEF(DEFAULT_SPIN_LOCK, 1, "default spin lock", LATCH_FIFO, 2000, 0, DEFAULT_SPIN_LOCK_WAIT, "default spin lock")
LATCH_DEF(
    DEFAULT_SPIN_RWLOCK, 2, "default spin rwlock", LATCH_FIFO, 2000, 0, DEFAULT_SPIN_RWLOCK_WAIT, "default spin rwlock")
LATCH_DEF(DEFAULT_MUTEX, 3, "default mutex", LATCH_FIFO, 2000, 0, DEFAULT_MUTEX_WAIT, "default mutex")
LATCH_DEF(KV_CACHE_BUCKET_LOCK, 4, "kv cache bucket latch", LATCH_FIFO, 2000, 0, KV_CACHE_BUCKET_LOCK_WAIT,
    "kv cache bucket latch")
LATCH_DEF(TIME_WHEEL_TASK_LOCK, 5, "time wheel task latch", LATCH_FIFO, 2000, 0, TIME_WHEEL_TASK_LOCK_WAIT,
    "time wheel task latch")
LATCH_DEF(TIME_WHEEL_BUCKET_LOCK, 6, "time wheel bucket latch", LATCH_FIFO, 2000, 0, TIME_WHEEL_BUCKET_LOCK_WAIT,
    "time wheel bucket latch")
LATCH_DEF(ELECTION_LOCK, 7, "election latch", LATCH_FIFO, 20000000L, 0, ELECTION_LOCK_WAIT, "election latch")
LATCH_DEF(TRANS_CTX_LOCK, 8, "trans ctx latch", LATCH_FIFO, 2000, 0, TRANS_CTX_LOCK_WAIT, "trans ctx latch")
LATCH_DEF(
    PARTITION_LOG_LOCK, 9, "partition log latch", LATCH_FIFO, 2000, 0, PARTITION_LOG_LOCK_WAIT, "partition log latch")
LATCH_DEF(PCV_SET_LOCK, 10, "pcv set latch", LATCH_FIFO, 2000, 0, PCV_SET_LOCK_WAIT, "plan cache pcv set latch")
LATCH_DEF(CLOG_HISTORY_REPORTER_LOCK, 11, "clog history reporter latch", LATCH_FIFO, 2000, 0,
    CLOG_HISTORY_REPORTER_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_EXTERNAL_EXEC_LOCK, 12, "clog external executor latch", LATCH_FIFO, 2000, 0,
    CLOG_EXTERNAL_EXEC_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_MEMBERSHIP_MGR_LOCK, 13, "clog member ship mgr latch", LATCH_FIFO, 2000, 0,
    CLOG_MEMBERSHIP_MGR_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_RECONFIRM_LOCK, 14, "clog reconfirm latch", LATCH_FIFO, 2000, 0, CLOG_RECONFIRM_LOCK_WAIT,
    "plan cache value latch")
LATCH_DEF(CLOG_SLIDING_WINDOW_LOCK, 15, "clog sliding window latch", LATCH_FIFO, 2000, 0, CLOG_SLIDING_WINDOW_LOCK_WAIT,
    "plan cache value latch")
LATCH_DEF(CLOG_STAT_MGR_LOCK, 16, "clog stat mgr latch", LATCH_FIFO, 2000, 0, CLOG_STAT_MGR_LOCK_WAIT,
    "plan cache value latch")
LATCH_DEF(
    CLOG_TASK_LOCK, 17, "clog task latch", LATCH_FIFO, 20000000L, 0, CLOG_TASK_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_IDMGR_LOCK, 18, "clog id mgr latch", LATCH_FIFO, 2000, 0, CLOG_IDMGR_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_CACHE_LOCK, 19, "clog cache latch", LATCH_FIFO, 2000, 0, CLOG_CACHE_LOCK_WAIT, "clog cache latch")
LATCH_DEF(
    ELECTION_MSG_LOCK, 20, "election msg latch", LATCH_FIFO, 2000, 0, ELECTION_MSG_LOCK_WAIT, "election msg latch")
LATCH_DEF(PLAN_SET_LOCK, 21, "plan set latch", LATCH_FIFO, 2000, 0, PLAN_SET_LOCK_WAIT, "plan set latch")
LATCH_DEF(PS_STORE_LOCK, 22, "ps store latch", LATCH_FIFO, 2000, 0, PS_STORE_LOCK_WAIT, "ps store latch")
LATCH_DEF(
    TRANS_CTX_MGR_LOCK, 23, "trans ctx mgr latch", LATCH_FIFO, 2000, 0, TRANS_CTX_MGR_LOCK_WAIT, "trans ctx mgr latch")
LATCH_DEF(ROW_LOCK, 24, "row latch", LATCH_READ_PREFER, 200, 0, ROW_LOCK_WAIT, "row latch")
LATCH_DEF(DEFAULT_RECURSIVE_MUTEX, 25, "default recursive mutex", LATCH_FIFO, 2000L, 0, DEFAULT_RECURSIVE_MUTEX_WAIT,
    "default recursive mutex")
LATCH_DEF(DEFAULT_DRW_LOCK, 26, "default drw lock", LATCH_FIFO, 200000, 0, DEFAULT_DRW_LOCK_WAIT, "default drw lock")
LATCH_DEF(DEFAULT_BUCKET_LOCK, 27, "default bucket lock", LATCH_FIFO, 2000, 0, DEFAULT_BUCKET_LOCK_WAIT,
    "default bucket lock")
LATCH_DEF(TRANS_CTX_BUCKET_LOCK, 28, "trans ctx bucket lock", LATCH_FIFO, 20000000L, 0, TRANS_CTX_BUCKET_LOCK_WAIT,
    "trans ctx bucket lock")
LATCH_DEF(MACRO_META_BUCKET_LOCK, 29, "macro meta bucket lock", LATCH_FIFO, 2000, 0, MACRO_META_BUCKET_LOCK_WAIT,
    "macro meta bucket lock")
LATCH_DEF(
    TOKEN_BUCKET_LOCK, 30, "token bucket lock", LATCH_FIFO, 20000000L, 0, TOKEN_BUCKET_LOCK_WAIT, "token bucket lock")
LATCH_DEF(LIGHTY_HASHMAP_BUCKET_LOCK, 31, "light hashmap bucket lock", LATCH_FIFO, 2000, 0,
    LIGHTY_HASHMAP_BUCKET_LOCK_WAIT, "lighty hashmap bucket lock")
LATCH_DEF(ROW_CALLBACK_LOCK, 32, "row callback lock", LATCH_FIFO, 2000, 0, ROW_CALLBACK_LOCK_WAIT, "row callback lock")
LATCH_DEF(PARTITION_LOCK, 33, "partition latch", LATCH_FIFO, 2000, 0, PARTITION_LOCK_WAIT, "partition latch")
LATCH_DEF(SWITCH_LEADER_LOCK, 35, "switch leader lock", LATCH_FIFO, 2000, 0, SWITCH_LEADER_WAIT, "switch leader lock")
LATCH_DEF(PARTITION_FREEZE_LOCK, 36, "partition freeze lock", LATCH_FIFO, 2000, 0, PARTITION_FREEZE_WAIT,
    "partition freeze lock")
LATCH_DEF(SCHEMA_SERVICE_LOCK, 37, "schema service lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_SERVICE_LOCK_WAIT,
    "schema service lock")
LATCH_DEF(SCHEMA_SERVICE_STATS_LOCK, 38, "schema service stats lock", LATCH_READ_PREFER, 2000, 0,
    SCHEMA_SERVICE_STATS_LOCK_WAIT, "schema service stats lock")
LATCH_DEF(TENANT_LOCK, 39, "tenant lock", LATCH_READ_PREFER, 2000, 0, TENANT_LOCK_WAIT, "tenant lock")
LATCH_DEF(CONFIG_LOCK, 40, "config lock", LATCH_READ_PREFER, 2000, 0, CONFIG_LOCK_WAIT, "config lock")
LATCH_DEF(
    MAJOR_FREEZE_LOCK, 41, "major freeze lock", LATCH_READ_PREFER, 2000, 0, MAJOR_FREEZE_LOCK_WAIT, "major freeze lock")
LATCH_DEF(PARTITION_TABLE_UPDATER_LOCK, 42, "partition table updater lock", LATCH_READ_PREFER, 2000, 0,
    PARTITION_TABLE_UPDATER_LOCK_WAIT, "partition table updater lock")
LATCH_DEF(
    MULTI_TENANT_LOCK, 43, "multi tenant lock", LATCH_READ_PREFER, 2000, 0, MULTI_TENANT_LOCK_WAIT, "multi tenant lock")
LATCH_DEF(LEADER_COORDINATOR_LOCK, 44, "leader coordinator lock", LATCH_READ_PREFER, 2000, 0,
    LEADER_COORDINATOR_LOCK_WAIT, "leader coordinator lock")
LATCH_DEF(
    LEADER_STAT_LOCK, 45, "leader stat lock", LATCH_READ_PREFER, 2000, 0, LEADER_STAT_LOCK_WAIT, "leader stat lock")
LATCH_DEF(ROOT_MAJOR_FREEZE_LOCK, 46, "root major freeze lock", LATCH_READ_PREFER, 2000, 0, ROOT_MAJOR_FREEZE_LOCK_WAIT,
    "root major freeze lock")
LATCH_DEF(
    RS_BOOTSTRAP_LOCK, 47, "rs bootstrap lock", LATCH_READ_PREFER, 2000, 0, RS_BOOTSTRAP_LOCK_WAIT, "rs bootstrap lock")
LATCH_DEF(SCHEMA_MGR_ITEM_LOCK, 48, "schema mgr item lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_MGR_ITEM_LOCK_WAIT,
    "schema mgr item lock")
LATCH_DEF(SCHEMA_MGR_LOCK, 49, "schema mgr lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_MGR_LOCK_WAIT, "schema mgr lock")
LATCH_DEF(SUPER_BLOCK_LOCK, 50, "super block lock", LATCH_FIFO, 2000, 0, SUPER_BLOCK_LOCK_WAIT, "super block lock")
LATCH_DEF(FROZEN_VERSION_LOCK, 51, "frozen version lock", LATCH_READ_PREFER, 2000, 0, FROZEN_VERSION_LOCK_WAIT,
    "frozen version lock")
LATCH_DEF(
    RS_BROADCAST_LOCK, 52, "rs broadcast lock", LATCH_READ_PREFER, 2000, 0, RS_BROADCAST_LOCK_WAIT, "rs broadcast lock")
LATCH_DEF(SERVER_STATUS_LOCK, 53, "server status lock", LATCH_READ_PREFER, 2000, 0, SERVER_STATUS_LOCK_WAIT,
    "server status lock")
LATCH_DEF(SERVER_MAINTAINCE_LOCK, 54, "server maintaince lock", LATCH_READ_PREFER, 2000, 0, SERVER_MAINTAINCE_LOCK_WAIT,
    "server maintaince lock")
LATCH_DEF(
    UNIT_MANAGER_LOCK, 55, "unit manager lock", LATCH_READ_PREFER, 2000, 0, UNIT_MANAGER_LOCK_WAIT, "unit manager lock")
LATCH_DEF(ZONE_MANAGER_LOCK, 56, "zone manager maintaince lock", LATCH_READ_PREFER, 2000, 0, ZONE_MANAGER_LOCK_WAIT,
    "zone manager maintaince lock")
LATCH_DEF(
    ALLOC_OBJECT_LOCK, 57, "object set lock", LATCH_READ_PREFER, 2000, 0, ALLOC_OBJECT_LOCK_WAIT, "alloc object lock")
LATCH_DEF(ALLOC_BLOCK_LOCK, 58, "block set lock", LATCH_READ_PREFER, 2000, 0, ALLOC_BLOCK_LOCK_WAIT, "alloc block lock")
LATCH_DEF(TRACE_RECORDER_LOCK, 59, "normal trace recorder lock", LATCH_FIFO, 2000, 0, TRACE_RECORDER_LOCK_WAIT,
    "normal trace recorder latch")
LATCH_DEF(SESSION_TRACE_RECORDER_LOCK, 60, "session trace recorder lock", LATCH_FIFO, 2000, 0,
    SESSION_TRACE_RECORDER_LOCK_WAIT, "session trace recorder latch")
LATCH_DEF(TRANS_TRACE_RECORDER_LOCK, 61, "trans trace recorder lock", LATCH_FIFO, 2000, 0,
    TRANS_TRACE_RECORDER_LOCK_WAIT, "trans trace recorder latch")
LATCH_DEF(ELECT_TRACE_RECORDER_LOCK, 62, "election trace recorder lock", LATCH_FIFO, 2000, 0,
    ELECT_TRACE_RECORDER_LOCK_WAIT, "election trace recorder latch")
LATCH_DEF(ALIVE_SERVER_TRACER_LOCK, 63, "alive server tracer lock", LATCH_READ_PREFER, 2000, 0,
    ALIVE_SERVER_TRACER_LOCK_WAIT, "alive server tracer lock")
LATCH_DEF(
    ALLOC_CHUNK_LOCK, 64, "allocate chunk lock", LATCH_FIFO, 2000, 0, ALLOC_CHUNK_LOCK_WAIT, "allocate chunk lock")
LATCH_DEF(
    ALLOC_TENANT_LOCK, 65, "allocate tenant lock", LATCH_FIFO, 2000, 0, ALLOC_TENANT_LOCK_WAIT, "allocate tenant lock")
LATCH_DEF(IO_QUEUE_LOCK, 66, "io queue lock", LATCH_FIFO, 2000, 0, IO_QUEUE_LOCK_WAIT, "io queue lock")
LATCH_DEF(ZONE_INFO_RW_LOCK, 67, "zone infos rw lock", LATCH_READ_PREFER, 2000, 0, ZONE_INFO_RW_LOCK_WAIT,
    "zone infos rw lock")
LATCH_DEF(MT_TRACE_RECORDER_LOCK, 68, "memtable trace recorder lock", LATCH_READ_PREFER, 2000, 0,
    MT_TRACE_RECORDER_LOCK_WAIT, "memtable trace recorder latch")
LATCH_DEF(BANDWIDTH_THROTTLE_LOCK, 69, "bandwidth throttle lock", LATCH_FIFO, 2000, 0, BANDWIDTH_THROTTLE_LOCK_WAIT,
    "bandwidth throttle latch")
LATCH_DEF(RS_EVENT_TS_LOCK, 70, "rs event table timestamp lock", LATCH_FIFO, 2000, 0, RS_EVENT_TS_LOCK_WAIT,
    "rs event table timestamp lock")
LATCH_DEF(
    CLOG_FD_CACHE_LOCK, 71, "clog fd cache lock", LATCH_FIFO, 2000, 0, CLOG_FD_CACHE_LOCK_WAIT, "clog fd cache lock")
LATCH_DEF(MIGRATE_LOCK, 72, "migrate lock", LATCH_FIFO, 2000, 0, MIGRATE_LOCK_WAIT, "migrate lock")
LATCH_DEF(CLOG_CASCADING_INFO_LOCK, 73, "clog cascading info lock", LATCH_FIFO, 2000, 0, CLOG_CASCADING_INFO_LOCK_WAIT,
    "clog cascading info lock")
LATCH_DEF(
    CLOG_LOCALITY_LOCK, 74, "clog locality lock", LATCH_FIFO, 2000, 0, CLOG_LOCALITY_LOCK_WAIT, "clog locality lock")
LATCH_DEF(DTL_CHANNEL_WAIT, 75, "DTL channel wait", LATCH_FIFO, 2000, 0, DEFAULT_COND_WAIT, "DTL channel wait")
LATCH_DEF(
    GROUP_MIGRATE_LOCK, 76, "group migrate lock", LATCH_FIFO, 2000, 0, GROUP_MIGRATE_LOCK_WAIT, "group migrate lock")
LATCH_DEF(GROUP_MIGRATE_TASK_LOCK, 77, "group migrate task lock", LATCH_FIFO, 2000, 0, GROUP_MIGRATE_LOCK_WAIT,
    "group migrate task lock")
LATCH_DEF(LOG_ENGINE_ENV_SWITCH_LOCK, 78, "log engine env switch lock", LATCH_FIFO, 2000, 0,
    LOG_ENGINE_ENV_SWITCH_LOCK_WAIT, "log engine env switch lock")
LATCH_DEF(CLOG_CONSEC_INFO_MGR_LOCK, 79, "clog consec log info mgr latch", LATCH_FIFO, 2000, 0,
    CLOG_CONSEC_INFO_MGR_LOCK_WAIT, "clog consec log info mgr latch")
LATCH_DEF(RG_TRANSFER_LOCK, 80, "rg transfer lock", LATCH_FIFO, 2000, 0, RG_TRANSFER_LOCK_WAIT, "rg transfer lock")
LATCH_DEF(TABLE_MGR_LOCK, 81, "table mgr lock", LATCH_FIFO, 2000, 0, TBALE_MGR_LOCK_WAIT, "table mgr lock")
LATCH_DEF(PARTITION_STORE_LOCK, 82, "partition store lock", LATCH_FIFO, 2000, 0, PARTITION_STORE_LOCK_WAIT,
    "partition store lock")
LATCH_DEF(PARTITION_STORE_CHANGE_LOCK, 83, "partition store change lock", LATCH_FIFO, 2000, 0,
    PARTITION_STORE_CHANGE_LOCK_WAIT, "partition store change lock")
LATCH_DEF(PARTITION_STORE_NEW_MEMTABLE_LOCK, 84, "partition store new memtable lock", LATCH_FIFO, 2000, 0,
    PARTITION_STORE_NEW_MEMTABLE_LOCK_WAIT, "partition store new memtable lock")
LATCH_DEF(ELECTION_GROUP_LOCK, 85, "election group latch", LATCH_FIFO, 20000000L, 0, ELECTION_GROUP_LOCK_WAIT,
    "election group latch")
LATCH_DEF(ELECTION_GROUP_TRACE_RECORDER_LOCK, 86, "election group trace recorder lock", LATCH_FIFO, 2000, 0,
    ELECTION_GROUP_TRACE_RECORDER_LOCK_WAIT, "election group trace recorder latch")
LATCH_DEF(CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK, 87, "ObCacheLineSegregatedArrayBase alloc lock", LATCH_FIFO, 2000, 0,
    CACHE_LINE_SEGREGATED_ARRAY_BASE_LOCK_WAIT, "ObCacheLineSegregatedArrayBase alloc lock")
LATCH_DEF(SERVER_OBJECT_POOL_ARENA_LOCK, 88, "server object pool arena lock", LATCH_FIFO, 20000000L, 0,
    SERVER_OBJECT_POOL_ARENA_LOCK_WAIT, "server object pool arena lock")
LATCH_DEF(TABLE_MGR_MAP, 89, "table mgr map", LATCH_FIFO, 2000, 0, TBALE_MGR_MAP_WAIT, "table mgr map")
LATCH_DEF(ID_MAP_NODE_LOCK, 90, "id map node lock", LATCH_FIFO, 2000, 0, ID_MAP_NODE_LOCK_WAIT, "id map nodelock")
LATCH_DEF(EXTENT_STORAGE_MANAGER_LOCK, 91, "extent storage manager lock", LATCH_FIFO, 2000, 0,
    EXTENT_STORAGE_MANAGER_LOCK_WAIT, "extent storage manager lock")
LATCH_DEF(REBUILD_RETRY_LIST_LOCK, 93, "rebuild retry list lock", LATCH_FIFO, 2000, 0, REBUILD_RETRY_LIST_LOCK_WAIT,
    "rebuild retry list lock")
LATCH_DEF(PARTITION_AUDIT_SPIN_LOCK, 94, "partition audit spin lock", LATCH_FIFO, 20000000L, 0,
    PARTITION_AUDIT_SPIN_LOCK_WAIT, "partition audit spin lock")
LATCH_DEF(CLOG_SWITCH_INFO_LOCK, 95, "clog switch_info latch", LATCH_FIFO, 20000000L, 0, CLOG_SWITCH_INFO_LOCK_WAIT,
    "clog switch_info latch")
LATCH_DEF(PARTITION_GROUP_LOCK, 96, "partition group lock", LATCH_FIFO, 2000, 0, PARTITION_GROUP_LOCK_WAIT,
    "partition group lock")
LATCH_DEF(PX_WORKER_LEADER_LOCK, 97, "px worker leader lock", LATCH_FIFO, 2000, 0, PX_WORKER_LEADER_LOCK_WAIT,
    "px worker leader lock")
LATCH_DEF(CLOG_IDC_LOCK, 98, "clog idc lock", LATCH_FIFO, 2000, 0, CLOG_IDC_LOCK_WAIT, "clog idc lock")
LATCH_DEF(PG_STORAGE_BUCKET_LOCK, 99, "pg_storage bucket lock", LATCH_FIFO, 2000, 0, PG_STORAGE_BUCKET_LOCK_WAIT,
    "pg_storage bucket lock")
LATCH_DEF(OB_ALLOCATOR_LOCK, 100, "ob allocator lock", LATCH_READ_PREFER, 2000, 0, OB_ALLOCATOR_LOCK_WAIT,
    "ob allocator lock")
LATCH_DEF(MACRO_META_MAJOR_KEY_MAP_LOCK, 101, "macro block meta major key map lock", LATCH_FIFO, 2000, 0,
    MACRO_META_MAJOR_KEY_MAP_LOCK_WAIT, "macro block meta major key map lock")
LATCH_DEF(OB_CONTEXT_LOCK, 102, "ob context lock", LATCH_FIFO, 2000, 0, OB_CONTEXT_LOCK_WAIT, "ob context lock")
LATCH_DEF(OB_PG_INDEX_LOCK, 103, "ob pg index lock", LATCH_FIFO, 2000, 0, OB_CONTEXT_LOCK_WAIT, "ob pg index lock")
LATCH_DEF(OB_LOG_ARCHIVE_SCHEDULER_LOCK, 104, "ob log archive scheduler lock", LATCH_FIFO, 2000, 0,
    OB_LOG_ARCHIVE_SCHEDULER_LOCK_WAIT, "ob log archive scheduler lock")
LATCH_DEF(
    MEM_DUMP_ITER_LOCK, 105, "mem dump iter lock", LATCH_FIFO, 2000, 0, OB_CONTEXT_LOCK_WAIT, "mem dump iter lock")
LATCH_DEF(OB_REQ_TIMEINFO_LIST_LOCK, 106, "ob request timeinfo list lock", LATCH_FIFO, 2000, 0,
    OB_REQ_TIMEINFO_LIST_WAIT, "ob request timeinfo list lock")
LATCH_DEF(TRANS_BATCH_RPC_LOCK, 107, "trans batch rpc lock", LATCH_FIFO, 2000, 0, TRANS_BATCH_RPC_LOCK_WAIT,
    "trans batch rpc latch")
LATCH_DEF(BACKUP_INFO_MGR_LOCK, 108, "backup info mgr lock", LATCH_FIFO, 2000, 0, BACKUP_INFO_MGR_LOCK_WAIT,
    "backup info mgr lock")
LATCH_DEF(CLOG_RENEW_MS_TASK_LOCK, 109, "clog sw renew ms task spin lock", LATCH_FIFO, 20000000L, 0,
    CLOG_RENEW_MS_TASK_LOCK_WAIT, "clog sw renew ms task spin lock")
LATCH_DEF(HASH_MAP_LOCK, 110, "hashmap latch lock", LATCH_FIFO, 2000L, 0, HASH_MAP_LOCK_WAIT, "hashmap latch lock")
LATCH_DEF(FILE_REF_LOCK, 111, "file ref latch lock", LATCH_FIFO, 2000L, 0, FILE_REF_LOCK_WAIT, "file ref latch lock")
LATCH_DEF(TIMEZONE_LOCK, 112, "timezone lock", LATCH_READ_PREFER, 2000, 0, TIMEZONE_LOCK_WAIT, "timezone lock")
LATCH_DEF(UNDO_STATUS_LOCK, 113, "undo status lock", LATCH_FIFO, 2000, 0, UNDO_STATUS_LOCK_WAIT, "undo status lock")
LATCH_DEF(FREEZE_ASYNC_WORKER_LOCK, 114, "freeze async worker lock", LATCH_FIFO, 2000, 0, FREEZE_ASYNC_WORKER_LOCK_WAIT,
    "freeze async worker lock")
LATCH_DEF(ARCHIVE_RESTORE_QUEUE_LOCK, 115, "archive restore queue lock", LATCH_FIFO, 2000, 0,
    ARCHIVE_RESTORER_LOCK_WAIT, "archive restore queue lock")
LATCH_DEF(XA_STMT_LOCK, 116, "xa stmt lock", LATCH_FIFO, 2000, 0, XA_STMT_LOCK_WAIT, "xa stmt lock")
LATCH_DEF(XA_QUEUE_LOCK, 117, "xa queue lock", LATCH_FIFO, 2000, 0, XA_QUEUE_LOCK_WAIT, "xa queue lock")

LATCH_DEF(LATCH_END, 99999, "latch end", LATCH_FIFO, 2000, 0, WAIT_EVENT_END, "latch end")
#endif

#ifndef OB_LATCH_DEFINE_H_
#define OB_LATCH_DEFINE_H_
#include "lib/wait_event/ob_wait_event.h"

namespace oceanbase {
namespace common {

struct ObLatchIds {
  enum ObLatchIdEnum {
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, wait_event, display_name) def,
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
  };
};

struct ObLatchPolicy {
  enum ObLatchPolicyEnum { LATCH_READ_PREFER = 0, LATCH_FIFO };
};

struct ObLatchDesc {
  static const int64_t MAX_LATCH_NAME_LENGTH = 64;
  int64_t latch_id_;
  char latch_name_[MAX_LATCH_NAME_LENGTH];
  int32_t policy_;
  uint64_t max_spin_cnt_;
  uint64_t max_yield_cnt_;
  int64_t wait_event_idx_;
  char display_name_[MAX_LATCH_NAME_LENGTH];
};

extern const ObLatchDesc OB_LATCHES[];

}  // namespace common
}  // namespace oceanbase
#endif /* OB_LATCH_DEFINE_H_ */
