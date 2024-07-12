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
/**
 * WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, is_phy, enable)
 * @param def Name of this wait event
 * @param id Identifier of an wait event. ATTENTION: please add id placeholder on master.
 * @param name Name for this wait event. Display on virtual table v$event_name
 * @param param1 Additional showing param 1. Display on gv$active_session_history and gv$session_wait
 * @param prarm2 Additional showing param 2. Display on gv$active_session_history and gv$session_wait
 * @param param3 Additional showing param 3. Display on gv$active_session_history and gv$session_wait
 * @param class Every wait event belongs to a class of wait event on deps/oblib/src/lib/wait_event/ob_wait_class.h
 * @param is_phy Indicate whether this wait event can be nested. true for most cases.
 * @param enable Means whether this wait event is enabled. Marked it false it you merely need it as an placeholder.
 * NOTICE: do not reuse wait event id or rename wait event!
*/
// USER_IO & SYSTEM_IO 10001-11999
WAIT_EVENT_DEF(NULL_EVENT, 10000, "", "", "", "", OTHER, true, true)
WAIT_EVENT_DEF(DB_FILE_DATA_READ, 10001, "db file data read", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_DATA_INDEX_READ, 10002, "db file data `index read", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_READ, 11001, "db file compact read", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_WRITE, 11002, "db file compact write", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_READ, 11003, "db file index build read", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_WRITE, 11004, "db file index build write", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_READ, 11005, "db file migrate read", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_WRITE, 11006, "db file migrate write", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(BLOOM_FILTER_BUILD_READ, 11007, "bloomfilter build read", "fd", "offset", "size", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_WRITE, 11008, "interm result disk write", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(INTERM_RESULT_DISK_READ, 11009, "interm result disk read", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(ROW_STORE_DISK_WRITE, 11010, "row store disk write", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(ROW_STORE_DISK_READ, 11011, "row store disk read", "fd", "offset", "size", USER_IO, true, true)
WAIT_EVENT_DEF(MEMSTORE_MEM_PAGE_ALLOC_WAIT, 11015, "memstore memory page alloc wait", "cur_mem_hold", "sleep_interval", "cur_ts", CONFIGURATION, true, true)
WAIT_EVENT_DEF(PALF_READ, 11016, "palf read", "fd", "offset", "size", SYSTEM_IO, false, true)
WAIT_EVENT_DEF(PALF_WRITE, 11017, "palf write", "fd", "offset", "size", SYSTEM_IO, false, true)
WAIT_EVENT_DEF(OBJECT_STORAGE_WRITE, 11018, "object storage write", "fd", "offset", "size", SYSTEM_IO, true, false)
WAIT_EVENT_DEF(OBJECT_STORAGE_READ, 11019, "object storage read", "fd", "offset", "size", SYSTEM_IO, true, false)

// SCHEDULER 12001-12999
WAIT_EVENT_DEF(OMT_WAIT, 12001, "sched wait", "req type", "req start timestamp", "wait start timestamp", SCHEDULER, true, false)
WAIT_EVENT_DEF(OMT_IDLE, 12002, "sched idle", "wait start timestamp", "", "", IDLE, true, true)

// NETWORK 13000-13999
WAIT_EVENT_DEF(SYNC_RPC, 13000, "sync rpc", "pcode", "size", "", NETWORK, true, true)
WAIT_EVENT_DEF(MYSQL_RESPONSE_WAIT_CLIENT, 13001, "mysql response wait client", "", "", "", NETWORK, true, true)
WAIT_EVENT_DEF(DAS_ASYNC_RPC_LOCK_WAIT, 13002, "das wait remote response", "", "", "", NETWORK, true, true)
WAIT_EVENT_DEF(ASYNC_EXTERNAL_TABLE_LOCK_WAIT, 13003, "external table wait remote response", "", "", "", NETWORK, true, true)
WAIT_EVENT_DEF(NETWORK_QUEUE_WAIT, 13004, "wait for network request in queue", "pcode", "retry_times", "", CONFIGURATION, true, true)

// APPLICATION 14001-14999
WAIT_EVENT_DEF(MT_READ_LOCK_WAIT,14001,"memstore read lock wait","lock","waiter","owner",APPLICATION,false, true)
WAIT_EVENT_DEF(MT_WRITE_LOCK_WAIT,14002,"memstore write lock wait","lock","waiter","owner",APPLICATION,false, false)
WAIT_EVENT_DEF(ROW_LOCK_WAIT,14003,"row lock wait","lock","waiter","owner",APPLICATION,false, false)

// CONCURRENCY
// condition wait has one parameter e.g. address of the condition variable
WAIT_EVENT_DEF(IO_QUEUE_COND_WAIT, 15066, "io queue condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(LATCH_WAIT_QUEUE_LOCK_WAIT, 15084, "latch wait queue lock wait", "address", "number", "tries", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DEFAULT_COND_WAIT, 15101, "default condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DEFAULT_SLEEP, 15102, "sleep wait", "sleep_interval", "", "", IDLE, true, true)
WAIT_EVENT_DEF(CLOG_WRITER_COND_WAIT, 15103, "clog writer condition wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(IO_CONTROLLER_COND_WAIT, 15104, "io controller condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(IO_PROCESSOR_COND_WAIT, 15105, "io processor condition wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(DEDUP_QUEUE_COND_WAIT, 15106, "dedup queue condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SEQ_QUEUE_COND_WAIT, 15107, "seq queue condition wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(INNER_CONNECTION_POOL_COND_WAIT, 15108, "inner connection pool condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_COND_WAIT, 15109, "partition table updater condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(REBALANCE_TASK_MGR_COND_WAIT, 15110, "rebalance task mgr condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(ASYNC_RPC_PROXY_COND_WAIT, 15111, "async rpc proxy condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(THREAD_IDLING_COND_WAIT, 15112, "thread idling condition wait", "address", "", "", IDLE, true, true)
WAIT_EVENT_DEF(RPC_SESSION_HANDLER_COND_WAIT, 15113, "rpc session handler condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(LOCATION_CACHE_COND_WAIT, 15114, "location cache condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(REENTRANT_THREAD_COND_WAIT, 15115, "reentrant thread condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(MAJOR_FREEZE_COND_WAIT, 15116, "major freeze condition wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(MINOR_FREEZE_COND_WAIT, 15117, "minor freeze condition wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TH_WORKER_COND_WAIT, 15118, "th worker condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DEBUG_SYNC_COND_WAIT, 15119, "debug sync condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(EMPTY_SERVER_CHECK_COND_WAIT, 15120, "empty server check condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SCHEDULER_COND_WAIT, 15121, "scheduler condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DYNAMIC_THREAD_POOL_COND_WAIT, 15123, "dynamic thread pool condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SLOG_FLUSH_COND_WAIT, 15125, "slog flush condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(BUILD_INDEX_SCHEDULER_COND_WAIT, 15126, "build index scheduler condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DAG_WORKER_COND_WAIT, 15132, "dag worker condition wait", "address", "", "", CONCURRENCY, false, true)
WAIT_EVENT_DEF(IO_CALLBACK_QUEUE_LOCK_WAIT, 15134, "io callback queue condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(IO_CHANNEL_COND_WAIT, 15135, "io channel condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(BACKUP_TASK_SCHEDULER_COND_WAIT, 15158, "backup scheduler condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SLOG_CKPT_LOCK_WAIT, 15185, "slog checkpoint lock wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(BACKUP_DATA_SERVICE_COND_WAIT, 15248, "backup data service condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(BACKUP_CLEAN_SERVICE_COND_WAIT, 15249, "backup clean service condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(BACKUP_ARCHIVE_SERVICE_COND_WAIT, 15250, "backup archive service condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SQL_WF_PARTICIPATOR_COND_WAIT, 15256, "window function participator cond wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(HA_SERVICE_COND_WAIT, 15159, "ha service condition wait", "address", "", "", CONCURRENCY, false, true)
WAIT_EVENT_DEF(PX_LOOP_COND_WAIT, 15160, "px loop condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SQL_SHARED_HJ_COND_WAIT, 15165, "shared hash join cond wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(TENANT_IO_POOL_WAIT, 15261, "rwlock: tenant io pool wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(DISPLAY_TASKS_LOCK_WAIT, 15262, "latch: display tasks lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(RESOURCE_SERVICE_LOCK_WAIT, 15263, "latch: resource_service lock wait", "address", "number", "tries", CONCURRENCY, true, true)
WAIT_EVENT_DEF(RESOURCE_SERVICE_SWITCH_LOCK_WAIT, 15264, "latch:resource_service switch lock wait", "address", "number", "tries", CONCURRENCY, true, true)
WAIT_EVENT_DEF(COLUMN_STORE_DDL_RESCAN_LOCK_WAIT, 15265, "latch: column store ddl rescan lock wait", "address", "number", "tries", CONCURRENCY, true, true)
WAIT_EVENT_DEF(TABLET_DIRECT_LOAD_MGR_SCHEMA_WAIT, 15266, "latch: tablet direct load mgr schema wait", "address", "number", "tries", CONCURRENCY, true, true)
WAIT_EVENT_DEF(TENANT_SNAPSHOT_SERVICE_COND_WAIT, 15267, "tenant snapshot service condition wait", "address", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(END_TRANS_WAIT, 16001, "wait end trans", "rollback", "trans_hash_value", "participant_count", COMMIT,false, false)
WAIT_EVENT_DEF(START_STMT_WAIT, 16002, "wait start stmt", "trans_hash_value", "physic_plan_type", "participant_count", CLUSTER, false, false)
WAIT_EVENT_DEF(END_STMT_WAIT, 16003, "wait end stmt", "rollback", "trans_hash_value", "physic_plan_type", CLUSTER, false, false)
WAIT_EVENT_DEF(REMOVE_PARTITION_WAIT, 16004, "wait remove partition", "tenant_id", "table_id", "partition_id", ADMINISTRATIVE, false, false)
WAIT_EVENT_DEF(TABLET_LOCK_WAIT, 16016, "tablet lock wait", "", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(IND_NAME_CACHE_LOCK_WAIT, 16017, "latch:index name cache lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(ASYNC_COMMITTING_WAIT, 16018, "async commiting wait", "", "", "", COMMIT, true, true)
WAIT_EVENT_DEF(OBCDC_PART_MGR_SCHEMA_VERSION_WAIT, 18000, "oblog part mgr schema version wait", "", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(SYNC_GET_GTS_WAIT, 18101, "sync get gts timestamp wait", "address", "", "", NETWORK, true, true)

WAIT_EVENT_DEF(BANDWIDTH_THROTTLE_SLEEP, 20000, "sleep: bandwidth throttle sleep wait", "sleep_interval", "", "", NETWORK, true, true)
WAIT_EVENT_DEF(DTL_PROCESS_CHANNEL_SLEEP, 20001, "sleep: dtl process channel sleep wait", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(DTL_DESTROY_CHANNEL_SLEEP, 20002, "sleep: dtl destroy channel sleep wait", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(STORAGE_WRITING_THROTTLE_SLEEP, 20003, "sleep: storage writing throttle sleep", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(STORAGE_AUTOINC_FETCH_RETRY_SLEEP, 20004, "sleep: tablet autoinc fetch new range retry wait", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(STORAGE_AUTOINC_FETCH_CONFLICT_SLEEP, 20005, "sleep: tablet autoinc fetch new range conflict wait", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(STORAGE_HA_FINISH_TRANSFER, 20006, "sleep: finish transfer sleep wait", "sleep_interval", "", "", CONCURRENCY, true, true)
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_IO_TASK_WAIT, 20007, "latch: log external storage io task wait", "", "", "", SYSTEM_IO, true, true)
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_HANDLER_RW_WAIT, 20008, "latch: log external storage handler rw wait", "", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(LOG_EXTERNAL_STORAGE_HANDLER_WAIT, 20009, "latch: log external storage handler spin wait", "", "", "", CONCURRENCY, true, false)

// share storage 21001-21999
WAIT_EVENT_DEF(ZONE_STORAGE_MANAGER_LOCK_WAIT, 21001, "latch: zone storage manager maintaince lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(ZONE_STORAGE_INFO_RW_LOCK_WAIT, 21002, "latch: zone storage infos rw lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(DEVICE_MANIFEST_RW_LOCK_WAIT, 21003, "latch: device_manifest rw lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(MANIFEST_TASK_LOCK_WAIT, 21004, "latch: manifest task lock wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(OB_DEVICE_CREDENTIAL_MGR_WAIT, 21005, "latch: device credential mgr wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(DISK_SPACE_MANAGER_LOCK_WAIT, 21006, "share storage disk space manager lock wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TIERED_SUPER_BLOCK_LOCK_WAIT, 21007, "latch: tiered super block wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TSLOG_PROCESSING_MUTEX_WAIT, 21008, "latch: tslog processing mutex wait", "address", "number", "tries", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TSLOG_CKPT_LOCK_WAIT, 21009, "tslog checkpoint lock wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(FILE_MANAGER_LOCK_WAIT, 21010, "file manager lock wait", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TIERED_BLOCK_WRITE_REMOTE, 21011, "tiered block write remote", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TIERED_BLOCK_WRITE_LOCAL, 21012, "tiered block write local", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TIERED_BLOCK_READ_REMOTE, 21013, "tiered block read remote", "address", "", "", CONCURRENCY, true, false)
WAIT_EVENT_DEF(TIERED_BLOCK_READ_LOCAL, 21014, "tiered block read local", "address", "", "", CONCURRENCY, true, false)

// inner sql 30000-30099
WAIT_EVENT_DEF(INNER_SQL_EXEC_WAIT, 30000, "exec inner sql wait", "wait inner sql class", "inner session id", "", OTHER, true, true)
WAIT_EVENT_DEF(INNER_SESSION_IDLE_WAIT, 30001, "inner session wait to be called", "inner session id", "parent session id", "", IDLE, true, false)

// CONFIGURATION 30100-30999
WAIT_EVENT_DEF(WAIT_REFRESH_SCHEMA, 30100, "sleep: wait refresh schema", "sleep_interval", "schema_version", "", CONFIGURATION, true, true)
WAIT_EVENT_DEF(PALF_THROTTLING, 30101, "palf throttling sleep", "sleep_interval", "", "", USER_IO, false, true)
WAIT_EVENT_DEF(SLOG_NORMAL_RETRY_SLEEP, 30102, "sleep: slog has io error and retrying", "sleep_interval", "", "", USER_IO, true, true)

// sleep 31000-31999
WAIT_EVENT_DEF(GARBAGE_COLLECTOR_SLEEP, 31000, "sleep: wait log callback sleep wait", "sleep_interval", "", "", SYSTEM_IO, true, true)

// END. DO NOT MODIFY.
WAIT_EVENT_DEF(WAIT_EVENT_DEF_END, 99999, "event end", "", "", "", OTHER, false, true)
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

#define WAIT_EVENT_DEF_true(def, id, name, param1, param2, param3, wait_class, is_phy) def,
#define WAIT_EVENT_DEF_false(def, id, name, param1, param2, param3, wait_class, is_phy)

struct ObWaitEventIds
{
  enum ObWaitEventIdEnum
  {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, is_phy, enable)\
WAIT_EVENT_DEF_##enable(def, id, name, param1, param2, param3, wait_class, is_phy)
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
  };
};

#undef WAIT_EVENT_DEF_true
#undef WAIT_EVENT_DEF_false

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

struct ObWaitEventDef
{
  int64_t event_id_;
  char event_name_[MAX_WAIT_EVENT_NAME_LENGTH];
  char param1_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param2_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param3_[MAX_WAIT_EVENT_PARAM_LENGTH];
  int64_t wait_class_;
  bool is_phy_;
};


extern ObWaitEventDef OB_WAIT_EVENTS[];

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
