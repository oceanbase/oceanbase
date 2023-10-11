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

#ifdef NDEBUG
#define __ENABLE_TRACEPOINT__ 0
#else
#define __ENABLE_TRACEPOINT__ 1
#endif

#ifndef OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#define OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/list/ob_dlist.h"
#include "lib/coro/co_var.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "common/ob_clock_generator.h"
#include "lib/utility/ob_macro_utils.h"

#define TP_COMMA(x) ,
#define TP_EMPTY(x)
#define TP_PAIR_ARGS() 0, 0
#define TP_THIRD_rescan(x1, x2, x3, ...) x3
#define TP_THIRD(x1, x2, x3) TP_THIRD_rescan(x1, x2, x3, 0)
#define TP_COND(test, true_expr, false_expr) TP_THIRD(TP_PAIR_ARGS test, false_expr, true_expr)

#define TP_MAPCALL(f, cur) TP_COND(cur, TP_COMMA, TP_EMPTY)(cur) TP_COND(cur, f, TP_EMPTY)(cur)
#define TP_MAP8(f, a1, a2, a3, a4, a5, a6, a7, a8, ...)                              \
  TP_COND(a1, f, TP_EMPTY)(a1) TP_MAPCALL(f, a2) TP_MAPCALL(f, a3) TP_MAPCALL(f, a4) \
  TP_MAPCALL(f, a5) TP_MAPCALL(f, a6) TP_MAPCALL(f, a7) TP_MAPCALL(f, a8)
#define TP_MAP(f, ...) TP_MAP8(f, ##__VA_ARGS__, (), (), (), (), (), (), (), ())

#define TP_COMPILER_BARRIER() asm volatile("" ::: "memory")
#define TP_AL(ptr) ({ TP_COMPILER_BARRIER(); (OB_ISNULL(ptr)) ? 0 : *ptr; })
#define TP_AS(x, v) (      \
  {                        \
    TP_COMPILER_BARRIER(); \
    if (OB_ISNULL(x)) {    \
    } else {               \
      *(x) = v;            \
    }                      \
    __sync_synchronize();  \
  })

#define TP_BCAS(x, ov, nv) __sync_bool_compare_and_swap((x), (ov), (nv))
#define TP_RELAX() PAUSE()
#define TP_CALL_FUNC(ptr, ...)({                                             \
      int (*func)(TP_MAP(typeof, ##__VA_ARGS__)) = (typeof(func))TP_AL(ptr); \
      (NULL != func) ? func(__VA_ARGS__): 0; })

#define TRACEPOINT_CALL(name, ...) ({                                      \
      static void** func_ptr = ::oceanbase::common::tracepoint_get(name);  \
      (NULL != func_ptr) ? TP_CALL_FUNC(func_ptr, ##__VA_ARGS__): 0; })

#if __ENABLE_TRACEPOINT__
#define OB_I(key, ...)  \
  TRACEPOINT_CALL(::oceanbase::common::refine_tp_key(__FILE__, __FUNCTION__, #key), ##__VA_ARGS__)?:
#else
#define OB_I(...)
#endif


bool &get_tp_switch();

#define TP_SWITCH_GUARD(v) ::oceanbase::lib::ObSwitchGuard<get_tp_switch> osg_##__COUNTER__##_(v)


namespace oceanbase {
namespace lib {
using GetSwitchFunc = bool& ();

template<GetSwitchFunc fn>
class ObSwitchGuard
{
public:
  ObSwitchGuard(bool newval)
  {
    oldval_ = fn();
    fn() = newval;
  }
  ~ObSwitchGuard()
  {
    fn() = oldval_;
  }
private:
  bool oldval_;
};
}
}


#define EVENT_CALL(event_no, ...) ({ \
      EventItem &item = ::oceanbase::common::EventTable::instance().get_event(event_no); \
      item.call(SELECT(1, ##__VA_ARGS__)); })

#define ERRSIM_POINT_DEF(name) void name##name(){}; static oceanbase::common::NamedEventItem name( \
    #name, oceanbase::common::EventTable::global_item_list());
#define ERRSIM_POINT_CALL(name) name?:

// doc:

// to check if a certain tracepoint is set
// example: if (E(50) OB_SUCCESS) {...}
// you can also specify condition:
// if (E(50, session_id) OB_SUCCESS) { ... }
// which means:
//   check whether event 50 of session_id was raised
#define OB_E(event_no, ...)  \
  EVENT_CALL(event_no, ##__VA_ARGS__)?:

// to set a particular tracepoint
// example: TP_SET_EVENT(50, 4016, 1, 1)
// specify condition: TP_SET_EVENT(50, 4016, 1, 1, 3302201)
// which means:
//   when session id is 3302201, trigger event 50 with error -4016
#define TP_SET_EVENT(id, error_in, occur, trigger_freq, ...)            \
  {                                                                     \
    EventItem item;                                                     \
    item.error_code_ = error_in;                                        \
    item.occur_ = occur;                                                \
    item.trigger_freq_ = trigger_freq;                                  \
    item.cond_ = SELECT(1, ##__VA_ARGS__, 0);                           \
    ::oceanbase::common::EventTable::instance().set_event(id, item);    \
  }

#define TP_SET(file_name, func_name, key, trace_func)                           \
  *::oceanbase::common::tracepoint_get(refine_tp_key(file_name, func_name, key)) = (void*)(trace_func)
#define TP_SET_ERROR(file_name, func_name, key, err)                            \
  TP_SET(file_name, func_name, key, (int (*)())&tp_const_error<(err)>)

namespace oceanbase
{
namespace common
{
inline const char* tp_basename(const char* path)
{
  const char* ret = OB_ISNULL(path) ? NULL : strrchr(path, '/');
  return (NULL == ret) ? path: ++ret;
}

inline const char* refine_tp_key(const char* s1, const char* s2, const char* s3)
{
  constexpr int64_t BUFFER_SIZE = 256;
  typedef struct {
    char buffer_[BUFFER_SIZE];
  } BUFFER;
  RLOCAL(BUFFER, co_buffer);
  char *buffer = (&co_buffer)->buffer_;
  const char *cret = nullptr;
  if (OB_ISNULL(s1) || OB_ISNULL(s2) || OB_ISNULL(s3)) {
  } else {
    s1 = tp_basename(s1);
    snprintf(buffer, BUFFER_SIZE, "%s:%s:%s", s1, s2, s3);
    cret = buffer;
  }
  return cret;
}

template<const int err>
int tp_const_error()
{
  return err;
}

class TPSymbolTable
{
public:
  TPSymbolTable() {}
  ~TPSymbolTable() {}
  void** get(const char* name)
  {
    return (NULL != name) ? do_get(name): NULL;
  }

private:
  static uint64_t BKDRHash(const char *str);
  enum { SYMBOL_SIZE_LIMIT = 128, SYMBOL_COUNT_LIMIT = 64 * 1024 };

  struct SymbolEntry
  {
    enum { FREE = 0, SETTING = 1, OK = 2 };
    SymbolEntry(): lock_(FREE), value_(NULL)
    { name_[0] = '\0'; }
    ~SymbolEntry() {}

    bool find(const char* name);

    int lock_;
    void* value_;
    char name_[SYMBOL_SIZE_LIMIT];
  };

  void** do_get(const char* name);

  SymbolEntry symbol_table_[SYMBOL_COUNT_LIMIT];
};

inline void** tracepoint_get(const char* name)
{
  static TPSymbolTable symbol_table;
  return symbol_table.get(name);
}

struct EventItem
{
  int64_t occur_;            // number of occurrences
  int64_t trigger_freq_;         // trigger frequency
  int64_t error_code_;        // error code to return
  int64_t cond_;

  EventItem()
    : occur_(0),
      trigger_freq_(0),
      error_code_(0),
      cond_(0) {}

  int call(const int64_t v) { return cond_ == v ? call() : 0; }
  int call()
  {
    int ret = 0;
    int64_t trigger_freq = trigger_freq_;
    if (occur_ > 0) {
      do {
        int64_t occur = occur_;
        if (occur > 0) {
          if (ATOMIC_VCAS(&occur_, occur, occur - 1)) {
            ret = static_cast<int>(error_code_);
            break;
          }
        } else {
          ret = 0;
          break;
        }
      } while (true);
    } else if (OB_LIKELY(trigger_freq == 0)) {
      ret = 0;
    } else if (get_tp_switch()) { // true means skip errsim
      ret = 0;
    } else if (trigger_freq == 1) {
      ret = static_cast<int>(error_code_);
#ifdef NDEBUG
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000))
#endif
      {
        COMMON_LOG(WARN, "[ERRSIM] sim error", K(ret));
      }
    } else {
      if (rand() % trigger_freq == 0) {
        ret = static_cast<int>(error_code_);
        COMMON_LOG(WARN, "[ERRSIM] sim error", K(ret), K_(error_code), K(trigger_freq), KCSTRING(lbt()));
      } else {
        ret = 0;
      }
    }
    return ret;
  }

};

struct NamedEventItem : public ObDLinkBase<NamedEventItem>
{
  NamedEventItem(const char *name, ObDList<NamedEventItem> &l) : name_(name)
  {
    l.add_last(this);
  }
  operator int(void) { return item_.call(); }

  const char *name_;
  EventItem item_;
};

class EventTable
{
  static const int SIZE_OF_EVENT_TABLE = 100000; // max number of tracepoints supported
  public:
    EventTable() {
      for (int64_t i = 0; i < SIZE_OF_EVENT_TABLE; ++i) {
        memset(&(event_table_[i]), 0, sizeof(EventItem));
      }
    }
    virtual ~EventTable() {}

    // All tracepoints should be defined here before they can be used
    enum {
      EVENT_TABLE_INVALID = 0,
      EN_1,
      EN_2,
      EN_3,
      EN_4,
      EN_5,
      EN_6,
      EN_7,
      EN_8,
      EN_9,
      EN_IS_LOG_SYNC,                // 10
      EN_POST_ADD_REPILICA_MC,       // 11
      EN_MIGRATE_FETCH_MACRO_BLOCK,  // 12
      EN_WRITE_BLOCK,                // 13
      EN_COMMIT_SLOG,                // 14
      EN_SCHEDULE_INDEX_DAG,         // 15
      EN_INDEX_LOCAL_SORT_TASK,      // 16
      EN_INDEX_MERGE_TASK,           // 17
      EN_INDEX_WRITE_BLOCK,          // 18
      EN_INDEX_COMMIT_SLOG,          // 19
      EN_CHECK_CAN_DO_MERGE,         // 20
      EN_SCHEDULE_MERGE,             //21
      EN_MERGE_MACROBLOCK,           //22
      EN_MERGE_CHECKSUM,             //23
      EN_MERGE_FINISH,               // 24
      EN_IO_SETUP,                   //25
      EN_FORCE_WRITE_SSTABLE_SECOND_INDEX = 26,
      EN_SCHEDULE_MIGRATE = 27,
      EN_TRANS_AFTER_COMMIT = 28,
      EN_CHANGE_SCHEMA_VERSION_TO_ZERO = 29,
      EN_POST_REMOVE_REPLICA_MC_MSG = 30,
      EN_POST_ADD_REPLICA_MC_MSG = 31,
      EN_CHECK_SUB_MIGRATION_TASK = 32,
      EN_POST_GET_MEMBER_LIST_MSG = 33,
      EN_WRITE_CHECKPOIRNT = 34,
      EN_MERGE_SORT_READ_MSG = 35,
      EN_IO_SUBMIT = 36,
      EN_IO_GETEVENTS = 37,
      EN_TRANS_LEADER_ACTIVE = 38,
      EN_UNIT_MANAGER = 39,
      EN_IO_CANCEL = 40,
      EN_REPLAY_ROW = 41,
      EN_BIG_ROW_REPLAY_FOR_MINORING = 42,
      EN_START_STMT_INTERFACE_ERROR = 43,
      EN_START_PARTICIPANT_INTERFACE_ERROR = 44,
      EN_END_PARTICIPANT_INTERFACE_ERROR = 45,
      EN_END_STMT_INTERFACE_ERROR = 46,
      EN_GET_GTS_LEADER = 47,
      ALLOC_LOG_ID_AND_TIMESTAMP_ERROR = 48,
      AFTER_MIGRATE_FINISH_TASK = 49,
      EN_VALID_MIGRATE_SRC = 52,
      EN_BALANCE_TASK_EXE_ERR = 53,
      EN_ADD_REBUILD_PARENT_SRC = 54,
      EN_BAD_BLOCK_ERROR = 55,
      EN_ADD_RESTORE_TASK_ERROR = 56,
      EN_CTAS_FAIL_NO_DROP_ERROR = 57,
      EN_IO_CHANNEL_QUEUE_ERROR = 58,
      EN_GET_SCHE_CTX_ERROR = 59,
      EN_CLOG_RESTORE_REPLAYED_LOG = 60,
      EN_GEN_REBUILD_TASK = 61,
      EN_IO_HANG_ERROR = 62,
      EN_CREATE_TENANT_TRANS_ONE_FAILED = 63,
      EN_CREATE_TENANT_TRANS_TWO_FAILED = 64,
      EN_DELAY_REPLAY_SOURCE_SPLIT_LOG = 65,
      EN_BLOCK_SPLIT_PROGRESS_RESPONSE = 66,
      EN_RPC_ENCODE_SEGMENT_DATA_ERR = 67,
      EN_RPC_ENCODE_RAW_DATA_ERR = 68,
      EN_RPC_DECODE_COMPRESS_DATA_ERR = 69,
      EN_RPC_DECODE_RAW_DATA_ERR = 70,
      EN_BLOCK_SHUTDOWN_PARTITION = 71,
      EN_BLOCK_SPLIT_SOURCE_PARTITION = 72,
      EN_BLOCK_SUBMIT_SPLIT_SOURCE_LOG = 73,
      EN_BLOCK_SPLIT_DEST_PARTITION = 74,
      EN_CREATE_TENANT_TRANS_THREE_FAILED = 75,
      EN_ALTER_CLUSTER_FAILED = 76,
      EN_STANDBY_REPLAY_SCHEMA_FAIL = 77,
      EN_STANDBY_REPLAY_CREATE_TABLE_FAIL = 78,
      EN_STANDBY_REPLAY_CREATE_TENANT_FAIL = 79,
      EN_STANDBY_REPLAY_CREATE_USER_FAIL = 80,
      EN_CREATE_TENANT_BEFORE_PERSIST_MEMBER_LIST = 81,
      EN_CREATE_TENANT_END_PERSIST_MEMBER_LIST = 82,
      EN_BROADCAST_CLUSTER_STATUS_FAIL = 83,
      EN_SET_FREEZE_INFO_FAILED = 84,
      EN_UPDATE_MAJOR_SCHEMA_FAIL = 85,
      EN_RENEW_SNAPSHOT_FAIL = 86,
      EN_FOLLOWER_UPDATE_FREEZE_INFO_FAIL = 87,
      EN_PARTITION_ITERATOR_FAIL = 88,
      EN_REFRESH_INCREMENT_SCHEMA_PHASE_THREE_FAILED = 89,
      EN_MIGRATE_LOGIC_TASK = 90,
      EN_REPLAY_ADD_PARTITION_TO_PG_CLOG = 91,
      EN_REPLAY_ADD_PARTITION_TO_PG_CLOG_AFTER_CREATE_SSTABLE = 92,
      EN_BEFORE_RENEW_SNAPSHOT_FAIL = 93,
      EN_BUILD_INDEX_RELEASE_SNAPSHOT_FAILED = 94,
      EN_CREATE_PG_PARTITION_FAIL = 95,
      EN_PUSH_TASK_FAILED = 96,
      EN_PUSH_REFERENCE_TABLE_FAIL = 97,
      EN_SLOG_WAIT_FLUSH_LOG = 98,
      EN_SET_MEMBER_LIST_FAIL = 99,
      EN_CREATE_TABLE_TRANS_END_FAIL = 100,
      EN_ABROT_INDEX_FAIL = 101,
      EN_DELAY_REPLAY_SOURCE_SPLIT_LOG_R_REPLICA = 102,
      EN_SKIP_GLOBAL_SSTABLE_SCHEMA_VERSION = 103,
      EN_STOP_ROOT_INSPECTION = 104,
      EN_DROP_TENANT_FAILED = 105,
      EN_SKIP_DROP_MEMTABLE = 106,
      EN_SKIP_DROP_PG_PARTITION = 107,
      EN_OBSERVER_CREATE_PARTITION_FAILED = 109,
      EN_CREATE_PARTITION_WITH_OLD_MAJOR_TS = 110,
      EN_PREPARE_SPLIT_FAILED = 111,
      EN_REPLAY_SOURCE_SPLIT_LOG_FAILED = 112,
      EN_SAVE_SPLIT_STATE_FAILED = 113,
      EN_FORCE_REFRESH_TABLE = 114,
      EN_REPLAY_SPLIT_DEST_LOG_FAILED = 116,
      EN_PROCESS_TO_PRIMARY_ERR = 117,
      EN_CREATE_PG_AFTER_CREATE_SSTBALES = 118,
      EN_CREATE_PG_AFTER_REGISTER_TRANS_SERVICE = 119,
      EN_CREATE_PG_AFTER_REGISTER_ELECTION_MGR = 120,
      EN_CREATE_PG_AFTER_ADD_PARTITIONS_TO_MGR = 121,
      EN_CREATE_PG_AFTER_ADD_PARTITIONS_TO_REPLAY_ENGINE = 122,
      EN_CREATE_PG_AFTER_BATCH_START_PARTITION_ELECTION = 123,
      EN_BACKUP_MACRO_BLOCK_SUBTASK_FAILED = 124,
      EN_BACKUP_REPORT_RESULT_FAILED = 125,
      EN_RESTORE_UPDATE_PARTITION_META_FAILED = 126,
      EN_BACKUP_FILTER_TABLE_BY_SCHEMA = 127,
      EN_FORCE_DFC_BLOCK = 128,
      EN_SERVER_PG_META_WRITE_HALF_FAILED = 129,
      EN_SERVER_TENANT_FILE_SUPER_BLOCK_WRITE_HALF_FAILED = 130,
      EN_DTL_ONE_ROW_ONE_BUFFER = 131,
      EN_LOG_ARCHIVE_PUSH_LOG_FAILED = 132,

      EN_BACKUP_DATA_VERSION_GAP_OVER_LIMIT = 133,
      EN_LOG_ARHIVE_SCHEDULER_INTERRUPT = 134,
      EN_BACKUP_IO_LIST_FILE = 135,
      EN_BACKUP_IO_IS_EXIST = 136,
      EN_BACKUP_IO_GET_FILE_LENGTH = 137,
      EN_BACKUP_IO_BEFORE_DEL_FILE = 138,
      EN_BACKUP_IO_AFTER_DEL_FILE = 139,
      EN_BACKUP_IO_BEFORE_MKDIR = 140,
      EN_BACKUP_IO_AFTER_MKDIR = 141,
      EN_BACKUP_IO_UPDATE_FILE_MODIFY_TIME = 142,
      EN_BACKUP_IO_BEFORE_WRITE_SINGLE_FILE = 143,
      EN_BACKUP_IO_AFTER_WRITE_SINGLE_FILE = 144,
      EN_BACKUP_IO_READER_OPEN = 145,
      EN_BACKUP_IO_READER_PREAD = 146,
      EN_BACKUP_IO_WRITE_OPEN = 147,
      EN_BACKUP_IO_WRITE_WRITE = 148,
      EN_BACKUP_IO_APPENDER_OPEN = 149,
      EN_BACKUP_IO_APPENDER_WRITE = 150,
      EN_ROOT_BACKUP_MAX_GENERATE_NUM = 151,
      EN_ROOT_BACKUP_NEED_SWITCH_TENANT = 152,
      EN_BACKUP_FILE_APPENDER_CLOSE = 153,
      EN_RESTORE_MACRO_CRC_ERROR = 154,
      EN_BACKUP_DELETE_HANDLE_LS_TASK = 155,
      EN_BACKUP_DELETE_MARK_DELETING = 156,
      EN_RESTORE_FETCH_CLOG_ERROR = 157,
      EN_BACKUP_LEASE_CAN_TAKEOVER = 158,
      EN_BACKUP_EXTERN_INFO_ERROR = 159,
      EN_INCREMENTAL_BACKUP_NUM = 160,
      EN_LOG_ARCHIVE_BEFORE_PUSH_LOG_FAILED = 161,
      EN_BACKUP_META_INDEX_BUFFER_NOT_COMPLETED = 162,
      EN_BACKUP_MACRO_INDEX_BUFFER_NOT_COMPLETED = 163,
      EN_LOG_ARCHIVE_DATA_BUFFER_NOT_COMPLETED = 164,
      EN_LOG_ARCHIVE_INDEX_BUFFER_NOT_COMPLETED = 165,
      EN_FILE_SYSTEM_RENAME_ERROR = 166,
      EN_BACKUP_OBSOLETE_INTERVAL = 167,
      EN_BACKUP_BACKUP_LOG_ARCHIVE_INTERRUPTED = 168,
      EN_BACKUP_BACKUPSET_EXTERN_INFO_ERROR = 169,
      EN_BACKUP_SCHEDULER_GET_SCHEMA_VERSION_ERROR = 170,
      EN_BACKUP_BACKUPSET_FILE_TASK = 171,
      EN_LOG_ARCHIVE_RESTORE_ACCUM_CHECKSUM_TAMPERED = 172,
      EN_BACKUP_BACKUPPIECE_FILE_TASK = 173,
      EN_BACKUP_RS_BLOCK_FROZEN_PIECE = 174,
      EN_LOG_ARCHIVE_BLOCK_SWITCH_PIECE = 175,
      EN_BACKUP_ARCHIVELOG_RPC_FAILED = 176,
      EN_BACKUP_AFTER_UPDATE_EXTERNAL_ROUND_INFO_FOR_USER = 177,
      EN_BACKUP_AFTER_UPDATE_EXTERNAL_ROUND_INFO_FOR_SYS = 178,
      EN_BACKUP_AFTER_UPDATE_EXTERNAL_BOTH_PIECE_INFO_FOR_USER = 179,
      EN_BACKUP_AFTER_UPDATE_EXTERNAL_BOTH_PIECE_INFO_FOR_SYS = 180,
      EN_BACKUP_BACKUPPIECE_FINISH_UPDATE_EXTERN_AND_INNER_INFO = 181,
      EN_BACKUP_BACKUPPIECE_DO_SCHEDULE = 182,
      EN_STOP_TENANT_LOG_ARCHIVE_BACKUP = 183,
      EN_BACKUP_SERVER_DISK_IS_FULL = 184,
      EN_CHANGE_TENANT_FAILED = 185,
      EN_BACKUP_PERSIST_LS_FAILED = 186,
      EN_BACKUP_VALIDATE_DO_FINISH = 189,
      EN_BACKUP_SYS_META_TASK_FAILED = 190,
      EN_BACKUP_SYS_TABLET_TASK_FAILED = 191,
      EN_BACKUP_DATA_TABLET_MINOR_SSTABLE_TASK_FAILED = 192,
      EN_BACKUP_DATA_TABLET_MAJOR_SSTABLE_TASK_FAILED = 193,
      EN_BACKUP_BUILD_LS_LEVEL_INDEX_TASK_FAILED = 194,
      EN_BACKUP_BUILD_TENANT_LEVEL_INDEX_TASK_FAILED = 195,
      EN_BACKUP_PREFETCH_BACKUP_INFO_FAILED = 196,
      EN_BACKUP_COMPLEMENT_LOG_TASK_FAILED = 197,
      EN_BACKUP_USER_META_TASK_FAILED = 198,
      EN_BACKUP_PREPARE_TASK_FAILED = 199,
      EN_BACKUP_CHECK_TABLET_CONTINUITY_FAILED = 200,
      // 下面请从201开始
      EN_CHECK_STANDBY_CLUSTER_SCHEMA_CONDITION = 201,
      EN_ALLOCATE_LOB_BUF_FAILED = 202,
      EN_ALLOCATE_DESERIALIZE_LOB_BUF_FAILED = 203,
      EN_ENCRYPT_ALLOCATE_HASHMAP_FAILED = 204,
      EN_ENCRYPT_ALLOCATE_ROW_BUF_FAILED = 205,
      EN_ENCRYPT_GET_MASTER_KEY_FAILED = 206,
      EN_DECRYPT_ALLOCATE_ROW_BUF_FAILED = 207,
      EN_DECRYPT_GET_MASTER_KEY_FAILED = 208,
      EN_FAST_MIGRATE_CHANGE_MEMBER_LIST_NOT_BEGIN = 209,
      EN_FAST_MIGRATE_CHANGE_MEMBER_LIST_AFTER_REMOVE = 210,
      EN_FAST_MIGRATE_CHANGE_MEMBER_LIST_SUCCESS_BUT_TIMEOUT = 211,
      EN_SCHEDULE_DATA_MINOR_MERGE = 212,
      EN_LOG_SYNC_SLOW = 213,
      EN_WRITE_CONFIG_FILE_FAILED = 214,
      EN_INVALID_ADDR_WEAK_READ_FAILED = 215,
      EN_STACK_OVERFLOW_CHECK_EXPR_STACK_SIZE = 216,
      EN_ENABLE_PDML_ALL_FEATURE = 217,
      // slog checkpoint错误模拟占坑 218-230
      EN_SLOG_CKPT_ERROR = 218,
      EN_FAST_RECOVERY_AFTER_ALLOC_FILE = 219,
      EN_FAST_MIGRATE_ADD_MEMBER_FAIL = 220,
      EN_FAST_RECOVERY_BEFORE_ADD_MEMBER = 221,
      EN_FAST_RECOVERY_AFTER_ADD_MEMBER = 222,
      EN_FAST_RECOVERY_AFTER_REMOVE_MEMBER = 223,
      EN_OFS_IO_SUBMIT = 224,
      EN_MIGRATE_ADD_PARTITION_FAILED = 225,
      EN_PRINT_QUERY_SQL = 231,
      EN_ADD_NEW_PG_TO_PARTITION_SERVICE = 232,
      EN_DML_DISABLE_RANDOM_RESHUFFLE =233,
      EN_RESIZE_PHYSICAL_FILE_FAILED = 234,
      EN_ALLOCATE_RESIZE_MEMORY_FAILED = 235,
      EN_WRITE_SUPER_BLOCK_FAILED = 236,
      EN_GC_FAILED_PARTICIPANTS = 237,
      EN_SSL_INVITE_NODES_FAILED = 238,
      EN_ADD_TRIGGER_SKIP_MAP = 239,
      EN_DEL_TRIGGER_SKIP_MAP = 240,
      EN_RESET_FREE_MEMORY = 241,
      EN_BKGD_TASK_REPORT_COMPLETE = 242,
      EN_BKGD_TRANSMIT_CHECK_STATUS_PER_ROW = 243,
      EN_OPEN_REMOTE_ASYNC_EXECUTION = 244,
      EN_BACKUP_DELETE_EXCEPTION_HANDLING = 245,
      EN_SORT_IMPL_FORCE_DO_DUMP = 246,
      EN_ENFORCE_PUSH_DOWN_WF = 247,
      //
      EN_TRANS_SHARED_LOCK_CONFLICT = 250,
      EN_HASH_JOIN_OPTION = 251,
      EN_SET_DISABLE_HASH_JOIN_BATCH = 252,
      EN_INNER_SQL_CONN_LEAK_CHECK = 253,
      EN_ADAPTIVE_GROUP_BY_SMALL_CACHE = 254,

      // only work for remote execute
      EN_DISABLE_REMOTE_EXEC_WITH_PLAN = 255,
      EN_REMOTE_EXEC_ERR = 256,

      EN_XA_PREPARE_ERROR = 260,
      EN_XA_UPDATE_COORD_FAILED = 261,
      EN_XA_PREPARE_RESP_LOST = 262,
      EN_XA_RPC_TIMEOUT = 263,
      EN_XA_COMMIT_ABORT_RESP_LOST = 264,
      EN_XA_1PC_RESP_LOST = 265,
      EN_DISK_ERROR = 266,


      EN_CLOG_DUMP_ILOG_MEMSTORE_RENAME_FAILURE = 267,
      EN_CLOG_ILOG_MEMSTORE_ALLOC_MEMORY_FAILURE = 268,
      EN_CLOG_LOG_NOT_IN_SW = 269,
      EN_CLOG_PARTITION_IS_NOT_SYNC = 270,
      EN_CLOG_LOG_NOT_IN_ILOG_STORAGE = 271,
      EN_CLOG_SW_OUT_OF_RANGE = 272,
      EN_DFC_FACTOR = 273,
      EN_LOGSERVICE_IO_TIMEOUT = 274,

      EN_PARTICIPANTS_SIZE_OVERFLOW = 275,
      EN_UNDO_ACTIONS_SIZE_OVERFLOW = 276,
      EN_PART_PLUS_UNDO_OVERFLOW = 277,
      EN_HANDLE_PREPARE_MESSAGE_EAGAIN = 278,

      //simulate DAS errors 301-350
      EN_DAS_SCAN_RESULT_OVERFLOW = 301,
      EN_DAS_DML_BUFFER_OVERFLOW = 302,
      EN_DAS_SIMULATE_OPEN_ERROR = 303,
      EN_DAS_WRITE_ROW_LIST_LEN = 304,
      EN_DAS_SIMULATE_VT_CREATE_ERROR = 305,
      EN_DAS_SIMULATE_LOOKUPOP_INIT_ERROR = 306,
      EN_DAS_SIMULATE_ASYNC_RPC_TIMEOUT = 307,
      EN_DAS_SIMULATE_DUMP_WRITE_BUFFER = 308,

      EN_REPLAY_STORAGE_SCHEMA_FAILURE = 351,
      EN_SKIP_GET_STORAGE_SCHEMA = 352,

      EN_PREVENT_SYNC_REPORT = 360,
      EN_PREVENT_ASYNC_REPORT = 361,
      EN_REBALANCE_TASK_RETRY = 362,
      EN_LOG_IDS_COUNT_ERROR = 363,

      EN_AMM_WASH_RATIO = 364,
      EN_ENABLE_THREE_STAGE_AGGREGATE = 365,
      EN_ROLLUP_ADAPTIVE_KEY_NUM = 366,
      EN_ENABLE_OP_OUTPUT_DATUM_CHECK =  367,
      EN_LEADER_STORAGE_ESTIMATION = 368,

      // SQL table_scan, index_look_up and other dml_op 400-500
      EN_TABLE_LOOKUP_BATCH_ROW_COUNT = 400,
      EN_TABLE_REPLACE_BATCH_ROW_COUNT = 401,
      EN_TABLE_INSERT_UP_BATCH_ROW_COUNT = 402,
      EN_EXPLAIN_BATCHED_MULTI_STATEMENT = 403,
      EN_INS_MULTI_VALUES_BATCH_OPT = 404,

      // DDL related 500-550
      EN_DATA_CHECKSUM_DDL_TASK = 501,
      EN_HIDDEN_CHECKSUM_DDL_TASK = 502,
      EN_SUBMIT_INDEX_TASK_ERROR_BEFORE_STAT_RECORD = 503,
      EN_SUBMIT_INDEX_TASK_ERROR_AFTER_STAT_RECORD = 504,
      EN_BUILD_LOCAL_INDEX_WITH_CORRUPTED_DATA = 505,
      EN_BUILD_GLOBAL_INDEX_WITH_CORRUPTED_DATA = 506,
      EN_EARLY_RESPONSE_SCHEDULER = 509,
      EN_DDL_TASK_PROCESS_FAIL_STATUS = 510,
      EN_DDL_TASK_PROCESS_FAIL_ERROR = 511,
      EN_DDL_START_FAIL = 512,
      EN_DDL_COMPACT_FAIL = 513,
      EN_DDL_RELEASE_DDL_KV_FAIL = 514,
      EN_DDL_REPORT_CHECKSUM_FAIL = 515,
      EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL = 516,
      EN_DDL_DIRECT_LOAD_WAIT_TABLE_LOCK_FAIL = 517,

      // SQL Optimizer related 551-599
      EN_EXPLAIN_GENERATE_PLAN_WITH_OUTLINE = 551,
      EN_ENABLE_AUTO_DOP_FORCE_PARALLEL_PLAN = 552,
      EN_GENERATE_PLAN_WITH_RECONSTRUCT_SQL = 553,

      // 600-700 For PX use
      EN_PX_SQC_EXECUTE_FAILED = 600,
      EN_PX_SQC_INIT_FAILED = 601,
      EN_PX_SQC_INIT_PROCESS_FAILED = 602,
      EN_PX_PRINT_TARGET_MONITOR_LOG = 603,
      EN_PX_SQC_NOT_REPORT_TO_QC = 604,
      EN_PX_QC_EARLY_TERMINATE = 605,
      EN_PX_SINGLE_DFO_NOT_ERASE_DTL_INTERM_RESULT = 606,
      EN_PX_TEMP_TABLE_NOT_DESTROY_REMOTE_INTERM_RESULT = 607,
      EN_PX_NOT_ERASE_P2P_DH_MSG = 608,
      EN_PX_SLOW_PROCESS_SQC_FINISH_MSG = 609,
      EN_PX_JOIN_FILTER_NOT_MERGE_MSG = 610,
      EN_PX_P2P_MSG_REG_DM_FAILED= 611,
      EN_PX_JOIN_FILTER_HOLD_MSG = 612,
      EN_PX_DTL_TRACE_LOG_ENABLE = 613,
      // please add new trace point after 700 or before 600

      // Compaction Related 700-750
      EN_COMPACTION_DIAGNOSE_TABLE_STORE_UNSAFE_FAILED = 700,
      EN_COMPACTION_DIAGNOSE_CANNOT_MAJOR = 701,
      EN_COMPACTION_MERGE_TASK = 702,
      EN_MEDIUM_COMPACTION_SUBMIT_CLOG_FAILED = 703,
      EN_MEDIUM_COMPACTION_UPDATE_CUR_SNAPSHOT_FAILED = 704,
      EN_MEDIUM_REPLICA_CHECKSUM_ERROR = 705,
      EN_MEDIUM_CREATE_DAG = 706,
      EN_MEDIUM_VERIFY_GROUP_SKIP_SET_VERIFY = 707,
      EN_MEDIUM_VERIFY_GROUP_SKIP_COLUMN_CHECKSUM = 708,
      EN_SCHEDULE_MEDIUM_COMPACTION = 709,
      EN_SCHEDULE_MAJOR_GET_TABLE_SCHEMA = 710,
      EN_SKIP_INDEX_MAJOR = 711,
      EN_BUILD_DATA_MICRO_BLOCK = 712,

      // please add new trace point after 750
      EN_SESSION_LEAK_COUNT_THRESHOLD = 751,
      EN_END_PARTICIPANT = 800,

      //LS Migration Related 900 - 1000
      EN_INITIAL_MIGRATION_TASK_FAILED = 900,
      EN_START_MIGRATION_TASK_FAILED = 901,
      EN_SYS_TABLETS_MIGRATION_TASK_FAILED = 902,
      EN_DATA_TABLETS_MIGRATION_TASK_FAILED = 903,
      EN_TABLET_GROUP_MIGRATION_TASK_FAILED = 904,
      EN_TABLET_MIGRATION_TASK_FAILED = 905,
      EN_MIGRATION_FINISH_TASK_FAILED = 906,
      EN_MIGRATION_READ_REMOTE_MACRO_BLOCK_FAILED = 907,
      EN_MIGRATION_ENABLE_LOG_FAILED = 908,
      EN_MIGRATION_ENABLE_VOTE_RETRY = 909,
      EN_MIGRATION_ENABLE_VOTE_FAILED = 910,
      EN_MIGRATION_COPY_MACRO_BLOCK_NUM = 911,
      EN_FINISH_TABLET_GROUP_RESTORE_FAILED = 912,
      EN_MIGRATION_ONLINE_FAILED = 913,
      EN_MIGRATION_GENERATE_SYS_TABLETS_DAG_FAILED = 914,
      EN_COPY_MAJOR_SNAPSHOT_VERSION = 915,
      EN_TABLET_MIGRATION_DAG_INNER_RETRY = 916,
      EN_LS_REBUILD_PREPARE_FAILED = 917,
      EN_TABLET_GC_TASK_FAILED = 918,
      EN_UPDATE_TABLET_HA_STATUS_FAILED = 919,
      EN_GENERATE_REBUILD_TASK_FAILED = 920,
      EN_CHECK_TRANSFER_TASK_EXSIT = 921,
      EN_TABLET_EMPTY_SHELL_TASK_FAILED = 922,

      // Log Archive and Restore 1001 - 1100
      EN_START_ARCHIVE_LOG_GAP = 1001,
      EN_RESTORE_LOG_FAILED = 1002,
      EN_RESTORE_LOG_FROM_SOURCE_FAILED = 1003,
      EN_BACKUP_MULTIPLE_MACRO_BLOCK = 1004,
      EN_RESTORE_FETCH_TABLET_INFO = 1005,
      EN_RESTORE_COPY_MACRO_BLOCK_NUM = 1006,

      // START OF STORAGE HA - 1101 - 2000
      EN_BACKUP_META_REPORT_RESULT_FAILED = 1101,
      EN_RESTORE_LS_INIT_PARAM_FAILED = 1102,
      EN_RESTORE_TABLET_INIT_PARAM_FAILED = 1103,
      EN_ADD_BACKUP_META_DAG_FAILED = 1104,
      EN_ADD_BACKUP_DATA_DAG_FAILED = 1105,
      EN_ADD_BACKUP_BUILD_INDEX_DAG_FAILED = 1106,
      EN_ADD_BACKUP_PREPARE_DAG_FAILED = 1107,
      EN_ADD_BACKUP_FINISH_DAG_FAILED = 1108,
      EN_ADD_BACKUP_PREFETCH_DAG_FAILED = 1109,
      EN_BACKUP_PERSIST_SET_TASK_FAILED = 1110,
      EN_BACKUP_READ_MACRO_BLOCK_FAILED = 1111,
      EN_FETCH_TABLE_INFO_RPC = 1112,
      EN_RESTORE_TABLET_TASK_FAILED = 1113,
      EN_INSERT_USER_RECOVER_JOB_FAILED = 1114,
      EN_INSERT_AUX_TENANT_RESTORE_JOB_FAILED = 1115,
      EN_RESTORE_CREATE_LS_FAILED = 1116,
      // END OF STORAGE HA - 1101 - 2000

      // sql parameterization 1170-1180
      EN_SQL_PARAM_FP_NP_NOT_SAME_ERROR = 1170,
      EN_FLUSH_PC_NOT_CLEANUP_LEAK_MEM_ERROR = 1171,
      // END OF sql parameterization 1170-1180

      // session info verification
      // The types are used for error verification
      EN_SESS_INFO_VERI_SYS_VAR_ERROR = 1180,
      EN_SESS_INFO_VERI_APP_INFO_ERROR = 1181,
      EN_SESS_INFO_VERI_APP_CTX_ERROR = 1182,
      EN_SESS_INFO_VERI_CLIENT_ID_ERROR = 1183,
      EN_SESS_INFO_VERI_CONTROL_INFO_ERROR = 1184,
      EN_SESS_INFO_VERI_TXN_EXTRA_INFO_ERROR = 1185,
      EN_SESS_POOL_MGR_CTRL = 1186,
      // session info diagnosis control
      // EN_SESS_INFO_DIAGNOSIS_CONTROL = 1187,
      EN_ENABLE_NEWSORT_FORCE = 1200,

      // Transaction // 2001 - 2100
      // Transaction free route
      EN_TX_FREE_ROUTE_UPDATE_STATE_ERROR = 2001,
      EN_TX_FREE_ROUTE_ENCODE_STATE_ERROR = 2002,
      EN_TX_FREE_ROUTE_STATE_SIZE = 2003,
      // Transaction common
      EN_TX_RESULT_INCOMPLETE = 2011,
      EN_THREAD_HANG = 2022,

      EN_ENABLE_SET_TRACE_CONTROL_INFO = 2100,
      EN_CHEN = 2101,

      // WR && ASH
      EN_CLOSE_ASH = 2201,

      EVENT_TABLE_MAX = SIZE_OF_EVENT_TABLE
    };

    /* get an event value */
    inline EventItem &get_event(int64_t index)
    { return (index >= 0 && index < SIZE_OF_EVENT_TABLE) ? event_table_[index] : event_table_[0]; }

    /* set an event value */
    inline void set_event(int64_t index, const EventItem &item)
    {
      if (index >= 0 && index < SIZE_OF_EVENT_TABLE) {
         event_table_[index] = item;
      }
    }

    static inline void set_event(const char *name, const EventItem &item)
    {
      DLIST_FOREACH_NORET(i, global_item_list()) {
        if (NULL != i->name_ && NULL != name && strcmp(i->name_, name) == 0) {
          i->item_ = item;
        }
      }
    }

    static ObDList<NamedEventItem> &global_item_list()
    {
      static ObDList<NamedEventItem> g_list;
      return g_list;
    }

    static EventTable &instance()
    {
      static EventTable et;
      return et;
    }

  private:
    /*
       Array of error codes for all tracepoints.
       For normal error code generation, the value should be the error code itself.
     */
    EventItem event_table_[SIZE_OF_EVENT_TABLE];
};

inline void event_access(int64_t index,        /* tracepoint number */
                         EventItem &item,
                         bool is_get)                 /* is a 'get' */
{
  if (is_get) item = EventTable::instance().get_event(index);
  else EventTable::instance().set_event(index, item);
}

}
}

#endif //OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_

#if __TEST_TRACEPOINT__
#include <stdio.h>

int fake_syscall()
{
  return 0;
}

int test_tracepoint(int x)
{
  int err = 0;
  printf("=== start call: %d ===\n", x);
  if (0 != (err = OB_I(a, x) fake_syscall())) {
    printf("fail at step 1: err=%d\n", err);
  } else if (0 != (err = OB_I(b) fake_syscall())) {
    printf("fail at step 2: err=%d\n", err);
  } else {
    printf("succ\n");
  }
  return err;
}

int tp_handler(int x)
{
  printf("tp_handler: x=%d\n", x);
  return 0;
}

int run_tests()
{
  test_tracepoint(0);
  TP_SET("tracepoint.h", "test_tracepoint", "a",  &tp_handler);
  TP_SET_ERROR("tracepoint.h", "test_tracepoint", "b",  -1);
  test_tracepoint(1);
  TP_SET_ERROR("tracepoint.h", "test_tracepoint", "b",  -2);
  test_tracepoint(2);
  TP_SET("tracepoint.h", "test_tracepoint", "b",  NULL);
  test_tracepoint(3);
}
#endif
