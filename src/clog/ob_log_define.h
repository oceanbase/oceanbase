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

#ifndef OCEANBASE_CLOG_OB_LOG_DEFINE_H_
#define OCEANBASE_CLOG_OB_LOG_DEFINE_H_

#include <stdlib.h>
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/compress/ob_compress_util.h"
#include "common/ob_clock_generator.h"
#include "share/ob_proposal_id.h"
#include "share/config/ob_server_config.h"
#include "ob_clog_config.h"
#include "ob_log_timer_utility.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace clog {
#define CLOG_DIO_ALIGN_SIZE 4096
#define TMP_SUFFIX ".tmp"

typedef uint32_t file_id_t;
typedef int32_t offset_t;

const int64_t CLOG_RPC_TIMEOUT = 3000 * 1000 - 100 * 1000;
const int64_t CLOG_TRAILER_SIZE = 512;
const int64_t CLOG_TRAILER_OFFSET = CLOG_FILE_SIZE - CLOG_TRAILER_SIZE;                // 512B for the trailer block
const int64_t CLOG_TRAILER_ALIGN_WRITE_OFFSET = CLOG_FILE_SIZE - CLOG_DIO_ALIGN_SIZE;  // 4k aligned write
const int64_t CLOG_MAX_DATA_OFFSET = CLOG_TRAILER_OFFSET - common::OB_MAX_LOG_BUFFER_SIZE;
const int64_t CLOG_CACHE_SIZE = 64 * 1024;
const int64_t CLOG_REPLAY_CHECKSUM_WINDOW_SIZE = 1 << 9;
const int64_t CLOG_INFO_BLOCK_SIZE_LIMIT = 1 << 22;
const offset_t OB_INVALID_OFFSET = -1;

inline bool is_valid_log_id(const uint64_t log_id)
{
  return (common::OB_INVALID_ID != log_id && 0 != log_id);
}
inline bool is_valid_file_id(const file_id_t file_id)
{
  return (common::OB_INVALID_FILE_ID != file_id && 0 != file_id);
}
inline bool is_valid_offset(const offset_t offset)
{
  return (offset >= 0) && (static_cast<int64_t>(offset) <= CLOG_FILE_SIZE);
}

inline int64_t get_hot_cache_size(bool is_clog)
{
  int64_t hot_cache_size = 0;
  const int64_t ILOG_HOT_CACHE_HARD_LIMIT = 1L << 26;  // 64M
  if (is_clog) {
    const int64_t server_hot_cache_limit =
        common::ObMallocAllocator::get_instance()->get_tenant_limit(common::OB_SERVER_TENANT_ID) / 100 *
        HOT_CACHE_MEM_PERCENT;
    int64_t hot_cache_upper_limit = std::min(HOT_CACHE_UPPER_HARD_LIMIT, server_hot_cache_limit);
    // The hot_cache size refers to 5% of the memory of 500 tenants as the upper limit,
    // the actual value is minimum 256M, maximum 1G
    hot_cache_size = std::max(HOT_CACHE_LOWER_HARD_LIMIT, hot_cache_upper_limit);
  } else {
    hot_cache_size = ILOG_HOT_CACHE_HARD_LIMIT;
  }
  return hot_cache_size;
}

inline int file_id_cmp_func(const void* v1, const void* v2)
{
  return *(file_id_t*)v1 - *(file_id_t*)v2;
}

enum ObLogType {
  OB_LOG_UNKNOWN = 0,
  // Log type recognized by the log module
  OB_LOG_MEMBERSHIP = 101,        // member change log
  OB_LOG_PREPARED = 102,          // log that record proposal_id during multi-paxos prepare phase
  OB_LOG_NOP = 103,               // leader may generate nop log during reconfirm
  OB_LOG_START_MEMBERSHIP = 104,  // leader generates before provide service. This log is not only used as filtering,
                                  // but also to ensure that the membership change agreement is correct
  OB_LOG_NOT_EXIST = 105,         // Mark a log as non-existent
  OB_LOG_RENEW_MEMBERSHIP = 106,  // Log type of the standby database's persistent member list
  OB_LOG_STANDBY_PREPARED = 107,  // prepare log for standby cluster
  OB_LOG_TRUNCATE = 108,          // truncate log for standby failover,used to filter logs after hole

  // The log type submitted by the upper module, currently includes transaction log and frozen log
  OB_LOG_SUBMIT = 201,
  OB_LOG_AGGRE = 202,

  OB_LOG_ARCHIVE_CHECKPOINT = 301,  // Mark cold partition log archive heartbeat information
  OB_LOG_ARCHIVE_KICKOFF = 302,     // Mark the information opened in a certain round of log archive
};

inline bool is_log_type_need_replay(enum ObLogType log_type)
{
  return (OB_LOG_SUBMIT == log_type || OB_LOG_AGGRE == log_type || OB_LOG_START_MEMBERSHIP == log_type);
}

inline bool is_prepared_log(enum ObLogType log_type)
{
  return OB_LOG_PREPARED == log_type || OB_LOG_STANDBY_PREPARED == log_type;
}

inline bool is_nop_or_truncate_log(enum ObLogType log_type)
{
  return OB_LOG_NOP == log_type || OB_LOG_TRUNCATE == log_type;
}

inline bool is_archive_checkpoint_log(enum ObLogType log_type)
{
  return OB_LOG_ARCHIVE_CHECKPOINT == log_type;
}

inline bool is_archive_kickoff_log(enum ObLogType log_type)
{
  return OB_LOG_ARCHIVE_KICKOFF == log_type;
}

enum ObFetchParentRespMsgType {
  OB_FETCH_PARENT_RESP_MSG_TYPE_UNKNOWN = 0,
  OB_FETCH_PARENT_RESP_MSG_TYPE_PARENT = 1,  // assign parent for readonly replica
  OB_FETCH_PARENT_RESP_MSG_TYPE_LEADER = 2,  // notify readonly replica the new leader when leader changed
};

enum ObReplicaMsgType {
  OB_REPLICA_MSG_TYPE_UNKNOWN = 0,
  OB_REPLICA_MSG_TYPE_NOT_MASTER = 1,      // I'm not leader
  OB_REPLICA_MSG_TYPE_NOT_PARENT = 2,      // I'm not your parent
  OB_REPLICA_MSG_TYPE_NOT_CHILD = 3,       // I'm not your child
  OB_REPLICA_MSG_TYPE_NOT_EXIST = 4,       // partition not exist
  OB_REPLICA_MSG_TYPE_DISABLED_STATE = 5,  // server in disabled state
};

enum ObRegRespMsgType {
  OB_REG_RESP_MSG_UNKNOWN = 0,
  OB_REG_RESP_MSG_SICK = 1,             // dest server is sick
  OB_REG_RESP_MSG_REGION_DIFF = 2,      // region is not same
  OB_REG_RESP_MSG_CHILD_LIST_FULL = 3,  // children list is full
};

enum ObFetchLogType {
  OB_FETCH_LOG_UNKNOWN = 0,
  OB_FETCH_LOG_LEADER_RECONFIRM = 1,
  OB_FETCH_LOG_FOLLOWER_ACTIVE = 2,
  OB_FETCH_LOG_RESTORE_FOLLOWER = 3,
  OB_FETCH_LOG_STANDBY_RESTORE = 4,
  OB_FETCH_LOG_STANDBY_REPLICA = 5,
  OB_FETCH_LOG_TYPE_MAX,
};

enum ReceiveLogType {
  RL_TYPE_UNKNOWN = 0,
  PUSH_LOG = 1,
  FETCH_LOG = 2,
};

enum ObLogWritePoolType {
  INVALID_WRITE_POOL = 0,
  CLOG_WRITE_POOL = 1,
  ILOG_WRITE_POOL = 2,
  SLOG_WRITE_POOL = 3,
};

enum InfoBlockReaderType {
  CLOG_INFO_BLOCK_READER,
  ILOG_INFO_BLOCK_READER,
};

// enum ObLogState
//{
//  UNKNOWN = 0,
//  INIT,         // 1
//  REPLAY,       // 2
//  RECONFIRM,    // 3
//  ACTIVE,       // 4
//  TAKING_OVER,  // 5
//  REVOKING,     // 6
//};

enum ObReplicaReplayType {
  INVALID_REPLICA_REPLAY_TYPE = 0,
  REPLICA_REPLAY_ALL_LOG,        // F replica with memtable need replay all logs
  REPLICA_REPLAY_PARTITIAL_LOG,  // replica without memtable need replay logs except for transaction log
};

const int16_t UNKNOWN = 0;
const int16_t INIT = 1;
const int16_t REPLAY = 2;
const int16_t RECONFIRM = 3;
const int16_t ACTIVE = 4;
const int16_t TAKING_OVER = 5;
const int16_t REVOKING = 6;

enum ObBlockType {
  OB_NONE_BLOCK = 0,
  OB_DATA_BLOCK,        // block for storing data
  OB_INFO_BLOCK,        // block for storing INFO_BLOCK
  OB_TRAILER_BLOCK,     // block for storing Trailer block
  OB_HEADER_BLOCK,      // file header
  OB_BASE_BLOCK = 100,  // block for mocking SStable write BaseStorageInfo
};

// Cursor compare
// return 1 if (f1,o1)  > (f2,o2)
//       -1 if (f1,o1)  < (f2,o2)
//        0 if (f1,o1) == (f2,o2)
inline int cursor_cmp(const file_id_t f1, const offset_t o1, const file_id_t f2, const offset_t o2)
{
  int cmp_ret = 0;
  if (f1 > f2) {
    cmp_ret = 1;
  } else if (f1 < f2) {
    cmp_ret = -1;
  } else {
    if (o1 > o2) {
      cmp_ret = 1;
    } else if (o1 < o2) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }
  }
  return cmp_ret;
}

class ObTailCursor {
public:
  ObTailCursor() : file_id_(common::OB_INVALID_FILE_ID), offset_(OB_INVALID_OFFSET)
  {}
  ~ObTailCursor()
  {}
  void reset()
  {
    update(common::OB_INVALID_FILE_ID, OB_INVALID_OFFSET);
  }
  void init(const file_id_t file_id, const offset_t offset)
  {
    update(file_id, offset);
  }
  ObTailCursor get()
  {
    uint64_t v = ATOMIC_LOAD((uint64_t*)this);
    return *(ObTailCursor*)&v;
  }
  void advance(const file_id_t file_id, const offset_t offset)
  {
    assert_inc(file_id, offset);
    update(file_id, offset);
  }
  void update(const file_id_t file_id, const offset_t offset)
  {
    ObTailCursor new_tail;
    new_tail.file_id_ = file_id;
    new_tail.offset_ = offset;
    ATOMIC_STORE((uint64_t*)this, *(uint64_t*)&new_tail);
  }
  bool is_valid()
  {
    return is_valid_file_id(file_id_) && is_valid_offset(offset_);
  }
  void assert_inc(const file_id_t file_id, const offset_t offset)
  {
    ObTailCursor old_cursor = get();
    if (cursor_cmp(file_id, offset, old_cursor.file_id_, old_cursor.offset_) < 0) {
      CLOG_LOG(ERROR,
          "assert update cursor larger error",
          K(old_cursor.file_id_),
          K(old_cursor.offset_),
          K(file_id),
          K(offset));
    }
  }
  TO_STRING_KV(K_(file_id), K_(offset));

public:
  file_id_t file_id_;
  offset_t offset_;
} __attribute__((aligned(8)));

struct ObLogCursor {
  file_id_t file_id_;
  offset_t offset_;
  int32_t size_;
  ObLogCursor()
  {
    reset();
  }
  bool is_valid() const
  {
    return file_id_ != common::OB_INVALID_FILE_ID && file_id_ != 0 && offset_ >= 0 && size_ >= 0;
  }
  void reset()
  {
    file_id_ = common::OB_INVALID_FILE_ID;
    offset_ = 0;
    size_ = 0;
  }
  int deep_copy(const ObLogCursor& log_cursor)
  {
    int ret = common::OB_SUCCESS;
    file_id_ = log_cursor.file_id_;
    offset_ = log_cursor.offset_;
    size_ = log_cursor.size_;
    return ret;
  }
  TO_STRING_KV(K_(file_id), K_(offset), K_(size));

private:
  // Intentionally copyable and assigned
};

// ObLogCursor extended structure
// add acc_cksm and timestamp member
class ObLogCursorExt {
public:
  ObLogCursorExt()
  {
    reset();
  }
  bool is_valid() const
  {
    return common::OB_INVALID_FILE_ID != file_id_ && 0 != file_id_ && offset_ >= 0 && size_ > 0 &&
           common::OB_INVALID_TIMESTAMP != submit_timestamp_;
  }
  void reset()
  {
    file_id_ = common::OB_INVALID_FILE_ID;
    offset_ = 0;
    size_ = 0;
    padding_flag_ = 0;
    acc_cksm_ = 0;
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
  }
  int reset(const file_id_t file_id, const offset_t offset, const int32_t size, const int64_t acc_cksm,
      const int64_t submit_timestamp, const bool batch_committed)
  {
    int ret = common::OB_SUCCESS;
    file_id_ = file_id;
    offset_ = offset;
    size_ = size;
    padding_flag_ = 0;
    acc_cksm_ = acc_cksm;
    submit_timestamp_ = submit_timestamp;
    if (batch_committed) {
      submit_timestamp_ = submit_timestamp_ | MASK;
    }
    return ret;
  }
  file_id_t get_file_id() const
  {
    return file_id_;
  }
  offset_t get_offset() const
  {
    return offset_;
  }
  int32_t get_size() const
  {
    return size_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_ & (MASK - 1);
  }
  bool is_batch_committed() const
  {
    bool bool_ret = false;
    if (common::OB_INVALID_TIMESTAMP != submit_timestamp_) {
      bool_ret = submit_timestamp_ & MASK;
    }
    return bool_ret;
  }
  int64_t get_accum_checksum() const
  {
    return acc_cksm_;
  }
  TO_STRING_KV(K_(file_id), K_(offset), K_(size), "submit_timestamp", get_submit_timestamp(), "is_batch_committed",
      is_batch_committed(), K_(acc_cksm));
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  static const uint64_t MASK = 1ull << 63;
  file_id_t file_id_;
  offset_t offset_;
  int32_t size_;
  int32_t padding_flag_;  // sizeof(ObLogCursorExt) == 32, padding_flag_ 4 bytes
  int64_t acc_cksm_;
  // Use the highest bit of submit_timestamp_ to indicate whether batch_committed
  int64_t submit_timestamp_;
  // Intentionally copyable and assigned
};

struct ObGetCursorResult {
  ObLogCursorExt* csr_arr_;
  int64_t arr_len_;
  int64_t ret_len_;

  ObGetCursorResult() : csr_arr_(NULL), arr_len_(0), ret_len_(0)
  {}
  ObGetCursorResult(ObLogCursorExt* csr_arr, int64_t arr_len) : csr_arr_(csr_arr), arr_len_(arr_len), ret_len_(0)
  {}
  void reset()
  {
    csr_arr_ = NULL;
    arr_len_ = 0;
    ret_len_ = 0;
  }
  bool is_valid() const
  {
    return NULL != csr_arr_ && arr_len_ > 0;
  }
  TO_STRING_KV(KP(csr_arr_), K(arr_len_), K(ret_len_));
};

struct ObLogFlushCbArg {
  ObLogFlushCbArg()
  {
    reset();
  }
  ObLogFlushCbArg(ObLogType log_type, uint64_t log_id, int64_t submit_timestamp, common::ObProposalID proposal_id,
      common::ObAddr leader, const int64_t cluster_id, ObLogCursor log_cursor, int64_t after_consume_timestamp)
      : log_type_(log_type),
        log_id_(log_id),
        submit_timestamp_(submit_timestamp),
        proposal_id_(proposal_id),
        after_consume_timestamp_(after_consume_timestamp)
  {
    log_cursor_ = log_cursor;
    leader_ = leader;
    cluster_id_ = cluster_id;
  }

  void deep_copy(const ObLogFlushCbArg& cb_arg)
  {
    log_type_ = cb_arg.log_type_;
    log_id_ = cb_arg.log_id_;
    submit_timestamp_ = cb_arg.submit_timestamp_;
    proposal_id_ = cb_arg.proposal_id_;
    leader_ = cb_arg.leader_;
    cluster_id_ = cb_arg.cluster_id_;
    log_cursor_.deep_copy(cb_arg.log_cursor_);
    after_consume_timestamp_ = cb_arg.after_consume_timestamp_;
  }

  void reset()
  {
    log_type_ = OB_LOG_UNKNOWN;
    log_id_ = common::OB_INVALID_ID;
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
    proposal_id_.reset();
    leader_.reset();
    cluster_id_ = common::OB_INVALID_CLUSTER_ID;
    log_cursor_.reset();
    after_consume_timestamp_ = common::OB_INVALID_TIMESTAMP;
  }

  ObLogType log_type_;
  uint64_t log_id_;
  int64_t submit_timestamp_;
  common::ObProposalID proposal_id_;
  common::ObAddr leader_;
  int64_t cluster_id_;
  ObLogCursor log_cursor_;
  int64_t after_consume_timestamp_;
  TO_STRING_KV(K_(log_type), K_(log_id), K_(proposal_id), K_(leader), K_(cluster_id), K_(log_cursor),
      K_(after_consume_timestamp));

private:
  // Intentionally copyable and assigned
};

struct ObPGLogArchiveStatus {
  OB_UNIS_VERSION(1);

public:
  ObPGLogArchiveStatus()
  {
    reset();
  }
  ~ObPGLogArchiveStatus()
  {
    reset();
  }
  bool operator<(const ObPGLogArchiveStatus& other);
  bool is_interrupted() const
  {
    return share::ObLogArchiveStatus::INTERRUPTED == status_;
  }
  bool is_stopped() const
  {
    return share::ObLogArchiveStatus::STOP == status_;
  }
  void reset();
  bool is_valid_for_clog_info();
  bool is_round_start_info_valid();
  TO_STRING_KV(K_(status), "status_str", share::ObLogArchiveStatus::get_str(status_), K_(round_start_ts),
      K_(round_start_log_id), K_(round_snapshot_version), K_(round_log_submit_ts), K_(round_clog_epoch_id),
      K_(round_accum_checksum), K_(archive_incarnation), K_(log_archive_round), K_(last_archived_log_id),
      K_(last_archived_checkpoint_ts), K_(last_archived_log_submit_ts), K_(clog_epoch_id), K_(accum_checksum));

public:
  share::ObLogArchiveStatus::STATUS status_;

  int64_t round_start_ts_;
  uint64_t round_start_log_id_;
  int64_t round_snapshot_version_;
  int64_t round_log_submit_ts_;
  int64_t round_clog_epoch_id_;
  int64_t round_accum_checksum_;

  int64_t archive_incarnation_;
  int64_t log_archive_round_;
  // Log_id of the last log archived
  // 1. If the data is not archived, it is the starting archive location -1, for example, the starting archive log_id is
  // 1, then the value is 0
  // 2. If the data has been archived, it is the largest log_id archived
  uint64_t last_archived_log_id_;
  int64_t last_archived_checkpoint_ts_;
  int64_t last_archived_log_submit_ts_;

  // used for getting start log point during the migration process
  int64_t clog_epoch_id_;
  int64_t accum_checksum_;
};

enum ObArchiveFetchLogResult {
  OB_ARCHIVE_FETCH_LOG_INIT = 0,
  OB_ARCHIVE_FETCH_LOG_FINISH_AND_NOT_ENOUGH = 1,
  OB_ARCHIVE_FETCH_LOG_FINISH_AND_ENOUGH = 2,
};

// direct_reader read cost
// clog read cost
struct ObReadCost {
  int64_t read_disk_count_;
  int64_t read_disk_size_;

  int64_t valid_data_size_;

  int64_t read_clog_disk_time_;        // read clog cost
  int64_t fill_clog_cold_cache_time_;  // fill clog cache cost
  int64_t memcpy_time_;                // memory copy cost

  // total time for read clog that contains all the time above
  int64_t total_read_clog_time_;

  ObReadCost()
  {
    reset();
  }
  ~ObReadCost()
  {}

  void reset()
  {
    read_disk_count_ = 0;
    read_disk_size_ = 0;
    valid_data_size_ = 0;
    read_clog_disk_time_ = 0;
    fill_clog_cold_cache_time_ = 0;
    memcpy_time_ = 0;
    total_read_clog_time_ = 0;
  }

  // only check read disk count
  bool is_valid() const
  {
    return read_disk_count_ >= 0;
  }

  ObReadCost& operator+=(const ObReadCost& read_cost)
  {
    read_disk_count_ += read_cost.read_disk_count_;
    read_disk_size_ += read_cost.read_disk_size_;
    valid_data_size_ += read_cost.valid_data_size_;
    read_clog_disk_time_ += read_cost.read_clog_disk_time_;
    fill_clog_cold_cache_time_ += read_cost.fill_clog_cold_cache_time_;
    memcpy_time_ += read_cost.memcpy_time_;
    total_read_clog_time_ += read_cost.total_read_clog_time_;
    return *this;
  }

  TO_STRING_KV(K_(read_disk_count), K_(read_clog_disk_time), K_(fill_clog_cold_cache_time), K_(memcpy_time),
      K_(valid_data_size), K_(read_disk_size), K_(total_read_clog_time));
};

struct ObIlogStorageQueryCost {
  int64_t read_ilog_disk_count_;
  int64_t read_info_block_disk_count_;

  // get_cursor_batch() cost
  int64_t get_cursor_batch_time_;

  ObIlogStorageQueryCost() : read_ilog_disk_count_(0), read_info_block_disk_count_(0), get_cursor_batch_time_(0)
  {}

  void reset()
  {
    read_ilog_disk_count_ = 0;
    read_info_block_disk_count_ = 0;
    get_cursor_batch_time_ = 0;
  }

  ObIlogStorageQueryCost& operator+=(const ObIlogStorageQueryCost& csr_cost)
  {
    read_ilog_disk_count_ += csr_cost.read_ilog_disk_count_;
    read_info_block_disk_count_ += csr_cost.read_info_block_disk_count_;
    get_cursor_batch_time_ += csr_cost.get_cursor_batch_time_;
    return *this;
  }

  bool is_valid() const
  {
    return read_info_block_disk_count_ >= 0 && read_ilog_disk_count_ >= 0 && get_cursor_batch_time_ >= 0;
  }

  TO_STRING_KV(K_(read_ilog_disk_count), K_(read_info_block_disk_count), K_(get_cursor_batch_time));
};

inline bool is_offset_align(const offset_t offset, const int64_t align_size)
{
  return (0 == (static_cast<int64_t>(offset) & (align_size - 1)));
}

// Align downward, if offset is an integer multiple of align_size, return the value of offset
inline offset_t do_align_offset(const offset_t offset, const int64_t align_size)
{
  return static_cast<offset_t>((static_cast<int64_t>(offset) / align_size) * align_size);
}

// Align to the next align_size position, if offset is an integer multiple of align_size, return offset + align_size
inline offset_t do_align_offset_upper(const offset_t offset, const int64_t align_size)
{
  return static_cast<offset_t>((offset / align_size + 1) * align_size);
}

inline offset_t do_align_offset_lower(const offset_t offset, const int64_t align_size)
{
  return do_align_offset(offset, align_size);
}

// Convert <file_id, offset> to a one-dimensional long offset
inline int64_t get_long_offset(const file_id_t file_id, const offset_t offset)
{
  return (file_id - 1) * CLOG_FILE_SIZE + offset;
}

inline bool partition_reach_time_interval(const int64_t interval, int64_t& warn_time)
{
  bool bool_ret = false;
  if ((common::ObTimeUtility::current_time() - warn_time >= interval) || common::OB_INVALID_TIMESTAMP == warn_time) {
    warn_time = common::ObTimeUtility::current_time();
    bool_ret = true;
  }
  return bool_ret;
}

inline int on_fatal_error(int ret_value)
{
  int ret = common::OB_SUCCESS;
  BACKTRACE(ERROR, true, "ret = %d", ret_value);
  kill(getpid(), SIGKILL);
  return ret;
}

template <typename T>
inline void inc_update(T& v_, T x)
{
  T v = 0;
  while (true) {
    if (x <= (v = v_)) {
      break;
    } else if (ATOMIC_BCAS(&v_, v, x)) {
      break;
    }
  }
}

#define OB_IS_INVALID_FILE_ID(statement) (OB_UNLIKELY(!is_valid_file_id(statement)))
#define OB_IS_INVALID_OFFSET(statement) (OB_UNLIKELY(!is_valid_offset(statement)))
#define OB_IS_INVALID_LOG_ID(statement) (OB_UNLIKELY(!is_valid_log_id(statement)))
#define IS_VALID_ALIGN(align_size) (0 == (align_size & (align_size - 1)))
#define MODULE_NAME "CLOG"
#define ON_SUBMIT_SW "ON_SUBMIT_SW"
#define BEFORE_SUBMIT_TO_NET_AND_DISK "BEFORE_SUBMIT_TO_NET_AND_DISK"
#define AFTER_SUBMIT_TO_NET_AND_DISK "AFTER_SUBMIT_TO_NET_AND_DISK"
#define ON_AFTER_CONSUME "ON_AFTER_CONSUME"
#define ON_FLUSH_CB "ON_FLUSH_CB"
#define ON_ACK "ON_ACK"
#define ON_SUCCESS "ON_SUCCESS"
#define ON_FINISHED "ON_FINISHED"
#define N_VERSION "version"
#define N_PARTITION_KEY "partition_key"
#define N_LOG_TYPE "log_type"
#define N_LOG_ID "log_id"
#define N_REQ_ID "req_id"
#define N_DATA_LEN "data_len"
#define N_GENERATION_TIMESTAMP "generation_timestamp"
#define N_EPOCH_ID "epoch_id"
#define N_PROPOSAL_ID "proposal_id"
#define N_SUBMIT_TIMESTAMP "submit_timestamp"
#define N_DATA_CHECKSUM "data_checksum"
#define N_FREEZE_VERSION "freeze_version"
#define N_ACTIVE_FREEZE_VERSION "active_memstore_version"
#define N_HEADER_CHECKSUM "header_checksum"
#define N_HEADER "ObLogEntryHeader"
#define N_DATABUF "data_buf"
#define N_MAGIC "magic"
#define N_TYPE "type"
#define N_TOTAL_LEN "total_len"
#define N_PADDING_LEN "padding_len"
#define N_TIMESTAMP "timestamp"
#define N_META_CHECKSUM "meta_checksum"
#define N_BLOCK_META "block_meta"
#define N_START_POS "start_pos"
#define N_FILE_ID "file_id"
#define N_TABLE_ID "table_id"
#define N_PARTITION_IDX "partition_idx"
#define N_OFFSET "offset"
#define N_READ_LEN "read_len"
#define N_ALIGN_BUF "align_buf"
#define N_SIZE "size"
#define N_ALIGN_SIZE "align_size"
#define N_IS_INIT "is_inited"
#define N_BUF_LEN "buf_len"
#define N_BUF "buf"
#define N_FD "fd"
#define N_WRITE_LEN "write_len"
#define N_LOG_SERVICE "log_service"
#define N_STATE_MGR "state_mgr"
#define N_SCAN_LOG_FINISHED "scan_log_finished"
#define N_ROLE "role"
#define N_STATE "state"
#define N_LEADER "leader"
#define N_SELF "self"
#define N_COUNT "count"
#define N_SERVER "server"
#define N_PKEY "pkey"
#define N_PCODE "pcode"
#define N_REGION "region"
#define N_REPLICA_TYPE "replica_type"
#define N_REPLICA_MSG_TYPE "replica_msg_type"
#define N_IS_ASSIGN_PARENT_SUCCEED "is_assign_parent_succeed"
#define N_CANDIDATE_MEMBER_LIST "candidate_member_list"
#define N_REG_RESP_MSG_TYPE "reg_resp_msg_type"
#define N_NEXT_ILOG_TS "next_ilog_ts"
#define N_NEXT_REPLAY_LOG_TS "next_replay_log_ts"
#define N_IS_REQUEST_LEADER "is_request_leader"
#define N_IS_NEED_FORCE_REGISTER "is_need_force_register"
#define N_SICK_CHILD "sick_child"
#define N_MAX_LOG_TS "max_log_ts"
#define N_CLUSTER_ID "cluster_id"
#define N_IDC "idc"
#ifndef NDEBUG
#define OB_LOG_CACHE_DEBUG
#endif
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_LOG_DEFINE_H_
