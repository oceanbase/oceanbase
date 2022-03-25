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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_RESOLVER_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_PART_TRANS_RESOLVER_H__

#include "lib/string/ob_string.h"                 // ObString
#include "common/ob_partition_key.h"              // ObPartitionKey
#include "clog/ob_log_entry.h"                    // ObLogEntry
#include "ob_log_entry_wrapper.h"                 // ObLogEntryWrapper
#include "storage/transaction/ob_trans_define.h"  // ObTransID, ObRedoLogIdArray
#include "storage/ob_storage_log_type.h"          // ObStorageLogType

#include "ob_log_trans_log.h"                     // RedoLogList
#include "ob_log_task_pool.h"                     // ObLogTransTaskPool
#include "ob_log_utils.h"                         // _SEC_
#include "ob_log_common.h"                        // OB_INVALID_PROGRESS
#include "ob_log_part_trans_dispatcher.h"         // PartTransDispatcher

namespace oceanbase
{
namespace liboblog
{

struct TransStatInfo;
class IObLogFetcherDispatcher;
class IObLogClusterIDFilter;
struct PartServeInfo;

// Partitioned transaction parser
//
// Parses partitioned log streams into partitioned transactions and outputs them in order
// 1. single threaded data production only, multi-threaded dispatch() is supported
// 2. one partitioned transaction parser per partition
// 3. cannot produce data while calling the offline function, but dispatch() is supported
class IObLogPartTransResolver
{
public:
  // Test mode on or off
  static bool test_mode_on;
  static bool test_checkpoint_mode_on;
  static int64_t test_mode_ignore_redo_count;

  typedef common::ObSEArray<transaction::ObTransID, 16> ObTransIDArray;
  typedef common::ObSEArray<int64_t, 16> ObAggreLogIndexArray;
  // PG scenarios, as aggregated logs:
  // 1. it is possible to parse to an aggregated log and find multiple transactions with missing redo logs
  // 2. Missing log arrays are guaranteed to be ordered and de-duplicated: when multiple transactions have the same missing redo log, there may be duplicate log_id,
  struct ObLogMissingInfo
  {
    // array of missing log ids
    ObLogIdArray missing_log_ids_;
    // array of trans that contain missing log
    ObTransIDArray missing_trans_ids_;
    // 1. when parsing to prepre or sp_commit and finding a missing redo, read prepare or reda sp_commit again to advance the status after adding the missing redo log
    // 2. log_indexes_records the index (i.e. the number of entries) of the prepare/sp_commit in the PG aggregation log,
    //    ensuring that data from non-missing transactions is filtered out when read prepare/sp_commit is used to advance the status
    ObAggreLogIndexArray log_indexs_;

    ObLogMissingInfo();

    void reset()
    {
      missing_log_ids_.reset();
      missing_trans_ids_.reset();
      log_indexs_.reset();
    }

    int sort_and_unique_missing_log_ids();
    int64_t get_missing_log_count() const { return missing_log_ids_.count(); }

    int push_back_missing_log_id(const uint64_t log_id);
    int push_back_trans_id(const transaction::ObTransID &trans_id);
    int push_back_log_index(const int64_t log_index);

    TO_STRING_KV(K_(missing_log_ids),
        K_(missing_trans_ids),
        K_(log_indexs));
  };

public:
  virtual ~IObLogPartTransResolver() {}

public:
  virtual int init(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const int64_t start_global_trans_version) = 0;

public:
  /// Read log entries and parse logs into zone transactions
  ///
  /// @param [in] log_entry         Target log entry
  /// @param [out] missing          Missing info, where missing redo logs are guaranteed to be ordered and not duplicated
  /// @param [out] tsi              Transaction log parsing statistics
  /// @param [in] serve_info        Partition service information, used to determine if a partitioned transaction is served
  /// @param [out] log_type         The type of log to be parsed
  /// @param [in] need_filter_pg_no_missing_redo_trans
  ///             PG scenario: read missing log and then read prepare/sp_commit again needs to filter non-missing redo log transaction data
  /// @param [in] log_indexes       PG scenario, prepare/sp_commit log is at index of aggregated logs
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval OB_ITEM_NOT_SETTED    redo log is incomplete
  /// @retval Other error codes     Fail
  virtual int read(const clog::ObLogEntry &log_entry,
      ObLogMissingInfo &missing_info,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      storage::ObStorageLogType &log_type,
      const bool need_filter_pg_no_missing_redo_trans,
      const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs) = 0;

  /// Read missing mutator log entries/REDO log entries
  /// Supports aggregated logs, for PG aggregated logs.
  /// 1. only care about log_type, OB_LOG_MUTATOR, OB_LOG_TRANS_REDO, OB_LOG_SP_TRANS_REDO, OB_LOG_SP_TRANS_COMMIT that include redo data, other
  /// Log types are ignored
  /// 2. Filtering based on trans_id information: Synchronize over aggregated logs and only care about transaction data for missing logs
  ///
  /// @param log_entry                      Target log entry
  /// @param missing_log_trans_id_array     Array of missing log trans_id
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval Other error codes     Fail
  virtual int read_missing_redo(const clog::ObLogEntry &log_entry,
      const ObTransIDArray &missing_log_trans_id_array) = 0;

  /// 将READY的分区事务输出
  /// 支持多线程调用
  /// 注意：该操作可能会阻塞，直到dispatch成功
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval OB_IN_STOP_STATE      Exit
  /// @retval Other error codes     Fail
  virtual int dispatch(volatile bool &stop_flag, int64_t &pending_task_count) = 0;

  /// 分区下线，生成"分区下线"事务任务作为分区的最后一个任务输出，同时清理掉剩余所有任务
  /// 注意：该操作可能会阻塞
  ///
  /// @retval OB_SUCCESS            Success
  /// @retval OB_IN_STOP_STATE      Exit
  /// @retval Other error codes     Fail
  virtual int offline(volatile bool &stop_flag) = 0;

  /// 获取当前分区的tps信息
  virtual double get_tps() = 0;

  // 获取分派任务进度及其相关信息
  //
  // @retval OB_INVALID_PROGRESS  当前无任务待输出，进度无效
  // @retval 其他值               当前输出进度: 待输出的任务时间戳 - 1
  virtual int get_dispatch_progress(int64_t &progress, PartTransDispatchInfo &dispatch_info) = 0;

  // 下发心跳任务
  virtual int heartbeat(const common::ObPartitionKey &pkey, const int64_t hb_tstamp) = 0;
};

////////////////////////////////// ObLogPartTransResolver /////////////////////////////////

// 分区事务解析器
class ObLogPartTransResolver : public IObLogPartTransResolver
{
public:
  ObLogPartTransResolver(const char* pkey_str,
      TaskPool &task_pool,
      PartTransTaskMap &task_map,
      TransCommitMap &trans_commit_map,
      IObLogFetcherDispatcher &dispatcher,
      IObLogClusterIDFilter &cluster_id_filter);
  virtual ~ObLogPartTransResolver();

  static const int64_t DATA_OP_TIMEOUT = 10 * _SEC_;

public:
  virtual int init(const common::ObPartitionKey &pkey,
      const int64_t start_tstamp,
      const int64_t major_freeze_timestamp);
  virtual int read(const clog::ObLogEntry &log_entry,
      ObLogMissingInfo &missing_info,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      storage::ObStorageLogType &log_type,
      const bool need_filter_pg_no_missing_redo_trans,
      const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs);
  virtual int read_missing_redo(const clog::ObLogEntry &log_entry,
      const ObTransIDArray &missing_log_trans_id_array);
  virtual int dispatch(volatile bool &stop_flag, int64_t &pending_task_count);
  virtual int offline(volatile bool &stop_flag);
  virtual double get_tps() { return part_trans_dispatcher_.get_tps(); }
  virtual int get_dispatch_progress(int64_t &progress, PartTransDispatchInfo &dispatch_info)
  {
    return part_trans_dispatcher_.get_dispatch_progress(progress, dispatch_info);
  }
  virtual int heartbeat(const common::ObPartitionKey &pkey, const int64_t hb_tstamp)
  {
    return part_trans_dispatcher_.heartbeat(pkey, hb_tstamp);
  }

  TO_STRING_KV(K_(pkey),
      K_(offlined),
      K_(part_trans_dispatcher));
private:
  struct BatchCommitTransInfo
  {
    BatchCommitTransInfo() { reset(); }
    ~BatchCommitTransInfo() { reset(); }

    void reset()
    {
      is_sp_elr_trans_ = false;
      is_batch_commit_trans_ = false;
      trans_is_batch_committed_ = false;
    }

    void init_for_prepare_log(const bool is_batch_commit_trans, const bool trans_is_batch_committed)
    {
      is_sp_elr_trans_ = false;
      is_batch_commit_trans_ = is_batch_commit_trans;
      trans_is_batch_committed_ = trans_is_batch_committed;
    }

    void init_for_sp_elr_trans(const bool is_sp_elr_trans)
    {
      is_sp_elr_trans_ = is_sp_elr_trans;
      is_batch_commit_trans_ = false;
      trans_is_batch_committed_ = false;
    }

    TO_STRING_KV(K_(is_sp_elr_trans),
        K_(is_batch_commit_trans),
        K_(trans_is_batch_committed));

    bool is_sp_elr_trans_;
    bool is_batch_commit_trans_;
    bool trans_is_batch_committed_;
  };

  // 1. PG scenario: log_entry_index records the number of transaction logs in the aggregated log, starting from 0
  // 2. Non-PG scenario: log_entry_index=0
  int read_log_(const liboblog::ObLogEntryWrapper &log_entry,
      const int64_t log_entry_index,
      const int64_t begin_time,
      int64_t &pos,
      ObLogMissingInfo &missing,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      storage::ObStorageLogType &log_type);
  int read_missing_redo_(const liboblog::ObLogEntryWrapper &log_entry,
      const ObTransIDArray &missing_log_trans_id_array,
      int64_t &pos,
      TransStatInfo &tsi,
      storage::ObStorageLogType &log_type);
  int parse_and_read_aggre_log_(const clog::ObLogEntry &log_entry,
      const int64_t begin_time,
      ObLogMissingInfo &missing,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      const bool is_read_missing_log,
      const ObTransIDArray &missing_log_trans_id_array,
      const bool need_filter_pg_no_missing_redo_trans,
      const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs);
  // Any log_entry that is not located in log_indexes needs to be ignored
  int filter_pg_no_missing_redo_trans_(const ObAggreLogIndexArray &log_indexs,
      const int64_t cur_log_index,
      bool &if_filter);
  int obtain_task_(const PartTransID &part_trans_id, PartTransTask*& task);
  int decode_trans_log_header_(const clog::ObLogEntry& log_entry,
      int64_t &pos,
      bool &is_log_filtered,
      bool &is_log_aggre,
      bool &is_barrier_log,
      storage::ObStorageLogType &log_type);
  bool is_barrier_log_(const uint64_t log_id,
      const clog::ObLogEntry& log_entry);
  int read_mutator_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos,
      TransStatInfo &tsi,
      const ObTransIDArray &missing_log_trans_id_array,
      const bool is_missing_log = false);
  int read_mutator_abort_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos);
  int read_redo_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos,
      TransStatInfo &tsi,
      const ObTransIDArray &missing_log_trans_id_array,
      const bool is_missing_log = false);
  int parse_redo_log_(const int32_t log_offset,
      const transaction::ObTransMutator &log_mutator,
      const transaction::ObTransID &trans_id,
      const uint64_t log_id,
      const int64_t tstamp,
      const int64_t log_no,
      const bool is_sp_trans,
      const bool is_missing_log,
      const bool is_pg_aggre_log,
      const ObTransIDArray &missing_log_trans_id_array);
  // 1. Only PG read missing log scenarios are filtered based on trans_id
  // 2. All others are not filtered
  int filter_pg_log_based_on_trans_id_(const ObTransIDArray &missing_log_trans_id_array,
      const transaction::ObTransID &log_trans_id,
      const bool is_pg_missing_log,
      bool &is_filter);
  int read_prepare_(const liboblog::ObLogEntryWrapper &log_entry,
      const int64_t log_entry_index,
      ObLogMissingInfo &missing,
      int64_t &pos,
      const bool with_redo,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      bool &is_prepare_log_served);
  // 1. the prepare log timestamp can be passed in
  // 2. the commit log timestamp can be passed in (backup recovery scenario)
  int check_part_trans_served_(const int64_t trans_log_tstamp,
      const uint64_t log_id,
      const uint64_t cluster_id,
      const PartServeInfo &serve_info,
      bool &is_served);
  int handle_when_part_trans_not_served_(const PartTransID &part_trans_id,
      const uint64_t log_id,
      const int64_t tstamp);
  int parse_prepare_log_(const transaction::ObTransID &trans_id,
      const transaction::ObRedoLogIdArray &all_redos,
      const bool with_redo,
      const uint64_t cluster_id,
      const common::ObVersion freeze_version,
      const uint64_t log_id,
      const int64_t tstamp,
      ObLogMissingInfo &missing_info,
      const int64_t log_entry_index,
      const bool is_sp_trans,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const PartServeInfo &serve_info,
      bool &is_prepare_log_served,
      const BatchCommitTransInfo &batch_commit_trans_info,
      const transaction::ObElrTransInfoArray &elt_trans_info_array,
      const int64_t checkpoint,
      const int64_t commit_version = common::OB_INVALID_VERSION,
      const transaction::PartitionLogInfoArray *prepare_log_info = NULL);
  // Currently, partitioned transactions are advanced in the following ways.
  // 1. normal transactions are advanced by themselves, through commit logs, such as redo, prepare, commit or sp_redo, sp_commit
  // 2. rely on the checkpoint log for advancement, when no transaction occurs on the partition, a checkpoint log is written at 50ms (read timestamp on the standby)
  // 3. rely on checkpoint information carried by subsequent prepare or sp_commit logs to advance previous transactions
  // 4. rely on the clog batch commit flag
  // For example, for a single multipartition transaction with only redo, prepare logs, should advance through 2,3,4
  int prepare_normal_trans_task_(PartTransTask &task,
      const int64_t tstamp,
      const transaction::ObTransID &trans_id,
      const uint64_t log_id,
      const uint64_t cluster_id,
      const common::ObVersion freeze_version,
      const common::ObString &trace_id,
      const common::ObString &trace_info,
      const BatchCommitTransInfo &batch_commit_trans_info,
      const transaction::ObElrTransInfoArray &elt_trans_info_array,
      const int64_t commit_version,
      const int64_t checkpoint,
      const transaction::PartitionLogInfoArray *prepare_log_info);
  int check_redo_log_list_(
      const transaction::ObRedoLogIdArray& all_redos,
      const uint64_t prepare_log_id,
      const SortedRedoLogList &sorted_redos,
      const bool prepare_with_redo,
      ObLogMissingInfo &missing);
  //  Read commit/abort logs.
  int read_commit_(const liboblog::ObLogEntryWrapper &log_entry,
      const PartServeInfo &serve_info,
      int64_t &pos,
      bool with_prepare,
      TransStatInfo &tsi);
  int commit_task_(PartTransTask &task,
      const int64_t global_trans_version,
      const transaction::PartitionLogInfoArray &prepare_log_info,
      const uint64_t log_id,
      const int64_t tstamp,
      const transaction::ObTransID &trans_id,
      const bool commit_log_with_prepare,
      const bool is_ready_to_commit);
  // 1. 2pc commit日志一定提交成功, 非预提交, is_ready_to_commit=false
  // 2. 单分区事务 sp_commit日志一定提交成功, 非预提交, is_ready_to_commit=false
  // 3. 提前解行锁场景，单分区sp_commit预提交, is_ready_to_commit=true  // 1. 2pc commit log must commit, not pre-commit, is_ready_to_commit=false
  // 2. single-partition transaction sp_commit log must commit, non-pre-commit, is_ready_to_commit=false
  // 3. early unlock (row lock) scenario, single-partition sp_commit pre-commit, is_ready_to_commit=true
  int parse_commit_log_(const bool is_ready_to_commit,
      const bool with_prepare,
      const transaction::PartitionLogInfoArray &prepare_log_info,
      const transaction::ObTransID &trans_id,
      const int64_t global_trans_version,
      const uint64_t log_id,
      const int64_t tstamp,
      const uint64_t cluster_id,
      const bool is_sp_trans,
      const PartServeInfo &serve_info);
  int read_abort_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos);
  int parse_abort_log_(const transaction::ObTransID &trans_id,
      const uint64_t log_id,
      const int64_t tstamp);
  // Read unknown type log.
  int read_unknown_log_(const clog::ObLogEntry &log_entry);
  int read_sp_trans_redo_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos,
      TransStatInfo &tsi,
      const ObTransIDArray &missing_log_trans_id_array,
      const bool is_missing_log = false);
  int read_sp_trans_commit_(const liboblog::ObLogEntryWrapper &log_entry,
      const int64_t log_entry_index,
      ObLogMissingInfo &missing,
      int64_t &pos,
      TransStatInfo &tsi,
      const PartServeInfo &serve_info,
      const bool is_sp_elr_trans = false);
  int deserialize_sp_commit_and_parse_redo_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos,
      transaction::ObSpTransCommitLog &commit_log,
      int64_t &after_decode_time,
      bool &with_redo,
      const ObTransIDArray &missing_log_trans_id_array,
      const bool is_missing_log = false);
  int read_sp_trans_abort_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos);
  int read_checkpoint_log_(const liboblog::ObLogEntryWrapper &log_entry,
      int64_t &pos);
  // Handle the compatibility issue of the last redo log being empty
  bool is_last_redo_with_empty_content_(const transaction::ObTransRedoLog &redo_log) const;

  ////////////////////////////////////////////////////////////////////////
  // DML-ObLogEntryTask related implementations
  int dispatch_log_entry_task_(ObLogEntryTask *log_entry_task);

private:
  bool                      offlined_ CACHE_ALIGNED;     // Is the partition deleted
  common::ObPartitionKey    pkey_;
  // Record the log timestamp of the first log synced to the partition, to determine whether the predecessor transaction commits in the early unlock scenario
  // Consider that the predecessor transaction of T2 is T1 and that T1 was last aborted, when the liboblog start time loci fall exactly between T1 and T2 and only T2's data can be synchronized.
  // Parse to T2 whose predecessor transaction is T1, query T1's partitioned transaction context and abort transaction table will not exist, at this point think T1 commit, then it is wrong to determine the status in this way.
  // Option.
  // Record the timestamp of the partition sync to the first log, if T1_commit version > first_log_ts_, then you can follow the above process
  // otherwise rely on checkpoint or abort logs to advance the status of T2
  int64_t                   first_log_ts_;
  PartTransDispatcher       part_trans_dispatcher_;
  IObLogClusterIDFilter     &cluster_id_filter_;
  int64_t                   start_global_trans_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogPartTransResolver);
};

}
}

#endif
