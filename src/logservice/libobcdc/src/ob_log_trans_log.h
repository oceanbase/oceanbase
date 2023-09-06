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
 *
 * Transaction Log Structure
 */

#ifndef OCEANBASE_LIBOBCDC_TRANS_LOG_H__
#define OCEANBASE_LIBOBCDC_TRANS_LOG_H__

#include "storage/tx/ob_trans_log.h"       // ObTransPrepareLog, ObTransCommitLog
#include "lib/queue/ob_link.h"             // ObLink
#include "lib/allocator/ob_mod_define.h"
#include "ob_cdc_lightly_sorted_list.h"    // SortedLightyList
#include "ob_log_utils.h"                  // ObLogLSNArray

namespace oceanbase
{
namespace libobcdc
{
class RedoLogMetaNode;

/// LogEntryNode, to identify a palf::LogEntry
/// should hold redo log recoreded in corresponding LogEntry by redo_head_ and redo_tail_ pointer
class LogEntryNode
{
public:
  LogEntryNode(const palf::LSN &lsn) : lsn_(lsn), redo_head_(NULL), redo_tail_(NULL), next_(NULL) {}
  ~LogEntryNode() { reset(); }
protected:
  LogEntryNode() { reset(); }
public:
  void reset()
  {
    lsn_.reset();
    redo_head_ = NULL;
    redo_tail_ = NULL;
    next_ = NULL;
  }
  bool is_valid() const
  {
    return lsn_.is_valid();
  }
public:
  bool operator==(const LogEntryNode &other) const
  { return lsn_ == other.get_lsn(); }
  bool operator!=(const LogEntryNode &other) const
  { return lsn_ != other.get_lsn(); }
  bool operator<(const LogEntryNode &other) const
  { return lsn_ < other.get_lsn(); }
public:
  const palf::LSN &get_lsn() const { return lsn_; }
  void set_next(LogEntryNode *next_node) { next_ = next_node; }
  LogEntryNode *get_next() const { return next_; }
  /// append RedoLog(get by part_trans_resolver) into current LogEntryNode
  /// only support append operation, cause redo in the same palf::LogEntry should be handled by sequence
  int append_redo_node(RedoLogMetaNode *redo_node);
  TO_STRING_KV(K_(lsn), KP_(next));
private:
  palf::LSN       lsn_;           // lsn of corresponding palf::LogEntry
  RedoLogMetaNode *redo_head_;    // first redo recorded in LogEntry
  RedoLogMetaNode *redo_tail_;    // last redo recoreded in LogEntry
  LogEntryNode    *next_;         // next LogEntry(for the same PartTransTask)
};

typedef SortedLightyList<LogEntryNode> SortedLogEntryArray;
class SortedLogEntryInfo
{
public:
  SortedLogEntryInfo() :
      last_fetched_redo_log_entry_(NULL),
      fetched_log_entry_arr_(true), /*is_unique*/
      recorded_lsn_arr_() {}
  ~SortedLogEntryInfo() { reset(); }
  void reset()
  {
    last_fetched_redo_log_entry_ = NULL;
    fetched_log_entry_arr_.reset_data();
    recorded_lsn_arr_.reset();
  }

public:
  int push_fetched_log_entry_node(LogEntryNode *log_entry_node);
  int push_recorded_log_entry(const palf::LSN &lsn);
  int push_fetched_redo_node(RedoLogMetaNode *redo_log_meta);
  // invoke by CommitLog to verify all log fetched
  // 1. check log count
  // 2. check first and last log_entry(TODO)
  // NOTE:
  // 1. currently not check each log_lsn between fetched_log_entry_arr and recorded_log_entry_arr
  // 2. active_info_log must before commit_info log, thus won't affect fetched_log_entry_arr recorded_log_entry_arr.
  //
  // note: RollbackTo is treated as Redo.
  int is_all_log_entry_fetched(bool &is_all_redo_fetched);

  SortedLogEntryArray &get_fetched_log_entry_node_arr() { return fetched_log_entry_arr_; }

  TO_STRING_KV(
      "fetched_log_entry_count", fetched_log_entry_arr_.count(),
      "recorded_lsn_count", recorded_lsn_arr_.count(),
      K_(fetched_log_entry_arr),
      K_(recorded_lsn_arr));
private:
  LogEntryNode *last_fetched_redo_log_entry_;
  // hold all fetched log_entry_info.(include lsn of log_entry which contains redo_log and rollback_to log)
  SortedLogEntryArray fetched_log_entry_arr_;
  // hold all prev_redo_log_lsn in all TxLog:
  // 1. prev_redo_lsn_arr recorded by RecordLog/CommitInfoLog
  // 2. lsn of commit_info_log_entry that contains redo_log.
  ObLogLSNArray recorded_lsn_arr_;
};

// Redo log structure
// To support LOB, make sure that the mutator data is complete and that it contains one to many REDO logs
class RedoLogMetaNode
{
public:
  RedoLogMetaNode() { reset(); }
  ~RedoLogMetaNode() { reset(); }

  void reset();
  void reset(const palf::LSN &log_lsn);
  void reset(const palf::LSN &log_lsn, char *data, const int64_t size,
      const int64_t pos);

  /// Check the validity of RedoLogMetaNode, we should check the validity of data when need_check_data is true
  /// 1. For DDL node, need_check_data is always true
  /// 2. For DML node:
  ///    (1) If data is in memory(eg memory working mode), need_check_data is true
  ///    (1) If data is in stored(eg storage working mode), when data was read successfully through Reader,
  ///        need_check_data is true; Otherwise is false
  ///
  /// @param [in] need_check_data  Whether to check the validity of data
  //
  /// @retval true   valid
  /// @retval false  invalid
  bool is_valid(const bool need_check_data) const;

public:
  int32_t get_log_num() const { return 1; }

  const palf::LSN &get_start_log_lsn() const { return start_log_lsn_; }
  void set_host_logentry_node(LogEntryNode *log_entry_node) { host_log_entry_ = log_entry_node; };
  LogEntryNode *get_host_logentry_node() { return host_log_entry_; }
  // Is the log ID sequentially located before the target node
  bool before(const RedoLogMetaNode &node) { return start_log_lsn_ < node.start_log_lsn_; }

  void set_data(char *data, int64_t data_len)
  {
    data_ = data;
    size_ = data_len;
    pos_ = data_len;
  }
  const char *get_data() const { return data_; }
  char *get_data() { return data_; }
  void set_data_len(const int64_t size) { size_ = size;}
  int64_t get_data_len() const { return size_; }
  // Requires data to be valid and complete
  bool check_data_integrity() const;

  RedoLogMetaNode *get_next() { return next_; }
  RedoLogMetaNode *&get_next_ptr() { return next_; }
  void set_next(RedoLogMetaNode *next) { next_ = next; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  // TODO use start_log_entry and end_log_entry instead of start_log_lsn/no and end_log_lsn/no to support LOB
  // currently only support one log_entry to one or multi redo, may change according LOB after LOB is implied in OB Server
  LogEntryNode *host_log_entry_;
  // LOG LSN, one log_entry, which only contains only one ObTxRedoLog,  corresponding to one LSN
  palf::LSN   start_log_lsn_;      // start log lsn

  // Mutator data, which may contain the contents of multiple REDO logs
  // This holds the serialized data directly from the ObMemtableMutatorRow
  // Excluding the ObMemtableMutatorMeta part
  // See PartTransTask::push_redo_log() for details
  //
  // Note: If the data is stored, this value is not valid
  // data_ = NULL; size_ = 0; pos_ = 0;
  char        *data_;     // Mutator data
  int64_t     size_;      // Length of Mutator data
  int64_t     pos_;       // Length of data already filled

  RedoLogMetaNode *next_;         // next log
};

class DmlRedoLogNode : public RedoLogMetaNode
{
public:
  DmlRedoLogNode() { reset(); }
  ~DmlRedoLogNode() { reset(); }
  void reset();

  // Data is stored
  void init_for_data_persistence(
      const palf::LSN &log_lsn,
      const int64_t size);
  // Data is in memory
  void init_for_data_memory(
      const palf::LSN &log_lsn,
      char *data,
      const int64_t size,
      const int64_t pos);
  bool is_valid() const;

public:
  const palf::LSN &get_store_log_lsn() const { return RedoLogMetaNode::get_start_log_lsn(); }

  ObLink *get_row_head() { return row_head_; }
  const ObLink *get_row_head() const { return row_head_; }
  void set_row_head(ObLink *row_head) { row_head_ = row_head; }

  ObLink *get_row_tail() { return row_tail_; }
  const ObLink *get_row_tail() const { return row_tail_; }
  void set_row_tail(ObLink *row_tail) { row_tail_ = row_tail; }

  void inc_valid_row_num() { ATOMIC_INC(&valid_row_num_); }
  void set_valid_row_num(const int64_t row_num) { ATOMIC_SET(&valid_row_num_, row_num); }
  int64_t get_valid_row_num() const { return ATOMIC_LOAD(&valid_row_num_); }
  bool is_contain_valid_row() const { return get_valid_row_num() > 0; }

  // Retrieve the last digit of reserve_field_
  bool is_stored() const { return reserve_field_ & 0x01; }
  int set_data_info(char *data, int64_t data_len);
  bool is_readed() const { return ATOMIC_LOAD(&is_readed_); }
  void set_readed() { ATOMIC_SET(&is_readed_, true); }

  bool is_parsed() const { return ATOMIC_LOAD(&is_parsed_); }
  void set_parsed() { ATOMIC_SET(&is_parsed_, true); }

  bool is_formatted() const { return ATOMIC_LOAD(&is_formatted_); }
  void set_formatted() { ATOMIC_SET(&is_formatted_, true); }

  TO_STRING_KV(
      "RedoLog", static_cast<const RedoLogMetaNode &>(*this),
      K_(valid_row_num),
      K_(is_readed),
      K_(is_parsed),
      K_(is_formatted),
      "row_head", uint64_t(row_head_),
      "row_tail", uint64_t(row_tail_),
      K_(reserve_field));

private:
  // Set the last digit of reserve_field_ to 1
  void set_stored_() { reserve_field_ |= 0x01; }

private:
  // Reader read the data, set is_readed_ is true
  bool     is_readed_;

  // Formatter callback
  ObLink  *row_head_;
  ObLink  *row_tail_;
  int64_t  valid_row_num_;
  bool     is_parsed_;
  bool     is_formatted_;

  // An 8 bit reserved field:
  // The lowest bit represents the data is stored
  // The second low bit represents the data is parsed when contain rollback to savepoint
  int8_t   reserve_field_;           // reserved field
};

class DdlRedoLogNode : public RedoLogMetaNode
{
public:
  DdlRedoLogNode() { reset(); }
  ~DdlRedoLogNode() { reset(); }

  bool is_valid() const
  {
    // DDL data is always in memory
    return RedoLogMetaNode::is_valid(true/*need_check_data*/);
  }

  TO_STRING_KV("RedoLog", static_cast<const RedoLogMetaNode &>(*this));
};

class RedoSortedProgress
{
public:
  RedoSortedProgress() { reset(); }
  ~RedoSortedProgress() { reset(); }
public:
  void reset()
  {
    ATOMIC_SET(&dispatched_redo_count_, 0);
    ATOMIC_SET(&sorted_redo_count_, 0);
    sorted_row_seq_no_.atomic_reset();
  }
  void reset_for_sys_ls_dml_trans(const int64_t redo_node_count)
  {
    ATOMIC_SET(&dispatched_redo_count_, redo_node_count);
    ATOMIC_SET(&sorted_redo_count_, redo_node_count);
    sorted_row_seq_no_.atomic_reset();
  }
  OB_INLINE int64_t get_dispatched_redo_count() const { return ATOMIC_LOAD(&dispatched_redo_count_); }
  OB_INLINE void inc_dispatched_redo_count() { ATOMIC_INC(&dispatched_redo_count_); }
  OB_INLINE int64_t get_sorted_redo_count() const { return ATOMIC_LOAD(&sorted_redo_count_); }
  OB_INLINE void inc_sorted_redo_count() { ATOMIC_INC(&sorted_redo_count_); }
  OB_INLINE transaction::ObTxSEQ get_sorted_row_seq_no() const { return sorted_row_seq_no_.atomic_load(); }
  void set_sorted_row_seq_no(const transaction::ObTxSEQ &row_seq_no);
  OB_INLINE int64_t get_dispatched_not_sort_redo_count() const
  { return ATOMIC_LOAD(&dispatched_redo_count_) - ATOMIC_LOAD(&sorted_redo_count_); }
public:
  TO_STRING_KV(
      K_(dispatched_redo_count),
      K_(sorted_redo_count),
      K_(sorted_row_seq_no));
private:
  int64_t dispatched_redo_count_;
  int64_t sorted_redo_count_;
  transaction::ObTxSEQ sorted_row_seq_no_;
};

class IStmtTask;
class DmlStmtTask;
// Ordered Redo log list
struct SortedRedoLogList
{
  int32_t     node_num_;
  // When the data of node need be stored, than need callback to increase ready_node_num
  // Otherwise, we can increase ready_node_num directly
  int32_t     ready_node_num_;
  int32_t     log_num_;
  bool        is_dml_stmt_iter_end_;

  RedoLogMetaNode *head_;
  RedoLogMetaNode *tail_;
  RedoLogMetaNode *last_push_node_;
  RedoLogMetaNode *cur_dispatch_redo_;
  RedoLogMetaNode *cur_sort_redo_;
  ObLink          *cur_sort_stmt_;
  RedoSortedProgress sorted_progress_;

  SortedRedoLogList() :
      node_num_(0),
      ready_node_num_(0),
      log_num_(0),
      is_dml_stmt_iter_end_(false),
      head_(NULL),
      tail_(NULL),
      last_push_node_(NULL),
      cur_dispatch_redo_(NULL),
      cur_sort_redo_(NULL),
      cur_sort_stmt_(NULL),
      sorted_progress_()
  {}

  ~SortedRedoLogList() { reset(); }

  int32_t get_node_number() const { return ATOMIC_LOAD(&node_num_); }
  int32_t get_ready_node_number() const { return ATOMIC_LOAD(&ready_node_num_); }
  void inc_ready_node_num() { ATOMIC_INC(&ready_node_num_); }
  int check_node_num_equality(bool &is_equal);

  void reset()
  {
    node_num_ = 0;
    ready_node_num_ = 0;
    log_num_ = 0;
    is_dml_stmt_iter_end_ = false;
    head_ = NULL;
    tail_ = NULL;
    last_push_node_ = NULL;
    cur_dispatch_redo_ = NULL;
    cur_sort_redo_ = NULL;
    cur_sort_stmt_ = NULL;
    sorted_progress_.reset();
  }

  bool is_valid() const
  {
    return node_num_ > 0
        && log_num_ > 0
        && NULL != head_
        && NULL != tail_
        && NULL != last_push_node_;
  }

  OB_INLINE bool is_dispatch_finish() const { return node_num_ == sorted_progress_.get_dispatched_redo_count(); }

  OB_INLINE bool has_dispatched_but_unsorted_redo() const
  { return sorted_progress_.get_dispatched_not_sort_redo_count() > 0; }

  OB_INLINE void set_sorted_row_seq_no(const transaction::ObTxSEQ &row_seq_no) { sorted_progress_.set_sorted_row_seq_no(row_seq_no); }

  bool is_dml_stmt_iter_end() const { return is_dml_stmt_iter_end_; }

  /// Push RedoLogMetaNode with order
  ///
  /// @param [in] is_data_in_memory data is in memory
  /// @param [in] node
  //
  /// @retval OB_SUCCESS          Success
  /// @retval OB_ENTRY_EXIST      the redo log already exists
  /// @retval Other error codes   Fail
  int push(const bool is_data_in_memory,
      RedoLogMetaNode *node);

  // init variables for sorter(iterator of current sorted redo and stmt)
  // and dispatcher(iterator of current disptched redo)
  void init_iterator();

  /// get next dml redo to dispatch(by redo_dispatcher)
  /// note: must enusre init_iterator() called before this function
  /// note: not support concurrent call to this function
  /// note: if is_last_redo is true, dispatcher can't access PartTransTask because trans may be recycled any time
  /// @param dml_redo_node    [out] dml_redo_node to dispatch
  /// @param is_last_redo     [out] is last redo node in current PartTransTask
  /// @retval OB_SUCCESS      succ get redo_node
  /// @retval OB_EMPTY_RESULT no more redo to dispatch:
  ///                           (1) no redo in this part_trans_task or
  ///                           (2) all redo dispatched then call this functin again(should not happen)
  /// @retval other_code      unexpected error
  int next_dml_redo(RedoLogMetaNode *&dml_redo_meta, bool &is_last_redo);

  /// get next dml statement which with valid binlog
  /// note: must enusre init_iterator() called before this function
  /// @param [out] dml_stmt_task dml_stmt_task with valid br
  /// @retval OB_SUCCESS          Success
  /// @retval OB_ITER_END         all stmt are output:
  //                                (1) part_trans_task has no valid br at all or
  ///                               (2) all br are outputed to sorter(the caller)
  /// @retval OB_NEED_RETRY       redo not formatted, retry later
  /// @retval Other error codes   Fail
  int next_dml_stmt(ObLink *&dml_stmt_task);

  TO_STRING_KV(
      K_(node_num),
      K_(log_num),
      K_(ready_node_num),
      KP_(head),
      KP_(tail),
      KP_(last_push_node),
      "redo_sorted_progress", sorted_progress_,
      KP_(cur_dispatch_redo),
      KP_(cur_sort_redo),
      "cur_sort_redo", static_cast<DmlRedoLogNode*>(cur_sort_redo_),
      KP_(cur_sort_stmt),
      K_(is_dml_stmt_iter_end));

  void mark_sys_ls_dml_trans_dispatched()
  {
    cur_dispatch_redo_ = NULL;
    cur_sort_redo_ = NULL;
    cur_sort_stmt_ = NULL;
    sorted_progress_.reset_for_sys_ls_dml_trans(node_num_);
  }

};
} // namespace libobcdc
} // namespace oceanbase

#endif /* OCEANBASE_LIBOBCDC_TRANS_LOG_H__ */
