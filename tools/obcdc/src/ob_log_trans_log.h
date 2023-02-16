/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBLOG_TRANS_LOG_H__
#define OCEANBASE_LIBOBLOG_TRANS_LOG_H__

#include "storage/transaction/ob_trans_log.h"       // ObTransPrepareLog, ObTransCommitLog
#include "lib/queue/ob_link.h"                      // ObLink
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace liboblog
{
// Redo log structure
// To support LOB, make sure that the mutator data is complete and that it contains one to many REDO logs
// log_no and log_id are stored as ranges
class RedoLogMetaNode
{
public:
  RedoLogMetaNode() { reset(); }
  ~RedoLogMetaNode() { reset(); }

  void reset();
  // For big row, if the data is stored, we should call update_redo_meta to update logId/logNo meta
  int update_redo_meta(const int64_t log_no, const uint64_t log_id);
  void reset(const int64_t log_no, const uint64_t log_id);
  void reset(const int64_t log_no, const uint64_t log_id, char *data, const int64_t size,
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
  int32_t get_log_num() const { return static_cast<int32_t>(end_log_no_ - start_log_no_ + 1); }
  int64_t get_start_log_no() const { return start_log_no_; }

  uint64_t get_start_log_id() const { return start_log_id_; }
  // Is the log ID sequentially located before the target node
  bool before(const RedoLogMetaNode &node) { return end_log_id_ < node.start_log_id_; }

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
  // Continue to add redo log data, requiring consecutive log numbers and enough buffer space
  int append_redo_log(const int64_t log_no,
      const uint64_t log_id,
      const char *buf,
      const int64_t buf_len);

  RedoLogMetaNode *get_next() { return next_; }
  RedoLogMetaNode *&get_next_ptr() { return next_; }
  void set_next(RedoLogMetaNode *next) { next_ = next; }

  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  // The log numbering interval
  // The numbers in the interval are consecutive
  // For example, logs numbered 2, 3 and 4, with 2 being the start number and 4 being the end number
  int64_t     start_log_no_;      // start log no
  int64_t     end_log_no_;        // end log no

  // Log ID interval
  // The IDs in the interval are not consecutive
  // e.g. log IDs: 100, 120, 200, 100 is the starting log ID, 200 is the ending log ID
  uint64_t    start_log_id_;      // start log id
  uint64_t    end_log_id_;        // end log id

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
  void init_for_data_persistence(const int64_t log_no,
      const uint64_t log_id,
      const int32_t log_offset,
      const int64_t size);
  // Data is in memory
  void init_for_data_memory(const int64_t log_no,
      const uint64_t log_id,
      const int32_t log_offset,
      char *data,
      const int64_t size,
      const int64_t pos);
  bool is_valid() const;

public:
  uint64_t get_store_log_id() const { return RedoLogMetaNode::get_start_log_id(); }
  int32_t get_log_offset() const { return log_offset_; }

  ObLink *get_row_head() { return row_head_; }
  const ObLink *get_row_head() const { return row_head_; }
  void set_row_head(ObLink *row_head) { row_head_ = row_head; }

  ObLink *get_row_tail() { return row_tail_; }
  const ObLink *get_row_tail() const { return row_tail_; }
  void set_row_tail(ObLink *row_tail) { row_tail_ = row_tail; }

  void inc_valid_row_num() { ++valid_row_num_; }
  void set_valid_row_num(const int64_t row_num) { valid_row_num_ = row_num; }
  int64_t get_valid_row_num() const { return valid_row_num_; }
  bool is_contain_valid_row() const { return 0 != valid_row_num_; }

  // Retrieve the last digit of reserve_field_
  bool is_stored() const { return reserve_field_ & 0x01; }
  int set_data_info(char *data, int64_t data_len);
  bool is_readed() const { return ATOMIC_LOAD(&is_readed_); }
  void set_readed() { ATOMIC_SET(&is_readed_, true); }

  bool is_parsed() const { return ATOMIC_LOAD(&is_parsed_); }
  void set_parsed() { ATOMIC_SET(&is_parsed_, true); }

  bool is_formatted() const { return ATOMIC_LOAD(&is_formatted_); }
  void set_formatted() { ATOMIC_SET(&is_formatted_, true); }

  TO_STRING_KV("RedoLog", static_cast<const RedoLogMetaNode &>(*this),
      K_(log_offset),
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
  // For not aggre log: log_offset_=0
  // For aggre log: log_offset_ record offset
  int32_t log_offset_;

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
    ATOMIC_SET(&sorted_row_seq_no_, 0);
  }
  OB_INLINE int64_t get_dispatched_redo_count() const { return ATOMIC_LOAD(&dispatched_redo_count_); }
  OB_INLINE void inc_dispatched_redo_count() { ATOMIC_INC(&dispatched_redo_count_); }
  OB_INLINE int64_t get_sorted_redo_count() const { return ATOMIC_LOAD(&sorted_redo_count_); }
  OB_INLINE void inc_sorted_redo_count() { ATOMIC_INC(&sorted_redo_count_); }
  OB_INLINE int64_t get_sorted_row_seq_no() const { return ATOMIC_LOAD(&sorted_row_seq_no_); }
  void set_sorted_row_seq_no(const int64_t row_seq_no);
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
  int64_t sorted_row_seq_no_;
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
  int32_t     total_log_size_;
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

  OB_INLINE void set_sorted_row_seq_no(const int64_t row_seq_no) { sorted_progress_.set_sorted_row_seq_no(row_seq_no); }

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
};

} // namespace liboblog
} // namespace oceanbase

#endif /* OCEANBASE_LIBOBLOG_TRANS_LOG_H__ */
