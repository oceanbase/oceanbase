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

#ifndef OCEANBASE_LIBOBLOG_TRANS_LOG_H__
#define OCEANBASE_LIBOBLOG_TRANS_LOG_H__

#include "storage/transaction/ob_trans_log.h"       // ObTransPrepareLog, ObTransCommitLog
#include "lib/allocator/ob_mod_define.h"
#include "ob_log_row_data_index.h"

namespace oceanbase
{
namespace liboblog
{
struct RedoLogMetaNode
{
  RedoLogMetaNode() { reset(); }
  ~RedoLogMetaNode() { reset(); }

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

  RedoLogMetaNode *next_;             // next log

  void reset()
  {
    start_log_no_ = -1;
    end_log_no_ = -1;
    start_log_id_ = common::OB_INVALID_ID;
    end_log_id_ = common::OB_INVALID_ID;
    next_ = NULL;
  }

  void reset(const int64_t log_no, const uint64_t log_id)
  {
    start_log_no_ = log_no;
    end_log_no_ = log_no;
    start_log_id_ = log_id;
    end_log_id_ = log_id;
    next_ = NULL;
  }

  bool is_valid() const
  {
    return start_log_no_ >= 0
        && end_log_no_ >= start_log_no_
        && common::OB_INVALID_ID != start_log_id_
        && end_log_id_  >= start_log_id_;
  }

  int32_t get_log_num() const { return static_cast<int32_t>(end_log_no_ - start_log_no_ + 1); }

  // Is the log ID sequentially located before the target node
  bool before(const RedoLogMetaNode &node)
  {
    return end_log_id_ < node.start_log_id_;
  }

  int update_redo_meta(const int64_t log_no, const uint64_t log_id);

  // TODO to_string
  TO_STRING_KV(
      K_(start_log_no),
      K_(end_log_no),
      K_(start_log_id),
      K_(end_log_id),
      KP_(next));
};

struct DmlRedoLogMetaNode : public RedoLogMetaNode
{
  DmlRedoLogMetaNode() { reset(); }
  ~DmlRedoLogMetaNode() { reset(); }

  ObLogRowDataIndex *get_row_head() { return row_head_; }
  const ObLogRowDataIndex *get_row_head() const { return row_head_; }
  ObLogRowDataIndex *get_row_tail() { return row_tail_; }
  const ObLogRowDataIndex *get_row_tail() const { return row_tail_; }
  int64_t get_valid_row_num() const { return valid_row_num_; }
  bool is_contain_valid_row() const { return 0 != valid_row_num_; }
  bool is_contain_rollback_row() const { return is_contain_rollback_row_; }

  void reset()
  {
    RedoLogMetaNode::reset();
    row_head_ = NULL;
    row_tail_ = NULL;
    valid_row_num_ = 0;
    is_contain_rollback_row_ = false;
  }

  void reset(const int64_t log_no, const uint64_t log_id)
  {
    reset();
    RedoLogMetaNode::reset(log_no, log_id);
  }

  ObLogRowDataIndex *row_head_;
  ObLogRowDataIndex *row_tail_;
  int64_t           valid_row_num_;
  bool              is_contain_rollback_row_;

  TO_STRING_KV("RedoLogMetaNode", static_cast<const RedoLogMetaNode &>(*this),
      K_(row_head),
      K_(row_tail),
      K_(valid_row_num),
      K_(is_contain_rollback_row));
};

struct DmlRedoLogNode
{
  DmlRedoLogNode() { reset(); }
  ~DmlRedoLogNode() { reset(); }

  // Mutator data, which may contain the contents of multiple REDO logs
  // This holds the serialized data directly from the ObMemtableMutatorRow
  // Excluding the ObMemtableMutatorMeta part
  // See PartTransTask::push_redo_log() for details
  char        *data_;     // Mutator data
  int64_t     size_;      // Length of Mutator data
  int64_t     pos_;       // Length of data already filled

  bool is_valid() const
  {
    return NULL != data_
        && size_ > 0
        && pos_ > 0;
  }

  // Requires data to be valid and complete
  bool check_data_integrity() const
  {
    return NULL != data_ && size_ > 0 && size_ == pos_;
  }

  void reset()
  {
    data_ = NULL;
    size_ = 0;
    pos_ = 0;
  }

  void reset(char *data, const int64_t size, const int64_t pos)
  {
    data_ = data;
    size_ = size;
    pos_ = pos;
  }

  // Continue to add redo log data, requiring consecutive log numbers and enough buffer space
  int append_redo_log(const char *buf, const int64_t buf_len);

  TO_STRING_KV(
      KP_(data),
      K_(size),
      K_(pos));
};

// Redo log structure
// To support LOB, make sure that the mutator data is complete and that it contains one to many REDO logs
// log_no and log_id are stored as ranges
struct DdlRedoLogNode : public RedoLogMetaNode
{
  DdlRedoLogNode() { reset(); }
  ~DdlRedoLogNode() { reset(); }

  // Mutator data, which may contain the contents of multiple REDO logs
  // This holds the serialized data directly from the ObMemtableMutatorRow
  // Excluding the ObMemtableMutatorMeta part
  // See PartTransTask::push_redo_log() for details
  char        *data_;     // Mutator数据
  int64_t     size_;      // Mutator数据长度
  int64_t     pos_;       // 已经填充的数据长度

  bool is_valid() const
  {
    return RedoLogMetaNode::is_valid()
        && NULL != data_
        && size_ > 0
        && pos_ > 0;
  }

  // Requires data to be valid and complete
  bool check_data_integrity() const
  {
    return NULL != data_ && size_ > 0 && size_ == pos_;
  }

  void reset()
  {
    RedoLogMetaNode::reset();
    data_ = NULL;
    size_ = 0;
    pos_ = 0;
  }

  void reset(const int64_t log_no, const uint64_t log_id, char *data, const int64_t size,
      const int64_t pos)
  {
    RedoLogMetaNode::reset(log_no, log_id);
    data_ = data;
    size_ = size;
    pos_ = pos;
  }

  // Continue to add redo log data, requiring consecutive log numbers and enough buffer space
  int append_redo_log(const int64_t log_no, const uint64_t log_id, const char *buf,
      const int64_t buf_len);

  TO_STRING_KV(
      K_(start_log_no),
      K_(end_log_no),
      K_(start_log_id),
      K_(end_log_id),
      KP_(data),
      K_(size),
      K_(pos),
      KP_(next));
};

// Ordered Redo log list
struct SortedRedoLogList
{
  int32_t     node_num_;
  int32_t     log_num_;
  RedoLogMetaNode *head_;
  RedoLogMetaNode *tail_;
  RedoLogMetaNode *last_push_node_;

  SortedRedoLogList() : node_num_(0), log_num_(0), head_(NULL), tail_(NULL), last_push_node_(NULL)
  {}

  ~SortedRedoLogList() { reset(); }

  int32_t get_node_number() const { return ATOMIC_LOAD(&node_num_); }

  void reset()
  {
    node_num_ = 0;
    log_num_ = 0;
    head_ = NULL;
    tail_ = NULL;
    last_push_node_ = NULL;
  }

  bool is_valid() const
  {
    return node_num_ > 0 && log_num_ > 0 && NULL != head_ && NULL != tail_
        && NULL != last_push_node_;
  }

  // Returns OB_ENTRY_EXIST if the redo log already exists
  int push(RedoLogMetaNode *node);

  TO_STRING_KV(K_(node_num), K_(log_num), KP_(head), KP_(tail), KP_(last_push_node));
};

} // namespace liboblog
} // namespace oceanbase

#endif /* OCEANBASE_LIBOBLOG_TRANS_LOG_H__ */
