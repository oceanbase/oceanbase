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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_trans_log.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
void RedoLogMetaNode::reset()
{
  start_log_no_ = -1;
  end_log_no_ = -1;
  start_log_id_ = common::OB_INVALID_ID;
  end_log_id_ = common::OB_INVALID_ID;
  data_ = NULL;
  size_ = 0;
  pos_ = 0;
  next_ = NULL;
}

void RedoLogMetaNode::reset(const int64_t log_no, const uint64_t log_id)
{
  start_log_no_ = log_no;
  end_log_no_ = log_no;
  start_log_id_ = log_id;
  end_log_id_ = log_id;
  next_ = NULL;
}

void RedoLogMetaNode::reset(const int64_t log_no,
    const uint64_t log_id,
    char *data,
    const int64_t size,
    const int64_t pos)
{
  start_log_no_ = log_no;
  end_log_no_ = log_no;
  start_log_id_ = log_id;
  end_log_id_ = log_id;
  data_ = data;
  size_ = size;
  pos_ = pos;
  next_ = NULL;
}

bool RedoLogMetaNode::is_valid(const bool need_check_data) const
{
  bool bool_data_ret = false;

  if (need_check_data) {
    bool_data_ret = (NULL != data_ && size_ > 0 && pos_ > 0);
  } else {
    bool_data_ret = true;
  }

  return start_log_no_ >= 0
    && end_log_no_ >= start_log_no_
    && common::OB_INVALID_ID != start_log_id_
    && end_log_id_  >= start_log_id_
    && bool_data_ret;
}

bool RedoLogMetaNode::check_data_integrity() const
{
  return NULL != data_ && size_ > 0 && size_ == pos_;
}

int RedoLogMetaNode::update_redo_meta(const int64_t log_no, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_no < 0) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    LOG_ERROR("invalid argument", K(log_no), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid(false/*is_data_in_memory*/))) {
    LOG_ERROR("redo log node is not valid", KPC(this));
    ret = OB_INVALID_DATA;
  // Requires log numbers to be consecutive
  } else if (OB_UNLIKELY(log_no != (end_log_no_ + 1))) {
    LOG_ERROR("log no is not consecutive", K(end_log_no_), K(log_no));
    ret = OB_DISCONTINUOUS_LOG;
  } else {
    // update log range
    end_log_no_ = log_no;
    end_log_id_ = log_id;
  }

  return ret;
}

int RedoLogMetaNode::append_redo_log(const int64_t log_no,
    const uint64_t log_id,
    const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_no < 0) || OB_UNLIKELY(OB_INVALID_ID == log_id)
      || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(log_no), K(log_id), K(buf_len), K(buf));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid(true/*is_data_in_memory*/))) {
    LOG_ERROR("redo log node is not valid", KPC(this));
    ret = OB_INVALID_DATA;
  // Requires log numbers to be consecutive
  } else if (OB_UNLIKELY(log_no != (end_log_no_ + 1))) {
    LOG_ERROR("log no is not consecutive", K(end_log_no_), K(log_no));
    ret = OB_DISCONTINUOUS_LOG;
  // Requires enough buffer space to hold the data
  } else if (OB_UNLIKELY(size_ - pos_ < buf_len)) {
    LOG_ERROR("buffer is not enough", K(size_), K(pos_), K(buf_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    (void)MEMCPY(data_ + pos_, buf, buf_len);
    pos_ += buf_len;

    // update log range
    end_log_no_ = log_no;
    end_log_id_ = log_id;
  }

  return ret;
}

int64_t RedoLogMetaNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    (void)common::databuff_printf(buf, buf_len, pos, "lod_id=[%ld, %ld] log_no=[%ld, %ld],"
        "size=%ld, pos=%ld, next=%ld",
        start_log_id_, end_log_id_, start_log_no_, end_log_no_, size_, pos_, int64_t(next_));
  }

  return pos;
}

void DmlRedoLogNode::reset()
{
  RedoLogMetaNode::reset();

  log_offset_ = 0;
  ATOMIC_SET(&is_readed_, false);
  row_head_ = NULL;
  row_tail_ = NULL;
  valid_row_num_ = 0;
  ATOMIC_SET(&is_parsed_, false);
  ATOMIC_SET(&is_formatted_, false);
  reserve_field_ = 0;
}

void DmlRedoLogNode::init_for_data_persistence(const int64_t log_no,
    const uint64_t log_id,
    const int32_t log_offset,
    const int64_t size)
{
  reset();

  RedoLogMetaNode::reset(log_no, log_id);
  set_data_len(size);
  log_offset_ = log_offset;

  set_stored_();
}

void DmlRedoLogNode::init_for_data_memory(const int64_t log_no,
    const uint64_t log_id,
    const int32_t log_offset,
    char *data,
    const int64_t size,
    const int64_t pos)
{
  reset();

  RedoLogMetaNode::reset(log_no, log_id, data, size, pos);
  log_offset_ = log_offset;
}

bool DmlRedoLogNode::is_valid() const
{
  bool bool_ret = false;
  bool need_check_data = false;

  if (! is_stored()) {
    need_check_data = true;
  } else {
    if (is_readed()) {
      need_check_data = true;
    } else {
      need_check_data = false;
    }
  }

  bool_ret = RedoLogMetaNode::is_valid(need_check_data);

  return bool_ret;
}

int DmlRedoLogNode::set_data_info(char *data, int64_t data_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data) || OB_UNLIKELY(data_len <= 0)) {
    LOG_ERROR("invalid argument", K(data), K(data_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    RedoLogMetaNode::set_data(data, data_len);
  }

  return ret;
}

void RedoSortedProgress::set_sorted_row_seq_no(const int64_t row_seq_no)
{
  if (row_seq_no < ATOMIC_LOAD(&sorted_row_seq_no_)) {
    // TODO PDML may cause row_seq_no rollback
    LOG_WARN("row_seq_no rollbacked! check if PDML sence", K(row_seq_no), K_(sorted_row_seq_no));
  }
  ATOMIC_STORE(&sorted_row_seq_no_, row_seq_no);
}

int SortedRedoLogList::push(const bool is_data_in_memory,
    RedoLogMetaNode *node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node) || OB_UNLIKELY(! node->is_valid(is_data_in_memory))) {
    OBLOG_LOG(ERROR, "invalid argument", K(node));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == head_) {
    head_ = node;
    node->set_next(NULL);
    tail_ = node;
    node_num_ = 1;
    if (is_data_in_memory) {
      ready_node_num_ = 1;
    }
    log_num_ = node->get_log_num();
  } else { // NULL != head_
    if (OB_ISNULL(tail_)) {
      OBLOG_LOG(ERROR, "tail node is NULL, but head node is not NULL", K(head_), K(tail_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // quick-path
      if (tail_->before(*node)) {
        tail_->set_next(node);
        tail_ = node;
        node->set_next(NULL);
      } else {
        // Iterate through all nodes to find the first redo node that is greater than or equal to the target node
        RedoLogMetaNode **next_ptr = &head_;
        while ((*next_ptr)->before(*node)) {
          next_ptr = &((*next_ptr)->get_next_ptr());
        }

        // If the node value is duplicated, export error OB_ENTRY_EXIST
        if ((*next_ptr)->get_start_log_id() == node->get_start_log_id()) {
          OBLOG_LOG(INFO, "redo log is pushed twice", KPC(node), KPC(*next_ptr), KPC(this));
          ret = OB_ENTRY_EXIST;
        } else {
          node->set_next((*next_ptr));
          *next_ptr = node;
        }
      }

      if (OB_SUCCESS == ret) {
        log_num_ += node->get_log_num();
        ATOMIC_INC(&node_num_);

        if (is_data_in_memory) {
          ATOMIC_INC(&ready_node_num_);
        }
      }
    }
  }

  if (OB_SUCCESS == ret) {
    last_push_node_ = node;
  }

  return ret;
}

void SortedRedoLogList::init_iterator()
{
  cur_dispatch_redo_ = head_;
  cur_sort_redo_ = head_;
  cur_sort_stmt_ = NULL; // row not format and stmt should be null
  sorted_progress_.reset();
}

int SortedRedoLogList::next_dml_redo(RedoLogMetaNode *&dml_redo_meta, bool &is_last_redo)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cur_dispatch_redo_)) {
    if (is_dispatch_finish()) {
      ret = OB_EMPTY_RESULT;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("can't get redo to dispatch but part_trans not dispatch finished", KR(ret), KPC(this));
    }
  } else {
    RedoLogMetaNode *next_redo = cur_dispatch_redo_->get_next();
    dml_redo_meta = cur_dispatch_redo_;
    cur_dispatch_redo_ = next_redo;
    // Theoretically no concurrent call of this function
    sorted_progress_.inc_dispatched_redo_count();
    is_last_redo = is_dispatch_finish();
  }

  return ret;
}

// cur_sort_redo_/cur_sort_stmt_ points to last iter position
// if not ever start iter, both point to NULL,
// if start iter, cur_sort_redo can't be NULL, but cur_sort_stmt_ may point to NULL(ready to points to stmt of a new redo)
int SortedRedoLogList::next_dml_stmt(ObLink *&dml_stmt_task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cur_sort_redo_)) {
    if (OB_ISNULL(cur_sort_stmt_)) {
      ret = OB_ITER_END;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("iterator stmt with valid br found invalid redo but with valid stmt", KR(ret), KP(this), KPC(this));
    }
  } else {
    bool found = false;

    while(OB_SUCC(ret) && !found) {
      if (OB_ISNULL(cur_sort_redo_)) {
        ret = OB_ITER_END;
      } else if (OB_ISNULL(cur_sort_stmt_)) {
        // set cur_sort_stmt_ to the first stmt of cur_sort_redo
        DmlRedoLogNode *dml_redo_node = NULL;
        if (OB_ISNULL(dml_redo_node = static_cast<DmlRedoLogNode*>(cur_sort_redo_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("cast RedoLogMetaNode to DmlRedoLogNode fail", KR(ret), K_(cur_sort_redo), KP(this), KPC(this));
        } else if (!dml_redo_node->is_formatted()) {
          ret = OB_NEED_RETRY;
        } else {
          cur_sort_stmt_ = dml_redo_node->get_row_head();
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(cur_sort_stmt_)) {
          found = true;
          dml_stmt_task = cur_sort_stmt_;
          cur_sort_stmt_ = cur_sort_stmt_->next_;
        }

        if (OB_ISNULL(cur_sort_stmt_)) {
          // switch redo node:
          // 1. found dml_stmt_task and it is the last stmt of cur_sort_redo
          // 2. cur_sort_redo doesn't has any row
          cur_sort_redo_ = cur_sort_redo_->get_next();
          sorted_progress_.inc_sorted_redo_count();
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    is_dml_stmt_iter_end_ = true;
  }

  return ret;
}

int SortedRedoLogList::check_node_num_equality(bool &is_equal)
{
  int ret = OB_SUCCESS;
  const int64_t total_node_num = get_node_number();
  const int64_t cur_ready_node_num = get_ready_node_number();

  if (cur_ready_node_num > total_node_num) {
    LOG_ERROR("cur_ready_node_num is greater than sorted_redo_list_ total_node_num",
        KR(ret), K(cur_ready_node_num), K(total_node_num), KPC(this));
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_equal = (cur_ready_node_num == total_node_num);
  }

  return ret;
}

} // namespace liboblog */
} // namespace oceanbase */
