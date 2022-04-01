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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_trans_log.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
int RedoLogMetaNode::update_redo_meta(const int64_t log_no, const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_no < 0) || OB_UNLIKELY(OB_INVALID_ID == log_id)) {
    LOG_ERROR("invalid argument", K(log_no), K(log_id));
    ret = OB_INVALID_ARGUMENT;
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

int DmlRedoLogNode::append_redo_log(const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(buf_len), K(buf));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid())) {
    LOG_ERROR("redo log node is not valid", KPC(this));
    ret = OB_INVALID_DATA;
  }
  // Requires enough buffer space to hold the data
  else if (OB_UNLIKELY(size_ - pos_ < buf_len)) {
    LOG_ERROR("buffer is not enough", K(size_), K(pos_), K(buf_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    (void)MEMCPY(data_ + pos_, buf, buf_len);
    pos_ += buf_len;
  }

  return ret;
}

int DdlRedoLogNode::append_redo_log(const int64_t log_no, const uint64_t log_id, const char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(log_no < 0) || OB_UNLIKELY(OB_INVALID_ID == log_id)
      || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(log_no), K(log_id), K(buf_len), K(buf));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(! is_valid())) {
    LOG_ERROR("redo log node is not valid", KPC(this));
    ret = OB_INVALID_DATA;
  } else if (OB_FAIL(update_redo_meta(log_no, log_id))) {
    LOG_ERROR("update_redo_meta fail", KR(ret), K(log_no), K(log_id));
  }
  // Requires enough buffer space to hold the data
  else if (OB_UNLIKELY(size_ - pos_ < buf_len)) {
    LOG_ERROR("buffer is not enough", K(size_), K(pos_), K(buf_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    (void)MEMCPY(data_ + pos_, buf, buf_len);
    pos_ += buf_len;
  }

  return ret;
}

// push redo log with order
int SortedRedoLogList::push(RedoLogMetaNode *node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node) || OB_UNLIKELY(! node->is_valid())) {
    OBLOG_LOG(ERROR, "invalid argument", K(node));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == head_) {
    head_ = node;
    node->next_ = NULL;
    tail_ = node;
    node_num_ = 1;
    log_num_ = node->get_log_num();
  } else { // NULL != head_
    if (OB_ISNULL(tail_)) {
      OBLOG_LOG(ERROR, "tail node is NULL, but head node is not NULL", K(head_), K(tail_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // quick-path
      if (tail_->before(*node)) {
        tail_->next_ = node;
        tail_ = node;
        node->next_ = NULL;
      } else {
        // Iterate through all nodes to find the first redo node that is greater than or equal to the target node
        RedoLogMetaNode **next_ptr = &head_;
        while ((*next_ptr)->before(*node)) {
          next_ptr = &((*next_ptr)->next_);
        }

        // If the node value is duplicated, export error OB_ENTRY_EXIST
        if ((*next_ptr)->start_log_id_ == node->start_log_id_) {
          OBLOG_LOG(INFO, "redo log is pushed twice", KPC(node), KPC(*next_ptr), KPC(this));
          ret = OB_ENTRY_EXIST;
        } else {
          node->next_ = (*next_ptr);
          *next_ptr = node;
        }
      }

      if (OB_SUCCESS == ret) {
        log_num_ += node->get_log_num();
        ATOMIC_INC(&node_num_);
      }
    }
  }

  if (OB_SUCCESS == ret) {
    last_push_node_ = node;
  }

  return ret;
}

} // namespace liboblog */
} // namespace oceanbase */
