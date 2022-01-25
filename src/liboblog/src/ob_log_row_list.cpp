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

#include "ob_log_row_list.h"
#include "ob_log_instance.h"                        // TCTX
#include "ob_log_store_service.h"
#include "ob_log_binlog_record_pool.h"
#include "ob_log_resource_collector.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace liboblog
{
SortedDmlRowList::SortedDmlRowList()
  : row_num_(0),
    head_(NULL),
    tail_(NULL)
{
}

SortedDmlRowList::~SortedDmlRowList()
{
  reset();
}

void SortedDmlRowList::reset()
{
  row_num_ = 0;
  head_ = NULL;
  tail_ = NULL;
}

bool SortedDmlRowList::is_valid() const
{
  return row_num_ > 0 && NULL != head_ && NULL != tail_;
}

int SortedDmlRowList::push(ObLogRowDataIndex *row_head,
    ObLogRowDataIndex *row_tail,
    const int64_t row_num,
    const bool is_contain_rollback_row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_head) || OB_ISNULL(row_tail) || OB_UNLIKELY(row_num <= 0)) {
    LOG_ERROR("row_head or row_tail is NULL", K(row_head), K(row_tail), K(row_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (! is_contain_rollback_row) {
    if (OB_FAIL(push_when_not_contain_rollback_row_(row_head, row_tail, row_num))) {
      LOG_ERROR("push_when_not_contain_rollback_row_ fail", KR(ret), KPC(row_head), KPC(row_tail), K(row_num));
    }
  } else {
    if (OB_FAIL(push_when_contain_rollback_row_(row_head, row_tail, row_num))) {
      LOG_ERROR("push_when_contain_rollback_row_ fail", KR(ret), KPC(row_head), KPC(row_tail), K(row_num));
    }
  }


  return ret;
}

int SortedDmlRowList::push_when_not_contain_rollback_row_(ObLogRowDataIndex *row_head,
    ObLogRowDataIndex *row_tail,
    const int64_t row_num)
{
  int ret = OB_SUCCESS;
  const bool is_single_row = false;

  if (OB_ISNULL(row_head) || OB_ISNULL(row_tail) || OB_UNLIKELY(row_num <= 0)) {
    LOG_ERROR("row_head or row_tail is NULL", K(row_head), K(row_tail), K(row_num));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(push_(row_head, row_tail, row_num, is_single_row))){
    LOG_ERROR("push_ fail", KR(ret), KPC(row_head), KPC(row_tail), K(row_num), K(is_single_row));
  } else {
    // succ
  }

  return ret;
}

int SortedDmlRowList::push_when_contain_rollback_row_(ObLogRowDataIndex *row_head,
    ObLogRowDataIndex *row_tail,
    const int64_t row_num)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_head) || OB_ISNULL(row_tail) || OB_UNLIKELY(row_num <= 0)) {
    LOG_ERROR("row_head or row_tail is NULL", K(row_head), K(row_tail), K(row_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObLogRowDataIndex *cur_row_index = row_head;

    while (OB_SUCC(ret) && NULL != cur_row_index) {
      ObLogRowDataIndex *next_row_index = cur_row_index->get_next();
      const bool is_rollback_row = cur_row_index->is_rollback();

      if (! is_rollback_row) {
        // insert as a single node
        cur_row_index->set_next(NULL);
        const bool is_single_row = true;

        if (OB_FAIL(push_(cur_row_index, cur_row_index, 1, is_single_row))){
          LOG_ERROR("push_ fail", KR(ret), KPC(cur_row_index), K(is_single_row));
        }
      } else {
        const int32_t rollback_sql_no = cur_row_index->get_row_sql_no();

        if (OB_FAIL(rollback_row_(rollback_sql_no, *cur_row_index))) {
          LOG_ERROR("rollback_row_ fail", KR(ret), K(rollback_sql_no), KPC(cur_row_index));
        }
      }

      if (OB_SUCC(ret)) {
        cur_row_index = next_row_index;
      }
    } // while
  }

  return ret;
}

int SortedDmlRowList::push_(ObLogRowDataIndex *row_head,
    ObLogRowDataIndex *row_tail,
    const int64_t row_num,
    const bool is_single_row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(row_head) || OB_ISNULL(row_tail) || OB_UNLIKELY(row_num <= 0)) {
    LOG_ERROR("row_head or row_tail is NULL", K(row_head), K(row_tail), K(row_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (NULL == head_) {
      head_ = row_head;
      tail_ = row_tail;
      row_num_ = row_num;
    } else if (OB_ISNULL(tail_)) {
      LOG_ERROR("tail node is NULL, but head node is not NULL", K(head_), K(tail_));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // insert tail
      if (tail_->before(*row_head, is_single_row)) {
        tail_->set_next(row_head);
        tail_ = row_tail;
        row_num_ += row_num;
      // insert head
      } else if (row_tail->before(*head_, is_single_row)) {
        row_tail->set_next(head_);
        head_ = row_head;
        row_num_ += row_num;
      } else {
        // Iterate through all nodes to find the first redo node that is greater than or equal to the target node
        ObLogRowDataIndex *pre_ptr = NULL;
        ObLogRowDataIndex *cur_ptr = head_;

        while (cur_ptr->before(*row_head, is_single_row)) {
          pre_ptr = cur_ptr;
          cur_ptr = cur_ptr->get_next();
        }

        // If the node value is duplicated, the error node exists
        if (cur_ptr->equal(*row_head, is_single_row)) {
          LOG_INFO("redo log is pushed twice", KPC(row_head), KPC(row_tail), KPC(cur_ptr), KPC(this));
          ret = OB_ENTRY_EXIST;
        } else {
          row_tail->set_next(cur_ptr);
          pre_ptr->set_next(row_head);
          row_num_ += row_num;
        }

      }
    }
  }

  return ret;
}

int SortedDmlRowList::rollback_row_(const int32_t rollback_sql_no,
    ObLogRowDataIndex &rollback_row_data_index)
{
  int ret = OB_SUCCESS;
  const int64_t total_row_num = row_num_;
  int64_t save_row_num = 0;
  bool found = false;
  ObLogRowDataIndex *pre_row_index = NULL;
  ObLogRowDataIndex *cur_row_index = head_;
  IObLogResourceCollector *resource_collector = TCTX.resource_collector_;

  if (OB_ISNULL(resource_collector)) {
    LOG_ERROR("resource_collector is NULL");
    ret = OB_ERR_UNEXPECTED;
  }

  // 1. stmt with seq_no less than or equal to the sql_no specified by rollback savepoint is not processed, find the first sql_no greater than rollback_sql_no
  while (OB_SUCC(ret) && NULL != cur_row_index && !found) {
    const int32_t cur_row_sql_no = cur_row_index->get_row_sql_no();

    if (cur_row_sql_no <= rollback_sql_no) {
      pre_row_index = cur_row_index;
      cur_row_index = cur_row_index->get_next();
      ++save_row_num;
    } else {
      found = true;
    }
  } // while

  // 2. try to rollback and free
  if (OB_SUCC(ret) && found) {
    if (NULL == pre_row_index) {
      head_ = NULL;
      tail_ = NULL;
    } else {
      pre_row_index->set_next(NULL);
      tail_ = pre_row_index;
    }
    row_num_ = save_row_num;

    // Recycle resources: persistent data needs to be cleaned up
    // ObLogBR does not need to be recycled here, as the data is currently returned after persistence
    while (OB_SUCC(ret) && NULL != cur_row_index) {
      ObLogRowDataIndex *next_row_index = cur_row_index->get_next();

      if (OB_FAIL(resource_collector->revert_unserved_task(false/*is_rollback_row*/, *cur_row_index))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("revert_unserved_task fail", KR(ret), KPC(cur_row_index));
        }
      }

      if (OB_SUCC(ret)) {
        cur_row_index = next_row_index;
      }
    } // while
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(resource_collector->revert_unserved_task(true/*is_rollback_row*/, rollback_row_data_index))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("revert_unserved_task fail", KR(ret), K(rollback_row_data_index));
      }
    }
  }

  if (OB_SUCC(ret)) {
    _LOG_INFO("[SAVEPOINT][DML] ROLLBACK_SQL_NO=%d STMT_CNT=%ld/%ld",
        rollback_sql_no, total_row_num, save_row_num);
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
