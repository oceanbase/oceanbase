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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_UTIL_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_UTIL_H_

#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace archive
{
using oceanbase::palf::LSN;

#define GET_LS_TASK_CTX(mgr, id)                             \
    ObArchiveLSGuard guard(mgr);                                \
    ObLSArchiveTask *ls_archive_task = NULL;                 \
    if (OB_FAIL(mgr->get_ls_guard(id, guard))) {               \
      ARCHIVE_LOG(WARN, "get ls guard failed", K(ret), K(id));    \
    } else if (OB_ISNULL(ls_archive_task = guard.get_ls_task())) {  \
      ret = OB_ERR_UNEXPECTED;                                              \
      ARCHIVE_LOG(ERROR, "ls task is NULL", K(ret), K(id));       \
    }                                                                       \
    if (OB_SUCC(ret))

int64_t cal_archive_file_id(const LSN &lsn, const int64_t N);
// 不支持并发
struct SimpleQueue
{
  common::ObLink *head_;
  common::ObLink *tail_;

public:
  SimpleQueue() : head_(NULL), tail_(NULL) {}
  ~SimpleQueue() { head_ = NULL; tail_ = NULL;}

public:
  bool is_empty()
  {
    return NULL == head_ && NULL == tail_;
  }

  int push(common::ObLink *p)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(p)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
      p->next_ = NULL;
      head_ = p;
      tail_ = p;
    } else {
      common::ObLink *tmp = tail_;
      tail_ = p;
      tmp->next_ = tail_;
      tail_->next_ = NULL;
    }
    return ret;
  }

  int pop(common::ObLink *&p)
  {
    int ret = common::OB_SUCCESS;
    p = NULL;
    if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
    } else if (head_ == tail_) {
      p = head_;
      head_ = NULL;
      tail_ = NULL;
    } else {
      p = head_;
      head_ = head_->next_;
    }
    return ret;
  }

  int top(common::ObLink *&p)
  {
    int ret = common::OB_SUCCESS;
    p = NULL;
    if ((NULL == head_ && NULL != tail_) || (NULL != head_ && NULL == tail_)) {
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL == head_) {
    } else {
      p = head_;
    }
    return ret;
  }
};

} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_UTIL_H_ */
