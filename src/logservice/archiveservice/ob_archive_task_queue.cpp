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

#include "ob_archive_task_queue.h"
#include "share/ob_errno.h"             // ret
#include "ob_archive_worker.h"          // ObArchiveWorker

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace archive
{
ObArchiveTaskStatus::ObArchiveTaskStatus(const ObLSID &id) :
  issue_(false),
  ref_(0),
  num_(0),
  id_(id),
  queue_(),
  rwlock_()
{
}

ObArchiveTaskStatus::~ObArchiveTaskStatus()
{
  issue_ = false;
  ref_ = 0;
  num_ = 0;
  id_.reset();
}

void ObArchiveTaskStatus::inc_ref()
{
  WLockGuard guard(rwlock_);
  ref_ += 1;
}

int64_t ObArchiveTaskStatus::count()
{
  return ATOMIC_LOAD(&num_);
}

int ObArchiveTaskStatus::push(ObLink *link, ObArchiveWorker &worker)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(rwlock_);

  if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task is NULL", K(ret), K(link));
  } else if (0 >= ref_) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "ref_ already reach not bigger than zero, skip it", K(ret), K(ref_));
  } else if (OB_FAIL(queue_.push(link))) {
    ARCHIVE_LOG(WARN, "push task fail", K(ret), K(link));
  } else {
    num_++;
  }

  // try push task_status
  if (OB_SUCC(ret) && ! issue_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = worker.push_task_status(this))) {
      ARCHIVE_LOG(WARN, "push task_status fail", K(tmp_ret));
    } else {
      issue_ = true;
      ref_++;
    }
  }

  return ret;
}

int ObArchiveTaskStatus::pop(ObLink *&link, bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(0 >= ref_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task status ref_ already not bigger than zero",
        K(ret), K(ref_), K(id_));
  } else if (queue_.is_empty()) {
    // skip it
  } else if OB_FAIL(queue_.pop(link)) {
    ARCHIVE_LOG(WARN, "pop task fail", K(ret));
  } else {
    task_exist = true;
    num_--;
  }

  return ret;
}

int ObArchiveTaskStatus::top(ObLink *&link, bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;
  RLockGuard guard(rwlock_);

  if (queue_.is_empty()) {
    // skip it
  } else if (OB_FAIL(queue_.top(link))) {
    ARCHIVE_LOG(WARN, "top task fail", KR(ret));
  } else {
    task_exist = true;
  }

  return ret;
}

int ObArchiveTaskStatus::pop_front(const int64_t num)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(rwlock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObLink *link = NULL;
    if (queue_.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "queue_ is empty", KR(ret));
    } else if (OB_FAIL(queue_.pop(link))) {
      ARCHIVE_LOG(WARN, "pop task fail", KR(ret));
    } else {
      num_--;
    }
  }

  return ret;
}

int ObArchiveTaskStatus::retire(bool &is_empty, bool &is_discarded)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;
  is_empty = true;
  is_discarded = false;

  if (OB_UNLIKELY(0 >= ref_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task status ref_ already not bigger than zero",
        K(ret), K(ref_), K(id_));
  } else if (queue_.is_empty()) {
    if (OB_FAIL(retire_unlock(is_discarded))) {
      ARCHIVE_LOG(WARN, "ObArchiveTaskStatus retire fail", K(ret));
    }
  } else {
    is_empty = false;
  }

  return ret;
}

int ObArchiveTaskStatus::retire_unlock(bool &is_discarded)
{
  int ret = OB_SUCCESS;
  is_discarded = false;

  if (OB_UNLIKELY(! issue_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task not issue when retire", K(ret), K(issue_), K(ref_));
  } else {
    issue_ = false;
    ref_--;
    is_discarded = 0 == ref_;
  }

  return ret;
}

void ObArchiveTaskStatus::free(bool &is_discarded)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;
  is_discarded = false;

  if (ref_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObArchiveTaskStatus free too many", K(ret), K(ref_));
  } else {
    ref_--;
    is_discarded = 0 == ref_;
  }
}

/*
int ObArchiveSendTaskStatus::top(ObLink *&link, bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;

  RLockGuard guard(rwlock_);

  if (queue_.is_empty()) {
    // skip it
  } else if (OB_FAIL(queue_.top(link))) {
    ARCHIVE_LOG(WARN, "top task fail", K(ret));
  } else {
    task_exist = true;
  }

  return ret;
}

ObLink *ObArchiveSendTaskStatus::next(ObLink &pre)
{
  ObLink *link = NULL;

  RLockGuard guard(rwlock_);

  if (queue_.is_empty()) {
    // skip it
  } else {
    link = pre.next_;
  }

  return link;
}
*/

} // namespace archive
} // namespace oceanbase
