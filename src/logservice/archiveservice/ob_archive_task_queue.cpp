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
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/archiveservice/ob_archive_task.h"
#include "logservice/archiveservice/ob_archive_util.h"
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
  rwlock_(common::ObLatchIds::ARCHIVE_TASK_QUEUE_LOCK)
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

  WLockGuard guard(rwlock_);

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

int ObArchiveTaskStatus::get_next(ObLink *&link, bool &task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;
  link = NULL;
  int64_t basic_file_id = 0;
  RLockGuard guard(rwlock_);

  // pop with write lock, so if queue is not empty, top should succeed
  if (queue_.is_empty()) {
    // skip it
  } else if (OB_FAIL(queue_.top(link))) {
    ARCHIVE_LOG(WARN, "top task fail", KR(ret));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "link is NULL, unexpected", K(ret), KPC(this));
  } else {
    while (NULL != link && ! task_exist) {
      ObArchiveSendTask *task = static_cast<ObArchiveSendTask*>(link);
      const int64_t file_id = cal_archive_file_id(task->get_start_lsn(), MAX_ARCHIVE_FILE_SIZE);
      ARCHIVE_LOG(TRACE, "print cur_task", KPC(task), KPC(this));
      if (0 != basic_file_id && file_id != basic_file_id + 1) {
        // 1. archive parallel only supports inter-files
        // 2. file id gap of adjoint tasks should not be bigger than 1,
        //    they are pushed in sequentially
        break;
      } else if (task->issue_task()) {
        task_exist = true;
        ATOMIC_SET(&last_issue_timestamp_, common::ObTimeUtility::fast_current_time());
      } else {
        link = link->next_;
      }
      basic_file_id = file_id;
    }
  }

  if (! task_exist
      && ! queue_.is_empty()
      && common::ObTimeUtility::fast_current_time() - ATOMIC_LOAD(&last_issue_timestamp_) > PRINT_WARN_THRESHOLD) {
    print_self_();
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
    if (OB_FAIL(retire_unlock_(is_discarded))) {
      ARCHIVE_LOG(WARN, "ObArchiveTaskStatus retire fail", K(ret));
    }
  } else {
    is_empty = false;
  }

  return ret;
}

void ObArchiveTaskStatus::print_self()
{
  RLockGuard guard(rwlock_);
  print_self_();
}

int ObArchiveTaskStatus::retire_unlock_(bool &is_discarded)
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

void ObArchiveTaskStatus::print_self_()
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  const int64_t max_count = 10;
  ObLink *link = NULL;
  if (queue_.is_empty()) {
    ARCHIVE_LOG(INFO, "task status is empty", KPC(this));
  } else if (OB_FAIL(queue_.top(link))) {
    ARCHIVE_LOG(WARN, "top task fail", KR(ret), KPC(this));
  } else if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "link is NULL, unexpected", K(ret), KPC(this));
  } else {
    while (NULL != link && index < max_count) {
      ObArchiveSendTask *task = static_cast<ObArchiveSendTask*>(link);
      ARCHIVE_LOG(INFO, "print send task in queue", K(index), KPC(task), KPC(this));
      link = link->next_;
      index++;
    }
  }
}

} // namespace archive
} // namespace oceanbase
