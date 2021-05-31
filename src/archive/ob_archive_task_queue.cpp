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
#include "ob_archive_thread_pool.h"  // ObArchiveThreadPool
#include "ob_log_archive_define.h"   // ARCHIVE_IO_MAX_RETRY_TIME

using namespace oceanbase::common;
namespace oceanbase {
namespace archive {
ObArchiveTaskStatus::ObArchiveTaskStatus() : issue_(false), ref_(0), num_(0), pg_key_(), queue_(), rwlock_()
{}

ObArchiveTaskStatus::~ObArchiveTaskStatus()
{
  issue_ = false;
  ref_ = 0;
  num_ = 0;
  pg_key_.reset();
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

int ObArchiveTaskStatus::push_unlock(ObLink* link)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(link)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task is NULL", KR(ret), K(link));
  } else if (OB_FAIL(queue_.push(link))) {
    ARCHIVE_LOG(WARN, "push task fail", KR(ret), K(link));
  } else {
    num_++;
  }

  return ret;
}

int ObArchiveTaskStatus::pop(ObLink*& link, bool& task_exist)
{
  int ret = OB_SUCCESS;
  task_exist = false;

  RLockGuard guard(rwlock_);

  if (OB_UNLIKELY(0 >= ref_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task status ref_ already not bigger than zero", KR(ret), K(ref_), K(pg_key_));
  } else if (queue_.is_empty()) {
    // skip it
  } else if OB_FAIL (queue_.pop(link)) {
    ARCHIVE_LOG(WARN, "pop task fail", KR(ret));
  } else {
    task_exist = true;
    num_--;
  }

  return ret;
}

int ObArchiveTaskStatus::retire(bool& is_empty, bool& is_discarded)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;
  is_empty = true;
  is_discarded = false;

  if (OB_UNLIKELY(0 >= ref_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task status ref_ already not bigger than zero", KR(ret), K(ref_), K(pg_key_));
  } else if (queue_.is_empty()) {
    if (OB_FAIL(retire_unlock(is_discarded))) {
      ARCHIVE_LOG(WARN, "ObArchiveTaskStatus retire fail", KR(ret));
    }
  } else {
    is_empty = false;
  }

  return ret;
}

int ObArchiveTaskStatus::retire_unlock(bool& is_discarded)
{
  int ret = OB_SUCCESS;
  is_discarded = false;

  if (OB_UNLIKELY(!issue_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task not issue when retire", KR(ret), K(issue_), K(ref_));
  } else {
    issue_ = false;
    ref_--;
    is_discarded = 0 == ref_;
  }

  return ret;
}

void ObArchiveTaskStatus::free(bool& is_discarded)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;
  is_discarded = false;

  if (ref_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ObArchiveTaskStatus free too many", KR(ret), K(ref_));
  } else {
    ref_--;
    is_discarded = 0 == ref_;
  }
}

int ObArchiveSendTaskStatus::push(ObArchiveSendTask& task, ObArchiveThreadPool& worker)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid send task", KR(ret), K(task));
  } else if (0 >= ref_) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ref_ already reach not bigger than zero, skip it", KR(ret), K(ref_));
  } else if (OB_FAIL(ObArchiveTaskStatus::push_unlock(&task))) {
    ARCHIVE_LOG(WARN, "push fail", KR(ret), K(task));
  } else if (issue_) {
    // skip
  } else if (OB_FAIL(worker.push_task_status(this))) {
    ARCHIVE_LOG(WARN, "push send_task fail", KR(ret));
  } else {
    issue_ = true;
    ref_++;
  }

  return ret;
}

int ObArchiveSendTaskStatus::mock_push(ObArchiveSendTask& task, common::ObSpLinkQueue& queue)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (0 >= ref_) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "ref_ already reach not bigger than zero, skip it", KR(ret), K(ref_));
  } else if (OB_FAIL(ObArchiveTaskStatus::push_unlock(&task))) {
    ARCHIVE_LOG(WARN, "push fail", KR(ret), K(task));
  } else if (issue_) {
    // skip
  } else if (OB_FAIL(queue.push(this))) {
    ARCHIVE_LOG(WARN, "push send_task fail", KR(ret));
  } else {
    issue_ = true;
    ref_++;
  }

  return ret;
}

int ObArchiveSendTaskStatus::top(ObLink*& link, bool& task_exist)
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

ObLink* ObArchiveSendTaskStatus::next(ObLink& pre)
{
  ObLink* link = NULL;

  RLockGuard guard(rwlock_);

  if (queue_.is_empty()) {
    // skip it
  } else {
    link = pre.next_;
  }

  return link;
}

int ObArchiveSendTaskStatus::pop_front(const int64_t num)
{
  int ret = OB_SUCCESS;

  RLockGuard guard(rwlock_);

  for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
    ObLink* link = NULL;
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

bool ObArchiveSendTaskStatus::mark_io_error()
{
  bool bret = false;
  const int64_t now = ObTimeUtility::fast_current_time();
  // two adjoint IO error interval may bigger than ARCHIVE_IO_MAX_RETRY_TIME
  // so error count alse needed
  const int64_t DEFAULT_ERROR_TOLERATE = 2;

  error_occur_count_++;
  if (OB_INVALID_TIMESTAMP == error_occur_timestamp_) {
    error_occur_timestamp_ = now;
  } else {
    bret = now - error_occur_timestamp_ > ARCHIVE_IO_MAX_RETRY_TIME && error_occur_count_ > DEFAULT_ERROR_TOLERATE;
  }

  return bret;
}

void ObArchiveSendTaskStatus::clear_error_info()
{
  error_occur_count_ = 0;
  error_occur_timestamp_ = OB_INVALID_TIMESTAMP;
}

ObArchiveSendTaskStatus::ObArchiveSendTaskStatus(const ObPGKey& pg_key)
{
  error_occur_timestamp_ = OB_INVALID_TIMESTAMP;
  error_occur_count_ = 0;
  pg_key_ = pg_key;
}

ObArchiveSendTaskStatus::~ObArchiveSendTaskStatus()
{
  error_occur_timestamp_ = OB_INVALID_TIMESTAMP;
  error_occur_count_ = 0;
}

ObArchiveCLogTaskStatus::ObArchiveCLogTaskStatus(const ObPGKey& pg_key)
{
  pg_key_ = pg_key;
}

ObArchiveCLogTaskStatus::~ObArchiveCLogTaskStatus()
{}

int ObArchiveCLogTaskStatus::push(ObPGArchiveCLogTask& task, ObArchiveThreadPool& worker)
{
  WLockGuard guard(rwlock_);

  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid send task", KR(ret), K(task));
  } else if (OB_FAIL(ObArchiveTaskStatus::push_unlock(&task))) {
    ARCHIVE_LOG(WARN, "push fail", KR(ret), K(task));
  } else if (0 >= ref_) {
    ret = OB_LOG_ARCHIVE_LEADER_CHANGED;
    ARCHIVE_LOG(WARN, "ref_ already reach not bigger than zero, skip it", KR(ret), K(ref_));
  } else if (issue_) {
    // skip
  } else if (OB_FAIL(worker.push_task_status(this))) {
    ARCHIVE_LOG(WARN, "push clog_task fail", KR(ret));
  } else {
    issue_ = true;
    ref_++;
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
