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

#include "ob_ilog_fetch_task_mgr.h"
#include "ob_archive_allocator.h"

namespace oceanbase {
namespace archive {
using namespace oceanbase::common;
using namespace oceanbase::clog;
PGFetchTask::PGFetchTask()
    : pg_key_(),
      incarnation_(-1),
      archive_round_(-1),
      epoch_(-1),
      start_log_id_(OB_INVALID_ID),
      ilog_file_id_(OB_INVALID_FILE_ID),
      clog_task_(NULL),
      clog_size_(0),
      clog_count_(0),
      first_log_gen_tstamp_(OB_INVALID_TIMESTAMP)
{}

PGFetchTask::~PGFetchTask()
{
  destroy();
}

void PGFetchTask::destroy()
{
  pg_key_.reset();
  incarnation_ = -1;
  archive_round_ = -1;
  epoch_ = -1;
  start_log_id_ = OB_INVALID_ID;
  ilog_file_id_ = OB_INVALID_FILE_ID;

  clog_task_ = NULL;
  clog_size_ = 0;
  clog_count_ = 0;
  first_log_gen_tstamp_ = OB_INVALID_TIMESTAMP;
}

// call only if archive stop
void PGFetchTask::free()
{
  if (NULL != clog_task_) {
    clog_task_->clog_pos_list_.destroy();
    ob_archive_free(clog_task_);
    clog_task_ = NULL;
  }
}

bool PGFetchTask::is_valid()
{
  bool bret = true;

  if (OB_UNLIKELY(!pg_key_.is_valid()) || OB_UNLIKELY(OB_INVALID_ID == start_log_id_) ||
      OB_UNLIKELY(OB_INVALID_FILE_ID == ilog_file_id_) || OB_UNLIKELY(0 >= epoch_)) {
    bret = false;
    ARCHIVE_LOG(WARN, "invalid argument", K(bret), KPC(this));
  }

  return bret;
}

int PGFetchTask::assign(const PGFetchTask& task)
{
  int ret = OB_SUCCESS;

  pg_key_ = task.pg_key_;
  incarnation_ = task.incarnation_;
  archive_round_ = task.archive_round_;
  epoch_ = task.epoch_;
  start_log_id_ = task.start_log_id_;
  ilog_file_id_ = task.ilog_file_id_;

  clog_task_ = task.clog_task_;
  clog_size_ = task.clog_size_;
  clog_count_ = task.clog_count_;
  first_log_gen_tstamp_ = task.first_log_gen_tstamp_;

  return ret;
}

//============IlogPGFetchQueue Function Begin=================//
IlogPGFetchQueue::IlogPGFetchQueue(ObArchiveAllocator& allocator)
    : ilog_file_id_(OB_INVALID_FILE_ID), next_(NULL), allocator_(allocator), pg_array_()
{}

IlogPGFetchQueue::~IlogPGFetchQueue()
{
  destroy();
}

void IlogPGFetchQueue::destroy()
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret) && 0 != pg_array_.count()) {
    PGFetchTask task;
    if (OB_FAIL(pg_array_.pop_back(task))) {
      ARCHIVE_LOG(WARN, "pop fail", KR(ret));
    } else if (NULL != task.clog_task_) {
      allocator_.free_clog_split_task(task.clog_task_);
    }
  }

  ilog_file_id_ = OB_INVALID_FILE_ID;
  next_ = NULL;
  pg_array_.reset();
}

int IlogPGFetchQueue::push_task(PGFetchTask& task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(task.ilog_file_id_ != ilog_file_id_) || OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(task), K(ilog_file_id_));
  } else if (OB_FAIL(pg_array_.push_back(task))) {
    ARCHIVE_LOG(WARN, "push_back fail", KR(ret), K(task));
  }

  return ret;
}

int IlogPGFetchQueue::pop_task(PGFetchTask& task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(pg_array_.pop_back(task))) {
    ARCHIVE_LOG(WARN, "pop_back fail", KR(ret), K(ilog_file_id_));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task is not valid", KR(ret), K(task));
  }

  return ret;
}

file_id_t IlogPGFetchQueue::get_ilog_file_id()
{
  return ilog_file_id_;
}

void IlogPGFetchQueue::set_ilog_file_id(const file_id_t ilog_file_id)
{
  ilog_file_id_ = ilog_file_id;
}

void IlogPGFetchQueue::set_next(IlogPGFetchQueue* next)
{
  next_ = next;
}

IlogPGFetchQueue* IlogPGFetchQueue::get_next()
{
  return next_;
}

int64_t IlogPGFetchQueue::get_task_count()
{
  return pg_array_.count();
}

//============IlogPGFetchQueue Function End=================//

ObArchiveIlogFetchTaskMgr::ObArchiveIlogFetchTaskMgr()
    : stop_flag_(false), ilog_count_(0), cur_ilog_fetch_queue_(NULL), list_(), allocator_(NULL), rwlock_()
{}

ObArchiveIlogFetchTaskMgr::~ObArchiveIlogFetchTaskMgr()
{
  ilog_count_ = 0;

  destroy_task_list_();
}

int ObArchiveIlogFetchTaskMgr::init(ObArchiveAllocator* allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_ = allocator)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(allocator));
  }

  return ret;
}

void ObArchiveIlogFetchTaskMgr::reset()
{
  WLockGuard guard(rwlock_);

  stop_flag_ = false;
  ilog_count_ = 0;
  destroy_task_list_();
}

int ObArchiveIlogFetchTaskMgr::destroy()
{
  int ret = OB_SUCCESS;

  WLockGuard guard(rwlock_);

  stop_flag_ = true;

  ilog_count_ = 0;
  destroy_task_list_();

  return ret;
}

void ObArchiveIlogFetchTaskMgr::destroy_task_list_()
{
  IlogPGFetchQueue* task_queue = NULL;

  while (NULL != allocator_ && NULL != (task_queue = list_.pop())) {
    task_queue->destroy();
    ob_archive_free(task_queue);
    task_queue = NULL;
  }

  if (NULL != cur_ilog_fetch_queue_) {
    cur_ilog_fetch_queue_->~IlogPGFetchQueue();
    ob_archive_free(cur_ilog_fetch_queue_);
    cur_ilog_fetch_queue_ = NULL;
  }

  list_.reset();
}

int ObArchiveIlogFetchTaskMgr::add_ilog_fetch_task(PGFetchTask& task)
{
  int ret = OB_SUCCESS;
  IlogPGFetchQueue* located_ilog_fetch_queue = NULL;
  const file_id_t ilog_file_id = task.ilog_file_id_;
  bool located_exist = true;

  WLockGuard guard(rwlock_);
  if (stop_flag_) {
    // skip it
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(task));
  } else {
    located_ilog_fetch_queue = locat_ilog_pg_fetch_mgr_(ilog_file_id, located_exist);
    if (located_exist) {
      if (OB_FAIL(handle_add_task_with_ilog_exist_(task, located_ilog_fetch_queue))) {
        ARCHIVE_LOG(WARN, "handle_add_task_ilog_exist_ fail", KR(ret), K(task));
      }
    } else {
      if (OB_FAIL(handle_add_task_with_ilog_not_exist_(task, located_ilog_fetch_queue))) {
        ARCHIVE_LOG(WARN, "handle_add_task_ilog_not_exist_ fail", KR(ret), K(task));
      }
    }
  }

  if (stop_flag_ && NULL != task.clog_task_) {
    allocator_->free_clog_split_task(task.clog_task_);
  }

  return ret;
}

IlogPGFetchQueue* ObArchiveIlogFetchTaskMgr::locat_ilog_pg_fetch_mgr_(const file_id_t ilog_file_id, bool& located_exist)
{
  located_exist = false;
  IlogPGFetchQueue* ilog_fetch_queue = list_.head_;
  IlogPGFetchQueue* pre_ilog_fetch_queue = NULL;

  while (NULL != ilog_fetch_queue) {
    if (ilog_file_id == ilog_fetch_queue->get_ilog_file_id()) {
      located_exist = true;
      pre_ilog_fetch_queue = ilog_fetch_queue;
      break;
    } else if (ilog_file_id < ilog_fetch_queue->get_ilog_file_id()) {
      located_exist = false;
      break;
    } else {
      pre_ilog_fetch_queue = ilog_fetch_queue;
      ilog_fetch_queue = ilog_fetch_queue->get_next();
    }
  }

  return pre_ilog_fetch_queue;
}

int ObArchiveIlogFetchTaskMgr::handle_add_task_with_ilog_exist_(PGFetchTask& task, IlogPGFetchQueue* location)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(location) || OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", KR(ret), K(task), K(location));
  } else if (OB_FAIL(location->push_task(task))) {
    ARCHIVE_LOG(WARN, "push_back fail", KR(ret), K(task));
  }

  return ret;
}

int ObArchiveIlogFetchTaskMgr::handle_add_task_with_ilog_not_exist_(PGFetchTask& task, IlogPGFetchQueue* location)
{
  int ret = OB_SUCCESS;
  void* data = NULL;
  IlogPGFetchQueue* ilog_fetch_queue = NULL;

  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "task is not valid", KR(ret), K(task));
  } else if (OB_ISNULL(data = ob_archive_malloc(sizeof(IlogPGFetchQueue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "ob_archive_malloc is NULL", KR(ret), K(task));
  } else {
    ilog_fetch_queue = new (data) IlogPGFetchQueue(*allocator_);
    ilog_fetch_queue->set_ilog_file_id(task.ilog_file_id_);
    if (OB_FAIL(ilog_fetch_queue->push_task(task))) {
      ARCHIVE_LOG(WARN, "push_back fail", KR(ret), K(task));
    } else if (OB_FAIL(list_.insert(ilog_fetch_queue, location))) {
      ARCHIVE_LOG(WARN, "insert fail", KR(ret), K(task));
    } else {
      ilog_count_++;
    }
  }

  if (OB_FAIL(ret) && NULL != data) {
    ilog_fetch_queue->~IlogPGFetchQueue();
    ob_archive_free(ilog_fetch_queue);
  }

  return ret;
}

// identify if ilog queue switch or not, to optimize for clog_splitter
int ObArchiveIlogFetchTaskMgr::pop_ilog_fetch_task(PGFetchTask& task, bool& exist_task)
{
  int ret = OB_SUCCESS;
  bool new_ilog_file = false;
  exist_task = false;

  if (stop_flag_) {
    // skip it
  } else if (OB_FAIL(pop_ilog_fetch_task_(task, exist_task, new_ilog_file))) {
    ARCHIVE_LOG(WARN, "pop_ilog_fetch_task_ fail", KR(ret));
  } else if (new_ilog_file) {
    usleep(ILOG_FETCH_WAIT_INTERVAL);
  }

  return ret;
}

int ObArchiveIlogFetchTaskMgr::pop_ilog_fetch_task_(PGFetchTask& task, bool& exist_task, bool& new_ilog_file)
{
  int ret = OB_SUCCESS;
  exist_task = false;
  new_ilog_file = false;

  WLockGuard guard(rwlock_);

  if (NULL == cur_ilog_fetch_queue_) {
    cur_ilog_fetch_queue_ = list_.pop();
    new_ilog_file = true;
  }

  if (NULL != cur_ilog_fetch_queue_) {
    if (OB_FAIL(cur_ilog_fetch_queue_->pop_task(task))) {
      ARCHIVE_LOG(WARN, "pop_task fail", KR(ret));
    } else {
      exist_task = true;
      if (0 == cur_ilog_fetch_queue_->get_task_count()) {
        cur_ilog_fetch_queue_->~IlogPGFetchQueue();
        ob_archive_free(cur_ilog_fetch_queue_);
        cur_ilog_fetch_queue_ = NULL;
      }
    }
  }

  return ret;
}

}  // namespace archive
}  // namespace oceanbase
