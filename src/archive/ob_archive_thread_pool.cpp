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

#include "ob_archive_thread_pool.h"
#include "ob_archive_task_queue.h"      // ObArchiveTaskStatus
#include "lib/thread/ob_thread_name.h"  // set_thread_count

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace archive {
ObArchiveThreadPool::ObArchiveThreadPool() : inited_(false), round_stop_flag_(true), total_task_count_(0), task_queue_()
{}

ObArchiveThreadPool::~ObArchiveThreadPool()
{
  destroy();
}

void ObArchiveThreadPool::destroy()
{
  inited_ = false;
  round_stop_flag_ = true;
  total_task_count_ = 0;
}

int ObArchiveThreadPool::init()
{
  return task_queue_.init(MAX_ARCHIVE_TASK_STATUS_QUEUE_CAPACITY);
}

int ObArchiveThreadPool::modify_thread_count()
{
  int ret = OB_SUCCESS;
  const int64_t cur_thread_num = cal_work_thread_num();
  const int64_t pre_thread_count = get_thread_count();

  if (cur_thread_num < 1 || cur_thread_num == pre_thread_count) {
    // skip it
  } else if (OB_FAIL(set_thread_count(cur_thread_num))) {
    ARCHIVE_LOG(WARN, "set_thread_count fail", K(ret), K(cur_thread_num));
  } else {
    ARCHIVE_LOG(INFO, "modify_thread_count succ", K(pre_thread_count), K(cur_thread_num));
  }

  return ret;
}

int64_t ObArchiveThreadPool::get_total_task_num()
{
  return ATOMIC_LOAD(&total_task_count_);
}

int ObArchiveThreadPool::push_task_status(ObArchiveTaskStatus* task_status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(task_status)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(task_status));
  } else {
    do {
      if (OB_FAIL(task_queue_.push(task_status))) {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
          ARCHIVE_LOG(WARN, "push fail", KR(ret), KPC(task_status));
        }
        usleep(100);
      }
    } while (OB_SIZE_OVERFLOW == ret && !round_stop_flag_ && !has_set_stop());
  }

  return ret;
}

void ObArchiveThreadPool::run1()
{
  const int64_t thread_index = get_thread_idx();
  set_thread_name_str(thread_name_);

  ARCHIVE_LOG(INFO, "ObArchiveThreadPool thread run", K(thread_index), K(thread_name_));

  lib::set_thread_name(thread_name_);

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveThreadPool not init", K(thread_name_));
  } else {
    while (!has_set_stop() && !Thread::current().has_set_stop()) {
      do_thread_task_();
    }
  }
}

void ObArchiveThreadPool::do_thread_task_()
{
  int ret = OB_SUCCESS;
  void* data = NULL;
  ObArchiveTaskStatus* task_status = NULL;

  if (OB_FAIL(task_queue_.pop(data, MAX_ARCHIVE_TASK_STATUS_POP_TIMEOUT))) {
    // no task exist, just skip
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000L)) {
      ARCHIVE_LOG(DEBUG, "no task exist, just skip", KR(ret));
    }
  } else if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "task_status is NULL", KR(ret), K(data));
  } else {
    task_status = static_cast<ObArchiveTaskStatus*>(data);
    if (OB_FAIL(handle_task_list(task_status))) {
      ARCHIVE_LOG(WARN, "handle_task_list fail", KR(ret), KPC(task_status));
    }
  }
}

}  // namespace archive
}  // namespace oceanbase
