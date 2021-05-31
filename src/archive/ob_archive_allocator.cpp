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

#include "ob_log_archive_struct.h"
#include "ob_archive_allocator.h"
#include "ob_archive_task_queue.h"
#include "lib/ob_running_mode.h"  // is_mini_mode

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace archive {
ObArchiveAllocator::ObArchiveAllocator() : inited_(false), clog_task_allocator_()
{}

ObArchiveAllocator::~ObArchiveAllocator()
{
  inited_ = false;
}

int ObArchiveAllocator::init()
{
  int ret = OB_SUCCESS;
  const int64_t clog_task_size = sizeof(ObPGArchiveCLogTask);
  const int64_t clog_task_status_size = sizeof(ObArchiveCLogTaskStatus);
  const int64_t send_task_status_size = sizeof(ObArchiveSendTaskStatus);
  const int64_t UNUSED_HOLD_LIMIT = 0;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(ERROR, "ObArchiveAllocator has been inited", KR(ret));
  } else if (OB_FAIL(clog_task_allocator_.init(clog_task_size, "ArcCLogTask"))) {
    ARCHIVE_LOG(WARN, "clog_task_allocator_ init fail", KR(ret));
  } else if (OB_FAIL(send_task_allocator_.init(SEND_TASK_CAPACITY_LIMIT, UNUSED_HOLD_LIMIT, SEND_TASK_PAGE_SIZE))) {
    ARCHIVE_LOG(WARN, "send_task_allocator_ init fail", KR(ret));
  } else if (OB_FAIL(clog_task_status_allocator_.init(clog_task_status_size, "ArcCLogTS"))) {
    ARCHIVE_LOG(WARN, "clog_task_status_allocator_ init fail", KR(ret));
  } else if (OB_FAIL(send_task_status_allocator_.init(send_task_status_size, "ArcSendTS"))) {
    ARCHIVE_LOG(WARN, "send_task_status_allocator_ init fail", KR(ret));
  } else {
    send_task_allocator_.set_label("ArcSendTask");
    inited_ = true;
  }

  return ret;
}

ObPGArchiveCLogTask* ObArchiveAllocator::alloc_clog_split_task()
{
  void* data = NULL;
  ObPGArchiveCLogTask* task = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveAllocator not init");
  } else if (OB_ISNULL(data = clog_task_allocator_.alloc())) {
    ARCHIVE_LOG(WARN, "alloc data fail");
  } else {
    task = new (data) ObPGArchiveCLogTask();
  }

  return task;
}

void ObArchiveAllocator::free_clog_split_task(ObPGArchiveCLogTask* task)
{
  if (NULL != task) {
    task->~ObPGArchiveCLogTask();
    clog_task_allocator_.free(task);
    task = NULL;
  }
}

ObArchiveSendTask* ObArchiveAllocator::alloc_send_task(const int64_t buf_len)
{
  char* data = NULL;
  ObArchiveSendTask* task = NULL;
  const int64_t size = sizeof(ObArchiveSendTask);

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveAllocator not init");
  } else if (OB_ISNULL(data = static_cast<char*>(send_task_allocator_.alloc(size + buf_len)))) {
    // alloc fail
  } else {
    task = new (data) ObArchiveSendTask();
    task->buf_ = data + size;
    task->buf_len_ = buf_len;
  }

  return task;
}

void ObArchiveAllocator::free_send_task(ObArchiveSendTask* task)
{
  if (NULL != task) {
    task->~ObArchiveSendTask();
    send_task_allocator_.free(task);
    task = NULL;
  }
}

int64_t ObArchiveAllocator::get_send_task_capacity()
{
  return send_task_allocator_.allocated();
}

ObArchiveSendTaskStatus* ObArchiveAllocator::alloc_send_task_status(const common::ObPGKey& pg_key)
{
  void* data = NULL;
  ObArchiveSendTaskStatus* task_status = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveAllocator not init");
  } else if (OB_ISNULL(data = send_task_status_allocator_.alloc())) {
    ARCHIVE_LOG(WARN, "alloc data fail");
  } else {
    task_status = new (data) ObArchiveSendTaskStatus(pg_key);
  }

  return task_status;
}

void ObArchiveAllocator::free_send_task_status(ObArchiveSendTaskStatus* status)
{
  if (NULL != status) {
    status->~ObArchiveSendTaskStatus();
    send_task_status_allocator_.free(status);
    status = NULL;
  }
}

ObArchiveCLogTaskStatus* ObArchiveAllocator::alloc_clog_task_status(const common::ObPGKey& pg_key)
{
  void* data = NULL;
  ObArchiveCLogTaskStatus* task_status = NULL;

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(WARN, "ObArchiveAllocator not init");
  } else if (OB_ISNULL(data = clog_task_status_allocator_.alloc())) {
    ARCHIVE_LOG(WARN, "alloc data fail");
  } else {
    task_status = new (data) ObArchiveCLogTaskStatus(pg_key);
  }

  return task_status;
}

void ObArchiveAllocator::free_clog_task_status(ObArchiveCLogTaskStatus* status)
{
  if (NULL != status) {
    status->~ObArchiveCLogTaskStatus();
    clog_task_status_allocator_.free(status);
    status = NULL;
  }
}

int ObArchiveAllocator::set_archive_batch_buffer_limit()
{
  int ret = OB_SUCCESS;
  const int64_t mini_capacity = MAX_ARCHIVE_BLOCK_SIZE;
  const int64_t buffer_limit = GCONF.log_archive_batch_buffer_limit;
  const int64_t capacity_limit = !lib::is_mini_mode() ? std::max(buffer_limit, SEND_TASK_CAPACITY_LIMIT) : buffer_limit;

  if (OB_UNLIKELY(mini_capacity > capacity_limit)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN,
        "invalid archive batch buffer capacity limit",
        KR(ret),
        K(buffer_limit),
        K(capacity_limit),
        K(mini_capacity));
  } else {
    send_task_allocator_.set_total_limit(capacity_limit);
  }

  return ret;
}

};  // namespace archive
};  // namespace oceanbase
