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

#define USING_LOG_PREFIX CLOG

#include <cstring>

#include "ob_log_transport_task_owner_state.h"
#include "lib/ob_errno.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace logservice
{

ObLogTransportTaskHolder::ObLogTransportTaskHolder()
  : ptr_(nullptr),
    state_(ObLogTransportTaskOwnerState::BORROWED)
{
}

int ObLogTransportTaskHolder::borrowed(const ObLogTransportReq *task,
                                       ObLogTransportTaskHolder &holder)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid transport task for borrowed holder", K(ret), KP(task));
  } else if (!holder.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "holder is not empty", K(ret), K(holder));
  } else {
    holder.reset();
    holder.ptr_ = task;
    holder.state_ = ObLogTransportTaskOwnerState::BORROWED;
  }
  return ret;
}

int ObLogTransportTaskHolder::owned(ObLogTransportReq *task,
                                    ObLogTransportTaskHolder &holder)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid transport task for owned holder", K(ret), KP(task));
  } else if (!holder.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "holder is not empty", K(ret), K(holder));
  } else {
    holder.reset();
    holder.ptr_ = task;
    holder.state_ = ObLogTransportTaskOwnerState::OWNED;
  }
  return ret;
}

int ObLogTransportTaskHolder::ensure_owned(const common::ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (is_owned()) {
    // already owned, do nothing
  } else if (OB_ISNULL(ptr_) || !ptr_->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "invalid transport request", K(ret), K(ptr_));
  } else {
    ObLogTransportReq *copied = static_cast<ObLogTransportReq*>(share::mtl_malloc(sizeof(ObLogTransportReq), attr));
    if (OB_ISNULL(copied)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      *copied = *ptr_;
      copied->log_data_ = nullptr;
      if (ptr_->log_size_ > 0 && OB_NOT_NULL(ptr_->log_data_)) {
        char *log_data = static_cast<char*>(share::mtl_malloc(ptr_->log_size_, attr));
        if (OB_ISNULL(log_data)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          ::memcpy(log_data, ptr_->log_data_, ptr_->log_size_);
          copied->log_data_ = log_data;
        }
      }

      if (OB_FAIL(ret)) {
        free_owned_(copied);
        copied = nullptr;
      } else {
        ptr_ = copied;
        state_ = ObLogTransportTaskOwnerState::OWNED;
      }
    }
  }
  return ret;
}

bool ObLogTransportTaskHolder::is_owned() const
{
  return ObLogTransportTaskOwnerState::OWNED == state_;
}

bool ObLogTransportTaskHolder::is_empty() const
{
  return OB_ISNULL(ptr_) && ObLogTransportTaskOwnerState::BORROWED == state_;
}

const ObLogTransportReq *ObLogTransportTaskHolder::ptr() const
{
  return ptr_;
}

int64_t ObLogTransportTaskHolder::size() const
{
  return OB_ISNULL(ptr_) ? 0 : ptr_->log_size_;
}

void ObLogTransportTaskHolder::reset()
{
  if (is_owned()) {
    free_owned_(ptr_);
  }
  ptr_ = nullptr;
  state_ = ObLogTransportTaskOwnerState::BORROWED;
}

ObLogTransportTaskHolder::ObLogTransportTaskHolder(ObLogTransportTaskHolder &&other) noexcept
  : ptr_(other.ptr_),
    state_(other.state_)
{
  other.ptr_ = nullptr;
  other.state_ = ObLogTransportTaskOwnerState::BORROWED;
}

ObLogTransportTaskHolder &ObLogTransportTaskHolder::operator=(ObLogTransportTaskHolder &&other) noexcept
{
  if (this != &other) {
    reset();
    ptr_ = other.ptr_;
    state_ = other.state_;
    other.ptr_ = nullptr;
    other.state_ = ObLogTransportTaskOwnerState::BORROWED;
  }
  return *this;
}

ObLogTransportTaskHolder::~ObLogTransportTaskHolder()
{
  reset();
}

void ObLogTransportTaskHolder::free_owned_(const ObLogTransportReq *task)
{
  if (OB_NOT_NULL(task)) {
    ObLogTransportReq *mutable_task = const_cast<ObLogTransportReq*>(task);
    if (OB_NOT_NULL(mutable_task->log_data_) && mutable_task->log_size_ > 0) {
      share::mtl_free(const_cast<char*>(mutable_task->log_data_));
      mutable_task->log_data_ = nullptr;
    }
    share::mtl_free(mutable_task);
  }
}

} // namespace logservice
} // namespace oceanbase
