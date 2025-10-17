/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_io_pipeline.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

/*-----------------------------------------TaskState-----------------------------------------*/
bool is_valid_state(const TaskState state)
{
  return state >= TASK_INIT && state < TASK_MAX_STATE;
}

const char *get_state_str(const TaskState state)
{
  static const char *TASK_INFO_STATE_STRS[] = {
      "TASK_INIT", "TASK_READ_IN_PROGRESS", "TASK_READ_DONE",
      "TASK_WRITE_IN_PROGRESS", "TASK_WRITE_DONE", "TASK_FINISHED",
      "TASK_MAX_STATE"
  };
  STATIC_ASSERT(static_cast<int64_t>(TASK_MAX_STATE) + 1 ==
      ARRAYSIZEOF(TASK_INFO_STATE_STRS), "task states count mismatch");

  const char *str = nullptr;
  if (OB_UNLIKELY(state < TASK_INIT || state > TASK_MAX_STATE)) {
    str = "UNKNOWN";
  } else {
    str = TASK_INFO_STATE_STRS[state];
  }
  return str;
}

/*-----------------------------------------ObStorageIOPipelineTaskInfo-----------------------------------------*/
ObStorageIOPipelineTaskInfo::ObStorageIOPipelineTaskInfo()
  : state_(TASK_INIT),
    buf_size_(-1)
{
}

int ObStorageIOPipelineTaskInfo::assign(const ObStorageIOPipelineTaskInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this), K(other));
  } else {
    state_ = other.state_;
    buf_size_ = other.buf_size_;
  }
  return ret;
}

void ObStorageIOPipelineTaskInfo::reset()
{
  state_ = TASK_INIT;
  buf_size_ = -1;
}

bool ObStorageIOPipelineTaskInfo::is_valid() const
{
  // buf size should not equal to 0
  return is_valid_state(state_) && buf_size_ > 0;
}

int ObStorageIOPipelineTaskInfo::set_state(const TaskState target_state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_state(target_state))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid state", KR(ret), K(target_state), K(get_state_str(target_state)));
  } else {
    switch (state_) {
      case TASK_INIT:
      case TASK_READ_IN_PROGRESS:
      case TASK_READ_DONE:
      case TASK_WRITE_IN_PROGRESS:
      case TASK_WRITE_DONE: {
        if (OB_UNLIKELY(target_state != state_ + 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid state transition", KR(ret),
              KPC(this), K(target_state), K(get_state_str(target_state)));
        } else {
          state_ = target_state;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid state transition", KR(ret),
            KPC(this), K(target_state), K(get_state_str(target_state)));
        break;
      }
    }
  }
  return ret;
}

int ObStorageIOPipelineTaskInfo::set_macro_block_id(
    blocksstable::ObStorageObjectHandle &handle, const MacroBlockId &macro_id)
{
  return handle.set_macro_block_id(macro_id);
}

int ObStorageIOPipelineTaskInfo::set_macro_block_id(
    blocksstable::ObMacroBlockHandle &handle, const MacroBlockId &macro_id)
{
  return handle.set_macro_block_id(macro_id);
}

/*-----------------------------------------TaskInfoWithRWHandle-----------------------------------------*/
TaskInfoWithRWHandle::TaskInfoWithRWHandle()
    : ObStorageIOPipelineTaskInfo(),
      read_handle_(),
      write_handle_()
{
}

int TaskInfoWithRWHandle::assign(const TaskInfoWithRWHandle &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (OB_FAIL(ObStorageIOPipelineTaskInfo::assign(other))) {
    LOG_WARN("fail to assign base", KR(ret), K(other), KPC(this));
  } else {
    read_handle_ = other.read_handle_;
    write_handle_ = other.write_handle_;
  }
  return ret;
}

void TaskInfoWithRWHandle::reset()
{
  read_handle_.reset();
  write_handle_.reset();
  ObStorageIOPipelineTaskInfo::reset();
}

bool TaskInfoWithRWHandle::is_valid() const
{
  bool bret = ObStorageIOPipelineTaskInfo::is_valid();
  if (bret) {
    // When state >= TASK_READ_DONE, the read operation has completed and
    // subsequent steps require a valid read handle. During TASK_READ_IN_PROGRESS,
    // the read handle might still be invalid as the task could be in read preparation
    // phase without initiating actual I/O. Similarly, when state >= TASK_WRITE_DONE,
    // the write handle must be validated since write completion requires it for
    // following operations.
    if (state_ >= TASK_READ_DONE) {
      bret &= read_handle_.is_valid();
    }
    if (state_ >= TASK_WRITE_DONE) {
      bret &= write_handle_.is_valid();
    }
  }
  return bret;
}

int TaskInfoWithRWHandle::refresh_state(TaskState &cur_state)
{
  int ret = OB_SUCCESS;
  cur_state = TASK_MAX_STATE;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    if (TASK_READ_IN_PROGRESS == state_) {
      if (OB_FAIL(refresh_read_state_())) {
        LOG_WARN("fail to refresh read state", KR(ret), KPC(this));
      }
    } else if (TASK_WRITE_IN_PROGRESS == state_) {
      if (OB_FAIL(refresh_write_state_())) {
        LOG_WARN("fail to refresh write state", KR(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      cur_state = state_;
    }
  }
  return ret;
}

// internal func, skips validation check
int TaskInfoWithRWHandle::refresh_read_state_()
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  if (TASK_READ_IN_PROGRESS == state_) {
    if (OB_FAIL(read_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check is finished", KR(ret), KPC(this));
    } else if (is_finished) {
      if (OB_FAIL(read_handle_.wait())) {
        LOG_WARN("fail to wait result", KR(ret), KPC(this));
      } else {
        state_ = TASK_READ_DONE;
      }
    }
  }
  return ret;
}

int TaskInfoWithRWHandle::refresh_write_state_()
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  if (TASK_WRITE_IN_PROGRESS == state_) {
    if (OB_FAIL(write_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check is finished", KR(ret), KPC(this));
    } else if (is_finished) {
      if (OB_FAIL(write_handle_.wait())) {
        LOG_WARN("fail to wait result", KR(ret), KPC(this));
      } else {
        state_ = TASK_WRITE_DONE;
      }
    }
  }
  return ret;
}

void TaskInfoWithRWHandle::cancel()
{
  if (read_handle_.is_valid()) {
    read_handle_.reset();
  }
  if (write_handle_.is_valid()) {
    write_handle_.reset();
  }
  state_ = TASK_MAX_STATE;
}

} // storage
} // oceanbase