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

#ifndef OCEANBASE_CLOG_OB_LOG_EXT_RING_BUFFER_H_
#define OCEANBASE_CLOG_OB_LOG_EXT_RING_BUFFER_H_

#include "share/ob_define.h"
#include "lib/container/ob_ext_ring_buffer.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
namespace clog {
class ObILogExtRingBufferData : public common::ObExternalRef::IERefPtr {
public:
  virtual bool can_overwrite(const ObILogExtRingBufferData* data) = 0;

  virtual bool can_be_removed() = 0;

  void reset()
  {}
  void on_quiescent()
  {
    destroy();
  }
  virtual void destroy() = 0;
};

class ObILogTaskCallBack {
public:
  ObILogTaskCallBack()
  {}
  virtual ~ObILogTaskCallBack()
  {}

public:
  virtual int sliding_cb(const int64_t sn, const ObILogExtRingBufferData* data) = 0;
};

struct ObLogExtRingBufferPopCond {
  ObLogExtRingBufferPopCond(ObILogTaskCallBack* task_cb)
      : ret_(common::OB_SUCCESS), task_cb_(task_cb), is_replay_failed_(false)
  {}
  // Pop condition.
  // NULL can't be popped, and pop() should return OB_EAGAIN.
  // if can_be_removed() returns false, pop() should return OB_EAGAIN.
  // on_pop() is called before data been popped.
  bool operator()(const int64_t sn, ObILogExtRingBufferData* data)
  {
    bool ret = false;
    ret_ = common::OB_SUCCESS;
    if (NULL == data) {
      // NULL can't be popped, return false, err code is OB_EAGAIN.
      ret = false;
      ret_ = common::OB_EAGAIN;
    } else if (!data->can_be_removed()) {
      // can_be_removed() returns false, return false, err code is OB_EAGAIN.
      ret = false;
      ret_ = common::OB_EAGAIN;
    } else if (common::OB_SUCCESS != (ret_ = task_cb_->sliding_cb(sn, data))) {
      // on_pop() err, return false, err code is on_pop()'s return code.
      ret = false;
      is_replay_failed_ = true;
    } else {
      // can pop, err code is OB_SUCCESS.
      ret = true;
    }
    return ret;
  }
  int ret_;
  ObILogTaskCallBack* task_cb_;
  bool is_replay_failed_;
};

struct ObLogExtRingBufferTruncateCond {
  // Pop everything.
  bool operator()(const int64_t sn, ObILogExtRingBufferData* data)
  {
    UNUSED(sn);
    UNUSED(data);
    return true;
  }
};

struct ObLogExtRingBufferSetCond {
  ObLogExtRingBufferSetCond() : olddata_(NULL)
  {}
  // Set condition. NULL can always be overwritten. olddata_ records the replaced ptr.
  bool operator()(ObILogExtRingBufferData* olddata, ObILogExtRingBufferData* newdata)
  {
    // Old and new data can be NULL.
    bool ret = false;
    olddata_ = olddata;
    if ((NULL == olddata) || (NULL != olddata && olddata->can_overwrite(newdata))) {
      ret = true;
    }
    return ret;
  }
  ObILogExtRingBufferData* olddata_;
};

class ObLogExtRingBuffer : protected common::ObExtendibleRingBuffer<ObILogExtRingBufferData> {
  typedef ObLogExtRingBuffer MyType;
  typedef common::ObExtendibleRingBuffer<ObILogExtRingBufferData> BaseType;

public:
  ObLogExtRingBuffer();
  virtual ~ObLogExtRingBuffer();
  inline int init(const int64_t start_id);
  int destroy();

public:
  int get(const int64_t id, ObILogExtRingBufferData*& val, const int64_t*& ref) const;
  int revert(const int64_t* ref);
  int set(const int64_t id, ObILogExtRingBufferData* data);
  // -- TODO -- block seems unnecessary.
  int pop(const bool block, const int64_t timeout, bool& is_replay_failed, ObILogTaskCallBack* task_cb);
  /*
   * Truncate ringbuffer & set new start id.
   * Notice:
   *  - not thread safe.
   * Error:
   *  - ...
   */
  int truncate(const int64_t new_start_id);
  int64_t get_start_id() const;

private:
  int truncate_(const int64_t new_start_id);

private:
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExtRingBuffer);
};

// Definitions.

inline ObLogExtRingBuffer::ObLogExtRingBuffer() : BaseType(), inited_(false)
{}

inline ObLogExtRingBuffer::~ObLogExtRingBuffer()
{
  destroy();
}

inline int ObLogExtRingBuffer::init(const int64_t start_id)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    CLOG_LOG(WARN, "double init", K(ret));
  } else if (common::OB_SUCCESS != (ret = BaseType::init(start_id))) {
    CLOG_LOG(WARN, "failed to init base", K(ret), K(start_id));
  } else {
    inited_ = true;
  }
  return ret;
}

inline int ObLogExtRingBuffer::destroy()
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    CLOG_LOG(INFO, "ObLogExtRingBuffer::destroy not inited");
  } else if (common::OB_SUCCESS != (ret = truncate(end_sn()))) {
    CLOG_LOG(WARN, "failed to truncate", K(ret));
  } else if (common::OB_SUCCESS != (ret = BaseType::destroy())) {
    CLOG_LOG(WARN, "failed to destroy base", K(ret));
  } else {
    inited_ = false;
  }
  return ret;
}

inline int ObLogExtRingBuffer::get(const int64_t id, ObILogExtRingBufferData*& val, const int64_t*& ref) const
{
  int ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != (ret = BaseType::get(id, val))) {
    // Donnot print WARN log.
    // CLOG_LOG(WARN, "failed to get data", K(ret), K(id));
  }
  ref = (int64_t*)val;
  // It's in expectation that user may get() something out of upper bound.
  // So convert this error code to ob_success.
  if (common::OB_ERR_OUT_OF_UPPER_BOUND == ret) {
    ret = common::OB_SUCCESS;
  } else if (common::OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    ret = common::OB_ERROR_OUT_OF_RANGE;
  }
  return ret;
}

inline int ObLogExtRingBuffer::revert(const int64_t* ref)
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else if (NULL == ref) {
    // pass
  } else {
    get_val_ref().release((common::ObExternalRef::IERefPtr*)ref, get_retire_list());
  }
  return ret;
}

inline int ObLogExtRingBuffer::set(const int64_t id, ObILogExtRingBufferData* data)
{
  int ret = common::OB_SUCCESS;
  ObLogExtRingBufferSetCond cond;
  bool set = false;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else if (NULL == data) {
    ret = common::OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args", K(ret), K(id), K(data));
  } else if (common::OB_SUCCESS != (ret = BaseType::set(id, data, cond, set))) {
    CLOG_LOG(WARN, "failed to set data", K(ret), K(id), K(data), K(set));
  } else if (!set) {
    // cond returns false;
    ret = common::OB_EAGAIN;
  } else if (NULL != cond.olddata_) {
    get_val_ref().retire(cond.olddata_, get_retire_list());
  }
  return ret;
}

inline int ObLogExtRingBuffer::pop(
    const bool block, const int64_t timeout, bool& is_replay_failed, ObILogTaskCallBack* task_cb)
{
  UNUSED(block);
  int ret = common::OB_SUCCESS;
  ObLogExtRingBufferPopCond cond(task_cb);
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "not init", K(ret));
  } else {
    int64_t begin_time = common::ObClockGenerator::getClock();
    while (OB_SUCC(ret)) {
      bool popped = false;
      ObILogExtRingBufferData* data = NULL;
      ret = BaseType::pop(cond, data, popped);
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        // Empty queue.
        ret = common::OB_EAGAIN;
      } else if (OB_FAIL(ret)) {
        // Error.
        CLOG_LOG(WARN, "failed to pop", K(ret), K(data), K(popped));
      } else {
        if (!popped) {
          // Not popped, use cond's ret.
          ret = cond.ret_;
        } else {
          get_val_ref().retire(data, get_retire_list());
        }
      }
      if (OB_SUCC(ret) && (timeout != 0) && ((common::ObClockGenerator::getClock() - begin_time) > timeout)) {
        ret = common::OB_CLOG_SLIDE_TIMEOUT;
        break;
      }
    }
    if (common::OB_EAGAIN == ret) {
      ret = common::OB_SUCCESS;
      is_replay_failed = cond.is_replay_failed_;
    }
  }
  return ret;
}

inline int ObLogExtRingBuffer::truncate_(const int64_t new_start_id)
{
  int ret = common::OB_SUCCESS;

  // Param check.
  if (new_start_id < begin_sn()) {
    ret = common::OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args", K(ret), K(new_start_id), "begin_sn", begin_sn());
  }

  // Prepare ringbuffer if new start id is greater than the last log id.
  if (common::OB_SUCCESS == ret && end_sn() < new_start_id) {
    // Set a NULL ptr at [new_start_id - 1], so new_start_id == end_sn().
    // Ptrs before end_sn() are set to NULL automatically.
    if (common::OB_SUCCESS != (ret = BaseType::set(new_start_id - 1, NULL))) {
      CLOG_LOG(WARN, "failed to set data", K(ret), K(new_start_id));
    }
  }

  // Keep popping till new_start_id.
  ObLogExtRingBufferTruncateCond cond;
  while (common::OB_SUCCESS == ret && begin_sn() < new_start_id) {
    bool popped = false;
    ObILogExtRingBufferData* data = NULL;
    ret = BaseType::pop(cond, data, popped);
    if (common::OB_SUCCESS != ret || !popped) {
      // Should never happened.
      CLOG_LOG(WARN, "failed to truncate", K(ret), K(new_start_id), "begin_sn", begin_sn());
      ret = common::OB_ERR_UNEXPECTED;
    } else if (NULL != data) {
      get_val_ref().retire(data);
    }
  }

  return ret;
}

inline int ObLogExtRingBuffer::truncate(const int64_t new_start_id)
{
  int ret = common::OB_SUCCESS;
  if (new_start_id < end_sn()) {
    ret = truncate_(new_start_id);
  } else {
    if (OB_FAIL(truncate_(end_sn()))) {
      CLOG_LOG(WARN, "failed to clear sliding window", K(ret));
    } else {
      set_range(new_start_id, new_start_id);
    }
  }
  return ret;
}

inline int64_t ObLogExtRingBuffer::get_start_id() const
{
  return BaseType::begin_sn();
}

}  // namespace clog
}  // namespace oceanbase

#endif
