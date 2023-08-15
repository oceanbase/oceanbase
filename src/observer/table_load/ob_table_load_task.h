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

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace observer
{
class ObITableLoadTaskProcessor;
class ObITableLoadTaskCallback;

class ObTableLoadTask
{
  friend class ObITableLoadTaskProcessor;
  friend class ObITableLoadTaskCallback;
public:
  ObTableLoadTask(uint64_t tenant_id);
  ~ObTableLoadTask();
  template<typename Processor, typename... Args>
  int set_processor(Args&&... args);
  template<typename Callback, typename... Args>
  int set_callback(Args&&... args);
  const common::ObCurTraceId::TraceId get_trace_id() const { return trace_id_; }
  ObITableLoadTaskProcessor *get_processor() const { return processor_; }
  ObITableLoadTaskCallback *get_callback() const { return callback_; }
  bool is_valid() const { return nullptr != processor_ && nullptr != callback_; }
  int do_work();
  void callback(int ret_code);
  TO_STRING_KV(KPC_(processor), KP_(callback));
protected:
  common::ObCurTraceId::TraceId trace_id_;
  common::ObArenaAllocator allocator_;
  ObITableLoadTaskProcessor *processor_;
  ObITableLoadTaskCallback *callback_;
private:
  DISABLE_COPY_ASSIGN(ObTableLoadTask);
};

template<typename Processor, typename... Args>
int ObTableLoadTask::set_processor(Args&&... args)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(processor_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected alloc processor", KR(ret));
  } else {
    if (OB_ISNULL(processor_ = OB_NEWx(Processor, (&allocator_), *this, args...))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to new processor", KR(ret));
    }
  }
  return ret;
}

template<typename Callback, typename... Args>
int ObTableLoadTask::set_callback(Args&&... args)
{
  int ret = common::OB_SUCCESS;
  if (OB_NOT_NULL(callback_)) {
    ret = common::OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected alloc callback", KR(ret));
  } else {
    if (OB_ISNULL(callback_ = OB_NEWx(Callback, (&allocator_), args...))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to new callback", KR(ret));
    }
  }
  return ret;
}

class ObITableLoadTaskProcessor
{
public:
  ObITableLoadTaskProcessor(ObTableLoadTask &task)
    : parent_(task), allocator_(task.allocator_) {}
  virtual ~ObITableLoadTaskProcessor() = default;
  virtual int process() = 0;
  VIRTUAL_TO_STRING_KV(KP(this));
protected:
  ObTableLoadTask &parent_;
  common::ObIAllocator &allocator_;
private:
  DISABLE_COPY_ASSIGN(ObITableLoadTaskProcessor);
};

class ObITableLoadTaskCallback
{
public:
  ObITableLoadTaskCallback() = default;
  virtual ~ObITableLoadTaskCallback() = default;
  virtual void callback(int ret_code, ObTableLoadTask *task) = 0;
private:
  DISABLE_COPY_ASSIGN(ObITableLoadTaskCallback);
};

}  // namespace observer
}  // namespace oceanbase
