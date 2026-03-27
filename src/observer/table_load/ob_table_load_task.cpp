/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_task.h"
#include "src/share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObTableLoadTask::ObTableLoadTask(uint64_t tenant_id)
  : trace_id_(*ObCurTraceId::get_trace_id()),
    allocator_("TLD_Task"),
    processor_(nullptr),
    callback_(nullptr)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadTask::~ObTableLoadTask()
{
  if (nullptr != processor_) {
    processor_->~ObITableLoadTaskProcessor();
    allocator_.free(processor_);
    processor_ = nullptr;
  }
  if (nullptr != callback_) {
    callback_->~ObITableLoadTaskCallback();
    allocator_.free(callback_);
    callback_ = nullptr;
  }
}

int ObTableLoadTask::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(processor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null processor", KR(ret));
  } else {
    ret = processor_->process();
  }
  return ret;
}

void ObTableLoadTask::callback(int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(callback_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null callback", KR(ret));
  } else {
    callback_->callback(ret_code, this);
  }
}

}  // namespace observer
}  // namespace oceanbase
