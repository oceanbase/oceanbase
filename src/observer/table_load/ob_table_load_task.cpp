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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_task.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObTableLoadTask::ObTableLoadTask(uint64_t tenant_id)
  : trace_id_(*ObCurTraceId::get_trace_id()),
    allocator_("TLD_Task", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    processor_(nullptr),
    callback_(nullptr)
{
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
