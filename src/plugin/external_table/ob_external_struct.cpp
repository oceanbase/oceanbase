/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_struct.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace plugin {

////////////////////////////////////////////////////////////////////////////////
// ObExternalTableSchema

////////////////////////////////////////////////////////////////////////////////
// ObExternalTableScanParam
ObExternalTableScanParam::~ObExternalTableScanParam()
{
  reset();
}

int ObExternalTableScanParam::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (FALSE_IT(mem_attr_.tenant_id_ = MTL_ID())) {
  } else if (FALSE_IT(mem_attr_.label_ = OB_PLUGIN_MEMORY_LABEL)) {
  } else if (FALSE_IT(columns_.set_attr(mem_attr_))) {
  } else {
    inited_ = true;
  }
  return ret;
}

void ObExternalTableScanParam::reset()
{
  if (inited_) {
    columns_.reset();
    task_.reset();
    storage_param_ = nullptr;
    inited_ = false;
  }
}

void ObExternalTableScanParam::set_storage_param(const storage::ObTableScanParam *storage_param)
{
  storage_param_ = storage_param;
}

void ObExternalTableScanParam::set_task(const ObString &task)
{
  task_ = task;
}
int ObExternalTableScanParam::append_column(const ObString &column_name)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(columns_.push_back(column_name))) {
    LOG_WARN("failed to push column to columns", K(ret), K(column_name));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////
// ObExternalTaskList
int ObExternalTaskList::append_task(const char buf[], int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObString task_src(buf_len, buf);
  ObString task_dst;
  if (OB_FAIL(ob_write_string(allocator_, task_src, task_dst))) {
    LOG_WARN("failed to write task", K(ret), K(task_src));
  } else if (OB_FAIL(task_list_.push_back(task_dst))) {
    LOG_WARN("failed to append task", K(ret), K(task_dst));
    allocator_.free(task_dst.ptr());
  }
  return ret;
}
} // namespace plugin
} // namespace oceanbase
