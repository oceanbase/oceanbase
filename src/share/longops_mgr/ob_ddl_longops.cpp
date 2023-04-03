// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX SHARE

#include "ob_ddl_longops.h"
#include "rootserver/ddl_task/ob_ddl_task.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share;

ObDDLLongopsKey::ObDDLLongopsKey()
  : task_id_(OB_INVALID_ID)
{
}

int ObDDLLongopsKey::to_key_string()
{
  int ret = OB_SUCCESS;
  int64_t name_pos = 0;
  int64_t target_pos = 0;
  if (OB_FAIL(databuff_printf(name_, MAX_LONG_OPS_NAME_LENGTH, name_pos, "DDL TASK"))) {
    LOG_WARN("fail to set name string", K(ret));
  } else if (OB_FAIL(databuff_printf(target_, MAX_LONG_OPS_TARGET_LENGTH, target_pos, "task_id=%ld, ", task_id_))) {
    LOG_WARN("fail to convert index_table_id to string", K(ret));
  }
  return ret;
}

ObDDLLongopsStatCollector::ObDDLLongopsStatCollector()
  : is_inited_(false), ddl_task_(nullptr)
{
}

int ObDDLLongopsStatCollector::init(rootserver::ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLLongopsStatCollector init twice", K(ret));
  } else if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_task));
  } else {
    ddl_task_ = ddl_task;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLLongopsStatCollector::collect(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLLongopsStatCollector is not inited", K(ret));
  } else if (OB_FAIL(ddl_task_->collect_longops_stat(value))) {
    LOG_WARN("failed to collect ddl task longops stat", K(ret));
  }
  return ret;
}

ObDDLLongopsStat::ObDDLLongopsStat()
  : is_inited_(false), key_(), value_(), collector_()
{
}

int ObDDLLongopsStat::get_longops_value(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  value.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLLongopsStat is not inited", K(ret));
  } else if (OB_FAIL(collector_.collect(value))) {
    LOG_WARN("failed to collect longops value", K(ret));
  } else {
    value_ = value;
  }
  return ret;
}

int ObDDLLongopsStat::init(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLLongopsStat init twice", K(ret));
  } else if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_task));
  } else if (OB_FAIL(collector_.init(ddl_task))) {
    LOG_WARN("failed to init collector", K(ret));
  } else {
    key_.tenant_id_ = ddl_task->get_tenant_id();
    key_.task_id_ = ddl_task->get_task_id();
    key_.to_key_string();
    is_inited_ = true;
  }
  return ret;
}
