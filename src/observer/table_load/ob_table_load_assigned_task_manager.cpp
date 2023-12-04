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

#include "observer/table_load/ob_table_load_assigned_task_manager.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace table;

/**
 * ObTableLoadAssignedTaskManager
 */
ObTableLoadAssignedTaskManager::ObTableLoadAssignedTaskManager()
  : is_inited_(false)
{
}

ObTableLoadAssignedTaskManager::~ObTableLoadAssignedTaskManager()
{
}

int ObTableLoadAssignedTaskManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadAssignedTaskManager init twice", KR(ret), KP(this));
  } else if (OB_FAIL(assigned_tasks_map_.create(bucket_num, "TLD_AssignedMgr", "TLD_AssignedMgr", MTL_ID()))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTableLoadAssignedTaskManager::add_assigned_task(ObDirectLoadResourceApplyArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadAssignedTaskManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(assigned_tasks_map_.set_refactored(arg.task_key_, arg))) {
      if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {
        LOG_WARN("fail to set refactored", KR(ret), K(arg.task_key_));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    }
  }
  LOG_INFO("ObTableLoadAssignedTaskManager::add_assigned_task", KR(ret), K(arg));

  return ret;
}

int ObTableLoadAssignedTaskManager::delete_assigned_task(ObTableLoadUniqueKey &task_key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadAssignedTaskManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(task_key));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(assigned_tasks_map_.erase_refactored(task_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("ObTableLoadAssignedTaskManager::delete_assigned_task", KR(ret), K(task_key));

  return ret;
}

int ObTableLoadAssignedTaskManager::get_assigned_tasks(ObSArray<ObDirectLoadResourceApplyArg> &assigned_tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadAssignedTaskManager not init", KR(ret), KP(this));
  } else {
    ObMutexGuard guard(mutex_);
    if (OB_FAIL(assigned_tasks.reserve(assigned_tasks_map_.size()))) {
      LOG_WARN("fail to reserve assigned_tasks", KR(ret), K(assigned_tasks));
    } else {
      for(ResourceApplyMap::iterator iter = assigned_tasks_map_.begin(); iter != assigned_tasks_map_.end(); iter++) {
        if (OB_FAIL(assigned_tasks.push_back(iter->second))) {
          LOG_WARN("fail to push_back", KR(ret), K(assigned_tasks));
          break;
        }
      }
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase