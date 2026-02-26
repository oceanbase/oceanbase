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

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_service.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

/**
 * ObTableLoadClientService
 */

int64_t ObTableLoadClientService::next_task_sequence_ = 0;

int64_t ObTableLoadClientService::generate_task_id()
{
  return ObTimeUtil::current_time() * 1000 + ATOMIC_FAA(&next_task_sequence_, 1) % 1000;
}

int ObTableLoadClientService::alloc_task(ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else if (OB_FAIL(service->get_manager().acquire_client_task(client_task))) {
    LOG_WARN("fail to acquire client task", KR(ret));
  }
  return ret;
}

void ObTableLoadClientService::revert_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->get_manager().revert_client_task(client_task);
  }
}

int ObTableLoadClientService::add_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else {
    ObTableLoadUniqueKey key(client_task->get_table_id(), client_task->get_task_id());
    ret = service->get_manager().add_client_task(key, client_task);
  }
  return ret;
}

int ObTableLoadClientService::get_task(const ObTableLoadUniqueKey &key,
                                       ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else {
    ret = service->get_manager().get_client_task(key, client_task);
  }
  return ret;
}

int ObTableLoadClientService::get_task_brief(const ObTableLoadUniqueKey &key,
                                             ObTableLoadClientTaskBrief *&client_task_brief)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else if (OB_UNLIKELY(service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("service is stop", KR(ret));
  } else {
    if (OB_FAIL(service->get_manager().get_client_task_brief(key, client_task_brief))) {
      LOG_WARN("fail to get client task brief", KR(ret), K(key));
    } else {
      // update active time
      client_task_brief->active_time_ = ObTimeUtil::current_time();
    }
  }
  return ret;
}

void ObTableLoadClientService::revert_task_brief(ObTableLoadClientTaskBrief *client_task_brief)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    service->get_manager().revert_client_task_brief(client_task_brief);
  }
}

} // namespace observer
} // namespace oceanbase
