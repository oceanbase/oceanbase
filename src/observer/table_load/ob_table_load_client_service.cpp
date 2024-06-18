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

#include "observer/table_load/ob_table_load_client_service.h"
#include "observer/table_load/ob_table_load_client_task.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace table;

/**
 * ClientTaskBriefEraseIfExpired
 */

bool ObTableLoadClientService::ClientTaskBriefEraseIfExpired::operator()(
  const ObTableLoadUniqueKey &key, ObTableLoadClientTaskBrief *client_task_brief) const
{
  return client_task_brief->active_time_ < expired_ts_;
}

/**
 * ObTableLoadClientService
 */

ObTableLoadClientService::ObTableLoadClientService() : next_task_id_(1), is_inited_(false) {}

ObTableLoadClientService::~ObTableLoadClientService() {}

int ObTableLoadClientService::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadClientService init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(
          client_task_map_.create(bucket_num, "TLD_ClientTask", "TLD_ClientTask", MTL_ID()))) {
      LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
    } else if (OB_FAIL(client_task_brief_map_.init("TLD_ClientBrief", MTL_ID()))) {
      LOG_WARN("fail to init link hashmap", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

ObTableLoadClientService *ObTableLoadClientService::get_client_service()
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  ObTableLoadClientService *client_service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    client_service = &service->get_client_service();
  }
  return client_service;
}

int ObTableLoadClientService::alloc_task(ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadClientTask *new_client_task = nullptr;
    if (OB_ISNULL(new_client_task =
                    OB_NEW(ObTableLoadClientTask, ObMemAttr(MTL_ID(), "TLD_ClientTask")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadClientTask", KR(ret));
    } else {
      new_client_task->task_id_ = service->get_client_service().generate_task_id();
      client_task = new_client_task;
      client_task->inc_ref_count();
    }
    if (OB_FAIL(ret)) {
      if (nullptr != new_client_task) {
        free_task(new_client_task);
        new_client_task = nullptr;
      }
    }
  }
  return ret;
}

void ObTableLoadClientService::free_task(ObTableLoadClientTask *client_task)
{
  if (OB_NOT_NULL(client_task)) {
    OB_ASSERT(0 == client_task->get_ref_count());
    OB_DELETE(ObTableLoadClientTask, "TLD_ClientTask", client_task);
    client_task = nullptr;
  }
}

void ObTableLoadClientService::revert_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(client_task));
  } else {
    const int64_t ref_count = client_task->dec_ref_count();
    OB_ASSERT(ref_count >= 0);
    if (0 == ref_count) {
      const int64_t task_id = client_task->task_id_;
      const uint64_t table_id = client_task->param_.get_table_id();
      LOG_INFO("free client task", K(task_id), K(table_id), KP(client_task));
      free_task(client_task);
      client_task = nullptr;
    }
  }
}

int ObTableLoadClientService::add_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(client_task->param_.get_table_id(), client_task->task_id_);
    ret = service->get_client_service().add_client_task(key, client_task);
  }
  return ret;
}

int ObTableLoadClientService::remove_task(ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    ObTableLoadUniqueKey key(client_task->param_.get_table_id(), client_task->task_id_);
    ret = service->get_client_service().remove_client_task(key, client_task);
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
  } else {
    if (OB_FAIL(service->get_client_service().get_client_task(key, client_task))) {
      LOG_WARN("fail to get client task", KR(ret), K(key));
    }
  }
  return ret;
}

int ObTableLoadClientService::add_client_task(const ObTableLoadUniqueKey &key,
                                              ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid() || nullptr == client_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key), KP(client_task));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.set_refactored(key, client_task))) {
      if (OB_UNLIKELY(OB_HASH_EXIST != ret)) {
        LOG_WARN("fail to set refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_EXIST;
      }
    } else {
      client_task->inc_ref_count(); // hold by map
    }
  }
  return ret;
}

int ObTableLoadClientService::remove_client_task(const ObTableLoadUniqueKey &key,
                                                 ObTableLoadClientTask *client_task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(key));
  } else {
    HashMapEraseIfEqual erase_if_equal(client_task);
    bool is_erased = false;
    obsys::ObWLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.erase_if(key, erase_if_equal, is_erased))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
        LOG_WARN("fail to erase refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_UNLIKELY(!is_erased)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected client task", KR(ret), KPC(client_task));
    }
    if (OB_SUCC(ret)) {
      client_task->dec_ref_count();
    }
    // add client task brief
    if (OB_SUCC(ret)) {
      ObTableLoadClientTaskBrief *client_task_brief = nullptr;
      if (OB_FAIL(client_task_brief_map_.create(key, client_task_brief))) {
        LOG_WARN("fail to create client task brief", KR(ret), K(key));
      } else {
        client_task_brief->task_id_ = client_task->task_id_;
        client_task_brief->table_id_ = client_task->param_.get_table_id();
        client_task->get_status(client_task_brief->client_status_, client_task_brief->error_code_);
        client_task_brief->result_info_ = client_task->result_info_;
        client_task_brief->active_time_ = ObTimeUtil::current_time();
      }
      if (nullptr != client_task_brief) {
        client_task_brief_map_.revert(client_task_brief);
        client_task_brief = nullptr;
      }
    }
  }
  return ret;
}

int ObTableLoadClientService::get_all_client_task(
  ObIArray<ObTableLoadClientTask *> &client_task_array)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    client_task_array.reset();
    obsys::ObWLockGuard guard(rwlock_);
    for (ClientTaskMap::const_iterator iter = client_task_map_.begin();
         OB_SUCC(ret) && iter != client_task_map_.end(); ++iter) {
      const ObTableLoadUniqueKey &key = iter->first;
      ObTableLoadClientTask *client_task = iter->second;
      if (OB_FAIL(client_task_array.push_back(client_task))) {
        LOG_WARN("fail to push back", KR(ret), K(key));
      } else {
        client_task->inc_ref_count();
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < client_task_array.count(); ++i) {
        ObTableLoadClientTask *client_task = client_task_array.at(i);
        client_task->dec_ref_count();
      }
      client_task_array.reset();
    }
  }
  return ret;
}

int ObTableLoadClientService::get_client_task(const ObTableLoadUniqueKey &key,
                                              ObTableLoadClientTask *&client_task)
{
  int ret = OB_SUCCESS;
  client_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(client_task_map_.get_refactored(key, client_task))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      client_task->inc_ref_count();
    }
  }
  return ret;
}

int64_t ObTableLoadClientService::get_client_task_count() const
{
  obsys::ObRLockGuard guard(rwlock_);
  return client_task_map_.size();
}

void ObTableLoadClientService::purge_client_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    ObArray<ObTableLoadClientTask *> client_task_array;
    client_task_array.set_tenant_id(MTL_ID());
    if (OB_FAIL(get_all_client_task(client_task_array))) {
      LOG_WARN("fail to get all client task", KR(ret));
    }
    for (int64_t i = 0; i < client_task_array.count(); ++i) {
      ObTableLoadClientTask *client_task = client_task_array.at(i);
      ObTableLoadClientStatus client_status = client_task->get_status();
      if (client_status != ObTableLoadClientStatus::COMMIT &&
          client_status != ObTableLoadClientStatus::ABORT) {
        // ignore
      } else if (client_task->get_ref_count() > 2) {
        // ignore
      }
      // remove client task
      else if (OB_FAIL(remove_task(client_task))) {
        LOG_WARN("fail to remove client task", KR(ret), KPC(client_task));
      }
      revert_task(client_task);
    }
  }
}

int ObTableLoadClientService::get_task_brief(const ObTableLoadUniqueKey &key,
                                             ObTableLoadClientTaskBrief *&client_task_brief)
{
  int ret = OB_SUCCESS;
  ObTableLoadService *service = nullptr;
  if (OB_ISNULL(service = MTL(ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", KR(ret));
  } else {
    if (OB_FAIL(service->get_client_service().get_client_task_brief(key, client_task_brief))) {
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
    if (OB_FAIL(service->get_client_service().revert_client_task_brief(client_task_brief))) {
      LOG_WARN("fail to revert client task brief", KR(ret), KP(client_task_brief));
    }
  }
}

int ObTableLoadClientService::get_client_task_brief(const ObTableLoadUniqueKey &key,
                                                    ObTableLoadClientTaskBrief *&client_task_brief)
{
  int ret = OB_SUCCESS;
  client_task_brief = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(client_task_brief_map_.get(key, client_task_brief))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get refactored", KR(ret), K(key));
      }
    }
  }
  return ret;
}

int ObTableLoadClientService::revert_client_task_brief(
  ObTableLoadClientTaskBrief *client_task_brief)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == client_task_brief)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(client_task_brief));
  } else {
    client_task_brief_map_.revert(client_task_brief);
  }
  return ret;
}

void ObTableLoadClientService::purge_client_task_brief()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadClientService not init", KR(ret), KP(this));
  } else {
    const int64_t expired_ts = ObTimeUtil::current_time() - CLIENT_TASK_RETENTION_PERIOD;
    ClientTaskBriefEraseIfExpired erase_if_expired(expired_ts);
    if (OB_FAIL(client_task_brief_map_.remove_if(erase_if_expired))) {
      LOG_WARN("fail to remove if client task brief", KR(ret));
    }
  }
}

} // namespace observer
} // namespace oceanbase
