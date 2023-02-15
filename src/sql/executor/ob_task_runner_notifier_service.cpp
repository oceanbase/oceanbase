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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_task_runner_notifier_service.h"
#include "sql/executor/ob_task_info.h"
#include "sql/executor/ob_task.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObTaskRunnerNotifierService *ObTaskRunnerNotifierService::instance_ = NULL;

ObTaskRunnerNotifierService::ObTaskRunnerNotifierService()
  : inited_(false),
    notifier_map_()
{
}

ObTaskRunnerNotifierService::~ObTaskRunnerNotifierService()
{
}

void ObTaskRunnerNotifierService::ObKillTaskRunnerNotifier::operator()(
    hash::HashMapPair<ObTaskID, ObTaskRunnerNotifier*> &entry)
{
  ret_ = OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_ERROR_RET(ret_, "notifier is NULL", K(ret_), "key", entry.first);
  } else {
    if (OB_SUCCESS != (ret_ = entry.second->kill())) {
      LOG_WARN_RET(ret_, "notify kill task failed", K(ret_));
    }
  }
}


ObTaskRunnerNotifierService::Guard::Guard(const ObTaskID &task_id, ObTaskRunnerNotifier *notifier)
  : task_id_(task_id)
{
  int ret = OB_SUCCESS;
  if (NULL != notifier && NULL != ObTaskRunnerNotifierService::get_instance()) {
    if (OB_FAIL(ObTaskRunnerNotifierService::get_instance()->register_notifier(
        task_id, notifier))) {
      LOG_WARN("register notifier failed", K(ret), K(task_id));
    }
  }
}

ObTaskRunnerNotifierService::Guard::~Guard()
{
  int ret = OB_SUCCESS;
  if (NULL != ObTaskRunnerNotifierService::get_instance()) {
    if (OB_FAIL(ObTaskRunnerNotifierService::get_instance()->unregister_notifier(task_id_))) {
      LOG_WARN("delete notifier failed", K(ret), K(task_id_));
    }
  }
}

void ObTaskRunnerNotifierService::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(notifier_map_.destroy())) {
    LOG_ERROR("fail to destroy notifier map", K(ret));
  }
  inited_ = false;
}

int ObTaskRunnerNotifierService::build_instance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != instance_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("instance is not NULL, build twice", K(ret));
  } else if (OB_ISNULL(instance_ = OB_NEW(ObTaskRunnerNotifierService,
                                          "TaskRunnerSer"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("instance is NULL, unexpected", K(ret));
  } else if (OB_FAIL(instance_->init())) {
    instance_->reset();
    OB_DELETE(ObTaskRunnerNotifierService, "TaskRunnerSer", instance_);
    instance_ = NULL;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to init task runner notifier service", K(ret));
  } else {}
  return ret;
}

ObTaskRunnerNotifierService *ObTaskRunnerNotifierService::get_instance()
{
  ObTaskRunnerNotifierService *instance = NULL;
  if (OB_ISNULL(instance_) || OB_UNLIKELY(!instance_->inited_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "instance is NULL or not inited", K(instance_));
  } else {
    instance = instance_;
  }
  return instance;
}

int ObTaskRunnerNotifierService::register_notifier(const ObTaskID &key,
                                                   ObTaskRunnerNotifier *notifier)
{
  int ret = OB_SUCCESS;
  ObTaskRunnerNotifierService *notifier_service = NULL;
  if (OB_ISNULL(notifier)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("notifier is NULL", K(ret));
  } else if (OB_ISNULL(notifier_service =
                       ObTaskRunnerNotifierService::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("notifier service is NULL", K(ret));
  } else if (OB_FAIL(notifier_service->set_notifier(key, notifier))) {
    LOG_WARN("fail to set notifier", K(ret));
  } else {}
  return ret;
}

int ObTaskRunnerNotifierService::unregister_notifier(const ObTaskID &key)
{
  int ret = OB_SUCCESS;
  ObTaskRunnerNotifierService *notifier_service = NULL;
  if (OB_ISNULL(notifier_service =
                ObTaskRunnerNotifierService::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("notifier service is NULL", K(ret));
  } else if (OB_FAIL(notifier_service->erase_notifier(key))) {
    LOG_WARN("fail to erase notifier", K(ret));
  } else {}
  return ret;
}

int ObTaskRunnerNotifierService::kill_task_runner(const ObTaskID &key, bool *is_running)
{
  int ret = OB_SUCCESS;
  ObTaskRunnerNotifierService *notifier_service = NULL;
  if (OB_ISNULL(notifier_service =
                ObTaskRunnerNotifierService::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("notifier service is NULL", K(ret));
  } else {
    ObKillTaskRunnerNotifier kill_notifier;
    if (OB_FAIL(notifier_service->atomic(key, kill_notifier))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to atomic kill", K(ret), K(key));
      } else {
        // this notifier does not exist, it can be considered to be successful
        ret = OB_SUCCESS;
        if (OB_NOT_NULL(is_running)) {
          *is_running = false;
        }
        LOG_WARN("this notifiter not exist, return success", K(ret), K(key));
      }
    } else {
      if (OB_NOT_NULL(is_running)) {
        *is_running = true;
      }
    }
  }
  return ret;
}

int ObTaskRunnerNotifierService::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(notifier_map_.create(NOTIFIER_MAP_BUCKET_SIZE,
                                   "TaskRunnerSer"))) {
    LOG_WARN("fail to create notifier map", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObTaskRunnerNotifierService::set_notifier(const ObTaskID &key, ObTaskRunnerNotifier *notifier)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(notifier)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("notifier is NULL", K(ret));
  } else if (OB_FAIL(notifier_map_.set_refactored(key, notifier))) {
    LOG_WARN("fail to set notifier into map", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTaskRunnerNotifierService::erase_notifier(const ObTaskID &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(notifier_map_.erase_refactored(key))) {
    LOG_WARN("fail to erase notifier from map", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

template<class _callback>
int ObTaskRunnerNotifierService::atomic(const ObTaskID &key, _callback &callback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(notifier_map_.atomic_refactored(key, callback))) {
    LOG_WARN("fail to atomic do callback", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */




