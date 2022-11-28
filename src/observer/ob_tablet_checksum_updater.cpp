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

#include "ob_tablet_checksum_updater.h"
#include "observer/ob_service.h"
#include "share/ob_tablet_replica_checksum_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

ObTabletChecksumUpdateTask::ObTabletChecksumUpdateTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    tablet_id_(),
    add_timestamp_(OB_INVALID_TIMESTAMP)
{
}

ObTabletChecksumUpdateTask::ObTabletChecksumUpdateTask(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t add_timestamp)
  : tenant_id_(tenant_id),
    ls_id_(ls_id),
    tablet_id_(tablet_id),
    add_timestamp_(add_timestamp)
{
}

ObTabletChecksumUpdateTask::~ObTabletChecksumUpdateTask()
{
  reset();
}

void ObTabletChecksumUpdateTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  tablet_id_.reset();
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObTabletChecksumUpdateTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && tablet_id_.is_valid_with_tenant(tenant_id_)
      && 0 < add_timestamp_;
}

int ObTabletChecksumUpdateTask::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || !tablet_id.is_valid_with_tenant(tenant_id)
      || 0 >= add_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", KR(ret), K(tenant_id), K(ls_id), K(tablet_id), K(add_timestamp));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    add_timestamp_ = add_timestamp;
  }
  return ret;
}

int64_t ObTabletChecksumUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

void ObTabletChecksumUpdateTask::check_task_status() const
{
  // TODO @danling.fjk ignore this error log temp
  // int64_t now = ObTimeUtility::current_time();
  // const int64_t safe_interval = TABLET_CHECK_INTERVAL;
  // // print an error log if this task is not executed correctly since two mins ago
  // if (now - add_timestamp_ > safe_interval) {
  //   LOG_ERROR("tablet table update task cost too much time to execute",
  //             K(*this), K(safe_interval), "cost_time", now - add_timestamp_);
  // }
}

int ObTabletChecksumUpdateTask::assign(const ObTabletChecksumUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

bool ObTabletChecksumUpdateTask::operator==(const ObTabletChecksumUpdateTask &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else {
    bret = (tenant_id_ == other.tenant_id_
            && ls_id_ == other.ls_id_
            && tablet_id_ == other.tablet_id_);
  }
  return bret;
}

bool ObTabletChecksumUpdateTask::operator!=(const ObTabletChecksumUpdateTask &other) const
{
  return !(operator==(other));
}

bool ObTabletChecksumUpdateTask::compare_without_version(
    const ObTabletChecksumUpdateTask &other) const
{
  return (operator==(other));
}


ObTabletChecksumUpdater::ObTabletChecksumUpdater()
  : inited_(false),
    stopped_(true),
    ob_service_(nullptr),
    task_queue_()
{
}

ObTabletChecksumUpdater::~ObTabletChecksumUpdater()
{
  destroy();
}

int ObTabletChecksumUpdater::init(ObService &ob_service)
{
  int ret = OB_SUCCESS;
  const int64_t task_queue_size = !lib::is_mini_mode()
                                    ? TASK_QUEUE_SIZE
                                    : MINI_MODE_TASK_QUEUE_SIZE;
  const int64_t task_thread_cnt = !lib::is_mini_mode()
                                  ? UPDATE_TASK_THREAD_CNT
                                  : MINI_MODE_UPDATE_TASK_THREAD_CNT;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_FAIL(task_queue_.init(this,
      task_thread_cnt, task_queue_size, "TbltCksUp"))) {
    LOG_WARN("failed to init tablet checksum updater task queue", KR(ret),
             "thread_count", task_thread_cnt,
             "queue_size", task_queue_size);
  } else {
    ob_service_ = &ob_service;
    inited_ = true;
    stopped_ = false;
    LOG_INFO("success to init ObTabletChecksumUpdater");
  }
  return ret;
}

void ObTabletChecksumUpdater::stop()
{
  if (inited_) {
    stopped_ = true;
    task_queue_.stop();
    LOG_INFO("stop ObTabletChecksumUpdater success");
  }
}

void ObTabletChecksumUpdater::wait()
{
  if (inited_) {
    task_queue_.wait();
    LOG_INFO("wait ObTabletChecksumUpdater");
  }
}

void ObTabletChecksumUpdater::destroy()
{
  stop();
  wait();
  inited_ = false;
  stopped_ = true;
  ob_service_ = nullptr;
}

int ObTabletChecksumUpdater::async_update(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t add_timestamp = ObTimeUtility::current_time();
  ObTabletChecksumUpdateTask task;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletChecksumUpdater not inited", KR(ret));
  } else if (tablet_id.is_reserved_tablet() || is_virtual_tenant_id(tenant_id)) {
    LOG_TRACE("no need to report virtual tenant's tablet and reserved tablet",
             KR(ret), K(tablet_id), K(tenant_id));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid()
             || !ls_id.is_valid_with_tenant(tenant_id)
             || !tablet_id.is_valid()
             || !tablet_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(task.init(tenant_id,
                               ls_id,
                               tablet_id,
                               add_timestamp))) {
    LOG_WARN("set update task failed", KR(ret), K(tenant_id), K(ls_id), K(tablet_id),
             K(add_timestamp));
  } else if (OB_FAIL(add_task_(task))){
    LOG_WARN("fail to add task", KR(ret), K(tenant_id), K(ls_id), K(tablet_id),
             K(add_timestamp));
  }
  return ret;
}

int ObTabletChecksumUpdater::add_task_(const ObTabletChecksumUpdateTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else if (OB_FAIL(task_queue_.add(task))){
    // TODO: deal with barrier-tasks when execute
    if (OB_EAGAIN == ret) {
      LOG_TRACE("tablet table update task exist", K(task));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("add tablet table update task failed", KR(ret), K(task));
    }
  }
  return ret;
}

int ObTabletChecksumUpdater::reput_to_queue_(const ObIArray<ObTabletChecksumUpdateTask> &tasks)
{
  int ret = OB_SUCCESS;
  // try to push task back to queue, ignore ret code
  for (int64_t i = 0; i < tasks.count(); i++) {
    const ObTabletChecksumUpdateTask &task = tasks.at(i);
    if (!task.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid task", KR(ret), K(task));
    } else if (OB_FAIL(add_task_(task))) {
      LOG_ERROR("fail to reput to queue", KR(ret), K(task));
    }
  }
  return ret;
}

int ObTabletChecksumUpdater::process_barrier(
    const ObTabletChecksumUpdateTask &task,
    bool &stopped)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(task, stopped);
  return ret;
}

int ObTabletChecksumUpdater::batch_process_tasks(
    const common::ObIArray<ObTabletChecksumUpdateTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(stopped);
  int tmp_ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  ObArray<ObTabletReplicaChecksumItem> update_tablet_items;
  UpdateTabletChecksumTaskList update_tasks;
  RemoveTabletChecksumTaskList remove_tasks;
  ObCurTraceId::init(GCONF.self_addr_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletChecksumUpdater is not inited", KR(ret));
  } else if (OB_FAIL(generate_tasks_(tasks, update_tablet_items, update_tasks, remove_tasks))){
    if (OB_TENANT_NOT_IN_SERVER != ret && OB_NOT_RUNNING != ret) {
      LOG_ERROR("generate_tasks failed", KR(ret), "tasks count", tasks.count(),
                "update_tablet_items", update_tablet_items.count(),
                "update_tasks", update_tasks.count(),
                "remove_tasks", remove_tasks.count());
    } else {
      LOG_WARN("Tenant/LogStream has been stopped, skip to process tasks", KR(ret));
    }
  } else {
    tmp_ret = do_batch_update_(start_time, update_tasks, update_tablet_items);
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("do_batch_update_ failed", KR(tmp_ret), K(start_time), K(update_tasks),
               K(update_tablet_items));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("REPORT: success to update tablets", KR(tmp_ret), K(update_tablet_items));
    }
    tmp_ret = do_batch_remove_(start_time, remove_tasks);
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("do_batch_remove_ failed", KR(tmp_ret), K(start_time), K(remove_tasks));
    } else {
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("REPORT: success to remove tablets", KR(tmp_ret), K(remove_tasks));
    }
  }
  return ret;
}

int ObTabletChecksumUpdater::generate_tasks_(
    const ObIArray<ObTabletChecksumUpdateTask> &tasks,
    ObIArray<ObTabletReplicaChecksumItem> &update_tablet_items,
    UpdateTabletChecksumTaskList &update_tasks,
    RemoveTabletChecksumTaskList &remove_tasks)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t count = UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletChecksumUpdater is not inited", KR(ret));
  } else if (OB_ISNULL(ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), KP_(ob_service));
  } else if (OB_UNLIKELY(tasks.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tasks count <= 0", KR(ret), "tasks_count", tasks.count());
  } else if (FALSE_IT(tenant_id = tasks.at(0).get_tenant_id())) {
    // shall never be here
  } else {
    ObTabletReplicaChecksumItem item;
    FOREACH_CNT_X(task, tasks, OB_SUCC(ret)) {
      // split tasks into remove and update
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task", KR(ret), K(task));
      } else if (tenant_id != task->get_tenant_id()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tenant_id not the same", KR(ret), K(tenant_id), K(task));
      } else {
        task->check_task_status();
        item.reset();
        if (OB_FAIL(ob_service_->fill_tablet_replica_checksum_item(task->get_tenant_id(),
            task->get_ls_id(), task->get_tablet_id(), item))) {
          if (OB_TABLET_NOT_EXIST == ret || OB_LS_NOT_EXIST == ret) {
            LOG_INFO("try update a not exist or invalid tablet, turn to remove tablet table",
                     KR(ret), "tenant_id", task->get_tenant_id(),
                     "ls_id", task->get_ls_id(),
                     "tablet_id", task->get_tablet_id());
            ret = OB_SUCCESS;
            if (OB_FAIL(remove_tasks.reserve(count))) {
              // reserve() is reentrant, do not have to check whether first time
              LOG_WARN("fail to reserver remove_tasks", KR(ret), K(count));
            } else if (OB_FAIL(remove_tasks.push_back(*task))) {
              LOG_WARN("fail to push back remove task", KR(ret), KPC(task));
            }
          } else {
            LOG_WARN("fail to fill tablet item", KR(ret), "tenant_id", task->get_tenant_id(),
                     "ls_id", task->get_ls_id(), "tablet_id", task->get_tablet_id());
          }
        } else {
          LOG_TRACE("fill tablet checksum item success", K(task), K(item));
          if (OB_FAIL(update_tablet_items.reserve(count))) {
            // reserve() is reentrant, do not have to check whether first time
            LOG_WARN("fail to reserve update_tablet_items", KR(ret), K(count));
          } else if (OB_FAIL(update_tasks.reserve(count))) {
            // reserve() is reentrant, do not have to check whether first time
            LOG_WARN("fail to reserve update_tasks", KR(ret), K(count));
          } else if (OB_FAIL(update_tablet_items.push_back(item))) {
              LOG_WARN("fail to push back item", KR(ret), K(item));
          } else if (OB_FAIL(update_tasks.push_back(*task))) {
            LOG_WARN("fail to push back task", KR(ret), KPC(task));
          }
        }
      }
    } //FOREACH

    if (OB_SUCC(ret)
        && (update_tasks.count() != update_tablet_items.count()
            || update_tasks.count() + remove_tasks.count() != tasks.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet task count and item count not match", KR(ret),
               "tablet_update_tasks count", update_tasks.count(),
               "tablet_update_items count", update_tablet_items.count(),
               "tablet_remove_tasks count", remove_tasks.count(),
               "tasks count", tasks.count());
    }
  }
  return ret;
}

int ObTabletChecksumUpdater::do_batch_update_(
    const int64_t start_time,
    const ObIArray<ObTabletChecksumUpdateTask> &tasks,
    const ObIArray<ObTabletReplicaChecksumItem> &items)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  if (tasks.count() != items.count() || 0 == tasks.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tasks num not match", KR(ret), "task_cnt", tasks.count(), "item_cnt", items.count());
  } else if (FALSE_IT(tenant_id = tasks.at(0).get_tenant_id())) {
  } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_update(tenant_id, items, *GCTX.sql_proxy_))) {
    LOG_WARN("do tablet table update failed, try to reput to queue", KR(ret),
             "escape time", ObTimeUtility::current_time() - start_time);
    (void) throttle_(ret, ObTimeUtility::current_time() - start_time);
    if (OB_SUCCESS != (tmp_ret = reput_to_queue_(tasks))) {
      LOG_ERROR("fail to reput update task to queue", KR(tmp_ret), K(tasks.count()));
    } else {
      LOG_INFO("reput update task to queue success", K(tasks.count()));
    }
  } else {
    LOG_INFO("batch process update success", KR(ret), K(items.count()),
             "use_time", ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObTabletChecksumUpdater::do_batch_remove_(
    const int64_t start_time,
    const ObIArray<ObTabletChecksumUpdateTask> &tasks)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t tenant_id = OB_INVALID_TENANT_ID;
  if (0 == tasks.count()) {
    LOG_INFO("no need to remove task", KR(ret), "task_cnt", tasks.count());
  } else if (FALSE_IT(tenant_id = tasks.at(0).get_tenant_id())) {
  } else {
    ObArray<share::ObLSID> ls_ids;
    ObArray<common::ObTabletID> tablet_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      if (OB_FAIL(ls_ids.push_back(tasks.at(i).ls_id_))) {
        LOG_WARN("failed to add ls id", KR(ret));
      } else if (OB_FAIL(tablet_ids.push_back(tasks.at(i).tablet_id_))) {
        LOG_WARN("failed to add tablet id", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObTabletReplicaChecksumOperator::batch_remove(tenant_id, ls_ids, tablet_ids, *GCTX.sql_proxy_))) {
      LOG_WARN("do tablet checksum remove failed, try to reput to queue", KR(ret),
               "escape time", ObTimeUtility::current_time() - start_time);
      (void) throttle_(ret, ObTimeUtility::current_time() - start_time);
      if (OB_SUCCESS != (tmp_ret = reput_to_queue_(tasks))) {
        LOG_ERROR("fail to reput remove task to queue", KR(tmp_ret), K(tasks.count()));
      } else {
        LOG_INFO("reput remove task to queue success", K(tasks.count()));
      }
    } else {
      LOG_INFO("batch process remove success", KR(ret), K(tasks.count()),
             "use_time", ObTimeUtility::current_time() - start_time);
    }
  }
  return ret;
}

int ObTabletChecksumUpdater::throttle_(
    const int return_code,
    const int64_t execute_time_us)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  if (OB_SUCCESS != return_code) {
    sleep_us = 2l * 1000 * 1000; // 2s
  } else if (execute_time_us > 20 * 1000 * 1000) { // 20s
    sleep_us = MIN(1L * 1000 * 1000, (execute_time_us - 20 * 1000 * 1000));
    LOG_WARN("detected slow update, may be too many concurrent updating", K(sleep_us));
  }
  const static int64_t sleep_step_us = 20 * 1000; // 20ms
  for (; !stopped_ && sleep_us > 0;
      sleep_us -= sleep_step_us) {
    ob_usleep(static_cast<int32_t>(std::min(sleep_step_us, sleep_us)));
  }
  return ret;
}


} // observer
} // oceanbase
