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

#include "observer/report/ob_ls_table_updater.h"
#include "observer/ob_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ob_define.h"
#include "lib/ob_running_mode.h"


namespace oceanbase
{
using namespace common;
using namespace share;
namespace observer
{
int ObLSTableUpdateTask::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool inner_table_only,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || OB_INVALID_TIMESTAMP == add_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task init failed", KR(ret), K(tenant_id), K(ls_id), K(add_timestamp));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    inner_table_only_ = inner_table_only;
    add_timestamp_ = add_timestamp;
  }
  return ret;
}

void ObLSTableUpdateTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  inner_table_only_ = false;
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObLSTableUpdateTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && OB_INVALID_TIMESTAMP != add_timestamp_;
}

int ObLSTableUpdateTask::assign(const ObLSTableUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    inner_table_only_ = other.inner_table_only_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

int64_t ObLSTableUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}

bool ObLSTableUpdateTask::operator ==(const ObLSTableUpdateTask &other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (tenant_id_ == other.tenant_id_
             && ls_id_ == other.ls_id_);
    if (equal && is_sys_tenant(tenant_id_)) {
      equal = (inner_table_only_ == other.inner_table_only_);
    }
  }
  return equal;
}

bool ObLSTableUpdateTask::operator !=(const ObLSTableUpdateTask &other) const
{
  return !(*this == other);
}

bool ObLSTableUpdateTask::compare_without_version(
    const ObLSTableUpdateTask &other) const
{
  return (*this == other);
}

int ObLSTableUpdateTask::assign_when_equal(
    const ObLSTableUpdateTask &other)
{
  UNUSED(other);
  return OB_NOT_SUPPORTED;
}

ObLSTableUpdateQueueSet::ObLSTableUpdateQueueSet(
    ObLSTableUpdater *updater)
    : inited_(false),
      updater_(updater),
      sys_tenant_queue_(),
      meta_tenant_queue_(),
      user_tenant_queue_()
{
}

ObLSTableUpdateQueueSet::~ObLSTableUpdateQueueSet()
{
}

int ObLSTableUpdateQueueSet::init()
{
  int ret = OB_SUCCESS;
  const int64_t user_thread_cnt =
      lib::is_mini_mode()
      ? MINI_MODE_UPDATE_THREAD_CNT
      : USER_TENANT_UPDATE_THREAD_CNT;
  const int64_t user_queue_size =
      lib::is_mini_mode()
      ? MINI_MODE_USER_TENANT_QUEUE_SIZE
      : USER_TENANT_QUEUE_SIZE;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("queue set has already inited", KR(ret));
  } else if (OB_FAIL(sys_tenant_queue_.init(
      updater_,
      MINI_MODE_UPDATE_THREAD_CNT,
      LST_TASK_QUEUE_SIZE,
      "LSSysTblUp"))) {
    LOG_WARN("sys_tenant_queue init failed", KR(ret), K(updater_),
        "thread_cnt", MINI_MODE_UPDATE_THREAD_CNT,
        "queue_size", LST_TASK_QUEUE_SIZE);
  } else if (OB_FAIL(meta_tenant_queue_.init(
      updater_,
      MINI_MODE_UPDATE_THREAD_CNT,
      LST_TASK_QUEUE_SIZE,
      "LSMetaTblUp"))) {
    LOG_WARN("meta_tenant_queue init failed", KR(ret), K(updater_),
        "thread_cnt", MINI_MODE_UPDATE_THREAD_CNT,
        "queue_size", LST_TASK_QUEUE_SIZE);
  } else if (OB_FAIL(user_tenant_queue_.init(
      updater_,
      user_thread_cnt,
      user_queue_size,
      "LSUserTblUp"))) {
    LOG_WARN("user_tenant_queue init failed",
        KR(ret), K(updater_), K(user_thread_cnt), K(user_queue_size));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObLSTableUpdateQueueSet::add_task(const ObLSTableUpdateTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  } else {
    const uint64_t &tenant_id = task.get_tenant_id();
    if (is_sys_tenant(tenant_id)) {
      if (task.is_inner_table_only()) {
        // only report sys tenant's ls to inner table
      } else if (OB_FAIL(sys_tenant_queue_.add(task))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("sys_tenant_queue add_task failed", KR(ret), K(task));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        ObLSTableUpdateTask new_task(task.get_tenant_id(),
                                     task.get_ls_id(),
                                     true/*inner_table_only*/,
                                     task.get_add_timestamp());
        if (OB_FAIL(meta_tenant_queue_.add(new_task))) {
          if (OB_EAGAIN != ret) {
            LOG_WARN("meta_tenant_queue add_task failed", KR(ret), K(new_task));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    } else if (is_meta_tenant(tenant_id)) {
      if (OB_FAIL(meta_tenant_queue_.add(task))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("meta_tenant_queue add_task failed", KR(ret), K(task));
        } else {
          ret = OB_SUCCESS;
        }
      }
    } else {
      if (OB_FAIL(user_tenant_queue_.add(task))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("user_tenant_queue add_task failed", KR(ret), K(task));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

void ObLSTableUpdateQueueSet::stop()
{
  sys_tenant_queue_.stop();
  meta_tenant_queue_.stop();
  user_tenant_queue_.stop();
}

void ObLSTableUpdateQueueSet::wait()
{
  sys_tenant_queue_.wait();
  meta_tenant_queue_.wait();
  user_tenant_queue_.wait();
}

int ObLSTableUpdater::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(update_queue_set_.init())) {
    LOG_WARN("init update_queue_set failed", KR(ret));
  } else {
    stopped_ = false;
    inited_ = true;
  }
  return ret;
}

void ObLSTableUpdater::stop()
{
  update_queue_set_.stop();
  stopped_ = true;
}

void ObLSTableUpdater::wait()
{
  update_queue_set_.wait();
}

int ObLSTableUpdater::async_update(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  bool inner_table_only = false;
  ObLSTableUpdateTask task(tenant_id, ls_id, inner_table_only, now);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(update_queue_set_.add_task(task))) {
    LOG_WARN("async_update failed", KR(ret), K(tenant_id), K(ls_id));
  } else {
    LOG_TRACE("add ls table update task success", K(task));
  }
  return ret;
}

int ObLSTableUpdater::batch_process_tasks(
    const common::ObIArray<ObLSTableUpdateTask> &tasks,
    bool &stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(stopped);
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("updater not init", KR(ret));
  } else if (1 != tasks.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task count", KR(ret), "tasks count", tasks.count());
	} else {
    const ObLSTableUpdateTask &task = tasks.at(0);
    const uint64_t tenant_id = task.get_tenant_id();
    const ObLSID &ls_id = task.get_ls_id();
    const bool inner_table_only = task.is_inner_table_only();
    bool tenant_dropped = false;
    bool schema_not_ready = false;
    uint64_t superior_tenant_id = OB_INVALID_TENANT_ID;
    ObLSReplica replica;
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
        || !ls_id.is_valid()
        || OB_ISNULL(GCTX.ob_service_)
        || OB_ISNULL(GCTX.lst_operator_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret));
    } else {
      // Make sure the superior tenant which stores meta table has not been dropped and it's schema is ready.
      // User tenant's superior tenant is meta; Meta tenant's superior tenant is sys; Sys tenant manages itself.
      superior_tenant_id = get_private_table_exec_tenant_id(tenant_id);
      (void)check_tenant_status_(superior_tenant_id, tenant_dropped, schema_not_ready); // ignore ret code
    }
    if (OB_SUCC(ret) && !tenant_dropped) {
      const int64_t start_time = ObTimeUtility::current_time();
      int64_t execute_time_us = 0;
      if (schema_not_ready) { // need wait tenant schema refresh
        ret = OB_NEED_WAIT;
        if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
          LOG_WARN("tenant schema is not ready, need wait", KR(ret), K(superior_tenant_id), K(task));
        }
      } else {
        const ObAddr &server = GCONF.self_addr_;
        if (OB_FAIL(GCTX.ob_service_->fill_ls_replica(
            tenant_id,
            ls_id,
            replica))) {
          if (OB_LS_NOT_EXIST == ret || OB_TENANT_NOT_IN_SERVER == ret) { // remove from table if not exist
            if (OB_FAIL(GCTX.lst_operator_->remove(tenant_id, ls_id, server, inner_table_only))) {
              LOG_WARN("fail to remove replica",
                  KR(ret), K(tenant_id), K(ls_id), "self_addr", server);
            } else {
              FLOG_INFO("REPORT:remove ls from meta table success", K(tenant_id), K(ls_id), K(server));
            }
          } else {
            LOG_WARN("fail to fill log stream replica", KR(ret), K(tenant_id), K(ls_id));
          }
        } else if (OB_FAIL(GCTX.lst_operator_->update(replica, inner_table_only))) {
          LOG_WARN("fail to update replica", KR(ret), K(tenant_id), K(ls_id), K(replica));
        } else {
          FLOG_INFO("REPORT:success to process update task", K(replica));
        }
        execute_time_us = ObTimeUtility::current_time() - start_time;
      }

      throttle(ls_id, ret, execute_time_us, stopped_);
      // reput task to queue if failed
      if (OB_FAIL(ret) && OB_FAIL(update_queue_set_.add_task(task))) {
        LOG_WARN("reput task failed", KR(ret), K(task));
      }
    } // OB_SUCC(ret)
  }
  return ret;
}

int ObLSTableUpdater::process_barrier(const ObLSTableUpdateTask &task, bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

int ObLSTableUpdater::check_tenant_status_(
    const uint64_t tenant_id,
    bool &tenant_dropped,
    bool &schema_not_ready)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  tenant_dropped = false;
  schema_not_ready = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, tenant_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else if (!schema_service->is_tenant_full_schema(tenant_id)) {
    // need wait schema refresh
    schema_not_ready = true;
  }
  return ret;
}

void ObLSTableUpdater::throttle(
    const ObLSID &ls_id,
    const int return_code,
    const int64_t execute_time_us,
    bool &stopped)
{
  int64_t sleep_us = 0;
  if (OB_SUCCESS != return_code) {
    if (ls_id.is_sys_ls()) {
      sleep_us = SYS_RETRY_INTERVAL_US;
    } else {
      sleep_us = RETRY_INTERVAL_US;
    }
  } else if (execute_time_us > SLOW_UPDATE_TIME_US) {
    if (ls_id.is_sys_ls()) {
      // won't limit update for sys log stream
      sleep_us = 0;
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "detected slow update for sys table", K(ls_id));
    } else {
      sleep_us = MIN(RETRY_INTERVAL_US, (execute_time_us - SLOW_UPDATE_TIME_US));
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "detected slow update, may be too many concurrent updating",
          K(ls_id), K(sleep_us));
    }
  }
  const static int64_t sleep_step_us = 20 * 1000; // 20ms
  for (; !stopped && sleep_us > 0; sleep_us -= sleep_step_us) {
    ob_usleep(static_cast<int32_t>(std::min(sleep_step_us, sleep_us)));
  }
}

} // end namespace observer
} // end namespace oceanbase
