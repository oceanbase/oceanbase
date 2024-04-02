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

#define USING_LOG_PREFIX RS

#include "ob_disaster_recovery_task_table_updater.h"

#include "share/ob_define.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"   // for OB_ALL_LS_REPLICA_TASK_TNAME
#include "rootserver/ob_disaster_recovery_task_mgr.h"  // for ObDRTaskMgr
#include "share/schema/ob_multi_version_schema_service.h" // for GSCHEMASERVICE

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
int ObDRTaskTableUpdateTask::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObDRTaskType &task_type,
    const share::ObTaskId &task_id,
    const ObDRTaskKey &task_key,
    const int ret_code,
    const bool need_clear_server_data_in_limit,
    const bool need_record_event,
    const ObDRTaskRetComment &ret_comment,
    const int64_t add_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || rootserver::ObDRTaskType::MAX_TYPE == task_type
      || task_id.is_invalid())
      || OB_INVALID_TIMESTAMP == add_timestamp) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task init failed", KR(ret), K(tenant_id), K(ls_id),
             K(task_type), K(task_id), K(add_timestamp));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_type_ = task_type;
    task_id_ = task_id;
    task_key_ = task_key;
    ret_code_ = ret_code;
    need_clear_server_data_in_limit_ = need_clear_server_data_in_limit;
    need_record_event_ = need_record_event;
    ret_comment_ = ret_comment;
    add_timestamp_ = add_timestamp;
  }
  return ret;
}

int ObDRTaskTableUpdateTask::assign(const ObDRTaskTableUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    task_type_ = other.task_type_;
    task_id_ = other.task_id_;
    task_key_ = other.task_key_;
    ret_code_ = other.ret_code_;
    need_clear_server_data_in_limit_ = other.need_clear_server_data_in_limit_;
    need_record_event_ = other.need_record_event_;
    ret_comment_ = other.ret_comment_;
    add_timestamp_ = other.add_timestamp_;
  }
  return ret;
}

void ObDRTaskTableUpdateTask::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  task_type_ = rootserver::ObDRTaskType::MAX_TYPE;
  task_id_.reset();
  ret_code_ = OB_SUCCESS;
  need_clear_server_data_in_limit_ = false;
  need_record_event_ = true;
  ret_comment_ = ObDRTaskRetComment::MAX;
  add_timestamp_ = OB_INVALID_TIMESTAMP;
}

bool ObDRTaskTableUpdateTask::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid_with_tenant(tenant_id_)
      && rootserver::ObDRTaskType::MAX_TYPE != task_type_
      && !task_id_.is_invalid();
}

bool ObDRTaskTableUpdateTask::operator ==(const ObDRTaskTableUpdateTask &other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (tenant_id_ == other.tenant_id_
        && ls_id_ == other.ls_id_
        && task_type_ == other.task_type_
        && task_id_ == other.task_id_);
  }
  return equal;
}

bool ObDRTaskTableUpdateTask::operator !=(const ObDRTaskTableUpdateTask &other) const
{
  return !(*this == other);
}

bool ObDRTaskTableUpdateTask::compare_without_version(
    const ObDRTaskTableUpdateTask &other) const
{
  return (*this == other);
}

int ObDRTaskTableUpdater::init(
    common::ObMySQLProxy *sql_proxy,
    ObDRTaskMgr *task_mgr)
{
  int ret = OB_SUCCESS;
  const int64_t thread_cnt =
      lib::is_mini_mode()
      ? MINI_MODE_UPDATE_THREAD_CNT
      : UPDATE_THREAD_CNT;
  const int64_t queue_size =
      TASK_QUEUE_SIZE;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(sql_proxy) || OB_ISNULL(task_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is nullptr", KR(ret));
  } else if (OB_FAIL(update_queue_.init_only(this,
                                             thread_cnt,
                                             queue_size,
                                             "DRTaskTbUp"))) {
    LOG_WARN("init update_queue_set failed", KR(ret));
  } else {
    inited_ = true;
    stopped_ = true;
    sql_proxy_ = sql_proxy;
    task_mgr_ = task_mgr;
  }
  return ret;
}

int ObDRTaskTableUpdater::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRTaskTableUpdater is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!stopped_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not start ObDRTaskTableUpdater twice", KR(ret), K_(stopped));
  } else if (OB_FAIL(update_queue_.start())) {
    LOG_WARN("fail to start update queue", KR(ret));
  } else {
    stopped_ = false;
  }
  return ret;
}

void ObDRTaskTableUpdater::stop()
{
  update_queue_.stop();
  stopped_ = true;
  LOG_INFO("stop ObDRTaskTableUpdater success");
}

void ObDRTaskTableUpdater::wait()
{
  update_queue_.wait();
  LOG_INFO("wait ObDRTaskTableUpdater");
}

void ObDRTaskTableUpdater::destroy()
{
  stop();
  wait();
  inited_ = false;
  stopped_ = true;
  sql_proxy_ = nullptr;
  task_mgr_ = nullptr;
}

int ObDRTaskTableUpdater::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || stopped_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDRTaskTableUpdater is not inited or is stopped", KR(ret), K_(inited), K_(stopped));
  }
  return ret;
}

int ObDRTaskTableUpdater::async_update(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObDRTaskType &task_type,
    const ObDRTaskKey &task_key,
    const int ret_code,
    const bool need_clear_server_data_in_limit,
    const share::ObTaskId &task_id,
    const bool need_record_event,
    const ObDRTaskRetComment &ret_comment)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  ObDRTaskTableUpdateTask task;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else if (OB_FAIL(task.init(tenant_id, ls_id, task_type, task_id,
                               task_key, ret_code, need_clear_server_data_in_limit,
                               need_record_event, ret_comment, now))) {
    LOG_WARN("fail to init a update task", KR(ret), K(tenant_id), K(ls_id), K(task_type), K(task_id),
             K(task_key), K(ret_code), K(need_clear_server_data_in_limit), K(need_record_event),
             K(ret_comment), K(now));
  } else if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (OB_FAIL(update_queue_.add(task))) {
    LOG_WARN("async_update failed", KR(ret), K(tenant_id), K(ls_id),
             K(task_type), K(task_id));
  } else {
    FLOG_INFO("success to async update a task into queue", K(task));
  }
  return ret;
}

int ObDRTaskTableUpdater::batch_process_tasks(
    const common::ObIArray<ObDRTaskTableUpdateTask> &tasks,
    bool &stopped)
{
  UNUSED(stopped);
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else {
    int64_t end_idx = tasks.count();
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0 ; i < end_idx; i++) {
      if (OB_SUCCESS != (tmp_ret = process_task_(tasks.at(i)))) {
        LOG_WARN("fail to do process_task", KR(tmp_ret), "task_info", tasks.at(i));
      } else {
        LOG_INFO("success to do process_task", KR(tmp_ret), "task_info", tasks.at(i));
      }
    }
  }
  return ret;
}

int ObDRTaskTableUpdater::process_task_(
    const ObDRTaskTableUpdateTask &task)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_DELETE_DRTASK_FROM_INNER_TABLE);
  common::ObMySQLTransaction trans;
  int64_t affected_rows = 0;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(task.get_tenant_id());
  char task_id_to_set[OB_TRACE_STAT_BUFFER_SIZE] = "";
  ObSqlString sql;
  bool has_dropped = false;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret), K_(inited), K_(stopped));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(task_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy_ or task_mgr_ is nullptr", KR(ret), KP(sql_proxy_), KP(task_mgr_));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else if (false == task.get_task_id().to_string(task_id_to_set, sizeof(task_id_to_set))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert task id to string failed", KR(ret), "task_id", task.get_task_id());
  } else if (OB_UNLIKELY(!is_valid_tenant_id(task.get_tenant_id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "tenant_id", task.get_tenant_id());
  } else if (OB_FAIL(GSCHEMASERVICE.check_if_tenant_has_been_dropped(task.get_tenant_id(), has_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), "tenant_id", task.get_tenant_id());
  } else {
    // tenant exist, have to delete task from table and memory
    if (has_dropped) {
    } else if (OB_FAIL(trans.start(sql_proxy_, sql_tenant_id))) {
      LOG_WARN("start transaction failed", KR(ret), K(sql_tenant_id));
    } else {
      if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %lu "
          "AND task_type = '%s' AND task_id = '%s'",
          share::OB_ALL_LS_REPLICA_TASK_TNAME,
          task.get_tenant_id(),
          task.get_ls_id().id(),
          ob_disaster_recovery_task_type_strs(task.get_task_type()),
          task_id_to_set))) {
        LOG_WARN("assign sql string failed", KR(ret), K(task));
      } else if (OB_FAIL(sql_proxy_->write(sql_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", KR(ret), "sql", sql.ptr(), K(task), K(sql_tenant_id));
      } else if (!is_single_row(affected_rows)) {
        // ignore affected row check for task not exist
        LOG_INFO("expected deleted single row",
            K(affected_rows), K(sql), K(task), K(sql_tenant_id));
      }
    }
    if (FAILEDx(task_mgr_->do_cleaning(
                     task.get_task_id(),
                     task.get_task_key(),
                     task.get_ret_code(),
                     task.get_need_clear_server_data_in_limit(),
                     task.get_need_record_event(),
                     task.get_ret_comment()))) {
      LOG_WARN("fail to clean task info inside memory", KR(ret), K(task));
    } else {
      LOG_INFO("success to delete row from ls disaster task table and do cleaning",
               K(affected_rows), K(sql), K(task), K(sql_tenant_id));
    }
    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end transaction failed", KR(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }
  return ret;
}

int ObDRTaskTableUpdater::process_barrier(
    const ObDRTaskTableUpdateTask &task,
    bool &stopped)
{
  UNUSEDx(task, stopped);
  return OB_NOT_SUPPORTED;
}

} // end namespace observer
} // end namespace oceanbase
