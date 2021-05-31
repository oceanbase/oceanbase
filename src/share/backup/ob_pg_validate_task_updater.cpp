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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_pg_validate_task_updater.h"
#include "share/backup/ob_validate_operator.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace share {

ObPGValidateTaskUpdater::ObPGValidateTaskUpdater() : is_inited_(false), is_dropped_tenant_(false), sql_client_(NULL)
{}

ObPGValidateTaskUpdater::~ObPGValidateTaskUpdater()
{}

int ObPGValidateTaskUpdater::init(common::ObISQLClient& sql_client, bool is_dropped_tenant)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pg validate task updater init twice", K(ret));
  } else {
    sql_client_ = &sql_client;
    is_dropped_tenant_ = is_dropped_tenant;
    is_inited_ = true;
  }
  return ret;
}

int ObPGValidateTaskUpdater::update_pg_task_info(const common::ObIArray<common::ObPartitionKey>& pkeys,
    const common::ObAddr& addr, const share::ObTaskId& task_id, const ObPGValidateTaskInfo::ValidateStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;
  uint64_t write_tenant_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task updater do not init", K(ret));
  } else if (pkeys.empty() || status >= ObPGValidateTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update pg validate task status get invalid argument", K(ret), K(status));
  } else if (FALSE_IT(write_tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : pkeys.at(0).get_tenant_id())) {
    // set write tenant id
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_SIZE ? remain_cnt : MAX_BATCH_SIZE;
      if (OB_FAIL(trans.start(sql_client_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(report_idx + i);
          if (OB_FAIL(ObPGValidateTaskOperator::update_pg_task_info(write_tenant_id, trans, pkey))) {
            LOG_WARN("failed to update pg task info", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            OB_LOG(WARN, "failed to end trans", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            OB_LOG(WARN, "failed to end trans", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        report_idx += cur_batch_cnt;
      }
    }
  }
  // TODO
  UNUSED(addr);
  UNUSED(task_id);
  return ret;
}

int ObPGValidateTaskUpdater::update_pg_validate_task_status(
    const common::ObIArray<common::ObPartitionKey>& pkeys, const ObPGValidateTaskInfo::ValidateStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;
  uint64_t write_tenant_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task updater do not init", K(ret));
  } else if (pkeys.empty() || status >= ObPGValidateTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update pg validate task status get invalid argument", K(ret), K(status));
  } else if (FALSE_IT(write_tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : pkeys.at(0).get_tenant_id())) {
    // set write tenant id
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_SIZE ? remain_cnt : MAX_BATCH_SIZE;
      if (OB_FAIL(trans.start(sql_client_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(report_idx + i);
          if (OB_FAIL(ObPGValidateTaskOperator::update_pg_task_status(write_tenant_id, trans, pkey, status))) {
            LOG_WARN("failed to update pg validate task status");
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            OB_LOG(WARN, "failed to end transaction", K(ret), K(pkeys));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            OB_LOG(WARN, "failed to end transaction", K(ret), K(tmp_ret), K(pkeys));
          }
        }
      }
      if (OB_SUCC(ret)) {
        report_idx += cur_batch_cnt;
      }
    }
  }
  return ret;
}

int ObPGValidateTaskUpdater::update_status_and_result(const int64_t backup_set_id,
    const common::ObIArray<common::ObPartitionKey>& pkeys, const common::ObIArray<int32_t>& results,
    const ObPGValidateTaskInfo::ValidateStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;
  uint64_t write_tenant_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task updater do not init", K(ret));
  } else if (pkeys.empty() || status >= ObPGValidateTaskInfo::MAX || pkeys.count() != results.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg validate task updater get invalid arguments", K(ret), K(status));
  } else if (FALSE_IT(write_tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : pkeys.at(0).get_tenant_id())) {
    // set write tenant id
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_SIZE ? remain_cnt : MAX_BATCH_SIZE;
      if (OB_FAIL(trans.start(sql_client_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(report_idx + i);
          const int32_t result = results.at(report_idx + i);
          if (OB_FAIL(ObPGValidateTaskOperator::update_pg_task_result_and_status(
                  write_tenant_id, backup_set_id, *sql_client_, pkey, status, result))) {
            LOG_WARN("failed to update status and result", K(ret), K(pkey));
          } else {
            LOG_INFO("update pg task result and status success", K(ret), K(pkey), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            OB_LOG(WARN, "failed to end transaction", K(ret), K(pkeys));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            OB_LOG(WARN, "failed to end transaction", K(ret), K(pkeys));
          }
        }
      }
      if (OB_SUCC(ret)) {
        report_idx += cur_batch_cnt;
      }
    }
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_pg_validate_task(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, const common::ObPartitionKey& pkey,
    ObPGValidateTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0 || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg validate task get invalid arguments", K(ret), K(incarnation), K(backup_set_id), K(pkey));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_pg_task(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, pkey, *sql_client_, pg_task_info))) {
    LOG_WARN("failed to get pg validate task", K(ret));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_pg_validate_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task updater do not init", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg validate task get invalid argument", K(ret), K(task_tenant_id));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_pg_tasks(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, *sql_client_, pg_task_infos))) {
    LOG_WARN("get pg validate tasks failed", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_doing_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pending pg tasks get invalid argument", K(ret), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_doing_task(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_pending_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pending pg tasks get invalid argument", K(ret), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_pending_task(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_finished_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, const int64_t backup_set_id, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get finished pg tasks get invalid argument", K(ret), K(task_tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_finished_task(
                 tenant_id, job_id, task_tenant_id, incarnation, backup_set_id, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get finished pg tasks", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_pending_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pending pg tasks get invalid argument", K(ret), K(task_tenant_id), K(incarnation));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_pending_task(
                 tenant_id, job_id, task_tenant_id, incarnation, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get pending pg tasks", K(ret), K(tenant_id), K(task_tenant_id));
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_finished_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id,
    const int64_t incarnation, common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id || incarnation < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get finished pg tasks get invalid argument", K(ret), K(task_tenant_id), K(incarnation));
  } else if (OB_FAIL(ObPGValidateTaskOperator::get_finished_task(
                 tenant_id, job_id, task_tenant_id, incarnation, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get finished pg tasks");
  }
  return ret;
}

int ObPGValidateTaskUpdater::get_one_doing_pg_tasks(const uint64_t tenant_id, const ObPGValidateTaskInfo* pg_task,
    common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  ObPGValidateTaskRowKey row_key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get one doing pg tasks get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(row_key.set(pg_task))) {
    LOG_WARN("failed to set row key", K(ret), K(pg_task));
  } else if (OB_FAIL(
                 ObPGValidateTaskOperator::get_one_doing_pg_tasks(tenant_id, row_key, *sql_client_, pg_task_infos))) {
    LOG_WARN("failed to get one doing pg tasks", K(ret));
  }
  return ret;
}

int ObPGValidateTaskUpdater::delete_all_pg_tasks(const int64_t job_id, const uint64_t task_tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const int64_t max_delete_rows = 1024;
  const uint64_t tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : task_tenant_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_ID == task_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete all pg tasks get invalid argument", K(ret), K(task_tenant_id));
  } else {
    do {
      if (OB_FAIL(ObPGValidateTaskOperator::batch_remove_pg_task(
              tenant_id, job_id, task_tenant_id, max_delete_rows, *sql_client_, affected_rows))) {
        LOG_WARN("failed to batch remove pg validate tasks", K(ret), K(tenant_id), K(task_tenant_id));
      }
    } while (OB_SUCC(ret) && affected_rows > 0);
  }
  return ret;
}

int ObPGValidateTaskUpdater::batch_report_pg_task(const common::ObIArray<ObPGValidateTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg validate task upater do not init", K(ret));
  } else if (pg_task_infos.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg task infos should not be empty", K(ret));
  } else if (FALSE_IT(tenant_id = is_dropped_tenant_ ? OB_SYS_TENANT_ID : pg_task_infos.at(0).tenant_id_)) {
    // set tenant id
  } else if (OB_FAIL(ObPGValidateTaskOperator::batch_report_pg_task(tenant_id, pg_task_infos, *sql_client_))) {
    LOG_WARN("failed to batch report pg validate task", K(ret), K(tenant_id), K(pg_task_infos.count()));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
