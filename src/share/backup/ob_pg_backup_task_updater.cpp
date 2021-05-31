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
#include "ob_pg_backup_task_updater.h"

using namespace oceanbase;
using namespace common;
using namespace share;

ObPGBackupTaskUpdater::ObPGBackupTaskUpdater() : is_inited_(false), sql_proxy_(NULL)
{}

int ObPGBackupTaskUpdater::init(common::ObISQLClient& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pg backup task updater init twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObPGBackupTaskUpdater::update_pg_backup_task_status(
    const common::ObIArray<common::ObPartitionKey>& pkeys, const ObPGBackupTaskInfo::BackupStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update pg backup task get invalid argument", K(ret), K(status));
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_COUNT ? remain_cnt : MAX_BATCH_COUNT;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(i + report_idx);
          if (OB_FAIL(ObPGBackupTaskOperator::update_pg_backup_task_status(trans, status, pkey))) {
            LOG_WARN("failed to update pg backup task status", K(ret), K(status), K(pkey));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(pkeys));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*not commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(tmp_ret), K(pkeys));
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

int ObPGBackupTaskUpdater::update_pg_task_info(const common::ObIArray<common::ObPartitionKey>& pkeys,
    const common::ObAddr& addr, const share::ObTaskId& task_id, const common::ObReplicaType& type,
    const ObPGBackupTaskInfo::BackupStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (!addr.is_valid() || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update pg backup task addr get invalid argument", K(ret), K(addr), K(status));
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_COUNT ? remain_cnt : MAX_BATCH_COUNT;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(i + report_idx);
          if (OB_FAIL(ObPGBackupTaskOperator::update_pg_task_info(trans, addr, type, task_id, status, pkey))) {
            LOG_WARN("failed to update addr", K(ret), K(pkey), K(addr), K(status));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(pkeys));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*not commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(tmp_ret), K(pkeys));
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

int ObPGBackupTaskUpdater::get_pg_backup_tasks(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, ObIArray<ObPGBackupTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_pg_backup_task(
                 tenant_id, incarnation, backup_set_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  }
  return ret;
}

int ObPGBackupTaskUpdater::update_status_and_result_and_statics(
    const common::ObIArray<ObPGBackupTaskInfo>& pg_task_info_array)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else {
    while (OB_SUCC(ret) && report_idx < pg_task_info_array.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pg_task_info_array.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_COUNT ? remain_cnt : MAX_BATCH_COUNT;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPGBackupTaskInfo& pg_task_info = pg_task_info_array.at(i + report_idx);
          if (OB_FAIL(ObPGBackupTaskOperator::update_result_and_status_and_statics(trans, pg_task_info))) {
            LOG_WARN("failed to update addr", K(ret), K(pg_task_info));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(pg_task_info_array));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*not commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(tmp_ret), K(pg_task_info_array));
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

int ObPGBackupTaskUpdater::delete_all_pg_tasks(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t max_delete_rows, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0 || max_delete_rows <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete all pg tasks get invalid argument",
        K(ret),
        K(tenant_id),
        K(incarnation),
        K(backup_set_id),
        K(max_delete_rows));
  } else if (OB_FAIL(ObPGBackupTaskOperator::batch_remove_task(
                 tenant_id, incarnation, backup_set_id, max_delete_rows, *sql_proxy_, affected_rows))) {
    LOG_WARN("failed to batch remove pg backup task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_total_pg_task_count(const uint64_t tenant_id, int64_t& total_pg_task_cnt)
{
  int ret = OB_SUCCESS;
  total_pg_task_cnt = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get total pg task count get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_total_pg_task_count(*sql_proxy_, tenant_id, total_pg_task_cnt))) {
    LOG_WARN("failed to get total pg task count", K(ret), K(tenant_id));
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_latest_backup_task(const uint64_t tenant_id, ObPGBackupTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("get latest backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get latest backup task get invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_latest_backup_task(*sql_proxy_, tenant_id, pg_task_info))) {
    LOG_WARN("failed to get last backup task", K(ret), K(tenant_id));
  }
  return ret;
}

int ObPGBackupTaskUpdater::batch_report_pg_task(const ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (OB_FAIL(ObPGBackupTaskOperator::batch_report_task(pg_task_infos, *sql_proxy_))) {
    LOG_WARN("failed to batch report pg backup task", K(ret), K(pg_task_infos.count()));
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_finished_backup_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task results get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else {
    for (int64_t i = 0; i < OB_MAX_RETRY_TIMES; ++i) {
      pg_task_infos.reset();
      if (OB_FAIL(ObPGBackupTaskOperator::get_finished_backup_task(
              tenant_id, incarnation, backup_set_id, pg_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to get pg backup task results", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(i));
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_one_doing_pg_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get earliest pg task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_one_doing_pg_task(
                 tenant_id, incarnation, backup_set_id, *sql_proxy_, pg_task_infos))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get one doing pg task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    }
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_pending_pg_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, ObIArray<ObPGBackupTaskInfo>& pg_task_infos)
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get earliest pg task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_pending_pg_task(
                 tenant_id, incarnation, backup_set_id, *sql_proxy_, pg_task_infos))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get one doing pg task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    }
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_pg_backup_task(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const ObPartitionKey& pkey, ObPGBackupTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  pg_task_info.reset();
  pg_task_info.tenant_id_ = tenant_id;
  pg_task_info.incarnation_ = incarnation;
  pg_task_info.backup_set_id_ = backup_set_id;
  pg_task_info.table_id_ = pkey.get_table_id();
  pg_task_info.partition_id_ = pkey.get_partition_id();

  if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0 || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "get pg backup task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(pkey));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_pg_backup_task(pg_task_info, *sql_proxy_))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(pg_task_info));
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_pg_backup_tasks(const uint64_t tenant_id, const int64_t incarnation,
    const int64_t backup_set_id, const int64_t backup_task_id, ObIArray<ObPGBackupTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0 || backup_task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get pg backup task get invalid argument",
        K(ret),
        K(tenant_id),
        K(incarnation),
        K(backup_set_id),
        K(backup_task_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_pg_backup_task(
                 tenant_id, incarnation, backup_set_id, backup_task_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id), K(backup_task_id));
  }
  return ret;
}

int ObPGBackupTaskUpdater::update_status_and_result_without_trans(const common::ObIArray<common::ObPartitionKey>& pkeys,
    const common::ObIArray<int32_t>& results, const ObPGBackupTaskInfo::BackupStatus& status)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if ((pkeys.count() != results.count()) || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update status and result get invalid argument", K(ret), K(pkeys.count()), K(results.count()), K(status));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkeys.count(); ++i) {
      const ObPartitionKey& pkey = pkeys.at(i);
      const int32_t result = results.at(i);
      if (OB_FAIL(ObPGBackupTaskOperator::update_result_and_status(*sql_proxy_, status, result, pkey))) {
        LOG_WARN("failed to update addr", K(ret), K(pkey), K(status));
      }
    }
  }
  return ret;
}

int ObPGBackupTaskUpdater::get_one_pg_task(
    const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id, ObPGBackupTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  pg_task_info.reset();
  if (tenant_id == OB_INVALID_ID || incarnation < 0 || backup_set_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get earliest pg task get invalid argument", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
  } else if (OB_FAIL(ObPGBackupTaskOperator::get_one_pg_task(
                 tenant_id, incarnation, backup_set_id, *sql_proxy_, pg_task_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get one pg task", K(ret), K(tenant_id), K(incarnation), K(backup_set_id));
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObPGBackupTaskUpdater::update_status_and_result(const common::ObIArray<common::ObPartitionKey>& pkeys,
    const common::ObIArray<int32_t>& results, const ObPGBackupTaskInfo::BackupStatus& status)
{
  int ret = OB_SUCCESS;
  int64_t report_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg backup task updater do not init", K(ret));
  } else if ((pkeys.count() != results.count()) || status >= ObPGBackupTaskInfo::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update status and result get invalid argument", K(ret), K(pkeys.count()), K(results.count()), K(status));
  } else {
    while (OB_SUCC(ret) && report_idx < pkeys.count()) {
      ObMySQLTransaction trans;
      const int64_t remain_cnt = pkeys.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < MAX_BATCH_COUNT ? remain_cnt : MAX_BATCH_COUNT;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
          const ObPartitionKey& pkey = pkeys.at(i + report_idx);
          const int32_t result = results.at(i + report_idx);
          if (OB_FAIL(ObPGBackupTaskOperator::update_result_and_status(trans, status, result, pkey))) {
            LOG_WARN("failed to update addr", K(ret), K(pkey), K(status));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(pkeys));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /*not commit*/))) {
            OB_LOG(WARN, "fail to end transaction", K(ret), K(tmp_ret), K(pkeys));
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
