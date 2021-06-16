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

#ifndef OCEANBASE_SHARE_BACKUP_OB_VALIDATE_OPERATOR_H_
#define OCEANBASE_SHARE_BACKUP_OB_VALIDATE_OPERATOR_H_

#include <stdint.h>
#include "common/ob_partition_key.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
class ObMySQLProxy;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {

class ObIBackupValidateOperator {
public:
  ObIBackupValidateOperator() = default;
  virtual ~ObIBackupValidateOperator() = default;
  static int fill_one_item(const ObBackupValidateTaskItem& item, share::ObDMLSqlSplicer& splicer);
  static int get_backup_validate_task(const common::ObSqlString& sql, common::ObISQLClient& sql_proxy,
      common::ObIArray<ObBackupValidateTaskItem>& items);

private:
  static int extract_validate_task(common::sqlclient::ObMySQLResult* result, ObBackupValidateTaskItem& item);
};

class ObBackupValidateOperator {
public:
  ObBackupValidateOperator() = default;
  virtual ~ObBackupValidateOperator() = default;
  static int insert_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client);
  static int remove_task(const int64_t job_id, common::ObISQLClient& sql_client);
  static int get_task(const int64_t job_id, common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item);
  static int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
      common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item);
  static int get_not_finished_tasks(
      common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items);
  static int get_tasks(const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObBackupValidateTaskItem>& items);
  static int get_all_tasks(common::ObISQLClient& sql_client, common::ObIArray<ObBackupValidateTaskItem>& items);
  static int report_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client);
};

class ObBackupValidateHistoryOperator {
public:
  ObBackupValidateHistoryOperator() = default;
  virtual ~ObBackupValidateHistoryOperator() = default;
  static int insert_task(const ObBackupValidateTaskItem& item, common::ObISQLClient& sql_client);
  static int remove_task(const int64_t job_id, common::ObISQLClient& sql_client);
  static int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t backup_set_id,
      common::ObISQLClient& sql_client, ObBackupValidateTaskItem& item);
  static int get_tasks(const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObBackupValidateTaskItem>& items);
};

class ObITenantValidateTaskOperator {
public:
  ObITenantValidateTaskOperator() = default;
  virtual ~ObITenantValidateTaskOperator() = default;
  static int fill_one_item(const ObTenantValidateTaskItem& item, share::ObDMLSqlSplicer& splicer);
  static int get_tenant_validate_task(const uint64_t tenant_id, const common::ObSqlString& sql,
      common::ObISQLClient& sql_proxy, common::ObIArray<ObTenantValidateTaskItem>& items);

private:
  static int extract_tenant_task(common::sqlclient::ObMySQLResult* result, ObTenantValidateTaskItem& item);
};

class ObTenantValidateTaskOperator {
public:
  ObTenantValidateTaskOperator() = default;
  virtual ~ObTenantValidateTaskOperator() = default;
  static int insert_task(
      const uint64_t tenant_id, const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client);
  static int remove_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, common::ObISQLClient& sql_client);
  static int get_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client,
      ObTenantValidateTaskItem& item);
  static int get_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item);
  static int get_tasks(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items);
  static int get_tasks(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items);
  static int get_not_finished_tasks(const uint64_t tenant_id, const uint64_t task_tenant_id,
      common::ObISQLClient& sql_client, common::ObIArray<ObTenantValidateTaskItem>& items);
  static int get_finished_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item);
  static int report_task(
      const uint64_t tenant_id, const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client);
};

class ObTenantValidateHistoryOperator {
public:
  ObTenantValidateHistoryOperator() = default;
  virtual ~ObTenantValidateHistoryOperator() = default;
  static int insert_task(const ObTenantValidateTaskItem& item, common::ObISQLClient& sql_client);
  static int remove_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObISQLClient& sql_client);
  static int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, common::ObISQLClient& sql_client, ObTenantValidateTaskItem& item);
  static int get_tasks(const int64_t job_id, const uint64_t tenant_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObTenantValidateTaskItem>& items);
};

class ObPGValidateTaskOperator {
public:
  ObPGValidateTaskOperator() = default;
  virtual ~ObPGValidateTaskOperator() = default;

  static int batch_report_pg_task(
      const uint64_t tenant_id, const common::ObIArray<ObPGValidateTaskItem>& items, common::ObISQLClient& sql_client);
  static int batch_remove_pg_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t max_delete_rows, common::ObISQLClient& sql_client, int64_t& affected_rows);
  static int get_pg_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, const common::ObPartitionKey& pkey,
      common::ObISQLClient& sql_client, ObPGValidateTaskItem& item);
  static int get_pg_tasks(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_doing_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_pending_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_pending_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_finished_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_finished_task(const uint64_t tenant_id, const int64_t job_id, const uint64_t task_tenant_id,
      const int64_t incarnation, const int64_t backup_set_id, common::ObISQLClient& sql_client,
      common::ObIArray<ObPGValidateTaskItem>& items);
  static int get_one_doing_pg_tasks(const uint64_t tenant_id, const ObPGValidateTaskRowKey& row_key,
      common::ObISQLClient& sql_client, common::ObIArray<ObPGValidateTaskInfo>& items);
  static int update_pg_task_info(const uint64_t tenant_id, common::ObISQLClient& sql_client,
      // const ObTaskId &trace_id,
      const common::ObPartitionKey& pkey);
  static int update_pg_task_status(const uint64_t tenant_id, common::ObISQLClient& sql_client,
      const common::ObPartitionKey& pkey, const ObPGValidateTaskInfo::ValidateStatus& status);
  static int update_pg_task_result_and_status(const uint64_t tenant_id, const int64_t backup_set_id,
      common::ObISQLClient& sql_client, const common::ObPartitionKey& pkey,
      const ObPGValidateTaskInfo::ValidateStatus& status, const int32_t result);

private:
  static int fill_one_item(const ObPGValidateTaskItem& item, share::ObDMLSqlSplicer& splicer);
  static int get_pg_validate_task(const uint64_t tenant_id, const common::ObSqlString& sql,
      common::ObISQLClient& sql_proxy, common::ObIArray<ObPGValidateTaskItem>& items);
  static int extract_pg_task(common::sqlclient::ObMySQLResult* result, ObPGValidateTaskItem& item);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
