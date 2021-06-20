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

#ifndef OCEANBASE_SHARE_BACKUP_OB_TENANT_VALIDATE_TASK_UPDATER_H_
#define OCEANBASE_SHARE_BACKUP_OB_TENANT_VALIDATE_TASK_UPDATER_H_

#include <stdint.h>
#include "lib/container/ob_iarray.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
}
namespace share {

class ObTenantValidateTaskUpdater {
public:
  ObTenantValidateTaskUpdater();
  virtual ~ObTenantValidateTaskUpdater() = default;

  int init(common::ObISQLClient& sql_client, bool is_dropped_tenant = false);
  // not thread safe
  void set_dropped_tenant(const bool is_dropped_tenant)
  {
    is_dropped_tenant_ = is_dropped_tenant;
  }
  int insert_task(const ObTenantValidateTaskInfo& tenant_validate_tasks);
  int get_task(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      const int64_t backup_set_id, ObTenantValidateTaskInfo& tenant_validate_task);
  int get_task(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation,
      ObTenantValidateTaskInfo& tenant_validate_task);
  int get_tasks(const int64_t job_id, const uint64_t task_tenant_id,
      common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks);
  int get_not_finished_tasks(
      const uint64_t task_tenant_id, common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks);
  int get_finished_task(
      const int64_t job_id, const uint64_t task_tenant_id, ObTenantValidateTaskInfo& tenant_validate_task);
  int update_task(const ObTenantValidateTaskInfo& src_task_info, const ObTenantValidateTaskInfo& dst_task_info);
  int remove_task(const int64_t job_id, const uint64_t task_tenant_id, const int64_t incarnation);

private:
  bool is_inited_;
  bool is_dropped_tenant_;
  common::ObISQLClient* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantValidateTaskUpdater);
};

class ObTenantValidateHistoryUpdater {
public:
  ObTenantValidateHistoryUpdater();
  virtual ~ObTenantValidateHistoryUpdater() = default;

  int init(common::ObISQLClient& sql_client);
  int insert_task(const ObTenantValidateTaskInfo& tenant_validate_tasks);
  int get_task(const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id,
      ObTenantValidateTaskInfo& tenant_validate_task);
  int get_tasks(const int64_t job_id, const uint64_t tenant_id,
      common::ObIArray<ObTenantValidateTaskInfo>& tenant_validate_tasks);
  int remove_task(
      const int64_t job_id, const uint64_t tenant_id, const int64_t incarnation, const int64_t backup_set_id);

private:
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantValidateHistoryUpdater);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
