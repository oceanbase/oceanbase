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

#ifndef OCEANBASE_SHARE_BACKUP_OB_TENANT_BACKUP_CLEAN_INFO_UPDATER_H_
#define OCEANBASE_SHARE_BACKUP_OB_TENANT_BACKUP_CLEAN_INFO_UPDATER_H_

#include "share/ob_define.h"
#include "ob_backup_struct.h"
#include "ob_backup_operator.h"

namespace oceanbase {
namespace share {

class ObTenantBackupCleanInfoUpdater {
public:
  ObTenantBackupCleanInfoUpdater();
  virtual ~ObTenantBackupCleanInfoUpdater() = default;
  int init(common::ObISQLClient& sql_proxy);
  int insert_backup_clean_info(const uint64_t tenant_id, const ObBackupCleanInfo& clean_info);
  int get_backup_clean_info(const uint64_t tenant_id, ObBackupCleanInfo& tenant_backup_task);
  int update_backup_clean_info(
      const uint64_t tenant_id, const ObBackupCleanInfo& src_clean_info, const ObBackupCleanInfo& dest_clean_info);
  int remove_clean_info(const uint64_t tenant_id, const ObBackupCleanInfo& clean_info);
  int get_backup_clean_info_status(
      const uint64_t tenant_id, common::ObISQLClient& trans, ObBackupCleanInfoStatus::STATUS& status);
  int get_deleted_tenant_clean_infos(common::ObIArray<ObBackupCleanInfo>& deleted_tenant_clean_infos);

private:
  int check_can_update_backup_clean_info(
      const ObBackupCleanInfoStatus::STATUS& src_status, const ObBackupCleanInfoStatus::STATUS& dest_status);

private:
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupCleanInfoUpdater);
};

class ObBackupCleanInfoHistoryUpdater {
public:
  ObBackupCleanInfoHistoryUpdater();
  virtual ~ObBackupCleanInfoHistoryUpdater() = default;
  int init(common::ObISQLClient& sql_proxy);
  int insert_backup_clean_info(const ObBackupCleanInfo& clean_info);
  int remove_backup_clean_info(const uint64_t tenant_id);

private:
  bool is_inited_;
  common::ObISQLClient* sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanInfoHistoryUpdater);
};

}  // namespace share
}  // namespace oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_TENANT_BACKUP_CLEAN_INFO_UPDATER_H_ */
