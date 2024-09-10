/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_TASK_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_TASK_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/task/ob_timer.h"
#include "share/object_storage/ob_object_storage_struct.h"

namespace oceanbase
{
namespace share
{
class ObZoneStorageTableInfo;
class ObZoneStorageOperationTableInfo;
class ObDeviceManifestTask : public common::ObTimerTask
{
public:
  static ObDeviceManifestTask &get_instance();
  ObDeviceManifestTask();
  virtual ~ObDeviceManifestTask() = default;
  int init(common::ObMySQLProxy *proxy);
  virtual void runTimerTask() override;
  int run();
  int try_update_new_device_configs();
  int add_new_device_configs(const ObIArray<ObZoneStorageTableInfo> &storage_infos);
  static const int64_t SCHEDULE_INTERVAL_US = 60 * 1000 * 1000L; // 60s

private:
  int do_work();
  int try_update_all_device_config();
  int try_update_next_device_config(const uint64_t last_op_id, const uint64_t last_sub_op_id);
  int check_if_need_update(const ObZoneStorageState::STATE &op_type, bool &need_update);
  int check_connectivity(const ObZoneStorageTableInfo &zone_storage_info,
                         bool &is_connective);
  int add_device_config(const ObZoneStorageTableInfo &zone_storage_info,
                        const ObZoneStorageOperationTableInfo &storage_op_info,
                        const bool is_connective);
  int add_device_config(const ObZoneStorageTableInfo &zone_storage_info,
                        const bool is_connective);
  int update_device_manifest(const ObZoneStorageOperationTableInfo &storage_op_info);
  int remove_device_config(const ObZoneStorageTableInfo &zone_storage_info,
                           const ObZoneStorageOperationTableInfo &storage_op_info);
  int update_device_config(const ObZoneStorageTableInfo &zone_storage_info,
                           const ObZoneStorageOperationTableInfo &storage_op_info,
                           const bool is_connective);

private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  lib::ObMutex manifest_task_lock_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_TASK_H_
