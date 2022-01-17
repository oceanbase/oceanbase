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

#ifndef OB_RESTORE_POINT_SERVICE_H_
#define OB_RESTORE_POINT_SERVICE_H_
#include "rootserver/ob_ddl_service.h"
#include "ob_freeze_info_manager.h"
namespace oceanbase {
namespace rootserver {

// TODO(cangjun) need change file name ob_recovery_point maybe better
class ObRestorePointService {
  static const int64_t RETRY_CNT = 10;
  static const int64_t MAX_RESTORE_POINT = 10;

public:
  ObRestorePointService();
  ~ObRestorePointService();
  int init(rootserver::ObDDLService& ddl_service, rootserver::ObFreezeInfoManager& freeze_info_mgr);
  int create_restore_point(const uint64_t tenant_id, const char* name);
  int create_backup_point(
      const uint64_t tenant_id, const char* name, const int64_t snapshot_version, const int64_t schema_version);
  int drop_restore_point(const uint64_t tenant_id, const char* name);
  int drop_backup_point(const uint64_t tenant_id, const int64_t snapshot_version);

private:
  void convert_name(const char* name, char* tmp_name, const int64_t length);

private:
  bool is_inited_;
  rootserver::ObDDLService* ddl_service_;
  ObFreezeInfoManager* freeze_info_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObRestorePointService);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif
