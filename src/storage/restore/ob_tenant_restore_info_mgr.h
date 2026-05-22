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

#ifndef OCEANBASE_STORAGE_TENANT_RESTORE_INFO_MGR_H_
#define OCEANBASE_STORAGE_TENANT_RESTORE_INFO_MGR_H_

#include "lib/lock/ob_mutex.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTenantBackupDestInfoMgr final
{
public:
  enum class InfoType {
    NONE = 0,
    RESTORE = 1,
    VALIDATE = 2,
    MAX
  };

  ObTenantBackupDestInfoMgr();
  ~ObTenantBackupDestInfoMgr();

  static int mtl_init(ObTenantBackupDestInfoMgr *&restore_info_mgr);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  int get_backup_dest(const int64_t backup_set_id, share::ObBackupDest &backup_dest);
  int get_backup_type(const int64_t backup_set_id, share::ObBackupType &backup_type);
  int get_dest_id(int64_t &dest_id);
  int set_backup_set_info(
      const common::ObIArray<share::ObBackupSetBriefInfo> &backup_set_list,
      const int64_t dest_id,
      const InfoType type);
  void reset();

private:
  int get_backup_set_brief_info_(const int64_t backup_set_id, int64_t &idx);
  int check_restore_data_mode_();

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  common::ObArray<share::ObBackupSetBriefInfo> backup_set_list_;
  int64_t dest_id_;
  InfoType type_;
};

}
}

#endif