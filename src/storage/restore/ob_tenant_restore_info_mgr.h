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
#include "lib/task/ob_timer.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTenantRestoreInfoMgr final
{
public:
  ObTenantRestoreInfoMgr();
  ~ObTenantRestoreInfoMgr();

  static int mtl_init(ObTenantRestoreInfoMgr *&restore_info_mgr);
  int init(const uint64_t tenant_id);
  int start();
  void wait();
  void stop();
  void destroy();

  int refresh_restore_info();
  bool is_refreshed() const { return is_refreshed_; }
  int get_backup_dest(const int64_t backup_set_id, share::ObBackupDest &backup_dest);
  int get_backup_type(const int64_t backup_set_id, share::ObBackupType &backup_type);
  int get_restore_dest_id(int64_t &dest_id);

private:
  int get_restore_backup_set_brief_info_(const int64_t backup_set_id, int64_t &idx);
  void set_refreshed_() { is_refreshed_ = true; }

private:
  class RestoreInfoRefresher : public common::ObTimerTask
  {
  public:
    RestoreInfoRefresher(ObTenantRestoreInfoMgr &mgr) : mgr_(mgr) {}
    virtual ~RestoreInfoRefresher() {}
    virtual void runTimerTask();
  private:
    ObTenantRestoreInfoMgr &mgr_;
  };

private:
  static constexpr const int64_t REFRESH_INFO_INTERVAL = 5 * 1000L * 1000L; //5s

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  RestoreInfoRefresher refresh_info_task_;
  bool is_refreshed_;
  uint64_t tenant_id_;
  int64_t restore_job_id_;
  common::ObArray<share::ObRestoreBackupSetBriefInfo> backup_set_list_;
  int64_t dest_id_;
};

}
}

#endif