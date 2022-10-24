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

#ifndef SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_
#define SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_

#include "share/ob_define.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace share
{
class ObRsMgr;

class ObBackupInfoMgr final
{
public:
  typedef common::ObArray<ObPhysicalRestoreJob> RestoreJobArray;
  static ObBackupInfoMgr &get_instance();

  int init(common::ObMySQLProxy &sql_proxy);
  int start();
  void stop();
  void wait();
  void destroy();
  int get_backup_snapshot_version(int64_t &snapshot_version);
  int get_delay_delete_schema_version(const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service, bool &is_backup,
      int64_t &reserved_schema_version);
  int check_if_doing_backup(bool &is_doing);
  int get_restore_status(const uint64_t tenant_id, PhysicalRestoreStatus &status);
  int reload();

private:
  static const int64_t DEFAULT_UPDATE_INTERVAL_US = 10 * 1000 * 1000;//10s
  int get_restore_info_from_cache(const uint64_t tenant_id,
      ObSimplePhysicalRestoreJob &simple_job_info);
  int get_restore_status_from_cache(const uint64_t tenant_id, PhysicalRestoreStatus &status);
  int check_backup_dest_(ObLogArchiveBackupInfo &backup_info);

private:
  ObBackupInfoMgr();
  ~ObBackupInfoMgr();
  class ObBackupInfoUpdateTask : public common::ObTimerTask
  {
  public:
    ObBackupInfoUpdateTask() {}
    virtual ~ObBackupInfoUpdateTask() {}
    virtual void runTimerTask() override;
  };

private:

  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObTimer timer_;
  ObBackupInfoUpdateTask update_task_;
  ObLogArchiveBackupInfo backup_infos_[2];
  ObLogArchiveBackupInfo *cur_backup_info_;
  ObNonFrozenBackupPieceInfo backup_pieces_[2];
  ObNonFrozenBackupPieceInfo *cur_backup_piece_;
  RestoreJobArray restore_jobs_[2];
  RestoreJobArray *cur_restore_job_;
  common::SpinRWLock lock_;
  lib::ObMutex mutex_;
  bool is_backup_loaded_;
  bool is_restore_loaded_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupInfoMgr);
};

}//share
}//oceanbase
#endif /* SRC_SHARE_BACKUP_OB_BACKUP_INFO_MGR_H_ */
