// Copyright 2020 Alibaba Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>
// Normalizer:
//     yanfeng <yangyi.yyy@alibaba-inc.com

#include "rootserver/backup/ob_backup_backupset.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace rootserver {

class ObBackupBackupsetScheduler {
public:
  ObBackupBackupsetScheduler();
  int init(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t max_backup_times,
      const common::ObString& backup_dest, common::ObMySQLProxy& sql_proxy, ObBackupBackupset& backup_backupset);
  int start_schedule_backup_backupset();

private:
  int check_backup_dest_is_same(bool& same);
  int check_has_doing_job(bool& has);
  int check_backup_set_id_valid(const uint64_t tenant_id, const int64_t backup_set_id, bool& is_valid);
  int get_largest_backup_set_id_if_all(int64_t& backup_set_id);
  int check_backup_backup_dest_is_valid(const int64_t backup_set_id, bool& is_valid);
  int build_backup_backupset_job_info(
      const int64_t job_id, const int64_t real_backup_set_id, share::ObBackupBackupsetJobInfo& job_info);
  int insert_backup_backupset_job(const int64_t job_id, const int64_t real_backup_set_id);
  int check_is_greater_then_existing_backup_set_id(const int64_t backup_set_id, bool& greater);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t max_backup_times_;
  share::ObBackupDest backup_dest_;
  common::ObMySQLProxy* sql_proxy_;
  ObBackupBackupset* backup_backupset_;
};

}  // end namespace rootserver
}  // end namespace oceanbase
