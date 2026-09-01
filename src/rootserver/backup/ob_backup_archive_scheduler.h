/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_SCHEDULER_H_

#include "ob_backup_base_job.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace obrpc
{
struct ObBackupArchiveLogAllArg;
}
namespace rootserver
{
class ObBackupTaskScheduler;
class ObBackupArchiveService;

class ObBackupArchiveScheduler : public ObIBackupJobScheduler
{
public:
  ObBackupArchiveScheduler();
  virtual ~ObBackupArchiveScheduler() {}
  virtual int process() override;
  virtual int force_cancel(const uint64_t tenant_id) override;
  virtual int handle_execute_over(
      const ObBackupScheduleTask *task,
      const share::ObHAResultInfo &result_info,
      bool &can_remove) override;
  virtual int get_need_reload_task(
      common::ObIAllocator &allocator,
      common::ObIArray<ObBackupScheduleTask *> &tasks) override;
public:
  int init(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupArchiveService &backup_service);
  int add_job(const uint64_t target_tenant_id, const obrpc::ObBackupArchiveLogAllArg &arg);
private:
  int check_can_start_(
      const uint64_t target_tenant_id,
      share::ObBackupPathString &backup_archive_path);
  int fill_job_attr_(
      const uint64_t target_tenant_id,
      const obrpc::ObBackupArchiveLogAllArg &arg,
      const share::ObBackupPathString &backup_archive_path,
      share::ObBackupJobAttr &job_attr);
  int insert_job_(const share::ObBackupJobAttr &job_attr);
  int process_job_(share::ObBackupJobAttr &job_attr);
  int advance_job_status_(
      common::ObISQLClient &proxy,
      const share::ObBackupJobAttr &job_attr,
      const share::ObBackupStatus &next_status,
      const int result = OB_SUCCESS,
      const int64_t end_ts = 0);
  int start_job_(share::ObBackupJobAttr &job_attr);
  int process_doing_job_(share::ObBackupJobAttr &job_attr);
  int cancel_job_(share::ObBackupJobAttr &job_attr);
  // start archive piece clean job after backup archive job finished
  int schedule_delete_backed_up_archive_pieces_(const share::ObBackupJobAttr &job_attr);
  int dispatch_init_piece_tasks_(const share::ObBackupJobAttr &job_attr);
  int build_piece_tasks_(
      const share::ObBackupJobAttr &job_attr,
      const int64_t archive_dest_id,
      const common::ObIArray<share::ObTenantArchivePieceAttr> &pieces,
      common::ObIArray<share::ObBackupArchivePieceTaskAttr> &tasks);
private:
  static const int64_t MAX_PIECE_TASK_RETRY_CNT = 5;
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupTaskScheduler *task_scheduler_;
  ObBackupArchiveService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchiveScheduler);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_BACKUP_ARCHIVE_SCHEDULER_H_
