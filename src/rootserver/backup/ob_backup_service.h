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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_H_

#include "ob_backup_task_scheduler.h"
#include "ob_backup_data_scheduler.h"
#include "ob_backup_base_job.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "ob_backup_clean_scheduler.h"
namespace oceanbase 
{
namespace rootserver 
{
class ObBackupMgrIdling : public ObThreadIdling 
{
public:
  explicit ObBackupMgrIdling(volatile bool &stop) : ObThreadIdling(stop) {}
  virtual int64_t get_idle_interval_us();
};

class ObBackupService : public ObRsReentrantThread 
{
public:
  ObBackupService();
  virtual ~ObBackupService() {};
  int init(ObServerManager &server_mgr, common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service, ObBackupLeaseService &lease_service,
      ObBackupTaskScheduler &task_scheduler);
  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  void stop();
  void wakeup();
  int idle() const;
public:
  int handle_backup_database(const obrpc::ObBackupDatabaseArg &arg);
  int handle_backup_database_cancel(const uint64_t tenant_id, const ObIArray<uint64_t> &managed_tenant_ids);
  int handle_backup_delete(const obrpc::ObBackupCleanArg &arg);
  int handle_delete_policy(const obrpc::ObDeletePolicyArg &arg);
  int handle_backup_delete_obsolete(const obrpc::ObBackupCleanArg &arg);

  common::ObSEArray<ObIBackupJobScheduler *, 8> &get_jobs() { return jobs_; }
  virtual int get_job(const BackupJobType &type, ObIBackupJobScheduler *&new_job);
  int get_need_reload_task(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks);
  void disable_backup();
  void enable_backup();
  bool can_schedule();
private:
  int register_job_(ObIBackupJobScheduler *new_job);
  int register_trigger_(ObIBackupTrigger *new_trigger);
  void start_trigger_(int64_t &last_trigger_ts);
  void start_scheduler_();
private:
  bool is_inited_;
  lib::ObMutex mgr_mtx_;
  bool can_schedule_;
  mutable ObBackupMgrIdling idling_;
  ObBackupDataScheduler backup_data_scheduler_;
  ObBackupCleanScheduler backup_clean_scheduler_;
  ObBackupAutoObsoleteDeleteTrigger backup_auto_obsolete_delete_trigger_;
  common::ObSEArray<ObIBackupJobScheduler *, 8> jobs_;
  common::ObSEArray<ObIBackupTrigger *, 2> triggers_;
  ObBackupTaskScheduler *task_scheduler_;
  ObBackupLeaseService *lease_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupService);
};
}  // end namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_H_