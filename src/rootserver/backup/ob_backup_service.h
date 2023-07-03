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

#include "ob_backup_base_service.h"
#include "ob_backup_data_scheduler.h"
#include "ob_backup_clean_scheduler.h"
namespace oceanbase 
{
namespace rootserver 
{
class ObBackupTaskScheduler;
// the backup service who relay on ObBackupTaskScheduler must inherit this.
class ObBackupService : public ObBackupBaseService
{
public:
  ObBackupService(): is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), can_schedule_(false), task_scheduler_(nullptr),
      schema_service_(nullptr) {}
  virtual ~ObBackupService() {};
  int init();
  void run2() override final;
  virtual int process(int64_t &last_schedule_ts) = 0;
  void destroy() override final;
public:

  virtual ObIBackupJobScheduler *get_scheduler(const BackupJobType &type) = 0;
  virtual int get_need_reload_task(
      common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) = 0;

  virtual int switch_to_leader() override { disable_backup(); return ObBackupBaseService::switch_to_leader(); }
  virtual int resume_leader() override { disable_backup(); return ObBackupBaseService::resume_leader(); }

  // called by ObBackupTaskScheduler.
  // if ObBackupTaskScheduler reload ls task succeed, call enable_backup.
  // otherwise call disable_backup().
  void disable_backup();
  void enable_backup();
  bool can_schedule();
  TO_STRING_KV(K_(tenant_id), K_(can_schedule))
protected:
  virtual int sub_init(common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObLocationService &loacation_service,
           ObBackupTaskScheduler &task_scheduler) = 0;
protected:
  bool is_inited_;
  uint64_t tenant_id_;
  bool can_schedule_;
  ObBackupTaskScheduler *task_scheduler_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupService);
};

class ObBackupDataService final : public ObBackupService
{
public:
  ObBackupDataService(): ObBackupService(), backup_data_scheduler_() {}
  virtual ~ObBackupDataService() {}
  DEFINE_MTL_FUNC(ObBackupDataService);
  int process(int64_t &last_schedule_ts) override;

  ObIBackupJobScheduler *get_scheduler(const BackupJobType &type);
  int get_need_reload_task(
      common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) override;

  int handle_backup_database(const obrpc::ObBackupDatabaseArg &arg);
  int handle_backup_database_cancel(const uint64_t tenant_id, const ObIArray<uint64_t> &managed_tenant_ids);
private:
  int sub_init(common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service, share::ObLocationService &loacation_service,
    ObBackupTaskScheduler &task_scheduler) override;
private:
  ObBackupDataScheduler backup_data_scheduler_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataService);
};

class ObBackupCleanService final : public ObBackupService
{
public:
  ObBackupCleanService() : ObBackupService(), backup_clean_scheduler_(), backup_auto_obsolete_delete_trigger_(),
    jobs_(), triggers_() {}
  virtual ~ObBackupCleanService() {}
  DEFINE_MTL_FUNC(ObBackupCleanService);
  int process(int64_t &last_schedule_ts) override;

  ObIBackupJobScheduler *get_scheduler(const BackupJobType &type);
  int get_need_reload_task(
      common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) override;

  int handle_backup_delete(const obrpc::ObBackupCleanArg &arg);
  int handle_delete_policy(const obrpc::ObDeletePolicyArg &arg);
  int handle_backup_delete_obsolete(const obrpc::ObBackupCleanArg &arg);
private:
  virtual int sub_init(common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
                  share::schema::ObMultiVersionSchemaService &schema_service,
                  share::ObLocationService &loacation_service,
                  ObBackupTaskScheduler &task_scheduler) override;

private:
  int register_job_(ObIBackupJobScheduler *new_job);
  int register_trigger_(ObIBackupTrigger *new_trigger);
  void process_trigger_(int64_t &last_trigger_ts);
  void process_scheduler_();

private:
  ObBackupCleanScheduler backup_clean_scheduler_;
  ObBackupAutoObsoleteDeleteTrigger backup_auto_obsolete_delete_trigger_;
  common::ObSEArray<ObIBackupJobScheduler *, 8> jobs_;
  common::ObSEArray<ObIBackupTrigger *, 2> triggers_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanService);
};


}  // end namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_SERVICE_H_
