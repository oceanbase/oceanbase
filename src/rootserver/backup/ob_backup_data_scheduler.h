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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_SCHEDULER_H_

#include "ob_backup_base_job.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{

namespace common
{
class ObISQLClient;
}

namespace rootserver
{
class ObSysTenantBackupJobMgr;
class ObBackupSetTaskMgr;
class ObIBackupJobMgr;
class ObBackupDataScheduler : public ObIBackupJobScheduler
{
public:
  ObBackupDataScheduler();
  virtual ~ObBackupDataScheduler() {};
public:
  // virtual interfaces which should be realized
  // scheduler entrance
  virtual int process() override;
  virtual int force_cancel(const uint64_t &tenant_id) override;
  // if can_remove return true, scheudler can remove task from scheduler
  virtual int handle_execute_over(const ObBackupScheduleTask *task, const share::ObHAResultInfo &result_info, bool &can_remove) override;
  // reloading task from inner table which status is pending or doing
  virtual int get_need_reload_task(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) override; 
public:
  // common func used by backup
  static int get_backup_scn(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, const bool is_start, share::SCN &scn);
  static int check_tenant_status(share::schema::ObMultiVersionSchemaService &schema_service, uint64_t tenant_id, bool &is_valid);
  static int get_backup_path(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, share::ObBackupPathString &backup_path);
  static int get_next_job_id(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id);
  static int get_next_backup_set_id(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &next_backup_set_id);
public:
  int init(const uint64_t tenant_id, common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service, ObBackupTaskScheduler &task_scheduler,
      ObBackupDataService &backup_service);

  // constructing a ObBackupJobAttr according to ObBackupDatabaseArg, them insert the ObBackupJobAttr into __all_backup_job
  int start_backup_data(const obrpc::ObBackupDatabaseArg &in_arg);

  // canceling backup job. marking ObBackupJobAttr status CANCELING.
  // if tenant_id = OB_SYS_TENANT, and backup tenant ids is empty, canceling all the tenant's backup jobs;
  // if tenant_id = OB_SYS_TENANT, and backup tenant ids is not empty, canceling the jobs of backup tenant ids;
  // if tenant_id = OB_USER_TENANT, and backup tenant ids is empty, canceling the jobs of input tenant_id.
  int cancel_backup_data(uint64_t tenant_id, const common::ObIArray<uint64_t> &backup_tenant_ids);

private:
  int fill_template_job_(const obrpc::ObBackupDatabaseArg &in_arg, share::ObBackupJobAttr &job_attr);
  int start_sys_backup_data_(const share::ObBackupJobAttr &job_attr);
  int check_log_archive_status_(const uint64_t tenant_id, bool &is_doing);
  int check_initiate_twice_by_same_tenant_(const uint64_t tenant_id, const uint64_t initiator_tenant_id);
  int check_tenant_set_backup_dest_(const uint64_t tenant_id, bool &is_setted);
  int get_all_tenants_(const bool with_backup_dest, ObIArray<uint64_t> &tenants);

  int start_tenant_backup_data_(const share::ObBackupJobAttr &job_attr);
  int update_backup_type_if_need_(common::ObISQLClient &trans, const uint64_t tenant_id, const int64_t next_backup_set_id,
      const share::ObBackupPathString &backup_path, share::ObBackupType &backup_type);
  int get_need_cancel_tenants_(const uint64_t tenant_id, const common::ObIArray<uint64_t> &backup_tenant_ids, 
      common::ObIArray<uint64_t> &need_cancel_backup_tenants);
  int do_get_need_reload_task_(const share::ObBackupJobAttr &job, const share::ObBackupSetTaskAttr &set_task_attr, 
      ObIArray<share::ObBackupLSTaskAttr> &ls_tasks,
      common::ObIAllocator &allocator, ObIArray<ObBackupScheduleTask *> &tasks);
  int build_task_(const share::ObBackupJobAttr &job, const share::ObBackupSetTaskAttr &set_task_attr, 
      const share::ObBackupLSTaskAttr &ls_task,
      ObIAllocator &allocator, ObBackupScheduleTask *&task);
  int persist_backup_version_(common::ObISQLClient &sql_proxy, const uint64_t tenant_id, const uint64_t &cluster_version);
  template <typename T>
  int do_build_task_(const share::ObBackupJobAttr &job, const share::ObBackupSetTaskAttr &set_task_attr, 
      const share::ObBackupLSTaskAttr &ls_task,
      ObIAllocator &allocator, T &tmp_task, ObBackupScheduleTask *&task);
  int handle_failed_job_(
      const uint64_t tenant_id,
      const int64_t result,
      ObIBackupJobMgr &job_mgr,
      share::ObBackupJobAttr &job_attr);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy                       *sql_proxy_;
  obrpc::ObSrvRpcProxy                       *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupTaskScheduler                      *task_scheduler_;
  ObBackupDataService                            *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataScheduler);
};

class ObIBackupJobMgr 
{
public:
  ObIBackupJobMgr();
  virtual ~ObIBackupJobMgr() { reset(); }
  virtual int process() = 0;
  virtual int deal_non_reentrant_job(const int err) = 0;
public:
  int init(const uint64_t tenant_id, share::ObBackupJobAttr &job_attr, common::ObMySQLProxy &sql_proxy, 
      obrpc::ObSrvRpcProxy &rpc_proxy, ObBackupTaskScheduler &task_scheduler,
      share::schema::ObMultiVersionSchemaService &schema_service_, ObBackupDataService &backup_service);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  bool is_can_retry(const int err) const;
protected:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupJobAttr *job_attr_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupDataService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupJobMgr);
};

class ObUserTenantBackupJobMgr final : public ObIBackupJobMgr
{
public:
  ObUserTenantBackupJobMgr() {}
  virtual ~ObUserTenantBackupJobMgr() {}
  virtual int process() override;
  virtual int deal_non_reentrant_job(const int err) override;
private:
  int move_to_history_();
  int report_failed_to_initiator_();
  int check_can_backup_();
  int advance_job_status(common::ObISQLClient &trans, const share::ObBackupStatus &next_status,
      const int result = OB_SUCCESS, const int64_t end_ts = 0);
  int persist_set_task_();
  int insert_backup_set_task_(common::ObISQLClient &sql_proxy);
  int insert_backup_set_file_(common::ObISQLClient &sql_proxy);
  int fill_backup_set_desc_(
      const share::ObBackupJobAttr &job_attr,
      const int64_t prev_full_backup_set_id,
      const int64_t prev_inc_backup_set_id,
      const int64_t dest_id,
      share::ObBackupSetFileDesc &backup_set_desc);
  int get_next_task_id_(common::ObISQLClient &sql_proxy, int64_t &task_id);

  int do_set_task_();
  int check_dest_validity_();
  int cancel_();
  int update_set_task_to_canceling_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObUserTenantBackupJobMgr);
};

class ObSysTenantBackupJobMgr final : public ObIBackupJobMgr
{
public:
  ObSysTenantBackupJobMgr() {}
  virtual ~ObSysTenantBackupJobMgr() {}
public:
  virtual int process() override;
  virtual int deal_non_reentrant_job(const int err) override { return OB_SUCCESS; };
private:
  int handle_user_tenant_backupdatabase_();
  int do_handle_user_tenant_backupdatabase_(const uint64_t &tenant_id);
  int statistic_user_tenant_job_();
  int move_to_history_();
  int cancel_user_tenant_job_();
  int advance_status_(common::ObISQLClient &sql_proxy, const share::ObBackupStatus &next_status, 
      const int result = OB_SUCCESS, const int64_t end_ts = 0);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysTenantBackupJobMgr);
};

class ObBackupJobMgrAlloctor
{
public:
  static int alloc(const uint64_t tenant_id, ObIBackupJobMgr *&job_mgr);
  static void free(const uint64_t tenant_id, ObIBackupJobMgr *job_mgr);
};

}
}
#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_DATA_SCHEDULER_H_
