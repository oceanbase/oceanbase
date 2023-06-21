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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SCHEDULER_H_
#include "ob_backup_base_job.h"
#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_archive_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
struct ObBackupCleanArg;
struct ObDeletePolicyArg;
}
namespace common 
{
class ObISQLClient;
}
namespace rootserver 
{
class ObIBackupDeleteMgr;
class ObBackupTaskScheduler;
class ObBackupCleanService;
class ObServerManager;
class ObBackupCleanScheduler : public ObIBackupJobScheduler
{
public:
  ObBackupCleanScheduler();
  virtual ~ObBackupCleanScheduler() {};
public:
  virtual int process() override;
  virtual int force_cancel(const uint64_t &tenant_id) override; // for lease 
  virtual int handle_execute_over(
      const ObBackupScheduleTask *task,
      const share::ObHAResultInfo &result_info,
      bool &can_remove) override;
  virtual int get_need_reload_task(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) override; // reload tasks after switch master happend
public:
  int init(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupCleanService &backup_service);
  int start_schedule_backup_clean(const obrpc::ObBackupCleanArg &in_arg);
  int cancel_backup_clean_job(const obrpc::ObBackupCleanArg &in_arg);
  int add_delete_policy(const obrpc::ObDeletePolicyArg &in_arg);
  int drop_delete_policy(const obrpc::ObDeletePolicyArg &in_arg);
private:
  int start_sys_backup_clean_(const share::ObBackupCleanJobAttr &job_attr);
  int start_tenant_backup_clean_(const share::ObBackupCleanJobAttr &job_attr);
  int fill_template_job_(const obrpc::ObBackupCleanArg &in_arg, share::ObBackupCleanJobAttr &job_attr);
  int get_need_cancel_jobs_(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &backup_tenant_ids,
      common::ObIArray<share::ObBackupCleanJobAttr> &job_attrs);
  int get_next_job_id_(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id);
  int get_job_need_reload_task(
      const share::ObBackupCleanJobAttr &job, 
      common::ObIAllocator &allocator, 
      common::ObIArray<ObBackupScheduleTask *> &tasks);
  int do_get_need_reload_task_(
      const share::ObBackupCleanTaskAttr &task_attr,
      const ObArray<share::ObBackupCleanLSTaskAttr> &ls_tasks, 
      common::ObIAllocator &allocator,
      ObIArray<ObBackupScheduleTask *> &tasks);
  int build_task_(
      const share::ObBackupCleanTaskAttr &task_attr, 
      const share::ObBackupCleanLSTaskAttr &ls_task, 
      common::ObIAllocator &allocator, 
      ObBackupScheduleTask *&task);
  int persist_job_task_(share::ObBackupCleanJobAttr &job_attr);
  int get_need_clean_tenant_ids_(share::ObBackupCleanJobAttr &job_attr);
  int process_tenant_delete_jobs_(
      const uint64_t tenant_id,
      ObArray<share::ObBackupCleanJobAttr> &clean_jobs);
  int get_need_cancel_tenants_(
    const uint64_t tenant_id, 
    const common::ObIArray<uint64_t> &backup_tenant_ids,
    common::ObIArray<uint64_t> &need_cancel_backup_tenants);
  int cancel_tenant_jobs_(const uint64_t tenant_id);
  int fill_template_delete_policy_(
      const obrpc::ObDeletePolicyArg &in_arg,
      share::ObDeletePolicyAttr &policy_attr);
  int handle_failed_job_(const uint64_t tenant_id, const int64_t result, ObIBackupDeleteMgr &job_mgr, share::ObBackupCleanJobAttr &job_attr);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy                       *sql_proxy_;
  obrpc::ObSrvRpcProxy                       *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupTaskScheduler                      *task_scheduler_;
  ObBackupCleanService                            *backup_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanScheduler);        
};

class ObIBackupDeleteMgr 
{
public:
  ObIBackupDeleteMgr();
  virtual ~ObIBackupDeleteMgr() { reset(); }
  virtual int process() = 0;
  virtual int deal_non_reentrant_job(const int err) = 0;
public:
  int init(
      const uint64_t tenant_id,
      share::ObBackupCleanJobAttr &job_attr,
      common::ObMySQLProxy &sql_proxy, 
      obrpc::ObSrvRpcProxy &rpc_proxy,
      ObBackupTaskScheduler &task_scheduler,
      share::schema::ObMultiVersionSchemaService &schema_service_,
      ObBackupCleanService &backup_service);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  bool is_can_retry(const int err) const;
protected:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupCleanJobAttr *job_attr_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupCleanService *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupDeleteMgr);
};

class ObUserTenantBackupDeleteMgr : public ObIBackupDeleteMgr
{
public:
  ObUserTenantBackupDeleteMgr() {}
  virtual ~ObUserTenantBackupDeleteMgr() {}
  virtual int process() override;
  virtual int deal_non_reentrant_job(const int err) override;
private:
  int persist_backup_clean_task_();
  int get_need_cleaned_backup_infos_(
      ObArray<share::ObBackupSetFileDesc> &set_list,
      ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int get_delete_backup_set_infos_(ObArray<share::ObBackupSetFileDesc> &set_list);
  int get_delete_backup_piece_infos_(ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int get_delete_obsolete_backup_infos_(
      ObArray<share::ObBackupSetFileDesc> &set_list,
      ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int check_can_delete_set_();
  int check_can_delete_piece_();
  int insert_backup_task_attr_(common::ObISQLClient &sql_proxy, bool &is_exist);
  int get_next_task_id_(int64_t &task_id);
  int do_backup_clean_task_();
  int do_cleanup_();
  int cancel_();
  int do_cancel_();
  int advance_job_status_(
      common::ObISQLClient &trans,
      const share::ObBackupCleanStatus &next_status,
      const int result,
      const int64_t end_ts);
  int update_clean_data_statistics__();
  int move_to_history_();
  int get_backup_dest_(share::ObBackupCleanJobAttr &job_attr);
  int set_current_backup_dest_();
  int check_can_backup_clean_();
  int get_backup_piece_task_(const share::ObTenantArchivePieceAttr &backup_piece_info, share::ObBackupCleanTaskAttr &task_attr);
  int get_backup_clean_task_(const share::ObBackupSetFileDesc &backup_set_info, share::ObBackupCleanTaskAttr &task_attr);
  int update_task_to_canceling_(ObArray<share::ObBackupCleanTaskAttr> &task_attrs);
  int do_backup_clean_tasks_(const ObArray<share::ObBackupCleanTaskAttr> &task_attrs);
  int get_delete_obsolete_backup_piece_infos_(
      const share::ObBackupSetFileDesc &clog_data_clean_point,
      ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int get_delete_obsolete_backup_set_infos_(
      share::ObBackupSetFileDesc &clog_data_clean_point,
      ObArray<share::ObBackupSetFileDesc> &set_list);
  bool can_backup_pieces_be_deleted(const share::ObArchivePieceStatus &status);
  int persist_backup_clean_tasks_(common::ObISQLClient &trans, const ObArray<share::ObBackupSetFileDesc> &set_list);
  int persist_backup_piece_task_(common::ObISQLClient &trans, const ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int handle_backup_clean_task(
      const ObArray<share::ObBackupCleanTaskAttr> &task_attrs,
      share::ObBackupCleanStatus &next_status,
      int &result);
  int check_current_task_exist_(bool &is_exist);
  int advance_status_canceled();
  int get_backup_dest_str(const bool is_backup_backup, char *backup_dest_str, int64_t str_len);
  int report_failed_to_initiator_();
  int check_data_backup_dest_validity_();
  int check_log_archive_dest_validity_();
  int check_dest_validity_();
  int get_all_dest_backup_piece_infos_(
    const share::ObBackupSetFileDesc &clog_data_clean_point,
    ObArray<share::ObTenantArchivePieceAttr> &backup_piece_infos);
  struct CompareBackupSetInfo
  {
    bool operator()(const share::ObBackupSetFileDesc &lhs,
                    const share::ObBackupSetFileDesc &rhs) const
    {
      return lhs.backup_set_id_ < rhs.backup_set_id_;
    }
  };
  struct CompareBackupPieceInfo
  {
    bool operator()(const share::ObTenantArchivePieceAttr &lhs,
                    const share::ObTenantArchivePieceAttr &rhs) const
    {
      return lhs.key_.piece_id_ < rhs.key_.piece_id_;
    }
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObUserTenantBackupDeleteMgr);    
};

class ObSysTenantBackupDeleteMgr : public ObIBackupDeleteMgr
{
public:
  ObSysTenantBackupDeleteMgr() {}
  virtual ~ObSysTenantBackupDeleteMgr() {}
public:
  virtual int process() override;
  virtual int deal_non_reentrant_job(const int err) override { return OB_SUCCESS; }
private:
  int handle_user_tenant_backup_delete_();
  int do_handle_user_tenant_backup_delete_(const uint64_t &tenant_id);
  int statistic_user_tenant_job_();
  int move_to_history_();
  int cancel_user_tenant_job_();
  int advance_status_(
      common::ObISQLClient &sql_proxy,
      const share::ObBackupCleanStatus &next_status,
      const int result = OB_SUCCESS,
      const int64_t end_ts = 0);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysTenantBackupDeleteMgr);    
};

class ObBackupDeleteMgrAlloctor
{
public:
  static int alloc(const uint64_t tenant_id, ObIBackupDeleteMgr *&job_mgr);
  static void free(ObIBackupDeleteMgr *job_mgr);
};

class ObBackupAutoObsoleteDeleteTrigger : public ObIBackupTrigger
{
public:
  ObBackupAutoObsoleteDeleteTrigger();
  virtual ~ObBackupAutoObsoleteDeleteTrigger() {};
public:
  virtual int process() override;
public:
  int init(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupCleanService &backup_service);
private:
  int start_auto_delete_obsolete_data_();
  int get_delete_policy_parameter_(
      const share::ObDeletePolicyAttr &delete_policy,
      int64_t &recovery_window);
  int parse_time_interval_(const char *str, int64_t &val);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy                       *sql_proxy_;
  obrpc::ObSrvRpcProxy                       *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupTaskScheduler                      *task_scheduler_;
  ObBackupCleanService                            *backup_service_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupAutoObsoleteDeleteTrigger); 
};

class ObBackupCleanCommon final
{
public:
  static int get_all_tenants(
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObIArray<uint64_t> &tenants);
  static int check_tenant_status(
      share::schema::ObMultiVersionSchemaService &schema_service,
      const uint64_t tenant_id,
      bool &is_valid);
};

}
}

#endif  // OCEANBASE_ROOTSERVER_OB_BACKUP_CLEAN_SCHEDULER_H_
