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

 #ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_SCHEDULER_H_
 #define OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_SCHEDULER_H_
#include "ob_backup_base_job.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "rootserver/backup/ob_backup_validate_info_collector.h"

namespace oceanbase
{

namespace obrpc
{
class ObSrvRpcProxy;
struct ObBackupValidateArg;
}
namespace common
{
class ObISQLClient;
}
namespace rootserver
{
class ObSysTenantBackupValidateJobMgr;
class ObBackupTaskScheduler;
class ObBackupTaskSchedulerQueue;
class ObIBackupValidateMgr;
class ObBackupSetTaskMgr;
class ObBackupService;
class ObServerManager;
class ObBackupValidateScheduler: public ObIBackupJobScheduler
{
public:
  ObBackupValidateScheduler();
  virtual ~ObBackupValidateScheduler() {};
public:
  virtual int process() override;
  virtual int force_cancel(const uint64_t tenant_id) override;
  virtual int handle_execute_over(
    const ObBackupScheduleTask *task,
    const share::ObHAResultInfo &result_info,
    bool &can_remove) override;
  virtual int reload_task(
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue) override;
public:
  int init(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObBackupTaskScheduler &task_scheduler,
      ObBackupService &validate_service);
  int start_schedule_backup_validate(const obrpc::ObBackupValidateArg &arg);
  static int check_tenant_status(
    share::schema::ObMultiVersionSchemaService &schema_service,
    uint64_t tenant_id, bool &is_valid);
  int cancel_backup_validate_job(const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &backup_tenant_ids);
private:
  int start_sys_backup_validate_(const share::ObBackupValidateJobAttr &job_attr, const ObBackupDest &validate_dest);
  int start_tenant_backup_validate_(const share::ObBackupValidateJobAttr &job_attr, const ObBackupDest &validate_dest);
  int fill_template_job_(
      const obrpc::ObBackupValidateArg &in_arg,
      share::ObBackupValidateJobAttr &job_attr,
      ObBackupDest &validate_dest);
  int get_need_cancel_jobs_(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &backup_tenant_ids,
    common::ObIArray<share::ObBackupValidateJobAttr> &job_attrs);
  int get_next_job_id_(common::ObISQLClient &trans, const uint64_t tenant_id, int64_t &job_id);
  int get_job_need_reload_task(
    const share::ObBackupValidateJobAttr &job,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue);
  int get_need_cancel_tenants_(const uint64_t tenant_id, const common::ObIArray<uint64_t> &execute_tenant_ids,
    common::ObIArray<uint64_t> &need_cancel_tenants);
  int do_reload_for_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue);
  int do_reload_single_validate_ls_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    const share::ObBackupValidateLSTaskAttr &ls_task,
    common::ObIAllocator &allocator,
    ObBackupTaskSchedulerQueue &queue);
  bool check_has_canceling_job_(const common::ObIArray<share::ObBackupValidateJobAttr> &validate_jobs);
  int build_task_(
    const share::ObBackupValidateTaskAttr &task_attr,
    const share::ObBackupValidateLSTaskAttr &ls_task,
    common::ObIAllocator &allocator,
    ObBackupScheduleTask *&task);
  int handle_failed_job_(
    const uint64_t tenant_id,
    const int result,
    ObIBackupValidateMgr &job_mgr,
    share::ObBackupValidateJobAttr &job_attr);
  int check_tenant_set_backup_dest_(const uint64_t tenant_id, bool &is_setted) const;
  int check_tenant_set_archive_dest_(const uint64_t tenant_id, bool &is_setted) const;
  int get_path_and_set_path_type_(
      const ObBackupPathString &backup_dest,
      share::ObBackupValidateJobAttr &job_attr,
      ObBackupDest &dest);
  int check_backup_dest_and_archive_dest_(
    const uint64_t tenant_id,
    const share::ObBackupValidateJobAttr &job_attr,
    bool &need_validate) const;
  int get_need_validate_tenants_(const share::ObBackupValidateJobAttr &job_attr, ObIArray<uint64_t> &tenants) const;
  int persist_job_task_(
      common::ObISQLClient &trans,
      share::ObBackupValidateJobAttr &job_attr);
  int insert_backup_storage_info_for_validate_(
    common::ObMySQLTransaction &trans,
    const ObBackupDest &validate_dest,
    const uint64_t tenant_id);
  int check_dest_connectivity_(
    common::ObISQLClient &trans,
    const uint64_t tenant_id,
    const ObBackupPathString &validate_path);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupTaskScheduler *task_scheduler_;
  ObBackupService *validate_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupValidateScheduler);
};

class ObIBackupValidateMgr
{
public:
  ObIBackupValidateMgr();
  virtual ~ObIBackupValidateMgr() { reset(); }
  virtual int process() = 0;
  virtual int deal_non_reentrant_job(const int err) = 0;
  virtual int init(const uint64_t tenant_id, share::ObBackupValidateJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler, share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupService &backup_validate_service);
public:
  virtual void reset();
  uint64_t get_tenant_id() const {return tenant_id_;}
  bool is_can_retry(const int err) const;
  int advance_status(
    common::ObISQLClient &sql_proxy,
    const share::ObBackupValidateStatus &next_status,
    const int result,
    const int64_t end_ts);
protected:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupValidateJobAttr *job_attr_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  ObBackupTaskScheduler *task_scheduler_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObBackupService * backup_validate_service_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupValidateMgr);
};

class ObSysTenantBackupValidateJobMgr final : public ObIBackupValidateMgr
{
public:
  ObSysTenantBackupValidateJobMgr() {}
  ~ObSysTenantBackupValidateJobMgr() {}
  int process() override;
  int deal_non_reentrant_job(const int err) override {return OB_NOT_SUPPORTED;}
private:
  int handle_user_tenant_backup_validate_();
  int do_handle_user_tenant_backup_validate_(const uint64_t &tenant_id);
  int statistic_user_tenant_job_();
  int move_to_history_();
  int cancel_user_tenant_job_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSysTenantBackupValidateJobMgr);
};

class ObUserTenantBackupValidateJobMgr final : public ObIBackupValidateMgr
{
public:
  ObUserTenantBackupValidateJobMgr();
  ~ObUserTenantBackupValidateJobMgr();
  void reset() override;
  int process() override;
  int deal_non_reentrant_job(const int err) override;
  int init(const uint64_t tenant_id, share::ObBackupValidateJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy, obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler, share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupService &backup_validate_service) override;
private:
  int do_validate_task_();
  int check_dest_validity_();
  int check_data_backup_dest_validity_(bool &is_backup_dest_exist);
  int check_log_archive_dest_validity_(bool &is_archive_dest_exist);
  int persist_set_task_();
  int do_cancel_validate_();
  int move_to_history_();
  int report_failure_to_initiator_();
  int advance_status_canceled();
  int cancel_();
  int handle_backup_validate_task_(
      const ObArray<share::ObBackupValidateTaskAttr> &task_attrs,
      share::ObBackupValidateStatus &next_status,
      int &result);
  int update_task_to_canceling_(ObArray<share::ObBackupValidateTaskAttr> &task_attrs);
  int do_validate_tasks_(const ObArray<share::ObBackupValidateTaskAttr> &task_attrs);
  int get_need_validate_infos_(
      ObArray<share::ObBackupSetFileDesc> &set_list,
      ObArray<share::ObPieceKey> &piece_list,
      common::hash::ObHashMap<int64_t, ObArray<share::ObPieceKey>> &complement_piece_map);

  int get_backup_set_info_by_desc_(
      const share::ObBackupSetDesc &desc,
      storage::ObExternBackupSetInfoDesc &backup_set_info);
  int get_effective_set_dir_path(
    const share::ObBackupValidateJobAttr *job_attr,
    const share::ObBackupSetFileDesc &backup_set_info,
    share::ObBackupDest &set_dest);
  int get_effective_piece_dir_path(
    const share::ObBackupValidateJobAttr *job_attr,
    const share::ObPieceKey &piece_key,
    share::ObBackupDest &piece_dest);
  int construct_backup_set_task_(const share::ObBackupSetFileDesc &backup_set_info, ObBackupValidateTaskAttr &task_attr);
  int persist_backup_set_tasks_(
      common::ObISQLClient &trans,
      const ObArray<share::ObBackupSetFileDesc> &set_list,
      const common::hash::ObHashMap<int64_t, ObArray<share::ObPieceKey>> &complement_piece_map);
  int persist_complement_piece_tasks_(
      common::ObISQLClient &trans,
      const ObBackupValidateTaskAttr &task_attr,
      const ObArray<share::ObPieceKey> &piece_keys);
  int get_complement_dest_(
      const uint64_t tenant_id,
      const int64_t dest_id,
      const ObBackupPathString &set_path,
      ObBackupDest &complement_dest);
  int persist_archive_piece_tasks_(common::ObISQLClient &trans, const ObArray<share::ObPieceKey> &piece_list);
  int inner_persist_set_task_(
      const ObArray<share::ObBackupSetFileDesc> &set_list,
      const ObArray<share::ObPieceKey> &piece_list,
      const common::hash::ObHashMap<int64_t,
      ObArray<share::ObPieceKey>> &complement_piece_map);
  int construct_piece_task_common_(
      const share::ObBackupDest &piece_dest,
      const share::ObPieceKey &piece_key,
      const bool is_complement,
      const int64_t initiator_task_id,
      ObBackupValidateTaskAttr &task_attr);
  int get_initiator_task_backup_set_id_(
      const ObBackupValidateTaskAttr &task_attr,
      int64_t &backup_set_id) const;
  int check_backup_set_is_available_(
      const uint64_t tenant_id,
      const int64_t backup_set_id,
      bool &is_available) const;
  int check_need_advance_job_failed_(const ObBackupValidateTaskAttr &task_attr, bool &need_advance) const;
  int inner_move_to_history_();
private:
  int64_t backup_dest_id_;
  int64_t archive_dest_id_;
  ObBackupValidateInfoCollector info_collector_;
  DISALLOW_COPY_AND_ASSIGN(ObUserTenantBackupValidateJobMgr);
};

class ObBackupValidateJobMgrAllocator final
{
public:
  static int new_job_mgr(const uint64_t tenant_id, ObIBackupValidateMgr *&job_mgr);
  static void delete_job_mgr(const uint64_t tenant_id, ObIBackupValidateMgr *job_mgr);
};

}//namespace rootserver
}//namespace oceanbase


#endif //OCEANBASE_ROOTSERVER_OB_BACKUP_VALIDATE_SCHEDULER_H_