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

#ifndef OCEANBASE_ROOTSERVER_OB_CLONE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_CLONE_SCHEDULER_H_

#include "share/restore/ob_tenant_clone_table_operator.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_tenant_thread_helper.h"//ObTenantThreadHelper
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{
namespace share
{
class ObLSAttr;
class ObTenantSnapLSReplicaSimpleItem;
}

namespace rootserver
{

class TenantRestoreStatus;

class ObCloneScheduler : public ObTenantThreadHelper,
                         public logservice::ObICheckpointSubHandler,
                         public logservice::ObIReplaySubHandler
{
public:
  ObCloneScheduler();
  virtual ~ObCloneScheduler();
  int init();
  virtual void do_work() override;
  void destroy();
  void wakeup();
  int idle();
  DEFINE_MTL_FUNC(ObCloneScheduler);

public:
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
public:
  int process_sys_clone_job(const share::ObCloneJob &job);
  int process_user_clone_job(const share::ObCloneJob &job);
  int clone_lock(const share::ObCloneJob &job);
  int clone_create_resource_pool(const share::ObCloneJob &job);
  int clone_create_snapshot_for_fork_tenant(const share::ObCloneJob &job);
  int clone_wait_create_snapshot_for_fork_tenant(const share::ObCloneJob &job);
  int clone_create_tenant(const share::ObCloneJob &job);
  int clone_wait_tenant_restore_finish(const share::ObCloneJob &job);
  int clone_release_resource(const share::ObCloneJob &job);
  int clone_sys_finish(const share::ObCloneJob &job);
  int clone_prepare(const share::ObCloneJob &job);
  int clone_init_ls(const share::ObCloneJob &job);
  int clone_wait_ls_finish(const share::ObCloneJob &job);
  int clone_post_check(const share::ObCloneJob &job);
  int clone_user_finish(const share::ObCloneJob &job);
  int clone_recycle_failed_job(const share::ObCloneJob &job);

private:
  int try_update_job_status_(const int return_ret,
                             const share::ObCloneJob &job);
  share::ObTenantCloneStatus get_sys_next_status_(const int return_ret,
                                                  const share::ObTenantCloneStatus current_status,
                                                  const share::ObTenantCloneJobType job_type);
  share::ObTenantCloneStatus get_sys_next_status_in_normal_(const share::ObTenantCloneStatus current_status,
                                                            const share::ObTenantCloneJobType job_type);
  share::ObTenantCloneStatus get_sys_next_status_in_failed_(const share::ObTenantCloneStatus current_status);
  share::ObTenantCloneStatus get_user_next_status_(const int return_ret,
                             const share::ObTenantCloneStatus current_status);
  int check_sys_tenant_(const uint64_t tenant_id);
  int check_meta_tenant_(const uint64_t tenant_id);

  int wait_source_relative_task_finished_(const uint64_t user_tenant_id);
  int fill_clone_resource_pool_arg_(const share::ObCloneJob &job,
                                    const uint64_t resource_pool_id,
                                    obrpc::ObCloneResourcePoolArg &arg);
  int fill_create_tenant_arg_(const share::ObCloneJob &job,
                              const uint64_t clone_tenant_id,
                              obrpc::ObCreateTenantArg &arg);
  int clone_root_key_(const share::ObCloneJob &job);
  int clone_keystore_(const share::ObCloneJob &job);
  int get_ls_attrs_from_source_(const share::ObCloneJob &job,
                                ObSArray<share::ObLSAttr> &ls_attr_array);
  int create_all_ls_(const uint64_t user_tenant_id,
      const share::schema::ObTenantSchema &tenant_schema,
      const common::ObIArray<share::ObLSAttr> &ls_attr_array,
      const uint64_t source_tenant_id);
  int wait_all_ls_created_(const share::schema::ObTenantSchema &tenant_schema,
                           const share::ObCloneJob &job);
  int finish_create_ls_(const share::schema::ObTenantSchema &tenant_schema,
                        const common::ObIArray<share::ObLSAttr> &ls_attr_array);
  int get_source_tenant_archive_log_path_(const uint64_t source_tenant_id,
                                          share::ObBackupPathString& path);
  int convert_parameters_(const uint64_t user_tenant_id,
                          const uint64_t source_tenant_id);
  int trim_master_key_map_(const uint64_t user_tenant_id,
                           const uint64_t latest_key_id);
  int get_latest_key_id_(const uint64_t user_tenant_id,
                         uint64_t &latest_key_id);

  int get_tenant_snap_ls_replica_simple_items_(
    const share::ObCloneJob &job,
    ObArray<share::ObTenantSnapLSReplicaSimpleItem>& ls_snapshot_array);

  int check_all_ls_restore_finish_(const share::ObCloneJob &job,
                                   TenantRestoreStatus &tenant_restore_status);
  int check_one_ls_restore_finish_(const share::ObCloneJob& job,
                                   const share::ObLSStatusInfo& ls_status_info,
                                   const ObArray<share::ObLSInfo>& ls_info_array,
                                   const ObArray<share::ObTenantSnapLSReplicaSimpleItem>& ls_snapshot_array,
                                   TenantRestoreStatus &tenant_restore_status);
  int check_data_version_before_finish_clone_(const uint64_t source_tenant_id);
private:
  static const int32_t MAX_RETRY_CNT = 5;
  static const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
  static const int64_t DEFAULT_IDLE_TIME = 60 * 1000 * 1000L;
  static const int64_t PROCESS_IDLE_TIME = 1 * 1000 * 1000L;
private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  int64_t idle_time_us_;
  bool work_immediately_;
  DISALLOW_COPY_AND_ASSIGN(ObCloneScheduler);
};

}
}

#endif // OCEANBASE_ROOTSERVER_OB_CLONE_SCHEDULER_H_
