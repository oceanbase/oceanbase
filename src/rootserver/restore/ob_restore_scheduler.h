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

#ifndef OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_

#include "share/backup/ob_backup_info_mgr.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_thread_idling.h"
#include "rootserver/restore/ob_restore_stat.h"
#include "rootserver/restore/ob_restore_info.h"
#include "rootserver/restore/ob_restore_util.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_ddl_service.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_upgrade_utils.h"

namespace oceanbase {
namespace rootserver {
class ObRestoreIdling : public ObThreadIdling {
public:
  explicit ObRestoreIdling(volatile bool& stop) : ObThreadIdling(stop)
  {
    reset();
  }
  virtual int64_t get_idle_interval_us();
  void set_idle_interval_us(int64_t idle_us);
  void reset();

private:
  int64_t idle_us_;
};

// Schedule daily merge (merge dynamic data to base line data).
// Running in a single thread.
class ObRestoreScheduler : public ObRsReentrantThread, public share::ObCheckStopProvider {
public:
  static const int64_t MAX_RESTORE_TASK_CNT = 10000;
  enum SetMemberListAction { BALANCE, SET_MEMBER_LIST, DONE };

public:
  ObRestoreScheduler();
  virtual ~ObRestoreScheduler();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy,
      obrpc::ObCommonRpcProxy& rpc_proxy, obrpc::ObSrvRpcProxy& srv_rpc_proxy, ObFreezeInfoManager& freeze_info_manager,
      share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, ObServerManager& server_manager,
      ObZoneManager& zone_manager, ObUnitManager& unit_manager, ObDDLService& ddl_service,
      const common::ObAddr& self_addr);
  virtual void run3() override;
  void wakeup();
  void stop();
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }

public:
  int mark_job_failed(const int64_t job_id, int return_ret, share::PhysicalRestoreMod mod,
      const common::ObCurTraceId::TraceId& trace_id, const common::ObAddr& addr);

private:
  int idle();
  int check_stop() const;

  int process_restore_job(const share::ObPhysicalRestoreJob& job);
  int try_recycle_job(const share::ObPhysicalRestoreJob& job);

  int restore_tenant(const share::ObPhysicalRestoreJob& job_info);
  int restore_sys_replica(const share::ObPhysicalRestoreJob& job_info);
  int upgrade_pre(const share::ObPhysicalRestoreJob& job_info);
  int upgrade_post(const share::ObPhysicalRestoreJob& job_info);
  int modify_schema(const share::ObPhysicalRestoreJob& job_info);
  int create_user_partitions(const share::ObPhysicalRestoreJob& job_info);
  int restore_user_replica(const share::ObPhysicalRestoreJob& job_info);
  int rebuild_index(const share::ObPhysicalRestoreJob& job_info);
  int post_check(const share::ObPhysicalRestoreJob& job_info);
  int restore_success(const share::ObPhysicalRestoreJob& job_info);
  int restore_fail(const share::ObPhysicalRestoreJob& job_info);

  /* restore tenant related */
  int fill_job_info(share::ObPhysicalRestoreJob& job, obrpc::ObCreateTenantArg& arg);
  int fill_backup_info(share::ObPhysicalRestoreJob& job, obrpc::ObCreateTenantArg& arg);
  int fill_rs_info(share::ObPhysicalRestoreJob& job);
  int fill_create_tenant_arg(const share::ObPhysicalRestoreJob& job_info, obrpc::ObCreateTenantArg& arg);
  int fill_max_sys_table_id(
      const common::ObIArray<common::ObPartitionKey>& pkey_list, share::ObPhysicalRestoreJob& job);
  int assign_pool_list(const char* str, common::ObIArray<common::ObString>& pool_list);
  int convert_restore_tenant_info(share::ObPhysicalRestoreJob& job_info);
  /*------------------------*/

  /* modify schema related */
  int force_drop_schema(const uint64_t tenant_id);
  int convert_schema_options(const uint64_t tenant_id);
  int convert_database_options(const uint64_t tenant_id);
  int convert_tablegroup_options(const uint64_t tenant_id);
  int convert_table_options(const uint64_t tenant_id);
  int convert_index_status(const share::ObPhysicalRestoreJob& job_info);
  int update_index_status(const common::ObIArray<uint64_t>& index_ids, share::schema::ObIndexStatus index_status);
  int convert_parameters(const share::ObPhysicalRestoreJob& job_info);
  int log_nop_operation(const share::ObPhysicalRestoreJob& job_info);
  /*------------------------*/

  /* restore replica related */
  int schedule_restore_task(
      const share::ObPhysicalRestoreJob& job_info, ObPhysicalRestoreStat& stat, int64_t& task_cnt);
  int schedule_restore_task(const share::ObPhysicalRestoreJob& job_info, ObPhysicalRestoreStat& stat,
      const PhysicalRestorePartition* partition, int64_t& task_cnt);
  int schedule_leader_restore_task(const PhysicalRestorePartition& partition, const share::ObPartitionReplica& replica,
      const share::ObPhysicalRestoreJob& job_info, const ObPhysicalRestoreStat& stat, int64_t& task_cnt);
  int schedule_follower_restore_task(const PhysicalRestorePartition& partition,
      const share::ObPartitionReplica& replica, const share::ObPhysicalRestoreJob& job_info,
      const ObPhysicalRestoreStat& stat, int64_t& task_cnt);
  int fill_restore_arg(
      const common::ObPartitionKey& pg_key, const share::ObPhysicalRestoreJob& job, share::ObPhysicalRestoreArg& arg);
  int choose_restore_data_source(
      const PhysicalRestorePartition& partition, const ObPhysicalRestoreStat& stat, ObReplicaMember& data_src);

  int set_member_list(const share::ObPhysicalRestoreJob& job_info, const ObPhysicalRestoreStat& stat);
  int get_set_member_list_action(
      const PhysicalRestorePartition& partition, const ObPhysicalRestoreStat& stat, SetMemberListAction& action);
  int check_locality_match(
      const PhysicalRestorePartition& partition, const ObPhysicalRestoreStat& stat, bool& locality_match);
  int add_member_list_pkey(const PhysicalRestorePartition& partition, const ObPhysicalRestoreStat& stat,
      ObRecoveryHelper::ObMemberListPkeyList& partition_infos);
  int add_member_list_pkey(const common::ObPartitionKey& pkey, const share::ObPartitionReplica::MemberList& member_list,
      ObRecoveryHelper::ObMemberListPkeyList& partition_infos);
  int batch_set_member_list(const ObRecoveryHelper::ObMemberListPkeyList& partition_infos);
  int send_batch_set_member_list_rpc(obrpc::ObSetMemberListBatchArg& arg, ObSetMemberListBatchProxy& batch_rpc_proxy);
  int build_member_list_map(const uint64_t tenant_id,
      common::hash::ObHashMap<common::ObPartitionKey, share::ObPartitionReplica::MemberList>& member_list_map);
  int batch_persist_member_list(
      ObArray<const PhysicalRestorePartition*>& persist_partitions, const ObPhysicalRestoreStat& stat);
  int fill_dml_splicer(
      const PhysicalRestorePartition& partition, const ObPhysicalRestoreStat& stat, share::ObDMLSqlSplicer& dml);
  int clear_member_list_table(const uint64_t tenant_id);

  int fill_restore_partition_arg(
      const uint64_t schema_id, const share::schema::ObPartitionSchema* schema, obrpc::ObRestorePartitionsArg& arg);
  /*------------------------*/

  /* upgrade related */
  int check_source_cluster_version(const uint64_t cluster_version);
  int do_upgrade_pre(const share::ObPhysicalRestoreJob& job_info);
  int do_upgrade_post(const share::ObPhysicalRestoreJob& job_info);
  /* upgrade related end */

  int check_locality_valid(const share::schema::ZoneLocalityIArray& locality);
  int refresh_schema(const share::ObPhysicalRestoreJob& job_info);
  int check_gts(const share::ObPhysicalRestoreJob& job_info);
  int try_update_job_status(int return_ret, const share::ObPhysicalRestoreJob& job,
      share::PhysicalRestoreMod mod = share::PHYSICAL_RESTORE_MOD_RS);
  void record_rs_event(const share::ObPhysicalRestoreJob& job, const share::PhysicalRestoreStatus next_status);
  share::PhysicalRestoreStatus get_next_status(int return_ret, share::PhysicalRestoreStatus current_status);

private:
  int drop_tenant_force_if_necessary(const share::ObPhysicalRestoreJob& job_info);

private:
  bool inited_;
  mutable ObRestoreIdling idling_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  ObFreezeInfoManager* freeze_info_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObServerManager* server_manager_;
  ObZoneManager* zone_mgr_;
  ObUnitManager* unit_mgr_;
  ObDDLService* ddl_service_;
  share::ObUpgradeProcesserSet upgrade_processors_;
  common::ObAddr self_addr_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreScheduler);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_RESTORE_SCHEDULER_H_
