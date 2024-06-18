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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_SNAPSHOT_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_TENANT_SNAPSHOT_SCHEDULER_H_

#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "rootserver/ob_tenant_thread_helper.h"

namespace oceanbase
{
namespace rootserver
{

// scheduler tenant snapshot job, this include:
//    1: process snapshot creation job (send RPC to each related obs to process)
//    2: process snapshot deletion job (send RPC to each related obs to process)
//    3: check snapshot creation result of each related obs.
//Note: this thread does not care about deletion result of each related obs.
class ObTenantSnapshotScheduler : public ObTenantThreadHelper,
                                  public logservice::ObICheckpointSubHandler,
                                  public logservice::ObIReplaySubHandler
{
public:
  ObTenantSnapshotScheduler();
  virtual ~ObTenantSnapshotScheduler();
  int init();
  virtual void do_work() override;
  void destroy();
  void wakeup();
  int idle();
  DEFINE_MTL_FUNC(ObTenantSnapshotScheduler)

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

private:
  int get_tenant_snapshot_jobs_(ObArray<ObCreateSnapshotJob> &create_jobs,
                                ObArray<ObDeleteSnapshotJob> &delete_jobs);
  int build_tenant_snapshot_create_job_(const ObTenantSnapItem &item,
                                        const ObArbitrationServiceStatus &arbitration_service_status,
                                        const int64_t paxos_replica_num,
                                        const int64_t timeout,
                                        ObCreateSnapshotJob &job);
  int build_tenant_snapshot_delete_job_(const ObTenantSnapItem &item,
                                        ObDeleteSnapshotJob &job);
  int prepare_for_create_tenant_snapshot_(const ObCreateSnapshotJob &create_job);
  int wait_related_task_finished_(const uint64_t user_tenant_id);
  int generate_snap_ls_items_(const ObTenantSnapshotID &tenant_snapshot_id,
                              const uint64_t user_tenant_id,
                              ObArray<ObTenantSnapLSItem> &snap_ls_items);
  int generate_snap_ls_replica_base_info_(const ObTenantSnapshotID &tenant_snapshot_id,
                                          const uint64_t user_tenant_id,
                                          const ObArray<ObTenantSnapLSItem> &snap_ls_items,
                                          const int64_t paxos_replica_num,
                                          ObArray<ObTenantSnapLSReplicaSimpleItem> &ls_replica_items);
  int get_ls_valid_replicas_(const ObLSID &ls_id,
                             const ObArray<ObLSInfo> &tenant_ls_infos,
                             ObArray<const ObLSReplica *> &valid_replicas);
  int process_create_tenant_snapshot_(const ObCreateSnapshotJob &create_job);
  int send_create_tenant_snapshot_rpc_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                       const uint64_t user_tenant_id,
                                       const ObArray<ObAddr> &addr_array);
  int check_create_tenant_snapshot_result_(const ObCreateSnapshotJob &create_job,
                                           SCN &clog_start_scn,
                                           SCN &snapshot_scn,
                                           bool &need_wait_minority);
  void check_need_wait_minority_create_snapshot_(const ObCreateSnapshotJob &create_job,
                                                 bool &need_wait_minority);
  int send_flush_ls_archive_rpc_(const uint64_t user_tenant_id,
                                 const ObArray<ObAddr> &addr_array);
  int check_log_archive_finish_(const uint64_t user_tenant_id,
                                const SCN &snapshot_scn,
                                bool &need_wait);
  int finish_create_tenant_snapshot_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                     const uint64_t user_tenant_id,
                                     const SCN &clog_start_scn,
                                     const SCN &snapshot_scn);
  int create_tenant_snapshot_fail_(const ObCreateSnapshotJob& create_job);
  int process_delete_tenant_snapshots_(const ObArray<ObDeleteSnapshotJob> &delete_jobs);
  int send_delete_tenant_snapshot_rpc_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                       const uint64_t user_tenant_id,
                                       const ObArray<common::ObAddr> &addr_array);
  int finish_delete_tenant_snapshot_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                                     const uint64_t user_tenant_id);

  // decide snapshot scn for clone tenant snapshot
  // @params[in]  table_op, operator to use
  // @params[in]  tenant_snapshot_id, snapshot_id
  // @params[in]  tenant_info, user tenant info
  // @params[in]  snapshot_scn, the scn of snapshot
  // @params[out] output_snapshot_scn, the scn to persist
  // @params[out] need_persist_scn, whether persist scn in table
  int decide_tenant_snapshot_scn_(
      ObTenantSnapshotTableOperator &table_op,
      const ObTenantSnapshotID &tenant_snapshot_id,
      const ObAllTenantInfo &tenant_info,
      const SCN &snapshot_scn,
      SCN &output_snapshot_scn,
      bool &need_persist_scn);

  // check standby tenant gts_scn exceed sync_scn
  // @params[in] table_op, table operator to use
  // @params[in] tenant_id, which tenant
  // @params[in] tenant_snapshot_id, which snapshot
  // @params[in] snapshot_scn_to_check, snapshot scn
  int check_standby_gts_exceed_snapshot_scn_(
      ObTenantSnapshotTableOperator &table_op,
      const uint64_t &tenant_id,
      const ObTenantSnapshotID &tenant_snapshot_id,
      const SCN &snapshot_scn_to_check);
private:
  static const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
  static const int64_t DEFAULT_IDLE_TIME = 60 * 1000 * 1000L;
  static const int64_t SNAPSHOT_CREATION_TIMEOUT = 300 * 1000 * 1000L;
  static const int64_t SNAPSHOT_DELETION_TIMEOUT = 5 * 1000 * 1000L;
  static const int64_t PROCESS_IDLE_TIME = 1 * 1000 * 1000L;
  static const int64_t WAIT_MINORITY_CREATE_SNAPSHOT_TIME = 20 * 1000 * 1000;
private:
  bool inited_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t idle_time_us_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSnapshotScheduler);
};
}
}


#endif // OCEANBASE_ROOTSERVER_OB_TENANT_SNAPSHOT_SCHEDULER_H_
