/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_PARTITION_RANDOM_DISTRIBUTION_HELPER_H_
#define OCEABASE_STORAGE_OB_PARTITION_RANDOM_DISTRIBUTION_HELPER_H_

#include "common/ob_tablet_id.h"
#include "logservice/ob_log_base_type.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/ls/ob_ls_operator.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#include "storage/tablet/ob_tablet_random_mds_helper.h"
#include "storage/tablet/ob_tablet_random_mds_user_data.h"

namespace oceanbase
{
namespace storage
{
class ObTenantLSCntInfo
{
public:
  ObTenantLSCntInfo () : inited_(false), tenant_ls_cnt_map_() {}
  ~ObTenantLSCntInfo () {}
  int init();
  void reset();
  int get_changed_tenant_ids(const ObIArray<uint64_t> &tenant_ids,
                             const ObIArray<int64_t> &tenant_ls_arrays,
                             ObIArray<uint64_t> &changed_tenant_ids);
  int set_tenant_ls_cnt(const uint64_t tenant_id, const share::ObLSAttrArray &ls_array_array);
public:
  bool inited_;
  hash::ObHashMap<uint64_t, int64_t> tenant_ls_cnt_map_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantLSCntInfo);
};

class ObRandomPartitionArgBuilder final
{
public:
  ObRandomPartitionArgBuilder() {}
  ~ObRandomPartitionArgBuilder() {}
  int build_arg(const uint64_t tenant_id,
                const uint64_t table_id,
                const ObIArray<ObTabletID> &inactive_tablet_ids,
                const uint64_t specified_value,
                const int64_t available_ls_cnt,
                obrpc::ObAlterTableArg &arg);
private:
  int acquire_schema_info_of_table_(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const share::schema::ObTableSchema *&table_schema,
                                     const share::schema::ObSimpleDatabaseSchema *&db_schema,
                                     share::schema::ObSchemaGetterGuard &guard,
                                     obrpc::ObAlterTableArg &arg);
  int build_arg_(const uint64_t tenant_id,
                 const ObString &db_name,
                 const share::schema::ObTableSchema &table_schema,
                 const uint64_t table_id,
                 const ObIArray<ObTabletID> &inactive_tablet_ids,
                 const uint64_t specified_value,
                 obrpc::ObAlterTableArg &arg);
  int build_alter_table_schema_(const uint64_t tenant_id,
                                const ObString &db_name,
                                const share::schema::ObTableSchema &table_schema,
                                const uint64_t table_id,
                                share::schema::AlterTableSchema &alter_table_schema);
};

class ObRsRandomPartitionScheduler final : public share::ObRsSplitScheduler,
                                           public logservice::ObICheckpointSubHandler,
                                           public logservice::ObIReplaySubHandler,
                                           public logservice::ObIRoleChangeSubHandler
{
public:
  ObRsRandomPartitionScheduler() : ObRsSplitScheduler(), mutex_(common::ObLatchIds::OB_RS_RANDOM_PARTITION_SCHEDULER_LOCK), is_inited_(false), ls_cnt_info_() {}
  ~ObRsRandomPartitionScheduler() {}

  // for checkpoint
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &rec_scn) override { return OB_SUCCESS; }
  // for replay
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;
  // mtl_functions
  static int mtl_init(ObRsRandomPartitionScheduler *&scheduler);
  static void mtl_stop(ObRsRandomPartitionScheduler *&scheduler);
  static void mtl_wait(ObRsRandomPartitionScheduler *&scheduler);

  int init();
  void reset();
  void destroy() { reset(); }

  static int schedule_random_part_task();
  static int refresh_auto_random_part();

  // delegate functions
  virtual int push_tasks(const ObArray<share::ObAutoSplitTask> &task_array) override;
  virtual int pop_tasks(const int64_t num_tasks_can_pop, const bool throttle_by_table, ObArray<share::ObAutoSplitTask> &task_array) override;
  int get_changed_tenant_ids(const ObIArray<uint64_t> &tenant_ids,
                             const ObIArray<int64_t> &tenant_ls_arrays,
                             ObIArray<uint64_t> &changed_tenant_ids);
  int set_tenant_ls_cnt(const uint64_t tenant_id, const share::ObLSAttrArray &ls_array_array);
  static int get_random_distributed_table(const ObIArray<uint64_t> &changed_tenant_ids,
                                          ObIArray<ObArray<int64_t>> &tenant_table_ids);
private:
  lib::ObMutex mutex_;
  bool is_inited_;
  ObTenantLSCntInfo ls_cnt_info_;
  DISALLOW_COPY_AND_ASSIGN(ObRsRandomPartitionScheduler);
};

class ObServerRandomPartitionScheduler final : public share::ObServerSplitScheduler
{
public:
  static ObServerRandomPartitionScheduler &get_instance();
private:
  ObServerRandomPartitionScheduler () {}
  ~ObServerRandomPartitionScheduler () {}
  int check_and_fetch_tablet_split_info(const storage::ObTabletHandle &teblet_handle, storage::ObLS &ls, bool &can_random_part, share::ObAutoSplitTask &task);
};

}//share
}//oceanbase

#endif /* OCEABASE_SHARE_OB_PARTITION_RANDOM_DISTRIBUTION_HELPER_H_ */
