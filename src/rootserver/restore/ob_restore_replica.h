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

#ifndef __OB_RS_RESTORE_REPLICA_H__
#define __OB_RS_RESTORE_REPLICA_H__

#include "rootserver/restore/ob_restore_stat.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObPartitionKey;
class ObMySQLProxy;
}  // namespace common
namespace share {
namespace schema {
class ObSchemaGetterGuard;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share

namespace rootserver {
class ObRebalanceTaskMgr;
class TenantBalanceStat;

class ObRebalanceTaskInfo;
class OnlineReplica;
class Partition;
class Replica;
class ObRestoreReplica {
public:
  explicit ObRestoreReplica(ObRestoreMgrCtx& restore_ctx, RestoreJob& job_info, const volatile bool& is_stop);
  ~ObRestoreReplica();
  int restore();

private:
  /* functions */
  int check_stop()
  {
    return is_stop_ ? common::OB_CANCELED : common::OB_SUCCESS;
  }
  int do_restore();
  int finish_restore();
  int check_has_more_task(bool& has_more);
  int schedule_restore_task(int64_t& task_cnt);
  int choose_restore_dest_server(const common::ObPartitionKey& pkey, OnlineReplica& dst);
  int check_if_need_restore_partition(const PartitionRestoreTask& task, bool& need);
  int update_tenant_stat(share::schema::ObSchemaGetterGuard& schema_guard);
  int update_progress();
  int update_task_status(const PartitionRestoreTask& t, RestoreTaskStatus status);
  int update_job_status(RestoreTaskStatus status);
  int mark_task_doing(PartitionRestoreTask& task);
  int mark_task_done(PartitionRestoreTask& task);
  int mark_job_done();
  int schedule_restore_follower_task(int64_t& task_cnt);
  int restore_follower_replica(Partition& part, Replica& leader, Replica& r, int64_t& task_cnt);
  int choose_restore_dest_server(const Replica& r, OnlineReplica& dst);

  /* variables */
  const volatile bool& is_stop_;
  ObRestoreMgrCtx& ctx_;
  ObRestoreStat stat_;
  RestoreJob& job_info_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreReplica);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OB_RS_RESTORE_REPLICA_H__ */
//// end of header file
