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

#ifndef OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H
#define OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H

#include "lib/container/ob_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace rootserver
{

struct ObDDLSingleReplicaExecutorParam final
{
public:
  ObDDLSingleReplicaExecutorParam()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      dest_tenant_id_(common::OB_INVALID_TENANT_ID),
      type_(share::DDL_INVALID),
      source_tablet_ids_(),
      dest_tablet_ids_(),
      source_table_id_(common::OB_INVALID_ID),
      dest_table_id_(common::OB_INVALID_ID),
      schema_version_(0),
      dest_schema_version_(0),
      snapshot_version_(0),
      task_id_(0),
      parallelism_(0),
      execution_id_(-1),
      data_format_version_(0),
      consumer_group_id_(0)
  {}
  ~ObDDLSingleReplicaExecutorParam() = default;
  bool is_valid() const {
    return common::OB_INVALID_TENANT_ID != tenant_id_ && common::OB_INVALID_TENANT_ID != dest_tenant_id_
           && share::DDL_INVALID != type_ && source_tablet_ids_.count() > 0 && dest_tablet_ids_.count() > 0
           && common::OB_INVALID_ID != source_table_id_ && common::OB_INVALID_ID != dest_table_id_
           && schema_version_ > 0 && dest_schema_version_ > 0 && snapshot_version_ > 0 && task_id_ > 0
           && execution_id_ >= 0 && data_format_version_ > 0 && consumer_group_id_ >= 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(dest_tenant_id), K_(type), K_(source_tablet_ids), K_(dest_tablet_ids),
               K_(source_table_id), K_(dest_table_id), K_(schema_version), K_(dest_schema_version),
               K_(snapshot_version), K_(task_id), K_(parallelism), K_(execution_id),
               K_(data_format_version), K_(consumer_group_id));
public:
  uint64_t tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObDDLType type_;
  common::ObArray<common::ObTabletID> source_tablet_ids_;
  common::ObArray<common::ObTabletID> dest_tablet_ids_;
  int64_t source_table_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t dest_schema_version_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t parallelism_;
  int64_t execution_id_;
  int64_t data_format_version_;
  int64_t consumer_group_id_;
};

class ObDDLSingleReplicaExecutor
{
public:
  int build(const ObDDLSingleReplicaExecutorParam &param);
  int check_build_end(bool &is_end, int64_t &ret_code);
  int set_partition_task_status(const common::ObTabletID &tablet_id,
                                const int ret_code,
                                const int64_t row_scanned,
                                const int64_t row_inserted);
  int get_progress(int64_t &row_scanned, int64_t &row_inserted);
private:
  int schedule_task();
private:
  enum class ObPartitionBuildStat
  {
    BUILD_INIT = 0,
    BUILD_REQUESTED = 1,
    BUILD_SUCCEED = 2,
    BUILD_RETRY = 3,
    BUILD_FAILED = 4
  };
  struct ObPartitionBuildInfo final
  {
  public:
    static const int64_t PARTITION_BUILD_HEART_BEAT_TIME = 10 * 1000 * 1000;
    ObPartitionBuildInfo()
      : ret_code_(common::OB_SUCCESS), stat_(ObPartitionBuildStat::BUILD_INIT), heart_beat_time_(0),
        row_inserted_(0), row_scanned_(0)
    {}
    ~ObPartitionBuildInfo() = default;
    bool need_schedule() const {
      return ObPartitionBuildStat::BUILD_INIT == stat_
        || ObPartitionBuildStat::BUILD_RETRY == stat_
        || (ObPartitionBuildStat::BUILD_REQUESTED == stat_
          && ObTimeUtility::current_time() - heart_beat_time_ > PARTITION_BUILD_HEART_BEAT_TIME);
    }
    TO_STRING_KV(K_(ret_code), K_(stat), K_(heart_beat_time), K_(row_inserted), K_(row_scanned));
  public:
    int64_t ret_code_;
    ObPartitionBuildStat stat_;
    int64_t heart_beat_time_;
    int64_t row_inserted_;
    int64_t row_scanned_;
  };
private:
  uint64_t tenant_id_;
  uint64_t dest_tenant_id_;
  share::ObDDLType type_;
  common::ObArray<common::ObTabletID> source_tablet_ids_;
  common::ObArray<common::ObTabletID> dest_tablet_ids_;
  common::ObArray<int64_t> tablet_task_ids_;
  int64_t source_table_id_;
  int64_t dest_table_id_;
  int64_t schema_version_;
  int64_t dest_schema_version_;
  int64_t snapshot_version_;
  int64_t task_id_;
  int64_t parallelism_;
  int64_t execution_id_;
  int64_t data_format_version_;
  int64_t consumer_group_id_;
  common::ObArray<ObPartitionBuildInfo> partition_build_stat_;
  common::ObSpinLock lock_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVICE_OB_DDL_SINGLE_REPLICA_EXECUTOR_H
