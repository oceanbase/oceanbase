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

#define USING_LOG_PREFIX RS
#include "ob_ddl_single_replica_executor.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/ob_ddl_common.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::storage;

int ObDDLSingleReplicaExecutor::build(const ObDDLSingleReplicaExecutorParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    ObSpinLockGuard guard(lock_);
    tenant_id_ = param.tenant_id_;
    dest_tenant_id_ = param.dest_tenant_id_;
    type_ = param.type_;
    source_table_id_ = param.source_table_id_;
    dest_table_id_ = param.dest_table_id_;
    schema_version_ = param.schema_version_;
    dest_schema_version_ = param.dest_schema_version_;
    snapshot_version_ = param.snapshot_version_;
    task_id_ = param.task_id_;
    execution_id_ = param.execution_id_;
    parallelism_ = param.parallelism_;
    data_format_version_ = param.data_format_version_;
    consumer_group_id_ = param.consumer_group_id_;

    common::ObIArray<ObPartitionBuildInfo> &build_infos = partition_build_stat_;
    const common::ObIArray<ObTabletID> &tablet_ids = param.source_tablet_ids_;
    if (0 == build_infos.count()) {   // first time init
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        ObPartitionBuildInfo build_info_tmp;
        build_info_tmp.stat_ = ObPartitionBuildStat::BUILD_INIT;
        if (OB_FAIL(build_infos.push_back(build_info_tmp))) {
          LOG_WARN("fail to push back build info", K(ret));
        } else if (OB_FAIL(tablet_task_ids_.push_back(i + 1))) {
          LOG_WARN("fail to push tablet task id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(source_tablet_ids_.assign(param.source_tablet_ids_))) {
        LOG_WARN("fail to assign tablet ids", K(ret));
      } else if (OB_FAIL(dest_tablet_ids_.assign(param.dest_tablet_ids_))) {
        LOG_WARN("fail to assign tablet ids", K(ret));
      }
      if (OB_FAIL(ret)) {
        source_tablet_ids_.reset();
        dest_tablet_ids_.reset();
        build_infos.reset();
        tablet_task_ids_.reset();
      }
    } else {      // timeout, need reset task status
      for (int64_t i = 0; OB_SUCC(ret) && i < build_infos.count(); ++i) {
        ObPartitionBuildInfo &build_info = build_infos.at(i);
        if (ObPartitionBuildStat::BUILD_REQUESTED == build_info.stat_) {
          build_info.stat_ = ObPartitionBuildStat::BUILD_INIT;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("start to schedule task", K(source_tablet_ids_.count()), K(dest_table_id_));
    if (OB_FAIL(schedule_task())) {
      LOG_WARN("fail to schedule tasks", K(ret));
    }
  }
  return ret;
}

int ObDDLSingleReplicaExecutor::schedule_task()
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  share::ObLocationService *location_service = GCTX.location_service_;
  if (OB_ISNULL(rpc_proxy) || OB_ISNULL(location_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(rpc_proxy), KP(location_service));
  } else {
    ObDDLBuildSingleReplicaRequestProxy proxy(*rpc_proxy,
        &obrpc::ObSrvRpcProxy::build_ddl_single_replica_request);
    common::ObIArray<ObPartitionBuildInfo> &build_infos = partition_build_stat_;
    ObArray<int64_t> idxs; // to record the tablets position in build_infos that need to be scheduled.
    const int64_t current_time = ObTimeUtility::current_time();
    int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    const bool force_renew = true;
    bool is_cache_hit = false;
    const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
    ObLSID orig_ls_id;
    ObLSID dest_ls_id;
    common::ObArray<int> ret_array;
    ObArray<common::ObTabletID> request_source_tablet_ids;
    ObArray<common::ObTabletID> request_dest_tablet_ids;
    ObArray<int64_t> request_tablet_task_ids;
    int tmp_ret = OB_SUCCESS;
    {
      ObSpinLockGuard guard(lock_);
      // send rpc request may cost too much time, thus set some status before rpc request under lock.
      for (int64_t i = 0; OB_SUCC(ret) && i < build_infos.count(); i++) {
        ObPartitionBuildInfo &build_info = build_infos.at(i);
        if (build_info.need_schedule()) {
          if (OB_FAIL(request_source_tablet_ids.push_back(source_tablet_ids_.at(i)))) {
            LOG_WARN("push backed failed", K(ret));
          } else if (OB_FAIL(request_dest_tablet_ids.push_back(dest_tablet_ids_.at(i)))) {
            LOG_WARN("push back failed", K(ret));
          } else if (OB_FAIL(request_tablet_task_ids.push_back(tablet_task_ids_.at(i)))) {
            LOG_WARN("push back failed", K(ret));
          } else if (OB_FAIL(idxs.push_back(i))) {
            LOG_WARN("fail to push back idx", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < request_source_tablet_ids.count(); ++i) {
      // get leader of partition
      ObAddr orig_leader_addr;
      ObAddr dest_leader_addr;
      obrpc::ObDDLBuildSingleReplicaRequestArg arg;
      arg.ls_id_ = share::ObLSID::INVALID_LS_ID;
      arg.dest_ls_id_ = share::ObLSID::INVALID_LS_ID;
      arg.tenant_id_ = tenant_id_;
      arg.dest_tenant_id_ = dest_tenant_id_;
      arg.source_tablet_id_ = request_source_tablet_ids.at(i);
      arg.dest_tablet_id_ = request_dest_tablet_ids.at(i);
      arg.source_table_id_ = source_table_id_;
      arg.dest_schema_id_ = dest_table_id_;
      arg.schema_version_ = schema_version_;
      arg.dest_schema_version_ = dest_schema_version_;
      arg.snapshot_version_ = snapshot_version_;
      arg.ddl_type_ = type_;
      arg.task_id_ = task_id_;
      arg.parallelism_ = parallelism_;
      arg.execution_id_ = execution_id_;
      arg.data_format_version_ = data_format_version_;
      arg.tablet_task_id_ = request_tablet_task_ids.at(i);
      arg.consumer_group_id_ = consumer_group_id_;
      if (OB_FAIL(location_service->get(tenant_id_, arg.source_tablet_id_,
              expire_renew_time, is_cache_hit, orig_ls_id))) {
        LOG_WARN("get ls failed", K(ret), K(arg.source_tablet_id_));
      } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id, tenant_id_, orig_ls_id, force_renew, orig_leader_addr))) {
        LOG_WARN("get leader failed", K(ret), K(tenant_id_), K(orig_ls_id));
      } else if (FALSE_IT(arg.ls_id_ = orig_ls_id)) {
      } else if (OB_FAIL(location_service->get(dest_tenant_id_, arg.dest_tablet_id_,
                          expire_renew_time, is_cache_hit, dest_ls_id))) {
        LOG_WARN("get ls failed", K(ret), K(arg.dest_tablet_id_));
      } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id, dest_tenant_id_, dest_ls_id, force_renew, dest_leader_addr))) {
        LOG_WARN("get leader failed", K(ret), K(dest_tenant_id_), K(dest_ls_id));
      } else if (ObDDLType::DDL_TABLE_RESTORE != arg.ddl_type_ && orig_leader_addr != dest_leader_addr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orig leader addr not equal dest leader addr", K(ret), K(orig_leader_addr), K(dest_leader_addr), K(arg));
      } else if (FALSE_IT(arg.dest_ls_id_ = dest_ls_id)) {
      } else if (OB_FAIL(proxy.call(dest_leader_addr, rpc_timeout, dest_tenant_id_, arg))) {
        LOG_WARN("fail to send rpc", K(ret), K(rpc_timeout));
      } else {
        LOG_INFO("send build single replica request", K(arg), K(dest_leader_addr));
      }
    }
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(ret_array))) {
      LOG_WARN("rpc_proxy wait failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      const ObIArray<const obrpc::ObDDLBuildSingleReplicaRequestResult *> &result_array = proxy.get_results();
      if (ret_array.count() != idxs.count() || result_array.count() != idxs.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, ret array count is not equal to request count", K(ret));
      }
      ObSpinLockGuard guard(lock_);
      for (int64_t i = 0; OB_SUCC(ret) && i < ret_array.count(); ++i) {
        const int64_t idx = idxs.at(i);
        if (!build_infos.at(idx).need_schedule()) { // already handle respone rpc
          continue;
        } else if (OB_SUCCESS == ret_array.at(i)) {
          build_infos.at(idx).stat_ = ObPartitionBuildStat::BUILD_REQUESTED;
          build_infos.at(idx).heart_beat_time_ = ObTimeUtility::current_time();
          build_infos.at(idx).row_inserted_ = MAX(build_infos.at(idx).row_inserted_, result_array.at(i)->row_inserted_);
          build_infos.at(idx).row_scanned_ = MAX(build_infos.at(idx).row_scanned_, result_array.at(i)->row_scanned_);
          LOG_INFO("rpc send successfully", K(source_tablet_ids_.at(idx)), K(dest_tablet_ids_.at(idx)));
        } else if (ObIDDLTask::in_ddl_retry_white_list(ret_array.at(i))) {
          build_infos.at(idx).stat_ = ObPartitionBuildStat::BUILD_RETRY;
          build_infos.at(idx).row_inserted_ = 0;
          build_infos.at(idx).row_scanned_ = 0;
          LOG_INFO("task need retry", K(ret_array.at(i)), K(source_tablet_ids_.at(idx)), K(dest_tablet_ids_.at(idx)));
        } else {
          build_infos.at(idx).stat_ = ObPartitionBuildStat::BUILD_FAILED;
          build_infos.at(idx).ret_code_ = ret_array.at(i);
          build_infos.at(idx).heart_beat_time_ = ObTimeUtility::current_time();
          build_infos.at(idx).row_inserted_ = 0;
          build_infos.at(idx).row_scanned_ = 0;
          LOG_INFO("task is failed", K(build_infos.at(idx)), K(source_tablet_ids_.at(idx)), K(dest_tablet_ids_.at(idx)));
        }
      }
    }
  }
  return ret;
}

int ObDDLSingleReplicaExecutor::check_build_end(bool &is_end, int64_t &ret_code)
{
  int ret = OB_SUCCESS;
  is_end = false;
  bool has_fail = false;
  bool need_schedule = false;
  int64_t succ_cnt = 0;
  {
    ObSpinLockGuard guard(lock_);
    const common::ObIArray<ObPartitionBuildInfo> &build_infos = partition_build_stat_;
    for (int64_t i = 0; OB_SUCC(ret) && i < build_infos.count() && !has_fail; ++i) {
      has_fail = (ObPartitionBuildStat::BUILD_FAILED == build_infos.at(i).stat_);
      if (has_fail) {
        is_end = true;
        ret_code = build_infos.at(i).ret_code_;
        LOG_WARN("check build end, task has failed", K(ret_code));
      }
    }
    if (OB_SUCC(ret) && !is_end) {
      for (int64_t i = 0; OB_SUCC(ret) && i < build_infos.count(); ++i) {
        succ_cnt +=  ObPartitionBuildStat::BUILD_SUCCEED == build_infos.at(i).stat_;
        need_schedule |= build_infos.at(i).need_schedule();
      }
      if (OB_SUCC(ret) && build_infos.count() == succ_cnt) {
        if (OB_FAIL(ObCheckTabletDataComplementOp::check_finish_report_checksum(
              dest_tenant_id_, dest_table_id_, execution_id_, task_id_))) {
          LOG_WARN("fail to check sstable checksum_report_finish",
            K(ret), K(dest_tenant_id_), K(dest_table_id_), K(execution_id_), K(task_id_));
        }
        is_end = true;
        ret_code = ret;
      }
    }
  }
  if (OB_SUCC(ret) && need_schedule) {
    if (OB_FAIL(schedule_task())) {
      LOG_WARN("fail to schedule task", K(ret));
    } else {
      LOG_INFO("need schedule again", K(succ_cnt));
    }
  }
  return ret;
}

int ObDDLSingleReplicaExecutor::set_partition_task_status(const common::ObTabletID &tablet_id,
                                                          const int ret_code,
                                                          const int64_t row_scanned,
                                                          const int64_t row_inserted)
{
  LOG_INFO("ObDDLSingleReplicaExecutor::set_partition_task_status", K(ret_code));
  int ret = OB_SUCCESS;
  common::ObIArray<ObPartitionBuildInfo> &build_infos = partition_build_stat_;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else {
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < source_tablet_ids_.count(); ++i) {
      if (tablet_id == source_tablet_ids_.at(i)) {
        if (OB_SUCCESS == ret_code) {
          build_infos.at(i).ret_code_ = OB_SUCCESS;
          build_infos.at(i).stat_ = ObPartitionBuildStat::BUILD_SUCCEED;
          build_infos.at(i).row_inserted_ = row_inserted;
          build_infos.at(i).row_scanned_ = row_scanned;
        } else if (ObIDDLTask::in_ddl_retry_white_list(ret_code)) {
          build_infos.at(i).ret_code_ = OB_SUCCESS;
          build_infos.at(i).stat_ = ObPartitionBuildStat::BUILD_RETRY;
          build_infos.at(i).row_inserted_ = 0;
          build_infos.at(i).row_scanned_ = 0;
        } else {
          build_infos.at(i).ret_code_ = ret_code;
          build_infos.at(i).stat_ = ObPartitionBuildStat::BUILD_FAILED;
          build_infos.at(i).row_inserted_ = 0;
          build_infos.at(i).row_scanned_ = 0;
        }
      }
    }
  }
  return ret;
}

int ObDDLSingleReplicaExecutor::get_progress(int64_t &row_scanned, int64_t &row_inserted)
{
  int ret = OB_SUCCESS;
  row_scanned = 0;
  row_inserted = 0;
  ObSpinLockGuard guard(lock_);
  const common::ObIArray<ObPartitionBuildInfo> &build_infos = partition_build_stat_;
  for (int64_t i = 0; OB_SUCC(ret) && i < build_infos.count(); ++i) {
    row_scanned += build_infos.at(i).row_scanned_;
    row_inserted += build_infos.at(i).row_inserted_;
  }
  return ret;
}
