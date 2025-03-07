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
#include "rootserver/ob_root_service.h"
#include "share/ob_ddl_sim_point.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::storage;

int ObSingleReplicaBuildCtx::init(
    const ObAddr& addr,
    const share::ObDDLType ddl_type,
    const int64_t src_table_id,
    const int64_t dest_table_id,
    const int64_t src_schema_version,
    const int64_t dest_schema_version,
    const int64_t tablet_task_id,
    const int64_t compaction_scn,
    const ObTabletID &src_tablet_id,
    const ObTabletID &dest_tablet_id,
    const bool can_reuse_macro_block,
    const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (!addr.is_valid() ||
             src_table_id == OB_INVALID_ID ||
             dest_table_id == OB_INVALID_ID ||
             tablet_task_id == 0 ||
             !src_tablet_id.is_valid() ||
             !dest_tablet_id.is_valid() ||
             (is_tablet_split(ddl_type) && (compaction_scn == 0 || parallel_datum_rowkey_list.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr), K(src_table_id), K(dest_table_id),
                                 K(tablet_task_id), K(src_tablet_id), K(dest_tablet_id),
                                 K(ddl_type), K(compaction_scn), K(parallel_datum_rowkey_list));
  } else if (OB_FAIL(parallel_datum_rowkey_list_.assign(parallel_datum_rowkey_list))) { // shallow copy.
    LOG_WARN("assign failed", K(ret), K(parallel_datum_rowkey_list));
  } else {
    addr_ = addr;
    ddl_type_ = ddl_type;
    src_table_id_ = src_table_id;
    dest_table_id_ = dest_table_id;
    src_schema_version_ = src_schema_version;
    dest_schema_version_ = dest_schema_version;
    tablet_task_id_ = tablet_task_id;
    compaction_scn_ = compaction_scn;
    src_tablet_id_ = src_tablet_id;
    dest_tablet_id_ = dest_tablet_id;
    can_reuse_macro_block_ = can_reuse_macro_block;
    reset_build_stat();
    is_inited_ = true;
  }
  return ret;
}

void ObSingleReplicaBuildCtx::reset_build_stat()
{
  stat_ = ObReplicaBuildStat::BUILD_INIT;
  ret_code_ = OB_SUCCESS;
  heart_beat_time_ = 0;
  row_inserted_ = 0;
  row_scanned_ = 0;
  physical_row_count_ = 0;
}

bool ObSingleReplicaBuildCtx::is_valid() const
{
  bool valid =  is_inited_ && addr_.is_valid() && src_table_id_ != OB_INVALID_ID &&
                dest_table_id_ != OB_INVALID_ID && src_schema_version_ != 0 &&
                dest_schema_version_ != 0 && tablet_task_id_ != 0 &&
                src_tablet_id_.is_valid() && dest_tablet_id_.is_valid();
  if (is_tablet_split(ddl_type_)) {
    valid &= (compaction_scn_ != 0 && !parallel_datum_rowkey_list_.empty());
  }
  return valid;
}

int ObSingleReplicaBuildCtx::assign(const ObSingleReplicaBuildCtx &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parallel_datum_rowkey_list_.assign(other.parallel_datum_rowkey_list_))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    is_inited_ = other.is_inited_;
    addr_ = other.addr_;
    ddl_type_ = other.ddl_type_;
    src_table_id_ = other.src_table_id_;
    dest_table_id_ = other.dest_table_id_;
    src_schema_version_ = other.src_schema_version_;
    dest_schema_version_ = other.dest_schema_version_;
    tablet_task_id_ = other.tablet_task_id_;
    compaction_scn_ = other.compaction_scn_;
    src_tablet_id_ = other.src_tablet_id_;
    can_reuse_macro_block_ = other.can_reuse_macro_block_;
    stat_ = other.stat_;
    ret_code_ = other.ret_code_;
    heart_beat_time_ = other.heart_beat_time_;
    row_inserted_ = other.row_inserted_;
    row_scanned_ = other.row_scanned_;
    physical_row_count_ = other.physical_row_count_;
    dest_tablet_id_ = other.dest_tablet_id_;
  }
  return ret;
}

int ObSingleReplicaBuildCtx::check_need_schedule(bool &need_schedule) const
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    const int64_t elapsed_time = ObTimeUtility::current_time() - heart_beat_time_;
    const bool timeout = (elapsed_time > REPLICA_BUILD_HEART_BEAT_TIME);
    if (stat_ == ObReplicaBuildStat::BUILD_INIT ||
        stat_ == ObReplicaBuildStat::BUILD_RETRY ||
        (stat_ == ObReplicaBuildStat::BUILD_REQUESTED && timeout)) {
      need_schedule = true;
    }
  }
  return ret;
}

int ObDDLReplicaBuildExecutor::build(const ObDDLReplicaBuildExecutorParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(DDL_SIM(param.tenant_id_, param.task_id_, SINGLE_REPLICA_EXECUTOR_BUILD_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(param.tenant_id_), K(param.task_id_));
  } else { // lock scope, keep construct_replica_build_ctxs() out of scope
    ObSpinLockGuard guard(lock_);
    tenant_id_ = param.tenant_id_;
    dest_tenant_id_ = param.dest_tenant_id_;
    ddl_type_ = param.ddl_type_;
    ddl_task_id_ = param.task_id_;
    snapshot_version_ = param.snapshot_version_;
    parallelism_ = param.parallelism_;
    execution_id_ = param.execution_id_;
    data_format_version_ = param.data_format_version_;
    consumer_group_id_ = param.consumer_group_id_;
    min_split_start_scn_ = param.min_split_start_scn_;
    is_no_logging_ = param.is_no_logging_;
    ObArray<ObSingleReplicaBuildCtx> replica_build_ctxs;
    if (OB_FAIL(construct_replica_build_ctxs(param, replica_build_ctxs))) {
      LOG_WARN("failed to construct replica build ctxs", K(ret));
    } else if (OB_FAIL(lob_col_idxs_.assign(param.lob_col_idxs_))) {
      LOG_WARN("failed to assign to lob col idxs", K(ret));
    } else if (OB_FAIL(src_tablet_ids_.assign(param.source_tablet_ids_))) {
      LOG_WARN("failed to assign to tablet ids", K(ret));
    } else if (OB_FAIL(dest_tablet_ids_.assign(param.dest_tablet_ids_))) {
      LOG_WARN("failed to assign to dest tablet ids", K(ret));
    } else if (OB_FAIL(replica_build_ctxs_.assign(replica_build_ctxs))) {
      LOG_WARN("failed to setup replica build ctxs", K(ret));
    } else {
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      is_inited_ = false;
    }
  } // lock scope, keep schedule_task() out of lock scope

  // TODO(lihongqin.lhq)
  // char table_id_buffer[256];
  // snprintf(table_id_buffer, sizeof(table_id_buffer), "dest_table_id:%ld, source_table_id:%ld", dest_table_id_, source_table_id_);
  // ROOTSERVICE_EVENT_ADD("ddl scheduler", "build single replica",
  //   "tenant_id",tenant_id_,
  //   "ret", ret,
  //   "trace_id", *ObCurTraceId::get_trace_id(),
  //   K_(task_id),
  //   "type", type_,
  //   K_(schema_version),
  //   table_id_buffer);

  if (OB_SUCC(ret)) {
    LOG_INFO("start to schedule task", K(src_tablet_ids_.count()), "ddl_event_info", ObDDLEventInfo());
    if (OB_FAIL(schedule_task())) {
      LOG_WARN("fail to schedule tasks", K(ret));
    } else {
      LOG_INFO("start to schedule task", K(param.source_tablet_ids_));
    }
  } else {
    LOG_INFO("fail to build single replica task", K(ret), "ddl_event_info", ObDDLEventInfo());
  }
  return ret;
}

int ObDDLReplicaBuildExecutor::schedule_task()
{
  int ret = OB_SUCCESS;
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(rpc_proxy));
  } else if (OB_FAIL(DDL_SIM(dest_tenant_id_, ddl_task_id_, SINGLE_REPLICA_EXECUTOR_SCHEDULE_TASK_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(dest_tenant_id_), K(ddl_task_id_));
  } else {
    ObDDLBuildSingleReplicaRequestProxy proxy(*rpc_proxy,
        &obrpc::ObSrvRpcProxy::build_ddl_single_replica_request);
    ObArray<obrpc::ObDDLBuildSingleReplicaRequestArg> args;
    ObArray<ObAddr> addrs;
    ObArray<ObTabletID> tablet_ids;
    { // lock scope
      ObSpinLockGuard guard(lock_); // ensure build ctxs will not change
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_build_ctxs_.count(); ++i) {
        ObSingleReplicaBuildCtx &replica_build_ctx = replica_build_ctxs_.at(i);
        bool need_schedule = false;
        if (OB_FAIL(replica_build_ctx.check_need_schedule(need_schedule))) {
          LOG_WARN("failed to check need schedule", K(ret));
        } else if (need_schedule) {
          obrpc::ObDDLBuildSingleReplicaRequestArg arg;
          if (OB_FAIL(construct_rpc_arg(replica_build_ctx, arg))) {
            LOG_WARN("failed to construct single replica request arg", K(ret));
          } else if (OB_FAIL(args.push_back(arg))) {
            LOG_WARN("failed to push back arg", K(ret));
          } else if (OB_FAIL(addrs.push_back(replica_build_ctx.addr_))) {
            LOG_WARN("failed to push back addr", K(ret));
          } else if (OB_FAIL(tablet_ids.push_back(replica_build_ctx.src_tablet_id_))) {
            LOG_WARN("failed to push back tablet id", K(ret));
          }
        }
      }
    } // lock scope
    // keep send rpc out of lock scope
    for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
      const ObAddr &addr = addrs.at(i);
      if (OB_FAIL(proxy.call(addr, rpc_timeout, dest_tenant_id_, args.at(i)))) {
        LOG_WARN("failed to send rpc", K(ret), K(addr), K(rpc_timeout),
            K(args.at(i)));
      } else {
        LOG_INFO("send build single replica request", K(addr), K(args.at(i)));
      }
    }
    int tmp_ret = OB_SUCCESS;
    common::ObArray<int> ret_array;
    if (OB_SUCCESS != (tmp_ret = proxy.wait_all(ret_array))) {
      LOG_WARN("rpc_proxy wait failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      const ObIArray<const obrpc::ObDDLBuildSingleReplicaRequestResult *>
        &result_array = proxy.get_results();
      if (OB_FAIL(process_rpc_results(tablet_ids, addrs, result_array, ret_array))) {
        LOG_WARN("failed to process result", K(ret));
      }
    }
  }
  return ret;
}

/* before check if build is finished, get refreshed replica addrs for all tablets,
 * after that, refresh replica build ctxs, finally check each replica build status
 */
int ObDDLReplicaBuildExecutor::check_build_end(const bool need_checksum, bool &is_end, int64_t &ret_code)
{
  int ret = OB_SUCCESS;
  is_end = false;
  ret_code = OB_SUCCESS;
  ObArray<ObTabletID> replica_tablet_ids;
  ObArray<ObAddr> replica_addrs;
  int64_t succ_cnt = 0;
  int64_t failed_cnt = 0;
  int64_t reschedule_cnt = 0;
  int64_t waiting_cnt = 0;
  int64_t total_cnt = 0;
  int64_t dest_table_id = OB_INVALID_ID;
  const bool send_to_all_replicas = is_tablet_split(ddl_type_);
  // keep get replica addr out of lock scope
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (OB_FAIL(get_refreshed_replica_addrs(send_to_all_replicas,
          replica_tablet_ids, replica_addrs))) {
    LOG_WARN("failed to get refreshed replica addrs", K(ret));
  }
  { // lock scope
    ObSpinLockGuard guard(lock_);
    // first use refresh addrs to refresh build ctxs
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(refresh_replica_build_ctxs(replica_tablet_ids, replica_addrs))) {
      LOG_WARN("failed to refresh replica build ctxs", K(ret));
    } else if (!replica_build_ctxs_.empty()) {
      dest_table_id = replica_build_ctxs_.at(0).dest_table_id_;
    }
    total_cnt = replica_build_ctxs_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      const ObSingleReplicaBuildCtx &replica_build_ctx = replica_build_ctxs_.at(i);
      bool need_schedule = false;
      if (replica_build_ctx.stat_ == ObReplicaBuildStat::BUILD_FAILED) {
        ++failed_cnt;
        LOG_WARN("check build end, task has failed", K(replica_build_ctx.ret_code_),
            K(replica_build_ctx));
        if (ret_code == OB_SUCCESS) {
          ret_code = replica_build_ctx.ret_code_;
        }
      } else if (replica_build_ctx.stat_ == ObReplicaBuildStat::BUILD_SUCCEED) {
        ++succ_cnt;
      } else if (OB_FAIL(replica_build_ctx.check_need_schedule(need_schedule))) {
        LOG_WARN("failed to check need schedule", K(ret));
      } else if (need_schedule) {
        ++reschedule_cnt;
        LOG_INFO("replica build need reschedule", K(replica_build_ctx));
      } else { // rpc requested, waiting for report
        ++waiting_cnt;
      }
    }
  } // lock scope, keep schedule task out of lock scope
  if (OB_FAIL(ret)) {
  } else if (failed_cnt != 0) {
    // ret_code already set in for loop
    is_end = true;
    LOG_INFO("exist replica build task failed", K(failed_cnt), K(total_cnt));
  } else if (reschedule_cnt != 0) {
    if (OB_FAIL(schedule_task())) {
      LOG_WARN("fail to schedule task", K(ret));
    } else {
      LOG_INFO("replica build task schedule again", K(reschedule_cnt), K(total_cnt));
    }
  } else if (succ_cnt == total_cnt) {
    is_end = true;
    ret_code = ret;
    LOG_INFO("all replica build finished", K(succ_cnt), K(total_cnt));
    if (!share::is_tablet_split(ddl_type_) && need_checksum) {
      if (OB_FAIL(ObCheckTabletDataComplementOp::check_finish_report_checksum(
              dest_tenant_id_, dest_table_id, execution_id_, ddl_task_id_))) {
        LOG_WARN("fail to check sstable checksum_report_finish",
            K(ret), K(dest_tenant_id_), K(dest_table_id), K(execution_id_), K(ddl_task_id_));
      }
    }
  }
  LOG_INFO("check build end:", K(succ_cnt), K(failed_cnt), K(reschedule_cnt),
      K(waiting_cnt), K(total_cnt));
  return ret;
}

// update replica build ctx if tablet_id && addr is matched
// do nothing if no one matches
int ObDDLReplicaBuildExecutor::update_build_progress(
    const common::ObTabletID &tablet_id,
    const ObAddr &addr,
    const int ret_code,
    const int64_t row_scanned,
    const int64_t row_inserted,
    const int64_t physical_row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(addr));
  } else { // lock scope
    ObSpinLockGuard guard(lock_);
    if (!is_inited_) { // hold lock before access is_inited_
      ret = OB_NOT_INIT;
      LOG_WARN("replica build executor not init", K(ret));
    } else {
      bool is_found = false;
      ObSingleReplicaBuildCtx *replica_build_ctx = nullptr;
      if (OB_FAIL(get_replica_build_ctx(tablet_id, addr,
              replica_build_ctx, is_found))) {
        LOG_WARN("failed to get replica build ctx", K(ret), K(tablet_id), K(addr));
      } else if (is_found) {
        if (OB_FAIL(update_replica_build_ctx(*replica_build_ctx,
                ret_code, row_scanned, row_inserted, physical_row_count, false/*is_rpc_request*/,
                true/*is_observer_report*/))) {
          LOG_WARN("failed to update replica build ctx", K(ret), K(tablet_id), K(addr), K(ret_code));
        }
        LOG_INFO("receive build progress report from replica", K(tablet_id), K(addr), K(ret_code));
      } else { // not found
        LOG_INFO("ignore build progress report from expired replica", K(tablet_id), K(addr), K(ret_code));
      }
    }
  } // lock scope
  return ret;
}

int ObDDLReplicaBuildExecutor::get_progress(int64_t &row_inserted, int64_t &physical_row_count, double &percent)
{
  int ret = OB_SUCCESS;
  bool all_done = true;
  row_inserted = 0;
  physical_row_count = 0;
  percent = 0;
  // lock scope
  ObSpinLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < replica_build_ctxs_.count(); ++i) {
    row_inserted += replica_build_ctxs_.at(i).row_inserted_;
    physical_row_count += replica_build_ctxs_.at(i).physical_row_count_;
    if (ObReplicaBuildStat::BUILD_SUCCEED != replica_build_ctxs_.at(i).stat_) {
      all_done = false;
    }
  }
  // 100% if all replica_build_ctxs_.at(i).stat_ == BUILD_SUCCEED
  if (OB_FAIL(ret)){
    // error occurred
  } else if (all_done) { // lob meta maybe 0 rows, percent should be 0; (in row storing)
    percent = 100.0;
  } else if (physical_row_count == 0) {
    percent = 0.0;
  } else {
    percent = row_inserted * 100.0 / physical_row_count;
  }
  return ret;
}

// as caller, schedule_task() will hold lock
int ObDDLReplicaBuildExecutor::construct_rpc_arg(
    const ObSingleReplicaBuildCtx &replica_build_ctx,
    obrpc::ObDDLBuildSingleReplicaRequestArg &arg) const
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObLSID dest_ls_id;
  share::ObLocationService *location_service = GCTX.location_service_;
  bool is_cache_hit = false;
  const bool force_renew = true;
  const int64_t expire_renew_time = force_renew ? INT64_MAX : 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (!replica_build_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica_build_ctx));
  } else {
    arg.tenant_id_ = tenant_id_;
    arg.dest_tenant_id_ = dest_tenant_id_;
    arg.source_tablet_id_ = replica_build_ctx.src_tablet_id_;
    arg.dest_tablet_id_ = replica_build_ctx.dest_tablet_id_;
    arg.source_table_id_ = replica_build_ctx.src_table_id_;
    arg.dest_schema_id_ = replica_build_ctx.dest_table_id_;
    arg.schema_version_ = replica_build_ctx.src_schema_version_;
    arg.dest_schema_version_ = replica_build_ctx.dest_schema_version_;
    arg.snapshot_version_ = snapshot_version_;
    arg.ddl_type_ = ddl_type_;
    arg.task_id_ = ddl_task_id_;
    arg.execution_id_ = execution_id_;
    arg.tablet_task_id_ = replica_build_ctx.tablet_task_id_;
    arg.data_format_version_ = data_format_version_;
    arg.consumer_group_id_ = consumer_group_id_;
    arg.compaction_scn_ = replica_build_ctx.compaction_scn_;
    arg.can_reuse_macro_block_ = replica_build_ctx.can_reuse_macro_block_;
    arg.min_split_start_scn_   = min_split_start_scn_;
    /** handle OB_SESSION_NOT_FOUND(-4067) may lead to infinite retry of table recovery task.
      * Due to the number limit(100) of blocked thread stream rpc receiver
      * reduce table recovery retry parallelism. */
    if (ObDDLType::DDL_TABLE_RESTORE == ddl_type_ && replica_build_ctx.sess_not_found_times_ > 0) {
      arg.parallelism_ = MAX(1, parallelism_ >> replica_build_ctx.sess_not_found_times_);
    } else {
      arg.parallelism_ = parallelism_;
    }
    arg.is_no_logging_ = is_no_logging_;
    if (OB_FAIL(arg.lob_col_idxs_.assign(lob_col_idxs_))) {
      LOG_WARN("failed to assign to lob col idxs", K(ret));
    } else if (OB_FAIL(arg.parallel_datum_rowkey_list_.assign(replica_build_ctx.parallel_datum_rowkey_list_))) {
      LOG_WARN("failed to assign split ranges", K(ret));
    } else if (OB_ISNULL(location_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location service is nullptr", K(ret));
    } else if (OB_FAIL(location_service->get(tenant_id_, arg.source_tablet_id_,
            expire_renew_time, is_cache_hit, ls_id))) {
      LOG_WARN("get ls failed", K(ret), K(arg.source_tablet_id_));
    } else if (OB_FAIL(location_service->get(dest_tenant_id_, arg.dest_tablet_id_,
            expire_renew_time, is_cache_hit, dest_ls_id))) {
      LOG_WARN("get dest ls failed", K(ret), K(arg));
    } else {
      arg.ls_id_ = ls_id;
      arg.dest_ls_id_ = dest_ls_id;
    }
  }
  return ret;
}

int ObDDLReplicaBuildExecutor::process_rpc_results(
    const ObArray<ObTabletID> &tablet_ids,
    const ObArray<ObAddr> addrs,
    const ObIArray<const obrpc::ObDDLBuildSingleReplicaRequestResult *> &result_array,
    const ObArray<int> &ret_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (tablet_ids.count() != addrs.count() ||
             ret_array.count() != addrs.count() ||
             result_array.count() != addrs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, rets count is not equal to request count", K(ret),
        K(tablet_ids.count()), K(addrs.count()), K(ret_array.count()),
        K(result_array.count()));
  }
  { // lock scope
    ObSpinLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < result_array.count(); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      const ObAddr &addr = addrs.at(i);
      bool is_found = false;
      ObSingleReplicaBuildCtx *replica_build_ctx = nullptr;
      if (OB_FAIL(get_replica_build_ctx(tablet_id, addr, replica_build_ctx,
              is_found))) {
        LOG_WARN("failed to get replica build ctx", K(ret));
      } else if (is_found) {
        if (replica_build_ctx->stat_ != ObReplicaBuildStat::BUILD_INIT) {
          continue; // already handle respone rpc
        } else if (OB_FAIL(update_build_ctx(*replica_build_ctx,
                result_array.at(i), ret_array.at(i)))) {
          LOG_WARN("failed to update build progress", K(ret));
        }
      } else { // not found, replica addr refreshed, ignore result
        LOG_INFO("replica addr refreshed, ignore rpc result from ",
            K(tablet_id), K(addr));
      }
    }
  } // lock scope
  return ret;
}

// as caller, process_rpc_result() will hold lock
int ObDDLReplicaBuildExecutor::update_build_ctx(
    ObSingleReplicaBuildCtx &build_ctx,
    const oceanbase::obrpc::ObDDLBuildSingleReplicaRequestResult *result,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (OB_FAIL(update_replica_build_ctx(build_ctx, ret_code,
          result->row_scanned_, result->row_inserted_, result->physical_row_count_, true/*is_rpc_request*/,
          false/*is_observer_report*/))) {
    LOG_WARN("failed to update replica build ctx", K(ret));
  }
  return ret;
}

// as caller, build() will hold lock
int ObDDLReplicaBuildExecutor::construct_replica_build_ctxs(
    const ObDDLReplicaBuildExecutorParam &param,
    ObArray<ObSingleReplicaBuildCtx> &replica_build_ctxs) const
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> split_replica_addrs;
  replica_build_ctxs.reuse();
  const bool send_to_all_replicas = is_tablet_split(param.ddl_type_);
  const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (send_to_all_replicas) {
    ObLSID ls_id;
    ObAddr unused_addr;
    const ObTabletID &any_tablet_id = param.source_tablet_ids_.at(0);
    if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(
          GCTX.location_service_, param.tenant_id_, any_tablet_id, rpc_timeout, ls_id, unused_addr))) {
      LOG_WARN("get ls id failed", K(ret), K(param.tenant_id_), K(any_tablet_id));
    } else if (OB_FAIL(ObDDLUtil::get_split_replicas_addrs(param.tenant_id_, ls_id, split_replica_addrs))) {
      LOG_WARN("get split replica addrs failed", K(ret), K(param.tenant_id_), K(ls_id));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.source_tablet_ids_.count(); ++i) {
      const ObTabletID &src_tablet_id = param.source_tablet_ids_.at(i);
      const ObTabletID &dest_tablet_id = param.dest_tablet_ids_.at(i);
      const int64_t src_table_id = param.source_table_ids_.at(i);
      const int64_t dest_table_id = param.dest_table_ids_.at(i);
      const int64_t src_schema_version = param.source_schema_versions_.at(i);
      const int64_t dest_schema_version = param.dest_schema_versions_.at(i);
      int64_t tablet_task_id = i + 1;
      int64_t compaction_scn = send_to_all_replicas ? param.compaction_scns_.at(i) : 0;
      const bool can_reuse_macro_block = send_to_all_replicas ? param.can_reuse_macro_blocks_.at(i) : false;
      ObSEArray<blocksstable::ObDatumRowkey, 8> unused_empty_rowkey_list; // placeholder only.
      const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list = send_to_all_replicas ?
          param.parallel_datum_rowkey_list_.at(i) : unused_empty_rowkey_list;
      if (send_to_all_replicas) {
        for (int64_t j = 0; OB_SUCC(ret) && j < split_replica_addrs.count(); ++j) {
          ObSingleReplicaBuildCtx replica_build_ctx;
          if (OB_FAIL(replica_build_ctx.init(split_replica_addrs.at(j), ddl_type_,
                  src_table_id, dest_table_id, src_schema_version,
                  dest_schema_version, tablet_task_id, compaction_scn,
                  src_tablet_id, dest_tablet_id, can_reuse_macro_block, parallel_datum_rowkey_list))) {
            LOG_WARN("failed to init replica build ctx", K(ret));
          } else if (OB_FAIL(replica_build_ctxs.push_back(replica_build_ctx))) {
            LOG_WARN("failed to push back replica build ctx", K(ret));
          }
        }
      } else { // send to all leader only
        ObLSID unused_ls_id;
        ObAddr orig_leader_addr;
        ObAddr dest_leader_addr;
        const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
        ObSingleReplicaBuildCtx replica_build_ctx;
        if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(GCTX.location_service_,
                param.tenant_id_, src_tablet_id, rpc_timeout, unused_ls_id, orig_leader_addr))) {
          LOG_WARN("failed to get orig leader addr", K(ret), K(src_tablet_id));
        } else if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(GCTX.location_service_,
                param.dest_tenant_id_, dest_tablet_id, rpc_timeout, unused_ls_id,
                dest_leader_addr))) {
          LOG_WARN("failed to get dest leader addr", K(ret), K(dest_tablet_id));
        } else if (ObDDLType::DDL_TABLE_RESTORE != ddl_type_ &&
            orig_leader_addr != dest_leader_addr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orig leader addr not equal dest leader addr", K(ret), K(orig_leader_addr),
              K(dest_leader_addr));
        } else if (OB_FAIL(replica_build_ctx.init(dest_leader_addr, ddl_type_,
                src_table_id, dest_table_id, src_schema_version, dest_schema_version,
                tablet_task_id, compaction_scn, src_tablet_id, dest_tablet_id, can_reuse_macro_block, parallel_datum_rowkey_list))) {
          LOG_WARN("failed to init replica build ctx", K(ret), K(src_tablet_id));
        } else if (OB_FAIL(replica_build_ctxs.push_back(replica_build_ctx))) {
          LOG_WARN("failed to push back replica build ctx", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLReplicaBuildExecutor::get_refreshed_replica_addrs(
    const bool send_to_all_replicas,
    ObArray<ObTabletID> &replica_tablet_ids,
    ObArray<ObAddr> &replica_addrs) const
{
  int ret = OB_SUCCESS;
  // TODO hanxuan optimization hold lock before refresh addrs (ObMemberListLockUtils)
  replica_tablet_ids.reuse();
  replica_addrs.reuse();
  const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (OB_UNLIKELY(src_tablet_ids_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), KPC(this));
  } else if (send_to_all_replicas) {
    ObLSID ls_id;
    ObAddr unused_addr;
    ObArray<ObAddr> split_replica_addrs;
    const ObTabletID &any_tablet_id = src_tablet_ids_.at(0);
    if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(
          GCTX.location_service_, tenant_id_, any_tablet_id, rpc_timeout, ls_id, unused_addr))) {
      LOG_WARN("get ls id failed", K(ret), K(tenant_id_), K(any_tablet_id));
    } else if (OB_FAIL(ObDDLUtil::get_split_replicas_addrs(tenant_id_, ls_id, split_replica_addrs))) {
      LOG_WARN("get split replica addrs failed", K(ret), K(tenant_id_), K(ls_id));
    } else { // send to all replicas.
      for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_ids_.count(); ++i) {
        const ObTabletID &src_tablet_id = src_tablet_ids_.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < split_replica_addrs.count(); ++j) {
          const ObAddr &addr = split_replica_addrs.at(j);
          if (OB_FAIL(replica_tablet_ids.push_back(src_tablet_id))) {
            LOG_WARN("failed to push back tablet id", K(ret));
          } else if (OB_FAIL(replica_addrs.push_back(addr))) {
            LOG_WARN("failed to push back addr", K(ret));
          }
        }
      }
    }
  } else { // send to leader only.
    for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_ids_.count(); ++i) {
      ObLSID ls_id;
      ObAddr orig_leader_addr;
      ObAddr dest_leader_addr;
      const ObTabletID &src_tablet_id = src_tablet_ids_.at(i);
      const ObTabletID &dest_tablet_id = dest_tablet_ids_.at(i);
      if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(GCTX.location_service_,
              tenant_id_, src_tablet_id, rpc_timeout, ls_id, orig_leader_addr))) {
        LOG_WARN("failed to get orig leader addr", K(ret), K(src_tablet_id));
      } else if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(GCTX.location_service_,
              dest_tenant_id_, dest_tablet_id, rpc_timeout, ls_id,
              dest_leader_addr))) {
        LOG_WARN("failed to get dest leader addr", K(ret), K(dest_tablet_id));
      } else if (ObDDLType::DDL_TABLE_RESTORE != ddl_type_ &&
          orig_leader_addr != dest_leader_addr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("orig leader addr not equal dest leader addr", K(ret),
            K(orig_leader_addr), K(dest_leader_addr));
      } else {
        if (OB_FAIL(replica_tablet_ids.push_back(src_tablet_id))) {
          LOG_WARN("failed to push back tablet id", K(ret));
        } else if (OB_FAIL(replica_addrs.push_back(dest_leader_addr))) {
          LOG_WARN("failed to push back addr", K(ret));
        }
      }
    }
  }
  return ret;
}

/* for each replica, check if there exists build ctx whose tablet id && addr matches
 *   - if exists, copy it to new replica build ctx with its build stat
 *   - otherwise, init a new build ctx whose build stat is BUILD_INIT
 *   NOTE as caller, check_build_end() will hold lock_
 */
int ObDDLReplicaBuildExecutor::refresh_replica_build_ctxs(
    const ObArray<ObTabletID> &replica_tablet_ids,
    const ObArray<ObAddr> &replica_addrs)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (replica_tablet_ids.empty() || replica_addrs.empty() ||
             replica_tablet_ids.count() != replica_addrs.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(replica_tablet_ids.count()), K(replica_addrs.count()));
  }
  ObArray<ObSingleReplicaBuildCtx> new_replica_build_ctxs;
  for (int64_t i = 0; OB_SUCC(ret) && i < replica_addrs.count(); ++i) {
    const ObTabletID &tablet_id = replica_tablet_ids.at(i);
    const ObAddr &replica_addr = replica_addrs.at(i);
    bool is_found = false;
    ObSingleReplicaBuildCtx *existing_ctx = nullptr;
    if (OB_FAIL(get_replica_build_ctx(tablet_id, replica_addr,
            existing_ctx, is_found))) {
      LOG_WARN("failed to get replica build ctx", K(ret),
          K(tablet_id), K(replica_addr));
    } else if (is_found) {
      if (OB_FAIL(new_replica_build_ctxs.push_back(*existing_ctx))) {
        LOG_WARN("failed to push back replica build ctx", K(ret));
      }
    } else { // not found
      ObSingleReplicaBuildCtx new_build_ctx;
      bool ctx_partial_inited = false;
      // 1. use existing replica build ctx to init new build ctx
      // NOTE currently new build ctx's addr is an unrelated addr
      for (int64_t j = 0; OB_SUCC(ret) && !ctx_partial_inited &&
          j < replica_build_ctxs_.count(); ++j) {
        // find corresponding tablet
        if (replica_build_ctxs_.at(j).src_tablet_id_ == tablet_id) {
          if (OB_FAIL(new_build_ctx.assign(replica_build_ctxs_.at(j)))) {
            LOG_WARN("failed to assign to replica build ctx", K(ret));
          } else {
            // use correspoinding tablet's old replica ctx to init
            ctx_partial_inited = true;
          }
        }
      }
      // 2. overwrite with correct addr && reset build stat
      if (OB_FAIL(ret)) {
      } else if (!ctx_partial_inited) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to setup new replica build ctx for refreshed replica addr",
            K(ret), K(tablet_id), K(replica_addr));
      } else if (FALSE_IT(new_build_ctx.addr_ = replica_addr)) {
      } else if (FALSE_IT(new_build_ctx.reset_build_stat())) {
      } else if (OB_FAIL(new_replica_build_ctxs.push_back(new_build_ctx))) {
        LOG_WARN("failed to push back replica build ctx", K(ret));
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to setup new replica build ctx for refreshed replica addr",
            K(ret), K(tablet_id), K(replica_addr), K(new_build_ctx));
      } else {
        LOG_INFO("setup new replica build ctx for refreshed replica addr",
            K(tablet_id), K(replica_addr), K(new_build_ctx));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(replica_build_ctxs_.assign(new_replica_build_ctxs))) {
    LOG_WARN("failed to assign to replica build ctxs", K(ret));
  } else if (new_replica_build_ctxs.count() != replica_tablet_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of replica build ctxs doesn't equal to the size of replica_tablet_ids", K(ret), K(replica_build_ctxs_.count()), K(replica_tablet_ids.count()));
  }
  return ret;
}

// look up repica build ctx by tablet id && addr
// NOTE as caller, update_build_progress() process_rpc_results()
//      refresh_replica_build_ctxs() will hold lock
int ObDDLReplicaBuildExecutor::get_replica_build_ctx(
    const ObTabletID &tablet_id,
    const ObAddr &addr,
    ObSingleReplicaBuildCtx *&replica_build_ctx,
    bool &is_found)
{
  int ret = OB_SUCCESS;
  replica_build_ctx = nullptr;
  is_found = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (!tablet_id.is_valid() || !addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(addr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < replica_build_ctxs_.count(); ++i) {
    if (replica_build_ctxs_.at(i).src_tablet_id_ == tablet_id &&
        replica_build_ctxs_.at(i).addr_ == addr) {
      replica_build_ctx = &replica_build_ctxs_.at(i);
      is_found = true;
    }
  }
  return ret;
}

// NOTE as caller, update_build_progress(), update_build_ctx() will hold lock
int ObDDLReplicaBuildExecutor::update_replica_build_ctx(
    ObSingleReplicaBuildCtx &build_ctx,
    const int64_t ret_code,
    const int64_t row_scanned,
    const int64_t row_inserted,
    const int64_t physical_row_count,
    const bool is_rpc_request,
    const bool is_observer_report)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("replica build executor not init", K(ret));
  } else if (ret_code == OB_SUCCESS) {
    build_ctx.ret_code_ = OB_SUCCESS;
    if (is_rpc_request) {
      build_ctx.row_inserted_ = MAX(build_ctx.row_inserted_, row_inserted);
      build_ctx.row_scanned_ = MAX(build_ctx.row_scanned_, row_scanned);
      build_ctx.physical_row_count_ = MAX(build_ctx.physical_row_count_, physical_row_count);
      build_ctx.stat_ = ObReplicaBuildStat::BUILD_REQUESTED;
      LOG_INFO("rpc send successfully", K(build_ctx.addr_),
          K(build_ctx.src_tablet_id_), K(build_ctx.dest_tablet_id_));
    } else if (is_observer_report) {
      build_ctx.row_inserted_ = row_inserted;
      build_ctx.row_scanned_ = row_scanned;
      build_ctx.physical_row_count_ = physical_row_count;
      build_ctx.stat_ = ObReplicaBuildStat::BUILD_SUCCEED;
      LOG_INFO("receive observer build success report", K(build_ctx.addr_),
          K(build_ctx.src_tablet_id_), K(build_ctx.dest_tablet_id_));
    }
  } else if (ObIDDLTask::in_ddl_retry_white_list(ret_code)) {
    build_ctx.ret_code_ = OB_SUCCESS;
    build_ctx.row_inserted_ = 0;
    build_ctx.row_scanned_ = 0;
    build_ctx.physical_row_count_ = 0;
    build_ctx.stat_ = ObReplicaBuildStat::BUILD_RETRY;
    if (ret_code == common::OB_SESSION_NOT_FOUND) {
      build_ctx.sess_not_found_times_++;
    }
    LOG_INFO("task need retry", K(ret_code), K(build_ctx.addr_),
        K(build_ctx.src_tablet_id_), K(build_ctx.dest_tablet_id_),
        K(is_rpc_request), K(is_observer_report));
  } else { // other error ret_code
    build_ctx.ret_code_ = ret_code;
    build_ctx.row_inserted_ = 0;
    build_ctx.row_scanned_ = 0;
    build_ctx.physical_row_count_ = 0;
    build_ctx.stat_ = ObReplicaBuildStat::BUILD_FAILED;
    LOG_INFO("task is failed", K(build_ctx.addr_), K(build_ctx.src_tablet_id_),
        K(build_ctx.dest_tablet_id_), K(is_rpc_request), K(is_observer_report), K(build_ctx));
  }
  if (OB_FAIL(ret)) {
  } else if (is_rpc_request) {
    build_ctx.heart_beat_time_ = ObTimeUtility::current_time();
  }
  return ret;
}
