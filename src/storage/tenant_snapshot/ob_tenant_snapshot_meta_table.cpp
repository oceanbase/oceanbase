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

#define USING_LOG_PREFIX STORAGE
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"

namespace oceanbase
{
namespace storage
{

void ObLSSnapshotBuildCtxVTInfo::reset()
{
  build_status_.reset();
  rebuild_seq_start_ = 0;
  rebuild_seq_end_ = 0;
  ls_meta_package_.reset();
  end_interval_scn_.reset();
}

void ObTenantSnapshotVTInfo::reset()
{
  tsnap_is_running_ = false;
  tsnap_has_unfinished_create_dag_ = false;
  tsnap_has_unfinished_gc_dag_ = false;
  tsnap_clone_ref_ = 0;
  tsnap_meta_existed_ = false;
}

void ObLSSnapshotVTInfo::reset()
{
  ls_id_.reset();
  tenant_snapshot_id_.reset();
  meta_existed_ = false;
  has_build_ctx_ = false;
  build_ctx_info_.reset();
  has_tsnap_info_ = false;
  tsnap_info_.reset();
}

bool ObLSSnapshotReportInfo::is_valid() const
{
  bool b_ret = ls_id_.is_valid();
  if (is_creating_succ_) {
    b_ret = b_ret &&
            end_interval_scn_.is_valid() &&
            begin_interval_scn_.is_valid() &&
            end_interval_scn_ >= begin_interval_scn_ &&
            ls_meta_package_ != nullptr && ls_meta_package_->is_valid();
  }
  return b_ret;
}

void ObLSSnapshotReportInfo::to_failed()
{
  is_creating_succ_ = false;
  begin_interval_scn_.reset();
  end_interval_scn_.reset();
  ls_meta_package_ = nullptr;
}

void ObLSSnapshotReportInfo::to_success(const SCN& begin_interval_scn,
                const SCN& end_interval_scn,
                const ObLSMetaPackage* ls_meta_package)
{
  is_creating_succ_ = true;
  begin_interval_scn_ = begin_interval_scn;
  end_interval_scn_ = end_interval_scn;
  ls_meta_package_ = ls_meta_package;
}

bool ObLSSnapshotReportInfo::scn_range_is_valid(const ObTenantSnapItem &tenant_snap_item) const
{
  int ret = OB_SUCCESS;
  bool bret = true;

  if ((ObTenantSnapStatus::CLONING == tenant_snap_item.get_status() ||
       ObTenantSnapStatus::NORMAL == tenant_snap_item.get_status())) {
    if (begin_interval_scn_ < tenant_snap_item.get_clog_start_scn() ||
        end_interval_scn_ > tenant_snap_item.get_snapshot_scn()) {
      ret = OB_BEYOND_THE_RANGE;
      bret = false;
    }
  }
  return bret;
}

void ObTenantSnapshotSvrInfo::reset()
{
  tenant_snap_item_.reset();
  ls_snap_item_arr_.reset();
}

int ObTenantSnapshotSvrInfo::get_creating_ls_id_arr(common::ObArray<ObLSID>& creating_ls_id_arr)
{
  int ret = OB_SUCCESS;

  creating_ls_id_arr.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_snap_item_arr_.count(); ++i) {
    if (ObLSSnapStatus::CREATING == ls_snap_item_arr_[i].get_status()) {
      if(OB_FAIL(creating_ls_id_arr.push_back(ls_snap_item_arr_[i].get_ls_id()))) {
        LOG_WARN("creating_ls_id_arr push back failed", KR(ret), KPC(this));
      }
    }
  }

  return ret;
}

int ObTenantSnapshotMetaTable::report_create_ls_snapshot_rlt(const ObTenantSnapshotID& tenant_snapshot_id,
                                                             ObLSSnapshotReportInfo& info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  ObTenantSnapItem tenant_snap_item;
  ObMySQLTransaction trans;
  ObTenantSnapshotTableOperator table_operator;
  ObTenantSnapLSReplicaSimpleItem ls_snap_replica_item;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant snapshot id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObLSSnapshotReportInfo is not valid", KR(ret), K(info));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("trans start failed", KR(ret));
  } else if (OB_FAIL(table_operator.init(tenant_id, &trans))) {
    LOG_WARN("fail to init operator", KR(ret));
  } else if (OB_FAIL(table_operator.get_tenant_snap_item(tenant_snapshot_id,
                                                         true /*need lock*/,
                                                         tenant_snap_item))) {
    LOG_WARN("fail to get tenant snapshot item", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(table_operator.get_tenant_snap_ls_replica_simple_item(tenant_snapshot_id,
                                                                           info.get_ls_id(),
                                                                           GCTX.self_addr(),
                                                                           true,
                                                                           ls_snap_replica_item))) {
    LOG_WARN("fail to get_tenant_snap_ls_replica_simple_item",
        KR(ret), K(tenant_snapshot_id), K(GCTX.self_addr()), K(info));
  } else if (ObTenantSnapStatus::DELETING == tenant_snap_item.get_status()) {
    ret = OB_TENANT_SNAPSHOT_NOT_EXIST;
    LOG_WARN("tenant snapshot is already in deleting status, do not need to be reported",
        KR(ret), K(tenant_snapshot_id), K(tenant_snap_item));
  } else if (ObLSSnapStatus::NORMAL == ls_snap_replica_item.get_status()) {
    LOG_INFO("ls snapshot is already in normal status, do not need to be reported",
        K(tenant_snapshot_id), K(info), K(ls_snap_replica_item));
  } else if (ObLSSnapStatus::CREATING != ls_snap_replica_item.get_status()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ls snapshot status not match",
        KR(ret), K(tenant_snapshot_id), K(info), K(ls_snap_replica_item));
  } else {
    // TODO: Currently, how to get the maximum scn in ObLSMetaPackage has not yet been solved;
    if (info.is_creating_succ()) {
      if (tenant_snap_item.get_status() != ObTenantSnapStatus::CREATING &&
          // ls_snap_replica_item.status condition will definitely be true,
          // just to enhance the check;
          ObLSSnapStatus::NORMAL != ls_snap_replica_item.get_status()) {
        LOG_WARN("local snapshot was created successfully, but the tenant snapshot status"
                 " in the meta table is not creating", K(info), K(tenant_snap_item));
        info.to_failed();
      }
    }

    if (OB_FAIL(update_ls_snap_replica_item_(info, ls_snap_replica_item))) {
      LOG_WARN("fail to update_ls_snap_replica_item_",
        KR(ret), K(tenant_snapshot_id), K(info), K(ls_snap_replica_item));
    } else if (OB_FAIL(table_operator.update_tenant_snap_ls_replica_item(ls_snap_replica_item,
                                                                         info.get_ls_meta_package()))) {
      LOG_WARN("fail to update_tenant_snap_ls_replica_item",
        KR(ret), K(tenant_snapshot_id), K(info), K(ls_snap_replica_item));
    }
  }

  const bool need_commit = OB_SUCC(ret);
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(need_commit))) {
      LOG_WARN("trans end failed", K(need_commit), KR(tmp_ret), KR(ret));
      ret = need_commit ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObTenantSnapshotMetaTable::update_ls_snap_replica_item_(const ObLSSnapshotReportInfo& info,
                                                            ObTenantSnapLSReplicaSimpleItem& item)
{
  int ret = OB_SUCCESS;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObLSSnapshotReportInfo is not valid", KR(ret), K(info));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObTenantSnapLSReplicaSimpleItem is not valid", KR(ret), K(item));
  } else {
    item.set_status(info.is_creating_succ() ? ObLSSnapStatus::NORMAL : ObLSSnapStatus::FAILED);
    item.set_begin_interval_scn(info.get_begin_interval_scn());
    item.set_end_interval_scn(info.get_end_interval_scn());
  }

  return ret;
}

int ObTenantSnapshotMetaTable::acquire_tenant_snapshot_svr_info(const ObTenantSnapshotID& tenant_snapshot_id,
                                                                ObTenantSnapshotSvrInfo& svr_info)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTenantSnapshotTableOperator table_operator;
  const uint64_t tenant_id = MTL_ID();

  svr_info.reset();
  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant snapshot id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("trans start failed", KR(ret));
  } else if (OB_FAIL(table_operator.init(tenant_id, &trans))) {
    LOG_WARN("fail to init operator", KR(ret));
  } else if (OB_FAIL(table_operator.get_tenant_snap_item(tenant_snapshot_id,
                                                         true /*need lock*/,
                                                         svr_info.get_tenant_snap_item()))) {
    LOG_WARN("fail to get snapshot item", KR(ret), K(tenant_snapshot_id));
  } else if (ObTenantSnapStatus::DELETING == svr_info.get_tenant_snap_item_const().get_status()) {
    LOG_INFO("tenant snapshot is deleting", KR(ret), K(tenant_snapshot_id), K(svr_info));
  } else if (OB_FAIL(table_operator.get_tenant_snap_ls_replica_simple_items(tenant_snapshot_id,
                                                                            GCTX.self_addr(),
                                                                            svr_info.get_ls_snap_item_arr()))) {
    LOG_WARN("fail to get_tenant_snap_ls_replica_simple_items", KR(ret),
        K(GCTX.self_addr()), K(tenant_snapshot_id));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(false))) {
      LOG_WARN("trans abort failed", KR(tmp_ret));
    }
  }

  return ret;
}

int ObTenantSnapshotMetaTable::acquire_all_tenant_snapshots(ObArray<ObTenantSnapItem>& item_arr)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();

  ObMySQLProxy *sql_client = GCTX.sql_proxy_;
  ObTenantSnapshotTableOperator table_operator;

  item_arr.reset();
  if (OB_FAIL(table_operator.init(tenant_id, sql_client))) {
    LOG_WARN("fail to init operator", KR(ret));
  } else if (OB_FAIL(table_operator.get_all_user_tenant_snap_items(item_arr))){
    LOG_WARN("fail to get_all_user_tenant_snap_items", KR(ret));
  }

  return ret;
}

void ObTenantSnapshotMetaTable::acquire_tenant_snapshot_trace_id(const ObTenantSnapshotID &tenant_snapshot_id,
                                                                 const ObTenantSnapOperation operation,
                                                                 common::ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;

  trace_id.reset();

  ObTenantSnapJobItem job_item;
  const uint64_t tenant_id = MTL_ID();

  ObMySQLProxy *sql_client = GCTX.sql_proxy_;
  ObTenantSnapshotTableOperator table_operator;

  if (OB_FAIL(table_operator.init(tenant_id, sql_client))) {
    LOG_WARN("fail to init operator", KR(ret));
  } else if (OB_FAIL(table_operator.get_tenant_snap_job_item(tenant_snapshot_id, operation, job_item))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to get tenant snapshot job item", KR(ret), K(tenant_snapshot_id), K(operation));
    }
  } else if (!job_item.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job item is not valid", KR(ret), K(tenant_snapshot_id), K(operation), K(job_item));
  }

  if (OB_SUCC(ret)) {
    trace_id = job_item.get_trace_id();
  } else {
    trace_id.init(GCTX.self_addr());
  }
}

int ObTenantSnapshotMetaTable::acquire_clone_ls_meta_package(const ObTenantSnapshotID &tenant_snapshot_id,
                                                             const ObLSID &ls_id,
                                                             ObLSMetaPackage& ls_meta_package)
{
  int ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();
  ls_meta_package.reset();

  share::SCN clog_checkpoint_scn;
  palf::LSN clog_base_lsn;
  ObTenantSnapshotTableOperator snap_op;

  if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant snapshot id is not valid", KR(ret), K(tenant_snapshot_id));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is not valid", KR(ret), K(ls_id), K(tenant_snapshot_id));
  } else if (OB_FAIL(snap_op.init(tenant_id, GCTX.sql_proxy_))) {
    LOG_WARN("fail to init snap op", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (OB_FAIL(snap_op.get_proper_ls_meta_package(tenant_snapshot_id,
                                                        ls_id,
                                                        ls_meta_package))) {
    LOG_WARN("fail to get proper ls meta package", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (!ls_meta_package.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min checkpoint ls replica's ls_meta_package is not valid",
        KR(ret), K(ls_meta_package), K(tenant_snapshot_id), K(ls_id));
  } else if (FALSE_IT(clog_checkpoint_scn = ls_meta_package.ls_meta_.get_clog_checkpoint_scn())) {
  } else if (FALSE_IT(clog_base_lsn = ls_meta_package.ls_meta_.get_clog_base_lsn())) {
  } else if (FALSE_IT(ls_meta_package.reset())) {
  } else if (OB_FAIL(snap_op.get_ls_meta_package(tenant_snapshot_id,
                                                 ls_id,
                                                 GCTX.self_addr(),
                                                 ls_meta_package))) {
    LOG_WARN("fail to get ls meta package", KR(ret), K(tenant_snapshot_id), K(ls_id));
  } else if (!ls_meta_package.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this ls replica's ls_meta_package is not valid",
        KR(ret), K(ls_meta_package), K(tenant_snapshot_id), K(ls_id));
  } else {
    ls_meta_package.update_clog_checkpoint_in_ls_meta(clog_checkpoint_scn, clog_base_lsn);
  }

  if (OB_FAIL(ret)) {
    ls_meta_package.reset();
  }

  return ret;
}

}
}
