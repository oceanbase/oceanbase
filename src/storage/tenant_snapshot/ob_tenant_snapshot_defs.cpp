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

#include "share/ob_errno.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"

namespace oceanbase
{
namespace storage
{

int ObTenantSnapshot::init(const ObTenantSnapshotID& tenant_snapshot_id,
                           ObLSSnapshotMgr* ls_snapshot_mgr,
                           ObTenantMetaSnapshotHandler* meta_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSnapshot init twice", KR(ret), K(tenant_snapshot_id_));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id_));
  } else if (OB_ISNULL(ls_snapshot_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot_mgr is nullptr", KR(ret));
  } else {
    tenant_snapshot_id_ = tenant_snapshot_id;
    is_inited_ = true;
    is_running_ = true;
    has_unfinished_create_dag_ = false;
    has_unfinished_gc_dag_ = false;
    clone_ref_ = 0;
    meta_existed_ = false;
    ls_snapshot_mgr_ = ls_snapshot_mgr;
    meta_handler_ = meta_handler;
  }

  return ret;
}

int ObTenantSnapshot::load()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard snapshot_guard(mutex_);

  meta_existed_ = true;
  ObArray<ObLSID> ls_id_arr;
  if (OB_FAIL(meta_handler_->get_all_ls_snapshot(tenant_snapshot_id_, ls_id_arr))) {
    LOG_WARN("fail to get_all_ls_snapshot", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_id_arr.count(); ++i) {
      ObLSID ls_id = ls_id_arr.at(i);
      ObLSSnapshot* ls_snapshot = nullptr;
      if (OB_FAIL(ls_snapshot_mgr_->acquire_ls_snapshot(tenant_snapshot_id_, ls_id, ls_snapshot))) {
        LOG_WARN("fail to acquire_ls_snapshot", KR(ret), K(ls_id));
      } else {
        if (OB_FAIL(ls_snapshot->load())) {
          LOG_WARN("fail to load", KR(ret), K(tenant_snapshot_id_), K(ls_id));
        } else {
          LOG_INFO("ls snapshot load succ", K(tenant_snapshot_id_), K(ls_id));
        }
        ls_snapshot_mgr_->revert_ls_snapshot(ls_snapshot);
      }
    }
  }
  LOG_INFO("tenant snapshot load finished", KR(ret), K(tenant_snapshot_id_));
  return ret;
}

int ObTenantSnapshot::try_start_create_tenant_snapshot_dag(ObArray<ObLSID>& creating_ls_id_arr,
                                                           common::ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard snapshot_guard(mutex_);
  ObTenantSnapshotSvrInfo svr_info;
  creating_ls_id_arr.reset();
  trace_id.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshot is not init", KR(ret));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTenantSnapshot is not running", KR(ret), KPC(this));
  } else if (has_unfinished_dag_()) {
    ret = OB_EAGAIN;
    LOG_INFO("ObTenantSnapshot has unfinished dag", KR(ret), KPC(this));
  } else if (OB_FAIL(ObTenantSnapshotMetaTable::acquire_tenant_snapshot_svr_info(tenant_snapshot_id_,
                                                                                 svr_info))) {
    if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
      ret = OB_NO_NEED_UPDATE;
    } else {
      LOG_WARN("fail to acquire_tenant_snapshot_svr_info", KR(ret), K(tenant_snapshot_id_));
    }
  // TODO: Currently, how to get the maximum scn in ObLSMetaPackage has not yet been solved;
  } else if (ObTenantSnapStatus::CREATING != svr_info.get_tenant_snap_item_const().get_status()) {
    ret = OB_NO_NEED_UPDATE;
    LOG_INFO("tenant snapshot status in the meta table is not creating",
        KR(ret), K(tenant_snapshot_id_));
  } else if (OB_FAIL(svr_info.get_creating_ls_id_arr(creating_ls_id_arr))) {
    LOG_WARN("fail to get_creating_ls_id_arr", KR(ret), K(tenant_snapshot_id_), K(svr_info));
  } else if (creating_ls_id_arr.empty()) {
    ret = OB_NO_NEED_UPDATE;
    LOG_INFO("creating_ls_id_arr is empty", KR(ret), K(tenant_snapshot_id_), K(svr_info));
  } else {
    ObTenantSnapshotMetaTable::acquire_tenant_snapshot_trace_id(tenant_snapshot_id_,
                                                                ObTenantSnapOperation::CREATE,
                                                                trace_id);
    create_dag_start_();
  }

  return ret;
}

int ObTenantSnapshot::try_start_gc_tenant_snapshot_dag(const bool tenant_has_been_dropped,
                                                       bool &gc_tenant_snapshot,
                                                       ObArray<ObLSID> &gc_ls_id_arr,
                                                       common::ObCurTraceId::TraceId& trace_id)
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotSvrInfo svr_info;

  lib::ObMutexGuard snapshot_guard(mutex_);
  // gc tenant snapshot when:
  //   1. no entry for this snapshot in __all_tenant_snapshot(but we see it in local storage);
  //   2. snapshot status in __all_tenant_snapshot is DELETING.
  // or gc ls_snapshot only
  gc_tenant_snapshot = false;
  gc_ls_id_arr.reset();
  trace_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshot is not init", KR(ret));
  } else if (0 != clone_ref_) {
    ret = OB_EAGAIN;
    LOG_INFO("This tenant snapshot is being used for cloning and cannot be gc currently",
        KR(ret), KPC(this));
  } else if (has_unfinished_dag_()) {
    ret = OB_EAGAIN;
    LOG_INFO("ObTenantSnapshot has unfinished dag", KR(ret), KPC(this));
  } else if (tenant_has_been_dropped) {
    gc_tenant_snapshot = true;
    FLOG_INFO("tenant has been dropped, need gc", KPC(this));
  } else if (OB_FAIL(ObTenantSnapshotMetaTable::acquire_tenant_snapshot_svr_info(tenant_snapshot_id_,
                                                                                 svr_info))) {
    if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      gc_tenant_snapshot = true;
      LOG_INFO("tenant snapshot not exist, need gc", K(tenant_snapshot_id_));
    }
  } else if (ObTenantSnapStatus::DELETING == svr_info.get_tenant_snap_item().get_status()) {
    gc_tenant_snapshot = true;
    LOG_INFO("tenant snapshot status is DELETING, need gc", K(tenant_snapshot_id_));
  }

  if (OB_SUCC(ret)) {
    if (gc_tenant_snapshot) {  // gc tenant snapshot (with corresponding ls_snapshot)
      is_running_ = false;
    } else {
      if (OB_FAIL(get_need_gc_ls_snapshot_arr_(svr_info.get_ls_snap_item_arr(),
                                               gc_ls_id_arr))) {
        LOG_WARN("fail to get_need_gc_ls_snapshot_arr_", KR(ret), K(svr_info));
      } else if (gc_ls_id_arr.count() > 0) {
        LOG_INFO("ls snapshot need gc", K(gc_ls_id_arr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (gc_tenant_snapshot || gc_ls_id_arr.count() > 0) {
      if (!tenant_has_been_dropped) {
        ObTenantSnapshotMetaTable::acquire_tenant_snapshot_trace_id(tenant_snapshot_id_,
                                                                    ObTenantSnapOperation::DELETE,
                                                                    trace_id);
      } else {
        trace_id.init(GCTX.self_addr());
      }
      gc_dag_start_();
    } else {
      ret = OB_NO_NEED_UPDATE;
    }
  } else {
    LOG_WARN("fail to try_start_gc_tenant_snapshot_dag", KR(ret), KPC(this));
  }
  return ret;
}

int ObTenantSnapshot::execute_gc_tenant_snapshot_dag(const bool gc_tenant_snapshot, const ObArray<ObLSID> &gc_ls_id_arr)
{
  int ret = OB_SUCCESS;
  {
    lib::ObMutexGuard snapshot_guard(mutex_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObTenantSnapshot not init", KR(ret));
    } else if (!has_unfinished_gc_dag_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObTenantSnapshot does not have unfinished gc dag", KR(ret), KPC(this));
    } else if (0 != clone_ref_) {
      ret = OB_EAGAIN;
      LOG_INFO("This tenant snapshot is being used for cloning and cannot be gc currently",
          KR(ret), KPC(this));
    }
  }
  if (OB_SUCC(ret)) {
    if (gc_tenant_snapshot) {
      LOG_INFO("gc_tenant_snapshot_ with ls_snapshot", K(tenant_snapshot_id_));
      if (OB_FAIL(gc_tenant_snapshot_())) {
        LOG_WARN("fail to gc_tenant_snapshot_", KR(ret), KPC(this));
      }
    } else {
      LOG_INFO("gc_ls_snapshots_ only", K(tenant_snapshot_id_), K(gc_ls_id_arr));
      if (OB_FAIL(gc_ls_snapshots_(gc_ls_id_arr))) {
        LOG_WARN("fail to gc_ls_snapshots_", KR(ret), KPC(this));
      }
    }
  }
  return ret;
}

int ObTenantSnapshot::execute_create_tenant_snapshot_dag(const ObArray<ObLSID> &creating_ls_id_arr)
{
  int ret = OB_SUCCESS;

  {
    lib::ObMutexGuard snapshot_guard(mutex_);

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", KR(ret));
    } else if (!ATOMIC_LOAD(&is_running_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ObTenantSnapshot is not in running state", KR(ret), KPC(this));
    } else if (!has_unfinished_create_dag_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ObTenantSnapshot does not have unfinished dag", KR(ret), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    build_all_snapshots_(creating_ls_id_arr);
  }
  return ret;
}

int ObTenantSnapshot::finish_create_tenant_snapshot_dag()
{
  lib::ObMutexGuard snapshot_guard(mutex_);
  return create_dag_finish_();
}

int ObTenantSnapshot::create_dag_start_()
{
  int ret = OB_SUCCESS;
  if (has_unfinished_create_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTenantSnapshot has unfinished dag", KR(ret), KPC(this));
  } else {
    has_unfinished_create_dag_ = true;
  }
  return ret;
}

int ObTenantSnapshot::create_dag_finish_()
{
  int ret = OB_SUCCESS;
  if (!has_unfinished_create_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant snapshot does not have unfinished dag", KR(ret), KPC(this));
  } else {
    has_unfinished_create_dag_ = false;
  }
  return ret;
}

int ObTenantSnapshot::gc_dag_start_()
{
  int ret = OB_SUCCESS;
  if (has_unfinished_gc_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTenantSnapshot has unfinished gc dag", KR(ret), KPC(this));
  } else {
    has_unfinished_gc_dag_ = true;
  }
  return ret;
}

int ObTenantSnapshot::gc_dag_finish_()
{
  int ret = OB_SUCCESS;
  if (!has_unfinished_gc_dag_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant snapshot does not have unfinished gc dag", KR(ret), KPC(this));
  } else {
    has_unfinished_gc_dag_ = false;
  }
  return ret;
}

int ObTenantSnapshot::finish_gc_tenant_snapshot_dag()
{
  lib::ObMutexGuard snapshot_guard(mutex_);
  return gc_dag_finish_();
}

int ObTenantSnapshot::clear_meta_snapshot_()
{
  LOG_INFO("clear tenant meta snapshot", KPC(this));

  int ret = OB_SUCCESS;
  if (meta_existed_) {
    if (OB_FAIL(meta_handler_->delete_tenant_snapshot(tenant_snapshot_id_))) {
      LOG_ERROR("fail to delete_tenant_snapshot", KR(ret), KPC(this));
    } else {
      meta_existed_ = false;
    }
  }
  return ret;
}

void ObTenantSnapshot::build_all_snapshots_(const ObArray<ObLSID>& creating_ls_id_arr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_tenant_snapshot_meta_())) {
    LOG_WARN("fail to do build_tenant_snapshot_meta_", KR(ret), KPC(this));
  } else {
    build_all_ls_snapshots_(creating_ls_id_arr);
  }
}

int ObTenantSnapshot::build_tenant_snapshot_meta_()
{
  int ret = OB_SUCCESS;

  if (!ATOMIC_LOAD(&meta_existed_)) {
    if (OB_FAIL(meta_handler_->create_tenant_snapshot(tenant_snapshot_id_))) {
      LOG_WARN("fail to do ObTenantMetaSnapshotHandler::create_tenant_snapshot", KR(ret), KPC(this));
    } else {
      meta_existed_ = true;
      LOG_INFO("ObTenantMetaSnapshotHandler::create_tenant_snapshot succ", KPC(this));
    }
  } else {
    LOG_INFO("the tenant snapshot meta already existed", KPC(this));
  }
  return ret;
}

void ObTenantSnapshot::build_all_ls_snapshots_(const ObArray<ObLSID>& creating_ls_id_arr)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < creating_ls_id_arr.count(); i++) {
    const ObLSID creating_ls_id = creating_ls_id_arr.at(i);

    if (OB_FAIL(build_one_ls_snapshot_(creating_ls_id))) {
      LOG_WARN("failed to build_one_ls_snapshot_", KR(ret), KPC(this), K(creating_ls_id));
    }
  }

  LOG_INFO("build_all_ls_snapshots_ complete");
}

int ObTenantSnapshot::build_one_ls_snapshot_(const ObLSID& creating_ls_id)
{
  int ret = OB_SUCCESS;
  ObLSSnapshot* ls_snapshot = nullptr;

  if (OB_FAIL(ls_snapshot_mgr_->acquire_ls_snapshot(tenant_snapshot_id_,
                                                    creating_ls_id,
                                                    ls_snapshot))) {
    LOG_WARN("fail to acquire_ls_snapshot",
        KR(ret), K(tenant_snapshot_id_), K(creating_ls_id));
  } else if (OB_ISNULL(ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_snapshot is unexpected nullptr", KR(ret));
  } else if (ls_snapshot->is_build_finished()) {
    LOG_INFO("ls_snapshot is already build finished", KR(ret), KPC(this), KPC(ls_snapshot));
  } else if (OB_FAIL(build_one_ls_snapshot_meta_(ls_snapshot))) {
    LOG_WARN("fail to build_one_ls_snapshot_meta_", KR(ret), KPC(this), KPC(ls_snapshot));
  }

  report_one_ls_snapshot_build_rlt_(ls_snapshot, ret);

  if (ls_snapshot != nullptr) {
    ls_snapshot_mgr_->revert_ls_snapshot(ls_snapshot);
  }
  return ret;
}

int ObTenantSnapshot::build_one_ls_snapshot_meta_(ObLSSnapshot* ls_snapshot)
{
  int ret = OB_SUCCESS;

  ObLS* ls = nullptr;
  ObLSHandle ls_handle;
  ObLSID creating_ls_id;

  if (OB_ISNULL(ls_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_snapshot is nullptr", KR(ret), KPC(this));
  } else if (FALSE_IT(creating_ls_id = ls_snapshot->get_ls_id())) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(creating_ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), KPC(this), KPC(ls_snapshot));
  } else if (OB_ISNULL((ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), KPC(this), KPC(ls_snapshot));
  } else if (OB_FAIL(ls->get_tablet_gc_handler()->disable_gc())) {
    LOG_WARN("fail to disable gc", KR(ret), KPC(ls_snapshot), KPC(ls));
    if (OB_TABLET_GC_LOCK_CONFLICT == ret) {
      ret = OB_EAGAIN;
    }
  } else {
    if (OB_FAIL(ls_snapshot->build_ls_snapshot(ls))) {
      LOG_WARN("fail to build_ls_snapshot", KR(ret), KPC(ls_snapshot), KPC(ls));
    }
    ls->get_tablet_gc_handler()->enable_gc();
  }
  return ret;
}

void ObTenantSnapshot::report_one_ls_snapshot_build_rlt_(ObLSSnapshot* ls_snapshot,
                                                         const int ls_ret)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_snapshot is unexpected nullptr", KR(ret));
  } else {
    const ObLSID creating_ls_id = ls_snapshot->get_ls_id();

    if (OB_SUCCESS != ls_ret && OB_EAGAIN != ls_ret) {
      ls_snapshot->try_set_failed();
    }

    if (OB_SUCCESS == ls_ret && ls_snapshot->is_valid_for_reporting_succ()) {
      if (OB_FAIL(report_create_ls_snapshot_succ_rlt_(ls_snapshot))) {
        LOG_WARN("fail to report_create_ls_snapshot_succ_rlt_",
            KR(ret), K(creating_ls_id), KPC(ls_snapshot));
      }
    } else {
      if (OB_EAGAIN != ls_ret) {
        if (OB_FAIL(report_create_ls_snapshot_fail_rlt_(creating_ls_id))) {
          LOG_WARN("fail to report_create_ls_snapshot_succ_rlt_", KR(ret), K(creating_ls_id));
        }
      } else {
        LOG_INFO("ls_ret is OB_EAGAIN, which can be retried", KR(ret), K(creating_ls_id));
      }
    }

    if (OB_SUCC(ret)) {
      ls_snapshot->try_free_build_ctx();
    }
  }
}

int ObTenantSnapshot::report_create_ls_snapshot_succ_rlt_(ObLSSnapshot* ls_snapshot)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls_snapshot is unexpected nullptr", KR(ret));
  } else {
    ObLSSnapshotReportInfo info(ls_snapshot->get_ls_id());
    if (OB_FAIL(ls_snapshot->get_report_info(info))) {
      LOG_WARN("fail to get_report_info", KR(ret), KPC(ls_snapshot));
    } else if (OB_FAIL(ObTenantSnapshotMetaTable::
          report_create_ls_snapshot_rlt(tenant_snapshot_id_, info))) {
      LOG_WARN("fail to report_create_ls_snapshot_rlt", KR(ret), KPC(ls_snapshot), K(info));
    }
  }

  return ret;
}

int ObTenantSnapshot::report_create_ls_snapshot_fail_rlt_(const ObLSID& ls_id)
{
  int ret = OB_SUCCESS;

  ObLSSnapshotReportInfo info(ls_id);
  info.to_failed();
  if (OB_FAIL(ObTenantSnapshotMetaTable::
        report_create_ls_snapshot_rlt(tenant_snapshot_id_, info))) {
    LOG_WARN("fail to report_create_ls_snapshot_rlt", KR(ret), K(info));
  }
  return ret;
}

void ObTenantSnapshot::stop()
{
  lib::ObMutexGuard snapshot_guard(mutex_);
  is_running_ = false;
}

bool ObTenantSnapshot::is_stopped()
{
  return !ATOMIC_LOAD(&is_running_);
}

template<class Fn>
bool ObTenantSnapshot::ForEachFilterFunctor<Fn>::operator()(
    const ObLSSnapshotMapKey &snapshot_key, ObLSSnapshot* ls_snapshot)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (!snapshot_key.is_valid() || OB_ISNULL(ls_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_key), KP(ls_snapshot));
  } else if (snapshot_key.tenant_snapshot_id_ != tenant_snapshot_id_) {
    bool_ret = true;
  } else {
    bool_ret = fn_(snapshot_key.ls_id_, ls_snapshot);
  }
  return bool_ret;
}

template <typename Fn> int ObTenantSnapshot::for_each_(Fn &fn)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_snapshot_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls_snapshot_mgr_ is nullptr", KR(ret), K(tenant_snapshot_id_));
  } else {
    ForEachFilterFunctor<Fn> filter_fn(tenant_snapshot_id_, fn);
    ret = ls_snapshot_mgr_->for_each(filter_fn);
  }
  return ret;
}

template<class Fn>
bool ObTenantSnapshot::RemoveIfFilterFunctor<Fn>::operator()(
    const ObLSSnapshotMapKey &snapshot_key, ObLSSnapshot* ls_snapshot)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (!snapshot_key.is_valid() || OB_ISNULL(ls_snapshot)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_key), KP(ls_snapshot));
  } else if (snapshot_key.tenant_snapshot_id_ != tenant_snapshot_id_) {
    bool_ret = false;
  } else {
    bool_ret = fn_(snapshot_key.ls_id_, ls_snapshot);
  }
  return bool_ret;
}

template <typename Fn> int ObTenantSnapshot::remove_if_(Fn &fn)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls_snapshot_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls_snapshot_mgr_ is nullptr", KR(ret), K(tenant_snapshot_id_));
  } else {
    RemoveIfFilterFunctor<Fn> filter_fn(tenant_snapshot_id_, fn);
    ret = ls_snapshot_mgr_->remove_if(filter_fn);
  }
  return ret;
}

int ObTenantSnapshot::gc_tenant_snapshot_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_meta_snapshot_())) {
    LOG_WARN("fail to clear_meta_snapshot_", KR(ret), KPC(this));
  } else {
    notify_ls_snapshots_tenant_gc_();
  }

  LOG_INFO("gc tenant snapshot finished", KR(ret), KPC(this));
  return ret;
}

int ObTenantSnapshot::gc_ls_snapshots_(const ObArray<ObLSID> &gc_ls_id_arr)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < gc_ls_id_arr.count(); ++i) {
    ObLSSnapshot* ls_snapshot = nullptr;
    const ObLSID gc_ls_id = gc_ls_id_arr[i];
    if (OB_FAIL(ls_snapshot_mgr_->get_ls_snapshot(tenant_snapshot_id_,
                                                  gc_ls_id,
                                                  ls_snapshot))) {
      LOG_WARN("fail to get_ls_snapshot", KR(ret), KPC(this), K(gc_ls_id));
    } else if (OB_ISNULL(ls_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_snapshot is unexpected nullptr", KR(ret), KPC(this), K(gc_ls_id));
    } else {
      if (OB_FAIL(ls_snapshot->gc_ls_snapshot())) {
        LOG_WARN("fail to gc_ls_snapshot", KR(ret), KPC(ls_snapshot));
      } else if (OB_FAIL(ls_snapshot_mgr_->del_ls_snapshot(tenant_snapshot_id_, gc_ls_id))) {
        LOG_WARN("fail to gc_ls_snapshot", KR(ret), KPC(ls_snapshot));
      }
      ls_snapshot_mgr_->revert_ls_snapshot(ls_snapshot);
    }
  }
  return ret;
}

class ObLSSnapshotNotifyTenantGcFunctor
{
public:
  bool operator()(const ObLSID &ls_id, ObLSSnapshot* ls_snapshot)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = false;

    if (!ls_id.is_valid() || OB_ISNULL(ls_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(ls_id), KP(ls_snapshot));
    } else {
      ls_snapshot->notify_tenant_gc();
      bool_ret = true;
    }
    return bool_ret;
  }
};

void ObTenantSnapshot::notify_ls_snapshots_tenant_gc_()
{
  int ret = OB_SUCCESS;
  ObLSSnapshotNotifyTenantGcFunctor fn;
  if (OB_FAIL(remove_if_(fn))) {
    LOG_ERROR("fail to remove_if for notify_ls_snapshots_tenant_gc_", KR(ret), KPC(this));
  }
}

class ObCheckLSSnapshotNeedGCFunctor
{
public:
  ObCheckLSSnapshotNeedGCFunctor(const ObArray<ObTenantSnapLSReplicaSimpleItem>& item_arr,
                                 ObArray<ObLSID>& gc_ls_id_arr)
      : item_arr_(item_arr), gc_ls_id_arr_(gc_ls_id_arr) {}
  ~ObCheckLSSnapshotNeedGCFunctor() {}

  bool operator()(const ObLSID &ls_id, ObLSSnapshot* ls_snapshot)
  {
    int ret = OB_SUCCESS;

    if (!ls_id.is_valid() || OB_ISNULL(ls_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(ls_id), KP(ls_snapshot));
    } else {
      bool found = false;
      int64_t i = 0;
      for (i = 0; i < item_arr_.count(); ++i) {
        if (item_arr_.at(i).get_ls_id() == ls_id) {
          found = true;
          break;
        }
      }

      bool need_gc = false;
      if (!found) {
        need_gc = true;
      } else if (ObLSSnapStatus::FAILED == item_arr_.at(i).get_status()) {
        need_gc = true;
      }

      if (need_gc) {
        gc_ls_id_arr_.push_back(ls_id);
      }
    }
    return true;
  }

private:
  const ObArray<ObTenantSnapLSReplicaSimpleItem>& item_arr_;
  ObArray<ObLSID>& gc_ls_id_arr_;
};

int ObTenantSnapshot::get_need_gc_ls_snapshot_arr_(
    const ObArray<ObTenantSnapLSReplicaSimpleItem>& item_arr,
    ObArray<ObLSID>& gc_ls_id_arr)
{
  int ret = OB_SUCCESS;
  gc_ls_id_arr.reset();

  ObCheckLSSnapshotNeedGCFunctor fn(item_arr, gc_ls_id_arr);
  if (OB_FAIL(for_each_(fn))) {
    LOG_ERROR("fail to for_each_ ls_snapshots", KR(ret), KPC(this));
  }
  return ret;
}

int ObTenantSnapshot::destroy()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    lib::ObMutexGuard snapshot_guard(mutex_);
    if (ATOMIC_LOAD(&is_running_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("when the tenant snapshot is to be destroyed, it should not be in the running state",
          KR(ret), KPC(this));
    } else if (OB_FAIL(destroy_all_ls_snapshots_())) {
      LOG_ERROR("fail to destroy_all_ls_snapshots_", KR(ret), KPC(this));
    } else {
      LOG_INFO("tenant snapshot destroy succ");
    }
    is_inited_ = false;
  }

  return ret;
}

class ObLSSnapshotDestroyFunctor
{
public:
  bool operator()(const ObLSID &ls_id, ObLSSnapshot* ls_snapshot)
  {
    int ret = OB_SUCCESS;
    bool bool_ret = false;

    if (!ls_id.is_valid() || OB_ISNULL(ls_snapshot)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(ls_id), KP(ls_snapshot));
    } else {
      ls_snapshot->destroy();
      bool_ret = true;
    }
    return bool_ret;
  }
};

int ObTenantSnapshot::destroy_all_ls_snapshots_()
{
  int ret = OB_SUCCESS;

  ObLSSnapshotDestroyFunctor fn;
  if (OB_FAIL(remove_if_(fn))) {
    LOG_ERROR("fail to remove_if for destroy ls snapshots", KR(ret), KPC(this));
  }
  return ret;
}

int ObTenantSnapshot::inc_clone_ref()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard snapshot_guard(mutex_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshot is not init", KR(ret));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTenantSnapshot is not running", KR(ret), KPC(this));
  } else if (has_unfinished_dag_()) {
    ret = OB_EAGAIN;
    LOG_INFO("ObTenantSnapshot has unfinished dag", KR(ret), KPC(this));
  } else if (!meta_existed_) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("ObTenantSnapshot meta not existed", KR(ret), KPC(this));
  } else {
    ++clone_ref_;
    LOG_INFO("ObTenantSnapshot inc clone ref succ", KPC(this));
  }

  return ret;
}

int ObTenantSnapshot::dec_clone_ref()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard snapshot_guard(mutex_);

  if (clone_ref_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantSnapshot clone_ref_ is unexpected value", KR(ret), KPC(this));
  } else {
    --clone_ref_;
    LOG_INFO("ObTenantSnapshot dec clone ref succ", KPC(this));
  }

  return ret;
}

int ObTenantSnapshot::get_tenant_snapshot_vt_info(ObTenantSnapshotVTInfo &info)
{
  int ret = OB_SUCCESS;

  info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshot is not init", KR(ret), KPC(this));
  } else {
    lib::ObMutexGuard snapshot_guard(mutex_);
    info.set_tsnap_is_running(is_running_);
    info.set_tsnap_has_unfinished_create_dag(has_unfinished_create_dag_);
    info.set_tsnap_has_unfinished_gc_dag(has_unfinished_gc_dag_);
    info.set_tsnap_clone_ref(clone_ref_);
    info.set_tsnap_meta_existed(meta_existed_);
  }
  return ret;
}

int ObTenantSnapshot::get_ls_snapshot_tablet_meta_entry(const ObLSID &ls_id,
                                                        blocksstable::MacroBlockId &tablet_meta_entry)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard snapshot_guard(mutex_);

  ObLSSnapshot* ls_snapshot = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshot is not init", KR(ret));
  } else if (!ATOMIC_LOAD(&is_running_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ObTenantSnapshot is not running", KR(ret), KPC(this));
  } else if (has_unfinished_dag_()) {
    ret = OB_EAGAIN;
    LOG_WARN("ObTenantSnapshot has unfinished dag", KR(ret), KPC(this));
  } else if (!ATOMIC_LOAD(&meta_existed_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("ObTenantSnapshot meta not existed", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_snapshot_mgr_->get_ls_snapshot(tenant_snapshot_id_, ls_id, ls_snapshot))){
    LOG_WARN("fail to get_ls_snapshot", KR(ret), KPC(this));
  } else {
    if (OB_FAIL(ls_snapshot->get_tablet_meta_entry(tablet_meta_entry))) {
      LOG_WARN("fail to get_tablet_meta_entry", KR(ret), KPC(this));
    } else {
      LOG_INFO("get_tablet_meta_entry succ", KR(ret), KPC(this), K(tablet_meta_entry));
    }
    ls_snapshot_mgr_->revert_ls_snapshot(ls_snapshot);
  }
  return ret;
}

}
}
