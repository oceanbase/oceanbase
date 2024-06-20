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

#include "lib/ob_errno.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "share/ob_ls_id.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/access/ob_dml_param.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace logservice::coordinator;
namespace storage
{

void ObStoreCtxGuard::reset()
{
  int ret = OB_SUCCESS;
  static const int64_t WARN_TIME_US = 5 * 1000 * 1000;
  if (IS_INIT) {
    if (OB_NOT_NULL(handle_.get_ls())) {
      if (ctx_.is_valid() && OB_FAIL(handle_.get_ls()->revert_store_ctx(ctx_))) {
        LOG_WARN("revert transaction context fail", K(ret), K_(ls_id));
      }
      handle_.reset();
    }
    const int64_t guard_used_us = ObClockGenerator::getClock() - init_ts_;
    if (guard_used_us >= WARN_TIME_US) {
      LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "guard used too much time", K(guard_used_us), K_(ls_id), K(lbt()));
    }
    ctx_.reset();
    ls_id_.reset();
    is_inited_ = false;
  }
}

int ObStoreCtxGuard::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument(s)", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
    ctx_.reset();
    ctx_.ls_id_ = ls_id;
    is_inited_ = true;
    init_ts_ = ObClockGenerator::getClock();
  }
  return ret;
}

ObAccessService::ObAccessService()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_svr_(nullptr)
{}

ObAccessService::~ObAccessService()
{
  destroy();
}

int ObAccessService::mtl_init(ObAccessService* &access_service)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();

  return access_service->init(tenant_id, MTL(ObLSService*));
}

int ObAccessService::init(
    const uint64_t tenant_id,
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("access service has been inited", K(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
             || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(ls_service));
  } else {
    ls_svr_ = ls_service;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObAccessService::destroy()
{
  if (IS_INIT) {
    ls_svr_ = nullptr;
    tenant_id_ = OB_INVALID_ID;
    is_inited_ = false;
  }
}

void ObAccessService::stop()
{
  destroy();
}

int ObAccessService::check_tenant_out_of_memstore_limit_(bool &is_out_of_mem)
{
  int ret = OB_SUCCESS;
  is_out_of_mem = false;
  ObTenantFreezer *freezer = nullptr;
  freezer = MTL(ObTenantFreezer *);
  if (OB_FAIL(freezer->check_memstore_full(is_out_of_mem))) {
    LOG_WARN("check tenant out of memstore limit", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObAccessService::check_data_disk_full_(
    const share::ObLSID &ls_id,
    bool &is_full)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObFailureDetector* detector = MTL(ObFailureDetector*);
  if (!is_user_tenant(tenant_id) || ls_id.is_sys_ls()) {
    is_full = false;
  } else if (OB_ISNULL(detector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl module detector is null", K(ret), KP(detector));
  } else {
    is_full = detector->is_data_disk_full();
  }
  return ret;
}

int ObAccessService::pre_check_lock(
    const share::ObLSID &ls_id,
    transaction::ObTxDesc &tx_desc,
    const transaction::tablelock::ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  int64_t user_specified_snapshot =
          transaction::ObTransVersion::INVALID_TRANS_VERSION;
  transaction::ObTxReadSnapshot snapshot;
  snapshot.init_none_read();
  concurrent_control::ObWriteFlag write_flag;
  write_flag.set_is_table_lock();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!tx_desc.is_valid())
             || OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tx_desc), K(param));
  } else if (OB_FAIL(get_write_store_ctx_guard_(ls_id,
                                                param.expired_time_, /*timeout*/
                                                tx_desc,
                                                snapshot,
                                                0,/*branch_id*/
                                                write_flag,
                                                ctx_guard))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else {
    ret = ls->check_lock_conflict(ctx_guard.get_store_ctx(), param);
  }
  return ret;
}

int ObAccessService::lock_obj(
    const share::ObLSID &ls_id,
    transaction::ObTxDesc &tx_desc,
    const transaction::tablelock::ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  transaction::ObTxReadSnapshot snapshot;
  snapshot.init_none_read();
    concurrent_control::ObWriteFlag write_flag;
  write_flag.set_is_table_lock();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!tx_desc.is_valid())
             || OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tx_desc), K(param));
  } else if (OB_FAIL(get_write_store_ctx_guard_(ls_id,
                                                param.expired_time_, /*timeout*/
                                                tx_desc,
                                                snapshot,
                                                0, /*branch_id*/
                                                write_flag,
                                                ctx_guard))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else {
    ret = ls->lock(ctx_guard.get_store_ctx(), param);
  }
  return ret;
}

int ObAccessService::unlock_obj(
    const share::ObLSID &ls_id,
    transaction::ObTxDesc &tx_desc,
    const transaction::tablelock::ObLockParam &param)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  int64_t user_specified_snapshot = transaction::ObTransVersion::INVALID_TRANS_VERSION;
  transaction::ObTxReadSnapshot snapshot;
  snapshot.init_none_read();
  concurrent_control::ObWriteFlag write_flag;
  write_flag.set_is_table_lock();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!tx_desc.is_valid())
             || OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tx_desc), K(param));
  } else if (OB_FAIL(get_write_store_ctx_guard_(ls_id,
                                                param.expired_time_, /*timeout*/
                                                tx_desc,
                                                snapshot,
                                                0,/*branch_id*/
                                                write_flag,
                                                ctx_guard))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else {
    ret = ls->unlock(ctx_guard.get_store_ctx(), param);
  }
  return ret;
}

int ObAccessService::table_scan(
    ObVTableScanParam &vparam,
    ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  const share::ObLSID &ls_id = vparam.ls_id_;
  const common::ObTabletID &data_tablet_id = vparam.tablet_id_;
  ObTableScanIterator *iter = nullptr;
  ObTabletHandle tablet_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  ObTableScanParam &param = static_cast<ObTableScanParam &>(vparam);
  ObStoreAccessType access_type = param.scan_flag_.is_read_latest() ?
    ObStoreAccessType::READ_LATEST : ObStoreAccessType::READ;
  SCN user_specified_snapshot_scn;
  if (ObAccessTypeCheck::is_read_access_type(access_type) && param.fb_snapshot_.is_valid()) {
    //todo lixinze:后续会判断是否有效
    user_specified_snapshot_scn = param.fb_snapshot_;
  }
  NG_TRACE(storage_table_scan_begin);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (!vparam.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(vparam), K(lbt()));
  } else if (OB_NOT_NULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("The result_ is already pointed to an valid object",
        K(ret), K(vparam), KPC(result), K(lbt()));
  } else if (OB_ISNULL(iter = mtl_sop_borrow(ObTableScanIterator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc table scan iterator fail", K(ret));
  } else if (FALSE_IT(result = iter)) {
    // upper layer responsible for releasing iter object
  } else if (OB_FAIL(check_read_allowed_(ls_id,
                                         data_tablet_id,
                                         access_type,
                                         param,
                                         tablet_handle,
                                         iter->get_ctx_guard(),
                                         user_specified_snapshot_scn))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(data_tablet_id));
    }
    // skip inner table, one key reason is to let tablet merge going
  } else if (OB_ISNULL(ls = iter->get_ctx_guard().get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_service->table_scan(tablet_handle, *iter, param))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("Fail to scan table, ", K(ret), K(ls_id), K(param));
    }
  } else {
    NG_TRACE(storage_table_scan_end);
  }
  return ret;
}

int ObAccessService::table_rescan(
    ObVTableScanParam &vparam,
    ObNewRowIterator *result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObTableScanParam &param = static_cast<ObTableScanParam &>(vparam);
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_ISNULL(result) || OB_UNLIKELY(!vparam.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(result), K(vparam), K(lbt()));
  } else if (OB_UNLIKELY(ObNewRowIterator::ObTableScanIterator != result->get_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only table scan iter can be rescan", K(ret), K(result->get_type()));
  } else if (!param.need_switch_param_) {
    if (OB_FAIL(static_cast<ObTableScanIterator*>(result)->rescan(param))) {
      LOG_WARN("rescan ObTableScanIterator failed", K(ret), K(result), K(vparam));
    }
  } else {
    ObTableScanIterator *iter =  static_cast<ObTableScanIterator*>(result);
    const share::ObLSID &ls_id = vparam.ls_id_;
    const common::ObTabletID &data_tablet_id = vparam.tablet_id_;
    ObLS *ls = nullptr;
    ObLSTabletService *tablet_service = nullptr;
    ObStoreAccessType access_type = param.scan_flag_.is_read_latest() ?
      ObStoreAccessType::READ_LATEST : ObStoreAccessType::READ;
    SCN user_specified_snapshot_scn;
    if (ObAccessTypeCheck::is_read_access_type(access_type) && param.fb_snapshot_.is_valid()) {
      //todo lixinze:后续会判断是否有效
      user_specified_snapshot_scn = param.fb_snapshot_;
    }
    NG_TRACE(storage_table_scan_begin);
    if (OB_FAIL(check_read_allowed_(ls_id,
                                    data_tablet_id,
                                    access_type,
                                    param, /*scan_param*/
                                    tablet_handle,
                                    iter->get_ctx_guard(),
                                    user_specified_snapshot_scn))) {
      if (OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("fail to check query allowed", K(ret), K(result), K(ls_id), K(data_tablet_id));
      }
    // skip inner table, one key reason is to let tablet merge going
    } else if (OB_ISNULL(ls = iter->get_ctx_guard().get_ls_handle().get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", K(ret), K(result), KP(ls));
    } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
    } else if (OB_FAIL(tablet_service->table_rescan(tablet_handle, param, result))) {
      if (OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("Fail to scan table, ", K(ret), K(result), K(ls_id), K(param));
      }
    } else {
      NG_TRACE(storage_table_scan_end);
    }
  }
  return ret;
}
int ObAccessService::get_write_store_ctx_guard(
    const share::ObLSID &ls_id,
    const int64_t timeout,
    transaction::ObTxDesc &tx_desc,
    const transaction::ObTxReadSnapshot &snapshot,
    const int16_t branch_id,
    ObStoreCtxGuard &ctx_guard,
    const transaction::ObTxSEQ &spec_seq_no)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  // the write_flag is for tablet and does not need to be set here, just use default value,
  // it will be set by dml param in check_write_allowed_ when doing dml operations
  concurrent_control::ObWriteFlag default_write_flag;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tx_desc.is_valid() || !snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tx_desc), K(snapshot));
  } else if (OB_FAIL(get_write_store_ctx_guard_(
      ls_id, timeout, tx_desc, snapshot, branch_id, default_write_flag, ctx_guard, spec_seq_no))) {
    LOG_WARN("fail to get write store ctx gurad", K(ret), K(ls_id), K(tx_desc));
  }
  return ret;
}

int ObAccessService::get_write_store_ctx_guard_(
    const share::ObLSID &ls_id,
    const int64_t timeout,
    transaction::ObTxDesc &tx_desc,
    const transaction::ObTxReadSnapshot &snapshot,
    const int16_t branch_id,
    const concurrent_control::ObWriteFlag write_flag,
    ObStoreCtxGuard &ctx_guard,
    const transaction::ObTxSEQ &spec_seq_no)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  if (OB_FAIL(ctx_guard.init(ls_id))) {
    LOG_WARN("ctx_guard init fail", K(ret), K(ls_id), K(tx_desc));
  // DML statement will always use invalid snapshot
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ctx_guard.get_ls_handle(), ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get log stream failed.", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(ls_id), K_(tenant_id));
  } else {
    ObStoreCtx &ctx = ctx_guard.get_store_ctx();
    ctx.ls_ = ls;
    ctx.timeout_ = timeout;
    ctx.branch_ = branch_id;
    if (OB_FAIL(ls->get_write_store_ctx(tx_desc, snapshot, write_flag, ctx, spec_seq_no))) {
      LOG_WARN("can not get write store ctx", K(ret), K(ls_id), K(snapshot), K(tx_desc));
    }
  }
  if (OB_FAIL(ret)) {
    ctx_guard.reset();
  }
  return ret;
}

int ObAccessService::get_source_ls_tx_table_guard_(
  const ObTabletHandle &tablet_handle,
  ObStoreCtxGuard &ctx_guard)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  const ObTablet *tablet = nullptr;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KPC(tablet), K(tablet_handle));
  } else if (OB_LIKELY(!tablet->get_tablet_meta().has_transfer_table())) {
    // do nothing
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet), K(user_data));
  } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_ || !user_data.transfer_ls_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is unexpected", K(ret), K(user_data));
  } else if (ctx_guard.get_store_ctx().mvcc_acc_ctx_.get_tx_table_guards().is_src_valid()) {
    // The main tablet and local index tablets use the same mvcc_acc_ctx, if the src_tx_table_guard
    // has been set, you do not need to set it again and must skip start_request_for_transfer,
    // because it only call end_request_for_transfer once when revert store ctx.
    ObTxTableGuards &tx_table_guards = ctx_guard.get_store_ctx().mvcc_acc_ctx_.get_tx_table_guards();
    if (OB_UNLIKELY(tx_table_guards.src_ls_handle_.get_ls()->get_ls_id() != user_data.transfer_ls_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("main tablet and local index tablet must have same src ls", K(ret), K(tx_table_guards), K(user_data));
    }
  } else {
    ObLS *src_ls = nullptr;
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObTxTableGuard src_tx_table_guard;
    if (OB_FAIL(ls_service->get_ls(user_data.transfer_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(user_data));
    } else if (OB_ISNULL(src_ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(src_ls), K(user_data));
    } else if (OB_FAIL(src_ls->get_tx_table_guard(src_tx_table_guard))) {
      LOG_WARN("failed to get tablet", K(ret));
    } else if (!user_data.transfer_scn_.is_valid() || !src_tx_table_guard.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer_scn or source ls tx_table_guard is invalid", K(ret), K(src_tx_table_guard), K(user_data));
    } else if (OB_FAIL(src_ls->get_tx_svr()->start_request_for_transfer())) {
      LOG_WARN("start request for transfer failed", KR(ret), K(user_data));
    } else {
      ObStoreCtx &ctx = ctx_guard.get_store_ctx();
      ctx.mvcc_acc_ctx_.set_src_tx_table_guard(src_tx_table_guard, ls_handle);
      LOG_DEBUG("succ get src tx table guard", K(ret), K(src_ls->get_ls_id()), K(src_tx_table_guard), K(user_data));
    }
  }

  return ret;
}

int ObAccessService::construct_store_ctx_other_variables_(
    ObLS &ls,
    const common::ObTabletID &tablet_id,
    const int64_t timeout,
    const share::SCN &snapshot,
    ObTabletHandle &tablet_handle,
    ObStoreCtxGuard &ctx_guard)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls.get_ls_id();
  ObLSTabletService *tablet_service = ls.get_tablet_svr();
  if (OB_ISNULL(tablet_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_service->check_allow_to_read())) {
    if (OB_REPLICA_NOT_READABLE == ret) {
      LOG_WARN("replica unreadable", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_ERROR("failed to check allow to read", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(tablet_service->get_tablet_with_timeout(
      tablet_id, tablet_handle, timeout, ObMDSGetTabletMode::READ_READABLE_COMMITED, snapshot))) {
    LOG_WARN("failed to check and get tablet", K(ret), K(ls_id), K(tablet_id), K(timeout), K(snapshot));
  } else if (OB_FAIL(get_source_ls_tx_table_guard_(tablet_handle, ctx_guard))) {
    LOG_WARN("failed to get src ls tx table guard", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}
/*
 * check_read_allowed - check replica can serve transactional read
 *
 * if replica can serve read, store_ctx will be prepared
 */
int ObAccessService::check_read_allowed_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObStoreAccessType access_type,
    const ObTableScanParam &scan_param,
    ObTabletHandle &tablet_handle,
    ObStoreCtxGuard &ctx_guard,
    SCN user_specified_snapshot)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;

  LOG_TRACE("print check read allowed, scan param", K(ls_id), K(tablet_id), K(scan_param.fb_read_tx_uncommitted_));
  if (OB_FAIL(ctx_guard.init(ls_id))) {
    LOG_WARN("ctx_guard init fail", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ctx_guard.get_ls_handle(), ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get log stream failed.", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(ls_id), K_(tenant_id));
  } else {
    ObStoreCtx &ctx = ctx_guard.get_store_ctx();
    ctx.ls_ = ls;
    ctx.timeout_ = scan_param.timeout_;
    ctx.tablet_id_ = tablet_id;
    if (user_specified_snapshot.is_valid() && !scan_param.fb_read_tx_uncommitted_) {
      if (OB_FAIL(ls->get_read_store_ctx(user_specified_snapshot,
                                         scan_param.tx_lock_timeout_,
                                         ctx))) {
        LOG_WARN("get read store ctx fail", K(user_specified_snapshot), K(ls_id), K(ret));
      }
    } else {
      bool read_latest = access_type == ObStoreAccessType::READ_LATEST;
      if (user_specified_snapshot.is_valid()) {
        transaction::ObTxReadSnapshot spec_snapshot;
        if (OB_FAIL(spec_snapshot.assign(scan_param.snapshot_))) {
          LOG_WARN("copy snapshot fail", K(ret));
        } else if (FALSE_IT(spec_snapshot.specify_snapshot_scn(user_specified_snapshot))) {
        } else if (OB_FAIL(ls->get_read_store_ctx(spec_snapshot,
                                                  read_latest,
                                                  scan_param.tx_lock_timeout_,
                                                  ctx))) {
          LOG_WARN("get read store ctx fail", K(ret), K(read_latest), K(spec_snapshot), K(user_specified_snapshot), K(ls_id));
        }
      } else if (OB_FAIL(ls->get_read_store_ctx(scan_param.snapshot_,
                                                read_latest,
                                                scan_param.tx_lock_timeout_,
                                                ctx))) {
        LOG_WARN("get read store ctx fail", K(ret), K(read_latest), K(scan_param.snapshot_), K(ls_id));
      }
      if (OB_FAIL(ret)) {
      } else if (read_latest) {
        if (!scan_param.tx_id_.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("readlatest need scan_param.tx_id_ valid", K(ret));
        } else {
          ctx.mvcc_acc_ctx_.tx_id_ = scan_param.tx_id_;
        }
      }
    }

    // If this select is for foreign key check,
    // we should get tx_id and tx_desc for deadlock detection.
    if (OB_SUCC(ret)) {
      if (scan_param.is_for_foreign_check_) {
        if (scan_param.tx_id_.is_valid()) {
          ctx.mvcc_acc_ctx_.tx_id_ = scan_param.tx_id_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("foreign key check need scan_param.tx_id_ valid", K(ret), K(scan_param.tx_id_));
        }
        if (OB_NOT_NULL(scan_param.trans_desc_) && scan_param.trans_desc_->is_valid()) {
          ctx.mvcc_acc_ctx_.tx_desc_ = scan_param.trans_desc_;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("foreign key check need scan_param.trans_desc_ valid", K(ret), KPC(scan_param.trans_desc_));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_LS_NOT_EXIST == ret) {
        int tmp_ret = OB_SUCCESS;
        bool is_dropped = false;
        schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is nullptr", "tmp_ret", tmp_ret);
        } else if (OB_SUCCESS != (tmp_ret = schema_service->check_if_tenant_has_been_dropped(tenant_id_, is_dropped))) {
          LOG_WARN("check if tenant has been dropped fail", "tmp_ret", tmp_ret);
        } else {
          ret = is_dropped ? OB_TENANT_HAS_BEEN_DROPPED : ret;
        }
      }
    } else if (OB_FAIL(construct_store_ctx_other_variables_(*ls, tablet_id, scan_param.timeout_,
         ctx.mvcc_acc_ctx_.get_snapshot_version(), tablet_handle, ctx_guard))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param.fb_snapshot_.is_valid()) {
        ret = OB_TABLE_DEFINITION_CHANGED;
      } else {
        LOG_WARN("failed to check replica allow to read", K(ret), K(tablet_id), "timeout", scan_param.timeout_);
      }
    }
  }
  return ret;
}


/*
 * check_write_allowed - check replica can serve transactional write
 *
 * if can serve write, store_ctx will be prepared
 */
int ObAccessService::check_write_allowed_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObStoreAccessType access_type,
    const ObDMLBaseParam &dml_param,
    const int64_t lock_wait_timeout_ts,
    transaction::ObTxDesc &tx_desc,
    ObTabletHandle &tablet_handle,
    ObStoreCtxGuard &ctx_guard)
{
  int ret = OB_SUCCESS;
  bool is_out_of_mem = false;
  bool is_disk_full = false;
  ObLS *ls = nullptr;
  ObLockID lock_id;
  ObLockParam lock_param;
  const ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  const ObTableLockOpType lock_op_type = IN_TRANS_DML_LOCK;
  ObTableLockOwnerID lock_owner;
  lock_owner.set_default();
  const bool is_deadlock_avoid_enabled = false;
  bool is_try_lock = lock_wait_timeout_ts <= 0;
  const int64_t abs_timeout_ts = MIN(lock_wait_timeout_ts, tx_desc.get_expire_ts());
  bool enable_table_lock = true;
  ret = OB_E(EventTable::EN_ENABLE_TABLE_LOCK) OB_SUCCESS;
  if (OB_ERR_UNEXPECTED == ret) {
    enable_table_lock = false;
    ret = OB_SUCCESS;
  }
  if (!dml_param.is_direct_insert()
      && OB_FAIL(check_tenant_out_of_memstore_limit_(is_out_of_mem))) {
    LOG_WARN("fail to check tenant out of mem limit", K(ret), K_(tenant_id));
  } else if (is_out_of_mem && !tablet_id.is_inner_tablet()) {
    ret = OB_TENANT_OUT_OF_MEM;
    LOG_WARN("this tenant is already out of memstore limit", K(ret), K_(tenant_id));
  } else if (OB_FAIL(check_data_disk_full_(ls_id, is_disk_full))) {
    LOG_WARN("fail to check data disk full", K(ret));
  } else if (is_disk_full) {
    ret = OB_USER_OUTOF_DATA_DISK_SPACE;
    LOG_WARN("data disk full, you should not do io now", K(ret));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(ls_id), K_(tenant_id));
  } else {
    ObStoreCtx &store_ctx = ctx_guard.get_store_ctx();
    store_ctx.tablet_id_ = tablet_id;
    store_ctx.timeout_ = abs_timeout_ts;
    store_ctx.mvcc_acc_ctx_.set_write_flag(dml_param.write_flag_);
    store_ctx.mvcc_acc_ctx_.set_abs_lock_timeout_ts(abs_timeout_ts);
    store_ctx.tablet_stat_.reset();

    const int64_t lock_expired_ts = MIN(dml_param.timeout_, tx_desc.get_expire_ts());
    const ObTableSchemaParam &schema_param = dml_param.table_param_->get_data_table();
    const bool is_local_index_table = schema_param.is_index_table() && schema_param.is_index_local_storage();

    if (!enable_table_lock) {
      // do nothing
    } else if (dml_param.is_direct_insert() || is_local_index_table) {
      // skip table lock
    } else if (OB_FAIL(get_lock_id(tablet_id, lock_id))) {
      LOG_WARN("get lock id failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(lock_param.set(lock_id,
                                      lock_mode,
                                      lock_owner,
                                      lock_op_type,
                                      dml_param.schema_version_,
                                      is_deadlock_avoid_enabled,
                                      is_try_lock,
                                      // we can not use abs_timeout_ts here,
                                      // because we may meet select-for-update nowait,
                                      // and abs_timeout_ts is 0. We will judge
                                      // timeout before meet lock conflict in tablelock,
                                      // so it will lead to incorrect error
                                      lock_expired_ts))) {
      LOG_WARN("get lock param failed", K(ret), K(lock_id));
    // When locking the table, the tablet is not detected to be deleted.
    } else if (OB_FAIL(ls->lock(ctx_guard.get_store_ctx(), lock_param))) {
      LOG_WARN("lock tablet failed", K(ret), K(lock_param));
    } else {
      // do nothing
    }
  }
  // After locking the table, it can prevent the tablet from being deleted.
  // It is necessary to obtain the tablet handle after locking the table to avoid operating the deleted tablet.
  if (OB_SUCC(ret) && OB_FAIL(construct_store_ctx_other_variables_(*ls, tablet_id, dml_param.timeout_,
      share::SCN::max_scn(), tablet_handle, ctx_guard))) {
    LOG_WARN("failed to check replica allow to read", K(ret), K(tablet_id));
  }
  return ret;
}

int ObAccessService::delete_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), KP(row_iter));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->delete_rows(tablet_handle,
                                      dml_param.store_ctx_guard_->get_store_ctx(),
                                      dml_param,
                                      column_ids,
                                      row_iter,
                                      affected_rows);
  }
  return ret;
}

int ObAccessService::put_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), K(row_iter));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->put_rows(tablet_handle,
                                   dml_param.store_ctx_guard_->get_store_ctx(),
                                   dml_param,
                                   column_ids,
                                   row_iter,
                                   affected_rows);
  }
  return ret;
}

int ObAccessService::insert_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), K(row_iter));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->insert_rows(tablet_handle,
                                      dml_param.store_ctx_guard_->get_store_ctx(),
                                      dml_param,
                                      column_ids,
                                      row_iter,
                                      affected_rows);
  }
  return ret;
}

int ObAccessService::insert_row(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<uint64_t> &duplicated_column_ids,
    const common::ObNewRow &row,
    const ObInsertFlag flag,
    int64_t &affected_rows,
    common::ObNewRowIterator *&duplicated_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_UNLIKELY(duplicated_column_ids.count() <= 0)
      || OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), K(duplicated_column_ids), K(row));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->insert_row(tablet_handle,
                                     dml_param.store_ctx_guard_->get_store_ctx(),
                                     dml_param,
                                     column_ids,
                                     duplicated_column_ids,
                                     row,
                                     flag,
                                     affected_rows,
                                     duplicated_rows);
  }
  return ret;
}

int ObAccessService::revert_insert_iter(common::ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr != iter)) {
    ObQueryIteratorFactory::free_insert_dup_iter(iter);
  }
  return ret;
}

int ObAccessService::update_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray< uint64_t> &updated_column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_UNLIKELY(updated_column_ids.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), K(updated_column_ids));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->update_rows(tablet_handle,
                                      dml_param.store_ctx_guard_->get_store_ctx(),
                                      dml_param,
                                      column_ids,
                                      updated_column_ids,
                                      row_iter,
                                      affected_rows);
  }
  return ret;
}

int ObAccessService::lock_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const int64_t abs_lock_timeout,
    const ObLockFlag lock_flag,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  int64_t lock_wait_timeout_ts = get_lock_wait_timeout_(abs_lock_timeout, dml_param.timeout_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(abs_lock_timeout), K(lock_flag), KP(row_iter));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::ROW_LOCK,
                                          dml_param,
                                          lock_wait_timeout_ts,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->lock_rows(tablet_handle,
                                    dml_param.store_ctx_guard_->get_store_ctx(),
                                    dml_param,
                                    lock_flag,
                                    false,
                                    row_iter,
                                    affected_rows);
  }
  return ret;
}

int ObAccessService::lock_row(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const int64_t abs_lock_timeout,
    const common::ObNewRow &row,
    const ObLockFlag lock_flag)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_write);
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  // Attention!!! This handle is only used for ObLSTabletService, will be reset inside ObLSTabletService.
  ObTabletHandle tablet_handle;
  int64_t lock_wait_timeout_ts = get_lock_wait_timeout_(abs_lock_timeout, dml_param.timeout_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(row));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::ROW_LOCK,
                                          dml_param,
                                          lock_wait_timeout_ts,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = dml_param.store_ctx_guard_->get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), KP(ls));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null.", K(ret), K(ls_id));
  } else {
    ret = tablet_service->lock_row(tablet_handle,
                                    dml_param.store_ctx_guard_->get_store_ctx(),
                                    dml_param,
                                    row,
                                    lock_flag,
                                    false);
  }
  return ret;
}

int ObAccessService::estimate_row_count(
    const ObTableScanParam &param,
    const ObTableScanRange &scan_range,
    const int64_t timeout_us,
    common::ObIArray<ObEstRowCountRecord> &est_records,
    int64_t &logical_row_count,
    int64_t &physical_row_count) const
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_ERROR;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!param.is_estimate_valid() || !scan_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param), K(scan_range), K(ret));
  } else if (OB_FAIL(ls_svr_->get_ls(param.ls_id_, ls_handle, ObLSGetMod::DAS_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param.ls_id_));
  } else if (nullptr == (ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret));
  } else if (OB_FAIL(ls->get_tablet_svr()->estimate_row_count(
      param, scan_range, timeout_us, est_records,
      logical_row_count, physical_row_count))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to estimate row count", K(ret), K(param), K(scan_range), K(timeout_us));
    }
  }
  return ret;
}

int ObAccessService::estimate_block_count_and_row_count(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count,
    common::ObIArray<int64_t> &cg_macro_cnt_arr,
    common::ObIArray<int64_t> &cg_micro_cnt_arr) const
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ob access service is not running.", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_id), K(tablet_id), K(ret));
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ls_handle, ObLSGetMod::DAS_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (nullptr == (ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret));
  } else if (OB_FAIL(ls->get_tablet_svr()->estimate_block_count_and_row_count(
      tablet_id, timeout_us,
      macro_block_count, micro_block_count,
      sstable_row_count, memtable_row_count,
      cg_macro_cnt_arr, cg_micro_cnt_arr))) {
    LOG_WARN("failed to estimate block count and row count", K(ret), K(ls_id), K(tablet_id), K(timeout_us));
  }
  return ret;
}

int ObAccessService::get_multi_ranges_cost(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    const common::ObIArray<common::ObStoreRange> &ranges,
    int64_t &total_size)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_ERROR;
    LOG_WARN("ob access service is not running", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ls_handle, ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get log stream failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls hould not be null", K(ret), K(ls_id), K_(tenant_id));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_service->get_multi_ranges_cost(tablet_id, timeout_us, ranges, total_size))) {
    LOG_WARN("Fail to get multi ranges cost", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObAccessService::reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("access service is not initiated", K(ret));
  } else if (OB_ISNULL(iter)) {
    //do nothing
  } else if (iter->get_type() == ObNewRowIterator::ObTableScanIterator) {
    ObTableScanIterator *scan_iter = static_cast<ObTableScanIterator*>(iter);
    if (OB_LIKELY(!switch_param)) {
      scan_iter->reuse();
    } else {
      scan_iter->reset_for_switch();
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only local das scan task can be reuse", K(ret), K(iter->get_type()));
  }
  return ret;
}

int ObAccessService::revert_scan_iter(ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  NG_TRACE(S_revert_iter_begin);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("access service is not initiated", K(ret));
  } else if (OB_ISNULL(iter)) {
    //do nothing
  } else if (iter->get_type() == ObNewRowIterator::ObTableScanIterator) {
    ObTableScanIterator *table_scan_iter = nullptr;
    table_scan_iter = static_cast<ObTableScanIterator *>(iter);
    if (OB_FAIL(table_scan_iter->check_ls_offline_after_read())) {
      LOG_WARN("discover ls offline after table scan", K(ret), KPC(table_scan_iter));
    }
    mtl_sop_return(ObTableScanIterator, table_scan_iter);
  } else {
    iter->~ObNewRowIterator();
  }
  iter = nullptr;
  NG_TRACE(S_revert_iter_end);
  return ret;
}

int ObAccessService::split_multi_ranges(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    const ObIArray<ObStoreRange> &ranges,
    const int64_t expected_task_count,
    common::ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_ERROR;
    LOG_WARN("ob access service is not running", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_svr_->get_ls(ls_id, ls_handle, ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get log stream failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls hould not be null", K(ret), K(ls_id), K_(tenant_id));
  } else if (OB_ISNULL(tablet_service = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet service should not be null", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_service->split_multi_ranges(
      tablet_id, timeout_us, ranges,
      expected_task_count, allocator, multi_range_split_array))) {
    LOG_WARN("Fail to split multi ranges", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

}
}
