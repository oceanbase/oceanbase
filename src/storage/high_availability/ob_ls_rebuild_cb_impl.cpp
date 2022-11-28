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
#include "ob_ls_rebuild_cb_impl.h"
#include "ob_storage_ha_service.h"
#include "share/ls/ob_ls_table_operator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObLSRebuildCbImpl::ObLSRebuildCbImpl()
  : is_inited_(false),
    ls_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

ObLSRebuildCbImpl::~ObLSRebuildCbImpl()
{
}

int ObLSRebuildCbImpl::init(
    ObLS *ls,
    common::ObInOutBandwidthThrottle *bandwidth_throttle,
    obrpc::ObStorageRpcProxy *svr_rpc_proxy,
    storage::ObStorageRpc *storage_rpc)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls rebuild cb impl init twice", K(ret));
  } else if (OB_ISNULL(ls) || OB_ISNULL(bandwidth_throttle) || OB_ISNULL(svr_rpc_proxy) || OB_ISNULL(storage_rpc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls rebuild cb impl get invalid argument", K(ret), KP(ls),
        KP(bandwidth_throttle), KP(svr_rpc_proxy), KP(storage_rpc));
  } else {
    ls_ = ls;
    bandwidth_throttle_ = bandwidth_throttle;
    svr_rpc_proxy_ = svr_rpc_proxy;
    storage_rpc_ = storage_rpc;
    is_inited_ = true;
  }
  return ret;
}

int ObLSRebuildCbImpl::on_rebuild(
    const int64_t id, const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id(id);
  bool is_ls_in_rebuild = false;
  bool need_rebuild = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (id <= 0 || !lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on rebuild get invalid argument", K(ret), K(id), K(lsn));
  } else if (ls_->get_ls_id() != ls_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id is not same with local ls", K(ret), K(ls_id), KPC(ls_));
  } else if (OB_FAIL(check_ls_in_rebuild_status_(is_ls_in_rebuild))) {
    LOG_WARN("failed to check ls in rebuild status", K(ret), K(ls_id), KPC(ls_));
  } else if (is_ls_in_rebuild) {
    wakeup_ha_service_();
  } else if (OB_FAIL(check_need_rebuild_(lsn, need_rebuild))) {
    LOG_WARN("failed to check need rebuild", K(ret), K(ls_id));
  } else if (!need_rebuild) {
    ret = OB_NO_NEED_REBUILD;
    LOG_WARN("ls no need rebuild", K(ret), K(lsn), KPC(ls_));
  } else if (OB_FAIL(execute_rebuild_())) {
    LOG_WARN("failed to execute rebuild", K(ret), K(lsn), KPC(ls_));
  } else {
    SERVER_EVENT_ADD("storage_ha", "on_rebuild",
                     "ls_id", ls_id.id(),
                     "lsn", lsn,
                     "rebuild_seq", ls_->get_rebuild_seq());
  }
  return ret;
}

int ObLSRebuildCbImpl::check_ls_in_rebuild_status_(
    bool &is_ls_in_rebuild)
{
  int ret = OB_SUCCESS;
  is_ls_in_rebuild = false;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (OB_FAIL(ls_->get_migration_status(migration_status))) {
    LOG_WARN("failed to get ls migration status", K(ret), KPC(ls_));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
    is_ls_in_rebuild = true;
    LOG_INFO("ls is already in rebuild status", K(ret), K(migration_status), KPC(ls_));
  }
  return ret;
}

int ObLSRebuildCbImpl::check_need_rebuild_(
    const palf::LSN &lsn,
    bool &need_rebuild)
{
  int ret = OB_SUCCESS;
  ObLSInfo ls_info;
  int64_t cluster_id = GCONF.cluster_id;
  uint64_t tenant_id = MTL_ID();
  ObAddr leader_addr;
  need_rebuild = false;
  share::ObLocationService *location_service = nullptr;
  ObStorageHASrcInfo src_info;
  obrpc::ObFetchLSMemberListInfo member_info;
  const bool force_renew = true;
  src_info.cluster_id_ = cluster_id;
  ObRole role;
  int64_t proposal_id = 0;
  logservice::ObLogService *log_service = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log service should not be NULL", K(ret), KP(log_service));
  } else if (OB_FAIL(log_service->get_palf_role(ls_->get_ls_id(), role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), KPC(ls_));
  } else if (is_strong_leader(role)) {
    need_rebuild = false;
    LOG_INFO("replica is leader, can not rebuild", KPC(ls_));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service should not be NULL", K(ret), KP(location_service));
  } else if (OB_FAIL(location_service->get_leader(src_info.cluster_id_, tenant_id, ls_->get_ls_id(), force_renew, src_info.src_addr_))) {
    LOG_WARN("fail to get ls leader server", K(ret), K(tenant_id), KPC(ls_));
    //for rebuild without leader exist
    ret = OB_SUCCESS;
  } else if (OB_FAIL(storage_rpc_->post_ls_member_list_request(tenant_id, src_info, ls_->get_ls_id(), member_info))) {
    LOG_WARN("failed to get ls member info", K(ret), KPC(ls_));
  } else if (!member_info.member_list_.contains(GCONF.self_addr_)) {
    ret = OB_WORKING_PARTITION_NOT_EXIST;
    LOG_WARN("can not rebuild, it is not normal ls", K(ret), KPC(ls_));
  }

  if (OB_FAIL(ret)) {
  } else {
    //TODO(muwei.ym) send rpc to check member list lsn
    need_rebuild = true;
  }
  return ret;
}

int ObLSRebuildCbImpl::execute_rebuild_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (OB_FAIL(ls_->set_ls_rebuild())) {
    LOG_WARN("failed to set ls rebuild", K(ret));
  } else {
    LOG_INFO("succeed execute rebuild", KPC(ls_));
    wakeup_ha_service_();
  }
  return ret;
}

void ObLSRebuildCbImpl::wakeup_ha_service_()
{
  int ret = OB_SUCCESS;
  ObStorageHAService *ha_service = nullptr;

  if (OB_ISNULL(ha_service = (MTL(ObStorageHAService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ha_service));
  } else {
    ha_service->wakeup();
  }
}


}
}
