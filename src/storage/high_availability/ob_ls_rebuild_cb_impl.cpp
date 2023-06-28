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
#include "ob_rebuild_service.h"
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
    wakeup_rebuild_service_();
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
  ObLSRebuildInfo rebuild_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (OB_FAIL(ls_->get_rebuild_info(rebuild_info))) {
    LOG_WARN("failed to get rebuild info", K(ret), KPC(ls_));
  } else if (!rebuild_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild info is invalid", K(ret), KPC(ls_));
  } else if (rebuild_info.is_in_rebuild()) {
    is_ls_in_rebuild = true;
    LOG_INFO("ls is already in rebuild status", K(ret), K(rebuild_info), KPC(ls_));
  }
  return ret;
}

int ObLSRebuildCbImpl::execute_rebuild_()
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = nullptr;
  const ObLSRebuildType rebuild_type(ObLSRebuildType::CLOG);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls rebuild cb impl do not init", K(ret));
  } else if (OB_ISNULL(rebuild_service = (MTL(ObRebuildService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild service not be NULL", K(ret), KP(rebuild_service));
  } else if (OB_FAIL(rebuild_service->add_rebuild_ls(ls_->get_ls_id(), rebuild_type))) {
    LOG_WARN("failed to add rebuild ls", K(ret), KPC(ls_), K(rebuild_type));
  } else {
    LOG_INFO("succeed execute rebuild", KPC(ls_));
    wakeup_rebuild_service_();
  }
  return ret;
}

void ObLSRebuildCbImpl::wakeup_rebuild_service_()
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = nullptr;

  if (OB_ISNULL(rebuild_service = (MTL(ObRebuildService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild service not be NULL", K(ret), KP(rebuild_service));
  } else {
    rebuild_service->wakeup();
  }
}


}
}
