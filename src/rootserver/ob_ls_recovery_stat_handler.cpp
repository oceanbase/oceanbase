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

#include "ob_ls_recovery_stat_handler.h" // ObLSRecoveryStatHandler
#include "lib/utility/ob_macro_utils.h" // OB_FAIL
#include "lib/oblog/ob_log_module.h"  // LOG_*
#include "lib/utility/ob_print_utils.h" // TO_STRING_KV
#include "logservice/ob_log_service.h" // ObLogService
#include "rootserver/ob_tenant_info_loader.h" // ObTenantInfoLoader

namespace oceanbase
{
namespace rootserver
{
int ObLSRecoveryStatHandler::init(const uint64_t tenant_id, ObLS *ls)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSRecoveryStatHandler init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tenant_id));
  } else {
    ls_ = ls;
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("ObLSRecoveryStatHandler init success", K(this));
  }

  return ret;
}

void ObLSRecoveryStatHandler::reset()
{
  is_inited_ = false;
  ls_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObLSRecoveryStatHandler::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(ls_) || !is_valid_tenant_id(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Member variables is not init", KR(ret), KP(ls_), K_(tenant_id));
  }
  return ret;
}

int ObLSRecoveryStatHandler::get_ls_replica_readable_scn(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  share::SCN unused_sync_scn = SCN::min_scn();
  readable_scn = SCN::min_scn();
  share::SCN readable_scn_to_increase = SCN::min_scn();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K(is_inited_));
  } else if (OB_FAIL(ls_->get_max_decided_scn(readable_scn))) {
    LOG_WARN("failed to get readable scn", KR(ret), KPC_(ls));
  } else if (FALSE_IT(readable_scn_to_increase = readable_scn)) {
  } else if (OB_FAIL(increase_ls_replica_readable_scn_(readable_scn_to_increase, unused_sync_scn))) {
    if (OB_NOT_MASTER == ret) {
      // if not master, do not increase_ls_replica_readable_scn
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to increase_ls_replica_readable_scn_", KR(ret), K(readable_scn_to_increase), KPC_(ls));
    }
  } else {
    readable_scn = readable_scn_to_increase;
  }

  return ret;
}

int ObLSRecoveryStatHandler::increase_ls_replica_readable_scn_(SCN &readable_scn, SCN &sync_scn)
{
  int ret = OB_SUCCESS;
  sync_scn = SCN::min_scn();
  int64_t first_proposal_id = palf::INVALID_PROPOSAL_ID;
  int64_t second_proposal_id = palf::INVALID_PROPOSAL_ID;
  common::ObRole first_role;
  common::ObRole second_role;
  logservice::ObLogService *ls_svr = MTL(logservice::ObLogService*);
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader*);
  share::ObAllTenantInfo tenant_info;
  palf::PalfHandleGuard palf_handle_guard;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls_svr) || OB_ISNULL(tenant_info_loader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(ls_svr), KP(tenant_info_loader));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), first_role, first_proposal_id))) {
    LOG_WARN("failed to get first role", KR(ret), K(ls_->get_ls_id()), KP(ls_svr));
  } else if (!is_strong_leader(first_role)) {
    ret = OB_NOT_MASTER;
    // Since the follower replica also call this function, return OB_NOT_MASTER does not LOG_WARN
  } else if (!readable_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(readable_scn));
  } else if (OB_FAIL(ls_svr->open_palf(ls_->get_ls_id(), palf_handle_guard))) {
    LOG_WARN("failed to open palf", KR(ret), K_(ls));
  // scn get order: read_scn before replayable_scn before sync_scn
  } else if (OB_FAIL(tenant_info_loader->get_tenant_info(tenant_info))) {
    LOG_WARN("failed to get_tenant_info", KR(ret));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(sync_scn))) {
    LOG_WARN("failed to get end ts", KR(ret), K_(ls));
  } else if (OB_FAIL(ls_svr->get_palf_role(ls_->get_ls_id(), second_role, second_proposal_id))) {
    LOG_WARN("failed to get second role", KR(ret), K(ls_->get_ls_id()), KP(ls_svr));
  } else if (!(first_proposal_id == second_proposal_id
             && first_role == second_role)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("not leader", KR(ret), K(first_proposal_id), K(second_proposal_id), K(first_role),
                           K(second_role), KPC_(ls));
  } else {
    if (sync_scn < tenant_info.get_replayable_scn() && readable_scn == sync_scn
        && sync_scn.is_valid_and_not_min() && tenant_info.get_replayable_scn().is_valid_and_not_min()
        && readable_scn.is_valid_and_not_min()) {
      // two scenarios
      // 1. when sync scn is pushed forward in switchover
      // 2. wait offline LS
      sync_scn = readable_scn = tenant_info.get_replayable_scn();
    }
  }

  return ret;
}

}
}