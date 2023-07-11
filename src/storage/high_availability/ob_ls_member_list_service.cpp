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

#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/high_availability/ob_ls_member_list_service.h"

namespace oceanbase
{
namespace storage
{

ObLSMemberListService::ObLSMemberListService()
  : is_inited_(false),
    ls_(NULL),
    log_handler_(NULL)
{
}

ObLSMemberListService::~ObLSMemberListService()
{
}

int ObLSMemberListService::init(storage::ObLS *ls, logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "member list service is inited", K(ret), KP(ls));
  } else if (OB_UNLIKELY(nullptr == ls || nullptr == log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(ls), KP(log_handler));
  } else {
    ls_ = ls;
    log_handler_ = log_handler;
    is_inited_ = true;
    STORAGE_LOG(INFO, "success to init member list service", K(ret), KP(ls), "ls_id", ls_->get_ls_id(), KP(this));
  }
  return ret;
}

void ObLSMemberListService::destroy()
{
  is_inited_ = false;
  ls_ = nullptr;
  log_handler_ = nullptr;
}

int ObLSMemberListService::get_config_version_and_transfer_scn(
    palf::LogConfigVersion &config_version,
    share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  config_version.reset();
  transfer_scn.reset();
  if (OB_FAIL(log_handler_->get_leader_config_version(config_version))) {
    STORAGE_LOG(WARN, "failed to get config version", K(ret));
  } else if (OB_FAIL(ls_->get_transfer_scn(transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get transfer scn", K(ret), KP_(ls));
  }
  return ret;
}

int ObLSMemberListService::add_member(
    const common::ObMember &member,
    const int64_t paxos_replica_num,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion leader_config_version;
  share::SCN leader_transfer_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(get_leader_config_version_and_transfer_scn_(
      leader_config_version, leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get leader config version and transfer scn", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
  } else if (OB_FAIL(log_handler_->add_member(member,
                                              paxos_replica_num,
                                              leader_config_version,
                                              timeout))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret), K(member), K(paxos_replica_num));
  } else {
    STORAGE_LOG(INFO, "add member success", K(ret), K(member), K(paxos_replica_num), K(leader_config_version));
  }
  return ret;
}

int ObLSMemberListService::replace_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion leader_config_version;
  share::SCN leader_transfer_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(get_leader_config_version_and_transfer_scn_(
      leader_config_version, leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get leader config version and transfer scn", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
  } else if (OB_FAIL(log_handler_->replace_member(added_member,
                                                  removed_member,
                                                  leader_config_version,
                                                  timeout))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret), K(added_member), K(removed_member), K(leader_config_version));
  } else {
    STORAGE_LOG(INFO, "replace member success", K(ret), K(added_member), K(removed_member), K(leader_config_version), K(leader_transfer_scn));
  }
  return ret;
}

int ObLSMemberListService::switch_learner_to_acceptor(
    const common::ObMember &learner,
    const int64_t paxos_replica_num,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion leader_config_version;
  share::SCN leader_transfer_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(get_leader_config_version_and_transfer_scn_(
      leader_config_version, leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get leader config version and transfer scn", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret), K(leader_config_version));
  } else if (OB_FAIL(log_handler_->switch_learner_to_acceptor(learner,
                                                              paxos_replica_num,
                                                              leader_config_version,
                                                              timeout))) {
    STORAGE_LOG(WARN, "failed to switch learner to acceptor", K(ret));
  } else {
    STORAGE_LOG(INFO, "switch learner to acceptor success", K(ret), K(learner), K(paxos_replica_num), K(leader_config_version));
  }
  return ret;
}

int ObLSMemberListService::get_leader_config_version_and_transfer_scn_(
    palf::LogConfigVersion &leader_config_version,
    share::SCN &leader_transfer_scn)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObStorageRpc *storage_rpc = NULL;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(ls_->get_tenant_id(), ls_->get_ls_id(), src_info.src_addr_))) {
    STORAGE_LOG(WARN, "failed to get ls leader", K(ret), KPC(ls_));
  } else if (OB_ISNULL(storage_rpc = ls_svr->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "storage rpc should not be NULL", K(ret), KP(storage_rpc));
  } else if (OB_FAIL(storage_rpc->get_config_version_and_transfer_scn(ls_->get_tenant_id(),
                                                                      src_info,
                                                                      ls_->get_ls_id(),
                                                                      leader_config_version,
                                                                      leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get config version and transfer scn", K(ret), KPC(ls_));
  }
  return ret;
}

// TODO(yangyi.yyy): how to check standby transfer scn
int ObLSMemberListService::check_ls_transfer_scn_(const share::SCN &leader_transfer_scn)
{
  int ret = OB_SUCCESS;
  share::SCN local_transfer_scn;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be null", K(ret), KP_(ls));
  } else if (OB_FAIL(ls_->get_transfer_scn(local_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get transfer scn", K(ret), KP_(ls));
  } else if (leader_transfer_scn > local_transfer_scn) {
    ret = OB_LS_TRANSFER_SCN_TOO_SMALL;
    STORAGE_LOG(WARN, "local transfer scn is less than leader transfer scn",
        K(ret), K(leader_transfer_scn), K(local_transfer_scn));
  } else {
    STORAGE_LOG(INFO, "check ls transfer scn", KPC_(ls), K(leader_transfer_scn), K(local_transfer_scn));
  }
  return ret;
}

}
}
