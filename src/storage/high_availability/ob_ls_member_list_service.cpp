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

int ObLSMemberListService::add_member(
    const common::ObMember &member,
    const int64_t paxos_replica_num,
    const share::SCN &transfer_scn,
    const int64_t add_member_timeout_us)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_->get_leader_config_version(config_version))) {
    STORAGE_LOG(WARN, "failed to get config version", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
  } else if (OB_FAIL(log_handler_->add_member(member, paxos_replica_num, config_version, add_member_timeout_us))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret));
  } else {
    STORAGE_LOG(INFO, "add member success", K(ret), K(member), K(paxos_replica_num), K(config_version));
  }
  return ret;
}

int ObLSMemberListService::replace_member(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const share::SCN &transfer_scn,
    const int64_t replace_member_timeout_us)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_->get_leader_config_version(config_version))) {
    STORAGE_LOG(WARN, "failed to get config version", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
  } else if (OB_FAIL(log_handler_->replace_member(added_member, removed_member, config_version, replace_member_timeout_us))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret));
  } else {
    STORAGE_LOG(INFO, "replace member success", K(ret));
  }
  return ret;
}

int ObLSMemberListService::switch_learner_to_acceptor(
    const common::ObMember &learner,
    const int64_t paxos_replica_num,
    const share::SCN &transfer_scn,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_->get_leader_config_version(config_version))) {
    STORAGE_LOG(WARN, "failed to get config version", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(transfer_scn))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
  } else if (OB_FAIL(log_handler_->switch_learner_to_acceptor(
      learner, paxos_replica_num, config_version, timeout_us))) {
    STORAGE_LOG(WARN, "failed to switch learner to acceptor", K(ret));
  } else {
    STORAGE_LOG(INFO, "switch learner to acceptor success", K(ret));
  }
  return ret;
}

// TODO(yangyi.yyy): how to check standby transfer scn
int ObLSMemberListService::check_ls_transfer_scn_(const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  share::SCN local_transfer_scn;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be null", K(ret), KP_(ls));
  } else if (OB_FAIL(ls_->get_transfer_scn(local_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get transfer scn", K(ret), KP_(ls));
  } else if (transfer_scn < local_transfer_scn) {
    ret = OB_LS_TRANSFER_SCN_TOO_SMALL;
    STORAGE_LOG(WARN, "transfer scn is less than local transfer scn", K(ret), K(transfer_scn));
  } else {
    STORAGE_LOG(INFO, "check ls transfer scn", KPC_(ls), K(transfer_scn), K(local_transfer_scn));
  }
  return ret;
}

}
}
