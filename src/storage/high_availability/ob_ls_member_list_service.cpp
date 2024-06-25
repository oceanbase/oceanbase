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
#include "storage/high_availability/ob_storage_ha_src_provider.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

ObLSMemberListService::ObLSMemberListService()
  : is_inited_(false),
    ls_(NULL),
    transfer_scn_iter_lock_(),
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
    const bool need_get_config_version,
    palf::LogConfigVersion &config_version,
    share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  config_version.reset();
  transfer_scn.reset();
  if (need_get_config_version && OB_FAIL(log_handler_->get_leader_config_version(config_version))) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_validity_(leader_config_version))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn validity", K(ret));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_validity_(leader_config_version))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn validity", K(ret));
  } else if (OB_FAIL(log_handler_->replace_member(added_member,
                                                  removed_member,
                                                  leader_config_version,
                                                  timeout))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret), K(added_member), K(removed_member), K(leader_config_version));
  } else {
    STORAGE_LOG(INFO, "replace member success", K(ret), K(added_member), K(removed_member), K(leader_config_version));
  }
  return ret;
}

// TODO(yangyi.yyy) :replace member with learner
int ObLSMemberListService::replace_member_with_learner(
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t replace_member_timeout_us)
{
  int ret = OB_SUCCESS;
  palf::LogConfigVersion leader_config_version;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_validity_(leader_config_version))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn validity", K(ret));
  } else if (OB_FAIL(log_handler_->replace_member_with_learner(added_member,
                                                               removed_member,
                                                               leader_config_version,
                                                               replace_member_timeout_us))) {
    STORAGE_LOG(WARN, "failed to add member", K(ret));
  } else {
    STORAGE_LOG(INFO, "replace member with learner success", K(ret));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_validity_(leader_config_version))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn validity", K(ret));
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

int ObLSMemberListService::get_max_tablet_transfer_scn(share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  const bool need_initial_state = false;
  ObHALSTabletIDIterator iter(ls_->get_ls_id(), need_initial_state);
  share::SCN max_transfer_scn = share::SCN::min_scn();
  static const int64_t LOCK_TIMEOUT = 100_ms; // 100ms
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(ls_->build_tablet_iter(iter))) {
    STORAGE_LOG(WARN, "failed to build tablet iter", K(ret));
  } else if (OB_FAIL(transfer_scn_iter_lock_.lock(LOCK_TIMEOUT))) {
    STORAGE_LOG(WARN, "failed to lock transfer scn iter lock", K(ret));
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    common::ObTabletID tablet_id;
    ObTabletMapKey key;
    key.ls_id_ = ls_->get_ls_id();
    ObTabletCreateDeleteMdsUserData mds_data;
    ObTabletHandle tablet_handle;
    const WashTabletPriority priority = WashTabletPriority::WTP_LOW;
    while (OB_SUCC(ret)) {
      mds_data.reset();
      if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get tablet id", K(ret));
        }
      } else if (OB_FALSE_IT(key.tablet_id_ = tablet_id)) {
      } else if (OB_FAIL(t3m->get_tablet(priority, key, tablet_handle))) {
        STORAGE_LOG(WARN, "failed to get tablet", K(ret), K(key));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_tablet_status(share::SCN::max_scn(), mds_data))) {
        if (OB_EMPTY_RESULT == ret) {
          STORAGE_LOG(INFO, "committed tablet_status does not exist", K(ret), K(key));
          ret = OB_SUCCESS;
        } else if (OB_ERR_SHARED_LOCK_CONFLICT == ret) {
          if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
            STORAGE_LOG(INFO, "committed tablet_status does not exist", K(ret), K(tablet_id));
            break;
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          STORAGE_LOG(WARN, "failed to get mds table", KR(ret), K(key));
        }
      } else if (share::SCN::invalid_scn() == mds_data.transfer_scn_) {
        // do nothing
      } else {
        transfer_scn = mds_data.transfer_scn_;
        max_transfer_scn = MAX(transfer_scn, max_transfer_scn);
      }
    }
    if (OB_SUCC(ret)) {
      transfer_scn = max_transfer_scn;
    }
    transfer_scn_iter_lock_.unlock();
  }
  return ret;
}

int ObLSMemberListService::get_leader_config_version_and_transfer_scn_(
    palf::LogConfigVersion &leader_config_version,
    share::SCN &leader_transfer_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  common::ObAddr addr;
  const bool need_get_config_version = true;
  storage::ObHAChangeMemberProxy proxy(
      *(GCTX.storage_rpc_proxy_), &obrpc::ObStorageRpcProxy::get_config_version_and_transfer_scn);
  if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(ls_->get_tenant_id(), ls_->get_ls_id(), addr))) {
    STORAGE_LOG(WARN, "failed to get ls leader", K(ret), KPC(ls_));
  } else if (OB_FAIL(get_config_version_and_transfer_scn_(proxy,
                                                          addr,
                                                          need_get_config_version,
                                                          ls_->get_tenant_id(),
                                                          ls_->get_ls_id()))) {
    STORAGE_LOG(WARN, "failed to get config version and transfer scn", K(ret), K(addr));
  }
  ObArray<int> return_code_array;
  if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
    STORAGE_LOG(WARN, "fail to wait all batch result", KR(ret), KR(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  int64_t check_pass_count = 0;
  if (OB_FAIL(ret)) {
  } else if (1 != return_code_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cnt not match", KR(ret),
                      "return_cnt", return_code_array.count());
  } else if (OB_FAIL(process_result_from_async_rpc_(proxy,
                                                    addr,
                                                    return_code_array,
                                                    false/*for_standby*/,
                                                    check_pass_count,
                                                    leader_config_version,
                                                    leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to process result from async rpc", KR(ret), KR(tmp_ret));
  }
  return ret;
}

int ObLSMemberListService::get_config_version_and_transfer_scn_(
    ObHAChangeMemberProxy &proxy,
    const common::ObAddr &addr,
    const bool need_get_config_version,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObStorageChangeMemberArg arg;
  arg.tenant_id_ = tenant_id;
  arg.ls_id_ = ls_id;
  arg.need_get_config_version_ = need_get_config_version;
  const int64_t cluster_id = GCONF.cluster_id;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  const uint64_t group_id = share::OBCG_STORAGE;
  if (OB_FAIL(proxy.call(addr,
                         timeout,
                         cluster_id,
                         tenant_id,
                         group_id,
                         arg))) {
    STORAGE_LOG(WARN, "failed to call get config version and transfer scn", K(ret), K(addr), K(timeout), K(tenant_id), K(arg));
  }
  return ret;
}

int ObLSMemberListService::check_ls_transfer_scn_(const share::SCN &transfer_scn, bool &check_pass)
{
  int ret = OB_SUCCESS;
  check_pass = false;
  share::SCN local_transfer_scn;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be null", K(ret), KP_(ls));
  } else if (OB_FAIL(ls_->get_transfer_scn(local_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get transfer scn", K(ret), KP_(ls));
  } else if (transfer_scn > local_transfer_scn) {
    STORAGE_LOG(WARN, "local transfer scn is less than leader transfer scn",
        K(ret), K(transfer_scn), K(local_transfer_scn));
  } else {
    check_pass = true;
    STORAGE_LOG(INFO, "check ls transfer scn", KPC_(ls), K(transfer_scn), K(local_transfer_scn));
  }
  return ret;
}

int ObLSMemberListService::get_ls_member_list_(common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  ObStorageHAGetMemberHelper get_member_helper;
  ObLSService *ls_svr = NULL;
  ObStorageRpc *storage_rpc = NULL;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be null", K(ret), KP_(ls));
  } else if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_ISNULL(storage_rpc = ls_svr->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "storage rpc should not be NULL", K(ret), KP(storage_rpc));
  } else if (OB_FAIL(get_member_helper.init(storage_rpc))) {
    STORAGE_LOG(WARN, "failed to init palf helper", K(ret), KP_(ls));
  } else if (OB_FAIL(get_member_helper.get_ls_member_list(ls_->get_tenant_id(), ls_->get_ls_id(), addr_list))) {
    STORAGE_LOG(WARN, "failed to get ls member list", K(ret), KP_(ls));
  }
  return ret;
}

int ObLSMemberListService::check_ls_transfer_scn_validity_(palf::LogConfigVersion &leader_config_version)
{
  int ret = OB_SUCCESS;
  if (MTL_TENANT_ROLE_CACHE_IS_INVALID()) {
    ret = OB_NEED_RETRY;
    STORAGE_LOG(WARN, "tenant role is invalid, need retry", KR(ret));
  } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    if (OB_FAIL(check_ls_transfer_scn_validity_for_primary_(leader_config_version))) {
      STORAGE_LOG(WARN, "failed to check ls transfer scn validity for primary", K(ret), KP_(ls));
    }
  } else {//standby restore
    SERVER_EVENT_SYNC_ADD("storage_ha", "before_check_ls_transfer_scn_validity_for_standby",
                          "tenant_id", ls_->get_tenant_id(),
                          "ls_id", ls_->get_ls_id().id());
    DEBUG_SYNC(BEFORE_CHECK_LS_TRANSFER_SCN_FOR_STANDBY);
    if (OB_FAIL(check_ls_transfer_scn_validity_for_standby_(leader_config_version))) {
      STORAGE_LOG(WARN, "failed to check ls transfer scn validity for standby", K(ret), KP_(ls));
    }
  }
  return ret;
}

int ObLSMemberListService::check_ls_transfer_scn_validity_for_primary_(palf::LogConfigVersion &leader_config_version)
{
  int ret = OB_SUCCESS;
  bool check_pass = false;
  share::SCN leader_transfer_scn;
  if (OB_FAIL(get_leader_config_version_and_transfer_scn_(
      leader_config_version, leader_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get leader config version and transfer scn", K(ret));
  } else if (OB_FAIL(check_ls_transfer_scn_(leader_transfer_scn, check_pass))) {
    STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret), K(leader_config_version));
  } else if (!check_pass) {
    ret = OB_LS_TRANSFER_SCN_TOO_SMALL;
    STORAGE_LOG(WARN, "ls transfer scn too small", K(ret), K(leader_transfer_scn));
  }
  return ret;
}

int ObLSMemberListService::check_ls_transfer_scn_validity_for_standby_(palf::LogConfigVersion &leader_config_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObAddr> addr_list;
  ObAddr leader_addr;
  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls should not be null", K(ret));
  } else if (OB_FAIL(get_ls_member_list_(addr_list))) {
    STORAGE_LOG(WARN, "failed to get ls member list", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(ls_->get_tenant_id(), ls_->get_ls_id(), leader_addr))) {
    STORAGE_LOG(WARN, "failed to get ls leader", K(ret), KPC(ls_));
  } else {
    int64_t check_pass_count = 0;
    storage::ObHAChangeMemberProxy batch_proxy(
        *(GCTX.storage_rpc_proxy_), &obrpc::ObStorageRpcProxy::get_config_version_and_transfer_scn);
    for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
      const ObAddr &addr = addr_list.at(i);
      bool check_pass = false;
      share::SCN transfer_scn;
      palf::LogConfigVersion config_version;
      bool need_get_config_version = (addr == leader_addr);
      if (OB_TMP_FAIL(get_config_version_and_transfer_scn_(batch_proxy,
                                                           addr,
                                                           need_get_config_version,
                                                           ls_->get_tenant_id(),
                                                           ls_->get_ls_id()))) {
        STORAGE_LOG(WARN, "failed to get config version and transfer scn", K(ret), K(addr));
      }
    }
    ObArray<int> return_code_array;
    if (OB_TMP_FAIL(batch_proxy.wait_all(return_code_array))) {
      STORAGE_LOG(WARN, "fail to wait all batch result", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    share::SCN leader_transfer_scn;
    if (OB_FAIL(ret)) {
    } else if (return_code_array.count() != addr_list.count()
               || return_code_array.count() != batch_proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "cnt not match", KR(ret),
                        "return_cnt", return_code_array.count(),
                        "result_cnt", batch_proxy.get_results().count(),
                        "server_cnt", addr_list.count());
    } else if (OB_FAIL(process_result_from_async_rpc_(batch_proxy,
                                                      leader_addr,
                                                      return_code_array,
                                                      true/*for_standby*/,
                                                      check_pass_count,
                                                      leader_config_version,
                                                      leader_transfer_scn))) {
      STORAGE_LOG(WARN, "failed to process result from async rpc", KR(ret), KR(tmp_ret));
    } else {
      STORAGE_LOG(INFO, "process result from async rpc", KR(ret), K(return_code_array), K(addr_list), K(batch_proxy.get_results()));
    }
    if (OB_SUCC(ret)) {
      // standby check transfer scn need reach majority
      if (check_pass_count < (addr_list.count() / 2 + 1)) {
        ret = OB_LS_TRANSFER_SCN_TOO_SMALL;
        STORAGE_LOG(WARN, "transfer scn compare do not reach majority", K(ret), K(addr_list));
#ifdef ERRSIM
        SERVER_EVENT_ADD("storage_ha", "standby_check_transfer_scn_too_small",
                         "tenant_id", ls_->get_tenant_id(),
                         "ls_id", ls_->get_ls_id().id(),
                         "member_list_count", addr_list.count(),
                         "check_pass_count", check_pass_count);
#endif
      } else {
        STORAGE_LOG(INFO, "passed transfer scn check for standby", K(ret), K(addr_list), K(check_pass_count));
      }
    }
  }
  return ret;
}

int ObLSMemberListService::process_result_from_async_rpc_(
    ObHAChangeMemberProxy &proxy,
    const common::ObAddr &leader_addr,
    const common::ObIArray<int> &return_code_array,
    const bool for_standby,
    int64_t &pass_count,
    palf::LogConfigVersion &leader_config_version,
    share::SCN &leader_transfer_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ARRAY_FOREACH_X(proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
    const ObStorageChangeMemberRes *response = proxy.get_results().at(idx);
    const int res_ret = return_code_array.at(idx);
    bool check_pass = false;
    if (OB_SUCCESS != res_ret) {
#ifdef ERRSIM
      SERVER_EVENT_ADD("storage_ha", "check_ls_transfer_scn_validity_for_standby_failed",
                      "tenant_id", ls_->get_tenant_id(),
                      "ls_id", ls_->get_ls_id().id(),
                      "result", res_ret);
#endif
    } else if (OB_ISNULL(response)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "hb_response is null", KR(ret), KR(tmp_ret));
    } else if (for_standby && OB_FAIL(check_ls_transfer_scn_(response->transfer_scn_, check_pass))) {
      STORAGE_LOG(WARN, "failed to check ls transfer scn", K(ret));
    } else if (for_standby && !check_pass) {
      continue;
    } else {
      const palf::LogConfigVersion &config_version = response->config_version_;
      const ObAddr &addr = proxy.get_dests().at(idx);
      if (addr == leader_addr) {
        if (!config_version.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "config version is not valid", K(ret), K(config_version));
        } else {
          leader_config_version = config_version;
          leader_transfer_scn = response->transfer_scn_;
        }
      }
      if (OB_SUCC(ret)) {
        pass_count++;
      }
    }
  }
  return ret;
}



}
}
