/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_transfer_lock_utils.h"
#include "ob_transfer_lock_info_operator.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_max_id_fetcher.h"
#include "storage/ob_common_id_utils.h"
#include "share/ob_common_id.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/ob_storage_rpc.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_tenant_info_proxy.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase {
namespace storage {

ERRSIM_POINT_DEF(EN_START_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED);
ERRSIM_POINT_DEF(EN_DOING_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED);

static int get_ls_handle(const uint64_t tenant_id, const share::ObLSID &ls_id, storage::ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ls_handle.reset();
  ObLSService *ls_service = NULL;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObMemberListLockUtils::batch_lock_ls_member_list(const uint64_t tenant_id, const int64_t task_id,
    const common::ObArray<share::ObLSID> &lock_ls_list, const common::ObMemberList &member_list,
    const ObTransferLockStatus &status, const int32_t group_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObLSID> sorted_ls_list;
  if (OB_FAIL(sorted_ls_list.assign(lock_ls_list))) {
    LOG_WARN("failed to assign ls list", K(ret));
  } else {
    lib::ob_sort(sorted_ls_list.begin(), sorted_ls_list.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_ls_list.count(); ++i) {
      const share::ObLSID &ls_id = sorted_ls_list.at(i);
      if (OB_FAIL(lock_ls_member_list(tenant_id, ls_id, task_id, member_list, status, group_id, sql_proxy))) {
        LOG_WARN("failed to lock ls member list", K(ret), K(ls_id), K(member_list));
      }
    }
  }
  return ret;
}

int ObMemberListLockUtils::lock_ls_member_list(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const int64_t task_id, const common::ObMemberList &member_list, const ObTransferLockStatus &status,
    const int32_t group_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  int64_t lock_owner = -1;
  int64_t real_lock_owner = -1;
  ObSqlString member_list_str;
  const int64_t lock_timeout = CONFIG_CHANGE_TIMEOUT;
  if (!ls_id.is_valid() || !member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id));
  } else if (OB_FAIL(unlock_ls_member_list(tenant_id, ls_id, task_id, member_list, status, group_id, sql_proxy))) {
    LOG_WARN("failed to unlock ls member list", K(ret));
  } else if (OB_FAIL(get_lock_owner(tenant_id, ls_id, task_id, status, group_id, sql_proxy, lock_owner))) {
    LOG_WARN("failed to get lock owner", K(ret), K(tenant_id), K(ls_id), K(task_id), K(status));
  } else if (OB_FAIL(get_member_list_str(member_list, member_list_str))) {
    LOG_WARN("failed to get member list str", K(ret), K(member_list));
  } else if (OB_FAIL(
                 insert_lock_info(tenant_id, ls_id, task_id, status, lock_owner, member_list_str.ptr(), group_id, real_lock_owner, sql_proxy))) {
    LOG_WARN("failed to insert lock info", K(ret), K(ls_id), K(task_id));
  } else {
#ifdef ERRSIM
    if (GCONF.errsim_transfer_ls_id == ls_id.id()) {
      SERVER_EVENT_SYNC_ADD("transfer", "BETWEEN_INSERT_LOCK_INFO_AND_TRY_LOCK_CONFIG_CHANGE");
      DEBUG_SYNC(BETWEEN_INSERT_LOCK_INFO_AND_TRY_LOCK_CONFIG_CHANGE);
    }
#endif
    ObTransferTaskLockInfo lock_info;
    int64_t palf_lock_owner = -1;
    bool palf_is_locked = false;
    bool need_lock_palf = false;
    if (OB_FAIL(lock_info.set(tenant_id, ls_id, task_id, status, real_lock_owner, member_list_str.ptr()))) {
      LOG_WARN("failed to set lock info", K(ret), K(tenant_id), K(ls_id), K(task_id), K(status), K(real_lock_owner));
    } else if (OB_FAIL(get_config_change_lock_stat_(lock_info, group_id, palf_lock_owner, palf_is_locked))) {
      LOG_WARN("failed to get config change lock stat", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(check_lock_status_(palf_is_locked, palf_lock_owner, real_lock_owner, need_lock_palf))) {
      LOG_WARN("failed to check lock status", K(ret), K(palf_is_locked), K(palf_lock_owner), K(real_lock_owner));
    } else if (need_lock_palf && OB_FAIL(try_lock_config_change_(lock_info, lock_timeout, group_id))) {
      LOG_WARN("failed to try lock config config",
          K(ret),
          K(tenant_id),
          K(ls_id),
          K(palf_lock_owner),
          K(palf_is_locked),
          K(lock_owner),
          K(lock_info));
    } else {
      LOG_INFO("lock ls member list", K(lock_info), K(palf_lock_owner), K(palf_is_locked), K(real_lock_owner));
    }
  }

  return ret;
}

int ObMemberListLockUtils::unlock_ls_member_list(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const int64_t task_id, const common::ObMemberList &member_list, const ObTransferLockStatus &status,
    const int32_t group_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t lock_timeout = CONFIG_CHANGE_TIMEOUT;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), K(member_list));
  } else {
    ObMySQLTransaction trans;
    ObTransferLockInfoRowKey row_key;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    row_key.tenant_id_ = tenant_id;
    row_key.ls_id_ = ls_id;
    if (OB_FAIL(trans.start(&sql_proxy, meta_tenant_id, false/*with_snapshot*/, group_id))) {
      LOG_WARN("failed to start trans", K(ret), K(meta_tenant_id));
    } else {
      int64_t lock_owner = -1;
      ObTransferTaskLockInfo lock_info;
      const bool for_update = true;
      bool palf_is_locked = false;
      int64_t palf_lock_owner = -1;
      bool need_unlock = true;
      bool need_relock_before_unlock = false;

      if (OB_FAIL(ObTransferLockInfoOperator::get(row_key, task_id, status, for_update, group_id, lock_info, trans))) {
        if (OB_ENTRY_NOT_EXIST == ret) {  // palf need to be unlocked
          ret = OB_SUCCESS;
          LOG_INFO("member list already unlocked",
              K(tenant_id),
              K(row_key),
              K(task_id),
              K(status),
              K(palf_lock_owner),
              K(palf_is_locked));
        } else {
          LOG_WARN("failed to get lock info", K(ret), K(tenant_id), K(row_key));
        }
      } else if (OB_FAIL(get_config_change_lock_stat_(lock_info, group_id, palf_lock_owner, palf_is_locked))) {
        LOG_WARN("failed to get config change lock stat");
      } else if (OB_FAIL(check_unlock_status_(palf_is_locked, palf_lock_owner, lock_info.lock_owner_,
          need_unlock, need_relock_before_unlock))) {
        LOG_WARN("failed to check unlock status", K(ret), K(palf_is_locked), K(palf_lock_owner), K(lock_info));
      } else if (FALSE_IT(lock_owner = lock_info.lock_owner_)) {
        // assign lock owner
      } else if (need_relock_before_unlock && OB_FAIL(relock_before_unlock_(lock_info, palf_lock_owner, lock_timeout, group_id))) {
        LOG_WARN("failed to relock config change", K(ret), K(lock_info), K(palf_lock_owner));
      } else if (need_unlock && OB_FAIL(unlock_config_change_(lock_info, lock_timeout, group_id))) {
        LOG_WARN("failed to get paxos member list", K(ret), K(lock_info));
      } else if (OB_FAIL(ObTransferLockInfoOperator::remove(tenant_id, ls_id, task_id, status, group_id, trans))) {
        LOG_WARN("failed to update lock info", K(ret), K(row_key), K(lock_info), K(palf_lock_owner), K(palf_is_locked));
      } else {
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          if (ObTransferLockStatus::START == status.get_status()) {
            ret = EN_START_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED ? : OB_SUCCESS;
            if (OB_FAIL(ret)) {
              STORAGE_LOG(ERROR, "fake EN_START_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED", K(ret));
            }
          }
          if (ObTransferLockStatus::DOING == status.get_status()) {
            ret = EN_DOING_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED ? : OB_SUCCESS;
            if (OB_FAIL(ret)) {
              STORAGE_LOG(ERROR, "fake EN_DOING_UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
            SERVER_EVENT_SYNC_ADD("TRANSFER", "UNLOCK_CONFIG_CHANGE_SUCC_AND_REMOVE_INNER_TABLE_FAILED",
                                  "status", status,
                                  "result", ret);
          }
        }
#endif
        LOG_INFO("unlock ls member list",
            K(palf_lock_owner),
            K(lock_owner),
            K(lock_info),
            K(tenant_id),
            K(ls_id),
            K(member_list));
      }

      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to end trans", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }
#ifdef ERRSIM
  if (OB_FAIL(ret)) {
    SERVER_EVENT_SYNC_ADD("transfer", "unlock_ls_member_list_failed",
                          "tenant_id", tenant_id,
                          "ls_id", ls_id.id(),
                          "task_id", task_id,
                          "status", status,
                          "result", ret);
  }
#endif
  return ret;
}

int ObMemberListLockUtils::unlock_member_list_when_switch_to_standby(
    const uint64_t tenant_id, const int32_t group_id, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<ObTransferTaskLockInfo> lock_infos;
  if (OB_FAIL(ObTransferLockInfoOperator::fetch_all(sql_proxy, tenant_id, group_id, lock_infos))) {
    LOG_WARN("failed to fetch all lock info", K(ret), K(tenant_id));
  } else if (lock_infos.empty()) {
    LOG_INFO("no need unlock member list when switch to standby", K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < lock_infos.count(); ++i) {
      const ObTransferTaskLockInfo &lock_info = lock_infos.at(i);
      const uint64_t tenant_id = lock_info.tenant_id_;
      const share::ObLSID &ls_id = lock_info.ls_id_;
      const int64_t task_id = lock_info.task_id_;
      const ObTransferLockStatus status = lock_info.status_;
      ObMemberList fake_member_list;
      if (OB_FAIL(unlock_ls_member_list(tenant_id,
                                        ls_id,
                                        task_id,
                                        fake_member_list,
                                        status,
                                        group_id,
                                        sql_proxy))) {
        LOG_WARN("failed to unlock ls member list", K(ret), K(lock_info));
      }
    }
  }
  return ret;
}

int ObMemberListLockUtils::try_lock_config_change_(
    const ObTransferTaskLockInfo &lock_info, const int64_t lock_timeout, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  bool ls_exist = false;
  ObLSService *ls_svr = NULL;
  const uint64_t tenant_id = lock_info.tenant_id_;
  const share::ObLSID &ls_id = lock_info.ls_id_;
  const int64_t lock_owner = lock_info.lock_owner_;
  obrpc::ObStorageRpcProxy storage_svr_rpc_proxy;
  storage::ObStorageRpc storage_rpc;
  if (OB_FAIL(init_storage_rpc_(storage_svr_rpc_proxy, storage_rpc))) {
    LOG_WARN("failed to init storage rpc", K(ret));
  } else if (OB_FAIL(inner_try_lock_config_change_(lock_info, lock_timeout, group_id, storage_rpc))) {
    LOG_WARN("failed to try lock config change fallback", K(ret), K(lock_info));
  } else {
    LOG_INFO("try lock config change fallback", K(lock_info), K(lock_timeout));
  }
  destory_storage_rpc_(storage_svr_rpc_proxy, storage_rpc);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("TRANSFER_LOCK", "LOCK_CONFIG_CHANGE",
      "tenant_id", lock_info.tenant_id_,
      "ls_id", lock_info.ls_id_.id(),
      "task_id", lock_info.task_id_,
      "status", lock_info.status_.str(),
      "lock_owner", lock_info.lock_owner_,
      "lock_member_list", lock_info.comment_);
  }
  return ret;
}

int ObMemberListLockUtils::inner_try_lock_config_change_(
    const ObTransferTaskLockInfo &lock_info, const int64_t lock_timeout, const int32_t group_id,
    storage::ObStorageRpc &storage_rpc)
{
  int ret = OB_SUCCESS;
  ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (!lock_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(lock_info));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(lock_info.tenant_id_, lock_info.ls_id_, src_info.src_addr_))) {
    LOG_WARN("failed to get ls leader", K(ret), K(lock_info));
  } else if (OB_FAIL(storage_rpc.lock_config_change(lock_info.tenant_id_, src_info, lock_info.ls_id_,
      lock_info.lock_owner_, lock_timeout, group_id))) {
    LOG_WARN("failed to try lock config config", K(ret), K(lock_info));
  }
  return ret;
}

int ObMemberListLockUtils::get_config_change_lock_stat_(
    const ObTransferTaskLockInfo &lock_info, const int32_t group_id, int64_t &palf_lock_owner, bool &is_locked)
{
  int ret = OB_SUCCESS;
  bool ls_exist = false;
  ObLSService *ls_svr = NULL;
  const uint64_t tenant_id = lock_info.tenant_id_;
  const share::ObLSID &ls_id = lock_info.ls_id_;
  obrpc::ObStorageRpcProxy storage_svr_rpc_proxy;
  storage::ObStorageRpc storage_rpc;
  if (OB_FAIL(init_storage_rpc_(storage_svr_rpc_proxy, storage_rpc))) {
    LOG_WARN("failed to init storage rpc", K(ret));
  } else if (OB_FAIL(get_config_change_lock_stat_fallback_(lock_info, group_id, palf_lock_owner, is_locked, storage_rpc))) {
    LOG_WARN("failed to get lock config change fallback", K(ret), K(lock_info));
  } else {
    LOG_INFO("get lock config change stat fallback", K(lock_info), K(palf_lock_owner), K(is_locked));
  }
  destory_storage_rpc_(storage_svr_rpc_proxy, storage_rpc);
#ifdef ERRSIM
  SERVER_EVENT_ADD("TRANSFER_LOCK", "GET_CONFIG_CHANGE_LOCK_STAT",
      "tenant_id", lock_info.tenant_id_,
      "ls_id", lock_info.ls_id_.id(),
      "task_id", lock_info.task_id_,
      "status", lock_info.status_.str(),
      "palf_lock_owner", palf_lock_owner,
      "is_locked", is_locked);
#endif
  return ret;
}

int ObMemberListLockUtils::get_config_change_lock_stat_fallback_(
    const ObTransferTaskLockInfo &lock_info, const int32_t group_id, int64_t &palf_lock_owner,
    bool &is_locked, storage::ObStorageRpc &storage_rpc)
{
  int ret = OB_SUCCESS;
  palf_lock_owner = -1;
  is_locked = false;
  ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (!lock_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(lock_info));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(lock_info.tenant_id_, lock_info.ls_id_, src_info.src_addr_))) {
    LOG_WARN("failed to get ls leader", K(ret), K(lock_info));
  } else if (OB_FAIL(storage_rpc.get_config_change_lock_stat(lock_info.tenant_id_, src_info,
      lock_info.ls_id_, group_id, palf_lock_owner, is_locked))) {
    LOG_WARN("failed to get config change lock stat", K(ret), K(lock_info));
  }
  return ret;
}

int ObMemberListLockUtils::unlock_config_change_(
    const ObTransferTaskLockInfo &lock_info, const int64_t lock_timeout, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  bool ls_exist = false;
  ObLSService *ls_svr = NULL;
  const uint64_t tenant_id = lock_info.tenant_id_;
  const share::ObLSID &ls_id = lock_info.ls_id_;
  const int64_t lock_owner = lock_info.lock_owner_;
  obrpc::ObStorageRpcProxy storage_svr_rpc_proxy;
  storage::ObStorageRpc storage_rpc;
  if (OB_FAIL(init_storage_rpc_(storage_svr_rpc_proxy, storage_rpc))) {
    LOG_WARN("failed to init storage rpc", K(ret));
  } else if (OB_FAIL(unlock_config_change_fallback_(lock_info, lock_timeout, group_id, storage_rpc))) {
    LOG_WARN("failed to try lock config change fallback", K(ret), K(lock_info));
  } else {
    LOG_INFO("unlock lock config change fallback", K(lock_info), K(lock_timeout));
  }
  destory_storage_rpc_(storage_svr_rpc_proxy, storage_rpc);
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("TRANSFER_LOCK", "UNLOCK_CONFIG_CHANGE",
        "tenant_id", lock_info.tenant_id_,
        "ls_id", lock_info.ls_id_.id(),
        "task_id", lock_info.task_id_,
        "status", lock_info.status_.str(),
        "lock_owner", lock_info.lock_owner_,
        "unlock_member_list", lock_info.comment_);
  }
  return ret;
}

int ObMemberListLockUtils::unlock_config_change_fallback_(
    const ObTransferTaskLockInfo &lock_info, const int64_t lock_timeout, const int32_t group_id,
    storage::ObStorageRpc &storage_rpc)
{
  int ret = OB_SUCCESS;
  ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (!lock_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(lock_info));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(lock_info.tenant_id_, lock_info.ls_id_, src_info.src_addr_))) {
    LOG_WARN("failed to get ls leader", K(ret), K(lock_info));
  } else if (OB_FAIL(storage_rpc.unlock_config_change(lock_info.tenant_id_, src_info, lock_info.ls_id_,
      lock_info.lock_owner_, lock_timeout, group_id))) {
    LOG_WARN("failed to try lock config config", K(ret), K(lock_info));
  }
  return ret;
}

/* ObMemberListLockUtils */

int ObMemberListLockUtils::get_lock_owner(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
    const ObTransferLockStatus &status, const int32_t group_id, common::ObMySQLProxy &proxy, int64_t &lock_owner)
{
  int ret = OB_SUCCESS;
  share::ObCommonID common_id;
  const ObMaxIdType id_type = ObMaxIdType::OB_MAX_USED_LOCK_OWNER_ID_TYPE;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id not valid", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObCommonIDUtils::gen_monotonic_id(meta_tenant_id, id_type, group_id, proxy, common_id))) {
    LOG_WARN("failed to gen monotonic id", K(ret), K(meta_tenant_id));
  } else {
    lock_owner = common_id.id();
    LOG_INFO("get lock owner", K(tenant_id), K(lock_owner), K(common_id));
#ifdef ERRSIM
    SERVER_EVENT_ADD("TRANSFER_LOCK", "GET_LOCK_OWNER",
        "tenant_id", tenant_id,
        "ls_id", ls_id.id(),
        "task_id", task_id,
        "status", status.str(),
        "lock_owner", lock_owner);
#endif
  }
  return ret;
}

int ObMemberListLockUtils::get_member_list_str(
    const common::ObMemberList &member_list, ObSqlString &member_list_str)
{
  int ret = OB_SUCCESS;
  ObLSReplica::MemberList ls_member_list;
  if (!member_list.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(member_list));
  } else if (OB_FAIL(share::ObLSReplica::transform_ob_member_list(member_list, ls_member_list))) {
    LOG_WARN("failed to transform ob member list", K(ret), K(member_list));
  } else if (OB_FAIL(share::ObLSReplica::member_list2text(ls_member_list, member_list_str))) {
    LOG_WARN("failed to member list to text", K(ret), K(ls_member_list));
  }
  return ret;
}

int ObMemberListLockUtils::insert_lock_info(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
    const ObTransferLockStatus &status, const int64_t lock_owner, const common::ObString &comment,
    const int32_t group_id, int64_t &real_lock_owner, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  real_lock_owner = -1;
  ObMySQLTransaction trans;
  ObTransferTaskLockInfo lock_info;
  ObAllTenantInfo tenant_info;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_FAIL(lock_info.set(tenant_id, ls_id, task_id, status, lock_owner, comment))) {
    LOG_WARN("failed to set lock info", K(ret), K(tenant_id), K(ls_id), K(status), K(real_lock_owner));
  } else if (OB_FAIL(trans.start(&sql_proxy, meta_tenant_id, false/*with_snapshot*/, group_id))) {
    LOG_WARN("failed to start trans", K(ret), K(meta_tenant_id));
  } else {
    if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true/*for_update*/, tenant_info))) {
      LOG_WARN("failed to load tenant info", K(ret), K(tenant_id));
    } else if (!tenant_info.is_primary()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("tenant is not primary, do not allow insert lock info", K(tenant_id), K(tenant_info));
    } else if (OB_FAIL(ObTransferLockInfoOperator::insert(lock_info, group_id, trans))) {
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        ObTransferTaskLockInfo tmp_lock_info;
        if (OB_FAIL(get_lock_info(tenant_id, ls_id, task_id, status, group_id, tmp_lock_info, trans))) {
          LOG_WARN("failed to get lock info", K(ret), K(ls_id));
        } else {
          real_lock_owner = tmp_lock_info.lock_owner_;
          LOG_INFO("lock info already exist", K(ret), K(real_lock_owner), K(lock_owner));
        }
      } else {
        LOG_WARN("failed to insert lock info", K(ret), K(ls_id));
      }
    } else {
      real_lock_owner = lock_owner;
    }
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObMemberListLockUtils::get_lock_info(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
    const ObTransferLockStatus &status, const int32_t group_id, ObTransferTaskLockInfo &lock_info, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  lock_info.reset();
  const bool for_update = false;
  ObTransferLockInfoRowKey row_key;
  row_key.tenant_id_ = tenant_id;
  row_key.ls_id_ = ls_id;
  if (!row_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(row_key));
  } else if (OB_FAIL(ObTransferLockInfoOperator::get(row_key, task_id, status, for_update, group_id, lock_info, sql_proxy))) {
    LOG_WARN("failed to get lock info", K(ret), K(row_key), K(task_id), K(status));
  }
  return ret;
}

int ObMemberListLockUtils::check_lock_status_(
    const bool palf_is_locked, const int64_t palf_lock_owner, const int64_t inner_table_lock_owner, bool &need_lock)
{
  int ret = OB_SUCCESS;
  need_lock = false;
  // Checking the status of a lock on a palf and an inner table, If the palf is not locked and the palf lock owner is
  // greater than the inner table lock owner, we need to report an error, else we need to lock palf
  if (!palf_is_locked) {
    if (palf_lock_owner == inner_table_lock_owner) {
      // This code block handles the scenario where the PALF unlock operation of the member list was successful,
      // but the inner table removal failed. If the transfer thread attempts to retry the process,
      // it may result in the palf lock owner being the same as the inner table lock owner.
      // In this case, we only log an error for notification purposes.
      need_lock = true;
      LOG_ERROR("palf lock owner equal to inner table lock owner, please check condition");
    } else if (palf_lock_owner > inner_table_lock_owner) {
      ret = OB_ERR_UNEXPECTED_LOCK_OWNER;
      LOG_WARN("palf lock owner can not be greater than or equal to inner table lock owner",
          K(ret),
          K(palf_lock_owner),
          K(inner_table_lock_owner));
    } else {
      need_lock = true;
    }
  } else {
    // If the palf is locked and the palf lock owner is not the same as the inner table lock owner, it need to report an
    // error code.
    if (palf_lock_owner != inner_table_lock_owner) {
      ret = OB_ERR_UNEXPECTED_LOCK_OWNER;
      LOG_WARN("palf lock owner and inner table not same", K(ret), K(palf_lock_owner), K(inner_table_lock_owner));
    } else {
      need_lock = false;
    }
  }
  return ret;
}

int ObMemberListLockUtils::check_unlock_status_(
    const bool palf_is_locked, const int64_t palf_lock_owner, const int64_t inner_table_lock_owner,
    bool &need_unlock, bool &need_relock_before_unlock)
{
  int ret = OB_SUCCESS;
  need_unlock = true;
  need_relock_before_unlock = false;
  if (!palf_is_locked) {
    // If the unlock_config_change operation has been executed before,
    // but the timeout is returned, the bottom layer may have been unlocked, and the row record in the internal table
    // has not been deleted. Therefore, when the lock owner of the palf record is greater than the lock owner of the
    // internal table, an error needs to be reported at this time.
    if (palf_lock_owner > inner_table_lock_owner) {
      ret = OB_ERR_UNEXPECTED_LOCK_OWNER;
      LOG_WARN("palf is not locked, and lock owner not match", K(ret), K(palf_lock_owner), K(inner_table_lock_owner));
    } else if (palf_lock_owner == inner_table_lock_owner) {
      need_relock_before_unlock = false;
      need_unlock = false;
    } else {
      LOG_INFO("palf lock owner is smaller than inner table lock owner, need relock before unlock", K(palf_lock_owner), K(inner_table_lock_owner));
      need_relock_before_unlock = true;
      need_unlock = true;
    }
  } else {
    // If the PALF layer has not been unlocked, if the lock owner recorded in the palf layer is different from the
    // lock owner recorded in the internal table, an error will be reported, otherwise it can be unlocked
    if (palf_lock_owner != inner_table_lock_owner) {
      ret = OB_ERR_UNEXPECTED_LOCK_OWNER;
      LOG_WARN("palf lock owner not match", K(ret), K(palf_lock_owner), K(inner_table_lock_owner));
    } else {
      need_unlock = true;
    }
  }
  return ret;
}

int ObMemberListLockUtils::relock_before_unlock_(const ObTransferTaskLockInfo &lock_info,
    const int64_t palf_lock_owner, const int64_t lock_timeout, const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_lock_config_change_(lock_info, lock_timeout, group_id))) {
    LOG_WARN("failed to try lock config change", K(ret), K(lock_info));
  } else {
    LOG_WARN("relock before unlock", K(ret), K(lock_info));
  }
  if (OB_SUCC(ret)) {
    SERVER_EVENT_ADD("TRANSFER_LOCK", "RELOCK_BEFORE_UNLOCK",
                    "tenant_id", lock_info.tenant_id_,
                    "ls_id", lock_info.ls_id_,
                    "status", lock_info.status_,
                    "inner_table_lock_owner", lock_info.lock_owner_,
                    "palf_lock_owner", palf_lock_owner,
                    "result", ret);
  }
  return ret;
}

// TODO(yangyi.yyy): change the use of storage rpc later
int ObMemberListLockUtils::init_storage_rpc_(
    obrpc::ObStorageRpcProxy &storage_svr_rpc_proxy,
    storage::ObStorageRpc &storage_rpc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(storage_svr_rpc_proxy.init(GCTX.net_frame_->get_req_transport(), GCTX.self_addr()))) {
    LOG_WARN("failed to init storage svr rpc proxy", K(ret));
  } else if (OB_FAIL(storage_rpc.init(&storage_svr_rpc_proxy, GCTX.self_addr(), GCTX.rs_rpc_proxy_))) {
    STORAGE_LOG(WARN, "fail to init partition service rpc", K(ret));
  }
  return ret;
}

void ObMemberListLockUtils::destory_storage_rpc_(
    obrpc::ObStorageRpcProxy &storage_svr_rpc_proxy,
    storage::ObStorageRpc &storage_rpc)
{
  storage_svr_rpc_proxy.destroy();
  storage_rpc.destroy();
}

int ObMemberListLockUtils::unlock_for_ob_admin(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t lock_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t lock_timeout = CONFIG_CHANGE_TIMEOUT;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool palf_is_locked = false;
  int64_t palf_lock_owner = -1;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || lock_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(lock_id));
  } else if (OB_FAIL(get_ls_handle(tenant_id, ls_id, ls_handle))) {
    LOG_WARN("failed to get ls handle", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_config_change_lock_stat(palf_lock_owner, palf_is_locked))) {
    LOG_WARN("failed to get config change lock stat", K(ret));
  } else if (!palf_is_locked) {
    //do nothing
  } else if (lock_id != palf_lock_owner) {
    ret = OB_ERR_UNEXPECTED_LOCK_OWNER;
    LOG_WARN("palf lock owner and inner table not same", K(ret), K(palf_lock_owner), K(lock_id));
  } else if (OB_FAIL(ls->unlock_config_change(palf_lock_owner, lock_timeout))) {
    LOG_WARN("failed to unlock config change", K(ret), K(palf_lock_owner));
  }

  LOG_INFO("unlock for ob admin", K(ret), K(tenant_id), K(ls_id), K(palf_is_locked), K(palf_lock_owner));
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
