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
#include "ob_storage_ha_src_provider.h"
#include "ob_storage_ha_utils.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

namespace oceanbase {
using namespace share;
namespace storage {

ObStorageHASrcProvider::ObStorageHASrcProvider()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    type_(ObMigrationOpType::MAX_LS_OP),
    storage_rpc_(nullptr)
{}

ObStorageHASrcProvider::~ObStorageHASrcProvider()
{}

int ObStorageHASrcProvider::init(const uint64_t tenant_id, const ObMigrationOpType::TYPE &type,
    storage::ObStorageRpc *storage_rpc)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("storage ha src provider init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(storage_rpc)
      || type < ObMigrationOpType::ADD_LS_OP || type >= ObMigrationOpType::MAX_LS_OP) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(tenant_id), K(type), KP(storage_rpc));
  } else {
    tenant_id_ = tenant_id;
    type_ = type;
    storage_rpc_ = storage_rpc;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHASrcProvider::choose_ob_src(const share::ObLSID &ls_id, const SCN &local_clog_checkpoint_scn,
    ObStorageHASrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  src_info.reset();
  common::ObAddr leader_addr;
  common::ObArray<common::ObAddr> addr_list;
  int64_t choose_member_idx = -1;
  ObAddr chosen_src_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(get_ls_leader_(tenant_id_, ls_id, leader_addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K_(tenant_id), K(ls_id));
  } else if (OB_FAIL(fetch_ls_member_list_(tenant_id_, ls_id, leader_addr, addr_list))) {
    LOG_WARN("failed to fetch ls leader member list", K(ret), K_(tenant_id), K(ls_id), K(leader_addr));
  } else if (OB_FAIL(inner_choose_ob_src_(tenant_id_, ls_id, local_clog_checkpoint_scn, addr_list, chosen_src_addr))) {
    LOG_WARN("failed to inner choose ob src", K(ret), K_(tenant_id), K(ls_id), K(local_clog_checkpoint_scn), K(addr_list));
  } else {
    src_info.src_addr_ = chosen_src_addr;
    src_info.cluster_id_ = GCONF.cluster_id;
#ifdef ERRSIM
    if (ObMigrationOpType::ADD_LS_OP == type_ || ObMigrationOpType::MIGRATE_LS_OP == type_) {
      const ObString &errsim_server = GCONF.errsim_migration_src_server_addr.str();
      if (!errsim_server.empty()) {
        common::ObAddr tmp_errsim_addr;
        if (OB_FAIL(tmp_errsim_addr.parse_from_string(errsim_server))) {
          LOG_WARN("failed to parse from string", K(ret), K(errsim_server));
        } else {
          src_info.src_addr_ = tmp_errsim_addr;
          src_info.cluster_id_ = GCONF.cluster_id;
          LOG_INFO("storage ha choose errsim src", K(tmp_errsim_addr));
        }
      }
    }
#endif
    SERVER_EVENT_ADD("storage_ha", "choose_src",
                     "tenant_id", tenant_id_,
                     "ls_id", ls_id.id(),
                     "src_addr", src_info.src_addr_,
                     "op_type", ObMigrationOpType::get_str(type_));
  }
  return ret;
}

int ObStorageHASrcProvider::get_ls_member_list(const uint64_t tenant_id,
    const share::ObLSID &ls_id, common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  common::ObAddr leader_addr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_ls_leader_(tenant_id_, ls_id, leader_addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K_(tenant_id), K(ls_id));
  } else if (OB_FAIL(fetch_ls_member_list_(tenant_id_, ls_id, leader_addr, addr_list))) {
    LOG_WARN("failed to fetch ls leader member list", K(ret), K_(tenant_id), K(ls_id), K(leader_addr));
  }
  return ret;
}

int ObStorageHASrcProvider::get_ls_leader_(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  leader.reset();
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObStorageHAUtils::get_ls_leader(tenant_id, ls_id, leader))) {
    LOG_WARN("failed to get ls leader", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObStorageHASrcProvider::fetch_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &leader_addr, common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !leader_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(leader_addr));
  } else if (OB_FAIL(get_ls_member_list_(tenant_id, ls_id, leader_addr, addr_list))) {
    LOG_WARN("failed to get ls member list", K(ret), K(tenant_id), K(ls_id), K(leader_addr));
  }
  return ret;
}

int ObStorageHASrcProvider::fetch_ls_meta_info_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &member_addr, obrpc::ObFetchLSMetaInfoResp &ls_meta_info)
{
  int ret = OB_SUCCESS;
  ls_meta_info.reset();
  ObStorageHASrcInfo src_info;
  src_info.src_addr_ = member_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !member_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(member_addr));
  } else if (OB_FAIL(storage_rpc_->post_ls_meta_info_request(tenant_id, src_info, ls_id, ls_meta_info))) {
    LOG_WARN("failed to post ls info request", K(ret), K(tenant_id), K(src_info), K(ls_id));
  }
  return ret;
}

int ObStorageHASrcProvider::inner_choose_ob_src_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const SCN &local_clog_checkpoint_scn, const common::ObIArray<common::ObAddr> &addr_list,
    common::ObAddr &choosen_src_addr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t choose_member_idx = -1;
  SCN max_clog_checkpoint_scn = SCN::min_scn();
  for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
    const common::ObAddr &addr = addr_list.at(i);
    obrpc::ObFetchLSMetaInfoResp ls_info;
    ObMigrationStatus migration_status;
    share::ObLSRestoreStatus restore_status;
    if (OB_TMP_FAIL(fetch_ls_meta_info_(tenant_id, ls_id, addr, ls_info))) {
      LOG_WARN("failed to fetch ls meta info", K(tmp_ret), K(tenant_id), K(ls_id), K(addr));
    } else if (OB_FAIL(ObStorageHAUtils::check_server_version(ls_info.version_))) {
      if (OB_MIGRATE_NOT_COMPATIBLE == ret) {
        LOG_INFO("do not choose this src", K(ret), K(tenant_id), K(ls_id), K(ls_info));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check version", K(ret), K(tenant_id), K(ls_id), K(ls_info));
      }
    } else if (!ObReplicaTypeCheck::is_full_replica(REPLICA_TYPE_FULL)) {
      LOG_INFO("do not choose this src", K(tenant_id), K(ls_id), K(addr), K(ls_info));
    } else if (local_clog_checkpoint_scn > ls_info.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn()) {
      LOG_INFO("do not choose this src", K(tenant_id), K(ls_id), K(addr), K(local_clog_checkpoint_scn), K(ls_info));
    } else if (OB_FAIL(ls_info.ls_meta_package_.ls_meta_.get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(ls_info));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      LOG_INFO("do not choose this src", K(tenant_id), K(ls_id), K(addr), K(ls_info));
    } else if (OB_FAIL(ls_info.ls_meta_package_.ls_meta_.get_restore_status(restore_status))) {
      LOG_WARN("failed to get restore status", K(ret), K(ls_info));
    } else if (restore_status.is_restore_failed()) {
      choose_member_idx = -1;
      LOG_INFO("some ls replica restore failed, can not migrate", K(ls_info));
      break;
    } else {
      if (ls_info.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn() > max_clog_checkpoint_scn) {
        max_clog_checkpoint_scn = ls_info.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn();
        choose_member_idx = i;
      } else if (ls_info.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn() == max_clog_checkpoint_scn
          && !ls_info.has_transfer_table_) {
        max_clog_checkpoint_scn = ls_info.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn();
        choose_member_idx = i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (-1 == choose_member_idx) {
      ret = OB_DATA_SOURCE_NOT_EXIST;
      LOG_WARN("no available data source exist", K(ret), K(tenant_id), K(ls_id), K(addr_list));
    } else {
      choosen_src_addr = addr_list.at(choose_member_idx);
    }
  }
  return ret;
}

int ObStorageHASrcProvider::get_ls_member_list_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObAddr &leader_addr,
    common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  addr_list.reset();
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObStorageHASrcInfo src_info;
  src_info.src_addr_ = leader_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  obrpc::ObFetchLSMemberListInfo member_info;
  ObLSService *ls_service = nullptr;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !leader_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls member list get invalid argument", K(ret), K(tenant_id), K(ls_id), K(leader_addr));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(storage_rpc_->post_ls_member_list_request(tenant_id, src_info, ls_id, member_info))) {
    LOG_WARN("failed to post ls member list request", K(ret), K(tenant_id), K(src_info), K(ls_id));
    //overwrite ret
    member_info.reset();
    if (OB_FAIL(ls->get_log_handler()->get_election_leader(src_info.src_addr_))) {
      LOG_WARN("failed to get election leader", K(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(storage_rpc_->post_ls_member_list_request(tenant_id, src_info, ls_id, member_info))) {
      LOG_WARN("failed to post ls member list request", K(ret), K(tenant_id), K(src_info), K(ls_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(member_info.member_list_.get_addr_array(addr_list))) {
    LOG_WARN("failed to get addr array", K(ret), K(member_info));
  } else {
    FLOG_INFO("fetch ls member list", K(tenant_id), K(ls_id), K(src_info), K(member_info));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
