// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX STORAGE

#include "ob_storage_ha_utils.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_zone_merge_info.h"
#include "storage/tablet/ob_tablet.h"
#include "share/tablet/ob_tablet_table_operator.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/scn.h"
#include "share/ls/ob_ls_info.h"
#include "ob_storage_ha_struct.h"
#include "share/ls/ob_ls_table_operator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "share/ob_version.h"
#include "share/ob_cluster_version.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "src/observer/omt/ob_tenant_config.h"
#include "common/errsim_module/ob_errsim_module_type.h"
#include "common/ob_role.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

int ObStorageHAUtils::get_ls_leader(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  static const int64_t DEFAULT_CHECK_LS_LEADER_TIMEOUT = 1 * 60 * 1000 * 1000L;  // 1min
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else {
    uint32_t renew_count = 0;
    const uint32_t max_renew_count = 10;
    const int64_t retry_us = 200 * 1000;
    const int64_t start_ts = ObTimeUtility::current_time();
    do {
      if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader))) {
        if (OB_LS_LOCATION_NOT_EXIST == ret && renew_count++ < max_renew_count) {  // retry ten times
          LOG_WARN("failed to get location and force renew", K(ret), K(tenant_id), K(ls_id), K(cluster_id));
          if (OB_SUCCESS != (tmp_ret = GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
            LOG_WARN("failed to nonblock renew from location cache", K(tmp_ret), K(ls_id), K(cluster_id));
          } else if (ObTimeUtility::current_time() - start_ts > DEFAULT_CHECK_LS_LEADER_TIMEOUT) {
            renew_count = max_renew_count;
          } else {
            ob_usleep(retry_us);
          }
        }
      } else {
        LOG_INFO("get ls leader", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
      }
    } while (OB_LS_LOCATION_NOT_EXIST == ret && renew_count < max_renew_count);

    if (OB_SUCC(ret) && !leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(ls_id), K(leader), K(cluster_id));
    }
  }
  return ret;
}

int ObStorageHAUtils::check_tablet_replica_validity(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &src_addr, const common::ObTabletID &tablet_id, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  SCN compaction_scn;
  if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !src_addr.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(src_addr), K(tablet_id));
  } else if (OB_FAIL(check_merge_error_(tenant_id, sql_client))) {
    LOG_WARN("failed to check merge error", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(fetch_src_tablet_meta_info_(tenant_id, tablet_id, ls_id, src_addr, sql_client, compaction_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet may not has major sstable, no need check", K(tenant_id), K(tablet_id), K(ls_id), K(src_addr));
    } else {
      LOG_WARN("failed to fetch src tablet meta info", K(ret), K(tenant_id), K(tablet_id), K(ls_id), K(src_addr));
    }
  } else if (OB_FAIL(check_tablet_replica_checksum_(tenant_id, tablet_id, ls_id, compaction_scn, sql_client))) {
    LOG_WARN("failed to check tablet replica checksum", K(ret), K(tenant_id), K(tablet_id), K(ls_id), K(compaction_scn));
  }
  return ret;
}

int ObStorageHAUtils::get_server_version(uint64_t &server_version)
{
  int ret = OB_SUCCESS;
  server_version = CLUSTER_CURRENT_VERSION;
  return ret;
}

int ObStorageHAUtils::check_server_version(const uint64_t server_version)
{
  int ret = OB_SUCCESS;
  uint64_t cur_server_version = 0;
  if (OB_FAIL(get_server_version(cur_server_version))) {
    LOG_WARN("failed to get server version", K(ret));
  } else {
    bool can_migrate = cur_server_version >= server_version;
    if (!can_migrate) {
      ret = OB_MIGRATE_NOT_COMPATIBLE;
      LOG_WARN("migrate server not compatible", K(ret), K(server_version), K(cur_server_version));
    }
  }
  return ret;
}

int ObStorageHAUtils::report_ls_meta_table(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const storage::ObMigrationStatus &migration_status)
{
  int ret = OB_SUCCESS;
  share::ObLSReplica ls_replica;
  share::ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  const bool inner_table_only = false;
  if (OB_FAIL(GCTX.ob_service_->fill_ls_replica(tenant_id, ls_id, ls_replica))) {
    LOG_WARN("failed to fill ls replica", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(lst_operator->update(ls_replica, inner_table_only))) {
    LOG_WARN("failed to update ls meta table", K(ret), K(ls_replica));
  } else {
    SERVER_EVENT_ADD("storage_ha", "report_ls_meta_table",
                      "tenant_id", tenant_id,
                      "ls_id", ls_id,
                      "migration_status", migration_status);
    LOG_INFO("report ls meta table", K(ls_replica));
  }
  return ret;
}

int ObStorageHAUtils::check_merge_error_(const uint64_t tenant_id, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  share::ObGlobalMergeInfo merge_info;
  if (OB_FAIL(ObGlobalMergeTableOperator::load_global_merge_info(sql_client, tenant_id, merge_info))) {
    LOG_WARN("failed to laod global merge info", K(ret), K(tenant_id));
  } else if (merge_info.is_merge_error()) {
    ret = OB_CHECKSUM_ERROR;
    LOG_ERROR("merge error, can not migrate", K(ret), K(tenant_id), K(merge_info));
  }
  return ret;
}

int ObStorageHAUtils::fetch_src_tablet_meta_info_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const common::ObAddr &src_addr, common::ObISQLClient &sql_client, SCN &compaction_scn)
{
  int ret = OB_SUCCESS;
  ObTabletTableOperator op;
  ObTabletReplica tablet_replica;
  if (OB_FAIL(op.init(share::OBCG_STORAGE, sql_client))) {
    LOG_WARN("failed to init operator", K(ret));
  } else if (OB_FAIL(op.get(tenant_id, tablet_id, ls_id, src_addr, tablet_replica))) {
    LOG_WARN("failed to get tablet meta info", K(ret), K(tenant_id), K(tablet_id), K(ls_id), K(src_addr));
  } else if (OB_FAIL(compaction_scn.convert_for_tx(tablet_replica.get_snapshot_version()))) {
    LOG_WARN("failed to get tablet meta info", K(ret), K(compaction_scn), K(tenant_id), K(tablet_id), K(ls_id), K(src_addr));
  } else {/*do nothing*/}
  return ret;
}

int ObStorageHAUtils::check_tablet_replica_checksum_(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
    const share::ObLSID &ls_id, const SCN &compaction_scn, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletReplicaChecksumItem> items;
  ObArray<ObTabletLSPair> pairs;
  ObTabletLSPair pair;
  int64_t tablet_items_cnt = 0;
  if (OB_FAIL(pair.init(tablet_id, ls_id))) {
    LOG_WARN("failed to init pair", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(pairs.push_back(pair))) {
    LOG_WARN("failed to push back", K(ret), K(pair));
  } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_get(tenant_id, pairs, compaction_scn,
      sql_client, items, tablet_items_cnt, false/*include_larger_than*/, share::OBCG_STORAGE/*group_id*/))) {
    LOG_WARN("failed to batch get replica checksum item", K(ret), K(tenant_id), K(pairs), K(compaction_scn));
  } else {
    ObArray<share::ObTabletReplicaChecksumItem> filter_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObTabletReplicaChecksumItem &item = items.at(i);
      if (item.compaction_scn_ == compaction_scn) {
        if (OB_FAIL(filter_items.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < filter_items.count(); ++i) {
      const ObTabletReplicaChecksumItem &first_item = filter_items.at(0);
      const ObTabletReplicaChecksumItem &item = filter_items.at(i);
      if (OB_FAIL(first_item.verify_checksum(item))) {
        LOG_ERROR("failed to verify checksum", K(ret), K(tenant_id), K(tablet_id),
            K(ls_id), K(compaction_scn), K(first_item), K(item), K(filter_items));
      }
    }
  }
  return ret;
}

int ObStorageHAUtils::check_ls_deleted(
    const share::ObLSID &ls_id,
    bool &is_deleted)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  ObLSExistState state = ObLSExistState::MAX_STATE;
  is_deleted = false;

  // sys tenant should always return LS_NORMAL
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls status from inner table get invalid argument", K(ret), K(ls_id));
  } else if (!REACH_TENANT_TIME_INTERVAL(60 * 1000L * 1000L)) { //60s
    is_deleted = false;
  } else if (OB_FAIL(ObLocationService::check_ls_exist(tenant_id, ls_id, state))) {
    LOG_WARN("failed to check ls exist", K(ret), K(tenant_id), K(ls_id));
    //overwrite ret
    is_deleted = false;
    ret = OB_SUCCESS;
  } else if (state.is_deleted()) {
    is_deleted = true;
  } else {
    is_deleted = false;
  }
  return ret;
}

int ObStorageHAUtils::check_transfer_ls_can_rebuild(
    const share::SCN replay_scn,
    bool &need_rebuild)
{
  int ret = OB_SUCCESS;
  SCN readable_scn = SCN::base_scn();
  need_rebuild = false;
  if (!replay_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument invalid", K(ret), K(replay_scn));
  } else if (MTL_TENANT_ROLE_CACHE_IS_INVALID()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("tenant role is invalid, need retry", KR(ret), K(replay_scn));
  } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
    need_rebuild = true;
  } else if (OB_FAIL(get_readable_scn_(readable_scn))) {
    LOG_WARN("failed to get readable scn", K(ret), K(replay_scn));
  } else if (readable_scn >= replay_scn) {
    need_rebuild = true;
  } else {
    need_rebuild = false;
  }
  return ret;
}

int ObStorageHAUtils::get_readable_scn_with_retry(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  readable_scn.set_base();
  rootserver::ObTenantInfoLoader *info = MTL(rootserver::ObTenantInfoLoader*);
  const int64_t GET_READABLE_SCN_INTERVAL = 100 * 1000; // 100ms
  const int64_t GET_REABLE_SCN_TIMEOUT = 9 * 1000 * 1000; // 9s

  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info is null", K(ret), KP(info));
  } else {
    const int64_t start_ts = ObTimeUtility::current_time();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_readable_scn_(readable_scn))) {
        LOG_WARN("failed to get readable scn", K(ret));
        if (OB_EAGAIN == ret) {
          //overwrite ret
          if (ObTimeUtil::current_time() - start_ts >= GET_REABLE_SCN_TIMEOUT) {
            ret = OB_TIMEOUT;
            LOG_WARN("get valid readable scn timeout", K(ret), K(readable_scn));
          } else {
            ret = OB_SUCCESS;
            ob_usleep(GET_READABLE_SCN_INTERVAL);
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObStorageHAUtils::get_readable_scn_(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  readable_scn.set_base();
  rootserver::ObTenantInfoLoader *info = MTL(rootserver::ObTenantInfoLoader*);
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant info is null", K(ret), KP(info));
  } else if (OB_FAIL(info->get_readable_scn(readable_scn))) {
    LOG_WARN("failed to get readable scn", K(ret), K(readable_scn));
  } else if (!readable_scn.is_valid()) {
    ret = OB_EAGAIN;
    LOG_WARN("readable_scn not valid", K(ret), K(readable_scn));
  }
  return ret;
}

int ObStorageHAUtils::check_is_primary_tenant(const uint64_t tenant_id, bool &is_primary_tenant)
{
  int ret = OB_SUCCESS;
  is_primary_tenant = false;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check is primary tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(GCTX.sql_proxy_, tenant_id, is_primary_tenant))) {
    LOG_WARN("check is standby tenant failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObStorageHAUtils::check_disk_space()
{
  int ret = OB_SUCCESS;
  const int64_t required_size = 0;
  if (OB_FAIL(THE_IO_DEVICE->check_space_full(required_size))) {
    LOG_WARN("failed to check is disk full, cannot transfer in", K(ret));
  }
  return ret;
}

int ObStorageHAUtils::calc_tablet_sstable_macro_block_cnt(
    const ObTabletHandle &tablet_handle, int64_t &data_macro_block_count)
{
  int ret = OB_SUCCESS;
  data_macro_block_count = 0;
  storage::ObTableStoreIterator table_store_iter;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(table_store_iter))) {
    LOG_WARN("failed to get all tables", K(ret), K(tablet_handle));
  } else if (0 == table_store_iter.count()) {
    // do nothing
  } else {
    ObITable *table_ptr = NULL;
    while (OB_SUCC(ret)) {
      table_ptr = NULL;
      if (OB_FAIL(table_store_iter.get_next(table_ptr))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else if (!table_ptr->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is not sstable", K(ret), KPC(table_ptr));
      } else {
        data_macro_block_count += static_cast<blocksstable::ObSSTable *>(table_ptr)->get_data_macro_block_count();
      }
    }
  }
  return ret;
}

int ObStorageHAUtils::check_ls_is_leader(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_srv = NULL;
  common::ObRole role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  is_leader = false;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_srv = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    LOG_WARN("ls should not be null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), KP(ls));
  } else if (is_strong_leader(role)) {
    is_leader = true;
  } else {
    is_leader = false;
  }
  return ret;
}

int ObStorageHAUtils::check_tenant_will_be_deleted(
    bool &is_deleted)
{
  int ret = OB_SUCCESS;
  is_deleted = false;

  share::ObTenantBase *tenant_base = MTL_CTX();
  omt::ObTenant *tenant = nullptr;
  ObUnitInfoGetter::ObUnitStatus unit_status;
  if (OB_ISNULL(tenant_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant base should not be NULL", K(ret), KP(tenant_base));
  } else if (FALSE_IT(tenant = static_cast<omt::ObTenant *>(tenant_base))) {
  } else if (FALSE_IT(unit_status = tenant->get_unit_status())) {
  } else if (ObUnitInfoGetter::is_unit_will_be_deleted_in_observer(unit_status)) {
    is_deleted = true;
    FLOG_INFO("unit wait gc in observer, allow gc", K(tenant->id()), K(unit_status));
  }
  return ret;
}

bool ObTransferUtils::is_need_retry_error(const int err)
{
  bool bool_ret = false;
  //white list
  switch (err) {
  //Has active trans need retry
  case OB_TRANSFER_MEMBER_LIST_NOT_SAME:
  case OB_LS_LOCATION_LEADER_NOT_EXIST:
  case OB_PARTITION_NOT_LEADER:
  case OB_TRANS_TIMEOUT:
  case OB_TIMEOUT:
  case OB_EAGAIN:
  case OB_ERR_EXCLUSIVE_LOCK_CONFLICT:
      bool_ret = true;
      break;
    default:
      break;
  }
  return bool_ret;
}

int ObTransferUtils::block_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !gts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block tx get invalid argument", K(ret), K(tenant_id), K(ls_id), K(gts));
  } else if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(ls_handle));
  } else if (OB_FAIL(ls->ha_block_tx(gts))) {
    LOG_WARN("failed to kill all tx", K(ret), KPC(ls));
  } else {
    LOG_INFO("success to kill all tx", K(ret), K(gts));
  }
  return ret;
}

// TODO(yangyi.yyy): get gts before block and kill tx, unblock no need get gts
int ObTransferUtils::kill_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !gts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block tx get invalid argument", K(ret), K(tenant_id), K(ls_id), K(gts));
  } else if (OB_ISNULL(ls_svr = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls srv should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(ls_handle));
  } else if (OB_FAIL(ls->ha_kill_tx(gts))) {
    LOG_WARN("failed to kill all tx", K(ret), KPC(ls));
  } else {
    LOG_INFO("success to kill all tx", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTransferUtils::unblock_tx(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &gts)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_srv = NULL;
  ObLS *ls = NULL;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !gts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block tx get invalid argument", K(ret), K(tenant_id), K(ls_id), K(gts));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(ls_handle));
  } else if (OB_FAIL(ls->ha_unblock_tx(gts))) {
    LOG_WARN("failed to unblock tx", K(ret), K(tenant_id), K(ls_id), K(gts));
  }
  return ret;
}

int ObTransferUtils::get_gts(const uint64_t tenant_id, SCN &gts)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id));
  } else {
    ret = OB_EAGAIN;
    const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
    transaction::MonotonicTs unused_ts(0);
    const int64_t start_time = ObTimeUtility::fast_current_time();
    const int64_t TIMEOUT = 10 * 1000 * 1000; //10s
    while (OB_EAGAIN == ret) {
      if (ObTimeUtility::fast_current_time() - start_time > TIMEOUT) {
        ret = OB_TIMEOUT;
        LOG_WARN("get gts timeout", KR(ret), K(start_time), K(TIMEOUT));
      } else if (OB_FAIL(OB_TS_MGR.get_gts(tenant_id, stc, NULL, gts, unused_ts))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
        } else {
          // waiting 10ms
          ob_usleep(10L * 1000L);
        }
      }
    }
  }
  LOG_INFO("get tenant gts", KR(ret), K(tenant_id), K(gts));
  return ret;
}

int64_t ObStorageHAUtils::get_rpc_timeout()
{
  int64_t rpc_timeout = ObStorageRpcProxy::STREAM_RPC_TIMEOUT;
  int64_t tmp_rpc_timeout = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    tmp_rpc_timeout = tenant_config->_ha_rpc_timeout;
    rpc_timeout = std::max(rpc_timeout, tmp_rpc_timeout);
  }
  return rpc_timeout;
}

void ObTransferUtils::set_transfer_module()
{
#ifdef ERRSIM
  if (ObErrsimModuleType::ERRSIM_MODULE_NONE == THIS_WORKER.get_module_type().type_) {
    ObErrsimModuleType type(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
    THIS_WORKER.set_module_type(type);
  }
#endif
}

void ObTransferUtils::clear_transfer_module()
{
#ifdef ERRSIM
  if (ObErrsimModuleType::ERRSIM_MODULE_TRANSFER == THIS_WORKER.get_module_type().type_) {
    ObErrsimModuleType type(ObErrsimModuleType::ERRSIM_MODULE_NONE);
    THIS_WORKER.set_module_type(type);
  }
#endif
}

int ObTransferUtils::get_need_check_member(
    const common::ObIArray<ObAddr> &total_member_addr_list,
    const common::ObIArray<ObAddr> &finished_member_addr_list,
    common::ObIArray<ObAddr> &member_addr_list)
{
  int ret = OB_SUCCESS;
  member_addr_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < total_member_addr_list.count(); ++i) {
    const ObAddr &addr = total_member_addr_list.at(i);
    bool need_add = true;
    for (int64_t j = 0; OB_SUCC(ret) && j < finished_member_addr_list.count(); ++j) {
      if (finished_member_addr_list.at(j) == addr) {
        need_add = false;
        break;
      }
    }

    if (OB_SUCC(ret) && need_add) {
      if (OB_FAIL(member_addr_list.push_back(addr))) {
        LOG_WARN("failed to push addr into array", K(ret), K(addr));
      }
    }
  }

  return ret;
}

int ObTransferUtils::check_ls_replay_scn(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &check_scn,
    const int32_t group_id,
    const common::ObIArray<ObAddr> &member_addr_list,
    ObTimeoutCtx &timeout_ctx,
    common::ObIArray<ObAddr> &finished_addr_list)
{
  int ret = OB_SUCCESS;
  storage::ObFetchLSReplayScnProxy batch_proxy(
      *(GCTX.storage_rpc_proxy_), &obrpc::ObStorageRpcProxy::fetch_ls_replay_scn);
  ObFetchLSReplayScnArg arg;
  const int64_t timeout = 10 * 1000 * 1000; //10s
  const int64_t cluster_id = GCONF.cluster_id;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !check_scn.is_valid() || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer in tablet abort get invalid argument", K(ret), K(tenant_id), K(ls_id), K(check_scn), K(group_id));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_addr_list.count(); ++i) {
      const ObAddr &addr = member_addr_list.at(i);
      if (timeout_ctx.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("check transfer in tablet abort already timeout", K(ret), K(tenant_id), K(ls_id));
        break;
      } else if (OB_FAIL(batch_proxy.call(
          addr,
          timeout,
          cluster_id,
          arg.tenant_id_,
          group_id,
          arg))) {
        LOG_WARN("failed to send fetch ls replay scn request", K(ret), K(addr), K(tenant_id), K(ls_id));
      } else {
        LOG_INFO("fetch ls replay scn complete", K(arg), K(addr));
      }
    }

    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(batch_proxy.wait_all(return_code_array))) {
      LOG_WARN("fail to wait all batch result", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
    } else if (return_code_array.count() != member_addr_list.count()
        || return_code_array.count() != batch_proxy.get_results().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cnt not match", K(ret),
               "return_cnt", return_code_array.count(),
               "result_cnt", batch_proxy.get_results().count(),
               "server_cnt", member_addr_list.count());
    } else {
      ARRAY_FOREACH_X(batch_proxy.get_results(), idx, cnt, OB_SUCC(ret)) {
        const obrpc::ObFetchLSReplayScnRes *response = batch_proxy.get_results().at(idx);
        const int res_ret = return_code_array.at(idx);
        if (OB_SUCCESS != res_ret) {
          ret = res_ret;
          LOG_WARN("rpc execute failed", KR(ret), K(idx));
        } else if (OB_ISNULL(response)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("response is null", K(ret));
        } else if (response->replay_scn_ >= check_scn && OB_FAIL(finished_addr_list.push_back(member_addr_list.at(idx)))) {
          LOG_WARN("failed to push member addr into list", K(ret), K(idx), K(member_addr_list));
        }
      }
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
