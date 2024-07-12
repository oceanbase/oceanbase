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
#include "ob_storage_ha_diagnose_mgr.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "common/ob_role.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/access/ob_table_read_info.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_ALLOW_RETRY);
ERRSIM_POINT_DEF(EN_CHECK_LOG_NEED_REBUILD);

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
      sql_client, items, tablet_items_cnt, false/*include_larger_than*/, share::OBCG_STORAGE/*group_id*/,
      false/*with_order_by_field*/))) {
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
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
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

//TODO(yangyi.yyy) put this interface into tablet
int ObStorageHAUtils::get_sstable_read_info(
    const ObTablet &tablet,
    const ObITable::TableType &table_type,
    const bool is_normal_cg_sstable,
    const storage::ObITableReadInfo *&index_read_info)
{
  int ret = OB_SUCCESS;
  index_read_info = nullptr;
  if (!ObITable::is_table_type_valid(table_type) || !ObITable::is_sstable(table_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable read info get invalid argument", K(ret), K(tablet), K(table_type));
  } else {
    index_read_info = &tablet.get_rowkey_read_info();
    if (ObITable::is_mds_sstable(table_type)) {
      index_read_info = ObMdsSchemaHelper::get_instance().get_rowkey_read_info();
    } else if (!is_normal_cg_sstable) {
      //do nothing
    } else if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", K(ret), K(table_type), K(tablet));
    }
  }
  return ret;
}

int ObStorageHAUtils::check_replica_validity(const obrpc::ObFetchLSMetaInfoResp &ls_info)
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status;
  share::ObLSRestoreStatus restore_status;
  if (!ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ls_info));
  } else if (OB_FAIL(check_server_version(ls_info.version_))) {
    if (OB_MIGRATE_NOT_COMPATIBLE == ret) {
      LOG_WARN("this src is not compatible", K(ret), K(ls_info));
    } else {
      LOG_WARN("failed to check version", K(ret), K(ls_info));
    }
  } else if (OB_FAIL(ls_info.ls_meta_package_.ls_meta_.get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(ls_info));
  } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
    ret = OB_DATA_SOURCE_NOT_VALID;
    LOG_WARN("this src is not suitable, migration status check failed", K(ret), K(ls_info));
  } else if (OB_FAIL(ls_info.ls_meta_package_.ls_meta_.get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ls_info));
  } else if (restore_status.is_failed()) {
    ret = OB_DATA_SOURCE_NOT_EXIST;
    LOG_WARN("some ls replica restore failed, can not migrate", K(ret), K(ls_info));
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

#ifdef ERRSIM
  int tmp_ret = OB_SUCCESS;
  tmp_ret = EN_TRANSFER_ALLOW_RETRY ? : OB_SUCCESS;
  if (OB_TMP_FAIL(tmp_ret)) {
    bool_ret = false;
  }
#endif

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
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
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
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
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
  } else if (OB_FAIL(ls_srv->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
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

int ObStorageHAUtils::check_log_status(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    int32_t &result)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  bool is_log_sync = false;
  bool need_rebuild = false;
  bool has_fatal_error = false;
  result = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is not valid", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret), K(tenant_id), K(ls_id));
  } else {
    if (OB_FAIL(ls->get_log_handler()->is_replay_fatal_error(has_fatal_error))) {
      if (OB_EAGAIN == ret) {
        has_fatal_error = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to check replay fatal error", K(ret));
      }
    } else if (has_fatal_error) {
      result = OB_LOG_REPLAY_ERROR;
      LOG_WARN("log replay error", K(tenant_id), K(ls_id), K(result));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_SUCCESS != result) {
      //do nothing
    } else if (OB_FAIL(ls->get_log_handler()->is_in_sync(is_log_sync, need_rebuild))) {
      LOG_WARN("failed to get is_in_sync", K(ret), K(tenant_id), K(ls_id));
    } else if (need_rebuild) {
      result = OB_LS_NEED_REBUILD;
      LOG_WARN("ls need rebuild", K(tenant_id), K(ls_id), K(result));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    tmp_ret = EN_CHECK_LOG_NEED_REBUILD ? : OB_SUCCESS;
    if (OB_TMP_FAIL(tmp_ret)) {
      result = OB_LS_NEED_REBUILD;
      SERVER_EVENT_ADD("storage_ha", "check_log_need_rebuild",
                      "tenant_id", tenant_id,
                      "ls_id", ls_id.id(),
                      "result", result);
      DEBUG_SYNC(AFTER_CHECK_LOG_NEED_REBUILD);
    }
  }
#endif
  return ret;
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

void ObTransferUtils::add_transfer_error_diagnose_in_backfill(
    const share::ObLSID &dest_ls_id,
    const share::SCN &log_sync_scn,
    const int result_code,
    const common::ObTabletID &tablet_id,
    const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!dest_ls_id.is_valid()
      || !log_sync_scn.is_valid()
      || OB_SUCCESS == result_code
      || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(result_code), K(tablet_id), K(log_sync_scn));
  } else {
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    if (OB_FAIL(get_ls_migrate_status_(ls_handle, dest_ls_id, migration_status))) { //in migrate status
      LOG_WARN("fail to get migration status", K(ret), K(dest_ls_id));
    } else if (OB_FAIL(get_transfer_handler_(ls_handle, dest_ls_id, transfer_handler))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
    } else if (OB_FAIL(transfer_handler->record_error_diagnose_info_in_backfill(
          log_sync_scn, dest_ls_id, result_code, tablet_id, migration_status, result_msg))) {
      LOG_WARN("failed to record diagnose info", K(ret), K(log_sync_scn),
          K(dest_ls_id), K(result_code), K(tablet_id), K(migration_status), K(result_msg));
    }
  }
}

void ObTransferUtils::add_transfer_error_diagnose_in_replay(
    const share::ObTransferTaskID &task_id,
    const share::ObLSID &dest_ls_id,
    const int result_code,
    const bool clean_related_info,
    const share::ObStorageHADiagTaskType type,
    const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!task_id.is_valid()) {
    // do nothing
  } else if (!dest_ls_id.is_valid()
      || type < ObStorageHADiagTaskType::TRANSFER_START
      || type >= ObStorageHADiagTaskType::MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(type));
  } else if (OB_FAIL(get_transfer_handler_(ls_handle, dest_ls_id, transfer_handler))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
  } else if (OB_FAIL(transfer_handler->record_error_diagnose_info_in_replay(
        task_id, dest_ls_id, result_code, clean_related_info, type, result_msg))) {
    LOG_WARN("failed to record diagnose info", K(ret), K(dest_ls_id),
        K(task_id), K(result_code), K(clean_related_info), K(type), K(result_msg));
  }
}

void ObTransferUtils::set_transfer_related_info(
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTaskID &task_id,
    const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!dest_ls_id.is_valid() || !task_id.is_valid() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(dest_ls_id), K(task_id), K(start_scn));
  } else if (OB_FAIL(get_transfer_handler_(ls_handle, dest_ls_id, transfer_handler))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
  } else if (OB_FAIL(transfer_handler->set_related_info(task_id, start_scn))) {
    LOG_WARN("failed to set transfer related info", K(ret), K(dest_ls_id), K(task_id), K(start_scn));
  }
}

int ObTransferUtils::get_ls_(
    ObLSHandle &ls_handle,
    const share::ObLSID &dest_ls_id,
    ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ls = nullptr;
  if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(ls_service->get_ls(dest_ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(dest_ls_id), K(ls_handle));
  }
  return ret;
}

int ObTransferUtils::get_ls_migrate_status_(
    ObLSHandle &ls_handle,
    const share::ObLSID &dest_ls_id,
    ObMigrationStatus &migration_status)
{
  int ret = OB_SUCCESS;
  migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  ObLS *ls = nullptr;
  if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(get_ls_(ls_handle, dest_ls_id, ls))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("fail to get migration status", K(ret), K(dest_ls_id));
  }

  return ret;
}

int ObTransferUtils::get_transfer_handler_(
    ObLSHandle &ls_handle,
    const share::ObLSID &dest_ls_id,
    ObTransferHandler *&transfer_handler)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  transfer_handler = nullptr;
  if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(get_ls_(ls_handle, dest_ls_id, ls))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(dest_ls_id));
  } else {
    transfer_handler = ls->get_transfer_handler();
  }
  return ret;
}

void ObTransferUtils::reset_related_info(const share::ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(get_transfer_handler_(ls_handle, dest_ls_id, transfer_handler))) {
    LOG_WARN("failed to get ls", K(ls_handle), K(ret), K(dest_ls_id));
  } else {
    transfer_handler->reset_related_info();
  }
}

void ObTransferUtils::add_transfer_perf_diagnose_in_replay_(
    const share::ObStorageHAPerfDiagParams &params,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!params.is_valid()
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params), K(start_ts));
  } else if (OB_FAIL(get_transfer_handler_(ls_handle, params.dest_ls_id_, transfer_handler))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(params.dest_ls_id_));
  } else if (OB_FAIL(transfer_handler->record_perf_diagnose_info_in_replay(
        params, result, timestamp, start_ts, is_report))) {
    LOG_WARN("failed to record perf diagnose info",
        K(ret), K(params), K(result), K(timestamp), K(start_ts), K(is_report));
  }
}

void ObTransferUtils::add_transfer_perf_diagnose_in_backfill(
    const share::ObStorageHAPerfDiagParams &params,
    const share::SCN &log_sync_scn,
    const int result_code,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  ObTransferHandler *transfer_handler = nullptr;
  ObLSHandle ls_handle;
  if (!params.is_valid()
      || !log_sync_scn.is_valid()
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params), K(log_sync_scn), K(start_ts));
  } else {
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    if (OB_FAIL(get_ls_migrate_status_(ls_handle, params.dest_ls_id_, migration_status))) { //in migrate status
      LOG_WARN("fail to get migration status", K(ret), K(params.dest_ls_id_));
    } else if (OB_FAIL(get_transfer_handler_(ls_handle, params.dest_ls_id_, transfer_handler))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_handle), K(params.dest_ls_id_));
    } else if (OB_FAIL(transfer_handler->record_perf_diagnose_info_in_backfill(
        params, log_sync_scn, result_code, migration_status, timestamp, start_ts, is_report))) {
      LOG_WARN("failed to record perf diagnose info",
          K(ret), K(params), K(result_code), K(timestamp), K(start_ts), K(is_report));
    }
  }
}

void ObTransferUtils::construct_perf_diag_backfill_params_(
    const share::ObLSID &dest_ls_id,
    const common::ObTabletID &tablet_id,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName item_name,
    share::ObStorageHAPerfDiagParams &params)
{
  int ret = OB_SUCCESS;
  if (!dest_ls_id.is_valid()
      || !tablet_id.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || item_name >= ObStorageHACostItemName::MAX_NAME
      || item_name < ObStorageHACostItemName::TRANSFER_START_BEGIN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(tablet_id), K(item_type), K(item_name));
  } else {
    params.dest_ls_id_ = dest_ls_id;
    params.task_type_ = ObStorageHADiagTaskType::TRANSFER_BACKFILLED;
    params.item_type_ = item_type;
    params.name_ = item_name;
    params.tablet_id_ = tablet_id;
    params.tablet_count_ = 1;
  }
}

void ObTransferUtils::construct_perf_diag_replay_params_(
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTaskID &task_id,
    const share::ObStorageHADiagTaskType task_type,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName item_name,
    const int64_t tablet_count,
    share::ObStorageHAPerfDiagParams &params)
{
  int ret = OB_SUCCESS;
  if (!dest_ls_id.is_valid()
      || !task_id.is_valid()
      || task_type >= ObStorageHADiagTaskType::MAX_TYPE
      || task_type < ObStorageHADiagTaskType::TRANSFER_START
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || item_name >= ObStorageHACostItemName::MAX_NAME
      || item_name < ObStorageHACostItemName::TRANSFER_START_BEGIN
      || tablet_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(dest_ls_id), K(task_type), K(item_type), K(item_name), K(tablet_count));
  } else {
    params.dest_ls_id_ = dest_ls_id;
    params.task_id_ = task_id;
    params.task_type_ = task_type;
    params.item_type_ = item_type;
    params.name_ = item_name;
    params.tablet_count_ = tablet_count;
  }
}

void ObTransferUtils::process_backfill_perf_diag_info(
    const share::ObLSID &dest_ls_id,
    const common::ObTabletID &tablet_id,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName name,
    share::ObStorageHAPerfDiagParams &params)
{
  int ret = OB_SUCCESS;
  params.reset();
  if (!tablet_id.is_valid()
      || !dest_ls_id.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || name >= ObStorageHACostItemName::MAX_NAME
      || name < ObStorageHACostItemName::TRANSFER_START_BEGIN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(name), K(item_type), K(tablet_id));
  } else  {
    construct_perf_diag_backfill_params_(
                                  dest_ls_id,
                                  tablet_id,
                                  item_type,
                                  name,
                                  params);
  }
}

void ObTransferUtils::process_start_out_perf_diag_info(
    const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName name,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (!tx_start_transfer_out_info.task_id_.is_valid()) {
    // do nothing
  } else if (!tx_start_transfer_out_info.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || name >= ObStorageHACostItemName::MAX_NAME
      || name < ObStorageHACostItemName::TRANSFER_START_BEGIN
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_start_transfer_out_info), K(name), K(item_type), K(start_ts));
  } else {
    share::ObStorageHAPerfDiagParams params;
    construct_perf_diag_replay_params_(
                                    tx_start_transfer_out_info.dest_ls_id_,
                                    tx_start_transfer_out_info.task_id_,
                                    ObStorageHADiagTaskType::TRANSFER_START_OUT,
                                    item_type,
                                    name,
                                    tx_start_transfer_out_info.tablet_list_.count(),
                                    params);
    add_transfer_perf_diagnose_in_replay_(
                                    params,
                                    result,
                                    timestamp,
                                    start_ts,
                                    is_report);
  }
}

void ObTransferUtils::process_start_in_perf_diag_info(
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName name,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (!tx_start_transfer_in_info.task_id_.is_valid()) {
    // do nothing
  } else if (!tx_start_transfer_in_info.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || name >= ObStorageHACostItemName::MAX_NAME
      || name < ObStorageHACostItemName::TRANSFER_START_BEGIN
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_start_transfer_in_info), K(name), K(item_type), K(start_ts));
  } else {
    share::ObStorageHAPerfDiagParams params;
    construct_perf_diag_replay_params_(
                                    tx_start_transfer_in_info.dest_ls_id_,
                                    tx_start_transfer_in_info.task_id_,
                                    ObStorageHADiagTaskType::TRANSFER_START_IN,
                                    item_type,
                                    name,
                                    tx_start_transfer_in_info.tablet_meta_list_.count(),
                                    params);
    add_transfer_perf_diagnose_in_replay_(
                                    params,
                                    result,
                                    timestamp,
                                    start_ts,
                                    is_report);
  }
}

void ObTransferUtils::process_finish_in_perf_diag_info(
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName name,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (!tx_finish_transfer_in_info.task_id_.is_valid()) {
    // do nothing
  } else if (!tx_finish_transfer_in_info.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || name >= ObStorageHACostItemName::MAX_NAME
      || name < ObStorageHACostItemName::TRANSFER_START_BEGIN
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_finish_transfer_in_info), K(name), K(item_type), K(start_ts));
  } else {
    share::ObStorageHAPerfDiagParams params;
    construct_perf_diag_replay_params_(
                                    tx_finish_transfer_in_info.dest_ls_id_,
                                    tx_finish_transfer_in_info.task_id_,
                                    ObStorageHADiagTaskType::TRANSFER_FINISH_IN,
                                    item_type,
                                    name,
                                    tx_finish_transfer_in_info.tablet_list_.count(),
                                    params);
    add_transfer_perf_diagnose_in_replay_(
                                    params,
                                    result,
                                    timestamp,
                                    start_ts,
                                    is_report);
  }
}

void ObTransferUtils::process_finish_out_perf_diag_info(
    const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
    const share::ObStorageHACostItemType item_type,
    const share::ObStorageHACostItemName name,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (!tx_finish_transfer_out_info.task_id_.is_valid()) {
    // do nothing
  } else if (!tx_finish_transfer_out_info.is_valid()
      || item_type >= ObStorageHACostItemType::MAX_TYPE
      || item_type < ObStorageHACostItemType::ACCUM_COST_TYPE
      || name >= ObStorageHACostItemName::MAX_NAME
      || name < ObStorageHACostItemName::TRANSFER_START_BEGIN
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_finish_transfer_out_info), K(name), K(item_type), K(start_ts));
  } else {
    share::ObStorageHAPerfDiagParams params;
    construct_perf_diag_replay_params_(
                                    tx_finish_transfer_out_info.dest_ls_id_,
                                    tx_finish_transfer_out_info.task_id_,
                                    ObStorageHADiagTaskType::TRANSFER_FINISH_OUT,
                                    item_type,
                                    name,
                                    tx_finish_transfer_out_info.tablet_list_.count(),
                                    params);
    add_transfer_perf_diagnose_in_replay_(
                                    params,
                                    result,
                                    timestamp,
                                    start_ts,
                                    is_report);
  }
}

} // end namespace storage
} // end namespace oceanbase
