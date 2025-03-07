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

#include "ob_zone_storage_manager.h"
#include "deps/oblib/src/lib/container/ob_iarray.h"

#include "rootserver/ob_root_service.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_zone_table_operation.h"
#include "share/object_storage/ob_zone_storage_table_operation.h"
#include "share/object_storage/ob_device_connectivity.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;

namespace rootserver
{
///////////////////////////ObZoneStorageManagerBase///////////////////////////
ObZoneStorageManagerBase::ObZoneStorageManagerBase()
  : lock_(ObLatchIds::ZONE_STORAGE_INFO_RW_LOCK),
    inited_(false),
    loaded_(false),
    zone_storage_infos_(),
    proxy_(NULL),
    srv_rpc_proxy_(NULL)
{
}

ObZoneStorageManagerBase::~ObZoneStorageManagerBase() {}

void ObZoneStorageManagerBase::reset_zone_storage_infos() { zone_storage_infos_.reset(); }

int ObZoneStorageManagerBase::init(ObMySQLProxy &proxy, obrpc::ObSrvRpcProxy &srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    zone_storage_infos_.reset();
    proxy_ = &proxy;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    inited_ = true;
    loaded_ = false;
  }
  return ret;
}

int ObZoneStorageManagerBase::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner_stat_error", K_(inited), K_(loaded), KR(ret));
  }
  return ret;
}

int ObZoneStorageManagerBase::get_zone_storage_count(int64_t &zone_storage_count) const
{
  int ret = OB_SUCCESS;
  zone_storage_count = 0;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    zone_storage_count = zone_storage_infos_.count();
  }
  return ret;
}

int ObZoneStorageManagerBase::check_zone_storage_exist(const ObZone &zone,
                                                       const ObBackupDest &storage_dest,
                                                       const ObStorageUsedType::TYPE used_for,
                                                       int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    // Under the same zone, used_for_type ALL is used for DATA and LOG
    for (int64_t i = 0; i < zone_storage_infos_.count(); ++i) {
      if (zone == zone_storage_infos_.at(i).zone_ &&
          (used_for == zone_storage_infos_.at(i).used_for_ ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == zone_storage_infos_.at(i).used_for_) &&
          (0 == STRNCMP(storage_dest.get_root_path().ptr(), zone_storage_infos_.at(i).dest_attr_.path_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.path_))) &&
          (0 == STRNCMP(storage_dest.get_storage_info()->endpoint_,
                        zone_storage_infos_.at(i).dest_attr_.endpoint_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.endpoint_)))) {
        idx = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::check_zone_storage_exist(const ObZone &zone,
                                                       const ObString &storage_path,
                                                       const ObStorageUsedType::TYPE used_for,
                                                       int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(ObStorageInfoOperator::parse_storage_path(
               storage_path.ptr(), path, sizeof(path), endpoint, sizeof(endpoint)))) {
    LOG_WARN("failed to parse storage path", KR(ret), K(storage_path));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    // Under the same zone, used_for_type ALL is used for DATA and LOG
    for (int64_t i = 0; i < zone_storage_infos_.count(); ++i) {
      if (zone == zone_storage_infos_.at(i).zone_ &&
          (used_for == zone_storage_infos_.at(i).used_for_ ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == zone_storage_infos_.at(i).used_for_) &&
          (0 == STRNCMP(path, zone_storage_infos_.at(i).dest_attr_.path_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.path_))) &&
          (0 == STRNCMP(endpoint, zone_storage_infos_.at(i).dest_attr_.endpoint_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.endpoint_)))) {
        idx = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::check_add_storage_access_info_equal(
  const ObBackupDest &storage_dest) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); ++i) {
      if ((0 == STRNCMP(storage_dest.get_root_path().ptr(), zone_storage_infos_.at(i).dest_attr_.path_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.path_))) &&
          (0 == STRNCMP(storage_dest.get_storage_info()->endpoint_,
                        zone_storage_infos_.at(i).dest_attr_.endpoint_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.endpoint_)))) {
        ObBackupDest exist_storage_dest;
        if (OB_FAIL(exist_storage_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                           zone_storage_infos_.at(i).dest_attr_.endpoint_,
                                           zone_storage_infos_.at(i).dest_attr_.authorization_,
                                           zone_storage_infos_.at(i).dest_attr_.extension_))) {
          LOG_WARN("failed to set storage dest", KR(ret), K(zone_storage_infos_.at(i)));
        } else if (!(storage_dest.get_storage_info()->is_access_info_equal(
                     *(exist_storage_dest.get_storage_info())))) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cannot support different access info for the same path and endpoint", KR(ret),
                   K(storage_dest), K(exist_storage_dest));
          LOG_USER_ERROR(
            OB_NOT_SUPPORTED,
            "cannot support different access info for the same path and endpoint, it is");
        } else {
          LOG_INFO("storage dest access info is equal for the same path and endpoint",
                   K(storage_dest), K(zone_storage_infos_.at(i)));
        }
        break;
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::get_zone_storage_list_by_zone(
  const common::ObZone &zone, common::ObArray<int64_t> &drop_zone_storage_list) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); ++i) {
    if (zone == zone_storage_infos_.at(i).zone_) {
      if (OB_FAIL(drop_zone_storage_list.push_back(i))) {
        LOG_WARN("failed to push back", KR(ret), K(i), K(drop_zone_storage_list));
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::add_storage(const ObString &storage_path, const ObString &access_info,
                                          const ObString &attribute, const ObStorageUsedType::TYPE &use_for,
                                          const ObZone &zone, const bool &wait_type)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  SMART_VAR(share::ObZoneInfo, zone_info) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, compat_version))) {
      LOG_WARN("get min data_version failed", K(ret), K(OB_SYS_TENANT_ID));
    } else if (compat_version < DATA_VERSION_4_3_3_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("server state is not suppported when tenant's data version is below 4.3.3.0", KR(ret), K(compat_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.3.0, add storage is ");
    } else if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check_inner_stat failed", KR(ret));
    } else if (OB_UNLIKELY(storage_path.empty() || zone.is_empty() || attribute.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(storage_path), K(zone), K(attribute));
    } else if (ObStorageUsedType::TYPE::USED_TYPE_ALL != use_for) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("adding storage only supports used for all", KR(ret), K(use_for));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "adding storage only supports used for all");
    } else if (OB_FAIL(ObZoneTableOperation::get_zone_info(zone, *proxy_, zone_info))) {
      LOG_WARN("failed get zone, zone not exist", "zone", zone, KR(ret));
    } else {
      ObBackupDest storage_dest;
      char storage_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = {0};
      ObRegion region;
      if (!access_info.empty()) {
        if (OB_FAIL(databuff_printf(storage_dest_str, OB_MAX_BACKUP_DEST_LENGTH, "%s&%s",
                                          storage_path.ptr(), access_info.ptr()))) {
          LOG_WARN("failed to set storage_dest_str", KR(ret), K(storage_path));
        }
      } else {
        if (OB_FAIL(databuff_printf(storage_dest_str, OB_MAX_BACKUP_DEST_LENGTH, "%s", storage_path.ptr()))) {
          LOG_WARN("failed to set path", KR(ret), K(storage_path));
        }
      }
      ObDeviceConnectivityCheckManager device_conn_check_mgr;
      int64_t max_iops = 0;
      int64_t max_bandwidth = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(parse_attribute_str(attribute, max_iops, max_bandwidth))) {
        LOG_WARN("fail to parse attribute str", KR(ret), K(attribute));
      } else if (OB_FAIL(storage_dest.set(storage_dest_str))) {
        LOG_WARN("failed to set storage dest", KR(ret), K(storage_dest));
      } else if (OB_FAIL(device_conn_check_mgr.check_device_connectivity(storage_dest))) {
        LOG_WARN("fail to check device connectivity", KR(ret), K(storage_dest));
      } else if (OB_FAIL(zone_info.get_region(region))) {
        LOG_WARN("failed to get region from zone_info", KR(ret), K(zone_info), K(zone));
      } else if (OB_FAIL(check_zone_storage_with_region_scope(region, use_for, storage_dest))) {
        LOG_WARN("failed to check zone storage with region scope", KR(ret), K(zone), K(region), K(storage_dest));
      } else if (ObStorageType::OB_STORAGE_FILE != storage_dest.get_storage_info()->device_type_ &&
                 OB_FAIL(check_add_storage_access_info_equal(storage_dest))) {
        LOG_WARN("failed to check add storage access info equal", KR(ret), K(storage_dest));
      } else if (OB_FAIL(add_storage_operation(storage_dest, use_for, zone, wait_type, max_iops, max_bandwidth))) {
        LOG_WARN("failed to add storage", KR(ret), K(storage_dest), K(use_for), K(zone),
                K(wait_type), K(max_iops), K(max_bandwidth));
      } else {
        LOG_INFO("succeed to add storage", K(storage_path), K(use_for), K(zone),
                K(wait_type), K(max_iops), K(max_bandwidth));
        ROOTSERVICE_EVENT_ADD("storage", "add_storage", "path", storage_path, "use_for", use_for, "zone", zone,
                              "wait_type", wait_type, "max_iops", max_iops, "max_bandwidth", max_bandwidth);
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::add_storage_operation(const ObBackupDest &storage_dest,
                                                    const ObStorageUsedType::TYPE &used_for,
                                                    const ObZone &zone, const bool &wait_type,
                                                    const int64_t max_iops, const int64_t max_bandwidth)
{
  UNUSED(wait_type);
  int ret = OB_SUCCESS;
  char storage_dest_info[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  int64_t idx = OB_INVALID_INDEX;
  uint64_t op_id = OB_INVALID_ID;
  uint64_t storage_id = OB_INVALID_ID;
  bool is_need_fetch_storage_id = true;
  uint64_t sub_op_id = OB_INVALID_ID;
  ObZoneStorageState::STATE op_type = ObZoneStorageState::ADDING;
  int64_t pos = 0;
  if (!storage_dest.is_valid() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for) ||
      max_iops < 0 || max_bandwidth < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest), K(zone), K(used_for), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(
               storage_dest.get_backup_dest_str(storage_dest_info, sizeof(storage_dest_info)))) {
    LOG_WARN("failed to get storage info", KR(ret), K(storage_dest));
  } else if (FALSE_IT(pos = STRLEN(storage_dest_info))) {
  } else if (OB_FAIL(databuff_printf(storage_dest_info, sizeof(storage_dest_info), pos,
                     "&%s%ld&%s%ld", STORAGE_MAX_IOPS, max_iops, STORAGE_MAX_BANDWIDTH, max_bandwidth))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(pos), K(max_iops), K(max_bandwidth));
  } else if (OB_SUCCESS == (ret = check_zone_storage_exist(zone, storage_dest, used_for, idx))) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("zone storage is already exist, cannot add", KR(ret), K(zone), K(storage_dest),
             K(used_for));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("check_zone_storage_exist failed", KR(ret), K(zone), K(storage_dest), K(used_for));
  } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, op_id))) {
    LOG_WARN("failed to fetch new storage op id", KR(ret), K(op_id));
  } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, sub_op_id))) {
    LOG_WARN("failed to fetch new storage sub op id", KR(ret), K(sub_op_id));
  } else if (OB_FAIL(check_need_fetch_storage_id(storage_dest, is_need_fetch_storage_id, storage_id))) {
    LOG_WARN("fail to check need fetch storage id", KR(ret), K(is_need_fetch_storage_id), K(storage_id));
  } else if (is_need_fetch_storage_id && OB_FAIL(ObStorageInfoOperator::fetch_new_storage_id(*proxy_, storage_id))) {
    LOG_WARN("failed to fetch new storage id", KR(ret), K(storage_id));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(
               authorization, sizeof(authorization)))) {
    LOG_WARN("failed to get authorization", KR(ret), K(storage_dest));
  } else {
    SMART_VAR(share::ObZoneStorageTableInfo, new_zone_storage_info) {
      new_zone_storage_info.zone_ = zone;
      MEMCPY(new_zone_storage_info.dest_attr_.path_,
            storage_dest.get_root_path().ptr(), storage_dest.get_root_path().length());
      MEMCPY(new_zone_storage_info.dest_attr_.endpoint_,
            storage_dest.get_storage_info()->endpoint_,
            sizeof(storage_dest.get_storage_info()->endpoint_));
      MEMCPY(new_zone_storage_info.dest_attr_.extension_,
            storage_dest.get_storage_info()->extension_,
            sizeof(storage_dest.get_storage_info()->extension_));
      MEMCPY(new_zone_storage_info.dest_attr_.authorization_, authorization,
            sizeof(authorization));
      new_zone_storage_info.used_for_ = used_for;
      new_zone_storage_info.storage_id_ = storage_id;
      new_zone_storage_info.op_id_ = op_id;
      new_zone_storage_info.sub_op_id_ = sub_op_id;
      new_zone_storage_info.state_ = op_type;
      new_zone_storage_info.max_iops_ = max_iops;
      new_zone_storage_info.max_bandwidth_ = max_bandwidth;
      ObMySQLTransaction trans_adding;
      if (OB_FAIL(zone_storage_infos_.push_back(new_zone_storage_info))) {
        LOG_WARN("fail to push back new_zone_storage_info", KR(ret), K(new_zone_storage_info));
      } else if (OB_FAIL(trans_adding.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", KR(ret));
        // locked the service epoch to make storage operation exclusive with server operation
      } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans_adding))) {
        LOG_WARN("failed to check and update service epoch", KR(ret));
      } else if (OB_FAIL(ObStorageInfoOperator::select_for_update(trans_adding, zone))) {
        LOG_WARN("failed to select for update", KR(ret), K(zone));
      } else if (OB_FAIL(ObStorageInfoOperator::insert_storage(trans_adding, storage_dest, used_for,
                         zone, op_type, storage_id, op_id, max_iops, max_bandwidth))) {
        LOG_WARN("failed to insert zone storage table", KR(ret), K(storage_dest), K(used_for),
                K(zone), K(op_type), K(storage_id), K(op_id));
      } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                  trans_adding, storage_id, op_id, sub_op_id, zone, op_type, storage_dest_info))) {
        LOG_WARN("failed to insert zone storage operation table", KR(ret), K(storage_id), K(op_id),
                 K(sub_op_id), K(zone), K(op_type), K(storage_dest_info));
      }
      int tmp_ret = trans_adding.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", KR(ret), K(tmp_ret));
        ret = (OB_SUCCESS == ret ? tmp_ret : ret);
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::drop_storage(const ObString &storage_path,
                                           const ObStorageUsedType::TYPE &use_for,
                                           const ObZone &zone, const bool &force_type,
                                           const bool &wait_type)
{
  UNUSED(force_type);
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(OB_SYS_TENANT_ID));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("server state is not suppported when tenant's data version is below 4.3.3.0", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.3.0, drop storage is ");
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_UNLIKELY(storage_path.empty() || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_path), K(zone));
  } else {
    if (OB_FAIL(drop_storage_operation(storage_path, use_for, zone, wait_type))) {
      LOG_WARN("failed to drop storage", KR(ret), K(storage_path), K(zone), K(use_for),
               K(wait_type));
    } else {
      LOG_INFO("succeed to drop storage", K(storage_path), K(zone), K(use_for), K(wait_type));
      ROOTSERVICE_EVENT_ADD("storage", "drop_storage", "storage_path", storage_path, "zone", zone,
                            "used_for", use_for, "wait_type", wait_type);
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::drop_storage_operation(const ObString &storage_path,
                                                     const ObStorageUsedType::TYPE &used_for,
                                                     const ObZone &zone, const bool &wait_type)
{
  UNUSED(wait_type);
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  uint64_t op_id = OB_INVALID_ID;
  uint64_t old_op_id = OB_INVALID_ID;
  uint64_t storage_id = OB_INVALID_ID;
  uint64_t sub_op_id = OB_INVALID_ID;
  ObZoneStorageState::STATE op_type = ObZoneStorageState::DROPPING;
  if (storage_path.empty() || zone.is_empty() || !ObStorageUsedType::is_valid(used_for)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(zone));
  } else if (OB_FAIL(check_zone_storage_exist(zone, storage_path, used_for, idx))) {
    LOG_WARN("check zone storage exist failed", K(zone), K(storage_path), K(used_for), KR(ret));
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(idx), KR(ret));
  } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, op_id))) {
    LOG_WARN("failed to fetch new storage op id", KR(ret), K(op_id));
  } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, sub_op_id))) {
    LOG_WARN("failed to fetch new storage sub op id", KR(ret), K(sub_op_id));
  } else {
    old_op_id = zone_storage_infos_.at(idx).op_id_;
    zone_storage_infos_.at(idx).state_ = op_type;
    zone_storage_infos_.at(idx).op_id_ = op_id;
    zone_storage_infos_.at(idx).sub_op_id_ = sub_op_id;
    ObMySQLTransaction trans_dropping;
    bool zone_empty = false;
    ObArray<ObServerStatus> servers_status;
    if (OB_FAIL(trans_dropping.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans_dropping))) {
      LOG_WARN("failed to check and update service epoch", KR(ret));
    } else if (OB_FAIL(ObServerTableOperator::get(trans_dropping, zone, servers_status))) {
      LOG_WARN("failed to check zone empty", KR(ret), K(zone));
    } else if (!servers_status.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("zone not empty", KR(ret), K(zone), K(servers_status));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "drop storage of non-empty zone");
    } else if (OB_FAIL(ObStorageInfoOperator::update_storage_state(
                 trans_dropping, zone, storage_path, used_for, old_op_id, op_type))) {
      LOG_WARN("failed to update zone storage state", KR(ret), K(op_type));
    } else if (OB_FAIL(ObStorageInfoOperator::update_storage_op_id(
                 trans_dropping, zone, storage_path, used_for, old_op_id, op_id))) {
      LOG_WARN("failed to update zone storage op id", KR(ret), K(op_id));
    } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                 trans_dropping, zone_storage_infos_.at(idx).storage_id_, op_id, sub_op_id,
                 zone, op_type, storage_path.ptr()))) {
      LOG_WARN("failed to insert zone storage operation table", KR(ret),
               K(zone_storage_infos_.at(idx).storage_id_), K(op_id), K(sub_op_id),
               K(zone), K(op_type), K(storage_path));
    }
    int tmp_ret = trans_dropping.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::alter_storage(const ObString &storage_path, const ObString &access_info,
                                            const ObString &attribute, const bool &wait_type)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(OB_SYS_TENANT_ID));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("server state is not suppported when tenant's data version is below 4.3.3.0", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.3.0, alter storage is ");
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_UNLIKELY(storage_path.empty() || (access_info.empty() && attribute.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_path), K(attribute));
  } else if (!access_info.empty() && OB_FAIL(ObStorageDestCheck::check_change_storage_accessinfo_exist(
               *proxy_, storage_path, access_info))) {
    LOG_WARN("failed to check change storage accessinfo", KR(ret));
  } else if (!attribute.empty() && OB_FAIL(ObStorageDestCheck::check_change_storage_attribute_exist(
               *proxy_, storage_path, attribute))) {
    LOG_WARN("failed to check change storage attribute", KR(ret));
  } else {
    if (!access_info.empty()) {
      ObBackupDest storage_dest;
      ObDeviceConnectivityCheckManager device_conn_check_mgr;
      char storage_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = {0};
      if (OB_FAIL(databuff_printf(storage_dest_str, OB_MAX_BACKUP_DEST_LENGTH, "%s&%s", storage_path.ptr(),
                                  access_info.ptr()))) {
        LOG_WARN("failed to set storage_dest_str", KR(ret));
      } else if (OB_FAIL(storage_dest.set(storage_dest_str))) {
        LOG_WARN("failed to set dest", KR(ret), K(storage_dest));
      } else if (OB_FAIL(device_conn_check_mgr.check_device_connectivity(storage_dest))) {
        LOG_WARN("fail to check device connectivity", KR(ret), K(storage_dest));
      } else if (OB_FAIL(alter_storage_authorization(storage_dest, wait_type))) {
        LOG_WARN("failed to alter storage authorization", KR(ret), K(storage_dest), K(wait_type));
      } else {
        LOG_INFO("succeed to alter storage authorization", K(storage_path), K(wait_type));
        ROOTSERVICE_EVENT_ADD("storage", "alter storage authorization", "path", storage_path, "wait_type", wait_type);
      }
    }
    if (OB_SUCC(ret) && !attribute.empty()) {
      int64_t max_iops = OB_INVALID_MAX_IOPS;
      int64_t max_bandwidth = OB_INVALID_MAX_BANDWIDTH;
      if (OB_FAIL(parse_attribute_str(attribute, max_iops, max_bandwidth))) {
        LOG_WARN("fail to parse attribute str", KR(ret), K(attribute));
      } else if (OB_FAIL(alter_storage_attribute(storage_path, wait_type, max_iops, max_bandwidth))) {
        LOG_WARN("failed to alter storage attribute", KR(ret), K(storage_path), K(wait_type), K(attribute));
      } else {
        LOG_INFO("succeed to alter storage attribute", K(storage_path), K(wait_type), K(attribute));
        ROOTSERVICE_EVENT_ADD("storage", "alter storage attribute", "path", storage_path, "wait_type", wait_type, "attribute", attribute);
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::alter_storage_authorization(const ObBackupDest &storage_dest,
                                                          const bool &wait_type)
{
  UNUSED(wait_type);
  int ret = OB_SUCCESS;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  char old_authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  if (!storage_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(
               authorization, sizeof(authorization)))) {
    LOG_WARN("failed to get authorization", KR(ret), K(storage_dest));
  } else {
    ObMySQLTransaction trans_changing;
    if (OB_FAIL(trans_changing.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans_changing))) {
      LOG_WARN("failed to check and update service epoch", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); i++) {
      ObBackupDest table_dest;
      bool is_equal = false;
      if (OB_FAIL(table_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                 zone_storage_infos_.at(i).dest_attr_.endpoint_,
                                 zone_storage_infos_.at(i).dest_attr_.authorization_,
                                 zone_storage_infos_.at(i).dest_attr_.extension_))) {
        LOG_WARN("failed to set storage dest", KR(ret), K(zone_storage_infos_.at(i)));
      } else if (storage_dest.is_backup_path_equal(table_dest, is_equal)) {
        LOG_WARN("fail to compare backup path", KR(ret), K(table_dest), K(storage_dest), K(is_equal));
      } else if (is_equal) {
        ObBackupDest target_storage_dest;
        uint64_t op_id = OB_INVALID_ID;
        uint64_t old_op_id = OB_INVALID_ID;
        uint64_t sub_op_id = OB_INVALID_ID;
        ObZoneStorageState::STATE op_type = ObZoneStorageState::CHANGING;
        common::ObZone zone = zone_storage_infos_.at(i).zone_;
        ObStorageUsedType::TYPE used_for = zone_storage_infos_.at(i).used_for_;
        uint64_t storage_id = zone_storage_infos_.at(i).storage_id_;
        if (ObZoneStorageState::ADDED != zone_storage_infos_.at(i).state_ &&
            ObZoneStorageState::CHANGED != zone_storage_infos_.at(i).state_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cannot support changing current storage state when undergoing modification", KR(ret),
                   K(zone_storage_infos_.at(i)), K(i));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "cannot support changing current storage state when undergoing modification, it is");
        } else if (OB_FAIL(target_storage_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                                   storage_dest.get_storage_info()))) {
          LOG_WARN("failed to set storage dest", KR(ret), K(storage_dest),
                   K(zone_storage_infos_.at(i)));
        } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, op_id))) {
          LOG_WARN("failed to fetch new storage op id", KR(ret), K(op_id));
        } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, sub_op_id))) {
          LOG_WARN("failed to fetch new storage sub op id", KR(ret), K(sub_op_id));
        } else {
          old_op_id = zone_storage_infos_.at(i).op_id_;
          MEMCPY(old_authorization, zone_storage_infos_.at(i).dest_attr_.authorization_,
                 sizeof(zone_storage_infos_.at(i).dest_attr_.authorization_));
          MEMCPY(zone_storage_infos_.at(i).dest_attr_.authorization_, authorization,
                 sizeof(authorization));
          zone_storage_infos_.at(i).state_ = op_type;
          zone_storage_infos_.at(i).op_id_ = op_id;
          zone_storage_infos_.at(i).sub_op_id_ = sub_op_id;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObStorageInfoOperator::update_storage_authorization(
                       trans_changing, zone, target_storage_dest, old_op_id, used_for))) {
            LOG_WARN("failed to update storage authorization", KR(ret), K(target_storage_dest));
          } else if (OB_FAIL(ObStorageInfoOperator::update_storage_state(
                       trans_changing, zone, target_storage_dest, used_for, old_op_id, op_type))) {
            LOG_WARN("failed to update zone storage state", KR(ret), K(op_type));
          } else if (OB_FAIL(ObStorageInfoOperator::update_storage_op_id(
                       trans_changing, zone, target_storage_dest, used_for, old_op_id, op_id))) {
            LOG_WARN("failed to update zone storage op id", KR(ret), K(op_id));
          } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                       trans_changing, storage_id, op_id, sub_op_id, zone, op_type, old_authorization))) {
            LOG_WARN("failed to insert zone storage operation table", KR(ret), K(storage_id),
                     K(op_id), K(sub_op_id), K(zone), K(op_type), K(old_authorization));
          }
        }
      }
    }
    int tmp_ret = trans_changing.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::alter_storage_attribute(const ObString &storage_path, const bool &wait_type,
                                                      const int64_t max_iops, const int64_t max_bandwidth)
{
  int ret = OB_SUCCESS;
  char root_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
  char endpoint[OB_MAX_BACKUP_ENDPOINT_LENGTH] = {0};
  char old_attribute[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  if (storage_path.empty() || (max_iops < 0 && max_bandwidth < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_path), K(max_iops), K(max_bandwidth));
  } else if (OB_FAIL(ObStorageInfoOperator::parse_storage_path(
             storage_path.ptr(), root_path, sizeof(root_path), endpoint, sizeof(endpoint)))) {
    LOG_WARN("fail to parse storage path", KR(ret), K(storage_path));
  } else {
    ObMySQLTransaction trans_changing;
    if (OB_FAIL(trans_changing.start(proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else if (OB_FAIL(ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(trans_changing))) {
      LOG_WARN("failed to check and update service epoch", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); i++) {
      if (0 == STRNCMP(zone_storage_infos_.at(i).dest_attr_.path_, root_path, sizeof(root_path)) &&
          0 == STRNCMP(zone_storage_infos_.at(i).dest_attr_.endpoint_, endpoint, sizeof(endpoint))) {
        ObBackupDest target_storage_dest;
        uint64_t op_id = OB_INVALID_ID;
        uint64_t old_op_id = OB_INVALID_ID;
        uint64_t sub_op_id = OB_INVALID_ID;
        ObZoneStorageState::STATE op_type = ObZoneStorageState::CHANGING;
        common::ObZone zone = zone_storage_infos_.at(i).zone_;
        ObStorageUsedType::TYPE used_for = zone_storage_infos_.at(i).used_for_;
        uint64_t storage_id = zone_storage_infos_.at(i).storage_id_;
        if (ObZoneStorageState::ADDED != zone_storage_infos_.at(i).state_ &&
            ObZoneStorageState::CHANGED != zone_storage_infos_.at(i).state_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("cannot support changing current storage state when undergoing modification", KR(ret),
                   K(zone_storage_infos_.at(i)), K(i));
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "cannot support changing current storage state when undergoing modification, it is");
        } else if (OB_FAIL(target_storage_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                                   zone_storage_infos_.at(i).dest_attr_.endpoint_,
                                                   zone_storage_infos_.at(i).dest_attr_.authorization_,
                                                   zone_storage_infos_.at(i).dest_attr_.extension_))) {
          LOG_WARN("failed to set storage dest", KR(ret), K(zone_storage_infos_.at(i)));
        } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, op_id))) {
          LOG_WARN("failed to fetch new storage op id", KR(ret), K(op_id));
        } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, sub_op_id))) {
          LOG_WARN("failed to fetch new storage sub op id", KR(ret), K(sub_op_id));
        } else {
          old_op_id = zone_storage_infos_.at(i).op_id_;
          int64_t pos = 0;
          if (max_iops >= 0 && OB_FAIL(databuff_printf(old_attribute, sizeof(old_attribute), pos, "%s%ld ",
                                                  STORAGE_MAX_IOPS, zone_storage_infos_.at(i).max_iops_))) {
            LOG_WARN("fail to databuff printf", KR(ret));
          } else if (max_bandwidth >= 0 && OB_FAIL(databuff_printf(old_attribute, sizeof(old_attribute), pos, "%s%ld",
                                              STORAGE_MAX_BANDWIDTH, zone_storage_infos_.at(i).max_bandwidth_))) {
            LOG_WARN("fail to databuff printf", KR(ret));
          }
          if (max_iops >= 0) {
            zone_storage_infos_.at(i).max_iops_ = max_iops;
          }
          if (max_bandwidth >= 0) {
            zone_storage_infos_.at(i).max_bandwidth_ = max_bandwidth;
          }
          zone_storage_infos_.at(i).state_ = op_type;
          zone_storage_infos_.at(i).op_id_ = op_id;
          zone_storage_infos_.at(i).sub_op_id_ = sub_op_id;
          if (OB_FAIL(ret)) {
          } else if (max_iops >= 0 && OB_FAIL(ObStorageInfoOperator::update_storage_iops(
                       trans_changing, zone, target_storage_dest, old_op_id, used_for, max_iops))) {
            LOG_WARN("failed to update storage max_iops", KR(ret), K(target_storage_dest), K(max_iops));
          } else if (max_bandwidth >= 0 && OB_FAIL(ObStorageInfoOperator::update_storage_bandwidth(
                       trans_changing, zone, target_storage_dest, old_op_id, used_for, max_bandwidth))) {
            LOG_WARN("failed to update storage bandwidth", KR(ret), K(target_storage_dest), K(max_bandwidth));
          } else if (OB_FAIL(ObStorageInfoOperator::update_storage_state(
                       trans_changing, zone, target_storage_dest, used_for, old_op_id, op_type))) {
            LOG_WARN("failed to update zone storage state", KR(ret), K(op_type));
          } else if (OB_FAIL(ObStorageInfoOperator::update_storage_op_id(
                       trans_changing, zone, target_storage_dest, used_for, old_op_id, op_id))) {
            LOG_WARN("failed to update zone storage op id", KR(ret), K(op_id));
          } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                       trans_changing, storage_id, op_id, sub_op_id, zone, op_type, old_attribute))) {
            LOG_WARN("failed to insert zone storage operation table", KR(ret), K(storage_id),
                     K(op_id), K(sub_op_id), K(zone), K(op_type), K(old_attribute));
          }
        }
      }
    }
    int tmp_ret = trans_changing.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::reload()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObZoneStorageTableInfo> storage_table_infos;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(OB_SYS_TENANT_ID));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("server state is not suppported when tenant's data version is below 4.3.3.0", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.3.0, alter storage is ");
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(
               ObStorageInfoOperator::get_zone_storage_table_info(*proxy_, storage_table_infos))) {
    LOG_WARN("failed to get zone storage table info", KR(ret), K(storage_table_infos));
  }
  if (OB_SUCC(ret)) {
    reset_zone_storage_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_table_infos.count(); ++i) {
      if (OB_FAIL(zone_storage_infos_.push_back(storage_table_infos.at(i)))) {
        LOG_WARN("fail to push back item", KR(ret), K(i), K(storage_table_infos.at(i)));
      }
    }
  }
  if (OB_SUCC(ret)) {
    loaded_ = true;
    LOG_INFO("succeed to reload zone storage manager", "zone_storage_manager_info", this);
  } else {
    LOG_WARN("failed to reload zone storage manager", KR(ret));
  }
  return ret;
}

int ObZoneStorageManagerBase::check_storage_operation_state()
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, compat_version))) {
    LOG_WARN("get min data_version failed", K(ret), K(OB_SYS_TENANT_ID));
  } else if (compat_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("server state is not suppported when tenant's data version is below 4.3.3.0", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.3.0, alter storage is ");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); i++) {
    if (ObZoneStorageState::ADDING == zone_storage_infos_.at(i).state_ ||
        ObZoneStorageState::DROPPING == zone_storage_infos_.at(i).state_ ||
        ObZoneStorageState::CHANGING == zone_storage_infos_.at(i).state_) {
      common::ObZone zone = zone_storage_infos_.at(i).zone_;
      common::ObArray<ObAddr> server_list;
      uint64_t complete_server_count = 0;
      int64_t idx = i;
      const int64_t rpc_timeout = GCONF.rpc_timeout;
      int64_t timeout = 0;
      rootserver::ObCheckStorageOperationStatusProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::check_storage_operation_status);
      if (OB_FAIL(ObShareUtil::get_ctx_timeout(rpc_timeout, timeout))) {
        LOG_WARN("fail to get timeout", KR(ret));
      } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, server_list))) {
        SERVER_LOG(WARN, "fail to get server list of zone", KR(ret), K(zone));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
        obrpc::ObCheckStorageOperationStatusResult result;
        obrpc::ObCheckStorageOperationStatusArg arg;
        arg.op_id_ = zone_storage_infos_.at(i).op_id_;
        arg.sub_op_id_ = zone_storage_infos_.at(i).sub_op_id_;
        const ObAddr &addr = server_list.at(j);
        if (OB_UNLIKELY(!arg.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid arguments", KR(ret), K(arg));
        } else if (OB_FAIL(proxy.call(addr, timeout, arg))) {
          LOG_WARN("send check storage operation status rpc failed", KR(ret),
              K(timeout), K(arg), K(addr));
        }
      }
      int tmp_ret = OB_SUCCESS;
      ObArray<int> return_code_array;
      if (OB_SUCCESS != (tmp_ret = proxy.wait_all(return_code_array))) {
        LOG_WARN("wait batch result failed", KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      if (OB_FAIL(ret)) {
      } else if (return_code_array.count() != server_list.count() ||
                 return_code_array.count() != proxy.get_results().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cnt not match",
            KR(ret),
            "return_cnt",
            return_code_array.count(),
            "result_cnt",
            proxy.get_results().count(),
            "server_cnt",
            server_list.count());
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < server_list.count(); ++j) {
        const ObAddr &addr = proxy.get_dests().at(j);
        if (OB_FAIL(return_code_array.at(j))) {
          LOG_WARN("rpc execute failed", KR(ret), K(addr));
          ret = OB_SUCCESS;  // ignore error
        } else {
          const obrpc::ObCheckStorageOperationStatusResult *result = proxy.get_results().at(j);
          const obrpc::ObCheckStorageOperationStatusArg &arg = proxy.get_args().at(j);
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", KR(ret));
            ret = OB_SUCCESS;  // ignore error
          } else if (OB_SUCCESS == result->get_ret() && true == result->get_is_done() &&
                    (ObZoneStorageState::DROPPING == zone_storage_infos_.at(i).state_ ||
                      true == result->get_is_connective())) {
            ++complete_server_count;
          } else {
            if (OB_SUCCESS == result->get_ret() && true == result->get_is_done() &&
                ObZoneStorageState::DROPPING != zone_storage_infos_.at(i).state_ &&
                true != result->get_is_connective()) {
              ret = OB_STORAGE_DEST_NOT_CONNECT;
              LOG_WARN("failed to check storage dest connectivity", KR(ret), K(addr), K(arg),
                      K(result));
            } else {
              LOG_WARN("sync server operator storage timer task is not completed", K(addr), KPC(result),
                      K(arg));
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (complete_server_count == server_list.count()) {
        if (OB_FAIL(update_zone_storage_table_state(i))) {
          LOG_WARN("failed to update zone storage table state", KR(ret), K(i));
        } else if (OB_SUCC(ret) && ObZoneStorageState::DROPPED == zone_storage_infos_.at(i).state_) {
          if (OB_FAIL(zone_storage_infos_.remove(idx))) {
            LOG_WARN("fail to remove item", KR(ret), K(idx));
          } else {
            --i;
          }
        }
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::get_zone_storage_with_zone(const ObZone &zone,
                                                         const ObStorageUsedType::TYPE used_for,
                                                         ObBackupDest &storage_dest, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone storage manager is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(zone.is_empty() || !ObStorageUsedType::is_valid(used_for))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), K(used_for));
  } else {
    is_exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); ++i) {
      if (zone == zone_storage_infos_.at(i).zone_ &&
          (used_for == zone_storage_infos_.at(i).used_for_ ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for ||
           ObStorageUsedType::TYPE::USED_TYPE_ALL == zone_storage_infos_.at(i).used_for_)) {
        if (OB_FAIL(storage_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                     zone_storage_infos_.at(i).dest_attr_.endpoint_,
                                     zone_storage_infos_.at(i).dest_attr_.authorization_,
                                     zone_storage_infos_.at(i).dest_attr_.extension_))) {
          LOG_WARN("failed to set storage dest", KR(ret), K(zone_storage_infos_.at(i)));
        } else {
          is_exist = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::check_zone_storage_with_region_scope(const ObRegion &region,
                                                                   const ObStorageUsedType::TYPE used_for,
                                                                   const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObZone, ObRegion> zone_info_map;
  ObMemAttr attr(OB_SYS_TENANT_ID, "ZONE_REGION_MAP");
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("zone storage manager is not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(region.is_empty() || !ObStorageUsedType::is_valid(used_for))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(region), K(used_for));
  } else if (OB_FAIL(zone_info_map.create(7, attr, attr))) {
    LOG_WARN("create zone region map failed", KR(ret));
  } else if (OB_FAIL(ObZoneTableOperation::get_zone_region_list(*proxy_, zone_info_map))) {
    LOG_WARN("fail to get zone region list", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_storage_infos_.count(); ++i) {
      ObRegion table_region;
      if (OB_FAIL(zone_info_map.get_refactored(zone_storage_infos_.at(i).zone_, table_region))) {
        LOG_WARN("fail to get map", KR(ret), K(zone_storage_infos_.at(i)));
      } else if (region == table_region &&
                 (used_for == zone_storage_infos_.at(i).used_for_ ||
                  ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for ||
                  ObStorageUsedType::TYPE::USED_TYPE_ALL == zone_storage_infos_.at(i).used_for_)) {
        ObBackupDest table_dest;
        if (OB_FAIL(table_dest.set(zone_storage_infos_.at(i).dest_attr_.path_,
                                   zone_storage_infos_.at(i).dest_attr_.endpoint_,
                                   zone_storage_infos_.at(i).dest_attr_.authorization_,
                                   zone_storage_infos_.at(i).dest_attr_.extension_))) {
          LOG_WARN("failed to set storage dest", KR(ret), K(zone_storage_infos_.at(i)));
        } else if (table_dest != storage_dest) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("the server's shared storage info is different with inner table in region scope", KR(ret), K(storage_dest), K(table_dest));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "the server's shared storage info is different with inner table in region scope");
        }
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::update_zone_storage_table_state(const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  uint64_t sub_op_id = OB_INVALID_ID;
  uint64_t op_id = zone_storage_infos_.at(idx).op_id_;
  ObStorageUsedType::TYPE used_for = zone_storage_infos_.at(idx).used_for_;
  uint64_t storage_id = zone_storage_infos_.at(idx).storage_id_;
  common::ObZone zone = zone_storage_infos_.at(idx).zone_;
  ObZoneStorageState::STATE op_type = ObZoneStorageState::MAX;
  char authorization[OB_MAX_BACKUP_AUTHORIZATION_LENGTH] = {0};
  char storage_dest_info[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  ObBackupDest storage_dest;
  ObString storage_path;
  char path[OB_MAX_BACKUP_DEST_LENGTH] = {0};
  if (OB_FAIL(storage_dest.set(zone_storage_infos_.at(idx).dest_attr_.path_,
                               zone_storage_infos_.at(idx).dest_attr_.endpoint_,
                               zone_storage_infos_.at(idx).dest_attr_.authorization_,
                               zone_storage_infos_.at(idx).dest_attr_.extension_))) {
    LOG_WARN("fail to set storage dest", KR(ret), K(zone_storage_infos_.at(idx)));
  } else if (OB_FAIL(storage_dest.get_backup_path_str(path, sizeof(path)))) {
    LOG_WARN("failed to get storage path", KR(ret), K(storage_dest));
  } else if (OB_FAIL(ObStorageInfoOperator::fetch_new_storage_op_id(*proxy_, sub_op_id))) {
    LOG_WARN("failed to fetch new storage sub op id", KR(ret), K(sub_op_id));
  } else if (OB_FAIL(
               storage_dest.get_backup_dest_str(storage_dest_info, sizeof(storage_dest_info)))) {
    LOG_WARN("failed to get storage info", KR(ret), K(storage_dest));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_authorization_info(
               authorization, sizeof(authorization)))) {
    LOG_WARN("failed to get authorization", KR(ret), K(storage_dest));
  } else {
    storage_path.assign_ptr(path, static_cast<int32_t>(STRLEN(path)));
    if (ObZoneStorageState::ADDING == zone_storage_infos_.at(idx).state_) {
      op_type = ObZoneStorageState::ADDED;
      zone_storage_infos_.at(idx).state_ = op_type;
      zone_storage_infos_.at(idx).sub_op_id_ = sub_op_id;
      if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", KR(ret));
      } else if (OB_FAIL(ObStorageInfoOperator::update_storage_state(trans, zone, storage_dest,
                                                                     used_for, op_id, op_type))) {
        LOG_WARN("failed to update storage state", KR(ret), K(op_type));
      } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                   trans, storage_id, op_id, sub_op_id, zone, op_type, storage_dest_info))) {
        LOG_WARN("failed to insert zone storage operation", KR(ret), K(storage_id), K(op_id),
                 K(sub_op_id), K(zone), K(op_type), K(storage_dest_info));
      }
    } else if (ObZoneStorageState::DROPPING == zone_storage_infos_.at(idx).state_) {
      op_type = ObZoneStorageState::DROPPED;
      zone_storage_infos_.at(idx).state_ = op_type;
      zone_storage_infos_.at(idx).sub_op_id_ = sub_op_id;
      if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", KR(ret));
      } else if (OB_FAIL(ObStorageInfoOperator::remove_storage_info(trans, zone, storage_path,
                                                                    used_for))) {
        LOG_WARN("failed to delete storage info", KR(ret), K(zone), K(storage_path), K(used_for));
      } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                   trans, storage_id, op_id, sub_op_id, zone, op_type, storage_path.ptr()))) {
        LOG_WARN("failed to insert zone storage operation table", KR(ret), K(storage_id), K(op_id),
                 K(sub_op_id), K(zone), K(op_type), K(storage_path));
      }
    } else if (ObZoneStorageState::CHANGING == zone_storage_infos_.at(idx).state_) {
      op_type = ObZoneStorageState::CHANGED;
      zone_storage_infos_.at(idx).state_ = op_type;
      zone_storage_infos_.at(idx).sub_op_id_ = sub_op_id;
      if (OB_FAIL(trans.start(proxy_, OB_SYS_TENANT_ID))) {
        LOG_WARN("start transaction failed", KR(ret));
      } else if (OB_FAIL(ObStorageInfoOperator::update_storage_state(trans, zone, storage_dest,
                                                                     used_for, op_id, op_type))) {
        LOG_WARN("failed to update zone storage state", KR(ret), K(op_type));
      } else if (OB_FAIL(ObStorageOperationOperator::insert_storage_operation(
                   trans, storage_id, op_id, sub_op_id, zone, op_type, authorization))) {
        LOG_WARN("failed to insert zone storage operation table", KR(ret), K(storage_id), K(op_id),
                 K(sub_op_id), K(zone), K(op_type), K(authorization));
      }
    }
    int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", KR(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::parse_attribute_str(const ObString &attribute, int64_t &max_iops, int64_t &max_bandwidth)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(attribute.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaild argument", KR(ret), K(attribute));
  } else {
    char tmp[OB_MAX_STORAGE_ATTRIBUTE_LENGTH] = {0};
    char *token = NULL;
    char *saved_ptr = NULL;
    MEMCPY(tmp, attribute.ptr(), attribute.length());
    tmp[attribute.length()] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == STRNCASECMP(STORAGE_MAX_IOPS, token, strlen(STORAGE_MAX_IOPS))) {
        const char *max_iops_start_str = token + strlen(STORAGE_MAX_IOPS);
        if (false == ObString(max_iops_start_str).is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("max_iops is invalid argument", KR(ret), K(token));
        } else {
          char *end_str = nullptr;
          max_iops = strtoll(max_iops_start_str, &end_str, 10);
          if ('\0' != *end_str) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid int value", KR(ret), K(max_iops_start_str));
          }
        }
      } else if (0 == STRNCASECMP(STORAGE_MAX_BANDWIDTH, token, strlen(STORAGE_MAX_BANDWIDTH))) {
        bool is_valid = false;
        max_bandwidth = ObConfigCapacityParser::get(token + strlen(STORAGE_MAX_BANDWIDTH), is_valid, true /*check_unit*/);
        if (!is_valid) {
          ret = OB_CONVERT_ERROR;
          LOG_WARN("convert failed", KR(ret), K(token));
        }
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::check_need_fetch_storage_id(const share::ObBackupDest &storage_dest,
                                                          bool &is_need_fetch,
                                                          uint64_t &storage_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    for (int64_t i = 0; i < zone_storage_infos_.count(); ++i) {
      // the same path and endpoint use the same storage id, does not need fetch new storage id
      if ((0 == STRNCMP(storage_dest.get_root_path().ptr(), zone_storage_infos_.at(i).dest_attr_.path_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.path_))) &&
          (0 == STRNCMP(storage_dest.get_storage_info()->endpoint_,
                        zone_storage_infos_.at(i).dest_attr_.endpoint_,
                        sizeof(zone_storage_infos_.at(i).dest_attr_.endpoint_)))) {
        is_need_fetch = false;
        storage_id = zone_storage_infos_.at(i).storage_id_;
        break;
      }
    }
  }
  return  ret;
}

// only used for copying data to/from shadow_
int ObZoneStorageManagerBase::copy_infos(ObZoneStorageManagerBase &dest,
                                         const ObZoneStorageManagerBase &src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.zone_storage_infos_.count();
  dest.reset_zone_storage_infos();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
    if (OB_FAIL(dest.zone_storage_infos_.push_back(src.zone_storage_infos_.at(idx)))) {
      LOG_WARN("fail to push back", KR(ret), K(idx));
    }
  }
  if (OB_SUCC(ret)) {
    dest.inited_ = src.inited_;
    dest.loaded_ = src.loaded_;
  }
  return ret;
}

int ObZoneStorageManagerBase::get_storage_infos_by_zone(const ObZone &zone,
    ObIArray<share::ObZoneStorageTableInfo> &storage_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone is empty", KR(ret), K(zone));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check inner stat failed", KR(ret));
    } else {
      FOREACH_X(it, zone_storage_infos_, OB_SUCC(ret)) {
        const share::ObZoneStorageTableInfo &info = *it;
        if (OB_UNLIKELY(!info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is invalid", KR(ret), K(zone), K(info));
        } else if (info.zone_ != zone) {
        } else if (OB_FAIL(storage_infos.push_back(info))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to push_back info to storage_infos", KR(ret), K(zone), K(info), K(storage_infos));
        } else {}
      }
    }
  }
  return ret;
}

int ObZoneStorageManagerBase::check_zone_storage_exist(const ObZone &zone, bool &storage_exist) const
{
  int ret = OB_SUCCESS;
  storage_exist = false;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone is empty", KR(ret), K(zone));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check inner stat failed", KR(ret));
    } else {
      FOREACH_X(it, zone_storage_infos_, OB_SUCC(ret) && !storage_exist) {
        const share::ObZoneStorageTableInfo &info = *it;
        if (OB_UNLIKELY(!info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is invalid", KR(ret), K(zone), K(info));
        } else if (info.zone_ == zone) {
          storage_exist = true;
        } else {}
      }
    }
  }
  return ret;
}

///////////////////////////ObZoneStorageManager///////////////////////////
ObZoneStorageManager::ObZoneStorageManager()
  : write_lock_(ObLatchIds::ZONE_STORAGE_MANAGER_LOCK), shadow_()
{
}

ObZoneStorageManager::~ObZoneStorageManager() {}

ObZoneStorageManager::ObZoneStorageManagerShadowGuard::ObZoneStorageManagerShadowGuard(
  const common::SpinRWLock &lock, ObZoneStorageManagerBase &storage_mgr,
  ObZoneStorageManagerBase &shadow, int &ret)
  : lock_(const_cast<SpinRWLock &>(lock)), storage_mgr_(storage_mgr), shadow_(shadow), ret_(ret)
{
  SpinRLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS != (ret_ = ObZoneStorageManager::copy_infos(shadow_, storage_mgr_)))) {
    LOG_WARN("copy to shadow_ failed", K(ret_));
  }
}

ObZoneStorageManager::ObZoneStorageManagerShadowGuard::~ObZoneStorageManagerShadowGuard()
{
  SpinWLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(OB_SUCCESS !=
                         (ret_ = ObZoneStorageManager::copy_infos(storage_mgr_, shadow_)))) {
    LOG_WARN_RET(ret_, "copy from shadow_ failed", K(ret_));
  }
}

int ObZoneStorageManager::init(ObMySQLProxy &proxy, obrpc::ObSrvRpcProxy &srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    // uninit proxy is used to prevent somebody from
    // trying to directly use the set-interfaces in ObZoneStorageManagerBase
    // actually, all update operations are performed in shadow_
    if (OB_FAIL(ObZoneStorageManagerBase::init(uninit_proxy_, uninit_rpc_proxy_))) {
      LOG_WARN("init zone storage manager failed", KR(ret));
    } else if (OB_FAIL(shadow_.init(proxy, srv_rpc_proxy))) {
      LOG_WARN("init shadow_ failed", KR(ret));
    }
  }
  return ret;
}

int ObZoneStorageManager::reload()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(static_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.reload();
    }
  }
  return ret;
}

int ObZoneStorageManager::add_storage(const ObString &storage_path, const ObString &access_info,
                                      const ObString &attribute, const ObStorageUsedType::TYPE &use_for,
                                      const ObZone &zone, const bool &wait_type)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(dynamic_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.add_storage(storage_path, access_info, attribute, use_for, zone, wait_type);
    }
  }
  return ret;
}

int ObZoneStorageManager::drop_storage(const ObString &storage_path,
                                       const ObStorageUsedType::TYPE &use_for, const ObZone &zone,
                                       const bool &force_type, const bool &wait_type)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(dynamic_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.drop_storage(storage_path, use_for, zone, force_type, wait_type);
    }
  }
  return ret;
}

int ObZoneStorageManager::alter_storage(const ObString &storage_path, const ObString &access_info,
                                        const ObString &attribute, const bool &wait_type)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(dynamic_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.alter_storage(storage_path, access_info, attribute, wait_type);
    }
  }
  return ret;
}

int ObZoneStorageManager::check_storage_operation_state()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(static_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.check_storage_operation_state();
    }
  }
  return ret;
}

int ObZoneStorageManager::get_zone_storage_with_zone(const ObZone &zone,
                                                     const ObStorageUsedType::TYPE used_for,
                                                     ObBackupDest &storage_dest,
                                                     bool &is_exist)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(static_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.get_zone_storage_with_zone(zone, used_for, storage_dest, is_exist);
    }
  }
  return ret;
}

int ObZoneStorageManager::check_zone_storage_exist(const ObZone &zone, bool &storage_exist)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneStorageManagerShadowGuard shadow_copy_guard(
      lock_, *(static_cast<ObZoneStorageManagerBase *>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.check_zone_storage_exist(zone, storage_exist);
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
