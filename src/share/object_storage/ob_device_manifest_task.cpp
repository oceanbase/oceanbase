/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/object_storage/ob_device_manifest_task.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/object_storage/ob_device_connectivity.h"
#include "share/object_storage/ob_zone_storage_table_operation.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
namespace share
{

using namespace common;

ObDeviceManifestTask &ObDeviceManifestTask::get_instance()
{
  static ObDeviceManifestTask device_manifest_task;
  return device_manifest_task;
}

ObDeviceManifestTask::ObDeviceManifestTask()
  : is_inited_(false), sql_proxy_(nullptr),
    manifest_task_lock_(common::ObLatchIds::MANIFEST_TASK_LOCK)
{
}

int ObDeviceManifestTask::init(ObMySQLProxy *proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("device manifest task has already been initialized", KR(ret));
  } else {
    sql_proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObDeviceManifestTask::runTimerTask()
{
  run();
}

int ObDeviceManifestTask::run()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("device manifest task start", K(start_us));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("device manifest task has not been initialized", KR(ret));
  } else if (OB_FAIL(do_work())) {
    LOG_WARN("fail to do work", KR(ret));
  }
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("device manifest task finish", K(cost_us));
  return ret;
}

int ObDeviceManifestTask::try_update_new_device_configs()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("start to try update new device configs", K(start_us));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("device manifest task has not been initialized", KR(ret));
  } else {
    ObDeviceConfigMgr &device_config_mgr = ObDeviceConfigMgr::get_instance();
    uint64_t pre_op_id = UINT64_MAX;
    uint64_t pre_sub_op_id = UINT64_MAX;
    uint64_t cur_op_id = UINT64_MAX;
    uint64_t cur_sub_op_id = UINT64_MAX;
    do {
      if (OB_FAIL(device_config_mgr.get_last_op_id(pre_op_id))) {
        LOG_WARN("fail to get last_op_id", KR(ret));
      } else if (OB_FAIL(device_config_mgr.get_last_sub_op_id(pre_sub_op_id))) {
        LOG_WARN("fail to get last_sub_op_id", KR(ret));
      } else if (OB_FAIL(try_update_next_device_config(pre_op_id, pre_sub_op_id))) {
        LOG_WARN("fail to try update all device config", KR(ret), K(pre_op_id), K(pre_sub_op_id));
      } else if (OB_FAIL(device_config_mgr.get_last_op_id(cur_op_id))) {
        LOG_WARN("fail to get last_op_id", KR(ret));
      } else if (OB_FAIL(device_config_mgr.get_last_sub_op_id(cur_sub_op_id))) {
        LOG_WARN("fail to get last_sub_op_id", KR(ret));
      }
    } while ((cur_op_id > pre_op_id) || ((cur_op_id == pre_op_id) && (cur_sub_op_id > pre_sub_op_id)));
  }
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to try update new device configs", K(cost_us));
  return ret;
}

int ObDeviceManifestTask::add_new_device_configs(const ObIArray<ObZoneStorageTableInfo> &storage_infos)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("start to try update all device config", K(start_us));
  if (OB_UNLIKELY(storage_infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_infos));
  } else {
    // add into manifest for each storage info
    lib::ObMutexGuard guard(manifest_task_lock_);
    const int64_t zone_storage_info_cnt = storage_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < zone_storage_info_cnt); ++i) {
      const ObZoneStorageTableInfo &tmp_zone_storage_info = storage_infos.at(i);
      bool is_connective = true;
      if (OB_FAIL(add_device_config(tmp_zone_storage_info, is_connective))) {
        LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info));
      }
    }
  }
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to try update all device config", KR(ret), K(cost_us));
  return ret;
}

int ObDeviceManifestTask::do_work()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(manifest_task_lock_);
  ObDeviceConfigMgr &device_config_mgr = ObDeviceConfigMgr::get_instance();
  uint64_t last_op_id = UINT64_MAX;
  uint64_t last_sub_op_id = UINT64_MAX;
  if (OB_FAIL(device_config_mgr.get_last_op_id(last_op_id))) {
    LOG_WARN("fail to get last_op_id", KR(ret));
  } else if (OB_FAIL(device_config_mgr.get_last_sub_op_id(last_sub_op_id))) {
    LOG_WARN("fail to get last_sub_op_id", KR(ret));
  } else if ((UINT64_MAX == last_op_id) && (UINT64_MAX == last_sub_op_id)) {
    // device manifest has not been updated yet
    if (OB_FAIL(try_update_all_device_config())) {
      LOG_WARN("fail to try update all device config", KR(ret));
    }
  } else if ((UINT64_MAX == last_op_id) || (UINT64_MAX == last_sub_op_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected last_op_id and last_sub_op_id", KR(ret), K(last_op_id), K(last_sub_op_id));
  } else {
    // device manifest has already been updated
    if (OB_FAIL(try_update_next_device_config(last_op_id, last_sub_op_id))) {
      LOG_WARN("fail to try update all device config", KR(ret), K(last_op_id), K(last_sub_op_id));
    }
  }
  return ret;
}

int ObDeviceManifestTask::try_update_all_device_config()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("start to try update all device config", K(start_us));
  const char *zone = GCONF.zone.str();
  SMART_VAR(ObArray<ObZoneStorageTableInfo>, ordered_zone_storage_infos) {
    // 1. get all storage infos of this zone from __all_zone_storage, which are ordered by op_id
    if (OB_FAIL(ObStorageInfoOperator::get_ordered_zone_storage_infos(*sql_proxy_, zone,
                                       ordered_zone_storage_infos))) {
      LOG_WARN("fail to get ordered zone storage infos", KR(ret), K(zone));
    } else if (ordered_zone_storage_infos.empty()) {
      LOG_INFO("no storage infos exist in table, thus no need to update device config", K(zone));
    } else {
      // 2. check connectivity and add into manifest for each storage info
      const int64_t zone_storage_info_cnt = ordered_zone_storage_infos.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < zone_storage_info_cnt); ++i) {
        // 2.1 get storage operation info with MAX(sub_op_id) of this op_id from __all_zone_storage_operation
        const ObZoneStorageTableInfo &tmp_zone_storage_info = ordered_zone_storage_infos.at(i);
        const int64_t tmp_op_id = tmp_zone_storage_info.op_id_;
        ObZoneStorageOperationTableInfo tmp_storage_op_info;
        if (OB_FAIL(ObStorageOperationOperator::get_max_sub_op_info(*sql_proxy_, tmp_op_id,
                                                tmp_storage_op_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // ignore ret
            LOG_INFO("storage operation info does not exist in table", "op_id", tmp_op_id);
          } else {
            LOG_WARN("fail to get max sub_op info", KR(ret), "op_id", tmp_op_id);
          }
        } else {
          // 2.2 according to op_type, check connectivity and add device_config if need
          bool need_update = false; // represent if need check connectivity and update manifest
          if (OB_FAIL(check_if_need_update(tmp_storage_op_info.op_type_, need_update))) {
            LOG_WARN("fail to check if need update", KR(ret), "op_type", tmp_storage_op_info.op_type_);
          } else if (need_update) {
            int tmp_ret = OB_SUCCESS;
            bool is_connective = false;
            if (OB_TMP_FAIL(check_connectivity(tmp_zone_storage_info, is_connective))) {
              LOG_WARN("fail to check connectivity", KR(tmp_ret), K(tmp_zone_storage_info));
            }
            if (OB_FAIL(add_device_config(tmp_zone_storage_info, tmp_storage_op_info, is_connective))) {
              LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info),
                       K(tmp_storage_op_info));
            }
          }
        }
      }
    }
  }
  // if OB_FAIL(ret), rm manifest file?
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to try update all device config", KR(ret), K(cost_us));
  return ret;
}

int ObDeviceManifestTask::try_update_next_device_config(
    const uint64_t last_op_id,
    const uint64_t last_sub_op_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = common::ObTimeUtility::fast_current_time();
  LOG_INFO("start to try update next device config", K(start_us));
  const char *zone = GCONF.zone.str();
  // 1. get storage operation info with min (op_id, sub_op_id) larger than
  // (last_op_id, last_sub_op_id) from __all_zone_storage_operation
  ObZoneStorageOperationTableInfo tmp_storage_op_info;
  if ((UINT64_MAX == last_op_id) || (UINT64_MAX == last_sub_op_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid last_op_id and last_sub_op_id", KR(ret), K(last_op_id), K(last_sub_op_id));
  } else if (OB_FAIL(ObStorageOperationOperator::get_min_op_info_greater_than(*sql_proxy_, zone,
              last_op_id, last_sub_op_id, tmp_storage_op_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
      LOG_INFO("no new storage operation info exist in table", K(zone), K(last_op_id),
               K(last_sub_op_id));
    } else {
      LOG_WARN("fail to get min operation info", KR(ret), K(zone), K(last_op_id), K(last_sub_op_id));
    }
  } else {
    // 2. according to op_type, check connectivity and update manifest
    if (OB_FAIL(update_device_manifest(tmp_storage_op_info))) {
      LOG_WARN("fail to update device config", KR(ret), K(tmp_storage_op_info));
    }
  }
  const int64_t cost_us = common::ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to try update next device config", KR(ret), K(cost_us), K(last_op_id),
           K(last_sub_op_id));
  return ret;
}

int ObDeviceManifestTask::check_if_need_update(
    const ObZoneStorageState::STATE &op_type,
    bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = false;
  if (ObZoneStorageState::STATE::MAX == op_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op_type", KR(ret));
  } else if ((ObZoneStorageState::STATE::ADDING == op_type)
             || (ObZoneStorageState::STATE::ADDED == op_type)
             || (ObZoneStorageState::STATE::CHANGING == op_type)
             || (ObZoneStorageState::STATE::CHANGED == op_type)) {
    need_update = true;
  } else if ((ObZoneStorageState::STATE::DROPPING == op_type)
             || (ObZoneStorageState::STATE::DROPPED == op_type)) {
    // no need to check connectivity and update manifest for DROPPING and DROPPED storage
    need_update = false;
  }
  return ret;
}

int ObDeviceManifestTask::check_connectivity(
    const ObZoneStorageTableInfo &zone_storage_info,
    bool &is_connective)
{
  int ret = OB_SUCCESS;
  ObDeviceConnectivityCheckManager device_conn_check_mgr;
  ObBackupDest storage_dest;
  const char *path = zone_storage_info.dest_attr_.path_;
  const char *endpoint = zone_storage_info.dest_attr_.endpoint_;
  const char *authorization = zone_storage_info.dest_attr_.authorization_;
  const char *extension = zone_storage_info.dest_attr_.extension_;
  if (OB_FAIL(storage_dest.set(path, endpoint, authorization, extension))) {
    LOG_WARN("fail to set storage_dest", KR(ret), K(path), K(endpoint), K(authorization), K(extension));
  } else if ((ObStorageType::OB_STORAGE_FILE != storage_dest.get_storage_info()->device_type_) &&
             OB_FAIL(device_conn_check_mgr.check_device_connectivity(storage_dest))) {
    LOG_WARN("fail to check device connectivity", KR(ret), K(storage_dest));
  } else {
    LOG_INFO("succ to check device connectivity", K(storage_dest));
  }
  is_connective = OB_SUCC(ret) ? true : false;
  return ret;
}

int ObDeviceManifestTask::add_device_config(
    const ObZoneStorageTableInfo &zone_storage_info,
    const ObZoneStorageOperationTableInfo &storage_op_info,
    const bool is_connective)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    STRCPY(device_config.used_for_, ObStorageUsedType::get_str(zone_storage_info.used_for_));
    STRCPY(device_config.path_, zone_storage_info.dest_attr_.path_);
    STRCPY(device_config.endpoint_, zone_storage_info.dest_attr_.endpoint_);
    STRCPY(device_config.access_info_, zone_storage_info.dest_attr_.authorization_);
    STRCPY(device_config.extension_, zone_storage_info.dest_attr_.extension_);
    STRCPY(device_config.state_, ObZoneStorageState::get_str(storage_op_info.op_type_));
    char tmp_state_info[OB_MAX_STORAGE_STATE_INFO_LENGTH] = { 0 };
    MEMCPY(tmp_state_info, STORAGE_IS_CONNECTIVE_KEY, STRLEN(STORAGE_IS_CONNECTIVE_KEY));
    MEMCPY(tmp_state_info + STRLEN(tmp_state_info), is_connective ? "true" : "false",
           is_connective ? STRLEN("true") : STRLEN("false"));
    STRCPY(device_config.state_info_, tmp_state_info);
    device_config.create_timestamp_ = common::ObTimeUtility::fast_current_time();
    device_config.last_check_timestamp_ = common::ObTimeUtility::fast_current_time();
    device_config.op_id_ = storage_op_info.op_id_;
    device_config.sub_op_id_ = storage_op_info.sub_op_id_;
    device_config.storage_id_ = zone_storage_info.storage_id_;
    device_config.max_iops_ = zone_storage_info.max_iops_;
    device_config.max_bandwidth_ = zone_storage_info.max_bandwidth_;
#ifdef OB_BUILD_TDE_SECURITY
    // call get_root_key to ensure that root_key is dumped to local file, so as to eliminate the
    // dependency on RS when observer restarts
    ObRootKey root_key;
    if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID, root_key, true))) {
      LOG_WARN("failed to get root key", KR(ret));
    }
#endif
    if (FAILEDx(ObDeviceConfigMgr::get_instance().add_device_config(device_config))) {
      LOG_WARN("fail to add device config", KR(ret), K(zone_storage_info), K(device_config));
    }
  }
  LOG_INFO("finish to add device config", KR(ret), K(storage_op_info));
  return ret;
}

int ObDeviceManifestTask::add_device_config(
    const ObZoneStorageTableInfo &zone_storage_info,
    const bool is_connective)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    STRCPY(device_config.used_for_, ObStorageUsedType::get_str(zone_storage_info.used_for_));
    STRCPY(device_config.path_, zone_storage_info.dest_attr_.path_);
    STRCPY(device_config.endpoint_, zone_storage_info.dest_attr_.endpoint_);
    STRCPY(device_config.access_info_, zone_storage_info.dest_attr_.authorization_);
    STRCPY(device_config.extension_, zone_storage_info.dest_attr_.extension_);
    STRCPY(device_config.state_, ObZoneStorageState::get_str(zone_storage_info.state_));
    char tmp_state_info[OB_MAX_STORAGE_STATE_INFO_LENGTH] = { 0 };
    MEMCPY(tmp_state_info, STORAGE_IS_CONNECTIVE_KEY, STRLEN(STORAGE_IS_CONNECTIVE_KEY));
    MEMCPY(tmp_state_info + STRLEN(tmp_state_info), is_connective ? "true" : "false",
           is_connective ? STRLEN("true") : STRLEN("false"));
    STRCPY(device_config.state_info_, tmp_state_info);
    device_config.create_timestamp_ = common::ObTimeUtility::fast_current_time();
    device_config.last_check_timestamp_ = common::ObTimeUtility::fast_current_time();
    device_config.op_id_ = zone_storage_info.op_id_;
    device_config.sub_op_id_ = zone_storage_info.sub_op_id_;
    device_config.storage_id_ = zone_storage_info.storage_id_;
    device_config.max_iops_ = zone_storage_info.max_iops_;
    device_config.max_bandwidth_ = zone_storage_info.max_bandwidth_;
#ifdef OB_BUILD_TDE_SECURITY
    // call get_root_key to ensure that root_key is dumped to local file, so as to eliminate the
    // dependency on RS when observer restarts
    ObRootKey root_key;
    if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID, root_key, true))) {
      LOG_WARN("failed to get root key", KR(ret));
    }
#endif
    if (FAILEDx(ObDeviceConfigMgr::get_instance().add_device_config(device_config))) {
      LOG_WARN("fail to add device config", KR(ret), K(zone_storage_info), K(device_config));
    }
  }
  LOG_INFO("finish to add device config", KR(ret));
  return ret;
}

int ObDeviceManifestTask::update_device_manifest(const ObZoneStorageOperationTableInfo &storage_op_info)
{
  int ret = OB_SUCCESS;
  const ObZoneStorageState::STATE &op_type = storage_op_info.op_type_;
  const uint64_t op_id = storage_op_info.op_id_;
  const uint64_t sub_op_id = storage_op_info.sub_op_id_;
  SMART_VAR(ObZoneStorageTableInfo, tmp_zone_storage_info) {
    if (ObZoneStorageState::STATE::DROPPED == op_type) {
      // only need to update last_op_id and last_sub_op_id
      if (OB_FAIL(ObDeviceConfigMgr::get_instance().update_last_op_and_sub_op_id(op_id, sub_op_id))) {
        LOG_WARN("fail to update last_op_id and last_sub_op_id", KR(ret), K(op_id), K(sub_op_id));
      }
    } else if (OB_FAIL(ObStorageInfoOperator::get_zone_storage_info(*sql_proxy_, op_id, tmp_zone_storage_info))) {
      if ((OB_ENTRY_NOT_EXIST == ret) && ((ObZoneStorageState::STATE::ADDED == op_type)
          || (ObZoneStorageState::STATE::CHANGED == op_type))) {
        // e.g., op_type<op_id,sub_op_id>: adding/changing<6,0> -> added/changed<6,1> -> changing<7,2>
        // added/changed with op_id=6 is overwritten by changing with op_id=7 in __all_zone_storage
        ret = OB_SUCCESS; // ignore ret
        LOG_INFO("zone storage info does not exist, may be overwritten by subsequent operations", K(op_id), K(op_type));
        if (OB_FAIL(ObDeviceConfigMgr::get_instance().update_last_op_and_sub_op_id(op_id, sub_op_id))) {
          LOG_WARN("fail to update last_op_id and last_sub_op_id", KR(ret), K(op_id), K(sub_op_id));
        }
      } else {
        LOG_WARN("fail to get zone storage info", KR(ret), K(op_id));
      }
    } else if (ObZoneStorageState::STATE::ADDING == op_type) {
      // check connectivity and add device_config into manifest
      int tmp_ret = OB_SUCCESS;
      bool is_connective = false;
      if (OB_TMP_FAIL(check_connectivity(tmp_zone_storage_info, is_connective))) {
        LOG_WARN("fail to check connectivity", KR(tmp_ret), K(tmp_zone_storage_info));
      }
      if (OB_FAIL(add_device_config(tmp_zone_storage_info, storage_op_info, is_connective))) {
        LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info), K(storage_op_info));
      }
    } else if (ObZoneStorageState::STATE::DROPPING == op_type) {
      // remove device_config from manifest
      if (OB_FAIL(remove_device_config(tmp_zone_storage_info, storage_op_info))) {
        LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info), K(storage_op_info));
      }
    } else if (ObZoneStorageState::STATE::CHANGING == op_type) {
      // check connectivity and update device_config in manifest
      int tmp_ret = OB_SUCCESS;
      bool is_connective = false;
      if (OB_TMP_FAIL(check_connectivity(tmp_zone_storage_info, is_connective))) {
        LOG_WARN("fail to check connectivity", KR(tmp_ret), K(tmp_zone_storage_info));
      }
      if (OB_FAIL(update_device_config(tmp_zone_storage_info, storage_op_info, is_connective))) {
        LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info), K(storage_op_info));
      }
    } else if ((ObZoneStorageState::STATE::ADDED == op_type)
               || (ObZoneStorageState::STATE::CHANGED == op_type)) {
      // update device_config in manifest
      if (OB_FAIL(update_device_config(tmp_zone_storage_info, storage_op_info, true/*is_connective*/))) {
        LOG_WARN("fail to update device manifest", KR(ret), K(tmp_zone_storage_info), K(storage_op_info));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid op_type", KR(ret), K(op_type), K(storage_op_info));
    }
  }
  LOG_INFO("finish to update device manifest", KR(ret), K(storage_op_info));
  return ret;
}

int ObDeviceManifestTask::remove_device_config(
    const ObZoneStorageTableInfo &zone_storage_info,
    const ObZoneStorageOperationTableInfo &storage_op_info)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    STRCPY(device_config.used_for_, ObStorageUsedType::get_str(zone_storage_info.used_for_));
    STRCPY(device_config.path_, zone_storage_info.dest_attr_.path_);
    STRCPY(device_config.endpoint_, zone_storage_info.dest_attr_.endpoint_);
    device_config.op_id_ = storage_op_info.op_id_;
    device_config.sub_op_id_ = storage_op_info.sub_op_id_;
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().remove_device_config(device_config))) {
      LOG_WARN("fail to remove device config", KR(ret), K(device_config));
    }
  }
  LOG_INFO("finish to remove device config", KR(ret), K(zone_storage_info), K(storage_op_info));
  return ret;
}

int ObDeviceManifestTask::update_device_config(
    const ObZoneStorageTableInfo &zone_storage_info,
    const ObZoneStorageOperationTableInfo &storage_op_info,
    const bool is_connective)
{
  int ret = OB_SUCCESS;
  const ObZoneStorageState::STATE &op_type = storage_op_info.op_type_;
  ObArenaAllocator allocator;
  char *key_chars = nullptr;
  if ((ObZoneStorageState::STATE::ADDED != op_type)
      && (ObZoneStorageState::STATE::CHANGING != op_type)
      && (ObZoneStorageState::STATE::CHANGED != op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op_type", KR(ret), K(op_type));
  } else if (OB_ISNULL(key_chars = reinterpret_cast<char*>(
                                   allocator.alloc(OB_MAX_DEVICE_CONFIG_KEY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(OB_MAX_DEVICE_CONFIG_KEY_LENGTH));
  } else if (FALSE_IT(memset(key_chars, 0, OB_MAX_DEVICE_CONFIG_KEY_LENGTH))) {
  } else if (OB_FAIL(databuff_printf(key_chars, OB_MAX_DEVICE_CONFIG_KEY_LENGTH, "%s&%s&%s",
                     ObStorageUsedType::get_str(zone_storage_info.used_for_),
                     zone_storage_info.dest_attr_.path_,
                     zone_storage_info.dest_attr_.endpoint_))) {
    LOG_WARN("fail to construct device config key", KR(ret));
  } else {
    ObDeviceConfigKey device_config_key(key_chars);
    SMART_VAR(ObDeviceConfig, device_config) {
      if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(device_config_key, device_config))) {
        LOG_WARN("fail to get device config", KR(ret), K(device_config_key));
      } else if (FALSE_IT(device_config.op_id_ = storage_op_info.op_id_)) {
      } else if (FALSE_IT(device_config.sub_op_id_ = storage_op_info.sub_op_id_)) {
      // Note: must MEMSET zero before STRCPY, because calc checksum depends on sizeof()
      } else if (FALSE_IT(MEMSET(device_config.state_, 0, sizeof(device_config.state_)))) {
      } else if (FALSE_IT(STRCPY(device_config.state_, ObZoneStorageState::get_str(op_type)))) {
      } else if (ObZoneStorageState::STATE::CHANGING == op_type) {
        // Note: must MEMSET zero before STRCPY, because calc checksum depends on sizeof()
        MEMSET(device_config.old_access_info_, 0, sizeof(device_config.old_access_info_));
        MEMSET(device_config.old_encrypt_info_, 0, sizeof(device_config.old_encrypt_info_));
        MEMSET(device_config.old_extension_, 0, sizeof(device_config.old_extension_));
        STRCPY(device_config.old_access_info_, device_config.access_info_);
        STRCPY(device_config.old_encrypt_info_, device_config.encrypt_info_);
        STRCPY(device_config.old_extension_, device_config.extension_);
        MEMSET(device_config.access_info_, 0, sizeof(device_config.access_info_));
        MEMSET(device_config.extension_, 0, sizeof(device_config.extension_));
        STRCPY(device_config.access_info_, zone_storage_info.dest_attr_.authorization_);
        STRCPY(device_config.extension_, zone_storage_info.dest_attr_.extension_);
        device_config.max_iops_ = zone_storage_info.max_iops_;
        device_config.max_bandwidth_ = zone_storage_info.max_bandwidth_;
        device_config.last_check_timestamp_ = common::ObTimeUtility::fast_current_time();
        char tmp_state_info[OB_MAX_STORAGE_STATE_INFO_LENGTH] = { 0 };
        MEMCPY(tmp_state_info, STORAGE_IS_CONNECTIVE_KEY, STRLEN(STORAGE_IS_CONNECTIVE_KEY));
        MEMCPY(tmp_state_info + STRLEN(tmp_state_info), is_connective ? "true" : "false",
                is_connective ? STRLEN("true") : STRLEN("false"));
        MEMSET(device_config.state_info_, 0, sizeof(device_config.state_info_));
        STRCPY(device_config.state_info_, tmp_state_info);
      }
      if (FAILEDx(ObDeviceConfigMgr::get_instance().update_device_config(device_config))) {
        LOG_WARN("fail to update device config", KR(ret), K(device_config));
      }
    }
  }
  LOG_INFO("finish to update device config", KR(ret), K(zone_storage_info), K(storage_op_info));
  return ret;
}

}  // namespace share
}  // namespace oceanbase
