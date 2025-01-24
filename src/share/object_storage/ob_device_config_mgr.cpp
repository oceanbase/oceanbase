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

#include "share/object_storage/ob_device_config_mgr.h"
#include "share/ob_device_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_dir_manager.h"
#endif

namespace oceanbase
{
namespace share
{
ObDeviceConfigMgr &ObDeviceConfigMgr::get_instance()
{
  static ObDeviceConfigMgr device_config_mgr;
  return device_config_mgr;
}

ObDeviceConfigMgr::ObDeviceConfigMgr()
  : is_inited_(false), config_map_(), head_(), device_manifest_(),
    manifest_rw_lock_(common::ObLatchIds::DEVICE_MANIFEST_RW_LOCK),
    data_storage_dest_(), clog_storage_dest_()
{
}

int ObDeviceConfigMgr::init(const char *data_dir)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_dir is nullptr", KR(ret), K(data_dir));
  } else if (OB_FAIL(config_map_.create(ObDeviceManager::MAX_DEVICE_INSTANCE,
                                        "DeviceConfigMap", "DeviceConfigMap"))) {
    LOG_WARN("fail to init config map", KR(ret));
  } else if (OB_FAIL(device_manifest_.init(data_dir))) {
    LOG_WARN("fail to init device manifest", KR(ret), K(data_dir));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDeviceConfigMgr::destroy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("device config mgr start to destroy");
  common::SpinWLockGuard w_guard(manifest_rw_lock_);
  SMART_VAR(ObArray<ObDeviceConfig>, device_configs) {
    // save the content of config_map_, so as to print in log
    for (ConfigMap::const_iterator it = config_map_.begin();
         OB_SUCC(ret) && it != config_map_.end(); ++it) {
      if (OB_FAIL(device_configs.push_back(*(it->second)))) {
        LOG_WARN("fail to push back device config", KR(ret), "device_config", *(it->second));
      }
    }
    config_map_.destroy();
    device_manifest_.destroy();
    head_.reset();
    data_storage_dest_.reset();
    clog_storage_dest_.reset();
    is_inited_ = false;
    LOG_INFO("device config mgr finish to destroy", KR(ret), K(device_configs));
  }
}

int ObDeviceConfigMgr::load_configs()
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard w_guard(manifest_rw_lock_);
  config_map_.reuse();
  head_.reset();
  SMART_VAR(ObArray<ObDeviceConfig>, device_configs) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
    } else if (OB_FAIL(device_manifest_.load(device_configs, head_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) { // first start of observer
        LOG_INFO("device manifest file does not exist", KR(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to load device manifest file", KR(ret));
      }
    } else if (0 == device_configs.count()) {
      LOG_INFO("no config in device manifest file", KR(ret));
    } else if (device_configs.count() > 0) {
      ObArenaAllocator allocator;
      for (int64_t i = 0; OB_SUCC(ret) && (i < device_configs.count()); ++i) {
        ObDeviceConfig &tmp_device_config = device_configs.at(i);
        char *tmp_key_chars = nullptr;
        if (OB_ISNULL(tmp_key_chars = reinterpret_cast<char*>(
                                      allocator.alloc(OB_MAX_DEVICE_CONFIG_KEY_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(OB_MAX_DEVICE_CONFIG_KEY_LENGTH));
        } else if (OB_FAIL(databuff_printf(tmp_key_chars, OB_MAX_DEVICE_CONFIG_KEY_LENGTH, "%s&%s&%s",
            tmp_device_config.used_for_, tmp_device_config.path_, tmp_device_config.endpoint_))) {
          LOG_WARN("fail to construct device config key", KR(ret));
        } else {
          ObMemAttr attr(OB_SERVER_TENANT_ID, "DeviceConfigMgr");
          ObDeviceConfig *new_device_config = nullptr;
          if (OB_ISNULL(new_device_config = static_cast<ObDeviceConfig *>(
                                            ob_malloc(sizeof(ObDeviceConfig), attr)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            new (new_device_config) ObDeviceConfig();
            *new_device_config = tmp_device_config;
            ObDeviceConfigKey device_config_key(tmp_key_chars);
            if (OB_FAIL(config_map_.set_refactored(device_config_key, new_device_config, true))) {
              LOG_WARN("fail to insert manifest config", KR(ret), KP(new_device_config));
              ob_free(new_device_config); // free memory
              new_device_config = nullptr;
#ifdef OB_BUILD_SHARED_STORAGE
            } else if ((ObStorageUsedType::TYPE::USED_TYPE_ALL == ObStorageUsedType::get_type(tmp_device_config.used_for_)) ||
                       (ObStorageUsedType::TYPE::USED_TYPE_DATA == ObStorageUsedType::get_type(tmp_device_config.used_for_))) {
              if (OB_FAIL(OB_DIR_MGR.set_object_storage_root_dir(tmp_device_config.path_))) {
                LOG_WARN("fail to set object storage root dir", KR(ret), K(tmp_device_config.path_));
              }
#endif
            }
          }
        }
      }
    }
    LOG_INFO("finish to load configs", KR(ret), K(device_configs), K_(head));
  }
  return ret;
}

int ObDeviceConfigMgr::get_device_configs(
    const char *used_for,
    common::ObIArray<ObDeviceConfig> &configs) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    configs.reuse();
    for (ConfigMap::const_iterator it = config_map_.begin();
         OB_SUCC(ret) && it != config_map_.end(); ++it) {
      if (0 == STRCMP(used_for, it->second->used_for_)) {
        if (OB_FAIL(configs.push_back(*(it->second)))) {
          LOG_WARN("fail to push back device config", KR(ret), "device_config", *(it->second));
        }
      }
    }
  }
  LOG_INFO("finish to get device configs", KR(ret), K(used_for), K(configs));
  return ret;
}

int ObDeviceConfigMgr::get_device_config(
    const ObStorageUsedType::TYPE &used_for,
    ObDeviceConfig &config) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else if ((ObStorageUsedType::TYPE::USED_TYPE_DATA != used_for) &&
             (ObStorageUsedType::TYPE::USED_TYPE_LOG != used_for)) { // get_device_config's used_for either data or clog, cannot all
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", KR(ret), K(used_for));
  } else {
    const char *used_for_cstr = ObStorageUsedType::get_str(used_for);
    const char *used_for_all = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_ALL);
    ConfigMap::const_iterator it;
    for (it = config_map_.begin(); OB_SUCC(ret) && it != config_map_.end(); ++it) {
      if (0 == STRCMP(used_for_cstr, it->second->used_for_) ||
          0 == STRCMP(used_for_all, it->second->used_for_)) {
        config = *it->second;
        break;
      }
    }
    if (it == config_map_.end()) {
      // if get device config from config_map failed, try to get device config from storage_dest_
      // 1.when prepare_bootstrap, RS will send ak/sk to observer store in storage_dest_, temporary use
      // 2.when execute_bootstrap finish, shared storage info will dump to manifest, storage_dest_ will reset, do not use again
      if (OB_FAIL(get_device_config_from_storage_dest_(used_for, config))) {
        LOG_WARN("fail to get device config from storage dest", KR(ret), K(used_for));
      }
      if (OB_FAIL(ret)) {
        // if get device config from storage_dest failed, reset error code to OB_ENTRY_NOT_EXIST
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

// return 'path?host=endpoint', which is consistent with 'alter system add storage' command
// e.g., oss://bucket_name/root_dir?host=oss-cn-hangzhou.aliyuncs.com,
//       s3://bucket_name/root_dir?host=s3.cn-north-1.amazonaws.com.cn
int ObDeviceConfigMgr::get_device_config_str(
    const ObStorageUsedType::TYPE &used_for,
    char *buf,
    const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObStorageUsedType::is_valid(used_for) || OB_ISNULL(buf) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(used_for), KP(buf), K(buf_len));
  } else {
    SMART_VAR(ObDeviceConfig, device_config) {
      if (OB_FAIL(get_device_config(used_for, device_config))) {
        LOG_WARN("fail to get device config", KR(ret), K(used_for));
      } else {
        int64_t pos = 0;
        const int64_t need_len = STRLEN(device_config.path_)       // path len
                                + STRLEN(device_config.endpoint_)  // endpoint len
                                + 2;                               // '?' and '\0' len
        if (OB_UNLIKELY(buf_len < need_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf len is not enough", KR(ret), K(buf_len), K(need_len));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s?%s",
                                          device_config.path_,
                                          device_config.endpoint_))) {
          LOG_WARN("fail to print buf", KR(ret), K(buf_len), K(need_len));
        }
      }
    }
  }
  return ret;
}

int ObDeviceConfigMgr::get_all_device_configs(common::ObIArray<ObDeviceConfig> &configs) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    configs.reuse();
    for (ConfigMap::const_iterator it = config_map_.begin();
         OB_SUCC(ret) && it != config_map_.end(); ++it) {
      if (OB_FAIL(configs.push_back(*(it->second)))) {
        LOG_WARN("fail to push back device config", KR(ret), "device_config", *(it->second));
      }
    }
  }
  LOG_INFO("finish to get all device configs", KR(ret), K(configs));
  return ret;
}

int ObDeviceConfigMgr::get_device_config(
    const ObDeviceConfigKey &device_config_key,
    ObDeviceConfig &config) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  ObDeviceConfig *p_device_config = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else if (OB_FAIL(config_map_.get_refactored(device_config_key, p_device_config))) {
    LOG_WARN("fail to get_refactored", KR(ret), K(device_config_key));
  } else if (OB_ISNULL(p_device_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device config is null", K(device_config_key));
  } else {
    config = *p_device_config;
  }
  LOG_INFO("finish to get device config", KR(ret), K(config));
  return ret;
}

int ObDeviceConfigMgr::add_device_config(const ObDeviceConfig &config)
{
  int ret = OB_SUCCESS;
  DeviceConfigModifyType type = DeviceConfigModifyType::CONFIG_ADD_TYPE;
  bool is_dump_manifest = false;
  if (OB_FAIL(modify_device_config_(config, type))) {
    LOG_WARN("fail to modify device config", KR(ret), K(config));
  } else if (OB_FAIL(check_if_dump_manifest_(is_dump_manifest))) {
    LOG_WARN("fail to check shared storage info if dump manifest file", KR(ret), K(is_dump_manifest));
  } else if (is_dump_manifest) {
    // if shared storage info has been dumped to manifest, storage_dest_ need reset, cannot use anymore
    data_storage_dest_.reset();
    clog_storage_dest_.reset();
    LOG_INFO("shared storage info has been dumped to manifest, storage_dest reset succeed", K(is_dump_manifest));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to add device config", KR(ret), K(config));
  }
  return ret;
}

int ObDeviceConfigMgr::remove_device_config(const ObDeviceConfig &config)
{
  int ret = OB_SUCCESS;
  DeviceConfigModifyType type = DeviceConfigModifyType::CONFIG_REMOVE_TYPE;
  if (OB_FAIL(modify_device_config_(config, type))) {
    LOG_WARN("fail to modify device config", KR(ret), K(config));
  } else {
    LOG_INFO("succ to remove device config", KR(ret), K(config));
  }
  return ret;
}

int ObDeviceConfigMgr::update_device_config(const ObDeviceConfig &config)
{
  int ret = OB_SUCCESS;
  DeviceConfigModifyType type = DeviceConfigModifyType::CONFIG_UPDATE_TYPE;
  if (OB_FAIL(modify_device_config_(config, type))) {
    LOG_WARN("fail to modify device config", KR(ret), K(config));
  } else {
    LOG_INFO("succ to update device config", KR(ret), K(config));
  }
  return ret;
}

int ObDeviceConfigMgr::get_last_op_id(uint64_t &last_op_id) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    last_op_id = head_.last_op_id_;
  }
  return ret;
}

int ObDeviceConfigMgr::get_last_sub_op_id(uint64_t &last_sub_op_id) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    last_sub_op_id = head_.last_sub_op_id_;
  }
  return ret;
}

int ObDeviceConfigMgr::update_last_op_and_sub_op_id(
    const uint64_t last_op_id,
    const uint64_t last_sub_op_id)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard w_guard(manifest_rw_lock_);
  ObDeviceManifest::HeadSection ori_head;
  ori_head.assign(head_); // save the original head
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    head_.modify_timestamp_us_ = ObTimeUtility::fast_current_time();
    head_.last_op_id_ = last_op_id;
    head_.last_sub_op_id_ = last_sub_op_id;
    if (OB_FAIL(save_configs_())) {
      LOG_WARN("fail to save configs", KR(ret), K_(head));
    }
  }
  if (OB_FAIL(ret)) {
    // In order for is_op_done(last_op_id, last_sub_op_id) to return false
    // based on the original last_op_id & last_sub_op_id, here rollback the head.
    head_.assign(ori_head);
  }
  LOG_INFO("finish to update last_op_id and last_sub_op_id", KR(ret),
           K(last_op_id), K(last_sub_op_id), K_(head));
  return ret;
}

int ObDeviceConfigMgr::is_op_done(
    const uint64_t op_id,
    const uint64_t sub_op_id,
    bool &is_done) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_id);
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  is_done = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else if ((UINT64_MAX == head_.last_op_id_) && (UINT64_MAX == head_.last_sub_op_id_)) {
    // device manifest has not been updated yet
    is_done = false;
  } else if ((UINT64_MAX == head_.last_op_id_) || (UINT64_MAX == head_.last_sub_op_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected last_op_id and last_sub_op_id", "last_op_id", head_.last_op_id_,
             "last_sub_op_id", head_.last_sub_op_id_);
  } else if (head_.last_sub_op_id_ >= sub_op_id) {
    is_done = true;
  }
  return ret;
}

int ObDeviceConfigMgr::is_connective(
    const uint64_t op_id,
    const uint64_t sub_op_id,
    bool &is_connective) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard r_guard(manifest_rw_lock_);
  is_connective = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    bool is_find = false;
    for (ConfigMap::const_iterator it = config_map_.begin();
         OB_SUCC(ret) && it != config_map_.end(); ++it) {
      if ((op_id == it->second->op_id_) && (sub_op_id == it->second->sub_op_id_)) {
        is_find = true;
        ObArenaAllocator allocator;
        char *tmp_state_info = nullptr;
        if (OB_ISNULL(tmp_state_info = reinterpret_cast<char*>(
                                       allocator.alloc(OB_MAX_STORAGE_STATE_INFO_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc tmp_state_info", KR(ret), K(OB_MAX_STORAGE_STATE_INFO_LENGTH));
        } else if (FALSE_IT(STRCPY(tmp_state_info, it->second->state_info_))) {
        } else if (OB_FAIL(parse_is_connective_(tmp_state_info, is_connective))) {
          LOG_WARN("fail to parse is connective", KR(ret), KCSTRING(tmp_state_info));
        }
        break;
      }
    }
    if (OB_SUCC(ret) && !is_find) {
      LOG_WARN("device config does not exist", K(op_id), K(sub_op_id));
    }
  }
  return ret;
}

bool ObDeviceConfigMgr::config_map_is_empty(const ObStorageUsedType::TYPE used_for) const
{
  for (ConfigMap::const_iterator it = config_map_.begin();
       it != config_map_.end(); ++it) {
    if (used_for == ObStorageUsedType::get_type(it->second->used_for_) ||
        ObStorageUsedType::TYPE::USED_TYPE_ALL ==
          ObStorageUsedType::get_type(it->second->used_for_)) {
      return false;
    }
  }
  return true;
}

int ObDeviceConfigMgr::set_storage_dest(const ObStorageUsedType::TYPE used_for, const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else if (OB_UNLIKELY(!storage_dest.is_valid() || !ObStorageUsedType::is_valid(used_for))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(storage_dest), K(used_for));
  } else {
    bool is_used_for_clog = false;
    bool is_used_for_data = false;
    if (ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for) {
      is_used_for_clog = true;
      is_used_for_data = true;
    } else if (ObStorageUsedType::TYPE::USED_TYPE_DATA == used_for) {
      is_used_for_data = true;
    } else if (ObStorageUsedType::TYPE::USED_TYPE_LOG == used_for) {
      is_used_for_clog = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected used for type", KR(ret), K(used_for));
    }
    if (OB_FAIL(ret)) {
    } else if (is_used_for_clog && OB_FAIL(clog_storage_dest_.deep_copy(storage_dest))) {
      LOG_WARN("fail to deep copy dest", KR(ret), K(storage_dest));
    } else if (is_used_for_data && OB_FAIL(data_storage_dest_.deep_copy(storage_dest))) {
      LOG_WARN("fail to deep copy dest", KR(ret), K(storage_dest));
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (is_used_for_data && OB_FAIL(OB_DIR_MGR.set_object_storage_root_dir(storage_dest.get_root_path().ptr()))) {
      LOG_WARN("fail to set object storage root dir", KR(ret), K(storage_dest));
#endif
    } else {
      LOG_INFO("succ to set storage dest", K(used_for), K(storage_dest));
    }
  }
  return ret;
}

int ObDeviceConfigMgr::save_configs_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else {
    SMART_VAR(ObArray<ObDeviceConfig>, device_configs) {
      for (ConfigMap::const_iterator it = config_map_.begin();
           OB_SUCC(ret) && (it != config_map_.end()); ++it) {
        if (!it->second->is_valid()) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("can not save invalid device config", KR(ret), "device_config", *(it->second));
        } else if (OB_FAIL(device_configs.push_back(*(it->second)))) {
          LOG_WARN("fail to push back", KR(ret), "device_config", *(it->second));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(device_manifest_.dump2file(device_configs, head_))) {
          LOG_ERROR("fail to dump2file", KR(ret));
        }
      }
      LOG_INFO("finish to save configs", KR(ret), K(device_configs), K_(head));
    }
  }
  return ret;
}

int ObDeviceConfigMgr::modify_device_config_(
    const ObDeviceConfig &config,
    DeviceConfigModifyType type)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard w_guard(manifest_rw_lock_);
  ObDeviceManifest::HeadSection ori_head;
  ori_head.assign(head_); // save the original head
  ObArenaAllocator allocator;
  char *key_chars = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
  } else if (OB_ISNULL(key_chars = reinterpret_cast<char*>(
                                   allocator.alloc(OB_MAX_DEVICE_CONFIG_KEY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(OB_MAX_DEVICE_CONFIG_KEY_LENGTH));
  } else if (FALSE_IT(memset(key_chars, 0, OB_MAX_DEVICE_CONFIG_KEY_LENGTH))) {
  } else if (OB_FAIL(databuff_printf(key_chars, OB_MAX_DEVICE_CONFIG_KEY_LENGTH, "%s&%s&%s",
             config.used_for_, config.path_, config.endpoint_))) {
    LOG_WARN("fail to construct device config key", KR(ret));
  } else {
    ObDeviceConfigKey device_config_key(key_chars);
    if (DeviceConfigModifyType::CONFIG_ADD_TYPE == type) {
      ObMemAttr attr(OB_SERVER_TENANT_ID, "DeviceConfigMgr");
      ObDeviceConfig *new_device_config = nullptr;
      if (OB_ISNULL(new_device_config = static_cast<ObDeviceConfig *>(
                                        ob_malloc(sizeof(ObDeviceConfig), attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret));
      } else {
        new (new_device_config) ObDeviceConfig();
        *new_device_config = config;
        if (OB_FAIL(config_map_.set_refactored(device_config_key, new_device_config, true))) {
          LOG_WARN("fail to set_refactored config_map", KR(ret), K(device_config_key), K(config));
          ob_free(new_device_config); // free memory
          new_device_config = nullptr;
#ifdef OB_BUILD_SHARED_STORAGE
        } else if ((ObStorageUsedType::TYPE::USED_TYPE_ALL == ObStorageUsedType::get_type(config.used_for_)) ||
                    (ObStorageUsedType::TYPE::USED_TYPE_DATA == ObStorageUsedType::get_type(config.used_for_))) {
          if (OB_FAIL(OB_DIR_MGR.set_object_storage_root_dir(config.path_))) {
            LOG_WARN("fail to set object storage root dir", KR(ret), K(config.path_));
          }
#endif
        }
      }
    } else if ((DeviceConfigModifyType::CONFIG_UPDATE_TYPE == type)
               || (DeviceConfigModifyType::CONFIG_REMOVE_TYPE == type)) {
      ObDeviceConfig *p_device_config = nullptr;
      if (OB_FAIL(config_map_.get_refactored(device_config_key, p_device_config))) {
        LOG_WARN("fail to get device config", KR(ret), K(device_config_key));
      } else if (OB_ISNULL(p_device_config)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("device config is null", KR(ret), K(device_config_key));
      } else {
        if (DeviceConfigModifyType::CONFIG_UPDATE_TYPE == type) {
          *p_device_config = config;
        } else if (DeviceConfigModifyType::CONFIG_REMOVE_TYPE == type) {
          if (OB_FAIL(config_map_.erase_refactored(device_config_key))) {
            LOG_WARN("fail to erase_refactored config_map", KR(ret), K(device_config_key), K(config));
          } else {
            ob_free(p_device_config); // free memory
            p_device_config = nullptr;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    head_.device_num_ = config_map_.size();
    head_.modify_timestamp_us_ = ObTimeUtility::fast_current_time();
    head_.last_op_id_ = config.op_id_;
    head_.last_sub_op_id_ = config.sub_op_id_;
    if (OB_FAIL(save_configs_())) {
      LOG_WARN("fail to save configs", KR(ret), K_(head), K(config));
    }
  }
  if (OB_FAIL(ret)) {
    // In order for the background thread to execute add/remove/update_device_config again
    // based on the original op_id & sub_op_id, here rollback the head.
    head_.assign(ori_head);
  }
  LOG_INFO("finish to modify device config", KR(ret), K(config), K_(head));
  return ret;
}

int ObDeviceConfigMgr::parse_is_connective_(char *state_info, bool &is_connective) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(state_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    char *token = state_info;
    char *saved_ptr = NULL;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, ",", &saved_ptr);
      if (OB_ISNULL(token)) {
        break;
      } else if (0 == STRNCMP(STORAGE_IS_CONNECTIVE_KEY, token, STRLEN(STORAGE_IS_CONNECTIVE_KEY))) {
        const char *value_str = token + STRLEN(STORAGE_IS_CONNECTIVE_KEY);
        if ((0 == STRLEN(value_str)) || (STRLEN(value_str) >= OB_MAX_STORAGE_STATE_INFO_LENGTH)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("value str is empty or too long", KR(ret), "value_len", STRLEN(value_str),
                   K(OB_MAX_STORAGE_STATE_INFO_LENGTH));
        } else if (0 == STRCMP(value_str, "true")) {
          is_connective = true;
        } else if (0 == STRCMP(value_str, "false")) {
          is_connective = false;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid value of is_connective", KR(ret), KCSTRING(value_str));
        }
      }
    }
  }
  return ret;
}

int ObDeviceConfigMgr::check_if_dump_manifest_(bool &is_dump_manifest)
{
  int ret = OB_SUCCESS;
  is_dump_manifest = false;
  SMART_VAR(ObArray<ObDeviceConfig>, device_configs) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDeviceConfigMgr not init", KR(ret));
    } else if (OB_FAIL(get_all_device_configs(device_configs))) {
      LOG_WARN("fail to get all device configs", KR(ret));
    } else {
      bool is_used_for_clog_exist = false;
      bool is_used_for_data_exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && (i < device_configs.count()); ++i) {
        ObStorageUsedType::TYPE used_for_type = ObStorageUsedType::get_type(device_configs.at(i).used_for_);
        if (ObStorageUsedType::TYPE::USED_TYPE_LOG == used_for_type) {
          is_used_for_clog_exist = true;
        } else if (ObStorageUsedType::TYPE::USED_TYPE_DATA == used_for_type) {
          is_used_for_data_exist = true;
        } else if (ObStorageUsedType::TYPE::USED_TYPE_ALL == used_for_type) {
          is_used_for_clog_exist = true;
          is_used_for_data_exist = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected used for type", KR(ret), K(used_for_type));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_used_for_clog_exist && is_used_for_data_exist) {
        is_dump_manifest = true;
      }
    }
  }
  return ret;
}


int ObDeviceConfigMgr::get_device_config_from_storage_dest_(const ObStorageUsedType::TYPE &used_for,
                                                            ObDeviceConfig &config) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceConfigMgr not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((ObStorageUsedType::TYPE::USED_TYPE_DATA != used_for) &&
                          (ObStorageUsedType::TYPE::USED_TYPE_LOG != used_for))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(used_for));
  } else {
    const ObBackupDest *storage_dest = nullptr;
    if (ObStorageUsedType::TYPE::USED_TYPE_DATA == used_for) {
      storage_dest = &data_storage_dest_;
    } else if (ObStorageUsedType::TYPE::USED_TYPE_LOG == used_for) {
      storage_dest = &clog_storage_dest_;
    }
    if (OB_ISNULL(storage_dest)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stoage dest is null", KR(ret), KP(storage_dest));
    } else if (!storage_dest->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stoage dest is invalid", KR(ret), K(*storage_dest));
    } else if (OB_FAIL(storage_dest->get_storage_info()->get_unencrypted_authorization_info(
                config.access_info_, sizeof(config.access_info_)))) {
      LOG_WARN("fail to get authorization info", KR(ret), K(*storage_dest));
    } else {
      STRCPY(config.used_for_, ObStorageUsedType::get_str(used_for));
      STRCPY(config.path_, storage_dest->get_root_path().ptr());
      STRCPY(config.endpoint_, storage_dest->get_storage_info()->endpoint_);
      STRCPY(config.extension_, storage_dest->get_storage_info()->extension_);
      STRCPY(config.state_, ObZoneStorageState::get_str(ObZoneStorageState::ADDED));
      config.create_timestamp_ = common::ObTimeUtility::fast_current_time();
      config.last_check_timestamp_ = common::ObTimeUtility::fast_current_time();
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
