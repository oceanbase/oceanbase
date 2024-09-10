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

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_MGR_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_MGR_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/object_storage/ob_device_manifest.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{
const int64_t OB_MAX_DEVICE_CONFIG_KEY_LENGTH = OB_MAX_STORAGE_USED_FOR_LENGTH
                                                + OB_MAX_BACKUP_DEST_LENGTH
                                                + OB_MAX_BACKUP_ENDPOINT_LENGTH;
typedef common::ObFixedLengthString<OB_MAX_DEVICE_CONFIG_KEY_LENGTH> ObDeviceConfigKey;

class ObDeviceConfigMgr
{
public:
  static ObDeviceConfigMgr &get_instance();
  ObDeviceConfigMgr();
  int init(const char *data_dir);
  void destroy();
  int load_configs();
  int get_device_configs(const char *used_for, common::ObIArray<ObDeviceConfig> &configs) const;
  int get_all_device_configs(common::ObIArray<ObDeviceConfig> &configs) const;
  int get_device_config(const ObDeviceConfigKey &device_config_key, ObDeviceConfig &config) const;
  int get_device_config(const ObStorageUsedType::TYPE &used_for, ObDeviceConfig &config) const;
  int get_device_config_str(const ObStorageUsedType::TYPE &used_for, char *buf, const int64_t buf_len) const;
  int add_device_config(const ObDeviceConfig &config);
  int remove_device_config(const ObDeviceConfig &config);
  int update_device_config(const ObDeviceConfig &config);
  int get_last_op_id(uint64_t &last_op_id) const;
  int get_last_sub_op_id(uint64_t &last_sub_op_id) const;
  int update_last_op_and_sub_op_id(const uint64_t last_op_id, const uint64_t last_sub_op_id);
  int is_op_done(const uint64_t op_id, const uint64_t sub_op_id, bool &is_done) const;
  int is_connective(const uint64_t op_id, const uint64_t sub_op_id, bool &is_connective) const;
  // check if config_map_ is empty in current used_for_type
  bool config_map_is_empty(const ObStorageUsedType::TYPE used_for) const;

private:
  enum class DeviceConfigModifyType
  {
    CONFIG_ADD_TYPE = 0,
    CONFIG_REMOVE_TYPE = 1,
    CONFIG_UPDATE_TYPE = 2,
  };
  int save_configs_();
  int modify_device_config_(const ObDeviceConfig &config, DeviceConfigModifyType type);
  int parse_is_connective_(char *state_info, bool &is_connective) const;

private:
  typedef common::hash::ObHashMap<ObDeviceConfigKey, ObDeviceConfig*> ConfigMap;
  bool is_inited_;
  ConfigMap config_map_;
  ObDeviceManifest::HeadSection head_;
  ObDeviceManifest device_manifest_;
  common::SpinRWLock manifest_rw_lock_;
};

}  // namespace share
}  // namespace oceanbase

#define OB_DEVICE_CONF_MGR (share::ObDeviceConfigMgr::get_instance())

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_MGR_H_
