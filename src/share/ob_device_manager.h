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

#ifndef SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_MANAGER_H_
#define SRC_LIBRARY_SRC_COMMON_STORAGE_OB_DEVICE_MANAGER_H_

#include "common/storage/ob_io_device.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_qsync_lock.h"
#include "lib/restore/ob_storage_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace common
{

class ObObjectStorageInfo;
class ObTenantStsCredentialMgr : public ObTenantStsCredentialBaseMgr
{
public:
  ObTenantStsCredentialMgr() {}
  virtual ~ObTenantStsCredentialMgr() {}
  virtual int get_sts_credential(char *sts_credential, const int64_t sts_credential_buf_len) override;
  virtual int check_sts_credential(omt::ObTenantConfigGuard &tenant_config) const;
  static ObTenantStsCredentialBaseMgr &get_instance()
  {
    static ObTenantStsCredentialMgr mgr;
    return mgr;
  }
  const static int64_t LOG_INTERVAL_US = 5 * 1000 * 1000; // 5s
};

class ObClusterStateMgr: public ObClusterStateBaseMgr
{
public:
  ObClusterStateMgr() {}
  virtual ~ObClusterStateMgr() {}
  virtual int is_supported_assume_version() const override;
  virtual int is_supported_enable_worm_version() const override;
  virtual int is_supported_azblob_version() const override;
  virtual bool is_shared_storage_mode() const override;
  virtual bool is_write_with_if_match() const override;
  virtual bool is_enable_obdal() const override;
  static ObClusterStateMgr &get_instance()
  {
    static ObClusterStateMgr mgr;
    return mgr;
  }
};

class ObDeviceManager
{
public:
  const static int MAX_DEVICE_INSTANCE = 50;
  int init_devices_env();
  void destroy();
  static ObDeviceManager &get_instance();

  /*for object device, will return a new object to caller*/
  /*ofs/local will share in upper logical*/
  // 1. ObObjectStorageInfo is a member of ObObjectDevice, which is used for accessing object storage.
  //    Hence, different ObObjectStorageInfo should use different ObObjectDevice.
  // 2. ObStorageIdMod is a member of ObObjectDevice, which is used for ObIOManager QoS.
  //    Hence, different ObStorageIdMod should use different ObObjectDevice.
  // 3. In summary, ObDeviceManager::get_device should be based on both ObObjectStorageInfo and ObStorageIdMod.
  int get_device(const ObString &uri,
                 const ObObjectStorageInfo &storage_info,
                 const ObStorageIdMod &storage_id_mod,
                 ObIODevice *&device_handle);
  // get ObLocalDevice or ObLocalCacheDevice.
  // @storage_type_prefix only allows OB_LOCAL_PREFIX and OB_LOCAL_CACHE_PREFIX.
  static int get_local_device(const ObString &storage_type_prefix,
                              const ObStorageIdMod &storage_id_mod,
                              ObIODevice *&device_handle);
  int release_device(common::ObIODevice*& device_handle);
  //for test
  int64_t get_device_cnt() {return device_count_;}

private:
  ObDeviceManager();
  ~ObDeviceManager() { destroy(); }

  struct ObDeviceInsInfo {
    ObIODevice* device_;
    char *device_key_; // dynamically alloc memory
  };

  /*notice:
  int the implement of hashtable, use the assign fun of class to copy key/value
  but for string, assign fun just copy the pointer, so in device manager, should manager
  key mem space, in case upper lever release the pointer.
  */
  typedef common::hash::ObHashMap<ObString, ObDeviceInsInfo*> StoragInfoDeviceInfoMap;
  typedef common::hash::ObHashMap<int64_t, ObDeviceInsInfo*> DeviceHandleDeviceInfoMap;

  int alloc_device_(const ObString &uri,
                    const ObString &device_key,
                    ObDeviceInsInfo *&device_info);
  int get_device_key_(ObIAllocator &allcator,
                      const ObString &uri,
                      const ObObjectStorageInfo &storage_info,
                      const ObStorageIdMod &storage_id_mod,
                      char *&device_key);
  int inc_device_ref_nolock_(ObDeviceInsInfo *dev_info);
  int get_deivce_(const ObString &device_key, ObIODevice *&device_handle);
  int alloc_device_and_init_(const ObString &uri,
                             const ObString &device_key,
                             const ObStorageIdMod &storage_id_mod,
                             ObIODevice *&device_handle);

  common::ObFIFOAllocator allocator_; /*alloc/free dynamic device mem*/
  int32_t device_count_;
  common::ObQSyncLock lock_;  /*the manager is global used, so need lock to guarante thread safe*/
  bool is_init_;
  ObDeviceInsInfo device_ins_[MAX_DEVICE_INSTANCE];
  StoragInfoDeviceInfoMap device_map_;
  DeviceHandleDeviceInfoMap handle_map_; /*the key is a ObIODevice pointer, need cast when used*/
};


}
}

#endif
