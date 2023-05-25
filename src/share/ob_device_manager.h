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

namespace oceanbase
{
namespace common
{

class ObDeviceManager
{
public:
  const static int MAX_DEVICE_INSTANCE = 20;
  int init_devices_env();
  void destroy();
  static ObDeviceManager &get_instance();

  /*for object device, will return a new object to caller*/
  /*ofs/local will share in upper logical*/
  int get_device(const common::ObString& storage_info,
                 const common::ObString& storage_type_prefix,
                 ObIODevice*& device_handle);
  int release_device(common::ObIODevice*& device_handle);
  //for test
  int64_t get_device_cnt() {return device_count_;}

private:
  ObDeviceManager();
  ~ObDeviceManager() { destroy(); }

  struct ObDeviceInsInfo {
    ObIODevice* device_;
    char        storage_info_[OB_MAX_URI_LENGTH];
    int64_t     ref_cnt_;
  };

  /*notice:
  int the implement of hashtable, use the assign fun of class to copy key/value
  but for string, assign fun just copy the pointer, so in device manager, should manager
  key mem space, in case upper lever release the pointer.
  */
  typedef common::hash::ObHashMap<ObString, ObDeviceInsInfo*> StoragInfoDeviceInfoMap;
  typedef common::hash::ObHashMap<int64_t, ObDeviceInsInfo*> DeviceHandleDeviceInfoMap;

  int alloc_device(ObDeviceInsInfo*& device_info,
                   const common::ObString& storage_info,
                   const common::ObString& storage_type_prefix);

  common::ObFIFOAllocator allocator_; /*alloc/free dynamic device mem*/
  int32_t device_count_;
  common::ObSpinLock lock_;  /*the manager is global used, so need lock to guarante thread safe*/
  bool is_init_;
  ObDeviceInsInfo device_ins_[MAX_DEVICE_INSTANCE];
  StoragInfoDeviceInfoMap device_map_;
  DeviceHandleDeviceInfoMap handle_map_; /*the key is a ObIODevice pointer, need cast when used*/
};


}
}

#endif