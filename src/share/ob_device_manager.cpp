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

#include "common/storage/ob_device_common.h"
#include "lib/oblog/ob_log.h"
#include "lib/restore/ob_object_device.h"
#include "ob_device_manager.h"
#include "share/ob_local_device.h"

namespace oceanbase
{
namespace common
{

const int ObDeviceManager::MAX_DEVICE_INSTANCE;
ObDeviceManager::ObDeviceManager() : allocator_(), device_count_(0), lock_(ObLatchIds::LOCAL_DEVICE_LOCK), is_init_(false)
{
}

int ObDeviceManager::init_devices_env()
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(OB_SYS_TENANT_ID, "DEVICE_MANAGER");
  if (is_init_) {
    //do nothing, does not return error code
  } else {
    //init device manager
    for (int i = 0; i < MAX_DEVICE_INSTANCE; i++ ) {
      device_ins_[i].device_ = NULL;
      device_ins_[i].ref_cnt_ = 0;
      device_ins_[i].storage_info_[0] = '\0';
    }
    if (OB_FAIL(device_map_.create(MAX_DEVICE_INSTANCE*2, "DeviceMng", "DeviceMng"))) {
      OB_LOG(WARN, "fail to create device map", K(ret));
    } else if (OB_FAIL(handle_map_.create(MAX_DEVICE_INSTANCE*2, "DeviceMng", "DeviceMng"))) {
      OB_LOG(WARN, "fail to create handle map", K(ret));
    } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                      OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
      OB_LOG(WARN, "Fail to init allocator ", K(ret));
    } else if (OB_FAIL(init_oss_env())) {
      OB_LOG(WARN, "fail to init oss storage", K(ret));
    } else if (OB_FAIL(init_cos_env())) {
      OB_LOG(WARN, "fail to init cos storage", K(ret));
    }
  }

  if (OB_SUCCESS == ret) {
    is_init_ = true;
  } else {
    /*release the resource*/
    destroy();
  }
  return ret;
}

void ObDeviceManager::destroy()
{
  int ret_dev = OB_SUCCESS;
  int ret_handle = OB_SUCCESS;
  /*destroy fun wil release all the node*/
  if (is_init_) {
    ret_dev = device_map_.destroy();
    ret_handle = handle_map_.destroy();
    if (OB_SUCCESS != ret_dev || OB_SUCCESS != ret_handle) {
      OB_LOG_RET(WARN, ret_dev, "fail to destroy device map", K(ret_dev), K(ret_handle));
    }
    for (int i = 0; i < MAX_DEVICE_INSTANCE; i++ ) {
      ObIODevice* del_device = device_ins_[i].device_;
      if (OB_NOT_NULL(del_device)) {
        del_device->destroy();
        allocator_.free(del_device);
      }
      device_ins_[i].device_ = NULL;
    }
    allocator_.reset();
    fin_oss_env();
    fin_cos_env();
    is_init_ = false;
    device_count_ = 0;
    OB_LOG_RET(WARN, ret_dev, "release the init resource", K(ret_dev), K(ret_handle));
  }
  OB_LOG(INFO, "destroy device manager!");
}

ObDeviceManager& ObDeviceManager::get_instance()
{
  static ObDeviceManager static_instance;
  return static_instance;
}

/*parse storage info, get device type, alloc device mem*/
int parse_storage_info(common::ObString storage_type_prefix, ObIODevice*& device_handle, common::ObFIFOAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;
  void* mem = NULL;

  if (storage_type_prefix.prefix_match(OB_LOCAL_PREFIX)) {
    device_type = OB_STORAGE_LOCAL;
    mem = allocator.alloc(sizeof(share::ObLocalDevice));
    if (NULL != mem) {new(mem)share::ObLocalDevice();}
  } else if (storage_type_prefix.prefix_match(OB_FILE_PREFIX)) {
    device_type = OB_STORAGE_FILE;
    mem = allocator.alloc(sizeof(ObObjectDevice));
    if (NULL != mem) {new(mem)ObObjectDevice;}
  } else if (storage_type_prefix.prefix_match(OB_OSS_PREFIX)) {
    device_type = OB_STORAGE_OSS;
    mem = allocator.alloc(sizeof(ObObjectDevice));
    if (NULL != mem) {new(mem)ObObjectDevice;}
  } else if (storage_type_prefix.prefix_match(OB_COS_PREFIX)) {
    device_type = OB_STORAGE_COS;
    mem = allocator.alloc(sizeof(ObObjectDevice));
    if (NULL != mem) {new(mem)ObObjectDevice;}
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    OB_LOG(WARN, "invaild device name info!", K(storage_type_prefix));
  }

  if (OB_SUCCESS != ret) {
  } else if (OB_ISNULL(mem)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for device ins!", K(storage_type_prefix));
  } else {
    device_handle = static_cast<ObIODevice*>(mem);
    device_handle->device_type_ = device_type;
  }

  return ret;
}

int ObDeviceManager::alloc_device(ObDeviceInsInfo*& device_info,
                                  const common::ObString& storage_info,
                                  const common::ObString& storage_type_prefix)
{
  int ret = OB_SUCCESS;
  int64_t last_no_ref_idx = -1;
  int64_t avai_idx = -1;
  ObIODevice* device_handle = NULL;
  //first validate the key(storage info)
  if (OB_FAIL(parse_storage_info(storage_type_prefix, device_handle, allocator_))) {
    OB_LOG(WARN, "fail to alloc device!", K(ret), K(storage_type_prefix));
  } else {
    //find a device slot
    for (int i = 0; i < MAX_DEVICE_INSTANCE; i++) {
      if (0 == device_ins_[i].ref_cnt_ && NULL == device_ins_[i].device_) {
        avai_idx = i;
        break;
      } else if (0 == device_ins_[i].ref_cnt_ && NULL != device_ins_[i].device_) {
        last_no_ref_idx = i;
      }
    }

    if (-1 == avai_idx && -1 == last_no_ref_idx) {
      OB_LOG(WARN, "devices too mang!", K(MAX_DEVICE_INSTANCE), K(storage_info), K(storage_type_prefix));
      //cannot insert into device manager
      ret = OB_OUT_OF_ELEMENT;
    } else {
      //try to release one
      if (-1 == avai_idx && -1 != last_no_ref_idx) {
        //erase from map
        ObString old_key(device_ins_[last_no_ref_idx].storage_info_);
        if (OB_FAIL(device_map_.erase_refactored(old_key))) {
          OB_LOG(WARN, "fail to erase device from device map", K(old_key), K(ret), K(storage_info), K(storage_type_prefix));
        } else if (OB_FAIL(handle_map_.erase_refactored((int64_t)(device_ins_[last_no_ref_idx].device_)))) {
          OB_LOG(WARN, "fail to erase device from handle map", K(device_ins_[last_no_ref_idx].device_), K(ret), K(storage_info), K(storage_type_prefix));
        } else {
          /*free the resource*/
          ObIODevice* del_device = device_ins_[last_no_ref_idx].device_;
          del_device->destroy();
          allocator_.free(del_device);
          device_ins_[last_no_ref_idx].device_ = NULL;
          abort_unless(device_count_ == MAX_DEVICE_INSTANCE);
          device_count_--;
          avai_idx = last_no_ref_idx;
          OB_LOG(INFO, "release one device for realloc another!", K(old_key), K(storage_info), K(storage_type_prefix));
        }
      }

      if (OB_SUCCESS == ret) {
        //insert into map
        STRCPY(device_ins_[avai_idx].storage_info_, storage_info.ptr());
        ObString cur_key(device_ins_[avai_idx].storage_info_);
        if (OB_FAIL(device_map_.set_refactored(cur_key, &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to device map!", K(ret), K(cur_key), K(storage_type_prefix));
        } else if (OB_FAIL(handle_map_.set_refactored((int64_t)(device_handle), &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to handle map!", K(ret), K(storage_info), K(storage_type_prefix));
        } else {
          OB_LOG(INFO, "success insert into map!", K(storage_info), K(storage_type_prefix));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != device_handle) {
      allocator_.free(device_handle);
    }
    device_info = NULL;
  } else {
    device_ins_[avai_idx].device_ = device_handle;
    device_ins_[avai_idx].ref_cnt_ = 0;
    device_count_++;
    OB_LOG(INFO, "alloc a new device!", K(storage_info), K(storage_type_prefix), K(avai_idx), K(device_count_), K(device_handle));
    device_info = &(device_ins_[avai_idx]);
  }

  return ret;
}

int ObDeviceManager::get_device(const common::ObString& storage_info,
                                const common::ObString& storage_type_prefix,
                                ObIODevice*& device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceInsInfo* dev_info = NULL;
  device_handle = NULL;
  ObString storage_info_tmp;
//const_cast
  common::ObSpinLockGuard guard(lock_);
  if (!is_init_) {
    OB_LOG(INFO, "try to init device manager!");
    init_devices_env();
  }

  if (OB_ISNULL(storage_info.ptr())) {
    if (storage_type_prefix.prefix_match(OB_FILE_PREFIX)) {
      storage_info_tmp.assign_ptr(const_cast<char*>(OB_FILE_PREFIX), strlen(OB_FILE_PREFIX));
    } else {
      ret = OB_INVALID_BACKUP_DEST;
      OB_LOG(WARN, "invalid storage info(null, and not file storage)!", K(ret), K(storage_type_prefix));
    }
  } else {
    storage_info_tmp.assign_ptr(const_cast<char*>(storage_info.ptr()), storage_info.length());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(device_map_.get_refactored(storage_info_tmp, dev_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //alloc a device, and set into the map
      if (OB_FAIL(alloc_device(dev_info, storage_info_tmp, storage_type_prefix))) {
        OB_LOG(WARN, "fail to alloc device!", K(ret), K(storage_info_tmp), K(storage_type_prefix));
      }
    } else {
      OB_LOG(WARN, "fail to get device from device manager ", K(ret), K(storage_info_tmp), K(storage_type_prefix));
    }
  }

  if (NULL != dev_info) {
    dev_info->ref_cnt_++;
    device_handle = dev_info->device_;
  }
  return ret;
}

/*
* 1、release just modify the ref cnt, no need query map
* 2、when the device cnt exceed max cnt, will destroy a device which ref cnt is 0
*/
int ObDeviceManager::release_device(ObIODevice*& device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceInsInfo* device_info = NULL;
  common::ObSpinLockGuard guard(lock_);
  if (!is_init_) {
    OB_LOG(WARN, "device manager not init!");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "device_handle is null!");
  } else if (OB_FAIL(handle_map_.get_refactored((int64_t)(device_handle), device_info))) {
    OB_LOG(WARN, "fail to get device by handle!", K(ret));
  }

  if (OB_SUCCESS == ret) {
    if (OB_ISNULL(device_info)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "Exception: get a null device handle!", K(device_handle));
    } else {
      if (0 >= device_info->ref_cnt_) {
        OB_LOG(WARN, "the device ref is 0/small 0, maybe a invalid release!", K(device_info->device_));
        ret = OB_INVALID_ARGUMENT;
      } else {
        abort_unless(device_count_ > 0);
        abort_unless(device_info->ref_cnt_ > 0);
        device_info->ref_cnt_--;
        if (0 == device_info->ref_cnt_) {
          OB_LOG(DEBUG, "A Device has no others ref", K(device_info->device_), K(device_info->storage_info_));
        } else {
          OB_LOG(DEBUG, "released dev info", K(device_info->device_), K(device_info->ref_cnt_));
        }
        device_handle = NULL;
      }
    }
  }
  return ret;
}

}
}
