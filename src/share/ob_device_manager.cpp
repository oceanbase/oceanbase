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
#include "lib/restore/ob_storage_obdal_base.h"
#include "ob_device_manager.h"
#include "share/ob_cluster_version.h"
#include "share/ob_local_device.h"
#include "logservice/ob_log_device.h"

namespace oceanbase
{
namespace common
{
int ObTenantStsCredentialMgr::get_sts_credential(
    char *sts_credential, const int64_t sts_credential_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = MTL_ID();
  if (OB_ISNULL(sts_credential) || OB_UNLIKELY(sts_credential_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret),
        K(tenant_id), KP(sts_credential), K(sts_credential_buf_len));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    // If the tenant is invalid or illegal, the sts_credential of the system tenant will be used as
    // a backup. Please refer to the following document for specific reasons.
    //
    tenant_id = OB_SYS_TENANT_ID;
    OB_LOG(WARN, "invalid tenant ctx, use sys tenant", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    if (is_meta_tenant(tenant_id)) {
      tenant_id = gen_user_tenant_id(tenant_id);
    }
    const char *tmp_credential = nullptr;

    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    int tmp_ret = OB_SUCCESS;
    // If the tenant does not have sts_credential, return OB_EAGAIN to wait for the next try.
    if (OB_TMP_FAIL(check_sts_credential(tenant_config))) {
      ret = OB_EAGAIN;
      OB_LOG(WARN, "fail to check sts credential, should try again", K(ret), K(tmp_ret), K(tenant_id));
    } else {
      tmp_credential = tenant_config->sts_credential;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(sts_credential, sts_credential_buf_len,
                                       "%s", tmp_credential))) {
        OB_LOG(WARN, "fail to deep copy sts_credential", K(ret), K(tenant_id), KP(tmp_credential));
      } else if (OB_UNLIKELY(sts_credential[0] == '\0')) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "sts_credential is null", K(ret), K(tenant_id), KP(tmp_credential));
      }
      OB_LOG(INFO, "get sts credential successfully", K(tenant_id));
    }
    if (OB_FAIL(ret) && REACH_TIME_INTERVAL(LOG_INTERVAL_US)) {
      OB_LOG(WARN, "try to get sts credential", K(ret), K(tenant_id), KP(tmp_credential));
    }
  }
  return ret;
}

int ObTenantStsCredentialMgr::check_sts_credential(omt::ObTenantConfigGuard &tenant_config) const
{
  int ret = OB_SUCCESS;
  const char *sts_credential = nullptr;
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "tenant config is invalid", K(ret));
  } else if (OB_ISNULL(sts_credential = tenant_config->sts_credential)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "tenant config is invalid", K(ret), KP(sts_credential));
  } else if (OB_UNLIKELY(sts_credential[0] == '\0')) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "sts_credential is null", K(ret), KP(sts_credential));
  }
  return ret;
}

int ObClusterVersionMgr::is_supported_assume_version() const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_5_0) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "cluster version is too low for assume role", K(ret), K(GET_MIN_CLUSTER_VERSION()));
  }
  return ret;
}

int ObClusterVersionMgr::is_supported_azblob_version() const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t min_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    OB_LOG(WARN, "fail to get min data version, use cluster version", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    if (min_data_version < DATA_VERSION_4_2_5_6) {
      ret = OB_NOT_SUPPORTED;
      OB_LOG(WARN, "data version is too low for azblob", K(ret), K(min_data_version));
    }
  } else {
    ret = OB_SUCCESS;
    const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
    if (min_cluster_version < CLUSTER_VERSION_4_2_5_6) {
      ret = OB_NOT_SUPPORTED;
      OB_LOG(WARN, "cluster version is too low for azblob", K(ret), K(min_cluster_version));
    }
  }
  return ret;
}

int ObClusterVersionMgr::is_supported_enable_worm_version() const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  uint64_t min_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, min_data_version))) {
    OB_LOG(WARN, "fail to get min data version, use cluster version", K(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    if (min_data_version < DATA_VERSION_4_2_5_7) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "setting 'enable_worm' for data version lower than 4.2.5.7 is");
      OB_LOG(WARN, "setting 'enable_worm' for data version lower than 4.2.5.7 is not supported",
                K(ret), K(min_data_version));
    }
  } else {
    ret = OB_SUCCESS;
    const uint64_t min_cluster_version = GET_MIN_CLUSTER_VERSION();
    if (min_cluster_version < CLUSTER_VERSION_4_2_5_7) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "setting 'enable_worm' for cluster version lower than 4.2.5.7 is");
      OB_LOG(WARN, "setting 'enable_worm' for cluster version lower than 4.2.5.7 is not supported",
                K(ret), K(min_cluster_version));
    }
  }
  return ret;
}

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
    } else if (OB_FAIL(init_s3_env())) {
      OB_LOG(WARN, "fail to init s3 storage", K(ret));
    } else if (OB_FAIL(init_obdal_env())) {
      OB_LOG(WARN, "fail to init obdal env", K(ret));
    } else if (OB_FAIL(logservice::init_log_store_env())) {
      OB_LOG(WARN, "fail to init s3 storage", K(ret));
    } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_version_mgr(
        &ObClusterVersionMgr::get_instance()))) {
      OB_LOG(WARN, "fail to register cluster version mgr", K(ret));
    } else if (OB_FAIL(ObDeviceCredentialKey::register_sts_credential_mgr(
        &ObTenantStsCredentialMgr::get_instance()))) {
      OB_LOG(WARN, "fail to register sts crendential", K(ret));
    } else if (OB_FAIL(ObDeviceCredentialMgr::get_instance().init())) {
      OB_LOG(WARN, "fail to init device credential mgr", K(ret));
    } else {
      // When compliantRfc3986Encoding is set to true:
      // - Adhere to RFC 3986 by supporting the encoding of reserved characters
      //   such as '-', '_', '.', '$', '@', etc.
      // - This approach mitigates inconsistencies in server behavior when accessing
      //   COS using the S3 SDK.
      // Otherwise, the reserved characters will not be encoded,
      // following the default behavior of the S3 SDK.
      const bool compliantRfc3986Encoding =
          (0 == ObString(GCONF.ob_storage_s3_url_encode_type).case_compare("compliantRfc3986Encoding"));
      Aws::Http::SetCompliantRfc3986Encoding(compliantRfc3986Encoding);
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
    fin_s3_env();
    fin_obdal_env();
    logservice::fin_log_store_env();
    ObDeviceCredentialMgr::get_instance().destroy();
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
  } else if (storage_type_prefix.prefix_match(OB_S3_PREFIX)) {
    device_type = OB_STORAGE_S3;
    mem = allocator.alloc(sizeof(ObObjectDevice));
    if (NULL != mem) {new(mem)ObObjectDevice;}
  } else if (storage_type_prefix.prefix_match(OB_LOG_STORE_PREFIX)) {
    device_type = OB_STORAGE_LOG_STORE;
    mem = allocator.alloc(sizeof(logservice::ObLogDevice));
    if (NULL != mem) {new(mem)logservice::ObLogDevice;}
  } else if (storage_type_prefix.prefix_match(OB_AZBLOB_PREFIX)) {
    device_type = OB_STORAGE_AZBLOB;
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
      OB_LOG(WARN, "devices too mang!", K(MAX_DEVICE_INSTANCE), KP(storage_info.ptr()), K(storage_type_prefix));
      //cannot insert into device manager
      ret = OB_OUT_OF_ELEMENT;
    } else {
      //try to release one
      if (-1 == avai_idx && -1 != last_no_ref_idx) {
        //erase from map
        ObString old_key(device_ins_[last_no_ref_idx].storage_info_);
        if (OB_FAIL(device_map_.erase_refactored(old_key))) {
          OB_LOG(WARN, "fail to erase device from device map",
              KP(old_key.ptr()), K(ret), KP(storage_info.ptr()), K(storage_type_prefix));
        } else if (OB_FAIL(handle_map_.erase_refactored((int64_t)(device_ins_[last_no_ref_idx].device_)))) {
          OB_LOG(WARN, "fail to erase device from handle map", K(ret),
              K(device_ins_[last_no_ref_idx].device_), KP(storage_info.ptr()), K(storage_type_prefix));
        } else {
          /*free the resource*/
          ObIODevice* del_device = device_ins_[last_no_ref_idx].device_;
          del_device->destroy();
          allocator_.free(del_device);
          device_ins_[last_no_ref_idx].device_ = NULL;
          abort_unless(device_count_ == MAX_DEVICE_INSTANCE);
          device_count_--;
          avai_idx = last_no_ref_idx;
          OB_LOG(INFO, "release one device for realloc another!",
              KP(old_key.ptr()), KP(storage_info.ptr()), K(storage_type_prefix));
        }
      }

      if (OB_SUCCESS == ret) {
        //insert into map
        STRCPY(device_ins_[avai_idx].storage_info_, storage_info.ptr());
        ObString cur_key(device_ins_[avai_idx].storage_info_);
        if (OB_FAIL(device_map_.set_refactored(cur_key, &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to device map!",
              K(ret), KP(storage_info.ptr()), K(storage_type_prefix));
        } else if (OB_FAIL(handle_map_.set_refactored((int64_t)(device_handle), &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to handle map!",
              K(ret), KP(storage_info.ptr()), K(storage_type_prefix));
        } else {
          OB_LOG(INFO, "success insert into map!",
              KP(storage_info.ptr()), K(storage_type_prefix));
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
    OB_LOG(INFO, "alloc a new device!", KP(storage_info.ptr()),
        K(storage_type_prefix), K(avai_idx), K(device_count_), K(device_handle));
    device_info = &(device_ins_[avai_idx]);
  }

  return ret;
}

int ObDeviceManager::get_device_key(const common::ObString &storage_info,
                                    const common::ObString &uri,
                                    char *device_key,
                                    const int64_t device_key_len) const
{
  int ret = OB_SUCCESS;
  ObStorageType storage_type = OB_STORAGE_MAX_TYPE;
  if (OB_ISNULL(device_key) || OB_UNLIKELY(device_key_len <= 0
      || storage_info.empty() || uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(device_key), K(device_key_len),
        K(storage_info.length()), KP(storage_info.ptr()), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, storage_type))) {
    OB_LOG(WARN, "fail to get storage type from path", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(device_key, device_key_len, "%.*s&storage_type=%d",
                 storage_info.length(), storage_info.ptr(), storage_type))) {
    OB_LOG(WARN, "fail to get device key", K(ret), K(device_key_len), K(uri), K(storage_type));
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
  char device_key[OB_MAX_DEVICE_KEY_LENGTH] = {'\0'};

//const_cast
  common::ObSpinLockGuard guard(lock_);
  if (!is_init_) {
    OB_LOG(INFO, "try to init device manager!");
    init_devices_env();
  }

  if (storage_type_prefix.prefix_match(OB_FILE_PREFIX)) {
    storage_info_tmp.assign_ptr(OB_FILE_PREFIX, strlen(OB_FILE_PREFIX));
  } else if (storage_type_prefix.prefix_match(OB_LOG_STORE_PREFIX)) {
    storage_info_tmp.assign_ptr(OB_LOG_STORE_PREFIX, strlen(OB_LOG_STORE_PREFIX));
  } else if (storage_type_prefix.prefix_match(OB_LOCAL_PREFIX)) {
    storage_info_tmp.assign_ptr(storage_info.ptr(), storage_info.length());
  } else if (storage_type_prefix.prefix_match(OB_OSS_PREFIX)
            || storage_type_prefix.prefix_match(OB_COS_PREFIX)
            || storage_type_prefix.prefix_match(OB_S3_PREFIX)
            || storage_type_prefix.prefix_match(OB_AZBLOB_PREFIX)) {
    if (OB_FAIL(get_device_key(storage_info, storage_type_prefix,
        device_key, OB_MAX_DEVICE_KEY_LENGTH))) {
      OB_LOG(WARN, "fail to get device key!", K(ret), KP(storage_info.ptr()), K(storage_type_prefix));
    } else {
      storage_info_tmp.assign_ptr(device_key, strlen(device_key));
    }
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    OB_LOG(WARN, "invaild device prefix!", K(ret), KP(storage_info.ptr()), K(storage_type_prefix));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(device_map_.get_refactored(storage_info_tmp, dev_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //alloc a device, and set into the map
      if (OB_FAIL(alloc_device(dev_info, storage_info_tmp, storage_type_prefix))) {
        OB_LOG(WARN, "fail to alloc device!",
            K(ret), KP(storage_info_tmp.ptr()), K(storage_type_prefix));
      }
    } else {
      OB_LOG(WARN, "fail to get device from device manager ",
          K(ret), KP(storage_info_tmp.ptr()), K(storage_type_prefix));
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
          OB_LOG(DEBUG, "A Device has no others ref", K(device_info->device_), KP(device_info->storage_info_));
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
