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

#include "lib/restore/ob_object_device.h"
#include "ob_device_manager.h"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_local_device.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_local_cache_device.h"
#endif

namespace oceanbase
{
namespace common
{
int ObTenantStsCredentialMgr::get_sts_credential(
    char *sts_credential, const int64_t sts_credential_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = ObObjectStorageTenantGuard::get_tenant_id();
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
  uint64 min_cluster_version = GET_MIN_CLUSTER_VERSION();
  if (min_cluster_version < MOCK_CLUSTER_VERSION_4_2_5_0
      || (min_cluster_version >= CLUSTER_VERSION_4_3_0_0
      && min_cluster_version < CLUSTER_VERSION_4_3_5_0)) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "cluster version is too low for assume role", K(ret), K(GET_MIN_CLUSTER_VERSION()));
  }
  return ret;
}

const int ObDeviceManager::MAX_DEVICE_INSTANCE;
ObDeviceManager::ObDeviceManager() : allocator_(), device_count_(0), is_init_(false)
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
      device_ins_[i].device_key_ = NULL;
    }
    if (OB_FAIL(device_map_.create(MAX_DEVICE_INSTANCE*2, "DeviceMng", "DeviceMng"))) {
      OB_LOG(WARN, "fail to create device map", K(ret));
    } else if (OB_FAIL(handle_map_.create(MAX_DEVICE_INSTANCE*2, "DeviceMng", "DeviceMng"))) {
      OB_LOG(WARN, "fail to create handle map", K(ret));
    } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                      OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
      OB_LOG(WARN, "Fail to init allocator ", K(ret));
    } else if (OB_FAIL(lock_.init(mem_attr))) {
      OB_LOG(WARN, "fail to init lock", KR(ret));
    } else if (OB_FAIL(init_oss_env())) {
      OB_LOG(WARN, "fail to init oss storage", K(ret));
    } else if (OB_FAIL(init_cos_env())) {
      OB_LOG(WARN, "fail to init cos storage", K(ret));
    } else if (OB_FAIL(init_s3_env())) {
      OB_LOG(WARN, "fail to init s3 storage", K(ret));
    } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_version_mgr(
        &ObClusterVersionMgr::get_instance()))) {
      OB_LOG(WARN, "fail to register cluster version mgr", K(ret));
    } else if (OB_FAIL(ObStsCredential::register_sts_credential_mgr(
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
  int ret_io_mgr = OB_SUCCESS;
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
      char *del_device_key = device_ins_[i].device_key_;
      if (OB_NOT_NULL(del_device)) {
        ret_io_mgr = ObIOManager::get_instance().remove_device_channel(del_device);
        if (OB_SUCCESS != ret_io_mgr) {
          OB_LOG_RET(WARN, ret_io_mgr, "fail to remove device channel", K(ret_io_mgr), KP(del_device));
        }
        del_device->destroy();
        allocator_.free(del_device);
      }
      if (OB_NOT_NULL(del_device_key)) {
        allocator_.free(del_device_key);
      }
      device_ins_[i].device_ = NULL;
      device_ins_[i].device_key_ = NULL;
      del_device_key = NULL;
    }
    allocator_.reset();
    fin_oss_env();
    fin_cos_env();
    fin_s3_env();
    lock_.destroy();
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
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (storage_type_prefix.prefix_match(OB_LOCAL_CACHE_PREFIX)) {
    device_type = OB_STORAGE_LOCAL_CACHE;
    mem = allocator.alloc(sizeof(storage::ObLocalCacheDevice));
    if (NULL != mem) {new(mem)storage::ObLocalCacheDevice();}
#endif
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
  } else if (storage_type_prefix.prefix_match(OB_HDFS_PREFIX)) {
    device_type = OB_STORAGE_HDFS;
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

int ObDeviceManager::alloc_device_(
    const ObString &storage_type_prefix,
    const ObString &device_key,
    ObDeviceInsInfo *&device_info)
{
  int ret = OB_SUCCESS;
  int64_t last_no_ref_idx = -1;
  int64_t avai_idx = -1;
  ObIODevice *device_handle = nullptr;
  //first validate the key(storage info)
  if (OB_FAIL(parse_storage_info(storage_type_prefix, device_handle, allocator_))) {
    OB_LOG(WARN, "fail to alloc device!", K(ret), K(storage_type_prefix));
  } else {
    //find a device slot
    for (int i = 0; i < MAX_DEVICE_INSTANCE; i++) {
      if (NULL == device_ins_[i].device_) {
        avai_idx = i;
        break;
      } else if ((NULL != device_ins_[i].device_) && (0 == device_ins_[i].device_->get_ref_cnt())) {
        last_no_ref_idx = i;
      }
    }

    if (-1 == avai_idx && -1 == last_no_ref_idx) {
      //cannot insert into device manager
      ret = OB_OUT_OF_ELEMENT;
      OB_LOG(WARN, "devices too many!", KR(ret),
          K(MAX_DEVICE_INSTANCE), K(storage_type_prefix), KP(device_key.ptr()));
    } else {
      //try to release one
      if (-1 == avai_idx && -1 != last_no_ref_idx) {
        //erase from map
        ObIODevice* del_device = device_ins_[last_no_ref_idx].device_;
        ObString old_key(device_ins_[last_no_ref_idx].device_key_);
        if (OB_FAIL(ObIOManager::get_instance().remove_device_channel(del_device))) {
          OB_LOG(WARN, "fail to remove device channel", KR(ret), KP(del_device));
        } else if (OB_FAIL(device_map_.erase_refactored(old_key))) {
          OB_LOG(WARN, "fail to erase device from device map", KP(old_key.ptr()), KR(ret),
              K(storage_type_prefix), KP(device_key.ptr()));
        } else if (OB_FAIL(handle_map_.erase_refactored((int64_t)(device_ins_[last_no_ref_idx].device_)))) {
          OB_LOG(WARN, "fail to erase device from handle map", K(device_ins_[last_no_ref_idx].device_),
                 KR(ret), K(storage_type_prefix), KP(device_key.ptr()));
        } else {
          /*free the resource*/
          del_device->destroy();
          allocator_.free(del_device);
          device_ins_[last_no_ref_idx].device_ = NULL;
          char *del_device_key = device_ins_[last_no_ref_idx].device_key_;
          if (OB_NOT_NULL(del_device_key)) {
            allocator_.free(del_device_key);
            device_ins_[last_no_ref_idx].device_key_ = NULL;
            del_device_key = NULL;
          }
          abort_unless(device_count_ == MAX_DEVICE_INSTANCE);
          device_count_--;
          avai_idx = last_no_ref_idx;
          OB_LOG(INFO, "release one device for realloc another!", KP(old_key.ptr()),
              K(storage_type_prefix), KP(device_key.ptr()));
        }
      }

      if (OB_SUCCESS == ret) {
        //insert into map
        ObString cur_key;
        if (OB_FAIL(ob_write_string(allocator_, device_key, cur_key, true/*c_style*/))) {
          OB_LOG(WARN, "fail to deep copy device key",
              KR(ret), K(storage_type_prefix), KP(device_key.ptr()));
        } else if (FALSE_IT(device_ins_[avai_idx].device_key_ = cur_key.ptr())) {
        } else if (OB_FAIL(device_map_.set_refactored(cur_key, &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to device map!",
              KR(ret), KP(cur_key.ptr()), K(storage_type_prefix));
        } else if (OB_FAIL(handle_map_.set_refactored((int64_t)(device_handle), &(device_ins_[avai_idx])))) {
          OB_LOG(WARN, "fail to set device to handle map!",
              KR(ret), K(storage_type_prefix), KP(device_key.ptr()));
        } else {
          OB_LOG(INFO, "success insert into map!", K(storage_type_prefix), KP(device_key.ptr()));
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
    device_count_++;
    OB_LOG(INFO, "alloc a new device!",
           K(storage_type_prefix), K(avai_idx), K(device_count_), K(device_handle));
    device_info = &(device_ins_[avai_idx]);
  }

  return ret;
}

int ObDeviceManager::inc_device_ref_nolock_(ObDeviceInsInfo *dev_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dev_info)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "dev_info should not be null", KR(ret));
  } else if (OB_ISNULL(dev_info->device_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "device should not be null", KR(ret));
  } else {
    dev_info->device_->inc_ref();
  }
  return ret;
}

int ObDeviceManager::get_deivce_(const ObString &device_key, ObIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceInsInfo *dev_info = nullptr;
  device_handle = nullptr;
  ObQSyncLockReadGuard guard(lock_);

  if (OB_FAIL(device_map_.get_refactored(device_key, dev_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // device not found; defer creation to subsequent steps
    } else {
      OB_LOG(WARN, "fail to get device from device manager ", KR(ret), KP(device_key.ptr()));
    }
    // device_->inc_ref/dec_ref/get_ref_cnt are atomic operations, so acquiring a read lock suffices
  } else if (OB_FAIL(inc_device_ref_nolock_(dev_info))) {
    OB_LOG(WARN, "fail to inc device ref", KR(ret));
  } else {
    device_handle = dev_info->device_;
  }
  return ret;
}

int ObDeviceManager::alloc_device_and_init_(
    const ObString &storage_type_prefix,
    const ObString &device_key,
    const ObStorageIdMod &storage_id_mod,
    ObIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceInsInfo *dev_info = nullptr;
  device_handle = nullptr;
  const int64_t fake_max_io_depth = 256;
  ObQSyncLockWriteGuard guard(lock_);

  // Re-check to see if the device was created while acquiring the lock
  if (OB_FAIL(device_map_.get_refactored(device_key, dev_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // alloc a device, and set into the map
      if (OB_FAIL(alloc_device_(storage_type_prefix, device_key, dev_info))) {
        OB_LOG(WARN, "fail to alloc device!", KR(ret),
            K(storage_type_prefix), KP(device_key.ptr()), K(storage_id_mod));
      // only object device goes here to alloc device channel
      } else if (dev_info->device_->is_object_device()) {
        // Start a new thread under a normal tenant requires the same tenant context
        // So temporarily set expect_run_wrapper to nullptr
        lib::IRunWrapper *run_wrapper = lib::Threads::get_expect_run_wrapper();
        lib::Threads::get_expect_run_wrapper() = NULL;
        DEFER(lib::Threads::get_expect_run_wrapper() = run_wrapper);
        ObResetThreadTenantIdGuard tenant_guard;
        DISABLE_SQL_MEMLEAK_GUARD;
        if (OB_FAIL(ObIOManager::get_instance().add_device_channel(dev_info->device_,
                                                                   0/*async_channel_thread_count*/,
                                                                   GCONF.sync_io_thread_count,
                                                                   fake_max_io_depth))) {
          OB_LOG(WARN, "add device channel failed", K(ret), KP(dev_info), KP(device_key.ptr()));
        } else {
          // set storage_used_mod and storage_id into ObObjectDevice for QoS of ObIOManager
          ObObjectDevice *object_device = nullptr;
          if (OB_ISNULL(object_device = static_cast<ObObjectDevice *>(dev_info->device_))) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "object device is null", K(ret), KP(device_key.ptr()));
          } else {
            object_device->set_storage_id_mod(storage_id_mod);
          }
        }
      }
    } else {
      OB_LOG(WARN, "fail to re-check device existence", KR(ret),
          K(storage_type_prefix), K(storage_id_mod), KP(device_key.ptr()));
    }
  }

  if (FAILEDx(inc_device_ref_nolock_(dev_info))) {
    OB_LOG(WARN, "fail to inc device ref", KR(ret), KP(device_key.ptr()));
  } else {
    device_handle = dev_info->device_;
  }

  return ret;
}

int ObDeviceManager::get_device(
    const ObString &storage_type_prefix,
    const ObObjectStorageInfo &storage_info,
    const ObStorageIdMod &storage_id_mod,
    ObIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  device_handle = nullptr;
  char *tmp_device_key = nullptr;

  if (OB_UNLIKELY(!is_init_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "device manager is not inited", KR(ret));
  } else if (OB_FAIL(get_device_key_(
      allocator, storage_type_prefix, storage_info, storage_id_mod, tmp_device_key))) {
    OB_LOG(WARN, "fail to get device key", KR(ret),
        K(storage_type_prefix), K(storage_info), K(storage_id_mod));
  } else {
    if (OB_FAIL(get_deivce_(tmp_device_key, device_handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(alloc_device_and_init_(
              storage_type_prefix, tmp_device_key, storage_id_mod, device_handle))) {
          OB_LOG(WARN, "fail to alloc device from device manager ", KR(ret),
              K(storage_type_prefix), K(storage_info), K(storage_id_mod), KP(tmp_device_key));
        }
      } else {
        OB_LOG(WARN, "fail to get device from device manager ", KR(ret),
            K(storage_type_prefix), K(storage_info), K(storage_id_mod), KP(tmp_device_key));
      }
    }
  }

  return ret;
}

int ObDeviceManager::get_local_device(
    const ObString &storage_type_prefix,
    const ObStorageIdMod &storage_id_mod,
    ObIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  ObString local_prefix(OB_LOCAL_PREFIX);
  ObString local_cache_prefix(OB_LOCAL_CACHE_PREFIX);
  if (OB_UNLIKELY((0 != storage_type_prefix.compare(local_prefix))
                  && (0 != storage_type_prefix.compare(local_cache_prefix)))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid storage type prefix", K(ret), K(storage_type_prefix));
  } else {
     // local device does not need storage_info and storage_id_mod
    ObObjectStorageInfo default_storage_info;
    if (OB_FAIL(ObDeviceManager::get_instance().get_device(storage_type_prefix, default_storage_info,
                                                           storage_id_mod, device_handle))) {
      OB_LOG(WARN, "fail to get local device", K(ret));
    }
  }
  return ret;
}

/*
* 1、release just modify the ref cnt, no need query map
* 2、when the device cnt exceed max cnt, will destroy a device which ref cnt is 0
*/
int ObDeviceManager::release_device(ObIODevice *&device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceInsInfo *device_info = nullptr;
  // device_->inc_ref/dec_ref/get_ref_cnt are atomic operations, so acquiring a read lock suffices
  ObQSyncLockReadGuard guard(lock_);
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
    if (OB_ISNULL(device_info) || OB_ISNULL(device_info->device_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "Exception: get a null device handle!", K(device_handle));
    } else {
      if (0 >= device_info->device_->get_ref_cnt()) {
        OB_LOG(WARN, "the device ref is 0/small 0, maybe a invalid release!", K(device_info->device_));
        ret = OB_INVALID_ARGUMENT;
      } else {
        abort_unless(device_count_ > 0);
        abort_unless(device_info->device_->get_ref_cnt() > 0);
        device_info->device_->dec_ref();
        if (0 == device_info->device_->get_ref_cnt()) {
          OB_LOG(DEBUG, "A Device has no others ref", K(device_info->device_), KP(device_info->device_key_));
        } else {
          OB_LOG(DEBUG, "released dev info", K(device_info->device_), K(device_info->device_->get_ref_cnt()));
        }
        device_handle = NULL;
      }
    }
  }
  return ret;
}

int ObDeviceManager::get_device_key_(
    ObIAllocator &allcator,
    const ObString &storage_type_prefix,
    const ObObjectStorageInfo &storage_info,
    const ObStorageIdMod &storage_id_mod,
    char *&device_key)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(device_key)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "device key is already not null", K(ret));
  } else if (storage_type_prefix.prefix_match(OB_LOCAL_PREFIX)) {
    // uint64_t occupies up to 20 characters.
    // 20(storage_used_mod_) + 20(storage_id_) + 2(two '&') + 1(one '\0') = 43.
    // reserve some free space, increase 43 to 50.
    const int64_t alloc_size = STRLEN(OB_LOCAL_PREFIX) + 50;
    if (OB_ISNULL(device_key = static_cast<char *>(allcator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for device key", K(ret), K(alloc_size));
    } else if (OB_FAIL(databuff_printf(device_key, alloc_size, "%s&%lu&%lu",
                                       OB_LOCAL_PREFIX,
                                       (uint64_t)storage_id_mod.storage_used_mod_,
                                       storage_id_mod.storage_id_))) {
      OB_LOG(WARN, "fail to construct device key", K(ret));
    }
  } else if (storage_type_prefix.prefix_match(OB_LOCAL_CACHE_PREFIX)) {
    // uint64_t occupies up to 20 characters.
    // 20(storage_used_mod_) + 20(storage_id_) + 2(two '&') + 1(one '\0') = 43.
    // reserve some free space, increase 43 to 50.
    const int64_t alloc_size = STRLEN(OB_LOCAL_CACHE_PREFIX) + 50;
    if (OB_ISNULL(device_key = static_cast<char *>(allcator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for device key", K(ret), K(alloc_size));
    } else if (OB_FAIL(databuff_printf(device_key, alloc_size, "%s&%lu&%lu",
                                       OB_LOCAL_CACHE_PREFIX,
                                       (uint64_t)storage_id_mod.storage_used_mod_,
                                       storage_id_mod.storage_id_))) {
      OB_LOG(WARN, "fail to construct device key", K(ret));
    }
  } else if (storage_type_prefix.prefix_match(OB_FILE_PREFIX)) {
    // uint64_t occupies up to 20 characters.
    // 20(storage_used_mod_) + 20(storage_id_) + 2(two '&') + 1(one '\0') = 43.
    // reserve some free space, increase 43 to 50.
    const int64_t alloc_size = STRLEN(OB_FILE_PREFIX) + 50;
    if (OB_ISNULL(device_key = static_cast<char *>(allcator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for device key", K(ret), K(alloc_size));
    } else if (OB_FAIL(databuff_printf(device_key, alloc_size, "%s&%lu&%lu",
                                       OB_FILE_PREFIX,
                                       (uint64_t)storage_id_mod.storage_used_mod_,
                                       storage_id_mod.storage_id_))) {
      OB_LOG(WARN, "fail to construct device map key", K(ret), K(storage_id_mod));
    }
  } else if (storage_type_prefix.prefix_match(OB_OSS_PREFIX)
             || storage_type_prefix.prefix_match(OB_COS_PREFIX)
             || storage_type_prefix.prefix_match(OB_S3_PREFIX)
             || storage_type_prefix.prefix_match(OB_HDFS_PREFIX)) {
    const int64_t storage_info_key_len = storage_info.get_device_map_key_len();
    char storage_info_key_str[storage_info_key_len];
    storage_info_key_str[0] = '\0';
    // uint64_t occupies up to 20 characters.
    // 20(storage_used_mod_) + 20(storage_id_) + 2(two '&') + 1(one '\0') = 43.
    // reserve some free space, increase 43 to 50.
    const int64_t alloc_size = storage_info_key_len + 50;
    if (OB_ISNULL(device_key = static_cast<char *>(allcator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc mem for device key", K(ret), K(alloc_size));
    } else if (OB_FAIL(storage_info.get_device_map_key_str(storage_info_key_str, storage_info_key_len))) {
      OB_LOG(WARN, "fail to get device map key str", K(ret));
    } else if (OB_FAIL(databuff_printf(device_key, alloc_size, "%s&%lu&%lu",
                                       storage_info_key_str,
                                       (uint64_t)storage_id_mod.storage_used_mod_,
                                       storage_id_mod.storage_id_))) {
      OB_LOG(WARN, "fail to construct device map key", K(ret), K(storage_id_mod));
    }
  } else {
    ret = OB_INVALID_BACKUP_DEST; // keep the same errno with old-version observer
    OB_LOG(WARN, "invalid storage type prefix", K(ret), K(storage_type_prefix), K(storage_id_mod));
  }
  return ret;
}


}
}
