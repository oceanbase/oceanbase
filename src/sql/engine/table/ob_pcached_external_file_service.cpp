/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "ob_pcached_external_file_service.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "share/backup/ob_backup_io_adapter.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace sql
{
/*-----------------------------------------ObExternalFilePathMap-----------------------------------------*/
ObExternalFilePathMap::ObExternalFilePathMap()
    : is_inited_(false),
      mem_limiter_(),
      allocator_(),
      path_id_map_(),
      macro_id_map_()
{}

ObExternalFilePathMap::~ObExternalFilePathMap()
{
  destroy();
}

void ObExternalFilePathMap::destroy()
{
  is_inited_ = false;
  path_id_map_.destroy();
  macro_id_map_.destroy();
  allocator_.destroy();
}

int ObExternalFilePathMap::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = -1;
  const int64_t GB = 1 * 1024L * 1024L * 1024L;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  if ((tenant_mem / GB) <= 4) { // tenant_mem <= 4GB
    // 2 * 9001 buckets, which cost 0.42 memory
    bucket_num = 9001;
  } else { // tenant_mem > 4GB
    // 2 * 196613 buckets, which cost 9.17MB memory
    bucket_num = 196613;
  }

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalFilePathMap init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(path_id_map_.create(bucket_num, ObMemAttr(tenant_id, "ExtPathIdMap")))) {
    LOG_WARN("fail to create path_id_map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(macro_id_map_.create(bucket_num, ObMemAttr(tenant_id, "ExtMacroIdMap")))) {
    LOG_WARN("fail to create macro_id_map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(allocator_.init(
      DEFAULT_BLOCK_SIZE, mem_limiter_, ObMemAttr(tenant_id, "ExtPathAlloc")))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObExternalFilePathMap::get_or_generate(
    const common::ObString &url,
    const int64_t modify_time,
    const int64_t offset,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  uint64_t server_seq_id = UINT64_MAX;
  PathMapKey path_map_key(url, modify_time);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!path_map_key.is_valid() || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path_map_key), K(offset));
  } else if (OB_FAIL(get_or_generate_server_seq_id_(path_map_key, server_seq_id))) {
    LOG_WARN("fail to get_or_generate_server_seq_id_", KR(ret), K(path_map_key), K(offset));
  } else {
    MacroMapKey macro_map_key(server_seq_id, offset / OB_STORAGE_OBJECT_MGR.get_macro_block_size());
    if (OB_FAIL(get_or_generate_macro_id_(macro_map_key, macro_id))) {
      LOG_WARN("fail to get_or_generate_macro_id_", KR(ret), K(path_map_key), K(macro_map_key));
    }
  }
  return ret;
}

int ObExternalFilePathMap::generate_server_seq_id_(uint64_t &server_seq_id) const
{
  // TODO @fangdan: support SN mode
  int ret = OB_SUCCESS;
  server_seq_id = UINT64_MAX;
  if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(server_seq_id))) {
    LOG_WARN("fail to get write seq", KR(ret));
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::generate_macro_id_(
    const MacroMapKey &macro_map_key,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (OB_UNLIKELY(!macro_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_map_key));
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    ObStorageObjectOpt opt;
    opt.set_ss_external_table_file_opt(macro_map_key.server_seq_id_, macro_map_key.offset_idx_);
    if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, macro_id))) {
      LOG_WARN("fail to get object id", KR(ret), K(macro_map_key), K(opt));
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support for SN mode", KR(ret), K(macro_map_key));  // not support in SN mode
#endif
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::get_or_generate_server_seq_id_(
    const PathMapKey &path_map_key, uint64_t &server_seq_id)
{
  int ret = OB_SUCCESS;
  server_seq_id = UINT64_MAX;
  if (OB_UNLIKELY(!path_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path_map_key));
  } else if (OB_FAIL(path_id_map_.get_refactored(path_map_key, server_seq_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get path_id_map", KR(ret), K(path_map_key));
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    EmptySetCallback<PathMapKey, uint64_t> empty_set_callback;
    GetExistingValueUpdateCallback<PathMapKey, uint64_t> callback(server_seq_id);
    PathMapKey path_map_key_deep_copy;
    path_map_key_deep_copy.modify_time_ = path_map_key.modify_time_;

    if (OB_FAIL(ob_write_string(
        allocator_, path_map_key.url_, path_map_key_deep_copy.url_, true/*c_style*/))) {
      LOG_WARN("fail to deep copy path_map_key", KR(ret), K(path_map_key), K(server_seq_id));
    } else if (OB_FAIL(generate_server_seq_id_(server_seq_id))) {
      LOG_WARN("fail to generate server seq id", KR(ret), K(path_map_key));
    } else if (OB_FAIL(path_id_map_.set_or_update(
        path_map_key_deep_copy, server_seq_id, empty_set_callback, callback))) {
      LOG_WARN("fail to insert seq id into path_id_map",
          KR(ret), K(path_map_key), K(path_map_key_deep_copy), K(server_seq_id));
    }

    if (!path_map_key_deep_copy.url_.empty()) {
      if (OB_FAIL(ret) || callback.is_exist()) {
        allocator_.free(path_map_key_deep_copy.url_.ptr());
        path_map_key_deep_copy.reset();
      }
    }
  }
  return ret;
}

// internal func, skips initialization check
int ObExternalFilePathMap::get_or_generate_macro_id_(
    const MacroMapKey &macro_map_key,
    blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (OB_UNLIKELY(!macro_map_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_map_key));
  } else if (OB_FAIL(macro_id_map_.get_refactored(macro_map_key, macro_id))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get macro_id_map", KR(ret), K(macro_map_key));
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    EmptySetCallback<MacroMapKey, MacroBlockId> empty_set_callback;
    GetExistingValueUpdateCallback<MacroMapKey, MacroBlockId> callback(macro_id);

    if (OB_FAIL(generate_macro_id_(macro_map_key, macro_id))) {
      LOG_WARN("fail to generate macro_id", KR(ret), K(macro_map_key));
    } else if (OB_FAIL(macro_id_map_.set_or_update(
        macro_map_key, macro_id, empty_set_callback, callback))) {
      LOG_WARN("fail to insert macro id into macro_id_map",
          KR(ret), K(macro_map_key), K(macro_id));
    }
  }

  return ret;
}

/*-----------------------------------------ObPCachedExternalFileService-----------------------------------------*/
ObPCachedExternalFileService::ObPCachedExternalFileService()
    : is_inited_(false), is_stopped_(false), tenant_id_(OB_INVALID_TENANT_ID),
      io_callback_allocator_(), path_map_()
{}

ObPCachedExternalFileService::~ObPCachedExternalFileService()
{}

int ObPCachedExternalFileService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPCachedExternalFileService init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else if (OB_FAIL(io_callback_allocator_.init(
      OB_MALLOC_NORMAL_BLOCK_SIZE, "ExtDataIOCB", tenant_id, IO_CALLBACK_MEM_LIMIT))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id));
  } else if (OB_FAIL(path_map_.init(tenant_id))) {
    LOG_WARN("fail to init path map", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    is_stopped_ = false;
    tenant_id_ = tenant_id;
  }
  return ret;
}

int ObPCachedExternalFileService::mtl_init(ObPCachedExternalFileService *&accesser)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (is_meta_tenant(tenant_id)) {
    // skip meta tenant
  } else if (OB_ISNULL(accesser)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("accesser is NULL", KR(ret), K(tenant_id));
  } else if (OB_FAIL(accesser->init(tenant_id))) {
    LOG_WARN("fail to init external data accesser", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObPCachedExternalFileService::start()
{
  int ret = OB_SUCCESS;
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExternalFileService not init", KR(ret));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
  }
  return ret;
}

void ObPCachedExternalFileService::stop()
{
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_INIT) {
    ATOMIC_STORE(&is_stopped_, true);
  }
}

void ObPCachedExternalFileService::wait()
{
  ObTenantIOManager *io_manager = nullptr;
  if (is_meta_tenant(MTL_ID())) {
    // skip meta tenant
  } else if (IS_INIT) {
    if (is_virtual_tenant_id(tenant_id_)) {
      // do nothing
    } else if (OB_NOT_NULL(io_manager = MTL(ObTenantIOManager*))) {
      const int64_t start_time_us = ObTimeUtility::current_time();
      while (io_manager->get_ref_cnt() > 1) {
        if (REACH_TIME_INTERVAL(1000L * 1000L)) { // 1s
          LOG_INFO("wait tenant io manager quit",
              K(tenant_id_), K(start_time_us), KPC(io_manager));
        }
        ob_usleep((useconds_t)10L * 1000L); //10ms
      }
    }
  }
}

void ObPCachedExternalFileService::destroy()
{
  is_inited_ = false;
  is_stopped_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  io_callback_allocator_.destroy();
  path_map_.destroy();
}

int ObPCachedExternalFileService::async_read(
    const ObExternalAccessFileInfo &external_file_info,
    ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  const ObString &url = external_file_info.get_url();
  const ObObjectStorageInfo *access_info = external_file_info.get_access_info();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExternalFileService not init", KR(ret));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ObPCachedExternalFileService is stopped", KR(ret), K(is_stopped_));
  } else if (OB_ISNULL(access_info)
      || OB_UNLIKELY(!external_file_info.is_valid() || !external_read_info.is_valid()
      || url.empty() || !access_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(external_file_info), K(external_read_info));
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(path_map_.get_or_generate(
        url, external_file_info.get_modify_time(), external_read_info.offset_, macro_id))) {
      LOG_WARN("fail to get or create macro block id", KR(ret),
          K(external_file_info), K(external_read_info));
    } else {
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = macro_id;
      read_info.offset_ = external_read_info.offset_;
      read_info.size_ = external_read_info.size_;
      read_info.io_timeout_ms_ = external_read_info.io_timeout_ms_;
      read_info.io_desc_ = external_read_info.io_desc_;
      read_info.io_callback_ = external_read_info.io_callback_;
      read_info.buf_ = static_cast<char *>(external_read_info.buffer_);
      read_info.mtl_tenant_id_ = tenant_id_;
      read_info.path_ = url.ptr();
      read_info.access_info_ = access_info;

      if (OB_FAIL(ObObjectManager::async_read_object(read_info, io_handle))) {
        LOG_WARN("fail to async_read_object", KR(ret),
            K(external_file_info), K(external_read_info), K(io_handle));
      }
    }
  } else {
    ObIOFd fd;
    ObIODevice *io_device = nullptr;
    CONSUMER_GROUP_FUNC_GUARD(share::PRIO_IMPORT);
    if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(
        io_device, fd, access_info, url,
        ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
        ObStorageIdMod::get_default_external_id_mod()))) {
      LOG_WARN("fail to open device", KR(ret), K(external_read_info), K(external_read_info));
    } else if (OB_FAIL(ObBackupIoAdapter::async_pread(*io_device, fd,
        static_cast<char *>(external_read_info.buffer_),
        external_read_info.offset_,
        external_read_info.size_,
        io_handle.get_io_handle()))) {
      LOG_WARN("fail to async pread", KR(ret),
          KPC(io_device), K(fd), K(external_read_info), K(external_read_info), K(io_handle));
    }
  }

  return ret;
}

int ObPCachedExternalFileService::add_prefetch_task(
    const char *url,
    const ObObjectStorageInfo *access_info,
    const int64_t offset_idx)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObPCachedExternalFileService::get_io_callback_allocator(common::ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPCachedExternalFileService not init", KR(ret));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("ObPCachedExternalFileService is stopped", KR(ret), K(is_stopped_));
  } else {
    allocator = &io_callback_allocator_;
  }
  return ret;
}

/*-----------------------------------------ObExternalRemoteIOCallback-----------------------------------------*/
int ObExternalRemoteIOCallback::construct_io_callback(
    const blocksstable::ObStorageObjectReadInfo *read_info,
    common::ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *accesser = nullptr;
  ObIAllocator *allocator = nullptr;
  ObExternalRemoteIOCallback *load_callback = nullptr;

  if (OB_ISNULL(read_info) || OB_UNLIKELY(!read_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_UNLIKELY(ObStorageObjectType::EXTERNAL_TABLE_FILE !=
      read_info->macro_block_id_.storage_object_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info", KR(ret), KPC(read_info));
  } else if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is null", KR(ret), KPC(read_info), K(io_info));
  } else if (OB_FAIL(accesser->get_io_callback_allocator(allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), KPC(read_info), K(io_info));
  } else if (OB_ISNULL(load_callback =
      OB_NEWx(ObExternalRemoteIOCallback, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObExternalRemoteIOCallback", KR(ret),
        KP(allocator), KPC(read_info), K(io_info));
  } else if (OB_FAIL(load_callback->init(allocator,
      io_info.callback_,
      io_info.user_data_buf_,
      io_info.offset_ / OB_STORAGE_OBJECT_MGR.get_macro_block_size(),
      read_info->path_, read_info->access_info_))) {
    LOG_WARN("fail to init load callback", KR(ret), KP(allocator), KPC(read_info), K(io_info));
  }

  io_info.callback_ = load_callback;
  if (OB_FAIL(ret) && OB_NOT_NULL(load_callback)) {
    free_io_callback_and_detach_original(io_info.callback_);
    load_callback = nullptr;
  }
  return ret;
}

void ObExternalRemoteIOCallback::free_io_callback_and_detach_original(
    common::ObIOCallback *&io_callback)
{
  if (OB_NOT_NULL(io_callback)) {
    if (ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK == io_callback->get_type()) {
      ObExternalRemoteIOCallback *load_callback =
          static_cast<ObExternalRemoteIOCallback *>(io_callback);
      ObIOCallback *original_callback = load_callback->clear_original_io_callback();
      free_io_callback<ObExternalRemoteIOCallback>(io_callback);
      io_callback = original_callback;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected io callback type", KPC(io_callback));
    }
  }
}

ObExternalRemoteIOCallback::ObExternalRemoteIOCallback()
    : ObIOCallback(ObIOCallbackType::EXTERNAL_DATA_LOAD_FROM_REMOTE_CALLBACK),
      is_inited_(false),
      allocator_(nullptr),
      ori_callback_(nullptr),
      user_data_buf_(nullptr),
      offset_idx_(-1),
      url_(nullptr),
      access_info_(nullptr)
{
}

ObExternalRemoteIOCallback::~ObExternalRemoteIOCallback()
{
  if (OB_NOT_NULL(ori_callback_) && OB_NOT_NULL(ori_callback_->get_allocator())) {
    free_io_callback<ObIOCallback>(ori_callback_);
  }
}

int ObExternalRemoteIOCallback::init(
    common::ObIAllocator *allocator,
    common::ObIOCallback *original_callback,
    char *user_data_buf,
    const int64_t offset_idx,
    const char *url,
    const ObObjectStorageInfo *access_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObExternalRemoteIOCallback init twice", KR(ret));
  } else if (OB_ISNULL(allocator) || OB_ISNULL(url) || OB_ISNULL(access_info)
      || OB_UNLIKELY(!access_info->is_valid() || offset_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(allocator), K(offset_idx), KP(url), KPC(access_info));
  } else if (OB_ISNULL(original_callback) && OB_ISNULL(user_data_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("both callback and user data buf are NULL", KR(ret),
        KP(allocator), KPC(original_callback), KP(user_data_buf), K(url), KPC(access_info));
  } else {
    allocator_ = allocator;
    ori_callback_ = original_callback;
    user_data_buf_ = user_data_buf;
    offset_idx_ = offset_idx;
    url_ = url;
    access_info_ = access_info;
    is_inited_ = true;
  }
  return ret;
}

int ObExternalRemoteIOCallback::inner_process(
    const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t ori_callback_us = 0;
  int64_t load_callback_us = 0;
  const int64_t WARNING_TIME_LIMT_US = 2 * 1000 * 1000LL;  // 2s

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalRemoteIOCallback not init", KR(ret));
  } else if (OB_ISNULL(data_buffer) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", KR(ret), KP(data_buffer), K(size), KPC(this));
  } else if (OB_ISNULL(ori_callback_)) {
    if (OB_ISNULL(user_data_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("both callback and user data buf are null", KR(ret), KPC(this));
    } else {
      MEMCPY(user_data_buf_, data_buffer, size);
    }
  } else {  // nullptr != callback_
    const int64_t ori_callback_start_us = ObTimeUtility::current_time();
    if (OB_FAIL(ori_callback_->inner_process(data_buffer, size))) {
      LOG_WARN("fail to inner process", KR(ret), KPC(this), KP(data_buffer), K(size));
    }
    ori_callback_us = ObTimeUtility::current_time() - ori_callback_start_us;
  }

  if (OB_SUCC(ret)) {
    // generate preread task
    const int64_t load_callback_start_us = ObTimeUtility::current_time();
    int tmp_ret = OB_SUCCESS;
    ObPCachedExternalFileService *accesser = nullptr;
    if (OB_ISNULL(accesser = MTL(ObPCachedExternalFileService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObPCachedExternalFileService is null", KR(ret), "tenant_id", MTL_ID(), KPC(this));
    } else if (OB_TMP_FAIL(accesser->add_prefetch_task(url_, access_info_, offset_idx_))) {
      LOG_WARN("fail to add preread task for external table", KR(ret), KPC(this));
    }
    load_callback_us = ObTimeUtility::current_time() - load_callback_start_us;
  }

  if (OB_UNLIKELY((ori_callback_us + load_callback_us) > WARNING_TIME_LIMT_US)) {
    LOG_INFO("callback cost too much time",
        K(ori_callback_us), K(load_callback_us), KP(data_buffer), K(size), KPC(this));
  }
  return ret;
}

} // sql
} // oceanbase