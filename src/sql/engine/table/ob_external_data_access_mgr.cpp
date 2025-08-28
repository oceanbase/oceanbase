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

#include "ob_external_data_access_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_struct.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "ob_external_table_access_service.h"
#include "ob_pcached_external_file_service.h"

namespace oceanbase
{
namespace sql
{
/// @brief deep copy url
int ObExternalAccessFileInfo::copy_url(ObString &dest, const ObString &src, common::ObIAllocator *allocator) {
  using obstr_size_t = common::ObString::obstr_size_t;
  int ret = OB_SUCCESS;
  char *url_buf = nullptr;
  obstr_size_t url_size = src.length();
  obstr_size_t actual_write = 0;
  if (OB_UNLIKELY(!dest.empty() || src.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(dest), K(src));
  } else if (OB_ISNULL(url_buf = reinterpret_cast<char*>(allocator->alloc(url_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for url", K(ret), K(url_size));
  } else if (FALSE_IT(dest.assign_buffer(url_buf, url_size))) {
  } else if (FALSE_IT(actual_write = dest.write(src.ptr(), url_size))) {
  } else if (OB_UNLIKELY(url_size != actual_write)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to copy url", K(ret), K(src), K(url_size), K(actual_write));
  }
  return ret;
}
/***************** ObExternalAccessFileInfo ****************/
ObExternalAccessFileInfo::~ObExternalAccessFileInfo() { reset_(); }

bool ObExternalAccessFileInfo::is_valid() const
{
  return page_size_ > 0 &&
         file_size_ > 0 &&
         allocator_ != nullptr &&
         !url_.empty() &&
         access_info_ != nullptr &&
         access_info_->is_valid() &&
         device_handle_ != nullptr;
}

void ObExternalAccessFileInfo::reset_()
{
  OB_ASSERT(OB_ISNULL(url_.ptr()) || OB_NOT_NULL(allocator_));
  OB_ASSERT(OB_ISNULL(access_info_) || OB_NOT_NULL(allocator_));

  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(url_.ptr())) {
      allocator_->free(url_.ptr());
    }
    if (OB_NOT_NULL(content_digest_.ptr())) {
      allocator_->free(content_digest_.ptr());
    }

    if (OB_ISNULL(access_info_)) {
    } else if (FALSE_IT(access_info_->~ObObjectStorageInfo())) {
    } else {
      allocator_->free(access_info_);
      access_info_ = nullptr;
    }
  }

  url_.reset();
  content_digest_.reset();
  modify_time_ = -1;
  page_size_ = 0;
  file_size_ = 0;
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
  device_handle_ = nullptr;
  allocator_ = nullptr;
}

int ObExternalAccessFileInfo::assign(const ObExternalAccessFileInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid other_file_info", K(ret), K(other));
  } else if (this == &other) {
    // nothing to do
  } else if (OB_UNLIKELY(!other.is_copyable_())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other_file_info is not copyable", K(ret), K(other));
  } else {
    reset_();
    page_size_ = other.page_size_;
    file_size_ = other.file_size_;
    modify_time_ = other.modify_time_;
    device_handle_ = other.device_handle_;
    if (OB_FAIL(set_access_info(other.access_info_, other.allocator_))) {
      LOG_WARN("fail to set access info", K(ret), K(other));
    } else if (OB_FAIL(set_basic_file_info(other.url_, other.content_digest_, other.modify_time_,
                                           other.page_size_, other.file_size_,
                                           *other.allocator_))) {
      LOG_WARN("fail to set basic file info", K(ret), K(other));
    }
  }
  return ret;
}

int ObExternalAccessFileInfo::set_basic_file_info(const ObString &url,
                                                  const ObString &content_digest,
                                                  const int64_t modify_time,
                                                  const int64_t page_size,
                                                  const int64_t file_size,
                                                  common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(url.empty()) || OB_UNLIKELY(page_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty str", K(url), K(page_size));
  } else if (OB_FAIL(copy_url(url_, url, allocator_))) {
    LOG_WARN("fail to copy url", K(ret), K(url));
  } else if (OB_FAIL(ob_write_string(allocator, content_digest, content_digest_))) {
    LOG_WARN("failed to copy content digest", K(ret));
  } else {
    page_size_ = page_size;
    file_size_ = file_size;
    modify_time_ = modify_time;
  }
  return ret;
}

int ObExternalAccessFileInfo::set_access_info(
  const ObObjectStorageInfo *access_info,
  common::ObIAllocator *allocator) {
  int ret = OB_SUCCESS;
  if (allocator_ != nullptr || access_info_ != nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file info is already setted", K(ret), KP(allocator_), KP(access_info_));
  } else if (OB_ISNULL(access_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null access_info", K(ret));
  } else if (OB_UNLIKELY(!access_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access info is invalid", K(ret), KPC(access_info));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else {
    // clone access info
    void *alloc = nullptr;
    if (access_info->is_hdfs_storage()) {
      if (OB_ISNULL(alloc = allocator_->alloc(sizeof(ObHDFSStorageInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for hdfs access info",
          K(ret), K(sizeof(ObHDFSStorageInfo)));
      } else {
        access_info_ = new(alloc) ObHDFSStorageInfo;
      }
    } else {
      if (OB_ISNULL(alloc = allocator_->alloc(sizeof(ObBackupStorageInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for backup access info",
          K(ret), K(sizeof(ObBackupStorageInfo)));
      } else {
        access_info_ = new(alloc) ObBackupStorageInfo;
      }
    }

    OB_ASSERT(OB_SUCCESS != ret || nullptr != access_info_);
    if (OB_SUCC(ret) && OB_FAIL(access_info_->assign(*access_info))) {
      LOG_WARN("fail to assign access info", K(ret));
      access_info_->~ObObjectStorageInfo();
      allocator_->free(access_info_);
      access_info_ = nullptr;
    }
  }
  return ret;
}


/***************** FileMapKey ****************/
ObExternalDataAccessMgr::FileMapKey::FileMapKey() :
  page_size_(0), url_(), content_digest_(), modify_time_(-1), allocator_(nullptr)
{}

ObExternalDataAccessMgr::FileMapKey::~FileMapKey() {
  reset();
}

ObExternalDataAccessMgr::FileMapKey::FileMapKey(common::ObIAllocator *allocator) :
  page_size_(0), url_(), content_digest_(), modify_time_(-1), allocator_(allocator)
{}

bool ObExternalDataAccessMgr::FileMapKey::operator==(const FileMapKey &other) const
{
  return (other.page_size_ == page_size_) && (other.url_ == url_)
         && (other.content_digest_ == content_digest_) && (other.modify_time_ == modify_time_);
}

uint64_t ObExternalDataAccessMgr::FileMapKey::hash(const ObString &url,
                                                   const ObString &content_digest,
                                                   const int64_t modify_time,
                                                   const int64_t page_size)
{
  uint64_t hash_val = url.hash();
  hash_val = content_digest.hash(hash_val);
  hash_val = common::murmurhash(&modify_time, sizeof(modify_time), hash_val);
  hash_val = common::murmurhash(&page_size, sizeof(page_size), hash_val);
  return hash_val;
}

uint64_t ObExternalDataAccessMgr::FileMapKey::hash() const
{
  return hash(url_, content_digest_, modify_time_, page_size_);
}

int ObExternalDataAccessMgr::FileMapKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ObExternalDataAccessMgr::FileMapKey::is_copyable_() const {
  return allocator_ != nullptr;
}

int ObExternalDataAccessMgr::FileMapKey::init(const ObString &url,
                                              const ObString &content_digest,
                                              const int64_t modify_time,
                                              const int64_t page_size)
{
  int ret = OB_SUCCESS;
  page_size_ = page_size;
  modify_time_ = modify_time;
  if (!url_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file map key already initialized", K(ret), K(url_));
  } else if (OB_UNLIKELY(url.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty str", K(ret), K(url));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(ObExternalAccessFileInfo::copy_url(url_, url, allocator_))) {
    LOG_WARN("fail to copy url", K(ret), K(url));
  } else if (OB_FAIL(ob_write_string(*allocator_, content_digest, content_digest_))) {
    LOG_WARN("fail to copy content digest", K(ret), K(content_digest));
  }
  return ret;
}

int ObExternalDataAccessMgr::FileMapKey::assign(const FileMapKey &other) {
  int ret = OB_SUCCESS;
  if (!other.is_copyable_()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other file map key is not copyable", K(ret), K(other));
  } else if (this == &other) {
    // nothing to do
  } else {
    reset();
    page_size_ = other.page_size_;
    modify_time_ = other.modify_time_;
    allocator_ = other.allocator_;
    if (!other.url_.empty() &&
        OB_FAIL(ObExternalAccessFileInfo::copy_url(url_, other.url_, allocator_))) {
      LOG_WARN("fail to copy url", K(ret), K(other.url_));
    } else if (OB_UNLIKELY(other.url_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid empty url", K(ret), K(other.url_));
    } else if (OB_FAIL(ob_write_string(*allocator_, other.content_digest_, content_digest_))) {
      LOG_WARN("fail to copy content digest", K(ret), K(other.content_digest_));
    }
  }
  return ret;
}

void ObExternalDataAccessMgr::FileMapKey::reset() {
  OB_ASSERT(OB_ISNULL(url_.ptr()) || OB_NOT_NULL(allocator_));
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(url_.ptr())) {
      allocator_->free(url_.ptr());
    }
    if (OB_NOT_NULL(content_digest_.ptr())) {
      allocator_->free(content_digest_.ptr());
    }
  }
  url_.reset();
  content_digest_.reset();
  allocator_ = nullptr;
  modify_time_ = -1;
  page_size_ = 0;
}

/***************** ObExternalDataAccessMgr ****************/

ObExternalDataAccessMgr::ObExternalDataAccessMgr() :
    bucket_lock_(),
    kv_cache_(ObExternalDataPageCache::get_instance()),
    is_inited_(false)
{

}
ObExternalDataAccessMgr::~ObExternalDataAccessMgr()
{

}
int ObExternalDataAccessMgr::mtl_init(ObExternalDataAccessMgr *&ExDAM)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObExternalDataAccessMgr mtl init");
  return ExDAM->init();
}

int ObExternalDataAccessMgr::init()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  const int bucket_num = BUCKET_NUM;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(bucket_num))) {
    LOG_WARN("failed to init bucket lock", K(ret));
  } else if (OB_FAIL(fd_map_.create(bucket_num, "ExDAMFDMap", "ExDAMFDMap", MTL_ID()))) {
    LOG_WARN("fail to initialize ExDAMfd map", K(ret), K(bucket_num));
  } else if (OB_FAIL(file_map_.create(bucket_num, "ExDAMFileMap", "ExDAMFileMap", MTL_ID()))) {
    LOG_WARN("fail to initialize ExDAMFile map", K(ret), K(bucket_num));
  } else if (OB_FAIL(inner_file_info_alloc_.init(lib::ObMallocAllocator::get_instance(), 4096/*TODO page_size*/, ObMemAttr(tenant_id, "ExDAMFileInfo")))) {
    LOG_WARN("fail to init storage meta cache io allocator", K(ret), K(tenant_id));
  } else if (OB_FAIL(callback_alloc_.init(lib::ObMallocAllocator::get_instance(), 4096/*TODO page_size*/, ObMemAttr(tenant_id, "ExDAMCallBack")))) {
    LOG_WARN("fail to init storage meta cache io allocator", K(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObExternalDataAccessMgr::start()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObExternalDataAccessMgr::stop()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObExternalDataAccessMgr::wait()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObExternalDataAccessMgr::destroy()
{
  fd_map_.destroy();
  file_map_.destroy();
  inner_file_info_alloc_.reset();
  callback_alloc_.reset();
}

int ObExternalDataAccessMgr::open_and_reg_file(
    const ObString &url,
    const ObString &content_digest,
    const ObObjectStorageInfo *info,
    const int64_t modify_time,
    const int64_t file_size,
    ObIOFd &fd)
{
  static constexpr uint64_t OB_STORAGE_ID_EXTERNAL = 2001; // same as value in ob_external_table_access_service.cpp
  int ret = OB_SUCCESS;
  fd.reset();
  FileMapKey key(&inner_file_info_alloc_);
  InnerAccessFileInfo *inner_file_info = nullptr;
  int64_t page_size = ObExternalDataPageCache::get_page_size();
  if (OB_FAIL(key.init(url, content_digest, modify_time, page_size))) {
    LOG_WARN("fail to init file map key", K(ret), K(page_size), K(content_digest), K(modify_time), K(url));
  } else if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null access info", K(ret), KP(info));
  } else if (OB_UNLIKELY(!info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access info is invalid", K(ret), KPC(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  }

  if (OB_SUCC(ret)) { // get first
    ObBucketHashRLockGuard lock(bucket_lock_, key.hash());
    if (OB_FAIL(lock.get_ret())) {
      LOG_WARN("failed to hold bucket lock", K(ret), K(key.hash()));
    } else if (OB_FAIL(get_file_info_by_file_key_(key, fd, inner_file_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // do nothing
      } else {
        LOG_WARN("failed to get fd and file_info by key", K(ret), K(key), K(fd), KPC(inner_file_info));
      }
    } else {
      inner_file_info->inc_ref();
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ObBucketHashWLockGuard lock(bucket_lock_, key.hash());
    // double check
    if (OB_FAIL(lock.get_ret())) {
      LOG_WARN("failed to hold bucket lock", K(ret), K(key.hash()));
    } else if (OB_FAIL(get_file_info_by_file_key_(key, fd, inner_file_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // do nothing
      } else {
        LOG_WARN("failed to get fd and file_info by key", K(ret), K(key), K(fd), KPC(inner_file_info));
      }
    } else {
      inner_file_info->inc_ref();
    }

    void *buf = nullptr;
    if (OB_HASH_NOT_EXIST != ret) {
      // do nothing
      //   OB_SUCCESS                          : double get success (fd cache hit)
      //   other code but not OB_HASH_NOT_EXIST: failed
    } else if (OB_ISNULL(buf = inner_file_info_alloc_.alloc(sizeof(InnerAccessFileInfo)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buffer", K(ret), KP(buf));
    } else if (FALSE_IT(inner_file_info = new (buf) InnerAccessFileInfo())) {
    } else if (OB_FAIL(inner_file_info->info_.set_access_info(info, &inner_file_info_alloc_))) {
      LOG_WARN("fail to set url and access info", K(ret), K(key), K(url), KPC(info));
    } else if (OB_FAIL(inner_file_info->info_.set_basic_file_info(
                 url, content_digest, modify_time, page_size, file_size, inner_file_info_alloc_))) {
      LOG_WARN("fail to set url and access info", K(ret), K(key), K(url), KPC(info));
    } else if (OB_FAIL(ObExternalIoAdapter::open_with_access_type(
                 inner_file_info->info_.get_device_handle_(), // out
                 fd,                                          // out
                 inner_file_info->info_.get_access_info(),    // input
                 inner_file_info->info_.get_url(),            // input
                 OB_STORAGE_ACCESS_NOHEAD_READER,
                 ObStorageIdMod(OB_STORAGE_ID_EXTERNAL,
                                ObStorageUsedMod::STORAGE_USED_EXTERNAL)))) {
      LOG_WARN("fail to open Data Access Driver", KR(ret), KPC(inner_file_info), K(fd));
    } else if (!inner_file_info->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid inner_file_info", K(ret), KPC(inner_file_info));
    } else if (OB_FAIL(fd_map_.set_refactored(key, fd))) {
      LOG_WARN("failed to record fd", K(ret), K(key), K(fd));
    } else if (FALSE_IT(inner_file_info->inc_ref())) {
    } else if (OB_FAIL(file_map_.set_refactored(fd, inner_file_info))) {
      LOG_WARN("failed to record fd", K(ret), K(key), K(fd), KPC(inner_file_info));
      // delete record from fd_map_
      int tmp_ret = OB_ERR_UNEXPECTED;
      if (OB_TMP_FAIL(force_delete_from_fd_map_(key))) {
        LOG_WARN("failed to delete from fd_map", K(tmp_ret), K(key), KPC(inner_file_info));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(inner_file_info)) {
      inner_file_info->~InnerAccessFileInfo();
      inner_file_info_alloc_.free(inner_file_info);
      inner_file_info = nullptr;
      buf = nullptr;
    }
  }

  return ret;
}


int ObExternalDataAccessMgr::close_file(ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  InnerAccessFileInfo *inner_file_info = nullptr;
  if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
    LOG_WARN("failed to get file from file_map", K(ret), KPC(inner_file_info));
  } else {
    int64_t ref_cnt = INT_MAX64;
    {
      // avoid unnecessary copy of url
      const uint64_t bucket_hash = FileMapKey::hash(
        inner_file_info->info_.get_url(), inner_file_info->info_.get_file_content_digest(),
        inner_file_info->info_.get_modify_time(), inner_file_info->info_.get_page_size());
      ObBucketHashWLockGuard lock(bucket_lock_, bucket_hash);
      if (OB_FAIL(lock.get_ret())) {
        LOG_WARN("failed to hold bucket lock", K(ret), K(bucket_hash));
      } else if (FALSE_IT(ref_cnt = inner_file_info->dec_ref())) {
      } else if (0 == ref_cnt) {
        FileMapKey del_key( &inner_file_info_alloc_);
        if (OB_FAIL(force_delete_from_file_map_(fd))) {
          LOG_WARN("failed to erase file info", K(ret), K(fd), KPC(inner_file_info));
        } else if (OB_FAIL(del_key.init(inner_file_info->info_.get_url(),
                                        inner_file_info->info_.get_file_content_digest(),
                                        inner_file_info->info_.get_modify_time(),
                                        inner_file_info->info_.get_page_size()))) {
          LOG_WARN("failed to init del file map key", K(ret), KPC(inner_file_info));
        } else if (OB_FAIL(force_delete_from_fd_map_(del_key))) {
          LOG_WARN("failed to erase fd", K(ret), K(fd), KPC(inner_file_info));
        }
      }
    }

    if (0 == ref_cnt) {
      // close fd by device handle without lock held.
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ObExternalIoAdapter::close_device_and_fd(inner_file_info->info_.get_device_handle_(), fd))) {
        LOG_WARN("fail to close device and fd", KR(ret), KPC(inner_file_info));
      }
      inner_file_info->~InnerAccessFileInfo();
      inner_file_info_alloc_.free(inner_file_info);
    }

    fd.reset(); // reset fd whatever ref cnt is 0.

    if (0 > ref_cnt) {
      LOG_ERROR("invalid ref_cnt", K(ret), KPC(inner_file_info));
      ob_abort();
    }
  }
  return ret;
}

int ObExternalDataAccessMgr::force_delete_from_fd_map_(const FileMapKey &key)
{
  int ret = OB_SUCCESS;
  int64_t count = 50; // 50ms total
  do {
    if (OB_FAIL(fd_map_.erase_refactored(key))) {
      LOG_WARN("failed to erase external_access_file_info", K(ret), K(key));
      ob_usleep(1000); // 1ms
    }
    count--;
  } while(OB_FAIL(ret) && count >= 0);

  if (OB_FAIL(ret)) {
    LOG_ERROR("delete record from fd_map_ failed", K(ret), K(key));
    ob_abort();
  }
  return ret;
}
int ObExternalDataAccessMgr::force_delete_from_file_map_(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int64_t count = 50; // 50ms total
  do {
    if (OB_FAIL(file_map_.erase_refactored(fd))) {
      LOG_WARN("failed to erase external_access_file_info", K(ret), K(fd));
      ob_usleep(1000); // 1ms
    }
    count--;
  } while(OB_FAIL(ret) && count >= 0);

  if (OB_FAIL(ret)) {
    LOG_ERROR("delete record from fd_map_ failed", K(ret), K(fd));
    ob_abort();
  }
  return ret;
}

int ObExternalDataAccessMgr::get_file_info_by_file_key_(
    const FileMapKey &key,
    ObIOFd &fd,
    InnerAccessFileInfo *&inner_file_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fd_map_.get_refactored(key, fd))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get file from file_map", K(ret), K(key), KPC(inner_file_info));
    }
  } else if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // when fd exist in fd_map_, file_info should exist in file_map_ at the same time
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("failed to get file_info from file_map", K(ret), K(key), K(fd), KPC(inner_file_info));
  } else if (OB_ISNULL(inner_file_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner file info is nullptr", K(ret), K(key), K(fd), KPC(inner_file_info));
  }
  return ret;
}

int ObExternalDataAccessMgr::get_modify_time_by_fd_(
    const ObIOFd &fd,
    int64_t &modify_time)
{
  int ret = OB_SUCCESS;
  if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else {
    InnerAccessFileInfo *inner_file_info = nullptr;
    if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
      LOG_WARN("failed to get file from file_map", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_ISNULL(inner_file_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_file_info is nullptr", K(ret), K(fd));
    } else {
      modify_time = inner_file_info->info_.get_modify_time();
    }
  }
  return ret;
}

int ObExternalDataAccessMgr::record_one_and_reset_seg_(
    ObExtCacheMissSegment &seg,
    ObIArray<ObExtCacheMissSegment> &seg_arr) const
{
  int ret = OB_SUCCESS;
  if (!seg.is_valid()) {
    // do nothing
  } else if (OB_FAIL(seg_arr.push_back(seg))) {
    LOG_WARN("failed to push back", K(ret), K(seg_arr), K(seg));
  } else {
    seg.reset();
  }
  return ret;
}


int ObExternalDataAccessMgr::inner_cache_hit_process_(
    const int64_t cur_pos,
    const int64_t cur_rd_offset,
    const int64_t cur_buf_size,
    const int64_t cache_page_size,
    const ObExternalDataPageCacheValueHandle &v_hdl,
    char* buffer,
    ObExternalFileReadHandle &exReadhandle,
    ObExtCacheMissSegment &cur_seg,
    ObIArray<ObExtCacheMissSegment> &seg_arr)
{
  int ret = OB_SUCCESS;

  MEMCPY(buffer + cur_pos, // dest
          v_hdl.value_->get_buffer() + ( cur_rd_offset % cache_page_size ), // src
          cur_buf_size); // size
  exReadhandle.cache_hit_size_ += cur_buf_size;
  exReadhandle.metrics_->update_mem_cache_stat(true/*is_hit*/, cur_buf_size);
  if (OB_FAIL(record_one_and_reset_seg_(cur_seg, seg_arr))) {
    LOG_WARN("failed to record seg", K(ret), K(cur_seg), K(seg_arr));
  }
  return ret;
}
int ObExternalDataAccessMgr::inner_cache_miss_process_(
    const int64_t cur_pos,
    const int64_t cur_rd_offset,
    const int64_t cur_buf_size,
    char* buffer,
    ObExtCacheMissSegment &cur_seg,
    ObIArray<ObExtCacheMissSegment> &seg_arr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cur_seg.push_piece(buffer + cur_pos, cur_rd_offset, cur_buf_size))) {
    LOG_WARN("failed to push_piece", K(ret), KP(buffer + cur_pos), K(cur_rd_offset), K(cur_buf_size));
  } else if (cur_seg.reach_2MB_boundary() && OB_FAIL(record_one_and_reset_seg_(cur_seg, seg_arr))) {
    LOG_WARN("failed to record seg", K(ret), K(cur_seg), K(seg_arr));
  }
  return ret;
}

int ObExternalDataAccessMgr::fill_cache_hit_buf_and_get_cache_miss_segments_(
    const ObIOFd &fd,
    const ObString &url,
    const ObString &content_digest,
    const int64_t modify_time,
    const int64_t page_size,
    const int64_t rd_offset,
    const int64_t rd_len,
    const bool enable_page_cache,
    char* buffer,
    ObExternalFileReadHandle &exReadhandle,
    ObIArray<ObExtCacheMissSegment> &seg_arr)
{
  int ret = OB_SUCCESS;

  exReadhandle.cache_hit_size_ = 0;
  // argument test
  if (!fd.is_valid() || OB_ISNULL(buffer) || rd_offset < 0 || rd_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), KP(buffer), K(rd_offset), K(rd_len));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  }

  // get info of CacheKey involved
  uint64_t tenant_id = MTL_ID();

  // get data from page_cache, and record cache_miss_segment
  ObExtCacheMissSegment cur_seg;
  ObExternalDataPageCacheValueHandle handle;
  // cur_page_offset of page_cache
  int64_t cur_page_offset = (rd_offset / page_size) * page_size;
  // cur_rd_offset to external file
  int64_t cur_rd_offset = rd_offset;
  int64_t capacity = ((rd_offset + rd_len - cur_page_offset) / page_size) + 1;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(seg_arr.reserve(capacity))) {
    LOG_WARN("failed to reserve", K(ret));
  }
  while (OB_SUCC(ret) && cur_page_offset < rd_offset + rd_len) {
    ObExternalDataPageCacheKey cur_key(url.ptr(), url.length(), content_digest.ptr(),
                                       content_digest.length(), modify_time, page_size,
                                       cur_page_offset, tenant_id);
    // cur_pos to buffer
    const int64_t cur_pos = cur_rd_offset - rd_offset;
    // size of buffer in this page,
    //    e.g.: page_size 4096;
    //          read 0-1024,    cur_buf_size should be 1024
    //          read 3072-4096, cur_buf_size should be 1024
    //          read 4096-8192, cur_buf_size should be 4096
    //          read 1000-4000, cur_buf_size should be 3000
    const int64_t cur_buf_size = min(min(cur_rd_offset % page_size != 0 ? page_size - (cur_rd_offset % page_size) : page_size, // header
                                     rd_len - (cur_rd_offset - rd_offset)), // tailer
                                     rd_len); // mid
    if (!enable_page_cache) {
      // disable page_cache
      if (OB_FAIL(inner_cache_miss_process_(
          cur_pos, cur_rd_offset, cur_buf_size, buffer, cur_seg, seg_arr))) {
        LOG_WARN("failed to process cache_miss option", K(ret), K(cur_pos), K(cur_rd_offset), K(rd_offset), K(rd_len), K(cur_buf_size), K(cur_seg), K(seg_arr));
      }
    } else if (OB_SUCC(kv_cache_.get_page(cur_key, handle)) && cur_buf_size <= handle.value_->get_valid_data_size()) {
      // cache hit;
      if (OB_FAIL(inner_cache_hit_process_(
          cur_pos, cur_rd_offset, cur_buf_size, page_size, handle, buffer, exReadhandle, cur_seg, seg_arr))) {
        LOG_WARN("failed to process cache_miss option", K(ret), K(cur_pos), K(cur_rd_offset), K(rd_offset), K(rd_len), K(cur_buf_size), K(cur_seg), K(seg_arr));
      }
    } else if (ret != OB_ENTRY_NOT_EXIST) {
      // error
      LOG_WARN("fail to get page", KR(ret), K(fd), K(cur_key));
    } else {
      // (enable_page_cache && ret == OB_ENTRY_NOT_EXIST)
      // cache miss;
      ret = OB_SUCCESS;
      if (OB_FAIL(inner_cache_miss_process_(
          cur_pos, cur_rd_offset, cur_buf_size, buffer, cur_seg, seg_arr))) {
        LOG_WARN("failed to process cache_miss option", K(ret), K(cur_pos), K(cur_rd_offset), K(rd_offset), K(rd_len), K(cur_buf_size), K(cur_seg), K(seg_arr));
      } else {
        exReadhandle.metrics_->update_mem_cache_stat(false/*is_hit*/, cur_buf_size);
      }
    }
    if (OB_SUCC(ret)) {
      cur_page_offset += page_size;
      cur_rd_offset += cur_buf_size;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(record_one_and_reset_seg_(cur_seg, seg_arr))) {
    LOG_WARN("failed to record seg", K(ret), K(cur_seg), K(seg_arr));
  }

  LOG_TRACE("IO IO SEG", K(ret), K(rd_offset), K(rd_len), K(seg_arr));
  return ret;
}

int ObExternalDataAccessMgr::get_rd_info_arr_by_cache_miss_seg_arr_(
    const ObIOFd &fd,
    const ObString &url,
    const ObString &content_digest,
    const int64_t modify_time,
    const int64_t page_size,
    const int64_t file_size,
    const ObIArray<ObExtCacheMissSegment> &seg_arr,
    const ObExternalReadInfo &src_rd_info,
    const bool enable_page_cache,
    ObIArray<ObExternalReadInfo> &rd_info_arr)
{
  int ret = OB_SUCCESS;
  char *buffer_head = static_cast<char *>(src_rd_info.buffer_);
  uint64_t tenant_id = MTL_ID();
  ObExCachedReadPageIOCallback *callback = nullptr;
  void *callback_buf = nullptr;
  void *page_cache_buf_ = nullptr;
  if (OB_FAIL(rd_info_arr.reserve(seg_arr.count()))) {
    LOG_WARN("failed to reserve", K(ret));
  } else if (!enable_page_cache) {
    for (int64_t i = 0; OB_SUCC(ret) && i < seg_arr.count(); i++) {
      const ObExtCacheMissSegment cur_seg = seg_arr.at(i);
      ObExternalReadInfo cur_rd_info(
            cur_seg.get_rd_offset(),
            buffer_head + cur_seg.get_rd_offset() - src_rd_info.offset_,
            cur_seg.get_rd_len(),
            src_rd_info.io_timeout_ms_, src_rd_info.io_desc_);
      cur_rd_info.io_callback_ = nullptr;
      if (OB_FAIL(rd_info_arr.push_back(cur_rd_info))) {
        LOG_WARN("failed to push back cur_rd_info", K(ret), K(i), K(cur_seg), K(rd_info_arr), K(cur_rd_info));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < seg_arr.count(); i++) {
      const ObExtCacheMissSegment cur_seg = seg_arr.at(i);
      page_cache_buf_ = nullptr;
      callback_buf = nullptr;
      callback = nullptr;

      // init cur_read_info basic_info
      const int64_t cur_buf_pos = cur_seg.get_rd_offset() - src_rd_info.offset_; // buffer_head + cur_buf_pos
      const int64_t cur_cache_key_offset = cur_seg.get_page_offset(page_size);
      const int64_t cur_cache_buf_len =
        MIN(cur_seg.get_page_count(page_size) * page_size, file_size - cur_cache_key_offset);

      // alloc, create callback and record on read_info;
      if (OB_ISNULL(page_cache_buf_ = callback_alloc_.alloc(cur_cache_buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloca mem", K(ret), K(cur_cache_buf_len), KP(page_cache_buf_));
      } else {
        ObExternalReadInfo cur_rd_info(cur_cache_key_offset, page_cache_buf_, cur_cache_buf_len, src_rd_info.io_timeout_ms_, src_rd_info.io_desc_);
        // TODO mem of callback_buf should be free by @shifangdan.sfd's callback
        if (OB_ISNULL(callback_buf = callback_alloc_.alloc(sizeof(ObExCachedReadPageIOCallback)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloca mem", K(ret), K(sizeof(ObExCachedReadPageIOCallback)), KP(callback_buf));
        } else {
          ObExternalDataPageCacheKey key(url.ptr(), url.length(), content_digest.ptr(),
                                         content_digest.length(), modify_time, page_size,
                                         cur_cache_key_offset, tenant_id);
          callback = new (callback_buf) ObExCachedReadPageIOCallback(
                key, buffer_head + cur_buf_pos, page_cache_buf_, cur_seg.get_rd_offset() % page_size,
                cur_seg.get_rd_len(), &kv_cache_, &callback_alloc_);
          cur_rd_info.io_callback_ = callback;
        }

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(callback)) {
            callback->~ObExCachedReadPageIOCallback();
            // in callback deconstruct, page_cache_buf_ will be free;
            page_cache_buf_ = nullptr;
          }
          if (OB_NOT_NULL(page_cache_buf_)) {
            callback_alloc_.free(page_cache_buf_);
          }
          if (OB_NOT_NULL(callback_buf)) {
            callback_alloc_.free(callback_buf);
          }
          cur_rd_info.io_callback_ = nullptr;
          cur_rd_info.buffer_ = nullptr;
        } else if (OB_FAIL(rd_info_arr.push_back(cur_rd_info))) {
          LOG_WARN("failed to push back cur_rd_info", K(ret), K(i), K(cur_seg), K(rd_info_arr), K(cur_rd_info));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t j = 0; j < rd_info_arr.count(); ++j) {
      if (OB_NOT_NULL(rd_info_arr.at(j).io_callback_) &&
          OB_NOT_NULL(rd_info_arr.at(j).io_callback_->get_allocator())) {
        ObIAllocator *alloc_ = rd_info_arr.at(j).io_callback_->get_allocator();
        static_cast<ObExCachedReadPageIOCallback*>(rd_info_arr.at(j).io_callback_)->ObExCachedReadPageIOCallback::~ObExCachedReadPageIOCallback();
        alloc_->free(rd_info_arr.at(j).io_callback_);
      }
    }
  }
  return ret;
}


int ObExternalDataAccessMgr::async_read(
    const ObIOFd &fd,
    const ObExternalReadInfo &info,
    const bool enable_page_cache,
    ObExternalFileReadHandle &handle)
{
  int ret = OB_SUCCESS;

  ObSArray<ObExtCacheMissSegment> seg_arr;
  ObSArray<ObExternalReadInfo> rd_info_arr;

  if (!fd.is_valid() || !info.is_valid() || (nullptr == handle.metrics_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), K(info), K(handle.metrics_));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else {
    int64_t read_size = -1;
    InnerAccessFileInfo *inner_file_info = nullptr;

    if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
      LOG_WARN("failed to get file from file_map", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_ISNULL(inner_file_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_file_info is nullptr", K(ret), K(fd));
    } else if (OB_FAIL(fill_cache_hit_buf_and_get_cache_miss_segments_(
                 fd, inner_file_info->info_.get_url(),
                 inner_file_info->info_.get_file_content_digest(),
                 inner_file_info->info_.get_modify_time(), inner_file_info->info_.get_page_size(),
                 info.offset_, info.size_, enable_page_cache, static_cast<char *>(info.buffer_),
                 handle, seg_arr))) {
      LOG_WARN("failed to do preprocess", K(ret));
    } else if (OB_FAIL(get_rd_info_arr_by_cache_miss_seg_arr_(
                 fd, inner_file_info->info_.get_url(),
                 inner_file_info->info_.get_file_content_digest(),
                 inner_file_info->info_.get_modify_time(), inner_file_info->info_.get_page_size(),
                 inner_file_info->info_.get_file_size(), seg_arr, info, enable_page_cache,
                 rd_info_arr))) {
      LOG_WARN("failed to do preprocess", K(ret));
    } else if (seg_arr.count() != rd_info_arr.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count should be equal", K(ret), K(seg_arr), K(rd_info_arr));
    } else {
      ObStorageObjectHandle read_object_handle;
      for (int64_t i = 0; OB_SUCC(ret) && i < rd_info_arr.count(); i++) {
        ObExternalReadInfo &cur_info = rd_info_arr.at(i);
        cur_info.io_metrics_ = handle.metrics_;
        read_object_handle.reset();
        ObIOFd tmp_fd = fd;
        if (OB_FAIL(inner_async_read_tmp_(
              tmp_fd,
              *inner_file_info,
              cur_info,
              read_object_handle))) {
          LOG_WARN("fail to async pread", KR(ret), KPC(inner_file_info), K(info), K(info));
          for (int64_t j = i; j < rd_info_arr.count(); ++j) {
            if (OB_NOT_NULL(rd_info_arr.at(j).io_callback_) &&
                OB_NOT_NULL(rd_info_arr.at(j).io_callback_->get_allocator())) {
              ObIAllocator *alloc_ = rd_info_arr.at(j).io_callback_->get_allocator();
              static_cast<ObExCachedReadPageIOCallback*>(rd_info_arr.at(j).io_callback_)->ObExCachedReadPageIOCallback::~ObExCachedReadPageIOCallback();
              alloc_->free(rd_info_arr.at(j).io_callback_);
            }
          }
        } else if (OB_FAIL(handle.add_object_handle(read_object_handle, seg_arr.at(i).get_rd_len()))) {
          LOG_WARN("failed to add new object_handle", K(ret), KPC(inner_file_info), K(info), K(handle), K(read_object_handle));
        }
      }

      // // should be delete when call callback by accesser
      // if (OB_FAIL(ret)) {
      // } else if (!enable_page_cache) {
      //   // do nothing
      // } else if (OB_FAIL(handle.wait())) {
      //   LOG_WARN("failed to wait", K(ret));
      // } else {
      //   for (int64_t i = 0; OB_SUCC(ret) && i < rd_info_arr.count(); i++) {
      //     ObExternalReadInfo &cur_info = rd_info_arr.at(i);
      //     if (OB_FAIL(cur_info.io_callback_->inner_process(static_cast<char*>(cur_info.buffer_), handle.object_handles_.at(i).get_data_size()))) {
      //       LOG_WARN("failed to call callback", K(ret), K(i), K(cur_info));
      //     }
      //   }
      // }
    }

    // } else {
    //   ObStorageObjectHandle read_object_handle;
    //   ObExternalDataAccesser *exDAer = MTL(ObExternalDataAccesser*);
    //   if (OB_ISNULL(exDAer)) {
    //     ret = OB_ERR_UNEXPECTED;
    //     LOG_WARN("nullptr ObExternalDataAccesser", K(ret));
    //   } else {
    //     for (int64_t i = 0; OB_SUCC(ret) && i < rd_info_arr.count(); i++) {
    //       if (OB_FAIL(exDAer->async_read(inner_file_info->info_, rd_info_arr.at(i), read_object_handle))) {
    //         LOG_WARN("failed to do async_read", K(ret), KPC(inner_file_info), K(rd_info_arr.at(i)), K(read_object_handle));
    //       } else if (OB_FAIL(handle.add_object_handle(read_object_handle, seg_arr.at(i).get_rd_len()))) {
    //         LOG_WARN("failed to add new object_handle", K(ret), KPC(inner_file_info), K(info), K(handle), K(read_object_handle));
    //       }
    //     }
    //   }
    // }

  }
  return ret;
}

int ObExternalDataAccessMgr::inner_async_read_tmp_(
    ObIOFd &fd,
    InnerAccessFileInfo &inner_file_info,
    const ObExternalReadInfo &external_read_info,
    blocksstable::ObStorageObjectHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObPCachedExternalFileService *ext_file_service = nullptr;
  if (OB_UNLIKELY(!fd.is_valid()) ||
      OB_UNLIKELY(!inner_file_info.is_valid()) ||
      OB_UNLIKELY(!external_read_info.is_valid()) ||
      OB_UNLIKELY(external_read_info.size_ > OB_STORAGE_OBJECT_MGR.get_macro_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), K(inner_file_info), K(external_read_info));
  } else if (OB_ISNULL(ext_file_service = MTL(ObPCachedExternalFileService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPCachedExternalFileService is null", KR(ret), K(fd), K(inner_file_info), K(external_read_info));
  } else if (OB_FAIL(ext_file_service->async_read(inner_file_info.info_, external_read_info, io_handle))) {
    LOG_WARN("fail to async read", KR(ret), K(fd), K(inner_file_info), K(external_read_info));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
