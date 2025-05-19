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
// #include "ob_external_data_accesser.h"

namespace oceanbase
{
namespace sql
{
/***************** ObExternalAccessFileInfo ****************/

bool ObExternalAccessFileInfo::is_valid() const
{
  return modify_time_ >= 0 &&
         access_info_.is_valid() &&
         device_handle_ != nullptr;
}

void ObExternalAccessFileInfo::reset()
{
  url_.reset();
  modify_time_ = -1;
  access_info_.reset();
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
  device_handle_ = nullptr;
}

int ObExternalAccessFileInfo::assign(const ObExternalAccessFileInfo &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    reset();
    url_ = other.url_;
    modify_time_ = other.modify_time_;
    device_handle_ = other.device_handle_;
    if (OB_FAIL(access_info_.assign(other.access_info_))) {
      LOG_WARN("failed to assgin access_info", K(ret), K(other));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid other_file_info", K(other));
  }
  return ret;
}


/***************** FileMapKey ****************/
ObExternalDataAccessMgr::FileMapKey::FileMapKey():
  url_(), modify_time_(-1)
  {}

ObExternalDataAccessMgr::FileMapKey::FileMapKey(const ObString url, const int64_t modify_time):
  url_(url), modify_time_(modify_time)
  {}

bool ObExternalDataAccessMgr::FileMapKey::operator== (const FileMapKey &other) const {
  return (other.url_ == url_) && (other.modify_time_ == modify_time_);
}

uint64_t ObExternalDataAccessMgr::FileMapKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&url_, sizeof(url_), hash_val);
  hash_val = common::murmurhash(&modify_time_, sizeof(modify_time_), hash_val);
  return hash_val;
}

int ObExternalDataAccessMgr::FileMapKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

/***************** ObExternalDataAccessMgr ****************/

ObExternalDataAccessMgr::ObExternalDataAccessMgr() :
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

  const int64_t bucket_num = 524287;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
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
    const ObString url,
    const ObObjectStorageInfo *info,
    const int64_t modify_time,
    ObIOFd &fd)
{
  static constexpr uint64_t OB_STORAGE_ID_EXTERNAL = 2001; // same as value in ob_external_table_access_service.cpp
  int ret = OB_SUCCESS;
  fd.reset();
  FileMapKey key(url, modify_time);
  InnerAccessFileInfo *inner_file_info = nullptr;
  if (OB_ISNULL(info) || modify_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), KPC(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  }

  { // get first
    obsys::ObRLockGuard rg(rwlock_);
    if (OB_FAIL(ret)) {
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
    obsys::ObWLockGuard wg(rwlock_);
    // double check
    if (OB_FAIL(get_file_info_by_file_key_(key, fd, inner_file_info))) {
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
    } else if (FALSE_IT(inner_file_info->info_.url_ = url)) {
    } else if (FALSE_IT(inner_file_info->info_.modify_time_ = modify_time)) {
    } else if (OB_FAIL(inner_file_info->info_.access_info_.assign(*info))) {
      LOG_WARN("failed to assgin access_info", K(ret), K(key), KPC(info));
    } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(inner_file_info->info_.device_handle_,  // out
                                                                fd,  // out
                                                                &inner_file_info->info_.access_info_, // input
                                                                inner_file_info->info_.url_, // input
                                                                OB_STORAGE_ACCESS_READER,
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

    if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
      inner_file_info_alloc_.free(buf);
      inner_file_info = nullptr;
      buf = nullptr;
    }
  }

  return ret;
}


int ObExternalDataAccessMgr::close_file(ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard wg(rwlock_);
  InnerAccessFileInfo *inner_file_info = nullptr;
  if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
    LOG_WARN("failed to get file from file_map", K(ret), KPC(inner_file_info));
  } else if (0 == inner_file_info->dec_ref()) {
    if (OB_FAIL(force_delete_from_file_map_(fd))) {
      LOG_WARN("failed to erase file info", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_FAIL(force_delete_from_fd_map_(FileMapKey(inner_file_info->info_.url_, inner_file_info->info_.modify_time_)))) {
      LOG_WARN("failed to erase fd", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_FAIL(ObBackupIoAdapter::close_device_and_fd(inner_file_info->info_.device_handle_, fd))) {
      LOG_WARN("fail to close device and fd", KR(ret), KPC(inner_file_info));
    }

    fd.reset();
    inner_file_info_alloc_.free(inner_file_info);
  } else if (0 > inner_file_info->dec_ref()) {
    LOG_ERROR("invalid ref_cnt", K(ret), KPC(inner_file_info));
    ob_abort();
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
      modify_time = inner_file_info->info_.modify_time_;
    }
  }
  return ret;
}

int ObExternalDataAccessMgr::get_file_size_by_fd(
    const ObIOFd &fd,
    int64_t &file_size)
{
  int ret = OB_SUCCESS;
  if (!fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else {
    obsys::ObRLockGuard rg(rwlock_);
    InnerAccessFileInfo *inner_file_info = nullptr;
    if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
      LOG_WARN("failed to get file from file_map", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_ISNULL(inner_file_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_file_info is nullptr", K(ret), K(fd));
    } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(inner_file_info->info_.url_,
                                                          &inner_file_info->info_.access_info_,
                                                          file_size))) {
      STORAGE_LOG(WARN, "fail to get file length", KR(ret), KPC(inner_file_info));
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


int ObExternalDataAccessMgr::fill_cache_hit_buf_and_get_cache_miss_segments_(
    const ObIOFd &fd,
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
  int64_t modify_time = -1;
  uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(ret)) {
  } else {
    obsys::ObRLockGuard rg(rwlock_);
    if (OB_FAIL(get_modify_time_by_fd_(fd, modify_time))) {
      LOG_WARN("failed to get modify_time", K(ret), K(fd), K(modify_time));
    }
  }

  // get data from page_cache, and record cache_miss_segment
  ObExtCacheMissSegment cur_seg;
  ObExternalDataPageCacheValueHandle handle;
  const int64_t page_size = ObExternalDataPageCache::PAGE_SIZE;
  // cur_page_offset of page_cache
  int64_t cur_page_offset = (rd_offset / page_size) * page_size;
  // cur_rd_offset to external file
  int64_t cur_rd_offset = rd_offset;
  while (OB_SUCC(ret) && cur_page_offset < rd_offset + rd_len) {
    ObExternalDataPageCacheKey cur_key(fd, modify_time, cur_page_offset, tenant_id);
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

    if (enable_page_cache && OB_SUCC(kv_cache_.get_page(cur_key, handle)) && cur_buf_size <= handle.value_->get_valid_data_size()) {
      // cache hit;
      MEMCPY(buffer + cur_pos, // dest
             handle.value_->get_buffer() + ( cur_rd_offset % page_size ), // src
             cur_buf_size); // size
      exReadhandle.cache_hit_size_ += cur_buf_size;
      if (OB_FAIL(record_one_and_reset_seg_(cur_seg, seg_arr))) {
        LOG_WARN("failed to record seg", K(ret), K(cur_seg), K(seg_arr));
      }
    } else if (enable_page_cache && ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to get page", KR(ret), K(fd), K(cur_key));
    } else { // (!enable_page_cache || ret == OB_ENTRY_NOT_EXIST)
      // cache miss;
      ret = OB_SUCCESS;
      if (OB_FAIL(cur_seg.push_piece(buffer + cur_pos, cur_rd_offset, cur_buf_size))) {
        LOG_WARN("failed to push_piece", K(ret), KP(buffer + cur_pos), K(cur_rd_offset), K(cur_buf_size), K(rd_offset), K(rd_len));
      } else if (cur_seg.reach_2MB_boundary() && OB_FAIL(record_one_and_reset_seg_(cur_seg, seg_arr))) {
        LOG_WARN("failed to record seg", K(ret), K(cur_seg), K(seg_arr));
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

  FLOG_INFO("FEIDU DEBUG:IO IO SEG", K(ret), K(rd_offset), K(rd_len), K(seg_arr));
  return ret;
}

int ObExternalDataAccessMgr::get_rd_info_arr_by_cache_miss_seg_arr_(
    const ObIOFd &fd,
    const int64_t modify_time,
    const ObIArray<ObExtCacheMissSegment> &seg_arr,
    const ObExternalReadInfo &src_rd_info,
    const bool enable_page_cache,
    ObIArray<ObExternalReadInfo> &rd_info_arr)
{
  int ret = OB_SUCCESS;
  char *buffer_head = static_cast<char *>(src_rd_info.buffer_);
  const int64_t page_size = ObExternalDataPageCache::PAGE_SIZE;
  uint64_t tenant_id = MTL_ID();
  ObExternalDataPageCache::ObExCachedReadPageIOCallback *callback = nullptr;
  void *callback_buf = nullptr;
  void *page_cache_buf_ = nullptr;

  if (!enable_page_cache) {
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

      // init cur_read_info basic_info
      const int64_t cur_buf_pos = cur_seg.get_rd_offset() - src_rd_info.offset_; // buffer_head + cur_buf_pos
      const int64_t cur_cache_key_offset = cur_seg.get_page_offset();
      const int64_t cur_cache_buf_len = cur_seg.get_page_count() * page_size;

      // alloc, create callback and record on read_info;
      if (OB_ISNULL(page_cache_buf_ = callback_alloc_.alloc(cur_cache_buf_len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloca mem", K(ret), K(cur_cache_buf_len), KP(page_cache_buf_));
      } else {
        ObExternalReadInfo cur_rd_info(cur_cache_key_offset, page_cache_buf_, cur_cache_buf_len, src_rd_info.io_timeout_ms_, src_rd_info.io_desc_);
        // TODO mem of callback_buf should be free by @shifangdan.sfd's callback
        if (OB_ISNULL(callback_buf = callback_alloc_.alloc(sizeof(ObExternalDataPageCache::ObExCachedReadPageIOCallback)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloca mem", K(ret), K(sizeof(ObExternalDataPageCache::ObExCachedReadPageIOCallback)), KP(callback_buf));
        } else {
          ObExternalDataPageCacheKey key(fd, modify_time, cur_cache_key_offset, tenant_id);
          callback = new (callback_buf) ObExternalDataPageCache::ObExCachedReadPageIOCallback(key, buffer_head + cur_buf_pos, cur_seg.get_rd_offset() % page_size,
          cur_seg.get_rd_len(), &kv_cache_);
          if (OB_FAIL(callback->set_allocator(&callback_alloc_))) {
            LOG_WARN("failed to set allocator", K(ret), KPC(callback), K(&callback_alloc_));
          }

          if (OB_SUCC(ret)) {
            cur_rd_info.io_callback_ = callback;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rd_info_arr.push_back(cur_rd_info))) {
          LOG_WARN("failed to push back cur_rd_info", K(ret), K(i), K(cur_seg), K(rd_info_arr), K(cur_rd_info));
        }
      }
    }
  }
  FLOG_INFO("FEIDU DEBUG:IO info_arr", K(ret), K(rd_info_arr));
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

  if (!fd.is_valid() || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), K(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else {
    int64_t read_size = -1;
    obsys::ObRLockGuard rg(rwlock_);
    InnerAccessFileInfo *inner_file_info = nullptr;
    if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
      LOG_WARN("failed to get file from file_map", K(ret), K(fd), KPC(inner_file_info));
    } else if (OB_ISNULL(inner_file_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner_file_info is nullptr", K(ret), K(fd));
    } else if (OB_FAIL(fill_cache_hit_buf_and_get_cache_miss_segments_(fd, info.offset_, info.size_, enable_page_cache, static_cast<char*>(info.buffer_), handle, seg_arr))) {
      LOG_WARN("failed to do preprocess", K(ret));
    } else if (OB_FAIL(get_rd_info_arr_by_cache_miss_seg_arr_(fd, inner_file_info->info_.modify_time_, seg_arr, info, enable_page_cache, rd_info_arr))) {
      LOG_WARN("failed to do preprocess", K(ret));
    } else if (seg_arr.count() != rd_info_arr.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count should be equal", K(ret), K(seg_arr), K(rd_info_arr));
    } else {
      ObIOFd tmp_fd = fd;
      ObStorageObjectHandle read_object_handle;
      for (int64_t i = 0; OB_SUCC(ret) && i < rd_info_arr.count(); i++) {
        ObExternalReadInfo &cur_info = rd_info_arr.at(i);
        read_object_handle.reset();
        CONSUMER_GROUP_FUNC_GUARD(share::PRIO_IMPORT);
        if (OB_FAIL(ObBackupIoAdapter::async_pread(
              *inner_file_info->info_.device_handle_,
              tmp_fd,
              static_cast<char *>(cur_info.buffer_),
              cur_info.offset_,
              cur_info.size_,
              read_object_handle.get_io_handle()))) {
          LOG_WARN("fail to async pread", KR(ret), KPC(inner_file_info), K(info), K(info));
        } else if (OB_FAIL(handle.add_object_handle(read_object_handle, seg_arr.at(i).get_rd_len()))) {
          LOG_WARN("failed to add new object_handle", K(ret), KPC(inner_file_info), K(info), K(handle), K(read_object_handle));
        }
      }

      // should be delete when call callback by accesser
      if (OB_FAIL(ret)) {
      } else if (!enable_page_cache) {
        // do nothing
      } else if (OB_FAIL(handle.wait())) {
        LOG_WARN("failed to wait", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rd_info_arr.count(); i++) {
          ObExternalReadInfo &cur_info = rd_info_arr.at(i);
          if (OB_FAIL(cur_info.io_callback_->inner_process(static_cast<char*>(cur_info.buffer_), handle.object_handles_.at(i).get_data_size()))) {
            LOG_WARN("failed to call callback", K(ret), K(i), K(cur_info));
          }
        }
      }
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
int ObExternalDataAccessMgr::pread(
    const ObIOFd &fd,
    const ObExternalReadInfo &info,
    const bool enable_page_cache,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  InnerAccessFileInfo *inner_file_info = nullptr;
  obsys::ObRLockGuard rg(rwlock_);

  ObExternalFileReadHandle read_handle;
  if (!fd.is_valid() || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), K(info));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(fd), K(lbt()));
  } else if (OB_FAIL(async_read(fd, info, enable_page_cache, read_handle))) {
    LOG_WARN("failed to async_read", K(ret), K(fd), K(info), K(read_handle));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("wait failed", K(ret), K(fd), K(info));
  } else if (OB_FAIL(read_handle.get_user_buf_read_data_size(read_size))) {
    LOG_WARN("failed to get read_size", K(ret), K(read_size), K(info), K(read_handle));
  }
  FLOG_INFO("FEIDU DEBUG: Read size and Real_read_size", K(ret), K(info), K(read_size), K(read_handle));


  // } else if (OB_FAIL(file_map_.get_refactored(fd, inner_file_info))) {
  //   LOG_WARN("failed to get file from file_map", K(ret), K(fd), KPC(inner_file_info));
  // } else if (OB_ISNULL(inner_file_info)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("inner_file_info is nullptr", K(ret), K(fd));
  // } else {
  //   ObIOHandle io_handle;
  //   CONSUMER_GROUP_FUNC_GUARD(share::PRIO_IMPORT);
  //   if (OB_FAIL(ObBackupIoAdapter::async_pread(*inner_file_info->info_.device_handle_, tmp_fd,
  //       static_cast<char *>(info.buffer_), info.offset_, info.size_, io_handle))) {
  //     LOG_WARN("fail to async pread", KR(ret), KPC(inner_file_info), K(info), K(info));
  //   } else if (OB_FAIL(io_handle.wait())) {
  //     LOG_WARN("fail to wait pread result", KR(ret), KPC(inner_file_info), K(info), K(info));
  //   } else {
  //     read_size = io_handle.get_data_size();
  //   }
  // }

  return ret;
}

} // namespace sql
} // namespace oceanbase
