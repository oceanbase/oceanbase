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

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"
#include <math.h>

namespace oceanbase
{
namespace tmp_file
{
ObSharedNothingTmpFile::ObSharedNothingTmpFile()
    : ObITmpFile(),
      tmp_file_block_manager_(nullptr),
      write_cache_(nullptr),
      meta_tree_(),
      serial_write_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      alloc_incomplete_page_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      allocated_file_size_(0),
      pre_allocated_file_size_(0),
      pre_allocated_batch_page_num_(0),
      write_info_(),
      read_info_()
{
  mode_ = ObTmpFileMode::SHARED_NOTHING;
}

ObSharedNothingTmpFile::~ObSharedNothingTmpFile()
{
  reset();
}

int ObSharedNothingTmpFile::init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
                                 ObTmpFileBlockManager *block_manager,
                                 ObTmpFileWriteCache *write_cache,
                                 ObIAllocator *callback_allocator,
                                 const char* label)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_FAIL(ObITmpFile::init(tenant_id, dir_id, fd,
                                      callback_allocator,
                                      label))) {
    LOG_WARN("init ObITmpFile failed", KR(ret), K(tenant_id), K(dir_id), K(fd), KP(label));
  } else if (OB_ISNULL(block_manager) || OB_ISNULL(write_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(block_manager), KP(write_cache));
  } else if (OB_FAIL(meta_tree_.init(fd, write_cache, callback_allocator, block_manager))) {
    LOG_WARN("fail to init meta tree", KR(ret), K(fd), KP(write_cache));
  } else {
    read_info_.last_access_ts_ = ObTimeUtility::current_time();
    write_info_.last_modify_ts_ = ObTimeUtility::current_time();
    pre_allocated_batch_page_num_ = ObTmpFileGlobal::TMP_FILE_MIN_SHARED_PRE_ALLOC_PAGE_NUM;
    tmp_file_block_manager_ = block_manager;
    write_cache_ = write_cache;
  }
  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }

  return ret;
}

void ObSharedNothingTmpFile::reset()
{
  if (IS_INIT) {
    tmp_file_block_manager_ = nullptr;
    write_cache_ = nullptr;
    meta_tree_.reset();
    allocated_file_size_ = 0;
    pre_allocated_file_size_ = 0;
    pre_allocated_batch_page_num_ = 0;
    write_info_.reset();
    read_info_.reset();
    ObITmpFile::reset();
  }
}

int ObSharedNothingTmpFile::release_resource()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    LOG_INFO("tmp file release_resource start", KR(ret), K(fd_), KPC(this));
    ObTmpFileWriteCacheKey key;
    key.type_ = PageType::DATA;
    key.fd_ = fd_;
    for (int64_t virtual_page_id = 0; virtual_page_id <= get_page_virtual_id_(allocated_file_size_, true); virtual_page_id++) {
      key.virtual_page_id_ = virtual_page_id;
      if (OB_FAIL(write_cache_->free_page(key))) {
        if (ret != OB_ENTRY_NOT_EXIST) {
          LOG_WARN("fail to free page", KR(ret), K(key));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    if (FAILEDx(meta_tree_.clear(truncated_offset_, pre_allocated_file_size_))) {
      LOG_ERROR("fail to clear meta tree", KR(ret), K(fd_), K(truncated_offset_), K(pre_allocated_file_size_));
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::inner_delete_file_()
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObSharedNothingTmpFile::inner_read_valid_part_(ObTmpFileIOReadCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(io_ctx.get_todo_size() > 0)) {
    // if 'read_size' + read_offset > file_size_, we will also read the valid data, but return OB_ITER_END at lase
    const int64_t read_size = MIN(io_ctx.get_todo_size(), file_size_ - io_ctx.get_start_read_offset_in_file());
    ObArray<int64_t> uncached_pages;
    if (OB_FAIL(inner_read_cached_page_(io_ctx, uncached_pages))) {
      LOG_WARN("fail to read cached data", KR(ret), KPC(this), K(io_ctx));
    } else if (uncached_pages.count() > 0 && OB_FAIL(inner_read_uncached_page_(io_ctx, uncached_pages))) {
      LOG_WARN("fail to read uncached data", KR(ret), KPC(this), K(io_ctx),
               K(uncached_pages.count()), KP(&uncached_pages));
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), KPC(this), K(io_ctx), K(read_size));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() > 0 && io_ctx.get_start_read_offset_in_file() >= file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(io_ctx));
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::inner_read_cached_page_(ObTmpFileIOReadCtx &io_ctx, ObIArray<int64_t> &uncached_pages)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> uncached_pages_in_write_cache;
  const int64_t io_begin_read_offset = io_ctx.get_start_read_offset_in_file();
  const int64_t io_end_read_offset = MIN(io_ctx.get_end_read_offset_in_file(), file_size_);
  const int64_t io_begin_read_page_virtual_id = get_page_virtual_id_(io_begin_read_offset, false);
  const int64_t io_end_read_page_virtual_id = get_page_virtual_id_(io_end_read_offset, true);
  ObTmpFileWriteCacheKey write_cache_key;
  write_cache_key.type_ = PageType::DATA;
  write_cache_key.fd_ = fd_;
  ObTmpFilePageHandle page_handle;
  for (int64_t virtual_page_id = io_begin_read_page_virtual_id;
      OB_SUCC(ret) && virtual_page_id <= io_end_read_page_virtual_id;
      virtual_page_id++) {
    write_cache_key.virtual_page_id_ = virtual_page_id;
    page_handle.reset();

    if (OB_FAIL(write_cache_->get_page(write_cache_key, page_handle))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
        if (OB_FAIL(uncached_pages_in_write_cache.push_back(virtual_page_id))) {
          LOG_WARN("fail to push back virtual page id", KR(ret), K(fd_), K(virtual_page_id));
        }
        LOG_DEBUG("page not in write cache", KR(ret), K(fd_), K(virtual_page_id));
      } else {
        LOG_WARN("fail to get page", KR(ret), K(fd_), K(write_cache_key));
      }
    } else {
      ObTmpFilePage *page = page_handle.get_page();
      if (OB_ISNULL(page)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page is null", KR(ret), K(fd_), K(write_cache_key));
      } else if (OB_UNLIKELY(page->is_loading())) {
        if (OB_FAIL(uncached_pages_in_write_cache.push_back(virtual_page_id))) {
          LOG_WARN("fail to push back virtual page id", KR(ret), K(fd_), K(virtual_page_id));
        }
        LOG_DEBUG("page is loading", KR(ret), K(fd_), KPC(page));
      } else {  // read page
        const int64_t start_read_offset = virtual_page_id == io_begin_read_page_virtual_id ?
                                          io_begin_read_offset :
                                          get_page_begin_offset_by_virtual_id_(virtual_page_id);
        const int64_t end_read_offset = virtual_page_id == io_end_read_page_virtual_id ?
                                        io_end_read_offset :
                                        get_page_end_offset_by_virtual_id_(virtual_page_id);
        const int64_t read_size = end_read_offset - start_read_offset;
        const int64_t buf_offset = start_read_offset - io_begin_read_offset;
        char *read_buf = io_ctx.get_todo_buffer() + buf_offset;
        char *page_buf = page->get_buffer();
        if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(read_buf, read_size))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid buf range", KR(ret), K(fd_), K(read_buf), K(read_size), K(io_ctx));
        } else {
          LOG_DEBUG("DATA_DEBUG: hit write cache", K(fd_), K(virtual_page_id), K(get_offset_in_page_(start_read_offset)), K(read_size));
          MEMCPY(read_buf, page_buf + get_offset_in_page_(start_read_offset), read_size);
        }
        LOG_DEBUG("page read relative offset", KR(ret), K(fd_), K(virtual_page_id), K(start_read_offset),
                 K(end_read_offset), K(read_size), K(io_begin_read_offset), K(buf_offset),
                 KPC(page));
      }
    }
  } // end for

  if (OB_SUCC(ret) && !uncached_pages_in_write_cache.empty()) {
    if (io_ctx.is_disable_page_cache()) {
      if (OB_FAIL(uncached_pages.assign(uncached_pages_in_write_cache))) {
        LOG_WARN("fail to assign uncached_pages_in_write_cache", KR(ret), K(fd_),
                 K(uncached_pages_in_write_cache.count()), KP(&uncached_pages_in_write_cache));
      }
    } else {
      ObTmpPageCacheKey kv_cache_key;
      kv_cache_key.set_tenant_id(MTL_ID());
      ObTmpPageValueHandle p_handle;
      for (int64_t i = 0; OB_SUCC(ret) && i < uncached_pages_in_write_cache.count(); ++i) {
        const int64_t virtual_page_id = uncached_pages_in_write_cache.at(i);
        write_cache_key.virtual_page_id_ = virtual_page_id;
        kv_cache_key.set_page_key(write_cache_key);
        p_handle.reset();
        if (OB_UNLIKELY(!kv_cache_key.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid kv cache key", KR(ret), K(fd_), K(kv_cache_key));
        } else if (OB_FAIL(ObTmpPageCache::get_instance().get_page(kv_cache_key, p_handle))) {
          if (ret != OB_ENTRY_NOT_EXIST) {
            LOG_WARN("fail to get page", KR(ret), K(fd_), K(kv_cache_key));
          } else {
            ret = OB_SUCCESS;
            if (OB_FAIL(uncached_pages.push_back(virtual_page_id))) {
              LOG_WARN("fail to push back virtual page id", KR(ret), K(fd_), K(virtual_page_id));
            }
          }
        } else { // read page
          const int64_t start_read_offset = virtual_page_id == io_begin_read_page_virtual_id ?
                                            io_begin_read_offset :
                                            get_page_begin_offset_by_virtual_id_(virtual_page_id);
          const int64_t end_read_offset = virtual_page_id == io_end_read_page_virtual_id ?
                                          io_end_read_offset :
                                          get_page_end_offset_by_virtual_id_(virtual_page_id);
          const int64_t read_size = end_read_offset - start_read_offset;
          const int64_t buf_offset = start_read_offset - io_begin_read_offset;
          char *read_buf = io_ctx.get_todo_buffer() + buf_offset;
          ObTmpFileIOReadCtx::ObPageCacheHandle page_handle(read_buf, get_offset_in_page_(start_read_offset), read_size);
          page_handle.page_handle_.move_from(p_handle);
          if (OB_UNLIKELY(!page_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid page handle", KR(ret), K(fd_), K(kv_cache_key), K(page_handle));
          } else if (OB_FAIL(io_ctx.get_page_cache_handles().push_back(page_handle))) {
            LOG_WARN("Fail to push back into page_handles", KR(ret), K(fd_), K(page_handle));
          }
          LOG_DEBUG("DATA_DEBUG: hit kv cache", KR(ret), K(fd_), K(virtual_page_id), K(get_offset_in_page_(start_read_offset)), K(read_size));
        }
      } // end for
    }
  }

  // update stat
  if (OB_SUCC(ret)) {
    const int64_t total_read_page_num = io_end_read_page_virtual_id - io_begin_read_page_virtual_id + 1;
    int64_t read_write_cache_cnt = total_read_page_num;
    int64_t hit_write_cache_cnt = total_read_page_num - uncached_pages_in_write_cache.count();
    int64_t read_kv_cache_cnt = uncached_pages_in_write_cache.count();
    int64_t hit_kv_cache_cnt = uncached_pages_in_write_cache.count() - uncached_pages.count();
    io_ctx.update_read_wbp_page_stat(hit_write_cache_cnt, read_write_cache_cnt);
    io_ctx.update_read_kv_cache_page_stat(hit_kv_cache_cnt, read_kv_cache_cnt);
  }
  return ret;
}

// please guarantee the uncached_pages is ordered
int ObSharedNothingTmpFile::inner_read_uncached_page_(ObTmpFileIOReadCtx &io_ctx, ObIArray<int64_t> &uncached_pages)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(uncached_pages.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uncached_pages is empty", KR(ret), K(fd_));
  } else {
    int64_t start_range_idx = 0;
    int64_t end_range_idx = 0;
    bool range_over = false;
    ObSharedNothingTmpFileDataItem final_read_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < uncached_pages.count(); ++i) {
      const int64_t virtual_page_id = uncached_pages.at(i);
      if (i == (uncached_pages.count() - 1) || virtual_page_id != (uncached_pages.at(i + 1) - 1)) {
        end_range_idx = i;
        range_over = true;
      }

      if (range_over) {
        if (OB_FAIL(inner_read_uncached_continuous_page_(io_ctx, uncached_pages,
                                                         start_range_idx, end_range_idx,
                                                         final_read_item))) {
          LOG_WARN("fail to read uncached continuous data", KR(ret), K(fd_),
                   K(start_range_idx), K(end_range_idx), K(final_read_item));
        } else {
          start_range_idx = i + 1;
          range_over = false;
        }
      }
    }
  }
  return ret;
}

int ObSharedNothingTmpFile::inner_read_uncached_continuous_page_(ObTmpFileIOReadCtx &io_ctx,
                                                                 ObIArray<int64_t> &uncached_pages,
                                                                 const int64_t range_start_idx,
                                                                 const int64_t range_end_idx,
                                                                 ObSharedNothingTmpFileDataItem& previous_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_start_idx < 0 || range_start_idx > range_start_idx ||
                  range_end_idx >= uncached_pages.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid range idx", KR(ret), K(fd_),
             K(range_start_idx), K(range_start_idx), K(uncached_pages.count()));
  } else {
    const int64_t io_begin_read_offset = io_ctx.get_start_read_offset_in_file();
    const int64_t io_end_read_offset = io_ctx.get_end_read_offset_in_file();
    const int64_t io_begin_read_page_virtual_id = get_page_virtual_id_(io_begin_read_offset, false);
    const int64_t io_end_read_page_virtual_id = get_page_virtual_id_(io_end_read_offset, true);
    const int64_t range_start_read_offset = uncached_pages.at(range_start_idx) == io_begin_read_page_virtual_id ?
                                            io_begin_read_offset :
                                            get_page_begin_offset_by_virtual_id_(uncached_pages.at(range_start_idx));
    const int64_t range_end_read_offset = uncached_pages.at(range_end_idx) == io_end_read_page_virtual_id ?
                                          io_end_read_offset :
                                          get_page_end_offset_by_virtual_id_(uncached_pages.at(range_end_idx));
    const int64_t range_read_size = range_end_read_offset - range_start_read_offset;
    common::ObArray<ObSharedNothingTmpFileDataItem> data_items;
    common::ObArray<int64_t> sub_range_bounds;

    if (OB_FAIL(meta_tree_.search_data_items(range_start_read_offset, range_read_size, io_ctx.get_io_timeout_ms(), data_items))) {
      LOG_WARN("fail to search data items", KR(ret), K(fd_), K(range_start_read_offset), K(range_read_size), K(io_ctx.get_io_timeout_ms()));
    } else if (OB_UNLIKELY(data_items.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data items is empty", KR(ret), K(fd_), K(range_start_read_offset), K(range_end_read_offset));
    } else if (OB_FAIL(sub_range_bounds.prepare_allocate(data_items.count()))) {
      LOG_WARN("fail to prepare allocate", KR(ret), K(fd_), K(data_items.count()));
    } else if (data_items.count() == 1) {
      sub_range_bounds.at(0) = range_end_idx;
    } else { // cal sub_range_bounds
      int64_t item_start_idx = range_start_idx;
      int64_t item_end_idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count(); ++i) {
        ObSharedNothingTmpFileDataItem &item = data_items.at(i);
        int64_t item_high_bound = 0;
        if (OB_UNLIKELY(item_start_idx > range_end_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("item start idx is larger than range end idx", KR(ret), K(fd_), K(item_start_idx), K(range_end_idx));
        } else if (OB_UNLIKELY(!item.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid item", KR(ret), K(fd_), K(item));
        } else {
          item_high_bound = item.virtual_page_id_ + item.physical_page_num_ - 1;
          for (int64_t idx = item_start_idx; idx <= range_end_idx; ++idx) {
            if (idx == range_end_idx || uncached_pages.at(idx+1) > item_high_bound) {
              item_end_idx = idx;
              break;
            }
          } // end for
          sub_range_bounds.at(i) = item_end_idx;
          item_start_idx = item_end_idx + 1;
        }
      } // end for
    }

    // read data from block for each range (sub_range_bounds' count is equal to that of data_items)
    int64_t read_io_page_num = 0;
    const int64_t read_cnt = data_items.count();
    int64_t aggregate_read_cnt = io_ctx.is_prefetch() ? read_cnt : 0;
    if (OB_FAIL(ret)) {
    } else if (io_ctx.is_disable_page_cache()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count(); ++i) {
        ObSharedNothingTmpFileDataItem &item = data_items.at(i);
        const int64_t item_start_idx = i == 0 ?
                                       range_start_idx :
                                       (sub_range_bounds.at(i-1) + 1);
        const int64_t item_end_idx = sub_range_bounds.at(i);
        const int64_t begin_read_offset = MAX(uncached_pages.at(item_start_idx) * ObTmpFileGlobal::PAGE_SIZE,
                                              range_start_read_offset);
        const int64_t end_read_offset = MIN((uncached_pages.at(item_end_idx) + 1) * ObTmpFileGlobal::PAGE_SIZE,
                                            range_end_read_offset);
        const int64_t read_size = end_read_offset - begin_read_offset;
        const int64_t block_begin_read_offset = begin_read_offset - item.virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE +
                                                item.physical_page_id_ * ObTmpFileGlobal::PAGE_SIZE;
        const int64_t buf_offset = begin_read_offset - io_begin_read_offset;
        char *read_buf = io_ctx.get_todo_buffer() + buf_offset;
        if (OB_FAIL(inner_direct_read_from_block_(io_ctx, item.block_index_,
                                                  block_begin_read_offset,
                                                  read_size, read_buf))) {
          LOG_WARN("fail to direct read from block", KR(ret), K(fd_), K(item),
                    K(block_begin_read_offset), K(read_size), KP(read_buf));
        } else {
          read_io_page_num += get_page_virtual_id_(end_read_offset, true) - get_page_virtual_id_(begin_read_offset, false) + 1;
        }
        LOG_DEBUG("direct read from block", KR(ret), K(fd_), K(begin_read_offset), K(end_read_offset), K(read_size),
                 K(range_start_read_offset), K(range_end_read_offset), K(data_items.count()), K(i), K(item),
                 K(item_start_idx), K(uncached_pages.at(item_start_idx)), K(item_end_idx), K(uncached_pages.at(item_end_idx)),
                 K(sub_range_bounds.at(i)));
      } // end for
    } else {
      // TODO: wanyue.wy
      // it might happened that a set of pages in the same data_item has been divided into several sub_ranges.
      // in this case, if the prefetch mechanism is used, for avoiding redundant io requests,
      // we should check whether all pages of sub ranges are in the kv cache when prefetched_first_item == true

      // bool prefetched_first_item = io_ctx.is_prefetch() && previous_item == data_items.at(0);

      for (int64_t i = 0; OB_SUCC(ret) && i < data_items.count(); ++i) {
        const ObSharedNothingTmpFileDataItem &item = data_items.at(i);
        const int64_t item_start_idx = i == 0 ?
                                       range_start_idx :
                                       (sub_range_bounds.at(i-1) + 1);
        const int64_t item_end_idx = sub_range_bounds.at(i);
        const int64_t begin_read_offset = MAX(uncached_pages.at(item_start_idx) * ObTmpFileGlobal::PAGE_SIZE,
                                              range_start_read_offset);
        const int64_t user_end_read_offset = MIN((uncached_pages.at(item_end_idx) + 1) * ObTmpFileGlobal::PAGE_SIZE,
                                                 range_end_read_offset);
        const int64_t block_begin_read_offset = get_page_begin_offset_(begin_read_offset) - item.virtual_page_id_ * ObTmpFileGlobal::PAGE_SIZE +
                                                item.physical_page_id_ * ObTmpFileGlobal::PAGE_SIZE;
        const int64_t read_size = user_end_read_offset - begin_read_offset;
        const int64_t end_read_offset = io_ctx.is_prefetch() ?
                                        (item.virtual_page_id_ + item.physical_page_num_) * ObTmpFileGlobal::PAGE_SIZE :
                                        user_end_read_offset;
        const int64_t buf_offset = begin_read_offset - io_begin_read_offset;
        char *read_buf = io_ctx.get_todo_buffer() + buf_offset;

        if (OB_FAIL(inner_cached_read_from_block_(io_ctx, item.block_index_, block_begin_read_offset,
                                                  begin_read_offset, end_read_offset,
                                                  read_size, read_buf))) {
          LOG_WARN("fail to cached read from block", KR(ret), K(fd_), K(item),
                   K(begin_read_offset), K(end_read_offset), K(read_size), K(io_ctx.is_prefetch()));
        } else {
          read_io_page_num += get_page_virtual_id_(end_read_offset, true) - get_page_virtual_id_(begin_read_offset, false) + 1;
        }
        LOG_DEBUG("cached read from block", KR(ret), K(fd_), K(begin_read_offset), K(end_read_offset),
                 K(read_size), K(user_end_read_offset), K(range_start_read_offset), K(range_end_read_offset),
                 K(io_begin_read_offset), K(io_ctx.is_prefetch()), K(item));
      } // end for
      // if (OB_SUCC(ret) && io_ctx.is_prefetch()) {
      //   previous_item = data_items.at(data_items.count() - 1);
      // }
    }

    if (OB_SUCC(ret)) {
      io_ctx.update_read_uncached_page_stat(read_io_page_num, read_cnt, aggregate_read_cnt);
    }

    const int64_t range_start_virtual_page_id = get_page_virtual_id_(range_start_read_offset, false);
    const int64_t range_end_virtual_page_id = get_page_virtual_id_(range_end_read_offset, true);
    LOG_DEBUG("DATA_DEBUG: hit disk", KR(ret), K(fd_), K(range_start_virtual_page_id), K(range_end_virtual_page_id),
             K(get_offset_in_page_(range_start_read_offset)), K(get_offset_in_page_(range_end_read_offset)),
             K(range_read_size));
  }

  LOG_DEBUG("read data uncached continuous page over", KR(ret), K(fd_),
           K(uncached_pages.at(range_start_idx)), K(uncached_pages.at(range_end_idx)),
           K(range_start_idx), K(range_end_idx), K(uncached_pages.count()));
  return ret;
}

int ObSharedNothingTmpFile::inner_direct_read_from_block_(ObTmpFileIOReadCtx &io_ctx,
                                                          const int64_t block_index,
                                                          const int64_t block_begin_read_offset,
                                                          const int64_t read_size,
                                                          char *read_buf)
{
  int ret = OB_SUCCESS;
  ObTmpFileBlockHandle block_handle;
  ObTmpPageCacheReadInfo read_info;
  if (OB_UNLIKELY(block_begin_read_offset + read_size > ObTmpFileGlobal::SN_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(block_begin_read_offset), K(read_size));
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else if (OB_UNLIKELY(!block_handle.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block handle is not inited", KR(ret), K(fd_), K(block_index));
  } else if (OB_UNLIKELY(!block_handle.get()->get_macro_block_id().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block handle is not inited", KR(ret), K(fd_), K(block_index));
  } else {
    ObTmpFileIOReadCtx::ObIOReadHandle io_read_handle(read_buf,
                                                      0 /*offset_in_src_data_buf_*/,
                                                      read_size,
                                                      block_handle);
    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(read_info.init_read(block_handle.get()->get_macro_block_id(),
                                           read_size, block_begin_read_offset,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init read info", KR(ret), K(fd_), K(block_handle),
              K(read_size), K(block_begin_read_offset), K(io_ctx.get_io_handles().count()));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().direct_read(read_info, *callback_allocator_))) {
      LOG_WARN("fail to direct_read", KR(ret), K(fd_), K(read_info), K(io_ctx), KP(callback_allocator_));
    }
  }
  return ret;
}

// we will put the page whose offset is in the range of ['file_begin_read_offset', 'file_end_read_offset'] into kv cache.
// and copy the data of from 'file_begin_read_offset' with size of 'user_read_size' to user's read buf.
// (due to the prefetch mechanism, 'end_read_offset_in_file - begin_read_offset_in_file' >= 'user_read_size')
int ObSharedNothingTmpFile::inner_cached_read_from_block_(ObTmpFileIOReadCtx &io_ctx, const int64_t block_index,
                                                          const int64_t block_begin_read_offset,
                                                          const int64_t file_begin_read_offset,
                                                          const int64_t file_end_read_offset,
                                                          const int64_t user_read_size,
                                                          char *read_buf)
{
  int ret = OB_SUCCESS;
  ObArray<ObTmpPageCacheKey> page_keys;
  const int64_t begin_virtual_page_id = get_page_virtual_id_(file_begin_read_offset, false);
  const int64_t end_virtual_page_id = get_page_virtual_id_(file_end_read_offset, true);
  // from loaded disk block buf, from "offset_in_block_buf" read "user_read_size" size data to user's read buf
  const int64_t offset_in_block_buf = file_begin_read_offset - get_page_begin_offset_(file_begin_read_offset);

  for (int64_t virtual_page_id = begin_virtual_page_id;
       OB_SUCC(ret) && virtual_page_id <= end_virtual_page_id; virtual_page_id++) {
    ObTmpPageCacheKey key(fd_, virtual_page_id, tenant_id_);
    if (OB_FAIL(page_keys.push_back(key))) {
      LOG_WARN("fail to push back", KR(ret), K(fd_), K(key));
    }
  }
  const int64_t io_read_size = page_keys.count() * ObTmpFileGlobal::PAGE_SIZE;
  ObTmpFileBlockHandle block_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(io_read_size < user_read_size ||
                         block_begin_read_offset + io_read_size > ObTmpFileGlobal::SN_BLOCK_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid io_read_size", KR(ret), K(block_begin_read_offset), K(io_read_size), K(user_read_size));
  } else if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(block_index, block_handle))) {
    LOG_WARN("fail to get tmp file block_handle", KR(ret), K(fd_), K(block_index));
  } else if (OB_UNLIKELY(!block_handle.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block handle is not inited", KR(ret), K(fd_), K(block_index));
  } else if (OB_UNLIKELY(!block_handle.get()->get_macro_block_id().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block id is invalid", KR(ret), K(fd_), K(block_handle));
  } else {
    ObTmpFileIOReadCtx::ObIOReadHandle io_read_handle(read_buf,
                                                      offset_in_block_buf,
                                                      user_read_size, block_handle);
    ObTmpPageCacheReadInfo read_info;

    if (OB_FAIL(io_ctx.get_io_handles().push_back(io_read_handle))) {
      LOG_WARN("Fail to push back into io_handles", KR(ret), K(fd_));
    } else if (OB_FAIL(read_info.init_read(block_handle.get()->get_macro_block_id(),
                                           io_read_size,
                                           block_begin_read_offset,
                                           io_ctx.get_io_flag(), io_ctx.get_io_timeout_ms(),
                                           &io_ctx.get_io_handles().at(io_ctx.get_io_handles().count()-1).handle_))) {
      LOG_WARN("fail to init sn read info", KR(ret), K(fd_), K(block_handle), K(page_keys.count()),
                                            K(block_begin_read_offset), K(io_ctx));
    } else if (OB_FAIL(ObTmpPageCache::get_instance().cached_read(page_keys, read_info, *callback_allocator_))) {
      LOG_WARN("fail to cached_read", KR(ret), K(fd_), K(read_info), K(io_ctx), KP(callback_allocator_));
    }
  }
  LOG_DEBUG("inner_cached_read_from_block", KR(ret),  K(block_index), K(file_begin_read_offset),
            K(file_end_read_offset), K(io_read_size), K(user_read_size), K(offset_in_block_buf));
  return ret;
}

int ObSharedNothingTmpFile::write(ObTmpFileIOWriteCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t cur_file_size = 0;
  if (OB_FAIL(write(io_ctx, cur_file_size))) {
    LOG_WARN("fail to write", KR(ret), K(fd_), K(io_ctx));
  }
  return ret;
}

int ObSharedNothingTmpFile::write(ObTmpFileIOWriteCtx &io_ctx, int64_t &cur_file_size)
{
  int ret = OB_SUCCESS;
  cur_file_size = 0;
  LOG_DEBUG("write start", K(fd_), K(io_ctx));
  // TODO: wanyue.wy
  // remove it when support multi-writes, and limit the max number of writes
  ObSpinLockGuard guard(serial_write_lock_);
  int64_t start_write_offset = 0;
  int64_t end_write_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    // this check is just a hint.
    // although is_deleting_ == false, it might be set as true in the processing of write().
    // we will check is_deleting_ again in the following steps
    ret = OB_RESOURCE_RELEASED;
    LOG_WARN("attempt to write a deleting file", KR(ret), K(fd_));
  } else if (OB_FAIL(alloc_write_range_(io_ctx, start_write_offset, end_write_offset))) {
    LOG_WARN("fail to alloc write range", KR(ret), KPC(this), K(io_ctx));
  } else if (OB_UNLIKELY(start_write_offset >= end_write_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid alloc offset", KR(ret), K(start_write_offset), K(end_write_offset), KPC(this));
  } else {
    int64_t batch_start_write_offset = start_write_offset;
    int64_t remain_write_size = io_ctx.get_todo_size();
    while (OB_SUCC(ret) && remain_write_size > 0) {
      int64_t batch_write_size = MIN(ObTmpFileGlobal::TMP_FILE_WRITE_BATCH_SIZE, remain_write_size);
      int64_t batch_end_write_offset = batch_start_write_offset + batch_write_size;
      if (OB_FAIL(inner_batch_write_(io_ctx, batch_start_write_offset, batch_end_write_offset))) {
        LOG_WARN("fail to inner batch write", KR(ret), KPC(this), K(io_ctx));
      } else if (OB_FAIL(io_ctx.update_data_size(batch_write_size))) {
        LOG_WARN("fail to update data size", KR(ret), K(fd_), K(io_ctx));
      } else {
        remain_write_size -= batch_write_size;
        batch_start_write_offset += batch_write_size;
      }
    }
    if (OB_SUCC(ret)) {
      while (file_size_ < start_write_offset) {
        // TODO: wanyue.wy
        // using a signal queue
        PAUSE();
      }
      SpinWLockGuard guard(meta_lock_);
      if (OB_UNLIKELY(file_size_ > start_write_offset)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file size is not expected", KR(ret), KPC(this), K(start_write_offset), K(allocated_file_size_));
      } else if (OB_UNLIKELY(is_deleting_)) {
        ret = OB_RESOURCE_RELEASED;
        LOG_WARN("file is deleting", KR(ret), K(fd_));
      } else {
        bool is_unaligned_write = 0 != file_size_ % ObTmpFileGlobal::PAGE_SIZE ||
                                  0 != io_ctx.get_done_size() % ObTmpFileGlobal::PAGE_SIZE;
        io_ctx.set_is_unaligned_write(is_unaligned_write);
        file_size_ = end_write_offset;
        cur_file_size = file_size_;
        LOG_DEBUG("write success", K(fd_), K(start_write_offset), K(end_write_offset), KPC(this), K(io_ctx));
      }
    }
  }
  return ret;
}

// For files smaller than 1MB, disk space is allocated from shared blocks, and the allocation size
// doubles each time until it reaches a maximum of TMP_FILE_MIN_SHARED_PRE_ALLOC_PAGE_NUM.
// Once this limit is reached, disk space is allocated from exclusive blocks.
int ObSharedNothingTmpFile::alloc_write_range_(const ObTmpFileIOWriteCtx &io_ctx,
                                               int64_t &start_write_offset, int64_t &end_write_offset)
{
  int ret = OB_SUCCESS;
  const int64_t expected_write_size = io_ctx.get_todo_size();
  const int64_t io_timeout_ms = io_ctx.get_io_timeout_ms();
  SpinWLockGuard guard(meta_lock_);
  LOG_DEBUG("alloc write range start", KR(ret), K(file_size_), K(allocated_file_size_), K(pre_allocated_file_size_),
           K(expected_write_size), K(start_write_offset), K(end_write_offset));

  // the following virtual ids both are close interval
  const int64_t expected_last_page_virtual_id = get_page_virtual_id_(allocated_file_size_ + expected_write_size, true);
  int64_t cur_pre_allocated_page_virtual_id = get_page_virtual_id_(pre_allocated_file_size_, true);

  while (OB_SUCC(ret) && cur_pre_allocated_page_virtual_id < expected_last_page_virtual_id) {
    if (pre_allocated_batch_page_num_ == ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM) {
      if (OB_FAIL(alloc_write_range_from_exclusive_block_(io_timeout_ms, cur_pre_allocated_page_virtual_id))) {
        LOG_WARN("fail to alloc write range from exclusive block", KR(ret), K(fd_), K(io_timeout_ms),
                 K(cur_pre_allocated_page_virtual_id), K(pre_allocated_file_size_),
                 K(expected_last_page_virtual_id), K(allocated_file_size_), K(expected_write_size));
      }
    } else {
      if (OB_FAIL(alloc_write_range_from_shared_block_(io_timeout_ms, expected_last_page_virtual_id, cur_pre_allocated_page_virtual_id))) {
        LOG_WARN("fail to alloc write range from shared block", KR(ret), K(fd_), K(io_timeout_ms),
                 K(cur_pre_allocated_page_virtual_id), K(pre_allocated_file_size_),
                 K(expected_last_page_virtual_id), K(allocated_file_size_), K(expected_write_size));
      }
    }
  } // end while

  if (OB_SUCC(ret)) {
    start_write_offset = allocated_file_size_;
    allocated_file_size_ += expected_write_size;
    end_write_offset = allocated_file_size_;
  }
  LOG_DEBUG("alloc write range end", KR(ret), K(file_size_), K(allocated_file_size_), K(pre_allocated_file_size_),
           K(expected_write_size), K(start_write_offset), K(end_write_offset));
  return ret;
}

int ObSharedNothingTmpFile::alloc_write_range_from_exclusive_block_(
    const int64_t timeout_ms,
    int64_t &cur_pre_allocated_page_virtual_id)
{
  int ret = OB_SUCCESS;
  int64_t block_index = ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX;
  if (OB_FAIL(tmp_file_block_manager_->alloc_block(block_index))) {
    LOG_WARN("fail to alloc block", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(block_index == ObTmpFileGlobal::INVALID_TMP_FILE_BLOCK_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block index is invalid", KR(ret), K(fd_), K(block_index));
  } else {
    ObSEArray<ObSharedNothingTmpFileDataItem, 1> arr;
    if (OB_FAIL(arr.push_back(ObSharedNothingTmpFileDataItem(
                    block_index, 0, ObTmpFileGlobal::BLOCK_PAGE_NUMS,
                    cur_pre_allocated_page_virtual_id + 1)))) {
      LOG_WARN("fail to push back", KR(ret), K(fd_), K(block_index), K(cur_pre_allocated_page_virtual_id));
    } else if (OB_FAIL(meta_tree_.insert_items(arr, timeout_ms))) {
      LOG_WARN("fail to insert items", KR(ret), K(fd_), K(arr), K(timeout_ms));
    } else {
      pre_allocated_file_size_ += ObTmpFileGlobal::SN_BLOCK_SIZE;
      cur_pre_allocated_page_virtual_id += ObTmpFileGlobal::BLOCK_PAGE_NUMS;
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(tmp_file_block_manager_->release_page(block_index, 0, ObTmpFileGlobal::BLOCK_PAGE_NUMS))) {
        LOG_WARN("fail to release block", KR(tmp_ret), K(fd_), K(block_index));
      }
    }
  }
  LOG_DEBUG("alloc write range from exclusive block end", KR(ret), K(file_size_),
           K(allocated_file_size_), K(pre_allocated_file_size_),
           K(cur_pre_allocated_page_virtual_id));
  return ret;
}

int ObSharedNothingTmpFile::alloc_write_range_from_shared_block_(const int64_t timeout_ms,
                                                                 const int64_t expected_last_page_virtual_id,
                                                                 int64_t &cur_pre_allocated_page_virtual_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTmpFileBlockRange> alloced_ranges;
  ObArray<ObSharedNothingTmpFileDataItem> inserting_items;
  const int64_t remain_pre_allocated_page_num = expected_last_page_virtual_id - cur_pre_allocated_page_virtual_id;
  const int64_t n = std::ceil(std::log(remain_pre_allocated_page_num) /
                              std::log(ObTmpFileGlobal::TMP_FILE_MIN_SHARED_PRE_ALLOC_PAGE_NUM));
  const int64_t necessary_page_num = MIN(ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM,
                                          MAX(ObTmpFileGlobal::TMP_FILE_MIN_SHARED_PRE_ALLOC_PAGE_NUM << n,
                                              pre_allocated_batch_page_num_));
  const int64_t expected_page_num = MIN(ObTmpFileGlobal::TMP_FILE_MAX_SHARED_PRE_ALLOC_PAGE_NUM, 2 * necessary_page_num);
  // we pre-allocate at least 2 pages (up to 4 pages if there is enough space)
  // for writes smaller than 8KB. In the worst case (when all files are smaller than 1MB),
  // this can result in a 4x write amplification.
  if (OB_FAIL(tmp_file_block_manager_->alloc_page_range(necessary_page_num, expected_page_num, alloced_ranges))) {
    LOG_WARN("fail to alloc page range", KR(ret), K(fd_), K(necessary_page_num), K(expected_page_num));
  } else if (OB_UNLIKELY(alloced_ranges.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloced range is empty", KR(ret), K(fd_), K(alloced_ranges));
  } else if (OB_FAIL(inserting_items.prepare_allocate_and_keep_count(alloced_ranges.count()))) {
    LOG_WARN("fail to prepare allocate and keep count", KR(ret), K(fd_), K(alloced_ranges));
  } else {
    int64_t virtual_page_id = cur_pre_allocated_page_virtual_id + 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < alloced_ranges.count(); i++) {
      ObTmpFileBlockRange &range = alloced_ranges.at(i);
      if (OB_UNLIKELY(!range.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range is not valid", KR(ret), K(fd_), K(range));
      } else if (OB_FAIL(inserting_items.push_back(
                  ObSharedNothingTmpFileDataItem(range.block_index_, range.page_index_,
                                                 range.page_cnt_, virtual_page_id)))) {
        LOG_WARN("fail to push back", KR(ret), K(fd_), K(range), K(virtual_page_id));
      } else {
        virtual_page_id += range.page_cnt_;
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_tree_.insert_items(inserting_items, timeout_ms))) {
      LOG_WARN("fail to insert items", KR(ret), K(fd_), K(inserting_items), K(timeout_ms));
    } else {
      pre_allocated_batch_page_num_ = expected_page_num;
      pre_allocated_file_size_ = virtual_page_id * ObTmpFileGlobal::PAGE_SIZE;
      cur_pre_allocated_page_virtual_id = virtual_page_id - 1;
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = ret;
    for (int64_t i = 0; OB_SUCC(ret) && i < alloced_ranges.count(); i++) {
      ObTmpFileBlockRange &range = alloced_ranges.at(i);
      if (OB_UNLIKELY(!range.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("range is not valid", KR(ret), K(range));
      } else if (OB_FAIL(tmp_file_block_manager_->release_page(range.block_index_, range.page_index_, range.page_cnt_))) {
        LOG_WARN("fail to release page", KR(ret), K(fd_), K(range));
      }
    }
    ret = tmp_ret;
  }
  LOG_DEBUG("alloc write range from shared block end", KR(ret), K(file_size_),
           K(allocated_file_size_), K(pre_allocated_file_size_), K(pre_allocated_batch_page_num_),
           K(expected_last_page_virtual_id), K(cur_pre_allocated_page_virtual_id),
           K(n), K(necessary_page_num), K(expected_page_num));
  return ret;
}

// copy the data of 'write_buf' to the pages of file between [start_write_offset, end_write_offset]
int ObSharedNothingTmpFile::inner_batch_write_(ObTmpFileIOWriteCtx &io_ctx,
                                               const int64_t start_write_offset,
                                               const int64_t end_write_offset)
{
  int ret = OB_SUCCESS;
  const char* write_buf = io_ctx.get_todo_buffer();
  const int64_t batch_write_size = end_write_offset - start_write_offset;
  const int64_t begin_virtual_page_id = get_page_virtual_id_(start_write_offset, false);
  const int64_t end_virtual_page_id = get_page_virtual_id_(end_write_offset, true);
  const int64_t page_cnt = end_virtual_page_id - begin_virtual_page_id + 1;
  ObArray<ObSharedNothingTmpFileDataItem> data_items;
  ObTmpFileDataItemPageIterator page_iterator;
  if (OB_UNLIKELY(is_deleting_)) {
    // this check is just a hint.
    // although is_deleting_ == false, it might be set as true in the processing of write().
    // we will check is_deleting_ again in the following steps
    ret = OB_RESOURCE_RELEASED;
    LOG_WARN("attempt to write a deleting file", KR(ret), K(fd_));
  } else if (OB_UNLIKELY(batch_write_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write offset", KR(ret), K(fd_), K(start_write_offset), K(end_write_offset));
  } else if (OB_FAIL(meta_tree_.search_data_items(start_write_offset, batch_write_size, io_ctx.get_io_timeout_ms(), data_items))) {
    LOG_WARN("fail to search data items", KR(ret), K(fd_), K(start_write_offset), K(batch_write_size), K(io_ctx));
  } else if (OB_FAIL(page_iterator.init(&data_items, begin_virtual_page_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init page iterator", KR(ret), K(fd_), K(begin_virtual_page_id));
  } else {
    ObTmpFileBlockHandle cur_block_handle;
    for (int64_t virtual_page_id = begin_virtual_page_id;
        OB_SUCC(ret) && virtual_page_id <= end_virtual_page_id;
        virtual_page_id++) {
      const int64_t page_begin_write_offset = virtual_page_id == begin_virtual_page_id ?
                                              start_write_offset : get_page_begin_offset_by_virtual_id_(virtual_page_id);
      const int64_t page_end_write_offset = virtual_page_id == end_virtual_page_id ?
                                            end_write_offset : get_page_end_offset_by_virtual_id_(virtual_page_id);
      const int64_t page_write_size = page_end_write_offset - page_begin_write_offset;
      ObTmpFilePageId page_id;
      const bool write_whole_page = page_write_size == ObTmpFileGlobal::PAGE_SIZE;
      if (OB_FAIL(page_iterator.next(page_id.block_index_, page_id.page_index_in_block_))) {
        LOG_WARN("fail to get next page", KR(ret), K(fd_), K(virtual_page_id));
      } else if (!cur_block_handle.is_inited() || cur_block_handle.get()->get_block_index() != page_id.block_index_) {
        cur_block_handle.reset();
        if (OB_FAIL(tmp_file_block_manager_->get_tmp_file_block_handle(page_id.block_index_, cur_block_handle))) {
          LOG_WARN("fail to get tmp file block handle", KR(ret), K(fd_), K(virtual_page_id));
        } else if (OB_UNLIKELY(!cur_block_handle.is_inited())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("block handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(cur_block_handle));
        }
      }

      if (OB_SUCC(ret)) { // alloc and write a page
        const char* data_buf = write_buf + page_begin_write_offset - start_write_offset;
        if (!write_whole_page) {
          if (OB_FAIL(write_partly_page_(io_ctx, virtual_page_id, page_id,
                                         get_offset_in_page_(page_begin_write_offset),
                                         page_write_size, data_buf, cur_block_handle))) {
            LOG_WARN("fail to alloc partly write page", KR(ret), K(fd_), K(virtual_page_id), K(page_id),
                     K(page_begin_write_offset), K(page_write_size), KP(data_buf), K(io_ctx), K(cur_block_handle));
          }
        } else {
          if (OB_FAIL(write_fully_page_(io_ctx, virtual_page_id, page_id,
                                        get_offset_in_page_(page_begin_write_offset),
                                        page_write_size, data_buf, cur_block_handle))) {
            LOG_WARN("fail to alloc fully write page", KR(ret), K(fd_), K(virtual_page_id), K(page_id),
                     K(page_begin_write_offset), K(page_write_size), KP(data_buf), K(io_ctx), K(cur_block_handle));
          }
        }
      }
    } // end for
  }

  LOG_DEBUG("batch write over", KR(ret), K(fd_), K(begin_virtual_page_id), K(end_virtual_page_id),
           K(start_write_offset), K(end_write_offset), K(batch_write_size));
  return ret;
}

int ObSharedNothingTmpFile::write_fully_page_(ObTmpFileIOWriteCtx &io_ctx,
                                              const int64_t virtual_page_id,
                                              const ObTmpFilePageId &page_id,
                                              const int64_t page_offset,
                                              const int64_t write_size,
                                              const char* write_buf,
                                              ObTmpFileBlockHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFilePageHandle page_handle;
  const ObTmpFileWriteCacheKey page_key(PageType::DATA, fd_, virtual_page_id);
  bool trigger_swap_page = false;

  if (OB_FAIL(write_cache_->alloc_page(page_key, page_id, io_ctx.get_io_timeout_ms(), page_handle, trigger_swap_page))) {
    LOG_WARN("fail to alloc page", KR(ret), K(fd_), K(virtual_page_id), K(io_ctx));
  } else if (OB_UNLIKELY(!page_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("page handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(page_handle));
  } else if (trigger_swap_page) {
    io_ctx.add_lack_page_cnt();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(inner_write_page_(page_offset, write_size, write_buf, page_handle, block_handle))) {
    LOG_WARN("fail to write page", KR(ret), K(fd_), K(virtual_page_id), K(page_handle), K(block_handle));
  } else if (OB_FAIL(write_cache_->put_page(page_handle))) {
    LOG_WARN("fail to put page", KR(ret), K(fd_), K(virtual_page_id));
  }
  LOG_DEBUG("alloc fully page", KR(ret), K(fd_), K(virtual_page_id), K(page_id));
  return ret;
}

int ObSharedNothingTmpFile::write_partly_page_(ObTmpFileIOWriteCtx &io_ctx,
                                               const int64_t virtual_page_id,
                                               const ObTmpFilePageId &page_id,
                                               const int64_t page_offset,
                                               const int64_t write_size,
                                               const char* write_buf,
                                               ObTmpFileBlockHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFilePageHandle page_handle;
  ObTmpFilePage *page = nullptr;
  ObTmpFileBlock *block = nullptr;
  if (OB_UNLIKELY(!block_handle.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(block_handle));
  } else if (FALSE_IT(block = block_handle.get())) {
  } else if (OB_FAIL(write_cache_->shared_lock(fd_))) {
    LOG_WARN("fail to shared lock", KR(ret), K(fd_));
  } else {
    if (OB_FAIL(alloc_partly_write_page_(io_ctx, virtual_page_id, page_id, block_handle, page_handle))) {
      LOG_WARN("fail to alloc partly write page", KR(ret), K(fd_), K(virtual_page_id), K(page_id), K(block_handle));
    } else if (OB_UNLIKELY(!page_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(page_handle));
    } else if (FALSE_IT(page = page_handle.get_page())) {
    } else if (OB_FAIL(inner_write_page_(page_offset, write_size, write_buf, page_handle, block_handle))) {
      LOG_WARN("fail to write page", KR(ret), K(fd_), K(virtual_page_id), KPC(page), KPC(block));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(write_cache_->unlock(fd_))) {
      LOG_ERROR("fail to unlock", KR(tmp_ret), K(fd_), KPC(this));
    }
  }

  return ret;
}

int ObSharedNothingTmpFile::alloc_partly_write_page_(ObTmpFileIOWriteCtx &io_ctx,
                                                     const int64_t virtual_page_id,
                                                     const ObTmpFilePageId &page_id,
                                                     const ObTmpFileBlockHandle &block_handle,
                                                     ObTmpFilePageHandle &page_handle)
{
  int ret = OB_SUCCESS;
  const ObTmpFileWriteCacheKey page_key(PageType::DATA, fd_, virtual_page_id);
  const int64_t timeout_ms = io_ctx.get_io_timeout_ms();
  bool need_alloc_page = false;
  {
    ObSpinLockGuard guard(alloc_incomplete_page_lock_);
    if (OB_UNLIKELY(!block_handle.is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(block_handle));
    } else if (OB_FAIL(write_cache_->get_page(page_key, page_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_alloc_page = true;
      } else {
        LOG_WARN("fail to get page", KR(ret), K(fd_), K(virtual_page_id));
      }
    } else if (OB_UNLIKELY(!page_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(page_handle));
    }

    if (OB_FAIL(ret)) {
    } else if (need_alloc_page) {
      // the page is not in write cache, two case will be here:
      // 1. the page has not ever been allocated
      // 2. the page has been flushed
      bool is_flushed = false;
      const ObTmpFileBlock *block = block_handle.get();
      blocksstable::MacroBlockId macro_block_id;
      bool trigger_swap_page = false;
      if (OB_FAIL(write_cache_->alloc_page(page_key, page_id, timeout_ms, page_handle, trigger_swap_page))) {
        LOG_WARN("fail to alloc page", KR(ret), K(fd_), K(page_key), K(page_id));
      } else if (OB_UNLIKELY(!page_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page handle is not valid", KR(ret), K(fd_), K(virtual_page_id), K(page_handle));
      } else if (trigger_swap_page) {
        io_ctx.add_lack_page_cnt();
      }
      if (FAILEDx(write_cache_->put_page(page_handle))) {
        LOG_WARN("fail to put page", KR(ret), K(fd_), K(virtual_page_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!need_alloc_page) {
    // the page is in write cache
    while (page_handle.get_page()->is_loading()) {
      // TODO: wanyue.wy
      // using a signal or sleep() here
      PAUSE();
    }
    LOG_DEBUG("page has been allocated, wait for loading", K(fd_), K(virtual_page_id), K(page_key));
  } else {
    // the page is not in write cache, two case will be here:
    // 1. the page has not ever been allocated
    // 2. the page has been flushed
    bool is_flushed = false;
    const ObTmpFileBlock *block = block_handle.get();
    if (OB_FAIL(block->is_page_flushed(page_id, is_flushed))) {
      LOG_WARN("fail to check page is flushed", KR(ret), K(fd_), K(virtual_page_id), K(page_id));
    } else if (is_flushed) {
      blocksstable::MacroBlockId macro_block_id;
      if (OB_FAIL(tmp_file_block_manager_->get_macro_block_id(page_id.block_index_, macro_block_id))) {
        LOG_WARN("fail to get macro block id", KR(ret), K(fd_), K(virtual_page_id), K(page_id));
      } else if (OB_UNLIKELY(!macro_block_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro block id is not valid", KR(ret), K(fd_), K(virtual_page_id), K(page_id), K(macro_block_id));
      } else if (OB_FAIL(write_cache_->load_page(page_handle, macro_block_id, timeout_ms))) {
        LOG_WARN("fail to load page", KR(ret), K(fd_), K(virtual_page_id), K(macro_block_id));
      } else {
        io_ctx.add_write_persisted_tail_page_cnt();
      }
      LOG_DEBUG("alloc partly page for loading", KR(ret), K(fd_), K(is_flushed), K(virtual_page_id), K(page_id), K(page_key), KPC(block));
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(write_cache_->free_page(page_key))) {
        LOG_ERROR("fail to free page", KR(tmp_ret), K(fd_), K(virtual_page_id));
      }
    }
  }
  LOG_DEBUG("alloc partly page", KR(ret), K(fd_), K(virtual_page_id), K(page_id));

  return ret;
}

int ObSharedNothingTmpFile::inner_write_page_(const int64_t page_offset,
                                              const int64_t write_size,
                                              const char* write_buf,
                                              ObTmpFilePageHandle &page_handle,
                                              ObTmpFileBlockHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObTmpFilePage *page = page_handle.get_page();
  ObTmpFileBlock *block = block_handle.get();
  if (OB_ISNULL(page) || OB_ISNULL(block)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(fd_), KP(page), KP(block));
  } else {
    MEMCPY(page->get_buffer() + page_offset, write_buf, write_size);
    if ((page_offset + write_size) % ObTmpFileGlobal::PAGE_SIZE == 0) {
      page->set_is_full(true);
    }
    if (OB_FAIL(block->insert_page_into_flushing_list(page_handle))) {
      LOG_WARN("fail to insert page into flushing list", KR(ret), K(fd_), KPC(page), KPC(block));
    }
  }
  return ret;
}
// Attention!!
// 1. if truncate_offset is not the begin or end offset of page, we will do nothing for this page
// 2. truncate_offset is a open interval number, which means the offset before than it need to be truncated
int ObSharedNothingTmpFile::truncate(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("truncate start", K(fd_));
  SpinWLockGuard guard(meta_lock_);
  LOG_INFO("start to truncate a temporary file", KR(ret), K(fd_), K(truncate_offset), KPC(this));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to truncate a deleting file", KR(ret), K(tenant_id_), K(fd_), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= 0 || truncate_offset > file_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid truncate_offset", KR(ret), K(fd_), K(truncate_offset), K(file_size_), KPC(this));
  } else if (OB_UNLIKELY(truncate_offset <= truncated_offset_)) {
    // do nothing
  } else if (OB_FAIL(truncate_write_cache_(truncate_offset))) {
    LOG_WARN("fail to truncate write cache", KR(ret), K(fd_), K(truncate_offset), KPC(this));
  } else if (OB_FAIL(meta_tree_.truncate(truncated_offset_, truncate_offset))) {
    LOG_WARN("fail to truncate meta tree", KR(ret), K(fd_), K(truncate_offset), KPC(this));
  } else {
    truncated_offset_ = truncate_offset;
    write_info_.last_modify_ts_ = ObTimeUtility::current_time();
  }

  LOG_INFO("truncate over", KR(ret), K(truncate_offset), KPC(this));
  return ret;
}

int ObSharedNothingTmpFile::truncate_write_cache_(const int64_t truncate_offset)
{
  int ret = OB_SUCCESS;
  const int64_t begin_truncate_virtual_page_id = get_page_virtual_id_(truncated_offset_, false);
  const int64_t end_truncate_virtual_page_id = get_page_virtual_id_(truncate_offset, true);

  for (int64_t virtual_page_id = begin_truncate_virtual_page_id;
       OB_SUCC(ret) && virtual_page_id <= end_truncate_virtual_page_id;
       virtual_page_id++) {
    if (virtual_page_id != end_truncate_virtual_page_id || truncate_offset % ObTmpFileGlobal::PAGE_SIZE == 0) {
      ObTmpFileWriteCacheKey key(PageType::DATA, fd_, virtual_page_id);
      if (OB_FAIL(write_cache_->free_page(key))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to free page", KR(ret), K(fd_), K(virtual_page_id));
        }
      }
    }
  }
  return ret;
}

void ObSharedNothingTmpFile::inner_set_read_stats_vars_(const ObTmpFileIOReadCtx &ctx, const int64_t read_size)
{
  read_info_.record_read_stat(ctx.is_unaligned_read(), read_size,
                              ctx.get_total_truncated_page_read_cnt(),
                              ctx.get_total_kv_cache_page_read_cnt(),
                              ctx.get_total_uncached_page_read_cnt(),
                              ctx.get_total_wbp_page_read_cnt(),
                              ctx.get_truncated_page_read_hits(),
                              ctx.get_kv_cache_page_read_hits(),
                              ctx.get_uncached_page_read_hits(),
                              ctx.get_aggregate_read_io_cnt(),
                              ctx.get_wbp_page_read_hits());
}

void ObSharedNothingTmpFile::inner_set_write_stats_vars_(const ObTmpFileIOWriteCtx &ctx)
{
  write_info_.record_write_stat(ctx.is_unaligned_write(),
                                ctx.get_write_persisted_tail_page_cnt(),
                                ctx.get_lack_page_cnt());
}

int ObSharedNothingTmpFile::copy_info_for_virtual_table(ObTmpFileBaseInfo &tmp_file_info)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = common::ObTimeUtility::current_time() + 100 * 1000L;
  if (OB_FAIL(meta_lock_.rdlock(abs_timeout_us))) {
    LOG_WARN("fail to get lock for reading in virtual table", KR(ret), KPC(this));
  } else {
    {
      ObSpinLockGuard guard(stat_lock_);
      ObTmpFileMetaTreeInfo tree_info;
      ObSNTmpFileInfo &sn_tmp_file_info = static_cast<ObSNTmpFileInfo&>(tmp_file_info);
      if (OB_FAIL(meta_tree_.copy_info(tree_info))) {
          LOG_WARN("fail to copy tree info", KR(ret), KPC(this));
        } else if (OB_FAIL(sn_tmp_file_info.init(trace_id_, tenant_id_, dir_id_, fd_,
                                                 file_size_, truncated_offset_, is_deleting_, ref_cnt_,
                                                 birth_ts_, this, label_.ptr(),
                                                 file_type_, compressible_fd_,
                                                 tree_info, write_info_, read_info_))) {
        LOG_WARN("fail to init tmp_file_info", KR(ret), KPC(this));
      }
    }
    meta_lock_.rdunlock();
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
