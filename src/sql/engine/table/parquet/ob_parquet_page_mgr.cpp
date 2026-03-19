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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/parquet/ob_parquet_page_mgr.h"

#include "lib/container/ob_fixed_array_iterator.h"
#include "lib/container/ob_vector.h"
#include "sql/engine/table/ob_external_data_page_cache.h"
#include "sql/engine/table/ob_external_file_access.h"

namespace oceanbase
{
namespace sql
{

ObParquetPageMgr::ObParquetPageMgr() : data_page_cache_(ObExternalDataPageCache::get_instance())
{
  pages_.set_allocator(&allocator_);
  page_buffers_.set_allocator(&allocator_);
  allocator_.set_label("ParquetPageMgr");
}

ObParquetPageMgr::~ObParquetPageMgr()
{
  int ret = OB_SUCCESS;
  OZ(reset());
}

int ObParquetPageMgr::init(int64_t hole_size_limit,
                           int64_t range_size_limit,
                           bool enable_disk_cache,
                           int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  hole_size_limit_ = hole_size_limit;
  // range_size_limit_ = range_size_limit;
  range_size_limit_ = 2 * 1024 * 1024; // 目前测试下来 2MB 效果最好
  prefetch_size_ = 8 * range_size_limit_;
  enable_disk_cache_ = enable_disk_cache;
  timeout_ts_ = timeout_ts;
  return ret;
}

// open new file
int ObParquetPageMgr::open(const ObExternalFileUrlInfo &file_url_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset())) {
    LOG_WARN("failed to reset", K(ret));
  } else {
    // 关闭 ObExternalFileAccess 自己的 mem cache，由我们自己在外面 cache page
    ObExternalFileCacheOptions file_cache_options(false, enable_disk_cache_);
    file_path_ = file_url_info.get_url();
    file_content_digest_ = file_url_info.get_content_digest();
    file_size_ = file_url_info.get_file_size();
    file_modify_time_ = file_url_info.get_modify_time();
    OZ(file_reader_.open(file_url_info, file_cache_options));
  }

  return ret;
}

// reset before open new file
int ObParquetPageMgr::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(clear_all_pages())) {
    LOG_WARN("failed to clear all pages", K(ret));
  } else if (OB_FAIL(file_reader_.reset())) {
    LOG_WARN("failed to reset file reader", K(ret));
  } else {
    file_path_.reset();
    file_content_digest_.reset();
    file_size_ = -1;
    file_modify_time_ = -1;
  }
  return ret;
}

// clear_all_pages() before each row group start
int ObParquetPageMgr::clear_all_pages()
{
  int ret = OB_SUCCESS;
  // release each page
  for (int64_t i = 0; OB_SUCC(ret) && i < pages_.count(); i++) {
    if (OB_FAIL(release_page_by_idx(i, std::nullopt))) {
      LOG_WARN("failed to release page by idx", K(i));
    }
  }
  pages_.reset();
  page_buffers_.reset();
  allocator_.reset();
  return ret;
}

int ObParquetPageMgr::init_with_new_row_group(
    const ObIArray<std::tuple<int64_t, int64_t, ObParquetPageType>> &selected_pages)
{
  int ret = OB_SUCCESS;
  // 清除上一个 row group hold 的 pages
  if (OB_FAIL(clear_all_pages())) {
    LOG_WARN("failed to clear all pages", K(ret));
  } else if (selected_pages.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, empty pages", K(selected_pages.count()));
  }

  // 按照 Page ReadRange 的 offset 从小到大排序
  ObSortedVector<ObParquetPage> sorted_vector;
  for (int64_t i = 0; OB_SUCC(ret) && i < selected_pages.count(); i++) {
    const std::tuple<int64_t, int64_t, ObParquetPageType> &page = selected_pages.at(i);
    if (OB_FAIL(sorted_vector.push_back(
            {std::get<0>(page), std::get<1>(page), std::get<2>(page), ObParquetPageStatus::UNLOADED}))) {
      LOG_WARN("failed to push back page range");
    }
  }

  if (OB_SUCC(ret)) {
    sorted_vector.sort(ObParquetPage::compare);
  }

  // 检查 PageRange 是否存在重叠，确保所有的 Page ReadRange 都是合法的。
  if (OB_SUCC(ret)) {
    // check has overlap
    int64_t previous_end = sorted_vector.at(0).end();
    for (int64_t i = 1; OB_SUCC(ret) && i < sorted_vector.count(); i++) {
      ObParquetPage &current_page = sorted_vector.at(i);
      if (current_page.offset_ < previous_end) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("overlap read range", K(current_page), K(previous_end));
      } else {
        previous_end = current_page.end();
      }
    }
  }

  // assign page_ranges_ & page_buffers_ & page_status_
  if (OB_SUCC(ret)) {
    const int64_t total_page_count = sorted_vector.count();
    if (OB_FAIL(pages_.reserve(total_page_count))) {
      LOG_WARN("failed to reserve page ranges", K(total_page_count));
    } else if (OB_FAIL(page_buffers_.reserve(total_page_count))) {
      LOG_WARN("failed to reserve page buffers", K(total_page_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < total_page_count; i++) {
        if (OB_FAIL(pages_.push_back(sorted_vector.at(i)))) {
          LOG_WARN("failed to push back page range");
        } else if (OB_FAIL(page_buffers_.push_back(NULL))) {
          LOG_WARN("failed to push back page buffer");
        }
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::cache_page_by_offset(int64_t page_offset,
                                           bool is_decompressed,
                                           const std::shared_ptr<arrow::Buffer> &page_buffer)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to get page idx", K(page_offset), K(page_idx));
  } else if (OB_FAIL(cache_page_by_idx(page_idx, is_decompressed, page_buffer))) {
    LOG_WARN("failed to cache page by idx", K(page_idx));
  }
  return ret;
}

int ObParquetPageMgr::cache_page_by_idx(int64_t page_idx,
                                        bool is_decompressed,
                                        const std::shared_ptr<arrow::Buffer> &page_buffer)
{
  int ret = OB_SUCCESS;
  const ObParquetPage &page = pages_.at(page_idx);
  const ObExternalDataPageCacheKey &cache_key = create_page_cache_key_(page.offset_);
  const ObExternalDataPageCacheValue cache_value(const_cast<char *>(page_buffer->data_as<char>()),
                                                 page_buffer->size(),
                                                 is_decompressed);
  data_page_cache_.try_put_page_to_cache(cache_key, cache_value);
  return ret;
}

int ObParquetPageMgr::release_page_by_offset(int64_t page_offset,
                                             std::optional<bool> is_eager_access)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset), K(page_idx));
  } else if (OB_FAIL(release_page_by_idx(page_idx, is_eager_access))) {
    LOG_WARN("failed to release page idx", K(page_idx));
  }
  return ret;
}

// 如果 is_eager_access 没有被设置，则无条件释放指定 page
int ObParquetPageMgr::release_page_by_idx(int64_t page_idx, std::optional<bool> is_eager_access)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid page idx", K(page_idx));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::RELEASED) {
    // do nothing, already released
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
    // do nothing, page is unloaded
    if (OB_UNLIKELY(NULL != page_buffers_[page_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page is unloaded, but page buffer existed, illegal state", K(page_idx));
    }
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::LOADED) {
    bool need_release_page = false;
    ObParquetPageBufferBase *page_buffer = page_buffers_[page_idx];
    if (OB_FAIL(decide_need_to_release_page_(pages_[page_idx], is_eager_access, need_release_page))) {
      LOG_WARN("failed to decide_need_to_release_page", K(page_idx));
    } else if (!need_release_page) {
      // do nothing, do not need to release page
    } else if (OB_ISNULL(page_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page is loaded but page buffer is NULL, illegal state", K(page_idx));
    } else {
      // release buffer(call derived class' de-constructor)
      ObParquetPageBufferSlice *buffer_slice = NULL;
      ObParquetCachedPageBuffer *cached_buffer = NULL;
      if ((buffer_slice = dynamic_cast<ObParquetPageBufferSlice *>(page_buffer)) != nullptr) {
        // 确保 IO 完成后，才能 release 内存
        if (OB_FAIL(buffer_slice->coalesced_buffer_->wait(metrics_))) {
          LOG_WARN("failed to wait", K(page_idx));
        } else {
          ObCoalescedBuffer *coalesced_buffer = buffer_slice->coalesced_buffer_;
          buffer_slice->~ObParquetPageBufferSlice();
          allocator_.free(buffer_slice); // 目前此处 allocator 不会真的放内存
          if (coalesced_buffer->ref_counts_ <= 0) {
            // 当 CoalescedBuffer 上面的引用计数清零，释放其内存
            OB_DELETEx(ObCoalescedBuffer, &allocator_, coalesced_buffer);
          }
        }
      } else if ((cached_buffer = dynamic_cast<ObParquetCachedPageBuffer *>(page_buffer))
                 != nullptr) {
        cached_buffer->~ObParquetCachedPageBuffer();
        allocator_.free(cached_buffer);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unreachable code");
      }
    }
    if (need_release_page) {
      pages_[page_idx].status_ = ObParquetPageStatus::RELEASED;
      page_buffers_[page_idx] = NULL;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unreachable code");
  }
  return ret;
}

int ObParquetPageMgr::read_page(int64_t page_offset,
                                std::shared_ptr<arrow::Buffer> &page_buffer,
                                bool &has_mem_cached,
                                bool &has_decompressed)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::RELEASED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read released page buffer", K(page_offset));
  } else if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
    // start to fetch
    if (OB_FAIL(try_to_prefetch_by_idx_(page_idx))) {
      LOG_WARN("try to prefetch page failed", K(page_offset), K(page_idx));
    }
  }

  if (OB_SUCC(ret)) {
    ObParquetPageBufferBase *loaded_page_buffer = page_buffers_[page_idx];
    if (OB_UNLIKELY(pages_[page_idx].status_ != ObParquetPageStatus::LOADED)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page not loaded", K(page_offset), K(page_idx));
    } else if (OB_ISNULL(loaded_page_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page not loaded", K(page_offset), K(page_idx));
    } else if (OB_FAIL(loaded_page_buffer->get_arrow_buffer(metrics_,
                                                            page_buffer,
                                                            has_mem_cached,
                                                            has_decompressed))) {
      LOG_WARN("failed to get arrow buffer", K(page_offset), K(page_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_to_prefetch_by_idx_(page_idx + 1))) {
      LOG_WARN("failed to prefetch next page", K(page_idx + 1));
    }
  }

  return ret;
}

// 检查当前 page_offset 是否已经被 selected 了，如果当前 page_offset 经过 page-index 已经被过滤了，
// 直接返回下一个被 selected 的 page start offset
int ObParquetPageMgr::check_page_selected(int64_t page_offset,
                                          bool &is_selected,
                                          std::optional<int64_t> &next_page_offset) const
{
  int ret = OB_SUCCESS;
  const ObFixedArray<ObParquetPage, ObIAllocator>::const_iterator iter
      = std::lower_bound(pages_.begin(), pages_.end(), page_offset, ObParquetPageCmp());
  if (iter == pages_.end()) {
    is_selected = false;
    // page 已经在末尾了
    next_page_offset = std::nullopt;
  } else if (page_offset != iter->offset_) {
    // 当前访问的 page 没有被 selected，返回下一个 page 的 offset
    is_selected = false;
    next_page_offset = iter->offset_;
  } else {
    is_selected = true;
    next_page_offset = std::nullopt;
  }
  return ret;
}

int ObParquetPageMgr::register_metrics(ObLakeTableReaderProfile &reader_profile,
                                       const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reader_profile.register_metrics(&metrics_, label))) {
    LOG_WARN("failed to register metrics", K(ret));
  }
  return ret;
}

int ObParquetPageMgr::register_io_metrics(ObLakeTableReaderProfile &reader_profile,
                                          const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.register_io_metrics(reader_profile, label))) {
    LOG_WARN("failed to register io metrics", K(ret));
  }
  return ret;
}

int ObParquetPageMgr::find_page_idx_(int64_t offset, int64_t &page_idx) const
{
  int ret = OB_SUCCESS;
  page_idx = -1;
  const ObFixedArray<ObParquetPage, ObIAllocator>::const_iterator iter
      = std::lower_bound(pages_.begin(), pages_.end(), offset, ObParquetPageCmp());
  if (iter == pages_.end()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("page not found", K(offset));
  } else if (iter->offset_ != offset) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("page offset not matched", K(offset), K(iter->offset_));
  } else {
    page_idx = static_cast<int64_t>(std::distance(pages_.begin(), iter));
  }

  if (OB_SUCC(ret)) {
    if (page_idx < 0 || page_idx >= pages_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("page index out of range", K(page_idx));
    }
  }
  return ret;
}

int ObParquetPageMgr::try_to_prefetch_by_offset(int64_t page_offset)
{
  int ret = OB_SUCCESS;
  int64_t page_idx = -1;
  if (OB_FAIL(find_page_idx_(page_offset, page_idx))) {
    LOG_WARN("failed to find page idx", K(page_offset));
  } else if (OB_FAIL(try_to_prefetch_by_idx_(page_idx))) {
    LOG_WARN("failed to prefetch page", K(page_offset), K(page_idx));
  }
  return ret;
}

int ObParquetPageMgr::try_to_prefetch_by_idx_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    // do nothing
  } else {
    // 从当前 page_idx 开始，后面 range_size_limit_ * 2 数据都被 loaded，对标原有的 Prebuffer
    // 实现，预取后面两个 buffer
    int64_t start_offset = pages_[page_idx].offset_;
    int64_t end_offset = start_offset + prefetch_size_;
    for (int64_t i = page_idx; OB_SUCC(ret) && i < pages_.count() && pages_[i].end() <= end_offset;
         ++i) {
      if (pages_[i].status_ == ObParquetPageStatus::UNLOADED) {
        if (OB_FAIL(trigger_prefetch_(i))) {
          LOG_WARN("failed to prefetch page", K(pages_[i]), K(i));
        }
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::trigger_prefetch_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> page_indices_in_window;
  ObArray<int64_t> unloaded_page_indices_in_window;
  ObArray<CoalescedReadRange> coalesced_read_range;
  if (OB_UNLIKELY(page_idx < 0 || page_idx >= pages_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("page index out of range", K(page_idx), K(pages_.count()));
  } else if (OB_FAIL(get_page_indices_in_window_(page_idx, prefetch_size_, page_indices_in_window))) {
    // 获取当前 page 后面 2 * range_size_limit_ 的 page index
    LOG_WARN("failed to get page indices", K(page_idx));
  } else if (OB_FAIL(try_pin_pages_from_mem_cache_(page_indices_in_window))) {
    // 把后面 2 * range_size_limit_ 的 page pin 住
    LOG_WARN("failed to load pages from mem cache", K(page_idx));
  } else if (OB_FAIL(collect_unloaded_pages_(page_indices_in_window,
                                             unloaded_page_indices_in_window))) {
    // 收集仍然 unloaded 的 page，从远端加载
    LOG_WARN("failed to collect unloaded pages",
             K(page_indices_in_window),
             K(unloaded_page_indices_in_window));
  } else if (OB_FAIL(coalesce_pages_(unloaded_page_indices_in_window,
                                     hole_size_limit_,
                                     range_size_limit_,
                                     coalesced_read_range))) {
    LOG_WARN("failed to coalesce page ranges", K(page_idx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < coalesced_read_range.count(); i++) {
      const CoalescedReadRange &read_range = coalesced_read_range[i];
      // load coalesced_read_ranges
      if (OB_FAIL(load_pages_from_storage_(read_range))) {
        LOG_WARN("failed to load coalesced read ranges");
      }
    }
  }
  return ret;
}

// 获取一个 windows size 下面所有 page 的 index
int ObParquetPageMgr::get_page_indices_in_window_(int64_t start_page_idx,
                                                  int64_t window_size,
                                                  ObIArray<int64_t> &result) const
{
  int ret = OB_SUCCESS;
  int64_t start_offset = pages_[start_page_idx].start();
  bool is_reached_window_size = false;
  if (OB_FAIL(result.push_back(start_page_idx))) {
    LOG_WARN("failed to push back", K(start_page_idx));
  }
  for (int64_t page_idx = start_page_idx + 1;
       OB_SUCC(ret) && page_idx < pages_.count() && !is_reached_window_size;
       page_idx++) {
    const ObParquetPage &current_page = pages_[page_idx];
    if (current_page.end() - start_offset <= window_size) {
      if (OB_FAIL(result.push_back(page_idx))) {
        LOG_WARN("failed to push back", K(page_idx));
      }
    } else {
      is_reached_window_size = true;
    }
  }
  return ret;
}

int ObParquetPageMgr::collect_unloaded_pages_(const ObIArray<int64_t> &page_indices,
                                              ObIArray<int64_t> &unloaded_page_indices) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < page_indices.count(); i++) {
    int64_t page_idx = page_indices.at(i);
    if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
      OZ(unloaded_page_indices.push_back(page_idx));
    }
  }
  return ret;
}

int ObParquetPageMgr::try_pin_pages_from_mem_cache_(const ObIArray<int64_t> &page_indices)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < page_indices.count(); i++) {
    int64_t page_idx = page_indices.at(i);
    if (pages_[page_idx].status_ == ObParquetPageStatus::UNLOADED) {
      // try to load page from mem cache
      if (OB_FAIL(try_pin_page_from_mem_cache_(page_idx))) {
        LOG_WARN("failed to load page from mem cache", K(page_idx));
      }
    }
  }
  return ret;
}

int ObParquetPageMgr::try_pin_page_from_mem_cache_(int64_t page_idx)
{
  int ret = OB_SUCCESS;
  const ObParquetPage &page = pages_.at(page_idx);
  const ObExternalDataPageCacheKey &cache_key = create_page_cache_key_(page.offset_);

  ObExternalDataPageCacheValueHandle value_handle;
  if (OB_FAIL(data_page_cache_.get_page(cache_key, value_handle))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      metrics_.cached_page_miss_count_++;
      metrics_.cached_page_miss_size_ += page.length_;
      // cache missed, ignore it
      ret = OB_SUCCESS;
    }
  } else {
    // cache hit
    metrics_.cached_page_hit_count_++;
    metrics_.cached_page_hit_size_ += page.length_;
    ObParquetCachedPageBuffer *cached_page_buffer = NULL;
    if (OB_ISNULL(cached_page_buffer = OB_NEWx(ObParquetCachedPageBuffer, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory");
    } else if (OB_FAIL(cached_page_buffer->cache_handle_.assign(value_handle))) {
      LOG_WARN("failed to assign cache handle");
    } else {
      page_buffers_[page_idx] = cached_page_buffer;
      pages_[page_idx].status_ = ObParquetPageStatus::LOADED;
    }
  }
  return ret;
}

int ObParquetPageMgr::load_pages_from_storage_(const CoalescedReadRange &coalesced_read_range)
{
  int ret = OB_SUCCESS;
  ObCoalescedBuffer *coalesced_buffer = OB_NEWx(ObCoalescedBuffer,
                                                &allocator_,
                                                file_size_,
                                                coalesced_read_range.offset_,
                                                coalesced_read_range.length_);
  if (OB_ISNULL(coalesced_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else if (OB_FAIL(coalesced_buffer->alloc_buffer())) {
    LOG_WARN("failed to allocate memory for coalesced buffer");
  } else {
    const int64_t io_timeout_ms = MAX(0, (timeout_ts_ - ObTimeUtility::current_time()) / 1000);
    ObExternalReadInfo read_info(coalesced_buffer->offset_,
                                 coalesced_buffer->buf_,
                                 coalesced_buffer->length_,
                                 io_timeout_ms);
    if (OB_FAIL(file_reader_.async_read(read_info, coalesced_buffer->io_handle_))) {
      LOG_WARN("fail to async read file");
    } else {
      metrics_.async_io_count_++;
      metrics_.async_io_size_ += coalesced_buffer->length_;
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < coalesced_read_range.coalesced_page_indices_.count();
         i++) {
      int64_t page_idx = coalesced_read_range.coalesced_page_indices_.at(i);
      const ObParquetPage &page_read_range = pages_[page_idx];
      ObParquetPageBufferSlice *page_slice = NULL;
      if (OB_UNLIKELY(page_read_range.offset_ < coalesced_buffer->offset_
                      || page_read_range.end() > coalesced_buffer->end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page index out of range", K(page_read_range.offset_), K(page_read_range.length_));
      } else if (OB_ISNULL(page_slice
                           = OB_NEWx(ObParquetPageBufferSlice,
                                     &allocator_,
                                     coalesced_buffer,
                                     page_read_range.offset_ - coalesced_buffer->offset_,
                                     page_read_range.length_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for coalesced page slice");
      } else {
        pages_[page_idx].status_ = ObParquetPageStatus::LOADED;
        page_buffers_[page_idx] = page_slice;
        coalesced_buffer->ref_counts_++;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(coalesced_buffer)) {
      // 需要 wait() 才能释放，因为底下会拆 IO，可能部分 IO 成功，部分 IO 失败。
      // 这时候你需要 wait 成功的 IO 完成，才能释放 buffer
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(coalesced_buffer->wait(metrics_))) {
        LOG_WARN("failed to io wait");
      }
      OB_DELETEx(ObCoalescedBuffer,
                 &allocator_,
                 coalesced_buffer); // 显示调用其析构函数，释放 buffer 内存
    }
  }

  return ret;
}

int ObParquetPageMgr::coalesce_pages_(const ObIArray<int64_t> &unloaded_page_indices,
                                      const int64_t hold_size_limit,
                                      const int64_t range_size_limit,
                                      ObIArray<CoalescedReadRange> &result) const
{
  int ret = OB_SUCCESS;
  if (unloaded_page_indices.count() > 0) {
    CoalescedReadRange current_range;
    int64_t first_page_idx = unloaded_page_indices.at(0);

    // 初始化第一个合并范围
    current_range.offset_ = pages_[first_page_idx].offset_;
    current_range.length_ = pages_[first_page_idx].length_;
    if (OB_FAIL(current_range.coalesced_page_indices_.push_back(first_page_idx))) {
      LOG_WARN("failed to push back page index", K(first_page_idx));
    }

    // 遍历剩余的未加载页面索引
    for (int64_t i = 1; OB_SUCC(ret) && i < unloaded_page_indices.count(); i++) {
      int64_t page_idx = unloaded_page_indices.at(i);
      const ObParquetPage &page_range = pages_[page_idx];

      int64_t current_range_end = current_range.end();
      int64_t hole_size = page_range.offset_ - current_range_end;
      int64_t new_range_size = page_range.end() - current_range.offset_;

      // 判断是否应该合并当前页面到现有范围
      // 条件：1. 间隙大小不超过限制
      //      2. 合并后的总大小不超过范围大小限制
      if (hole_size <= hold_size_limit && new_range_size <= range_size_limit) {
        // 合并到当前范围
        current_range.length_ = new_range_size;
        if (OB_FAIL(current_range.coalesced_page_indices_.push_back(page_idx))) {
          LOG_WARN("failed to push back page index", K(page_idx));
        }
      } else {
        // 不能合并，保存当前范围并开始新范围
        if (OB_FAIL(result.push_back(current_range))) {
          LOG_WARN("failed to push back coalesced range",
                   K(current_range.offset_),
                   K(current_range.length_));
        } else {
          // 重置并初始化新范围
          current_range.offset_ = page_range.offset_;
          current_range.length_ = page_range.length_;
          current_range.coalesced_page_indices_.reuse();
          if (OB_FAIL(current_range.coalesced_page_indices_.push_back(page_idx))) {
            LOG_WARN("failed to push back page index", K(page_idx));
          }
        }
      }
    }

    // 保存最后一个范围
    if (OB_SUCC(ret) && current_range.coalesced_page_indices_.count() > 0) {
      if (OB_FAIL(result.push_back(current_range))) {
        LOG_WARN("failed to push back last coalesced range",
                 K(current_range.offset_),
                 K(current_range.length_));
      }
    }
  }

  return ret;
}

// 如果 is_eager_access 没有被设置，则无条件释放指定 page
int ObParquetPageMgr::decide_need_to_release_page_(const ObParquetPage &page,
                                                   std::optional<bool> is_eager_access,
                                                   bool &need_release_page) const
{
  int ret = OB_SUCCESS;
  if (!is_eager_access.has_value()) {
    need_release_page = true;
  } else {
    const bool eager_access = is_eager_access.value();
    switch (page.type_) {
      case ObParquetPageType::EAGER: {
        if (eager_access) {
          need_release_page = true;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected", K(eager_access), K(page));
        }
        break;
      }
      case ObParquetPageType::PROJECT: {
        if (eager_access) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected", K(eager_access), K(page));
        } else {
          need_release_page = true;
        }
        break;
      }
      case ObParquetPageType::PROJECT_EAGER: {
        if (eager_access) {
          need_release_page = false;
        } else {
          need_release_page = true;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unreachable code");
      }
    }
  }
  return ret;
}

ObExternalDataPageCacheKey ObParquetPageMgr::create_page_cache_key_(int64_t page_offset) const
{
  return ObExternalDataPageCacheKey(
      file_path_.ptr(),
      file_path_.length(),
      file_content_digest_.ptr(),
      file_content_digest_.size(),
      file_modify_time_,
      // page_size 这里传和 page_offset 一样的值，用于通过 cache 校验，此参数这里没用
      page_offset,
      page_offset,
      MTL_ID());
}

} // namespace sql
} // namespace oceanbase
