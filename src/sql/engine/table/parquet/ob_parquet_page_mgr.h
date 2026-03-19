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

#ifndef OCEANBASE_OB_PARQUET_PAGE_MGR_H
#define OCEANBASE_OB_PARQUET_PAGE_MGR_H

#include "share/ob_define.h"
#include "sql/engine/table/parquet/ob_parquet_page_buffer.h"

namespace oceanbase
{
namespace sql
{

class ObExternalFileAccess;
class ObExternalDataPageCache;
class CacheOptions;

// 标记该 Page 是 project 还是 eager 列的 page，还是既是 project 也是 eager。
enum class ObParquetPageType
{
  PROJECT = 0,
  EAGER,
  PROJECT_EAGER,
};

enum class ObParquetPageStatus
{
  UNLOADED = 0,
  LOADED,
  RELEASED,
};

struct ObParquetPage
{
public:
  static bool compare(const ObParquetPage &l, const ObParquetPage &r)
  {
    return l.offset_ < r.offset_;
  }

  int64_t start() const
  {
    return offset_;
  }

  int64_t end() const
  {
    return offset_ + length_;
  }

  TO_STRING_KV(K(offset_), K(length_), K(type_), K(status_))

  int64_t offset_;
  int64_t length_;
  ObParquetPageType type_;
  ObParquetPageStatus status_;
};

struct CoalescedReadRange
{
public:
  int64_t offset_;
  int64_t length_;
  int64_t end() const
  {
    return offset_ + length_;
  }
  ObArray<int64_t> coalesced_page_indices_;

  TO_STRING_KV(K(offset_), K(length_));
};

struct ObParquetPageCmp
{
  bool operator()(const ObParquetPage &read_range, const int64_t offset)
  {
    return read_range.offset_ < offset;
  }
};

class ObParquetPageMgr
{
public:
  ObParquetPageMgr();
  ~ObParquetPageMgr();
  int init(int64_t hole_size_limit,
           int64_t range_size_limit,
           bool enable_disk_cache,
           int64_t timeout_ts);
  int open(const ObExternalFileUrlInfo &file_url_info);
  int reset();
  int clear_all_pages();
  int init_with_new_row_group(const ObIArray<std::tuple<int64_t, int64_t, ObParquetPageType>> &selected_pages);
  int try_to_prefetch_by_offset(int64_t page_offset);
  int cache_page_by_offset(int64_t page_offset,
                           bool is_decompressed,
                           const std::shared_ptr<arrow::Buffer> &page_buffer);
  int cache_page_by_idx(int64_t page_idx,
                        bool is_decompressed,
                        const std::shared_ptr<arrow::Buffer> &page_buffer);
  int release_page_by_offset(int64_t page_offset, std::optional<bool> is_eager_access);
  int release_page_by_idx(int64_t page_idx, std::optional<bool> is_eager_access);
  int read_page(int64_t page_offset,
                std::shared_ptr<arrow::Buffer> &page_buffer,
                bool &has_mem_cached,
                bool &has_decompressed);
  int check_page_selected(int64_t page_offset,
                          bool &is_selected,
                          std::optional<int64_t> &next_page_offset) const;
  int register_metrics(ObLakeTableReaderProfile &reader_profile, const ObString &label);
  int register_io_metrics(ObLakeTableReaderProfile &reader_profile, const ObString &label);

private:
  int find_page_idx_(int64_t offset, int64_t &page_idx) const;
  // try to prefetch from page_idx(it may not prefetch happened)
  int try_to_prefetch_by_idx_(int64_t page_idx);
  // trigger prefetch from page_idx
  int trigger_prefetch_(int64_t page_idx);
  int get_page_indices_in_window_(int64_t start_page_idx,
                                  int64_t window_size,
                                  ObIArray<int64_t> &result) const;
  int collect_unloaded_pages_(const ObIArray<int64_t> &page_indices,
                              ObIArray<int64_t> &unloaded_page_indices) const;
  int try_pin_pages_from_mem_cache_(const ObIArray<int64_t> &page_indices);
  int try_pin_page_from_mem_cache_(int64_t page_idx);
  int load_pages_from_storage_(const CoalescedReadRange &coalesced_read_ranges);
  int coalesce_pages_(const ObIArray<int64_t> &unloaded_page_indices,
                      const int64_t hold_size_limit,
                      const int64_t range_size_limit,
                      ObIArray<CoalescedReadRange> &result) const;
  int decide_need_to_release_page_(const ObParquetPage &page, std::optional<bool> is_eager_access, bool &need_release_page) const;
  ObExternalDataPageCacheKey create_page_cache_key_(int64_t page_offset) const;

  ObFixedArray<ObParquetPage, ObIAllocator> pages_;
  ObFixedArray<ObParquetPageBufferBase *, ObIAllocator> page_buffers_;
  ObArenaAllocator allocator_;

  ObString file_path_;
  ObString file_content_digest_;
  int64_t file_size_;
  int64_t file_modify_time_;

  // 这些值其实会在 init() 被覆盖
  int64_t hole_size_limit_ = 1 * 1024 * 1024;
  int64_t range_size_limit_ = 2 * 1024 * 1024;
  int64_t prefetch_size_ = 8 * range_size_limit_;
  bool enable_disk_cache_ = true;
  int64_t timeout_ts_ = INT64_MAX;

  ObExternalFileAccess file_reader_;
  ObExternalDataPageCache &data_page_cache_;
  ObLakeTableParquetPageMgrMetrics metrics_;
};

// Functor for reading page
struct PageMgrReadPageFunctor
{
  ObParquetPageMgr *mgr;

  int operator()(int64_t page_offset,
                 std::shared_ptr<arrow::Buffer> &page_buffer,
                 bool &has_mem_cached,
                 bool &has_decompressed) const
  {
    return mgr->read_page(page_offset, page_buffer, has_mem_cached, has_decompressed);
  }
};

// Functor for releasing page
struct PageMgrReleasePageFunctor
{
  ObParquetPageMgr *mgr;

  int operator()(int64_t page_offset, std::optional<bool> is_eager_access) const
  {
    return mgr->release_page_by_offset(page_offset, is_eager_access);
  }
};

// Functor for caching page
struct PageMgrCachePageFunctor
{
  ObParquetPageMgr *mgr;

  int operator()(int64_t page_offset,
                 bool is_decompressed,
                 const std::shared_ptr<arrow::Buffer> &page_buffer) const
  {
    return mgr->cache_page_by_offset(page_offset, is_decompressed, page_buffer);
  }
};

// Functor for checking if page is selected
struct PageMgrCheckPageSelectedFunctor
{
  ObParquetPageMgr *mgr;

  int operator()(int64_t page_offset, bool &is_page_selected, std::optional<int64_t> &next_page_start_offset) const
  {
    return mgr->check_page_selected(page_offset, is_page_selected, next_page_start_offset);
  }
};

} // namespace sql

} // namespace oceanbase

#endif // OCEANBASE_OB_PARQUET_PAGE_MGR_H
