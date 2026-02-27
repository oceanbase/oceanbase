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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_CHUNK_BUILDER_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_CHUNK_BUILDER_H_

#include <algorithm>
#include "deps/oblib/src/lib/rc/context.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_heap.h"
#include "lib/list/ob_dlist.h"
#include "lib/oblog/ob_log_module.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/sort/ob_sort_resource_manager.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"

namespace oceanbase
{
namespace sql
{

// Helper function to free chunk
template <typename Store_Row, bool has_addon>
void free_sort_chunk(ObSortVecOpChunk<Store_Row, has_addon> *&chunk, lib::MemoryContext &mem_context) {
  if (OB_NOT_NULL(chunk)) {
    chunk->~ObSortVecOpChunk<Store_Row, has_addon>();
    mem_context->get_malloc_allocator().free(chunk);
    chunk = nullptr;
  }
}

template <typename Store_Row, bool has_addon>
class ObSortChunkSingleSlicer
{
public:
  explicit ObSortChunkSingleSlicer(lib::MemoryContext &mem_context);
  int get_write_chunk(const int64_t level, const int64_t slice_id,
      ObSortVecOpChunk<Store_Row, has_addon> *&chunk, bool &is_chunk_changed);
private:
  lib::MemoryContext &mem_context_;
  ObSortVecOpChunk<Store_Row, has_addon> *chunk_;
};

template <typename Store_Row, bool has_addon>
ObSortChunkSingleSlicer<Store_Row, has_addon>::ObSortChunkSingleSlicer(lib::MemoryContext &mem_context)
  : mem_context_(mem_context), chunk_(nullptr)
{}

template <typename Store_Row, bool has_addon>
int ObSortChunkSingleSlicer<Store_Row, has_addon>::get_write_chunk(
    const int64_t level,
    const int64_t slice_id,
    ObSortVecOpChunk<Store_Row, has_addon> *&chunk,
    bool &is_chunk_changed)
{
  int ret = OB_SUCCESS;
  using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;
  SortVecOpChunk *new_chunk = nullptr;
  // For single slicer, we only create chunk once at the beginning
  if (OB_ISNULL(chunk_)) {
    // Create the chunk only on the first call
    new_chunk = OB_NEWx(SortVecOpChunk,
                     (&mem_context_->get_malloc_allocator()),
                     0 /* level */, *(&mem_context_->get_malloc_allocator()));

    if (OB_ISNULL(new_chunk)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate chunk", K(ret));
    } else {
      // First time creating chunk
      chunk_ = new_chunk;
      chunk_->level_ = level;
      is_chunk_changed = true;
      SQL_ENG_LOG(DEBUG, "single slicer created initial chunk");
    }
  } else {
    is_chunk_changed = false;
  }

  if (OB_SUCC(ret)) {
    chunk = chunk_;
  } else if (OB_NOT_NULL(new_chunk)) {
    free_sort_chunk(new_chunk, mem_context_);
    chunk_ = nullptr;
  }
  return ret;
}

template <typename Store_Row, bool has_addon>
class ObSortChunkMultiSlicer {
public:
  explicit ObSortChunkMultiSlicer(lib::MemoryContext &mem_context);
  int get_write_chunk(const int64_t level, const int64_t slice_id,
      ObSortVecOpChunk<Store_Row, has_addon> *&chunk, bool &is_chunk_changed);
private:
  lib::MemoryContext &mem_context_;
  ObSortVecOpChunk<Store_Row, has_addon> *current_chunk_;
};

template <typename Store_Row, bool has_addon>
ObSortChunkMultiSlicer<Store_Row, has_addon>::ObSortChunkMultiSlicer(lib::MemoryContext &mem_context)
  : mem_context_(mem_context), current_chunk_(nullptr)
{}

template <typename Store_Row, bool has_addon>
int ObSortChunkMultiSlicer<Store_Row, has_addon>::get_write_chunk(
    const int64_t level,
    const int64_t slice_id,
    ObSortVecOpChunk<Store_Row, has_addon> *&chunk,
    bool &is_chunk_changed)
{
  int ret = OB_SUCCESS;
  using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;
  SortVecOpChunk *new_chunk = nullptr;
  // For multi slicer, we create a new chunk when slice_id changes
  int64_t last_slice_id = OB_ISNULL(current_chunk_) ? -1 : current_chunk_->slice_id_;
  if (last_slice_id != slice_id) {
    // Need to create a new chunk for the new slice
    new_chunk =
        OB_NEWx(SortVecOpChunk,
                (&mem_context_->get_malloc_allocator()),
                level,
                *(&mem_context_->get_malloc_allocator()));

    if (OB_ISNULL(new_chunk)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate chunk for slice", K(ret), K(slice_id));
    } else {
      current_chunk_ = new_chunk;
      current_chunk_->level_ = level;
      is_chunk_changed = true;
      current_chunk_->slice_id_ = slice_id;
      SQL_ENG_LOG(DEBUG, "multi slicer created new chunk for slice", K(slice_id));
    }
  } else {
    // Same slice as before, no new chunk needed
    // Note: This assumes the caller will manage the lifecycle of chunks
    is_chunk_changed = false;
  }

  if (OB_SUCC(ret)) {
    chunk = current_chunk_;
  } else if (OB_NOT_NULL(new_chunk)) {
    free_sort_chunk(new_chunk, mem_context_);
    current_chunk_ = nullptr;
  }
  return ret;
}


// Slice decider implementation based on range partitioning
template <typename Compare, typename Store_Row>
class ObSortChunkSliceDecider
{
public:
  // 缓存转换后的 range 边界，避免每次都浅拷贝
  using DatumArray = common::ObSEArray<ObDatum, 8>;
  using RangeBounds = common::ObArray<DatumArray>;


public:
  ObSortChunkSliceDecider()
    : last_slice_high_bound_idx_(0),
      range_bounds_(),
      is_inited_(false),
      comp_(nullptr)
    {}
  virtual ~ObSortChunkSliceDecider() {}

  // 初始化：预处理 range_cut，将 DatumKey 浅拷贝到 ObArray
  int init(const ObPxTabletRange *slice_range, Compare *comp)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(comp)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(slice_range), KP(comp));
    } else if (OB_ISNULL(slice_range)) {
      LOG_INFO("slice range is empty", K(slice_range));
    } else {
      comp_ = comp;
      // 预处理：将所有 range_cut 的 DatumKey 浅拷贝到普通数组，便于后续比较
      const ObPxTabletRange::RangeCut &range_cut = slice_range->range_cut_;
      if (OB_FAIL(range_bounds_.reserve(range_cut.count()))) {
        SQL_ENG_LOG(WARN, "failed to reserve range_bounds", K(ret), K(range_cut.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < range_cut.count(); ++i) {
        const ObPxTabletRange::DatumKey &datum_key = range_cut.at(i);
        DatumArray bound_array;
        // 浅拷贝：将 DatumKey (Ob2DArray) 的元素复制到 ObSEArray
        for (int64_t j = 0; OB_SUCC(ret) && j < datum_key.count(); ++j) {
          if (OB_FAIL(bound_array.push_back(datum_key.at(j)))) {
            SQL_ENG_LOG(WARN, "failed to push back datum", K(ret), K(i), K(j));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(range_bounds_.push_back(bound_array))) {
            SQL_ENG_LOG(WARN, "failed to push back bound_array", K(ret), K(i));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      last_slice_high_bound_idx_ = 0;
      is_inited_ = true;
    }
    return ret;
  }

  int decide(const Store_Row *sort_key_row, int64_t &slice_id)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "slice decider not inited", K(ret));
    } else if (OB_ISNULL(sort_key_row)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "sort_key_row is null", K(ret), KP(sort_key_row));
    } else {
      const int64_t range_cnt = range_bounds_.count();

      // 如果没有 range 边界，所有行都属于 slice 0
      if (range_cnt == 0) {
        slice_id = 0;
      } else {
        if (OB_FAIL(ret)) {
          // 提取 datum 失败
        } else {
          // 如果 row_datums < last_bound，说明还在同一个 slice
          if (last_slice_high_bound_idx_ < range_cnt) {
            int cmp = comp_->compare(sort_key_row, range_bounds_.at(last_slice_high_bound_idx_));
            if (OB_FAIL(comp_->get_error_code())) {
              SQL_ENG_LOG(WARN, "compare failed", K(ret));
            } else if (cmp < 0) {
              // row < last_bound，还在同一个 slice
              slice_id = last_slice_high_bound_idx_;
            } else if (cmp == 0) {
              last_slice_high_bound_idx_++;
              slice_id = last_slice_high_bound_idx_;
            } else {
              RangeBounds::iterator it = std::upper_bound(
                  range_bounds_.begin() + last_slice_high_bound_idx_,
                  range_bounds_.end(),
                  sort_key_row,
                  *comp_);
              if (OB_FAIL(comp_->get_error_code())) {
                SQL_ENG_LOG(WARN, "std::lower_bound compare failed", K(ret));
              } else {
                slice_id = it - range_bounds_.begin();
                last_slice_high_bound_idx_ = slice_id;
              }
            }
          } else {
            // last_slice_high_bound_idx_ 已经超出范围，说明在最后一个 slice
            slice_id = range_cnt;
          }
        }
      }
    }
    return ret;
  }

  void reset()
  {
    last_slice_high_bound_idx_ = 0;
    range_bounds_.reset();
    is_inited_ = false;
  }
  void reuse()
  {
    last_slice_high_bound_idx_ = 0;
  }
private:
  int64_t last_slice_high_bound_idx_;  // 上一次找到的边界索引，用于有序输入的优化
  RangeBounds range_bounds_;            // 缓存转换后的 range 边界
  bool is_inited_;
  Compare *comp_;
};

// Chunk builder class that encapsulates the entire build_chunk logic
template <typename Compare, typename Store_Row, bool has_addon, typename Slicer>
class ObSortChunkBuilder
{
public:
  explicit ObSortChunkBuilder(
      ObMonitorNode &op_monitor_info, lib::MemoryContext &mem_context,
      const RowMeta *sk_row_meta, const RowMeta *addon_row_meta, const int64_t max_batch_size,
      ObCompressorType compress_type,
      bool use_heap_sort, bool is_fetch_with_ties, bool has_addon_flag,
      int64_t topn_cnt, const uint64_t tenant_id,
      const int64_t tempstore_read_alignment_size,
      ObIOEventObserver *io_event_observer,
      ObSortResourceManager<Compare, Store_Row, has_addon>
          &sort_resource_mgr,
      Slicer &slicer, ObSortChunkSliceDecider<Compare, Store_Row> *slice_decider)
      : op_monitor_info_(op_monitor_info), mem_context_(mem_context),
        sk_row_meta_(sk_row_meta), addon_row_meta_(addon_row_meta), max_batch_size_(max_batch_size),
        compress_type_(compress_type),
        use_heap_sort_(use_heap_sort), is_fetch_with_ties_(is_fetch_with_ties),
        has_addon_(has_addon_flag), topn_cnt_(topn_cnt), tenant_id_(tenant_id),
        tempstore_read_alignment_size_(tempstore_read_alignment_size),
        io_event_observer_(io_event_observer),
        sort_resource_mgr_(sort_resource_mgr), slicer_(slicer), slice_decider_(slice_decider) {}

  int init_temp_row_store(const RowMeta *row_meta,
                          const int64_t mem_limit, const int64_t batch_size,
                          const bool need_callback, const bool enable_dump,
                          const int64_t extra_size,
                          ObCompressorType compress_type,
                          ObTempRowStore &row_store)
  {
    int ret = OB_SUCCESS;
    const bool enable_trunc = true;
    const bool reorder_fixed_expr = true;
    ObMemAttr mem_attr(tenant_id_, ObModIds::OB_SQL_SORT_ROW,
                       ObCtxIds::WORK_AREA);

    if (OB_FAIL(row_store.init(*row_meta, batch_size, mem_attr, mem_limit,
                               enable_dump,
                               compress_type,
                               enable_trunc,
                               tempstore_read_alignment_size_))) {
      SQL_ENG_LOG(WARN, "init row store failed", K(ret));
    } else {
      row_store.set_dir_id(sort_resource_mgr_.get_dir_id());
      row_store.set_allocator(mem_context_->get_malloc_allocator());
      row_store.set_io_event_observer(io_event_observer_);
      if (need_callback) {
        row_store.set_callback(&sort_resource_mgr_.get_sql_mem_processor());
      }
    }
    return ret;
  }

  int get_write_chunk(const int64_t level,
                      const int64_t slice_id,
                      ObSortVecOpChunk<Store_Row, has_addon> *&chunk,
                      bool &is_chunk_changed)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(slicer_.get_write_chunk(level, slice_id, chunk, is_chunk_changed))) {
      SQL_ENG_LOG(WARN, "failed to get write chunk", K(ret));
    } else if (!is_chunk_changed) {
    } else if (OB_NOT_NULL(chunk)) {
      chunk->sort_row_store_mgr_.set_sk_row_meta(sk_row_meta_);
      if (has_addon_) {
        chunk->sort_row_store_mgr_.set_addon_row_meta(addon_row_meta_);
      }
      if (OB_FAIL(init_temp_row_store(
                   sk_row_meta_, 1, max_batch_size_, true,
                   true /*enable dump*/, Store_Row::get_extra_size(true),
                   compress_type_, chunk->get_sk_store()))) {
        SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
      } else if (has_addon_ &&
                 OB_FAIL(init_temp_row_store(
                     addon_row_meta_, 1, max_batch_size_, true,
                     true /*enable dump*/, Store_Row::get_extra_size(false),
                     compress_type_, chunk->get_addon_store()))) {
        SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
      }
      if (OB_FAIL(ret)) {
        free_chunk(chunk);
      }
    }
    return ret;
  }

  template <typename Input> int build(const int64_t level, Input &input,
                                      common::ObIArray<ObSortVecOpChunk<Store_Row, has_addon>*> &output_chunks) {
    int ret = OB_SUCCESS;
    int64_t curr_time = ObTimeUtility::fast_current_time();
    int64_t stored_row_cnt = 0;
    const Store_Row *sort_key_row = nullptr;
    const Store_Row *addon_field_row = nullptr;
    ObCompactRow *dst_sk_row = nullptr;
    ObCompactRow *dst_addon_row = nullptr;
    using SortVecOpChunk = ObSortVecOpChunk<Store_Row, has_addon>;
    SortVecOpChunk *tmp_chunk = nullptr;
    SortVecOpChunk *chunk = nullptr;
    bool is_chunk_changed = false;
    int64_t slice_id = 0;
    if (OB_NOT_NULL(slice_decider_)) {
      slice_decider_->reuse();
    }
    while (OB_SUCC(ret)) {
      if (use_heap_sort_ && !is_fetch_with_ties_ &&
          stored_row_cnt >= topn_cnt_) {
        break;
      } else if (OB_FAIL(input(sort_key_row, addon_field_row))) {
        if (OB_ITER_END != ret) {
          SQL_ENG_LOG(WARN, "get input row failed", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_NOT_NULL(slice_decider_) && OB_FAIL(slice_decider_->decide(sort_key_row, slice_id))) {
        SQL_ENG_LOG(WARN, "failed to decide slice id", K(ret));
      } else if (OB_FAIL(get_write_chunk(level, slice_id, tmp_chunk, is_chunk_changed))) {
        SQL_ENG_LOG(WARN, "failed to get write chunk", K(ret));
      } else if (is_chunk_changed) {
        if (OB_NOT_NULL(chunk)) {
          if (OB_FAIL(dump_chunk(level, curr_time, chunk, output_chunks))) {
            SQL_ENG_LOG(WARN, "failed to dump chunk", K(ret));
          } else {
            curr_time = ObTimeUtility::fast_current_time();
            chunk = nullptr;
          }
        }
        if (OB_SUCC(ret)) {
          chunk = tmp_chunk;
          tmp_chunk = nullptr;
        } else {
          free_chunk(tmp_chunk);
        }
      } else {
        tmp_chunk = nullptr;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(chunk->get_sk_store().add_row(
                     reinterpret_cast<const ObCompactRow *>(sort_key_row),
                     dst_sk_row))) {
        SQL_ENG_LOG(WARN, "copy row to row store failed");
      } else if (has_addon_ &&
                 OB_FAIL(chunk->get_addon_store().add_row(
                     reinterpret_cast<const ObCompactRow *>(addon_field_row),
                     dst_addon_row))) {
        SQL_ENG_LOG(WARN, "copy row to row store failed");
      } else {
        stored_row_cnt++;
        if (level > 0) {
          op_monitor_info_.otherstat_1_id_ =
              ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
          op_monitor_info_.otherstat_1_value_ += 1;
        }
      }
    }
    if (OB_SUCC(ret) && nullptr != chunk) {
      if (OB_FAIL(dump_chunk(level, curr_time, chunk, output_chunks))) {
        SQL_ENG_LOG(WARN, "failed to dump chunk", K(ret));
      } else {
        chunk = nullptr;
      }
    }
    if (OB_FAIL(ret) && nullptr != chunk) {
      free_chunk(chunk);
    }

    return ret;
  }

private:
  int dump_chunk(const int64_t level, const int64_t begin_time,
                 ObSortVecOpChunk<Store_Row, has_addon> *&chunk,
                 common::ObIArray<ObSortVecOpChunk<Store_Row, has_addon>*> &output_chunks) {
    int ret = OB_SUCCESS;
    // 必须强制先dump，然后finish dump才有效
    if (has_addon_ &&
        (chunk->get_sk_store().get_row_cnt() != chunk->get_addon_store().get_row_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN,
                  "the number of rows in sort key store and addon store is "
                  "expected to be equal",
                  K(chunk->get_sk_store().get_row_cnt()),
                  K(chunk->get_addon_store().get_row_cnt()), K(ret));
    } else if (OB_FAIL(chunk->get_sk_store().dump(true))) {
      SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
    } else if (OB_FAIL(
                   chunk->get_sk_store().finish_add_row(true /*+ need dump */))) {
      SQL_ENG_LOG(WARN, "finish add row failed", K(ret));
    } else if (has_addon_ && OB_FAIL(chunk->get_addon_store().dump(true))) {
      SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
    } else if (has_addon_ && OB_FAIL(chunk->get_addon_store().finish_add_row(
                                 true /*+ need dump */))) {
      SQL_ENG_LOG(WARN, "finish add row failed", K(ret));
    } else {
      const int64_t sort_io_time =
          ObTimeUtility::fast_current_time() - begin_time;
      const int64_t file_size = has_addon_
                                    ? (chunk->get_sk_store().get_file_size() +
                                       chunk->get_addon_store().get_file_size())
                                    : chunk->get_sk_store().get_file_size();
      const int64_t mem_hold = has_addon_ ? (chunk->get_sk_store().get_mem_hold() +
                                             chunk->get_addon_store().get_mem_hold())
                                          : chunk->get_sk_store().get_mem_hold();
      op_monitor_info_.otherstat_4_id_ =
          ObSqlMonitorStatIds::SORT_DUMP_DATA_TIME;
      op_monitor_info_.otherstat_4_value_ += sort_io_time;
      LOG_TRACE("dump sort file", "level", level, "rows",
                chunk->get_sk_store().get_row_cnt(), "file_size", file_size,
                "memory_hold", mem_hold, "mem_used", mem_context_->used());
    }

    if (OB_SUCC(ret)) {
      // Add chunk to output array instead of inserting into linked list
      if (OB_FAIL(output_chunks.push_back(chunk))) {
        SQL_ENG_LOG(WARN, "failed to add chunk to output array", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(chunk)) {
      free_chunk(chunk);
    }
    return ret;
  }

  void free_chunk(ObSortVecOpChunk<Store_Row, has_addon> *&chunk) {
    free_sort_chunk(chunk, mem_context_);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSortChunkBuilder);

  ObMonitorNode &op_monitor_info_;
  lib::MemoryContext &mem_context_;
  const RowMeta *sk_row_meta_;
  const RowMeta *addon_row_meta_;
  const int64_t max_batch_size_;
  ObCompressorType compress_type_;
  bool use_heap_sort_;
  bool is_fetch_with_ties_;
  bool has_addon_;
  int64_t topn_cnt_;
  uint64_t tenant_id_;
  int64_t tempstore_read_alignment_size_;
  ObIOEventObserver *io_event_observer_;
  ObSortResourceManager<Compare, Store_Row, has_addon> &sort_resource_mgr_;
  Slicer &slicer_;
  ObSortChunkSliceDecider<Compare, Store_Row> *slice_decider_; // for slice decider
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_CHUNK_BUILDER_H_ */
