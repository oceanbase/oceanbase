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

#ifndef OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_VEC_IMPL_H_
#define OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_VEC_IMPL_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/sort/ob_sort_row_store_mgr.h"
#include "sql/engine/sort/ob_sort_vec_strategy.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_storage_sort_resource_manager.h"
#include "sql/engine/sort/ob_sort_vec_dump_strategy.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/sort/ob_sort_chunk_builder.h"
#include "sql/engine/sort/ob_external_merge_sorter.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace sql
{
struct ObBatchRows;
struct ObCompactRow;
struct RowMeta;

template <typename Compare, typename Store_Row, bool has_addon = false>
class ObStorageVecSortImpl
{
  typedef ObSortVecOpChunk<Store_Row, has_addon> ChunkType;
  typedef ObExternalMergeSorter<Compare, Store_Row, has_addon> ExternalMergeSorter;
public:
  explicit ObStorageVecSortImpl(lib::MemoryContext &mem_context, ObMonitorNode &op_monitor_info);
  virtual ~ObStorageVecSortImpl();

  int init(Compare *comp,
           const RowMeta *sk_row_meta,
           const RowMeta *addon_row_meta,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const common::ObCompressorType compressor_type,
           const bool enable_trunc,
           int64_t tempstore_read_alignment_size,
           const uint64_t tenant_id,
           const bool enable_encode_sortkey,
           ObSortChunkSliceDecider<Compare, Store_Row> *slice_decider);

  void reset();
  void reset_for_merge_sort()
  {
    while (!sort_chunks_.is_empty()) {
      ObSortVecOpChunk<Store_Row, has_addon> *chunk = sort_chunks_.remove_first();
      if(OB_NOT_NULL(chunk)) {
        chunk->~ObSortVecOpChunk<Store_Row, has_addon>();
        if (nullptr != mem_context_) {
          mem_context_->get_malloc_allocator().free(chunk);
        }
      }
    }
    if (OB_NOT_NULL(external_sorter_)) {
      external_sorter_->~ExternalMergeSorter();
      mem_context_->get_malloc_allocator().free(external_sorter_);
      external_sorter_ = nullptr;
    }
  }
  int add_batch(const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
                const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
                const uint16_t selector[],
                const int64_t batch_size,
                Store_Row **sk_rows,
                Store_Row **addon_rows,
                int64_t &inmem_row_size);
  int sort();
  int sort_inmem_data(const bool need_force_dump);
  int get_sort_chunks(common::ObIArray<ChunkType *> &sort_chunks);
  int add_sort_chunks(const int64_t level, common::ObIArray<ChunkType *> &output_chunks);
  int add_sort_chunk(const int64_t level, ChunkType *&chunk);
  int get_merge_ways(const int64_t max_merge_ways, int64_t &merge_ways);
  int merge_sort_chunks(ChunkType *&output_chunk);
  int get_next_batch(int64_t &output_row_cnt, ObIArray<ObIVector *> *&output_sk_rows, ObIArray<ObIVector *> *&output_addon_rows);
  int get_sort_chunks_size() { return sort_chunks_.get_size(); }
  ObIAllocator *get_allocator() { return &mem_context_->get_malloc_allocator(); }
  bool is_inited() { return is_inited_; }
  bool has_slice_decider() { return slice_decider_ != nullptr; }
private:
  int build_vector_array(const sql::RowMeta &row_meta, ObIArray<ObIVector *> *&vec_array, ObIArray<int64_t> &capacity_array);
  void release_vector_array(ObIArray<ObIVector *> *&vec_array, ObIArray<int64_t> &capacity_array);
  void release_ivector(common::ObIVector *&vec, ObIAllocator &allocator);
  static int write_row_to_vec(
    const RowMeta &row_meta, const Store_Row *row,
    const int64_t &row_index, const ObIArray<int64_t> &vec_capacities,
    ObIArray<ObIVector *> *&output_vec, bool &is_space_enough);
  int init_sort_strategy(const bool enable_encode_sortkey);
  void cleanup_sort_strategy();
  int sort_inmem_data(const int64_t begin, const int64_t end);
  int preprocess_dump(bool &dumped);
  int do_dump();
  int before_add_row();
  int after_add_row(Store_Row *sr);
  template <typename Input>
  int build_chunk(const int64_t level, Input &input, common::ObIArray<ChunkType *> &output_chunks);

private:
  lib::MemoryContext &mem_context_;
  Compare *comp_;
  const RowMeta *sk_row_meta_;
  const RowMeta *addon_row_meta_;
  ObSortChunkSliceDecider<Compare, Store_Row> *slice_decider_;
  common::ObArray<Store_Row *> rows_;
  ObFullSortStrategy<Compare, Store_Row, has_addon> *sort_strategy_;
  ObSortVecOpChunk<Store_Row, has_addon> *in_memory_chunk_;
  ObMonitorNode &op_monitor_info_;
  ObStorageSortResourceManager<Compare, Store_Row, has_addon> *sort_resource_mgr_;
  common::ObDList<ObSortVecOpChunk<Store_Row, has_addon>> sort_chunks_;
  ExternalMergeSorter *external_sorter_;
  uint64_t tenant_id_;
  int64_t tempstore_read_alignment_size_;
  int64_t max_batch_size_;
  common::ObCompressorType compress_type_;
  ObIArray<ObIVector *> *output_sk_rows_;
  ObIArray<ObIVector *> *output_addon_rows_;
  const Store_Row *last_sk_row_;
  const Store_Row *last_addon_row_;
  ObArray<int64_t> sk_vec_capacities_;
  ObArray<int64_t> addon_vec_capacities_;
  bool is_inited_;
};

template <typename Compare, typename Store_Row, bool has_addon>
ObStorageVecSortImpl<Compare, Store_Row, has_addon>::ObStorageVecSortImpl(
    lib::MemoryContext &mem_context, ObMonitorNode &op_monitor_info)
  : mem_context_(mem_context),
    comp_(nullptr),
    sk_row_meta_(nullptr),
    addon_row_meta_(nullptr),
    slice_decider_(nullptr),
    rows_(),
    sort_strategy_(nullptr),
    in_memory_chunk_(nullptr),
    op_monitor_info_(op_monitor_info),
    sort_resource_mgr_(nullptr),
    sort_chunks_(),
    external_sorter_(nullptr),
    tenant_id_(OB_INVALID_ID),
    tempstore_read_alignment_size_(0),
    max_batch_size_(0),
    compress_type_(NONE_COMPRESSOR),
    output_sk_rows_(nullptr),
    output_addon_rows_(nullptr),
    last_sk_row_(nullptr),
    last_addon_row_(nullptr),
    sk_vec_capacities_(),
    addon_vec_capacities_(),
    is_inited_(false)
{}

template <typename Compare, typename Store_Row, bool has_addon>
ObStorageVecSortImpl<Compare, Store_Row, has_addon>::~ObStorageVecSortImpl()
{
  reset();
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::init(Compare *comp,
                                                              const RowMeta *sk_row_meta,
                                                              const RowMeta *addon_row_meta,
                                                              const int64_t max_batch_size,
                                                              const lib::ObMemAttr &mem_attr,
                                                              const common::ObCompressorType compressor_type,
                                                              const bool enable_trunc,
                                                              int64_t tempstore_read_alignment_size,
                                                              const uint64_t tenant_id,
                                                              const bool enable_encode_sortkey,
                                                              ObSortChunkSliceDecider<Compare, Store_Row> *slice_decider)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "already initialized", K(ret));
  } else if (OB_ISNULL(sk_row_meta) || OB_ISNULL(comp)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(sk_row_meta), K(comp));
  } else {
    sk_row_meta_ = sk_row_meta;
    comp_ = comp;
    const int64_t input_rows = 100000;
    const int64_t input_width = 10;
    int64_t size =  input_rows * input_width;
    using ObSortVecOpChunkType = ObSortVecOpChunk<Store_Row, has_addon>;
    in_memory_chunk_ = OB_NEWx(ObSortVecOpChunkType,
      (&mem_context_->get_malloc_allocator()),
      0 /* level */, *(&mem_context_->get_malloc_allocator()));
    if (OB_ISNULL(in_memory_chunk_ )) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (OB_FAIL(in_memory_chunk_->sort_row_store_mgr_.init(*sk_row_meta,
                                has_addon ? addon_row_meta : nullptr,
                                max_batch_size,
                                mem_attr,
                                compressor_type,
                                INT64_MAX,
                                false,
                                enable_trunc,
                                tempstore_read_alignment_size))) {
      SQL_ENG_LOG(WARN, "failed to init store_mgr_", K(ret));
    } else if (OB_FAIL(init_sort_strategy(enable_encode_sortkey))) {
      SQL_ENG_LOG(WARN, "failed to init sort strategy", K(ret), K(enable_encode_sortkey));
    } else {
      // 创建默认的 ObSqlProfileExecInfo
      ObSqlProfileExecInfo exec_info;
      // 使用 mem_limit 作为 cache_size
      using ObStorageSortResourceManagerType = ObStorageSortResourceManager<Compare, Store_Row, has_addon>;
      if (OB_ISNULL(sort_resource_mgr_ = OB_NEWx(ObStorageSortResourceManagerType,
        (&mem_context_->get_malloc_allocator()),
        op_monitor_info_, &mem_context_, in_memory_chunk_->sort_row_store_mgr_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else if (OB_FAIL(sort_resource_mgr_->init(tenant_id, size, exec_info))) {
        SQL_ENG_LOG(WARN, "failed to init sort resource manager", K(ret));
      } else {
        is_inited_ = true;
        tenant_id_ = tenant_id;
        tempstore_read_alignment_size_ = tempstore_read_alignment_size;
        max_batch_size_ = max_batch_size;
        compress_type_ = compressor_type;
        addon_row_meta_ = has_addon ? addon_row_meta : nullptr;
        slice_decider_ = slice_decider; // can be nullptr
        SQL_ENG_LOG(DEBUG, "ObStorageVecSortImpl init success", K(has_addon), K(max_batch_size));
      }
    }
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::init_sort_strategy(const bool enable_encode_sortkey)
{
  int ret = OB_SUCCESS;

  cleanup_sort_strategy();

  ObIAllocator &allocator = mem_context_.ref_context()->get_malloc_allocator();
  void *buf = allocator.alloc(sizeof(ObFullSortStrategy<Compare, Store_Row, has_addon>));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    sort_strategy_ = new (buf) ObFullSortStrategy<Compare, Store_Row, has_addon>(
        *comp_, sk_row_meta_, &mem_context_, enable_encode_sortkey);

    if (OB_ISNULL(sort_strategy_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to create sort strategy", K(ret), K(enable_encode_sortkey));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::sort_inmem_data(const bool need_force_dump)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (OB_ISNULL(sort_strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sort strategy is null", K(ret));
  } else if (OB_FAIL(sort_strategy_->sort_inmem_data(0, rows_.count(), &rows_))) {
    SQL_ENG_LOG(WARN, "failed to sort in-memory data", K(ret));
  } else {
    if (0 != sort_chunks_.get_size() || need_force_dump) {
      NormalDumpStrategy<Compare, Store_Row, has_addon> input(&rows_, nullptr, sk_row_meta_);
      ObArray<ChunkType *> output_chunks;
      if (OB_FAIL(build_chunk(0/*level*/, input, output_chunks))) {
        SQL_ENG_LOG(WARN, "failed to build chunk", K(ret));
      } else if (OB_FAIL(add_sort_chunks(0/*level*/, output_chunks))) {
        SQL_ENG_LOG(WARN, "failed to add sort chunks", K(ret));
      } else {
        rows_.reset();
        in_memory_chunk_->~ObSortVecOpChunk<Store_Row, has_addon>();
        mem_context_.ref_context()->get_malloc_allocator().free(in_memory_chunk_);
        in_memory_chunk_ = nullptr;
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < output_chunks.count(); i++) {
          ChunkType *chunk = output_chunks.at(i);
          if (nullptr != chunk) {
            chunk->~ChunkType();
            mem_context_->get_malloc_allocator().free(chunk);
          }
        }
        output_chunks.reset();
      }
    } else {
      if (OB_FAIL(in_memory_chunk_->inmem_rows_.assign(rows_))) {
        SQL_ENG_LOG(WARN, "failed to assign inmem rows", K(ret));
      } else {
        in_memory_chunk_->use_inmem_data_ = true;
        rows_.reset();
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::get_sort_chunks(common::ObIArray<ObSortVecOpChunk<Store_Row, has_addon> *> &sort_chunks)
{
  int ret = OB_SUCCESS;
  sort_chunks.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (sort_chunks_.get_size() > 0) {
    while (OB_SUCC(ret) && !sort_chunks_.is_empty()) {
      ChunkType *chunk = sort_chunks_.remove_first();
      if (OB_FAIL(sort_chunks.push_back(chunk))) {
        SQL_ENG_LOG(WARN, "failed to push back chunk", K(ret));
        if (OB_NOT_NULL(chunk)) {
          chunk->~ObSortVecOpChunk<Store_Row, has_addon>();
          if (nullptr != mem_context_) {
            mem_context_->get_malloc_allocator().free(chunk);
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < sort_chunks.count(); ++i) {
        ChunkType *chunk = sort_chunks.at(i);
        if(OB_NOT_NULL(chunk)) {
          chunk->~ObSortVecOpChunk<Store_Row, has_addon>();
          if (nullptr != mem_context_) {
            mem_context_->get_malloc_allocator().free(chunk);
          }
        }
      }
      sort_chunks.reset();
    }
  } else {
    if (OB_FAIL(sort_chunks.push_back(in_memory_chunk_))) {
      SQL_ENG_LOG(WARN, "failed to push back chunk", K(ret));
    } else {
      in_memory_chunk_ = nullptr;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::sort_inmem_data(
    const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (OB_ISNULL(sort_strategy_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sort strategy is null", K(ret));
  } else {
    ret = sort_strategy_->sort_inmem_data(begin, end, &rows_);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObStorageVecSortImpl<Compare, Store_Row, has_addon>::cleanup_sort_strategy()
{
  if (OB_NOT_NULL(sort_strategy_)) {
    ObIAllocator &allocator = mem_context_.ref_context()->get_malloc_allocator();
    sort_strategy_->~ObFullSortStrategy<Compare, Store_Row, has_addon>();
    allocator.free(sort_strategy_);
    sort_strategy_ = nullptr;
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObStorageVecSortImpl<Compare, Store_Row, has_addon>::reset()
{
  cleanup_sort_strategy();
  sk_row_meta_ = nullptr;
  rows_.reset();
  comp_ = nullptr;
  slice_decider_ = nullptr;
  while (!sort_chunks_.is_empty()) {
    ObSortVecOpChunk<Store_Row, has_addon> *chunk = sort_chunks_.remove_first();
    if(OB_NOT_NULL(chunk)) {
      chunk->~ObSortVecOpChunk<Store_Row, has_addon>();
      if (nullptr != mem_context_) {
        mem_context_->get_malloc_allocator().free(chunk);
      }
    }
  }
  if (OB_NOT_NULL(in_memory_chunk_)) {
    in_memory_chunk_->~ObSortVecOpChunk<Store_Row, has_addon>();
    mem_context_.ref_context()->get_malloc_allocator().free(in_memory_chunk_);
    in_memory_chunk_ = nullptr;
  }
  if (OB_NOT_NULL(external_sorter_)) {
    external_sorter_->~ExternalMergeSorter();
    mem_context_->get_malloc_allocator().free(external_sorter_);
    external_sorter_ = nullptr;
  }
  if (OB_NOT_NULL(sort_resource_mgr_)) {
    sort_resource_mgr_->~ObStorageSortResourceManager<Compare, Store_Row, has_addon>();
    mem_context_.ref_context()->get_malloc_allocator().free(sort_resource_mgr_);
    sort_resource_mgr_ = nullptr;
  }

  if (OB_NOT_NULL(output_sk_rows_)) {
    release_vector_array(output_sk_rows_, sk_vec_capacities_);
    output_sk_rows_ = nullptr;
  }
  if (OB_NOT_NULL(output_addon_rows_)) {
    release_vector_array(output_addon_rows_, addon_vec_capacities_);
    output_addon_rows_ = nullptr;
  }
  last_sk_row_ = nullptr;
  last_addon_row_ = nullptr;
  is_inited_ = false;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::preprocess_dump(bool &dumped)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_resource_mgr_->preprocess_dump(dumped))) {
    SQL_ENG_LOG(WARN, "failed to preprocess dump", K(ret));
  } else {
    if (dumped && rows_.empty()) {
      dumped = false; //no data to dump, try to add a batch of data directly
      SQL_ENG_LOG(TRACE, "Insufficient memory, unable to store a batch of data", K(sort_resource_mgr_->get_memory_used()), K(sort_resource_mgr_->get_memory_bound()),
                    K(sort_resource_mgr_->get_cache_size()), K(sort_resource_mgr_->get_expect_size()));
    }
  }
  return ret;
}

template<typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::do_dump()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (rows_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(sort_inmem_data(0 /*begin*/, rows_.count() /*end*/))) {
    SQL_ENG_LOG(WARN, "sort in-memory data failed", K(ret));
  } else {
    const int64_t level = 0;
    NormalDumpStrategy<Compare, Store_Row, has_addon>
        input(&rows_, nullptr, sk_row_meta_);
    ObArray<ChunkType *> output_chunks;
    if (OB_FAIL(build_chunk(level, input, output_chunks))) {
      SQL_ENG_LOG(WARN, "build chunk failed", K(ret));
    } else if (OB_FAIL(add_sort_chunks(level, output_chunks))) {
      SQL_ENG_LOG(WARN, "failed to add sort chunks", K(ret));
    } else {
      rows_.reset();
      sort_resource_mgr_->free(sort_resource_mgr_->get_data_size());
      in_memory_chunk_->sort_row_store_mgr_.reuse();
      sort_resource_mgr_->set_number_pass(level + 1);
      sort_resource_mgr_->reset_sql_mem_processor();
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < output_chunks.count(); i++) {
        ChunkType *chunk = output_chunks.at(i);
        if (nullptr != chunk) {
          chunk->~ChunkType();
          mem_context_->get_malloc_allocator().free(chunk);
        }
      }
      output_chunks.reset();
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::before_add_row()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (!rows_.empty()) {
    bool updated = false;
    if (OB_FAIL(sort_resource_mgr_->update_max_available_mem_size_periodically(rows_.count(), updated))) {
      SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
    } else if (updated && OB_FAIL(sort_resource_mgr_->update_used_mem_size(sort_resource_mgr_->get_total_used_size()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled()) {
      if (rows_.count() >= ObSortResourceManager<Compare, Store_Row, has_addon>::MAX_ROW_CNT) {
        // 最大2G，超过2G会扩容到4G，4G申请会失败
        if (OB_FAIL(do_dump())) {
          SQL_ENG_LOG(WARN, "dump failed", K(ret));
        }
      } else if (sort_resource_mgr_->need_dump()) {
        bool dumped = false;
        if (OB_FAIL(preprocess_dump(dumped))) {
          SQL_ENG_LOG(WARN, "failed preprocess dump", K(ret));
        } else if (dumped && OB_FAIL(do_dump())) {
          SQL_ENG_LOG(WARN, "dump failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::after_add_row(Store_Row *sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rows_.push_back(sr))) {
    SQL_ENG_LOG(WARN, "array push back failed", K(ret), K(rows_.count()));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
template <typename Input>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::build_chunk(const int64_t level, Input &input, common::ObIArray<ChunkType *> &output_chunks)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else {
    ObSortChunkMultiSlicer<Store_Row, has_addon> slicer(mem_context_);
    // Create and use the chunk builder
    ObSortChunkBuilder<Compare, Store_Row, has_addon, ObSortChunkMultiSlicer<Store_Row, has_addon>> builder(
        op_monitor_info_,
        mem_context_,
        sk_row_meta_,
        has_addon ? addon_row_meta_ : nullptr,
        max_batch_size_,
        compress_type_,
        false,
        false,
        has_addon,
        INT64_MAX,
        tenant_id_,
        tempstore_read_alignment_size_,
        nullptr, // TODO: add io event observer
        *sort_resource_mgr_,
        slicer, slice_decider_);
    if (OB_FAIL(builder.build(level, input, output_chunks))) {
      SQL_ENG_LOG(WARN, "failed to build chunk", K(ret));
    }
  }
  SQL_ENG_LOG(INFO, "build chunk", K(level), K(output_chunks.count()));
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::add_batch(
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
    const common::ObFixedArray<ObIVector *, common::ObIAllocator> *addon_vec_ptrs,
    const uint16_t selector[],
    const int64_t batch_size,
    Store_Row **sk_rows,
    Store_Row **addon_rows,
    int64_t &inmem_row_size)
{
  int ret = OB_SUCCESS;
  inmem_row_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(before_add_row())) {
    SQL_ENG_LOG(WARN, "failed to before add row", K(ret));
  } else {
    if (OB_FAIL(in_memory_chunk_->sort_row_store_mgr_.add_batch(sk_vec_ptrs,
                                     has_addon ? addon_vec_ptrs : nullptr,
                                     selector,
                                     batch_size,
                                     sk_rows,
                                     has_addon ? addon_rows : nullptr,
                                     inmem_row_size))) {
      SQL_ENG_LOG(WARN, "failed to add batch via store_mgr", K(ret), K(batch_size));
    } else {
      sort_resource_mgr_->alloc(inmem_row_size);
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
        if (OB_FAIL(after_add_row(sk_rows[i]))) {
          SQL_ENG_LOG(WARN, "after add row process failed", K(ret));
        }
      }
    }
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::add_sort_chunk(const int64_t level, ChunkType *&chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (OB_UNLIKELY(level < 0 || OB_ISNULL(chunk))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument: level is invalid or chunk is null", K(ret), K(level), KP(chunk));
  } else if (OB_ISNULL(sort_resource_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sort resource manager is null", K(ret));
  } else {
    // Rebind memory callback for externally provided chunks.
    //
    // DDL merge-sort chunks can outlive the SortImpl instance that originally created them
    // and may be processed by another thread later (thread-local SortImpl reuse).
    // The TempRowStore inside chunk keeps a raw pointer (ObSqlMemoryCallback*) to the
    // original sql_mem_processor. If that processor is reset/destroyed, later reads may
    // crash in ObTempBlockStore::inc_mem_hold().
    //
    // Binding the callback to *current* sort_impl's sql_mem_processor makes the pointer
    // always valid for the lifetime of the current merge operation.
    chunk->sort_row_store_mgr_.set_callback(&sort_resource_mgr_->get_sql_mem_processor());

    // In increase sort, chunk->level_ may less than the last of sort chunks.
    // insert the chunk to the upper bound the level.
    ObSortVecOpChunk<Store_Row, has_addon> *pos = sort_chunks_.get_last();
    for (; pos != sort_chunks_.get_header() && pos->level_ > level;
         pos = pos->get_prev()) {
    }
    pos = pos->get_next();
    if (!sort_chunks_.add_before(pos, chunk)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "add link node to list failed", K(ret));
    } else {
      chunk = nullptr;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::add_sort_chunks(const int64_t level, common::ObIArray<ChunkType *> &output_chunks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_chunks.count(); i++) {
    ChunkType *&chunk = output_chunks.at(i);
    if (OB_FAIL(add_sort_chunk(level, chunk))) {
      SQL_ENG_LOG(WARN, "failed to add sort chunk", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::get_merge_ways(const int64_t max_merge_ways, int64_t &merge_ways)
{
  int ret = OB_SUCCESS;
  merge_ways = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (OB_FAIL(sort_resource_mgr_->get_merge_ways_by_memory(&mem_context_->get_malloc_allocator(),
                                                                  tempstore_read_alignment_size_,
                                                                  max_merge_ways,
                                                                  merge_ways))) {
    SQL_ENG_LOG(WARN, "failed to get merge ways by memory", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::merge_sort_chunks(
    ObSortVecOpChunk<Store_Row, has_addon> *&output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not initialized", K(ret));
  } else if (sort_chunks_.get_size() < 2) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "need at least 2 chunks to merge", K(ret), K(sort_chunks_.get_size()));
  } else if (nullptr == external_sorter_
    && OB_ISNULL(external_sorter_ = OB_NEWx(ExternalMergeSorter, (&mem_context_->get_malloc_allocator()),
                                            mem_context_->get_malloc_allocator(),
                                            *comp_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    external_sorter_->reset();
    ObArray<ChunkType *> output_chunks;
    auto input = [&](const Store_Row *&sk_row, const Store_Row *&addon_row) {
      return external_sorter_->get_next_row(sk_row, addon_row);
    };
    const int64_t merge_ways = sort_chunks_.get_size();
    if (OB_FAIL(external_sorter_->init(sort_chunks_, merge_ways))) {
      SQL_ENG_LOG(WARN, "init external sorter failed", K(ret));
    } else if (OB_FAIL(build_chunk(0/*level*/, input, output_chunks))) {
      SQL_ENG_LOG(WARN, "failed to build chunk", K(ret));
    } else if (output_chunks.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected output chunks count", K(ret), K(output_chunks.count()));
      // Cleanup all created chunks on unexpected count
      for (int64_t i = 0; i < output_chunks.count(); i++) {
        ChunkType *chunk = output_chunks.at(i);
        if (nullptr != chunk) {
          chunk->~ChunkType();
          mem_context_->get_malloc_allocator().free(chunk);
        }
      }
      output_chunks.reset();
    } else {
      output_chunk = output_chunks[0];
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::write_row_to_vec(
    const RowMeta &row_meta, const Store_Row *row,
    const int64_t &row_index, const ObIArray<int64_t> &vec_capacities,
    ObIArray<ObIVector *> *&output_vec, bool &is_space_enough)
{
  int ret = OB_SUCCESS;
  is_space_enough = true;
  if (nullptr == row || nullptr == output_vec || row_index < 0) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(output_vec), KP(row), K(row_index), K(vec_capacities.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_space_enough && i < row_meta.col_cnt_; i++) {
      if (common::VEC_CONTINUOUS == output_vec->at(i)->get_format()) {
        common::ObContinuousBase *vec = static_cast<common::ObContinuousBase *>(output_vec->at(i));
        int64_t curr_offset = vec->get_offsets()[row_index];
        int64_t row_len = row->get_length(row_meta, i);
        // Defensive checks:
        // - row_len must be non-negative (negative length can lead to huge memcpy and memory corruption)
        // - avoid overflow in (curr_offset + row_len)
        // - avoid out-of-bounds write beyond vec buffer capacity
        if (OB_UNLIKELY(row_len < 0 || curr_offset < 0)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "invalid vector write length/offset", K(ret), K(i), K(row_index), K(curr_offset), K(row_len));
        } else if (curr_offset + row_len > vec_capacities.at(i)) {
          is_space_enough  = false;
        } else if (OB_FAIL(output_vec->at(i)->from_row(row_meta, row, row_index, i))) {
          SQL_ENG_LOG(WARN, "decode row to vector failed", K(ret), K(row_index), K(i));
        }
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::get_next_batch(
    int64_t &output_row_cnt, ObIArray<ObIVector *> *&output_sk_rows, ObIArray<ObIVector *> *&output_addon_rows)
{
  int ret = OB_SUCCESS;
  int64_t produced = 0;
  bool using_external_sort = false;
  int in_mem_chunk_cnt = (nullptr != in_memory_chunk_ && in_memory_chunk_->use_inmem_data_) ? 1 : 0;
  int sorted_chunk_cnt = sort_chunks_.get_size();
  /* check params valid, check in mem chunk & sort chunk stat valid */
  if (0 == (in_mem_chunk_cnt ^ (sorted_chunk_cnt > 0))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sort chunks is empty", K(ret), K(in_mem_chunk_cnt), K(sorted_chunk_cnt));
  } else {
    using_external_sort = sorted_chunk_cnt > 0;
  }

  /* build output vectors if needed */
  if (OB_FAIL(ret)) {
  } else if (nullptr == output_sk_rows_ && OB_FAIL(build_vector_array(*sk_row_meta_, output_sk_rows_, sk_vec_capacities_))) {
    SQL_ENG_LOG(WARN, "failed to build vector array", K(ret));
  } else if (has_addon && nullptr == output_addon_rows_ && OB_FAIL(build_vector_array(*addon_row_meta_, output_addon_rows_, addon_vec_capacities_))) {
    SQL_ENG_LOG(WARN, "failed to build vector array", K(ret));
  }

  /* create & init external sorter if needed */
  if (OB_FAIL(ret)) {
  } else if (using_external_sort && sorted_chunk_cnt > 1) {
    ObIAllocator *inner_allocator = &mem_context_->get_malloc_allocator();
    if (nullptr == external_sorter_ &&
        OB_ISNULL(external_sorter_ = OB_NEWx(ExternalMergeSorter, (inner_allocator), *inner_allocator, *comp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else if (!external_sorter_->is_inited() && OB_FAIL(external_sorter_->init(sort_chunks_, sort_chunks_.get_size()))) {
      SQL_ENG_LOG(WARN, "init external sorter failed", K(ret));
    }
  } else if (using_external_sort && sorted_chunk_cnt == 1) {
    // Initialize iterator for single chunk before first use if not already initialized.
    ChunkType *chunk = sort_chunks_.get_first();
    if (OB_NOT_NULL(chunk) && !chunk->sk_row_iter_.is_valid()) {
      chunk->reset_row_iter();
      if (OB_FAIL(chunk->init_row_iter())) {
        SQL_ENG_LOG(WARN, "init row iterator failed", K(ret));
      }
    }
  }

  /* get batch rows */
  while (OB_SUCC(ret) && produced < max_batch_size_) {
    const Store_Row *sk_row    = nullptr;
    const Store_Row *addon_row = nullptr;
    /* get row from last_row | external_sorter | memory_chunk */
    if (nullptr != last_sk_row_) {
      sk_row = last_sk_row_;
      addon_row = last_addon_row_;
      last_sk_row_ = nullptr;
      last_addon_row_ = nullptr;
    } else if (using_external_sort && sorted_chunk_cnt > 1) {
      if (OB_FAIL(external_sorter_->get_next_row(sk_row, addon_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          SQL_ENG_LOG(WARN, "get next row failed", K(ret));
        }
      }
    } else if (using_external_sort && sorted_chunk_cnt == 1) {
      if (OB_FAIL(sort_chunks_.get_first()->get_next_row())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          SQL_ENG_LOG(WARN, "get next row failed", K(ret));
        }
      } else {
        sk_row = sort_chunks_.get_first()->sk_row_;
        addon_row = has_addon ? sort_chunks_.get_first()->addon_row_ : nullptr;
      }
    } else if (!using_external_sort) {
      if (OB_FAIL(in_memory_chunk_->get_next_row())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            SQL_ENG_LOG(WARN, "get next row failed", K(ret));
          }
      } else {
        sk_row = in_memory_chunk_->sk_row_;
        addon_row = has_addon ? in_memory_chunk_->addon_row_ : nullptr;
      }
    }

    bool is_space_enough = true;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(sk_row) || (has_addon && OB_ISNULL(addon_row))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sort row is null", K(ret));
    } else if (is_space_enough && OB_FAIL(write_row_to_vec(*sk_row_meta_, sk_row, produced, sk_vec_capacities_, output_sk_rows_, is_space_enough))) {
      SQL_ENG_LOG(WARN, "write row to vector failed", K(ret));
    } else if (has_addon && is_space_enough &&
               OB_FAIL(write_row_to_vec(*addon_row_meta_, addon_row, produced, addon_vec_capacities_, output_addon_rows_, is_space_enough))) {
      SQL_ENG_LOG(WARN, "write row to vector failed", K(ret));
    } else if (is_space_enough) {
      ++produced;
    } else if (!is_space_enough) {
      last_sk_row_ = sk_row;
      last_addon_row_ = addon_row;
      break;
    }
  }

  if (OB_SUCC(ret)) {
    output_row_cnt = produced;
    output_sk_rows = output_sk_rows_;
    output_addon_rows = output_addon_rows_;
    if (0 == produced) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::sort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (!rows_.empty()) {
    // Handle in-memory data
    if (sort_chunks_.is_empty()) {
      // Pure in-memory sort: no external chunks exist
      if (OB_FAIL(sort_inmem_data(false/*need_force_dump*/))) {
        SQL_ENG_LOG(WARN, "sort in-memory data failed", K(ret));
      }
    } else {
      // External chunks exist, dump in-memory data to a new chunk
      if (OB_FAIL(do_dump())) {
        SQL_ENG_LOG(WARN, "dump failed", K(ret));
      }
    }
  }

  // sort_chunks_.get_size() == 0: Pure in-memory sort completed, sorted data is in in_memory_chunk_
  // sort_chunks_.get_size() == 1: Only one sorted chunk, no need to merge sort
  if (OB_SUCC(ret) && sort_chunks_.get_size() >= 2) {
    // Need merge sort
    int64_t merge_ways = 0;
    op_monitor_info_.otherstat_9_id_ = ObSqlMonitorStatIds::MERGE_SORT_START_TIME;
    op_monitor_info_.otherstat_9_value_ = ObTimeUtility::fast_current_time();
    // create external sorter if need
    if (nullptr == external_sorter_
      && OB_ISNULL(external_sorter_ = OB_NEWx(ExternalMergeSorter, (&mem_context_->get_malloc_allocator()),
                                              mem_context_->get_malloc_allocator(),
                                              *comp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    }

    // Intermediate merge rounds: reduce chunks until count <= MAX_MERGE_WAYS
    // This ensures the final merge round can handle all remaining chunks
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sort_resource_mgr_->calc_merge_ways(sort_chunks_,
                                                      &mem_context_->get_malloc_allocator(),
                                                      tempstore_read_alignment_size_,
                                                      merge_ways))) {
        SQL_ENG_LOG(WARN, "failed to calc merge ways", K(ret));
      } else if (merge_ways == sort_chunks_.get_size()) {
        // last merge round, no need to merge
        break;
      } else {
        // Execute merge: read from external_sorter_ and write to a new chunk
        external_sorter_->reset();
        ObArray<ChunkType *> output_chunks;
        const int64_t level = sort_chunks_.get_first()->level_ + 1;
        auto input = [this](const Store_Row *&sk_row, const Store_Row *&addon_row) {
          return external_sorter_->get_next_row(sk_row, addon_row);
        };

        // Initialize external sorter with first merge_ways chunks
        if (OB_FAIL(external_sorter_->init(sort_chunks_, merge_ways))) {
          SQL_ENG_LOG(WARN, "init external sorter failed", K(ret));
        } else if (OB_FAIL(build_chunk(level, input, output_chunks))) {
          SQL_ENG_LOG(WARN, "build chunk failed", K(ret));
        }
        // Remove merged chunks from the front of the list
        for (int64_t i = 0; OB_SUCC(ret) && i < merge_ways; i++) {
          ChunkType *c = sort_chunks_.remove_first();
          if (OB_NOT_NULL(c)) {
            c->~ChunkType();
            mem_context_->get_malloc_allocator().free(c);
          }
        }
        // Add newly generated chunk(s) to the list
        if (OB_SUCC(ret)) {
          if (OB_FAIL(add_sort_chunks(level, output_chunks))) {
            SQL_ENG_LOG(WARN, "failed to add sort chunks", K(ret));
          } else {
            SQL_ENG_LOG(DEBUG, "intermediate merge round completed",
                       K(level), K(merge_ways), K(sort_chunks_.get_size()));
          }
        }
        if (OB_FAIL(ret)) {
          for (int64_t i = 0; i < output_chunks.count(); i++) {
            ChunkType *chunk = output_chunks.at(i);
            if (nullptr != chunk) {
              chunk->~ChunkType();
              mem_context_->get_malloc_allocator().free(chunk);
            }
          }
          output_chunks.reset();
        }
      }
    }
  }
  return ret;
}

/* for func, build_vector_array, release_vector_array, release ivector
 * only continous format is supported
*/
template <typename Compare, typename Store_Row, bool has_addon>
void ObStorageVecSortImpl<Compare, Store_Row, has_addon>::release_ivector(common::ObIVector *&vec, ObIAllocator &allocator)
{
  if (nullptr == vec) {
    /* do nothing */
  } else {
    common::ObContinuousBase *typed_vec = static_cast<common::ObContinuousBase *>(vec);
    uint32_t *offsets = typed_vec->get_offsets();
    char *data = typed_vec->get_data();
    ObBitVector *nulls = typed_vec->get_nulls();
    typed_vec->common::ObContinuousBase::~ObContinuousBase();
    allocator.free(typed_vec);
    if (OB_NOT_NULL(offsets)) {
      allocator.free(offsets);
    }
    if (OB_NOT_NULL(data)) {
      allocator.free(data);
    }
    if (OB_NOT_NULL(nulls)) {
      allocator.free(nulls);
    }
    vec = nullptr;
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObStorageVecSortImpl<Compare, Store_Row, has_addon>::build_vector_array(
    const sql::RowMeta &row_meta, ObIArray<ObIVector *> *&vec_array, ObIArray<int64_t> &capacity_array)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(vec_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vec_array is not null", K(ret), KPC(vec_array));
  } else if (OB_UNLIKELY(max_batch_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max_batch_size is not initialized", K(ret), K(max_batch_size_));
  } else if (nullptr == mem_context_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_context is null", K(ret));
  } else {
    capacity_array.reset();
    ObIAllocator &allocator = mem_context_->get_malloc_allocator();
    if (OB_ISNULL(vec_array = OB_NEWx(common::ObArray<common::ObIVector *>, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc vector array failed", K(ret));
    } else if (OB_FAIL(capacity_array.reserve(row_meta.col_cnt_))) {
      LOG_WARN("reserve capacity array failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_meta.col_cnt_; ++i) {
        common::ObIVector *vec = nullptr;
        const common::VectorFormat format = common::VEC_CONTINUOUS;
        const common::VecValueTypeClass value_tc = common::VEC_TC_STRING;

        if (OB_FAIL(storage::ObDirectLoadVectorUtils::new_vector(format, value_tc, allocator, vec))) {
          LOG_WARN("failed to new vector", K(ret), K(format), K(value_tc));
        } else if (OB_FAIL(storage::ObDirectLoadVectorUtils::prepare_vector(vec, max_batch_size_, allocator))) {
          LOG_WARN("failed to prepare vector", K(ret), K(max_batch_size_));
        } else if (OB_FAIL(vec_array->push_back(vec))) {
          LOG_WARN("push back vector failed", K(ret));
          release_ivector(vec, allocator);
        } else {
          char *data = nullptr;
          common::ObContinuousBase *c_vec = static_cast<common::ObContinuousBase *>(vec);
          int64_t capacity = 64LL * 1024LL;
          if (row_meta.is_reordered_fixed_expr(i)) {
            capacity = min(max_batch_size_ * row_meta.fixed_length(i), capacity);
          }

          if (OB_ISNULL(data = static_cast<char *>(allocator.alloc(capacity)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc data buffer failed", K(ret), K(capacity));
          } else if (OB_FAIL(capacity_array.push_back(capacity))) {
            LOG_WARN("push back capacity failed", K(ret));
            allocator.free(data);
          } else {
            c_vec->set_data(data);
          }
        }
      }
    }
  }

  /* clean up if failed */
  if (OB_FAIL(ret) && OB_NOT_NULL(vec_array)) {
    release_vector_array(vec_array, capacity_array);
  }
  return ret;
}


template <typename Compare, typename Store_Row, bool has_addon>
void ObStorageVecSortImpl<Compare, Store_Row, has_addon>::release_vector_array(
    ObIArray<ObIVector *> *&vec_array, ObIArray<int64_t> &capacity_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_array)) {
    /* do nothing*/
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem_context is null", K(ret));
  } else {
    ObIAllocator &allocator = mem_context_->get_malloc_allocator();
    const int64_t vec_count = vec_array->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < vec_count; ++i) {
      if (OB_NOT_NULL(vec_array->at(i))) {
        release_ivector(vec_array->at(i), allocator);
      }
    }
    vec_array->~ObIArray<common::ObIVector *>();
    allocator.free(vec_array);
    vec_array = nullptr;
    capacity_array.reset();
  }
  return ;
}

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_OB_STORAGE_SORT_VEC_IMPL_H_ */
