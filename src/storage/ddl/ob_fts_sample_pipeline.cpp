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
#include "storage/ddl/ob_fts_sample_pipeline.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "share/vector/ob_i_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_vector_define.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_discrete_base.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/ddl/ob_ddl_sort_provider.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

// -------------------------------- ObFtsSampleBaseOperator --------------------------------
int ObFtsSampleBaseOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObFtsSampleBaseOperator::get_ddl_tablet_context(ObDDLTabletContext *&tablet_context)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = nullptr;
  tablet_context = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("operator not init", K(ret));
  } else if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag is null", K(ret));
  } else if (OB_FALSE_IT(dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
  } else if (OB_FAIL(dag->get_tablet_context(tablet_id_, tablet_context))) {
    LOG_WARN("get tablet context failed", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet context is null", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObFtsForwardInvertSampleOperator::init(const ObTabletID &tablet_id,
                                           const ObDDLTableSchema &ddl_table_schema,
                                           const ObDDLTableSchema &fts_word_doc_ddl_table_schema,
                                           const sql::RowMeta *sk_row_meta,
                                           const sql::RowMeta *sk_row_meta_inverted)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_ISNULL(sk_row_meta) || OB_ISNULL(sk_row_meta_inverted)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), KP(sk_row_meta), KP(sk_row_meta_inverted));
  } else {
    if (OB_ISNULL(mem_ctx_)) {
      lib::ContextParam param;
      param.set_mem_attr(ObMemAttr(MTL_ID(), "FtsSampleOp", ObCtxIds::WORK_AREA)).set_properties(lib::ALLOC_THREAD_SAFE);
      if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_ctx_, param))) {
        LOG_WARN("create memory context failed", K(ret));
      } else if (OB_ISNULL(mem_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null memory context", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(sort_impl_forward_)) {
      sort_impl_forward_ = OB_NEWx(SortImpl, &mem_ctx_->get_malloc_allocator(), mem_ctx_, op_monitor_info_);
      if (OB_ISNULL(sort_impl_forward_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new sort_impl_forward_", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(sort_impl_inverted_)) {
      sort_impl_inverted_ = OB_NEWx(SortImpl, &mem_ctx_->get_malloc_allocator(), mem_ctx_, op_monitor_info_);
      if (OB_ISNULL(sort_impl_inverted_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new sort_impl_inverted_", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      forward_compare_ = Compare();
      inverted_compare_ = Compare();
      const ObCompressorType forward_compressor_type = ddl_table_schema.table_item_.compress_type_;
      const ObCompressorType inverted_compressor_type = fts_word_doc_ddl_table_schema.table_item_.compress_type_;
      if (OB_FAIL(forward_compare_.init(&ddl_table_schema, sk_row_meta, false, true /*rowkey_only*/))) {
        LOG_WARN("failed to init forward compare", K(ret));
      } else if (OB_FAIL(sort_impl_forward_->init(&forward_compare_, sk_row_meta, nullptr /*addon_row_meta*/, FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE,
                                       lib::ObMemAttr(MTL_ID(), "FtsSampleOp"),
                                       forward_compressor_type, false, 0, MTL_ID(), false, nullptr))) {
        LOG_WARN("fail to initialize forward sort operator", K(ret));
      } else if (OB_FAIL(inverted_compare_.init(&fts_word_doc_ddl_table_schema, sk_row_meta_inverted, false, true /*rowkey_only*/))) {
        LOG_WARN("failed to init inverted compare", K(ret));
      } else if (OB_FAIL(sort_impl_inverted_->init(&inverted_compare_, sk_row_meta_inverted, nullptr /*addon_row_meta*/, FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE,
                                       lib::ObMemAttr(MTL_ID(), "FtsSampleOp"),
                                       inverted_compressor_type, false, 0, MTL_ID(), false, nullptr))) {
        LOG_WARN("fail to initialize inverted sort operator", K(ret));
      } else if (OB_FAIL(selector_.reserve(FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE))) {
        LOG_WARN("failed to reserve selector array", K(ret), K(FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE));
      } else {
        selector_.reuse();
        for (int64_t i = 0; OB_SUCC(ret) && i < FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE; i++) {
          if (OB_FAIL(selector_.push_back(static_cast<uint16_t>(i)))) {
            LOG_WARN("failed to push back selector", K(ret), K(i));
          }
        }
        tablet_id_ = tablet_id;
        ddl_table_schema_ = ddl_table_schema;
        fts_word_doc_ddl_table_schema_ = fts_word_doc_ddl_table_schema;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

ObFtsForwardInvertSampleOperator::~ObFtsForwardInvertSampleOperator()
{
  if (nullptr != sort_impl_forward_) {
    sort_impl_forward_->~SortImpl();
    if (mem_ctx_ != nullptr) {
      mem_ctx_->get_malloc_allocator().free(sort_impl_forward_);
    }
    sort_impl_forward_ = nullptr;
  }
  if (nullptr != sort_impl_inverted_) {
    sort_impl_inverted_->~SortImpl();
    if (mem_ctx_ != nullptr) {
      mem_ctx_->get_malloc_allocator().free(sort_impl_inverted_);
    }
    sort_impl_inverted_ = nullptr;
  }
  if (nullptr != mem_ctx_) {
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = nullptr;
  }
}

int ObFtsForwardInvertSampleOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  output_chunk.reset();
  if (input_chunk.is_end_chunk()) {
    // do nothing
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
      LOG_WARN("get tablet context from chunk failed", K(ret));
    } else if (OB_ISNULL(tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret));
    } else if (OB_FAIL(build_final_samples(tablet_context->get_forward_sample_ranges(),
                                           false /*is_inverted*/,
                                           *tablet_context))) {
      LOG_WARN("build forward final samples failed", K(ret));
    } else if (OB_FAIL(build_final_samples(tablet_context->get_inverted_sample_ranges(),
                                           true /*is_inverted*/,
                                           *tablet_context))) {
      LOG_WARN("build inverted final samples failed", K(ret));
    } else {
      output_chunk = input_chunk;
    }
  }
  FLOG_INFO("build final samples finished", K(ret), K(tablet_id_.id()));
  return ret;
}

int ObFtsForwardInvertSampleOperator::build_final_samples(
    const common::Ob2DArray<sql::ObPxTabletRange> &local_ranges,
    const bool is_inverted,
    ObDDLTabletContext &tablet_ctx)
{
  int ret = OB_SUCCESS;
  int64_t total_rows = 0;
  const int64_t expect_sampling_count = tablet_ctx.get_expect_range_count();
  ObStorageVecSortImpl<Compare, sql::ObSortKeyStore<false>, false> *sort_impl = is_inverted ? sort_impl_inverted_ : sort_impl_forward_;
  if (OB_ISNULL(sort_impl)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sort impl not init", K(ret), K(is_inverted));
  } else if (local_ranges.count() == 0) {
    // where there is no sample, granule will split task with range (min, max)
    LOG_INFO("no local sample ranges to aggregate", K(ret), K(is_inverted));
  } else {
    common::ObSEArray<const sql::ObPxTabletRange::DatumKey *, 64> all_key_refs;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_ranges.count(); ++i) {
      const sql::ObPxTabletRange &range = local_ranges.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < range.range_cut_.count(); ++j) {
        if (OB_FAIL(all_key_refs.push_back(&range.range_cut_.at(j)))) {
          LOG_WARN("push back key ref failed", K(ret), K(i), K(j));
        }
      }
    }
    total_rows = all_key_refs.count();

    for (int64_t i = 0; OB_SUCC(ret) && i < total_rows; i += FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE) {
      int64_t batch_size = std::min<int64_t>(FTS_SAMPLE_DEFAULT_ADD_BATCH_SIZE, total_rows - i);
      common::ObSEArray<const sql::ObPxTabletRange::DatumKey *, 64> batch_key_refs;
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (OB_FAIL(batch_key_refs.push_back(all_key_refs.at(i + j)))) {
          LOG_WARN("push back batch key ref failed", K(ret), K(j));
        }
      }
      common::ObFixedArray<common::ObIVector *, common::ObIAllocator> sk_vec_ptrs;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(convert_keys_to_vectors(batch_key_refs, is_inverted, batch_size, sk_vec_ptrs, tablet_ctx.arena_))) {
        LOG_WARN("convert batch keys to vectors failed", K(ret), K(i), K(batch_size));
      } else {
        // Prepare sk_rows for add_batch
        sk_rows_.reuse();
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; j++) {
          if (OB_FAIL(sk_rows_.push_back(nullptr))) {
            LOG_WARN("failed to push back sk_row", K(ret), K(j));
          }
        }
        int64_t inmem_row_size = 0;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sort_impl->add_batch(sk_vec_ptrs, nullptr, selector_.get_data(), batch_size, sk_rows_.get_data(), nullptr, inmem_row_size))) {
            LOG_WARN("add batch failed", K(ret), K(i), K(batch_size));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sort_impl->sort())) {
      LOG_WARN("sort failed", K(ret));
    } else if (OB_FAIL(sample_and_merge_sorted_data(sort_impl, total_rows, expect_sampling_count, is_inverted, tablet_ctx, local_ranges))) {
      LOG_WARN("sample and merge sorted data failed", K(ret));
    }
  }
  return ret;
}

int ObFtsForwardInvertSampleOperator::sample_and_merge_sorted_data(
    SortImpl *sort_impl,
    const int64_t total_rows,
    const int64_t expect_sampling_count,
    const bool is_inverted,
    ObDDLTabletContext &tablet_ctx,
    const common::Ob2DArray<sql::ObPxTabletRange> &local_ranges)
{
  int ret = OB_SUCCESS;
  sql::ObPxTabletRange merged_range;
  merged_range.tablet_id_ = tablet_ctx.tablet_id_.id();
  const int64_t step = std::max<int64_t>(1, total_rows / expect_sampling_count);
  merged_range.range_weights_ = step;
  int64_t sampled_cnt = 0;
  int64_t idx = 0;
  bool iter_end = false;
  ObIArray<ObIVector *> *sk_vec_ptrs_batch = nullptr;
  ObIArray<ObIVector *> *add_on_vec_ptrs_batch = nullptr;
  while (OB_SUCC(ret) && !iter_end) {
    int64_t output_row_cnt = 0;
    if (OB_FAIL(sort_impl->get_next_batch(output_row_cnt, sk_vec_ptrs_batch, add_on_vec_ptrs_batch))) {
      if (OB_ITER_END == ret) {
        iter_end = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_row_cnt; ++i, ++idx) {
        if (((idx + 1) % step) == 0) {
          sql::ObPxTabletRange::DatumKey copied_key;
          for (int64_t col = 0; OB_SUCC(ret) && col < sk_vec_ptrs_batch->count(); ++col) {
            ObDatum cell;
            if (OB_FAIL(ObDirectLoadVectorUtils::to_datum(sk_vec_ptrs_batch->at(col), i, cell))) {
              LOG_WARN("fail to convert vector cell to datum", K(ret), K(col), K(i));
            } else {
              ObDatum copied_cell;
              if (OB_FAIL(copied_cell.deep_copy(cell, tablet_ctx.arena_))) {
                LOG_WARN("deep copy datum failed", K(ret), K(col), K(i));
              } else if (OB_FAIL(copied_key.push_back(copied_cell))) {
                copied_cell.~ObDatum();
                tablet_ctx.arena_.free(const_cast<char *>(copied_cell.ptr_));
                copied_cell.ptr_ = nullptr;
                LOG_WARN("push back datum to key failed", K(ret), K(col), K(i));
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(merged_range.range_cut_.push_back(copied_key))) {
              LOG_WARN("push back merged key failed", K(ret), K(idx));
            } else {
              ++sampled_cnt;
            }
          }

          if (OB_FAIL(ret)) {
            for (int64_t j = 0; j < copied_key.count(); j++) {
              if (OB_NOT_NULL(copied_key.at(j).ptr_)) {
                copied_key.at(j).~ObDatum();
                tablet_ctx.arena_.free(const_cast<char *>(copied_key.at(j).ptr_));
                copied_key.at(j).ptr_ = nullptr;
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_ctx.set_final_sample_range(is_inverted, merged_range))) {
      LOG_WARN("set final sample range failed", K(ret));
    } else {
      FLOG_INFO("fts sample ranges aggregated",
               K(is_inverted),
               "local_range_cnt", local_ranges.count(),
               "flatten_key_cnt", total_rows,
               "final_cut_cnt", merged_range.range_cut_.count(),
               K(expect_sampling_count), K(merged_range));
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < merged_range.range_cut_.count(); i++) {
      sql::ObPxTabletRange::DatumKey &key = merged_range.range_cut_.at(i);
      for (int64_t j = 0; j < key.count(); j++) {
        if (OB_NOT_NULL(key.at(j).ptr_)) {
          key.at(j).~ObDatum();
          tablet_ctx.arena_.free(const_cast<char *>(key.at(j).ptr_));
          key.at(j).ptr_ = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObFtsForwardInvertSampleOperator::create_output_vectors_for_sort(
    const bool is_inverted,
    const int64_t batch_size,
    common::ObIAllocator &allocator,
    common::ObIArray<common::ObIVector *> &output_vectors)
{
  int ret = OB_SUCCESS;
  output_vectors.reset();

  const ObDDLTableSchema &ddl_table_schema = is_inverted ? fts_word_doc_ddl_table_schema_ : ddl_table_schema_;

  common::ObSEArray<const ObColumnSchemaItem *, 4> rowkey_col_items;
  for (int64_t i = 0; OB_SUCC(ret) && i < ddl_table_schema.column_items_.count(); ++i) {
    const ObColumnSchemaItem &col_item = ddl_table_schema.column_items_.at(i);
    if (col_item.is_rowkey_column_) {
      if (OB_FAIL(rowkey_col_items.push_back(&col_item))) {
        LOG_WARN("push back rowkey col item failed", K(ret), K(i));
      }
    }
  }

  const int64_t col_count = rowkey_col_items.count();
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_count; ++col_idx) {
    const ObColumnSchemaItem &col_item = *rowkey_col_items.at(col_idx);

    VecValueTypeClass value_tc = get_vec_value_tc(
        col_item.col_type_.get_type(),
        col_item.col_accuracy_.get_scale(),
        col_item.col_accuracy_.get_precision());
    VectorFormat format = is_fixed_length_vec(value_tc) ? VEC_FIXED : VEC_DISCRETE;

    common::ObIVector *vector = nullptr;
    if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(format, value_tc, allocator, vector))) {
      LOG_WARN("fail to create output vector", K(ret), K(col_idx), K(format), K(value_tc));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(vector, batch_size, allocator))) {
      LOG_WARN("fail to prepare output vector", K(ret), K(col_idx), K(batch_size));
    } else if (OB_FAIL(output_vectors.push_back(vector))) {
      LOG_WARN("fail to push back output vector", K(ret), K(col_idx));
    }
  }
  return ret;
}

int ObFtsForwardInvertSampleOperator::convert_keys_to_vectors(
    const common::ObIArray<const sql::ObPxTabletRange::DatumKey *> &key_refs,
    const bool is_inverted,
    const int64_t total_rows,
    common::ObFixedArray<common::ObIVector *, common::ObIAllocator> &sk_vec_ptrs,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  sk_vec_ptrs.reset();
  sk_vec_ptrs.set_allocator(&allocator);
  common::ObSEArray<common::ObIVector *, 4> temp_vec_ptrs;
  int col_count = 0;
  if (OB_UNLIKELY(total_rows < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("total rows is 0", K(ret), K(total_rows));
  } else {
    col_count = key_refs.at(0)->count();
  }

  if (OB_FAIL(ret)){
  } else if (key_refs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key refs is empty", K(ret));
  } else if (OB_UNLIKELY(col_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column count", K(ret), K(col_count));
  } else if (OB_FAIL(sk_vec_ptrs.init(col_count))) {
    LOG_WARN("init sk_vec_ptrs failed", K(ret), K(col_count));
  } else if (OB_FAIL(create_output_vectors_for_sort(is_inverted, total_rows, allocator, temp_vec_ptrs))) {
    LOG_WARN("fail to create output vectors for sort", K(ret), K(is_inverted));
  }

  //fill vector with datum
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_count; ++col_idx) {
    common::ObIVector *vector = temp_vec_ptrs.at(col_idx);
    VectorFormat format = vector->get_format();

    if (format == VEC_FIXED) {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vector);
      const ObLength fixed_len = fixed_vec->get_length();
      char *data = fixed_vec->get_data();

      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < total_rows; ++row_idx) {
        const sql::ObDatum &datum = key_refs.at(row_idx)->at(col_idx);
        if (OB_UNLIKELY(datum.len_ != fixed_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum length mismatch", K(ret), K(datum.len_), K(fixed_len));
        } else {
          MEMCPY(data + fixed_len * row_idx, datum.ptr_, fixed_len);
        }
      }
    } else {
      ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *>(vector);
      ObLength *lens = discrete_vec->get_lens();
      char **ptrs = discrete_vec->get_ptrs();

      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < total_rows; ++row_idx) {
        const sql::ObDatum &datum = key_refs.at(row_idx)->at(col_idx);
        char *buf = static_cast<char *>(allocator.alloc(datum.len_));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), K(datum.len_));
        } else {
          MEMCPY(buf, datum.ptr_, datum.len_);
          lens[row_idx] = datum.len_;
          ptrs[row_idx] = buf;
        }
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < total_rows; ++i) {
          if (OB_NOT_NULL(ptrs[i])) {
            allocator.free(ptrs[i]);
            ptrs[i] = nullptr;
            lens[i] = 0;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sk_vec_ptrs.push_back(vector))) {
      LOG_WARN("push back vector failed", K(ret), K(col_idx));
    }
  }
  return ret;
}

int ObFtsWriteInnerTableOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  output_chunk.reset();
  if (input_chunk.is_end_chunk()) {
    // do nothing
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
      LOG_WARN("get tablet context from chunk failed", K(ret));
    } else if (OB_ISNULL(tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret));
    } else {
      // Get DAG and ddl_task_id
      share::ObIDag *dag = get_dag();
      if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag is null", K(ret));
      } else {
        storage::ObColumnClusteredDag *column_dag = static_cast<storage::ObColumnClusteredDag *>(dag);
        const int64_t ddl_task_id = column_dag->get_ddl_task_param().ddl_task_id_;
        const uint64_t tenant_id = MTL_ID();

        if (ddl_task_id <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl_task_id is invalid", K(ret), K(ddl_task_id));
        } else {
          rootserver::ObDDLSliceInfo ddl_slice_info;
          if (OB_FAIL(build_ddl_slice_info(tablet_context, ddl_task_id, ddl_slice_info))) {
            LOG_WARN("build ddl slice info failed", K(ret));
          } else if (!ddl_slice_info.is_valid() && !ddl_slice_info.is_inverted_valid()) {
            // where there is no sample, we skip the update, granule will split task with range (min, max)
            LOG_INFO("ddl slice info is not valid", K(ret), K(ddl_slice_info));
          } else if (!ddl_slice_info.is_valid() || !ddl_slice_info.is_inverted_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ddl slice info is not valid", K(ret), K(ddl_slice_info));
          } else {
            bool is_idempotent_mode = false;
            bool is_update = false;
            if (OB_FAIL(rootserver::ObDDLTaskRecordOperator::get_or_insert_schedule_info(
                tenant_id, ddl_task_id, allocator_, ddl_slice_info, is_idempotent_mode, is_update))) {
              LOG_WARN("get or insert schedule info failed", K(ret), K(tenant_id), K(ddl_task_id), K(tablet_id_));
            } else {
              bool found_forward = false;
              bool found_inverted = false;
              column_dag->set_need_update_tablet_range_count(is_update);
              for (int64_t i = 0; OB_SUCC(ret) && !found_forward && i < ddl_slice_info.part_ranges_.count(); ++i) {
                const sql::ObPxTabletRange &range = ddl_slice_info.part_ranges_.at(i);
                if (range.tablet_id_ == tablet_id_.id()) {
                  if (OB_FAIL(tablet_context->set_final_sample_range(false /*is_inverted*/, range))) {
                    LOG_WARN("set forward final sample range from persistent data failed", K(ret), K(range));
                  } else {
                    found_forward = true;
                  }
                }
              }

              for (int64_t i = 0; OB_SUCC(ret) && !found_inverted && i < ddl_slice_info.inverted_part_ranges_.count(); ++i) {
                const sql::ObPxTabletRange &range = ddl_slice_info.inverted_part_ranges_.at(i);
                if (range.tablet_id_ == tablet_id_.id()) {
                  if (OB_FAIL(tablet_context->set_final_sample_range(true /*is_inverted*/, range))) {
                    LOG_WARN("set inverted final sample range from persistent data failed", K(ret), K(range));
                  } else {
                    found_inverted = true;
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (!found_forward && !found_inverted) {
                  LOG_INFO("no durable slice range in inner table");
                } else if (!found_forward || !found_inverted) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("found forward or inverted range failed", K(ret), K(found_forward), K(found_inverted));
                }
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      output_chunk = input_chunk;
    }
  }
  FLOG_INFO("write inner table execute finished", K(ret), K(tablet_id_.id()));
  return ret;
}

int ObFtsWriteInnerTableOperator::build_ddl_slice_info(
    ObDDLTabletContext *tablet_context,
    const int64_t ddl_task_id,
    rootserver::ObDDLSliceInfo &ddl_slice_info)
{
  int ret = OB_SUCCESS;
  ddl_slice_info.reset();

  if (OB_ISNULL(tablet_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet context is null", K(ret));
  } else if (ddl_task_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ddl task id is invalid", K(ret), K(ddl_task_id));
  } else {
    const sql::ObPxTabletRange &forward_range = tablet_context->get_forward_final_sample_range();
    const sql::ObPxTabletRange &inverted_range = tablet_context->get_inverted_final_sample_range();
    if (forward_range.range_cut_.count() > 0) {
      sql::ObPxTabletRange tablet_range;
      tablet_range.tablet_id_ = tablet_id_.id();
      if (OB_FAIL(tablet_range.assign(forward_range))) {
        LOG_WARN("assign forward range failed", K(ret), K(forward_range));
      } else if (OB_FAIL(ddl_slice_info.part_ranges_.push_back(tablet_range))) {
        LOG_WARN("push back forward range failed", K(ret), K(tablet_range));
      }
    }

    if (OB_SUCC(ret) && inverted_range.range_cut_.count() > 0) {
      sql::ObPxTabletRange tablet_range;
      tablet_range.tablet_id_ = tablet_id_.id();
      if (OB_FAIL(tablet_range.assign(inverted_range))) {
        LOG_WARN("assign inverted range failed", K(ret), K(inverted_range));
      } else if (OB_FAIL(ddl_slice_info.inverted_part_ranges_.push_back(tablet_range))) {
        LOG_WARN("push back inverted range failed", K(ret), K(tablet_range));
      }
    }
  }
  return ret;
}

int ObFtsSamplePipeline::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  ObDDLIndependentDag *ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag());
  if (OB_ISNULL(ddl_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else if (OB_FAIL(init_sk_row_meta(false /*is_inverted*/))){
    LOG_WARN("init forward sk row meta failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(init_sk_row_meta(true /*is_inverted*/))) {
    LOG_WARN("init inverted sk row meta failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(final_sample_op_.init(tablet_id, ddl_dag->get_ddl_table_schema(), ddl_dag->get_fts_word_doc_ddl_table_schema(), &sk_row_meta_, &sk_row_meta_inverted_))) {
    LOG_WARN("init final sample operator failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(write_inner_table_op_.init(tablet_id))) {
    LOG_WARN("init write inner table operator failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(add_op(&final_sample_op_))) {
    LOG_WARN("add final sample operator failed", K(ret));
  } else if (OB_FAIL(add_op(&write_inner_table_op_))) {
    LOG_WARN("add write inner table operator failed", K(ret));
  }

  FLOG_INFO("fts sample pipeline init", K(ret), K(tablet_id_.id()));
  return ret;
}

ObITask::ObITaskPriority ObFtsSamplePipeline::get_priority()
{
  int ret = OB_SUCCESS;
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    ObColumnClusteredDag *column_dag = static_cast<ObColumnClusteredDag *>(get_dag());
    priority = column_dag->is_sample_scan_finished() ? ObITask::TASK_PRIO_2 : ObITask::TASK_PRIO_0;
  }
  return priority;
}

int ObFtsSamplePipeline::get_next_chunk(ObChunk *&next_chunk)
{
  int ret = OB_SUCCESS;
  next_chunk = nullptr;
  if (is_chunk_generated_) {
    chunk_.type_ = ObChunk::ITER_END_TYPE;
    next_chunk = &chunk_;
  } else {
    chunk_.type_ = ObChunk::DAG_TABLET_CONTEXT;
    ObDDLTabletContext *tablet_context = nullptr;
    ObDDLIndependentDag *dag = nullptr;
    if (OB_ISNULL(get_dag())) {
      ret = OB_ERR_SYS;
      LOG_WARN("dag is null", K(ret));
    } else if (OB_FALSE_IT(dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
    } else if (OB_FAIL(dag->get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("get tablet context failed", K(ret), K(tablet_id_));
    } else {
      chunk_.data_ptr_ = tablet_context;
      next_chunk = &chunk_;
      is_chunk_generated_ = true;
    }
  }
  return ret;
}

void ObFtsSamplePipeline::postprocess(int &ret_code)
{
  if (OB_UNLIKELY(OB_ITER_END != ret_code && OB_DAG_TASK_IS_SUSPENDED != ret_code)) {
    FLOG_INFO("ret code not expected", K(ret_code), KPC(this), KPC(get_dag()));
  } else if (OB_LIKELY(OB_ITER_END == ret_code)) {
    ret_code = OB_SUCCESS;
  }

  if (OB_NOT_NULL(get_dag())) {
    int ret = OB_SUCCESS;
    ObColumnClusteredDag *column_dag = static_cast<ObColumnClusteredDag *>(get_dag());
    if (OB_FAIL(column_dag->notify_sample_finished())) {
      column_dag->set_ret_code(ret);
      LOG_WARN("notify sample finished failed", K(ret));
    }
  }
}

int ObFtsSamplePipeline::init_sk_row_meta(const bool is_inverted)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag());
  if (OB_ISNULL(ddl_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl dag is null", K(ret));
  } else {
    const ObDDLTableSchema &ddl_table_schema = is_inverted ? ddl_dag->get_fts_word_doc_ddl_table_schema() : ddl_dag->get_ddl_table_schema();
    sql::RowMeta &row_meta = is_inverted ? sk_row_meta_inverted_ : sk_row_meta_;
    row_meta.reset();
    if (OB_FAIL(ObDDLSortProvider::build_row_meta_from_schema(ddl_table_schema, row_meta_alloc_, row_meta))) {
      LOG_WARN("fail to build row meta", K(ret));
    }
  }
  return ret;
}
