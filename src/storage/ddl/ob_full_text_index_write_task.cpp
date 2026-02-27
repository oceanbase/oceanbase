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
#include "storage/ddl/ob_full_text_index_write_task.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/ddl/ob_ddl_encode_sortkey_utils.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "common/ob_tablet_id.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "share/vector/ob_vector_define.h"
#include "share/datum/ob_datum.h"
#include "share/ob_order_perserving_encoder.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::sql;

// -------------------------------- ObInvertedIndexSortFlushOperator --------------------------------
int ObInvertedIndexSortFlushOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObDDLTableSchema *ddl_table_schema = nullptr;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret));
  } else if (OB_FAIL(selector_array_.reserve(ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE))) {
    LOG_WARN("failed to reserve selector array", K(ret), K(ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE));
  } else if (OB_FAIL((static_cast<ObDDLIndependentDag *>(get_dag()))->check_enable_encode_sortkey(enable_encode_sortkey_))) {
    LOG_WARN("failed to check can encode sortkey", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE; i++) {
      if (OB_FAIL(selector_array_.push_back(static_cast<uint16_t>(i)))) {
        LOG_WARN("failed to push back selector", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id_ = tablet_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObInvertedIndexSortFlushOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  ObDDLSortProvider::SortImpl *sort_impl = nullptr;
  ObDDLIndependentDag *ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag());
  int64_t slice_idx = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(pipeline_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pipeline is null", K(ret));
  } else if (OB_FAIL(static_cast<ObFullTextIndexWritePipeline *>(pipeline_)->get_slice_idx(slice_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get slice idx", K(ret), KP(pipeline_));
  } else if (OB_ISNULL(ddl_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", K(ret));
  } else if (OB_ISNULL(ddl_dag->get_sort_provider())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort provider is null", K(ret));
  } else if (OB_FAIL(ddl_dag->get_sort_provider()->get_sort_impl(slice_idx, sort_impl))) {
    LOG_WARN("get sort impl failed", K(ret));
  } else if (OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id_));
  } else if (input_chunk.is_end_chunk()) {
    result_state = ObPipelineOperator::NEED_MORE_INPUT;
    output_chunk.set_end_chunk();
    if (OB_FAIL(deliver_sorted_chunks(ddl_dag, sort_impl))) {
      LOG_WARN("failed to deliver sorted chunks", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(ddl_dag) && OB_NOT_NULL(ddl_dag->get_sort_provider())) {
      if (OB_TMP_FAIL(ddl_dag->get_sort_provider()->set_sort_handle_status(slice_idx, false/*is_in_use*/))) {
        LOG_WARN("failed to release sort impl", K(tmp_ret), K(slice_idx));
      }
    }
    if (OB_TMP_FAIL(ddl_dag->get_sort_provider()->finish_sort_impl(slice_idx))) {
      LOG_WARN("failed to reset sort impl", K(tmp_ret), K(slice_idx));
    }
    ret = ret == OB_SUCCESS ? tmp_ret : ret;
  } else if (OB_UNLIKELY(!input_chunk.is_valid() || !input_chunk.is_direct_load_batch_datum_rows_type() || OB_ISNULL(input_chunk.bdrs_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(input_chunk));
  } else if (OB_FAIL(add_unsorted_chunks(input_chunk, sort_impl))) {
    LOG_WARN("failed to add unsorted chunks", K(ret));
  } else {
    // 非 end_chunk 时，将 output_chunk 设置成 input_chunk，传递给下一个 operator
    output_chunk.type_ = input_chunk.type_;
    output_chunk.direct_load_batch_rows_ = input_chunk.direct_load_batch_rows_;
  }
  if (!input_chunk.is_end_chunk()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(ddl_dag) && OB_NOT_NULL(ddl_dag->get_sort_provider())) {
      if (OB_TMP_FAIL(ddl_dag->get_sort_provider()->set_sort_handle_status(slice_idx, false/*is_in_use*/))) {
        LOG_WARN("failed to release sort impl", K(tmp_ret), K(slice_idx));
      }
      ret = ret == OB_SUCCESS ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObInvertedIndexSortFlushOperator::deliver_sorted_chunks(ObDDLIndependentDag *ddl_dag,
                                                            ObDDLSortProvider::SortImpl *sort_impl)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObSortVecOpChunk<sql::ObSortKeyStore<false>, false> *> sort_chunks;
  if (OB_ISNULL(sort_impl) || OB_ISNULL(ddl_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort impl is null", K(ret));
  } else if (OB_FAIL(sort_impl->sort_inmem_data(true/*need_force_dump*/))) {
    LOG_WARN("failed to sort inmem data", K(ret));
  } else if (OB_FAIL(sort_impl->get_sort_chunks(sort_chunks))) {
    LOG_WARN("failed to get sort chunks", K(ret));
  } else {
    // 获取 tablet_context
    ObDDLTabletContext *tablet_context = nullptr;
    ObIAllocator *chunk_allocator = sort_impl->get_allocator();
    if (OB_FAIL(ddl_dag->get_tablet_context(tablet_id_, tablet_context))) {
      LOG_WARN("get tablet context failed", K(ret), K(tablet_id_));
    } else if (OB_ISNULL(tablet_context)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet context is null", K(ret), K(tablet_id_));
    } else {
      ObDDLSlice *ddl_slice = nullptr;
      bool is_new_slice = false;

      // 遍历 sort_chunks，将每个 chunk 添加到 slice 中
      if (OB_ISNULL(chunk_allocator)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("chunk allocator is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sort_chunks.count(); i++) {
          ObSortVecOpChunk<sql::ObSortKeyStore<false>, false> *&chunk = sort_chunks.at(i);
          if (OB_ISNULL(chunk)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sort chunk is null", K(ret), K(i));
          } else {
            if (OB_ISNULL(ddl_slice) || chunk->slice_id_ != ddl_slice->get_slice_idx()) {
              if (OB_FAIL(tablet_context->get_or_create_slice(chunk->slice_id_, ddl_slice, is_new_slice))) {
                LOG_WARN("get or create slice failed", K(ret), K(chunk->slice_id_));
              } else if (OB_ISNULL(ddl_slice)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("ddl slice is null", K(ret), K(chunk->slice_id_));
              }
            }
            if (OB_SUCC(ret)) {
              const int64_t file_size = chunk->get_file_size();
              if (OB_FAIL(ddl_slice->push_sorted_chunk(chunk, file_size, chunk_allocator))) {
                LOG_WARN("push sorted chunk failed", K(ret), K(i), K(file_size));
              } else {
                LOG_TRACE("add sort chunk", K(ddl_slice->get_tablet_id()), K(ddl_slice->get_slice_idx()));
              }
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < sort_chunks.count(); i++) {
        ObSortVecOpChunk<sql::ObSortKeyStore<false>, false> *&chunk = sort_chunks.at(i);
        if (OB_NOT_NULL(chunk)) {
          chunk->~ObSortVecOpChunk<sql::ObSortKeyStore<false>, false>();
          chunk_allocator->free(chunk);
          chunk = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObInvertedIndexSortFlushOperator::add_unsorted_chunks(const ObChunk &input_chunk,
                                                          ObDDLSortProvider::SortImpl *sort_impl)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObBatchDatumRows &bdrs = input_chunk.direct_load_batch_rows_->datum_rows_;
  const int64_t batch_size = bdrs.row_count_;
  const int64_t doc_id_idx_at_vectors = 0;
  const int64_t word_idx_at_vectors = 1;

  if (OB_UNLIKELY(batch_size <= 0)) {
    // 空批次，直接返回
  } else if (OB_UNLIKELY(bdrs.vectors_.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch datum rows, column count less than 2", K(ret), K(bdrs.vectors_.count()));
  } else {
    if (OB_UNLIKELY(batch_size > ObDDLIndependentDag::DEFAULT_ROW_BUFFER_SIZE)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to expand selector array", K(ret), K(batch_size));
    } else {
      // 构造排序键向量数组（前两或三列：变长列和定长列，可能还有encode列）
      allocator_.reuse();
      common::ObFixedArray<common::ObIVector *, common::ObIAllocator> sk_vec_ptrs(allocator_);
      ObDirectLoadBatchRows encode_sortkeys;
      if (OB_FAIL(sk_vec_ptrs.reserve(bdrs.vectors_.count() + (enable_encode_sortkey_ ? 1 : 0)))) {
        LOG_WARN("failed to init sk_vec_ptrs", K(ret));
      } else {
        if (enable_encode_sortkey_) {
          const bool is_oracle_mode = Worker::CompatMode::ORACLE == (static_cast<ObDDLIndependentDag *>(get_dag()))->get_compat_mode();
          const ObDDLTableSchema *ddl_table_schema = nullptr;
          ObSEArray<int64_t, 2> sortkey_idxs;
          ObSEArray<ObObjMeta, 2> column_descs;
          if (OB_FAIL((static_cast<ObDDLIndependentDag *>(get_dag()))->get_sort_ddl_table_schema(ddl_table_schema))) {
            LOG_WARN("failed to get sort ddl table schema", K(ret));
          } else if (OB_FAIL(sortkey_idxs.push_back(word_idx_at_vectors))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(sortkey_idxs.push_back(doc_id_idx_at_vectors))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(column_descs.push_back(ddl_table_schema->column_descs_.at(0).col_type_))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(column_descs.push_back(ddl_table_schema->column_descs_.at(1).col_type_))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(ObDDLEncodeSortkeyUtils::encode_batch(bdrs, sortkey_idxs, column_descs, is_oracle_mode, allocator_, encode_sortkeys))) {
            LOG_WARN("failed to encode batch", K(ret));
          } else if (OB_FAIL(sk_vec_ptrs.push_back(encode_sortkeys.get_vectors().at(0)->get_vector()))) {
            LOG_WARN("failed to push back vector", K(ret), K(encode_sortkeys));
          }
        }
        //data format is doc id and word, we need to reverse order
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sk_vec_ptrs.push_back(bdrs.vectors_.at(word_idx_at_vectors)))) {
          LOG_WARN("failed to push back vector", K(ret), K(word_idx_at_vectors));
        } else if (OB_FAIL(sk_vec_ptrs.push_back(bdrs.vectors_.at(doc_id_idx_at_vectors)))) {
          LOG_WARN("failed to push back vector", K(ret), K(doc_id_idx_at_vectors));
        }

        for (int64_t i = 2; OB_SUCC(ret) && i < bdrs.vectors_.count(); i++) {
          if (OB_FAIL(sk_vec_ptrs.push_back(bdrs.vectors_.at(i)))) {
            LOG_WARN("failed to push back vector", K(ret), K(i));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // 准备 sk_rows 数组
        if (OB_FAIL(sk_rows_.reserve(batch_size))) {
          LOG_WARN("failed to reserve sk_rows array", K(ret), K(batch_size));
        } else {
          sk_rows_.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
            if (OB_FAIL(sk_rows_.push_back(nullptr))) {
              LOG_WARN("failed to push back sk_row", K(ret), K(i));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        int64_t inmem_row_size = 0;
        // 使用预分配的 selector_buf_，无需每次重新构造
        if (OB_FAIL(sort_impl->add_batch(sk_vec_ptrs,
                                         nullptr,
                                         selector_array_.get_data(),
                                         batch_size,
                                         sk_rows_.get_data(),
                                         nullptr,
                                         inmem_row_size))) {
          LOG_WARN("add batch failed", K(ret), K(batch_size));
        }
      }
    }
  }
  return ret;
}

// -------------------------------- ObFullTextIndexWritePipeline --------------------------------
int ObFullTextIndexWritePipeline::init(ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObFullTextIndexWritePipeline has been initialized", K(ret));
  } else if (OB_ISNULL(ddl_slice) || !ddl_slice->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ddl slice", KR(ret), KP(ddl_slice));
  } else {
    ddl_slice_ = ddl_slice;
    const ObTabletID tablet_id = ddl_slice_->get_tablet_id();
    const int64_t slice_idx = ddl_slice_->get_slice_idx();
    // 先调用基类的 init 方法
    if (OB_FAIL(ObIDDLPipeline::init(tablet_id, slice_idx))) {
      LOG_WARN("fail to init base pipeline", K(ret));
    } else {
      ObDDLIndependentDag *ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag());
      if (OB_ISNULL(ddl_dag)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl dag is null", K(ret));
      } else if (OB_UNLIKELY(1 != ddl_dag->get_sort_ls_tablet_ids().count())) {
        ret = OB_NOT_SUPPORTED;
      } else if (OB_FAIL(inverted_index_sort_flush_op_.init(ddl_dag->get_sort_ls_tablet_ids().at(0).second))) {
        LOG_WARN("fail to initialize inverted index sort flush operator", K(ret));
      } else if (OB_FAIL(add_op(&inverted_index_sort_flush_op_))) {
        LOG_WARN("fail to add inverted index sort flush operator", K(ret));
      } else if (OB_FAIL(fts_macro_block_write_op_.init(tablet_id, slice_idx))) {
        LOG_WARN("fail to initialize fts macro block write operator", K(ret));
      } else if (OB_FAIL(add_op(&fts_macro_block_write_op_))) {
        LOG_WARN("fail to add fts macro block write operator", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObFullTextIndexWritePipeline::get_next_chunk(ObChunk *&next_chunk)
{
  int ret = OB_SUCCESS;
  next_chunk = nullptr;
  if (OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl slice is null", K(ret), KPC(ddl_slice_));
  } else if (OB_FAIL(ddl_slice_->pop_chunk(next_chunk))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pop ddl chunk failed", K(ret));
    }
  } else if (OB_NOT_NULL(monitor_info_)
    && OB_NOT_NULL(next_chunk)
    && next_chunk->is_direct_load_batch_datum_rows_type()
    && OB_NOT_NULL(next_chunk->direct_load_batch_rows_)) {
    static_cast<ObFullTextIndexWritePipelineMonitorInfo *>(monitor_info_)->add_row_count(next_chunk->direct_load_batch_rows_->datum_rows_.row_count_);
  }
  return ret;
}

int ObFullTextIndexWritePipeline::finish_chunk(ObChunk *chunk)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(chunk)) {
    chunk->~ObChunk();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}

void ObFullTextIndexWritePipeline::postprocess(int &ret_code)
{
  if (OB_UNLIKELY(OB_ITER_END != ret_code && OB_DAG_TASK_IS_SUSPENDED != ret_code)) {
    FLOG_INFO("ret code not expected", K(ret_code), KPC(this), KPC(get_dag()));
  } else if (OB_LIKELY(OB_ITER_END == ret_code)) {
    ret_code = OB_SUCCESS;
    LOG_INFO("the ObFullTextIndexWritePipeline has ret code iter end", K(ret_code));
  }
  if (OB_DAG_TASK_IS_SUSPENDED != ret_code) {
    ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_NOT_NULL(dag)) {
     dag->dec_pipeline_count();
   }
  }
}

ObITask::ObITaskPriority ObFullTextIndexWritePipeline::get_priority()
{
  ObITask::ObITaskPriority priority = ObITask::get_priority();
  const static int64_t PRIORITY_THRESHOLD = 1;
  if (nullptr != ddl_slice_ && nullptr != dag_) {
    if (ddl_slice_->get_queue_size() >= PRIORITY_THRESHOLD || ddl_slice_->has_end_chunk()) {
      priority = ObITask::TASK_PRIO_1;
    } else {
      priority = ObITask::TASK_PRIO_0;
    }
  }
  return priority;
}

int ObFullTextIndexWritePipeline::get_slice_idx(int64_t &slice_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_slice_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl slice is null", K(ret), KPC(ddl_slice_));
  } else {
    slice_idx = ddl_slice_->get_slice_idx();
  }
  return ret;
}


void ObFullTextIndexWritePipeline::reset()
{
  is_inited_ = false;
  ddl_slice_ = nullptr;
}

int ObFullTextIndexWritePipeline::inner_add_monitor_info(storage::ObDDLDagMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObFullTextIndexWritePipelineMonitorInfo *info = nullptr;
  if (OB_FAIL(node.alloc_monitor_info(this, info))) {
    LOG_WARN("alloc monitor info failed", K(ret));
  } else if (OB_NOT_NULL(info)) {
    monitor_info_ = info;
    if (OB_NOT_NULL(ddl_slice_)) {
      info->init_task_params(ddl_slice_->get_tablet_id(), ddl_slice_->get_slice_idx());
    }
  }
  return ret;
}

ObFullTextIndexWritePipelineMonitorInfo::ObFullTextIndexWritePipelineMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task)
  : ObDDLDagMonitorInfo(allocator, task),
    tablet_id_(),
    slice_idx_(-1),
    row_count_(0)
{
}

void ObFullTextIndexWritePipelineMonitorInfo::init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx)
{
  if (!tablet_id_.is_valid() && slice_idx_ < 0) {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
  }
}

void ObFullTextIndexWritePipelineMonitorInfo::add_row_count(const int64_t row_count)
{
  row_count_ += MAX(0, row_count);
}

int ObFullTextIndexWritePipelineMonitorInfo::convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLDagMonitorInfo::convert_to_monitor_entry(entry))) {
    LOG_WARN("convert base monitor entry failed", K(ret));
  } else {
    ObJsonObject *json_obj = nullptr;
    ObJsonInt *tablet_id_node = nullptr;
    ObJsonInt *slice_idx_node = nullptr;
    ObJsonInt *row_cnt_node = nullptr;
    ObString json_str = entry.get_message();
    common::ObArenaAllocator &allocator = entry.get_allocator();
    if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_object_form_str(allocator, json_str, json_obj))) {
      LOG_WARN("failed to get json object from message", K(ret), K(json_str));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, tablet_id_.id(), tablet_id_node))
               || OB_FAIL(json_obj->add("tablet_id", tablet_id_node))) {
      LOG_WARN("failed to add tablet_id to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, slice_idx_, slice_idx_node))
               || OB_FAIL(json_obj->add("slice_idx", slice_idx_node))) {
      LOG_WARN("failed to add slice_idx to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, row_count_, row_cnt_node))
               || OB_FAIL(json_obj->add("row_count", row_cnt_node))) {
      LOG_WARN("failed to add row_count to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::print_json_to_str(allocator, json_obj, json_str))) {
      LOG_WARN("failed to print json to string", K(ret));
    } else if (OB_FAIL(entry.set_message(json_str))) {
      LOG_WARN("failed to set message", K(ret));
    }
  }
  return ret;
}
