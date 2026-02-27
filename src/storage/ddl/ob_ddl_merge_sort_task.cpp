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

#include "storage/ddl/ob_ddl_merge_sort_task.h"
#include "storage/ddl/ob_merge_sort_prepare_task.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_group_write_macro_block_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "storage/ddl/ob_ddl_sort_provider.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace storage
{

ObDDLMergeSortTask::ObDDLMergeSortTask()
  : share::ObITaskWithMonitor(share::ObITask::TASK_TYPE_DDL_MERGE_SORT_TASK),
    is_inited_(false),
    ddl_dag_(nullptr),
    ddl_slice_(nullptr),
    final_merge_ways_(0)
{
}

ObDDLMergeSortTask::~ObDDLMergeSortTask()
{
}

int ObDDLMergeSortTask::init(ObDDLIndependentDag *ddl_dag,
                             ObDDLSlice *ddl_slice,
                             const int64_t final_merge_ways)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_ || nullptr == ddl_dag || nullptr == ddl_slice || final_merge_ways < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_dag), KP(ddl_slice));
  } else {
    ddl_dag_ = ddl_dag;
    ddl_slice_ = ddl_slice;
    final_merge_ways_ = final_merge_ways;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLMergeSortTask::inner_add_monitor_info(storage::ObDDLDagMonitorNode &node)
{
  int ret = OB_SUCCESS;
  ObDDLMergeSortTaskMonitorInfo *info = nullptr;
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

int ObDDLMergeSortTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLSortProvider::SortImpl *sort_impl = nullptr;
  int64_t merge_ways = 0;
  ObArray<ObDDLSortChunk> input_ddl_chunks;
  int64_t input_row_cnt = 0;
  int64_t output_row_cnt = 0;
  int64_t input_chunk_size = 0;
  int64_t output_chunk_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge task not init", K(ret));
  } else if (ddl_slice_->get_sorted_chunk_count() <= final_merge_ways_) {
    // do nothing
  } else if (OB_UNLIKELY(nullptr == ddl_dag_->get_sort_provider())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort provider is null", K(ret));
  } else if (OB_FAIL(ddl_dag_->get_sort_provider()->get_sort_impl(sort_impl))) {
    LOG_WARN("get sort impl failed", K(ret));
  } else if (OB_FAIL(sort_impl->get_merge_ways(final_merge_ways_, merge_ways))) {
    LOG_WARN("get merge ways failed", K(ret));
  } else if (OB_FAIL(ddl_slice_->pop_sorted_chunks(final_merge_ways_, merge_ways, input_ddl_chunks))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pop sorted chunks failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (input_ddl_chunks.count() > 0) {
    ObArray<ObDDLSortProvider::ChunkType *> input_sort_op_chunks;
    for (int64_t i = 0; OB_SUCC(ret) && i < input_ddl_chunks.count(); ++i) {
      ObDDLSortChunk &ddl_chunk = input_ddl_chunks.at(i);
      if (OB_UNLIKELY(!ddl_chunk.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid chunk", K(ret), K(i), K(ddl_chunk));
      } else if (OB_ISNULL(ddl_chunk.get_sort_op_chunk())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort op chunk is null", K(ret), K(i), K(ddl_chunk));
      } else if (FALSE_IT(input_row_cnt += reinterpret_cast<const ObDDLSortProvider::ChunkType *>(ddl_chunk.get_sort_op_chunk())->get_row_count())) {
      } else if (FALSE_IT(input_chunk_size += ddl_chunk.get_file_size())) {
      } else if (OB_FAIL(input_sort_op_chunks.push_back(reinterpret_cast<ObDDLSortProvider::ChunkType *>(ddl_chunk.get_sort_op_chunk())))) {
        LOG_WARN("push back chunk failed", K(ret), K(i), K(ddl_chunk));
      }
    }
    if (OB_SUCC(ret)) {
      ObDDLSortProvider::ChunkType *output_sort_op_chunk = nullptr;
      if (OB_FAIL(sort_impl->add_sort_chunks(0, input_sort_op_chunks))) {
        LOG_WARN("add sort chunks failed", K(ret));
      } else if (OB_FAIL(sort_impl->merge_sort_chunks(output_sort_op_chunk))) {
        LOG_WARN("merge sort chunks failed", K(ret));
      } else if (OB_ISNULL(output_sort_op_chunk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output sort op chunk is null", K(ret));
      } else if (FALSE_IT(output_row_cnt = output_sort_op_chunk->get_row_count())) {
      } else if (FALSE_IT(output_chunk_size = output_sort_op_chunk->get_file_size())) {
      } else if (OB_UNLIKELY(output_row_cnt != input_row_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output row count is not equal to input row count", K(ret), K(output_row_cnt), K(input_row_cnt));
      } else if (OB_FAIL(ddl_slice_->push_sorted_chunk(output_sort_op_chunk, output_chunk_size, sort_impl->get_allocator()))) {
        LOG_WARN("push sorted chunk failed", K(ret));
      } else {
        // Accumulate merge results (no current-round info).
        if (OB_NOT_NULL(monitor_info_)) {
          static_cast<ObDDLMergeSortTaskMonitorInfo *>(monitor_info_)->add_merged_stats(
              input_ddl_chunks.count(),
              input_chunk_size,
              output_row_cnt);
        }
        LOG_INFO("merge sort chunks success", K(ret), K(input_ddl_chunks.count()));
      }
      // Cleanup output_sort_op_chunk if it wasn't successfully transferred to slice
      if (OB_FAIL(ret) && OB_NOT_NULL(output_sort_op_chunk)) {
        using ChunkType = ObDDLSortProvider::ChunkType;
        output_sort_op_chunk->~ChunkType();
        sort_impl->get_allocator()->free(output_sort_op_chunk);
        output_sort_op_chunk = nullptr;
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < input_ddl_chunks.count(); ++i) {
        if (i >= input_sort_op_chunks.count() // ownership is not transfered to input_sort_op_chunks
            || nullptr != input_sort_op_chunks.at(i)) { // ownership is not transfered to sort impl
          input_ddl_chunks.at(i).free_sort_op_chunk();
        }
      }
    }
    sort_impl->reset(); // free input ddl chunks
    input_ddl_chunks.reset();
  }
  if (OB_SUCC(ret) && ddl_slice_->get_sorted_chunk_count() > final_merge_ways_) { // schedule again
    ret = OB_DAG_TASK_IS_SUSPENDED;
  }
  return ret;
}

ObDDLMergeSortTaskMonitorInfo::ObDDLMergeSortTaskMonitorInfo(common::ObIAllocator *allocator, share::ObITask *task)
  : ObDDLDagMonitorInfo(allocator, task),
    tablet_id_(),
    slice_idx_(-1),
    merged_chunk_count_(0),
    merged_chunk_size_(0),
    merged_row_count_(0)
{
}

ObDDLMergeSortTaskMonitorInfo::~ObDDLMergeSortTaskMonitorInfo()
{
}

void ObDDLMergeSortTaskMonitorInfo::init_task_params(const common::ObTabletID &tablet_id, const int64_t slice_idx)
{
  // Init once. Do not override if already set.
  if (!tablet_id_.is_valid() && slice_idx_ < 0) {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
  }
}

void ObDDLMergeSortTaskMonitorInfo::add_merged_stats(
    const int64_t merged_chunk_count,
    const int64_t merged_chunk_size,
    const int64_t merged_row_count)
{
  merged_chunk_count_ += MAX(0, merged_chunk_count);
  merged_chunk_size_ += MAX(0, merged_chunk_size);
  merged_row_count_ += MAX(0, merged_row_count);
}

int ObDDLMergeSortTaskMonitorInfo::convert_to_monitor_entry(ObDDLDagMonitorEntry &entry) const
{
  int ret = OB_SUCCESS;
  // Reuse base capability:
  // - Fill basic task fields (tenant_id/task_id/task_info/format_version)
  // - Fill JSON message: schedule_info
  if (OB_FAIL(ObDDLDagMonitorInfo::convert_to_monitor_entry(entry))) {
    LOG_WARN("convert base monitor entry failed", K(ret));
  } else {
    // Add merged chunk info, tablet_id and slice_idx to JSON message
    ObJsonObject *json_obj = nullptr;
    ObJsonInt *tablet_id_node = nullptr;
    ObJsonInt *slice_idx_node = nullptr;
    ObJsonInt *merged_chunk_cnt_node = nullptr;
    ObJsonInt *merged_chunk_size_node = nullptr;
    ObJsonInt *merged_row_cnt_node = nullptr;
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
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, merged_chunk_count_, merged_chunk_cnt_node))
               || OB_FAIL(json_obj->add("merged_chunk_count", merged_chunk_cnt_node))) {
      LOG_WARN("failed to add merged_chunk_count to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, merged_chunk_size_, merged_chunk_size_node))
               || OB_FAIL(json_obj->add("merged_chunk_size", merged_chunk_size_node))) {
      LOG_WARN("failed to add merged_chunk_size to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::get_json_int(allocator, merged_row_count_, merged_row_cnt_node))
               || OB_FAIL(json_obj->add("merged_row_count", merged_row_cnt_node))) {
      LOG_WARN("failed to add merged_row_count to json", K(ret));
    } else if (OB_FAIL(common::ObAIFuncJsonUtils::print_json_to_str(allocator, json_obj, json_str))) {
      LOG_WARN("failed to print json to string", K(ret));
    } else if (OB_FAIL(entry.set_message(json_str))) {
      LOG_WARN("failed to set message", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
