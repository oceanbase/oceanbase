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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_simple_op.h"
#include "sql/das/ob_das_ref.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

ObDASSimpleOp::ObDASSimpleOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc) {}

int ObDASSimpleOp::release_op()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASSimpleOp::init_task_info(uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  UNUSED(row_extend_size);
  return ret;
}

int ObDASSimpleOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  UNUSED(remote_info);
  return ret;
}
OB_SERIALIZE_MEMBER((ObDASSimpleOp, ObIDASTaskOp));

OB_SERIALIZE_MEMBER(ObDASEmptyCtDef);
OB_SERIALIZE_MEMBER(ObDASEmptyRtDef);

ObDASSplitRangesOp::ObDASSplitRangesOp(ObIAllocator &op_alloc)
  : ObDASSimpleOp(op_alloc), expected_task_count_(0) {}

int ObDASSplitRangesOp::open_op()
{
  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  if (OB_FAIL(access_service->split_multi_ranges(ls_id_,
                                                 tablet_id_,
                                                 ranges_,
                                                 expected_task_count_,
                                                 op_alloc_,
                                                 multi_range_split_array_))) {
    LOG_WARN("failed to split multi ranges", K(ret), K_(ls_id), K_(tablet_id));
  }
  return ret;
}

int ObDASSplitRangesOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  UNUSED(memory_limit);
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASSplitRangesResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASSplitRangesResult &result = static_cast<ObDASSplitRangesResult&>(task_result);
    result.assign(multi_range_split_array_);
    has_more = false;
  }
  return ret;
}

int ObDASSplitRangesOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASSplitRangesResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASSplitRangesResult *result = static_cast<ObDASSplitRangesResult*>(task_result);
    if (OB_FAIL(multi_range_split_array_.assign(result->get_split_array()))) {
      LOG_WARN("failed to decode multi_range_split_array", K(ret));
    }
  }
  return ret;
}

int ObDASSplitRangesOp::init(const common::ObIArray<ObStoreRange> &ranges, int64_t expected_task_count)
{
  int ret = OB_SUCCESS;
  expected_task_count_ = expected_task_count;
  if (OB_FAIL(ranges_.assign(ranges))) {
    LOG_WARN("failed to assign ranges array", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASSplitRangesOp, ObIDASTaskOp),
                     ranges_,
                     expected_task_count_);

ObDASSplitRangesResult::ObDASSplitRangesResult()
  : ObIDASTaskResult(), result_alloc_(nullptr) {}

ObDASSplitRangesResult::~ObDASSplitRangesResult()
{
  multi_range_split_array_.reset();
}

int ObDASSplitRangesResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  result_alloc_ = &alloc;
  multi_range_split_array_.reset();
  return ret;
}

int ObDASSplitRangesResult::reuse()
{
  int ret = OB_SUCCESS;
  multi_range_split_array_.reuse();
  return ret;
}

int ObDASSplitRangesResult::assign(const ObArrayArray<ObStoreRange> &array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(multi_range_split_array_.assign(array))) {
    LOG_WARN("failed to assign multi ranges array", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASSplitRangesResult)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObDASSplitRangesResult, ObIDASTaskResult));
  OB_UNIS_ADD_LEN(multi_range_split_array_);
  return len;
}

OB_DEF_SERIALIZE(ObDASSplitRangesResult)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObDASSplitRangesResult, ObIDASTaskResult));
  OB_UNIS_ENCODE(multi_range_split_array_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASSplitRangesResult)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObDASSplitRangesResult, ObIDASTaskResult));
  OB_UNIS_DECODE(multi_range_split_array_);

  if (OB_ISNULL(result_alloc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr result_alloc", K(ret));
  } else {
    int64_t count = multi_range_split_array_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_range_split_array_.count(i); j++) {
        ObStoreRange &store_range = multi_range_split_array_.at(i, j);

        // deep copy ObRowKey of store_range
        const ObStoreRowkey &start_key = store_range.get_start_key();
        const ObStoreRowkey &end_key = store_range.get_end_key();
        ObStoreRowkey dst_start_key;
        ObStoreRowkey dst_end_key;
        if (OB_FAIL(start_key.deep_copy(dst_start_key, *result_alloc_))) {
          LOG_WARN("failed to deep copy start key", K(start_key), K(ret));
        } else if (OB_FAIL(end_key.deep_copy(dst_end_key, *result_alloc_))) {
          LOG_WARN("failed to deep copy end key", K(start_key), K(ret));
        } else {
          store_range.set_start_key(dst_start_key);
          store_range.set_end_key(dst_end_key);
        }
      }
    }
  }

  return ret;
}

ObDASRangesCostOp::ObDASRangesCostOp(common::ObIAllocator &op_alloc)
  : ObDASSimpleOp(op_alloc), total_size_(0) {}

int ObDASRangesCostOp::open_op()
{
  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  if (OB_FAIL(access_service->get_multi_ranges_cost(ls_id_,
                                                    tablet_id_,
                                                    ranges_,
                                                    total_size_))) {
    LOG_WARN("failed to get multi ranges cost", K(ret), K_(ls_id), K_(tablet_id));
  }
  return ret;
}

int ObDASRangesCostOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  UNUSED(memory_limit);
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASSplitRangesResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASRangesCostResult &result = static_cast<ObDASRangesCostResult&>(task_result);
    result.set_total_size(total_size_);
    has_more = false;
  }
  return ret;
}

int ObDASRangesCostOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASSplitRangesResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASRangesCostResult *result = static_cast<ObDASRangesCostResult*>(task_result);
    total_size_ = result->get_total_size();
  }
  return ret;
}

int ObDASRangesCostOp::init(const common::ObIArray<ObStoreRange> &ranges)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ranges_.assign(ranges))) {
    LOG_WARN("failed to assign ranges array", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASRangesCostOp, ObIDASTaskOp),
                     ranges_,
                     total_size_);

ObDASRangesCostResult::ObDASRangesCostResult()
  : ObIDASTaskResult(), total_size_(0) {}

int ObDASRangesCostResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  UNUSED(alloc);
  total_size_ = 0;
  return ret;
}

int ObDASRangesCostResult::reuse()
{
  int ret = OB_SUCCESS;
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASRangesCostResult, ObIDASTaskResult),
                     total_size_);

int ObDASSimpleUtils::split_multi_ranges(ObExecContext &exec_ctx,
                                         ObDASTabletLoc *tablet_loc,
                                         const common::ObIArray<ObStoreRange> &ranges,
                                         const int64_t expected_task_count,
                                         ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = nullptr;
  ObDASSplitRangesOp *split_ranges_op = nullptr;
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASRef das_ref(eval_ctx, exec_ctx);
  das_ref.set_mem_attr(ObMemAttr(MTL_ID(), "DASSplitRanges"));
  if (OB_FAIL(das_ref.create_das_task(tablet_loc, DAS_OP_SPLIT_MULTI_RANGES, task_op))) {
    LOG_WARN("prepare das split_multi_ranges task failed", K(ret));
  } else {
    split_ranges_op = static_cast<ObDASSplitRangesOp*>(task_op);
    split_ranges_op->set_can_part_retry(true);
    if (OB_FAIL(split_ranges_op->init(ranges, expected_task_count))) {
      LOG_WARN("failed to init das split ranges op", K(ret));
    } else if (OB_FAIL(das_ref.execute_all_task())) {
      LOG_WARN("execute das split_multi_ranges task failed", K(ret));
    } else if (OB_FAIL(multi_range_split_array.assign(split_ranges_op->get_split_array()))) {
      LOG_WARN("assgin split multi ranges array failed", K(ret));
    } else {
      int64_t count = multi_range_split_array.count();
      common::ObIAllocator &alloc = exec_ctx.get_allocator();
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        for (int64_t j = 0; OB_SUCC(ret) && j < multi_range_split_array.count(i); j++) {
          ObStoreRange &store_range = multi_range_split_array.at(i, j);

          // deep copy ObRowKey of store_range
          const ObStoreRowkey &start_key = store_range.get_start_key();
          const ObStoreRowkey &end_key = store_range.get_end_key();
          ObStoreRowkey dst_start_key;
          ObStoreRowkey dst_end_key;
          if (OB_FAIL(start_key.deep_copy(dst_start_key, alloc))) {
            LOG_WARN("failed to deep copy start key", K(start_key), K(ret));
          } else if (OB_FAIL(end_key.deep_copy(dst_end_key, alloc))) {
            LOG_WARN("failed to deep copy end key", K(start_key), K(ret));
          } else {
            store_range.set_start_key(dst_start_key);
            store_range.set_end_key(dst_end_key);
          }
        }
      }
    }
  }
  return ret;
}

int ObDASSimpleUtils::get_multi_ranges_cost(ObExecContext &exec_ctx,
                                            ObDASTabletLoc *tablet_loc,
                                            const common::ObIArray<common::ObStoreRange> &ranges,
                                            int64_t &total_size)
{
  int ret = OB_SUCCESS;
  ObIDASTaskOp *task_op = nullptr;
  ObDASRangesCostOp *ranges_cost_op = nullptr;
  ObEvalCtx eval_ctx(exec_ctx);
  ObDASRef das_ref(eval_ctx, exec_ctx);
  das_ref.set_mem_attr(ObMemAttr(MTL_ID(), "DASGetRangeCost"));
  if (OB_FAIL(das_ref.create_das_task(tablet_loc, DAS_OP_GET_RANGES_COST, task_op))) {
    LOG_WARN("prepare das get_multi_ranges_cost task failed", K(ret));
  } else {
    ranges_cost_op = static_cast<ObDASRangesCostOp*>(task_op);
    ranges_cost_op->set_can_part_retry(true);
    if (OB_FAIL(ranges_cost_op->init(ranges))) {
      LOG_WARN("failed to init das ranges cost op", K(ret));
    } else if (OB_FAIL(das_ref.execute_all_task())) {
      LOG_WARN("execute das get_multi_ranges_cost task failed", K(ret));
    } else {
      total_size = ranges_cost_op->get_total_size();
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
