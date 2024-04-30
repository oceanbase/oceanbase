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
#include "sql/das/iter/ob_das_iter_utils.h"

namespace oceanbase
{
namespace sql
{

/***************** public begin *****************/
int ObDASIterUtils::create_table_scan_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                                ObTableScanRtDef &tsc_rtdef,
                                                ObEvalCtx &eval_ctx,
                                                ObExecContext &exec_ctx,
                                                ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                                const ObTableScanSpec &spec,
                                                ObDASMergeIter *&scan_iter,
                                                ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASMergeIter *iter = nullptr;
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  ObDASScanRtDef *scan_rtdef = &tsc_rtdef.scan_rtdef_;
  common::ObIAllocator &alloc = exec_ctx.get_allocator();
  ObDASIterParam param;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &tsc_ctdef.get_das_output_exprs();
  param.group_id_expr_ = scan_ctdef->group_id_expr_;
  param.child_ = nullptr;
  param.right_ = nullptr;
  if (OB_FAIL(create_das_merge_iter_help(param,
                                         alloc,
                                         false,
                                         eval_infos,
                                         spec,
                                         iter))) {
    LOG_WARN("failed to create das merge iter", K(ret));
  } else {
    scan_iter = iter;
    iter_tree = iter;
  }

  return ret;
}

int ObDASIterUtils::create_local_lookup_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                                  ObTableScanRtDef &tsc_rtdef,
                                                  ObEvalCtx &eval_ctx,
                                                  ObExecContext &exec_ctx,
                                                  ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                                  const ObTableScanSpec &spec,
                                                  ObDASMergeIter *&scan_iter,
                                                  ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  // Currently, the iter tree of local index lookup is the same as that of table scan,
  // this is because local index lookup is executed within a single DAS task.
  // TODO bingfan: unify local index lookup and global index lookup.
  if (OB_FAIL(create_table_scan_iter_tree(tsc_ctdef,
                                          tsc_rtdef,
                                          eval_ctx,
                                          exec_ctx,
                                          eval_infos,
                                          spec,
                                          scan_iter,
                                          iter_tree))) {
    LOG_WARN("failed to create local index lookup iter tree", K(ret));
  }

  return ret;
}

int ObDASIterUtils::create_global_lookup_iter_tree(const ObTableScanCtDef &tsc_ctdef,
                                                   ObTableScanRtDef &tsc_rtdef,
                                                   ObEvalCtx &eval_ctx,
                                                   ObExecContext &exec_ctx,
                                                   ObFixedArray<ObEvalInfo*, ObIAllocator> &eval_infos,
                                                   const ObTableScanSpec &spec,
                                                   bool can_retry,
                                                   ObDASMergeIter *&scan_iter,
                                                   ObDASIter *&iter_tree)
{
  int ret = OB_SUCCESS;
  ObDASMergeIter *index_table_iter = nullptr;
  ObDASMergeIter *data_table_iter = nullptr;
  ObDASGlobalLookupIter *lookup_iter = nullptr;
  /********* create index table iter *********/
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  ObDASScanRtDef *scan_rtdef = &tsc_rtdef.scan_rtdef_;
  common::ObIAllocator &alloc = exec_ctx.get_allocator();
  ObDASIterParam param;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &scan_ctdef->result_output_;
  param.group_id_expr_ = scan_ctdef->group_id_expr_;
  param.child_ = nullptr;
  param.right_ = nullptr;
  if (OB_FAIL(create_das_merge_iter_help(param,
                                         alloc,
                                         false,
                                         eval_infos,
                                         spec,
                                         index_table_iter))) {
    LOG_WARN("failed to create index table iter", K(ret));
  }
  /********* create data table iter *********/
  if (OB_SUCC(ret)) {
    param.output_ = &tsc_ctdef.lookup_ctdef_->result_output_;
    param.child_ = nullptr;
    param.right_ = index_table_iter;
    if (OB_FAIL(create_das_merge_iter_help(param,
                                           alloc,
                                           true,
                                           eval_infos,
                                           spec,
                                           data_table_iter))) {
      LOG_WARN("failed to create data table iter", K(ret));
    }
  }
  /********* create global lookup iter *********/
  if (OB_SUCC(ret)) {
    index_table_iter->set_global_lookup_iter(data_table_iter);
    param.child_ = data_table_iter;
    param.right_ = nullptr;
    if (OB_FAIL(create_das_global_lookup_iter_help(param,
                                                   alloc,
                                                   10000, // hard code 10000
                                                   index_table_iter,
                                                   data_table_iter,
                                                   can_retry,
                                                   tsc_ctdef,
                                                   tsc_rtdef,
                                                   lookup_iter))) {
      LOG_WARN("failed to create das global lookup iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    scan_iter = index_table_iter;
    iter_tree = lookup_iter;
  }

  return ret;
}

int ObDASIterUtils::create_group_fold_iter(const ObTableScanCtDef &tsc_ctdef,
                                           ObTableScanRtDef &tsc_rtdef,
                                           ObEvalCtx &eval_ctx,
                                           ObExecContext &exec_ctx,
                                           ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                           const ObTableScanSpec &spec,
                                           ObDASIter *iter_tree,
                                           ObDASGroupFoldIter *&fold_iter)
{
  int ret = OB_SUCCESS;
  ObDASGroupFoldIter *iter = nullptr;
  const ObDASScanCtDef *scan_ctdef = &tsc_ctdef.scan_ctdef_;
  ObDASScanRtDef *scan_rtdef = &tsc_rtdef.scan_rtdef_;
  common::ObIAllocator &alloc = exec_ctx.get_allocator();
  ObDASIterParam param;
  param.max_size_ = eval_ctx.is_vectorized() ? eval_ctx.max_batch_size_ : 1;
  param.eval_ctx_ = &eval_ctx;
  param.exec_ctx_ = &exec_ctx;
  param.output_ = &tsc_ctdef.get_das_output_exprs();
  param.group_id_expr_ = tsc_ctdef.scan_ctdef_.group_id_expr_;
  param.child_ = iter_tree;
  param.right_ = nullptr;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_das_group_fold_iter_help(param,
                                                alloc,
                                                scan_rtdef->need_check_output_datum_,
                                                iter_tree,
                                                iter))) {
      LOG_WARN("failed to create das group fold iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    fold_iter = iter;
  }

  return ret;
}

/***************** private begin *****************/
int ObDASIterUtils::create_das_merge_iter_help(ObDASIterParam &param,
                                               common::ObIAllocator &alloc,
                                               bool is_global_lookup,
                                               ObFixedArray<ObEvalInfo *, ObIAllocator> &eval_infos,
                                               const ObTableScanSpec &spec,
                                               ObDASMergeIter *&result)
{
  int ret = OB_SUCCESS;
  void *iter_buf = nullptr;
  ObDASMergeIter *iter = nullptr;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param", K(param), K(ret));
  } else if (OB_ISNULL(iter_buf = alloc.alloc(sizeof(ObDASMergeIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    iter = new (iter_buf) ObDASMergeIter();
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(iter)) {
    ObDASMergeIterParam merge_param;
    merge_param.assgin(param);
    merge_param.type_ = DAS_ITER_MERGE;
    merge_param.eval_infos_ = &eval_infos;
    merge_param.need_update_partition_id_ = !is_global_lookup;
    merge_param.pdml_partition_id_ = spec.pdml_partition_id_;
    merge_param.partition_id_calc_type_ = spec.partition_id_calc_type_;
    merge_param.should_scan_index_ = spec.should_scan_index();
    merge_param.ref_table_id_ = spec.ref_table_id_;
    merge_param.is_vectorized_ = spec.is_vectorized();
    merge_param.frame_info_ = &spec.plan_->get_expr_frame_info();
    merge_param.execute_das_directly_ = !is_global_lookup && !spec.use_dist_das_;

    if (OB_FAIL(iter->init(merge_param))) {
      LOG_WARN("failed to init das merge iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result = iter;
  } else {
    if (OB_NOT_NULL(iter)) {
      iter->release();
      iter = nullptr;
    }
    if (OB_NOT_NULL(iter_buf)) {
      alloc.free(iter_buf);
      iter_buf = nullptr;
    }
    result = nullptr;
  }

  return ret;
}

int ObDASIterUtils::create_das_global_lookup_iter_help(ObDASIterParam &param,
                                                       common::ObIAllocator &alloc,
                                                       int64_t default_batch_row_count,
                                                       ObDASMergeIter *index_table_iter,
                                                       ObDASMergeIter *data_table_iter,
                                                       bool can_retry,
                                                       const ObTableScanCtDef &tsc_ctdef,
                                                       ObTableScanRtDef &tsc_rtdef,
                                                       ObDASGlobalLookupIter *&result)
{
  int ret = OB_SUCCESS;
  void *iter_buf = nullptr;
  ObDASGlobalLookupIter *iter = nullptr;
  if (!param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid das iter param", K(param), K(ret));
  } else if (OB_ISNULL(iter_buf = alloc.alloc(sizeof(ObDASGlobalLookupIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    iter = new (iter_buf) ObDASGlobalLookupIter();
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(iter)) {
    ObDASLookupIterParam lookup_param;
    lookup_param.assgin(param);
    lookup_param.type_ = ObDASIterType::DAS_ITER_LOOKUP;
    lookup_param.default_batch_row_count_ = default_batch_row_count;
    lookup_param.index_table_iter_ = index_table_iter;
    lookup_param.data_table_iter_ = data_table_iter;
    lookup_param.can_retry_ = can_retry;
    lookup_param.calc_part_id_ = tsc_ctdef.calc_part_id_expr_;
    lookup_param.lookup_ctdef_ = tsc_ctdef.lookup_ctdef_;
    lookup_param.lookup_rtdef_ = tsc_rtdef.lookup_rtdef_;
    lookup_param.rowkey_exprs_ = &tsc_ctdef.global_index_rowkey_exprs_;
    if (OB_FAIL(iter->init(lookup_param))) {
      LOG_WARN("failed to init das global lookup iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result = iter;
  } else {
    if (OB_NOT_NULL(iter)) {
      iter->release();
      iter = nullptr;
    }
    if (OB_NOT_NULL(iter_buf)) {
      alloc.free(iter_buf);
      iter_buf = nullptr;
    }
    result = nullptr;
  }

  return ret;
}

int ObDASIterUtils::create_das_group_fold_iter_help(ObDASIterParam &param,
                                                    common::ObIAllocator &alloc,
                                                    bool need_check_output_datum,
                                                    ObDASIter *iter_tree,
                                                    ObDASGroupFoldIter *&result)
{
  int ret = OB_SUCCESS;
  void *iter_buf = nullptr;
  ObDASGroupFoldIter *iter = nullptr;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param", K(param), K(ret));
  } else if (OB_ISNULL(iter_buf = alloc.alloc(sizeof(ObDASGroupFoldIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    iter = new (iter_buf) ObDASGroupFoldIter();
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(iter)) {
    ObDASGroupFoldIterParam group_fold_param;
    group_fold_param.assgin(param);
    group_fold_param.type_ = ObDASIterType::DAS_ITER_GROUP_FOLD;
    group_fold_param.need_check_output_datum_ = need_check_output_datum;
    group_fold_param.iter_tree_ = iter_tree;

    if (OB_FAIL(iter->init(group_fold_param))) {
      LOG_WARN("failed to init das group fold iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    result = iter;
  } else {
    if (OB_NOT_NULL(iter)) {
      iter->release();
      iter = nullptr;
    }
    if (OB_NOT_NULL(iter_buf)) {
      alloc.free(iter_buf);
      iter_buf = nullptr;
    }
    result = nullptr;
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
