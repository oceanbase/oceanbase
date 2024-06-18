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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_table_scan_with_index_back_op.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/access/ob_table_scan_iterator.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObTableScanWithIndexBackSpec, ObTableScanSpec), index_scan_tree_id_);

ObTableScanWithIndexBackSpec::ObTableScanWithIndexBackSpec(common::ObIAllocator &alloc,
                                                           const ObPhyOperatorType type)
  : ObTableScanSpec(alloc, type),
    index_scan_tree_id_(OB_INVALID_ID)
{
}

ObTableScanWithIndexBackSpec::~ObTableScanWithIndexBackSpec()
{
}
/*
int ObTableScanWithIndexBackOp::inner_open()
{
  int ret = OB_SUCCESS;
  //index scan需要依赖table的一些参数,所以应该先prepare table scan param，再open index scan
  ObOperatorKit *op_kit = nullptr;
  if (OB_ISNULL(op_kit = ctx_.get_operator_kit(MY_SPEC.get_index_scan_tree_id()))
              || OB_ISNULL(op_kit->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ObOperater from exec ctx failed", K(MY_SPEC.get_index_scan_tree_id()));
  } else if (FALSE_IT(index_scan_tree_ = op_kit->op_)) {
  } else if (OB_FAIL(prepare_scan_param())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    }
  } else if (OB_FAIL(open_index_scan())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("open index scan tree failed", K(ret));
    }
  } else if (OB_FAIL(do_table_scan_with_index())) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("do table scan with index failed", K(ret));
    }
  }
  return ret;
}
*/

int ObTableScanWithIndexBackOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_scan_tree_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("index scan tree is null");
  } else if (OB_FAIL(index_scan_tree_->rescan())) {
    LOG_WARN("rescan index scan tree failed", K(ret));

  } else if (OB_FAIL(ObTableScanOp::inner_rescan())) {
    LOG_WARN("rescan no children physical operator failed", K(ret));
  } else {
    is_index_end_ = false;
    if (OB_ISNULL(result_)) {
      //第一次rescan，没有初始化table iterator
      if (OB_FAIL(do_table_scan_with_index())) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("do table scan with index failed", K(ret));
        }
      }
    } else if (OB_FAIL(do_table_rescan_with_index())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("do table rescan with index failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanWithIndexBackOp::open_index_scan()
{
  int ret = OB_SUCCESS;
  if (index_scan_tree_!= NULL) {
    if (OB_FAIL(index_scan_tree_->open())) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("open index scan tree failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanWithIndexBackOp::extract_range_from_index()
{
  return OB_SUCCESS;
}
/*
int ObTableScanWithIndexBackOp::extract_range_from_index()
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  if (OB_ISNULL(index_scan_tree_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("index_scan_tree is null");
  } else if (is_index_end_) {
    //do nothing
    scan_param_.key_ranges_.reset();
  } else if (OB_ISNULL(table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(table_allocator_), K(ret));
  } else {
    int64_t BATCH_SIZE = 1000;
    scan_param_.key_ranges_.reset();
    ObIAllocator &allocator = use_table_allocator_ ? *(table_allocator_) : ctx_.get_allocator();
    const ObIArray<ObExpr *> &exprs = index_scan_tree_->get_spec().output_;
    int64_t mem_size = exprs.count() * sizeof(ObObj);
    for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; ++i) {
      ObObj *range_start = NULL;
      ObObj *range_end = NULL;
      if (OB_SUCC(index_scan_tree_->get_next_row())) {
        range_start = static_cast<ObObj*>(allocator.alloc(mem_size));
        range_end = static_cast<ObObj*>(allocator.alloc(mem_size));
        if (NULL == range_start || NULL == range_end) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate range failed", K(range_start), K(range_end), K(mem_size));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
            ObExprDatum &col_datum = exprs.at(j)->locate_expr_datum(eval_ctx_);
            OB_ASSERT(col_datum.evaluated_);
            if (OB_FAIL(col_datum.to_obj(range_start[j], exprs.at(j)->obj_meta_,
                                        exprs.at(j)->obj_datum_map_))) {
              LOG_WARN("datum to obj failed", K(ret));
            } else {
              range_end[j] = range_start[j];
              LOG_DEBUG("static engine tsc with index back get row", K(j), K(range_start[j]));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        range.table_id_ = MY_SPEC.ref_table_id_;
        range.start_key_.assign(range_start, exprs.count());
        range.end_key_.assign(range_end, exprs.count());
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
        if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
          LOG_WARN("store range failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      use_table_allocator_ = true;
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
    LOG_WARN("get rowkey from index scan failed", K(ret));
  }
  if (OB_ITER_END == ret) {
    is_index_end_ = true;
    ret = OB_SUCCESS;
  }
  return ret;
}
*/
int ObTableScanWithIndexBackOp::do_table_scan_with_index()
{
  int ret = OB_SUCCESS;
//  ObITabletScan *das = NULL;
//  ObTaskExecutorCtx *task_exec_ctx = NULL;
//  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx_))) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("get task executor ctx failed", K(ret));
//  } else if (OB_FAIL(extract_range_from_index())) {
//    LOG_WARN("extract range from index failed", K(ret));
//  } else if (scan_param_.key_ranges_.count() <= 0) {
//    //do nothing
//    read_action_ = READ_ITER_END;
//  } else if (OB_FAIL(get_partition_service(*task_exec_ctx, das))) {
//    LOG_WARN("fail to get partition service", K(ret));
//  } else if (OB_FAIL(das->table_scan(scan_param_, result_))) {
//    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
//      LOG_WARN("fail to scan table", K(scan_param_), K(ret));
//    }
//  } else {
//    read_action_ = READ_ITERATOR;
//  }
  return ret;
}

int ObTableScanWithIndexBackOp::do_table_rescan_with_index()
{
  int ret = OB_SUCCESS;
  storage::ObTableScanIterator *table_iter = NULL;
  if (OB_FAIL(extract_range_from_index())) {
    LOG_WARN("extract range from index failed", K(ret));
  } else if (scan_param_.key_ranges_.count() <= 0) {
    //do nothing
    read_action_ = READ_ITER_END;
  } else if (OB_ISNULL(table_iter = static_cast<storage::ObTableScanIterator*>(result_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table iterator is null");
  } else if (OB_FAIL(table_iter->rescan(scan_param_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to rescan", K(ret), "scan_param", scan_param_);
    }
  } else {
    read_action_ = READ_ITERATOR;
  }
  return ret;
}

int ObTableScanWithIndexBackOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObNewRow *cur_row = NULL;
  bool need_continue = true;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  }
  while (OB_SUCC(ret) && need_continue) {
    switch (read_action_) {
    case READ_TABLE_PARTITION: {
      if (OB_FAIL(do_table_rescan_with_index())) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
        }
      }
      break;
    }
    case READ_ITERATOR: {
      if (OB_FAIL(result_->get_next_row(cur_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
        } else {
          //读到了迭代器末端，从storage重新读新的数据
          read_action_ = READ_TABLE_PARTITION;
          ret = OB_SUCCESS;
        }
      } else {
        output_row_cnt_++;
        need_continue = false;
        LOG_DEBUG("get next row from domain index look up");
      }
      break;
    }
    case READ_ITER_END: {
      //读到了数据的末端，对调用者返回OB_ITER_END
      ret = OB_ITER_END;
      need_continue = false;
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read action is unexpected", K_(read_action));
      break;
    }
  }
  return ret;
}

int ObTableScanWithIndexBackOp::inner_close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (index_scan_tree_ != NULL) {
    if (OB_FAIL(index_scan_tree_->close())) {
      LOG_WARN("close index scan tree failed", K(ret));
    }
  }
  tmp_ret = ret;
  if (OB_FAIL(ObTableScanOp::inner_close())) {
    // overwrite ret
    LOG_WARN("inner close ooerator failed", K(ret));
  }
  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
