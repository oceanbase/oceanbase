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
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_service.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_table_scan_iterator.h"
namespace oceanbase {
using namespace common;
namespace sql {
ObTableScanWithIndexBack::ObTableScanWithIndexBack(ObIAllocator& allocator)
    : ObTableScan(allocator), index_scan_tree_(NULL)
{
  set_type(PHY_TABLE_SCAN_WITH_DOMAIN_INDEX);
}

ObTableScanWithIndexBack::~ObTableScanWithIndexBack()
{}

int ObTableScanWithIndexBack::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObTableScanInput* scan_input = NULL;
  if (OB_ISNULL(scan_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, get_id()))) {
    if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTableScanInput, ctx, get_id(), get_type(), scan_input))) {
      LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
    } else {
      scan_input->set_location_idx(0);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id(), get_type(), op_ctx))) {
      LOG_WARN("create physical operator context failed", K(ret), K(get_type()));
    } else if (OB_ISNULL(op_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator context is null");
    } else if (OB_FAIL(init_cur_row(*op_ctx, false))) {
      LOG_WARN("init current row failed", K(ret));
    } else if (OB_FAIL(static_cast<ObTableScanCtx*>(op_ctx)->init_table_allocator(ctx))) {
      LOG_WARN("fail to init table allocator", K(ret));
    }
  }
  return ret;
}

int ObTableScanWithIndexBack::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_FAIL(prepare_scan_param(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("prepare scan param failed", K(ret));
    }
  } else if (OB_FAIL(open_index_scan(ctx))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("open index scan tree failed", K(ret));
    }
  } else if (OB_FAIL(do_table_scan_with_index(ctx))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("do table scan with index failed", K(ret));
    }
  }
  return ret;
}

int ObTableScanWithIndexBack::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanWithIndexBackCtx* scan_ctx = NULL;
  if (OB_ISNULL(index_scan_tree_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("index scan tree is null");
  } else if (OB_FAIL(index_scan_tree_->rescan(ctx))) {
    LOG_WARN("rescan index scan tree failed", K(ret));
  } else if (OB_FAIL(ObNoChildrenPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan no children physical operator failed", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id())) ||
             OB_ISNULL(scan_ctx->table_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan context is null");
  } else {
    scan_ctx->is_index_end_ = false;
    scan_ctx->table_allocator_->reuse();
    if (OB_ISNULL(scan_ctx->result_)) {
      if (OB_FAIL(do_table_scan_with_index(ctx))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("do table scan with index failed", K(ret));
        }
      }
    } else if (OB_FAIL(do_table_rescan_with_index(ctx))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("do table rescan with index failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanWithIndexBack::open_index_scan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (index_scan_tree_ != NULL) {
    if (OB_FAIL(index_scan_tree_->open(ctx))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("open index scan tree failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableScanWithIndexBack::extract_range_from_index(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const ObNewRow* cur_row = NULL;
  ObNewRange range;
  ObTableScanWithIndexBackCtx* scan_ctx = NULL;
  if (OB_ISNULL(index_scan_tree_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("index_scan_tree is null");
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get table scan with index context failed", K(get_id()));
  } else if (scan_ctx->is_index_end_) {
    // do nothing
    scan_ctx->scan_param_.key_ranges_.reset();
  } else if (OB_ISNULL(scan_ctx->table_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(scan_ctx->table_allocator_), K(ret));
  } else {
    int64_t BATCH_SIZE = 1000;
    scan_ctx->scan_param_.key_ranges_.reset();
    ObIAllocator& allocator = scan_ctx->use_table_allocator_ ? *(scan_ctx->table_allocator_) : ctx.get_allocator();
    for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; ++i) {
      ObObj* range_start = NULL;
      ObObj* range_end = NULL;
      if (OB_SUCC(index_scan_tree_->get_next_row(ctx, cur_row))) {
        int64_t mem_size = cur_row->get_count() * sizeof(ObObj);
        range_start = static_cast<ObObj*>(allocator.alloc(mem_size));
        range_end = static_cast<ObObj*>(allocator.alloc(mem_size));
        if (NULL == range_start || NULL == range_end) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate range failed", K(range_start), K(range_end), K(mem_size));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < cur_row->get_count(); ++j) {
            if (OB_FAIL(ob_write_obj(allocator, cur_row->get_cell(j), range_start[j]))) {
              LOG_WARN("write obj failed", K(ret), K(cur_row->get_cell(j)));
            } else {
              range_end[j] = range_start[j];
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        range.table_id_ = get_ref_table_id();
        range.start_key_.assign(range_start, cur_row->get_count());
        range.end_key_.assign(range_end, cur_row->get_count());
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
        if (OB_FAIL(scan_ctx->scan_param_.key_ranges_.push_back(range))) {
          LOG_WARN("store range failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scan_ctx->use_table_allocator_ = true;
    }
  }
  if (OB_FAIL(ret) && ret != OB_ITER_END) {
    LOG_WARN("get rowkey from index scan failed", K(ret));
  }
  if (OB_ITER_END == ret) {
    scan_ctx->is_index_end_ = true;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTableScanWithIndexBack::do_table_scan_with_index(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanWithIndexBackCtx* scan_ctx = NULL;
  ObIDataAccessService* das = NULL;
  ObTaskExecutorCtx& task_exec_ctx = ctx.get_task_exec_ctx();
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(get_id()));
  } else if (OB_FAIL(extract_range_from_index(ctx))) {
    LOG_WARN("extract range from index failed", K(ret));
  } else if (scan_ctx->scan_param_.key_ranges_.count() <= 0) {
    // do nothing
    scan_ctx->read_action_ = READ_ITER_END;
  } else if (OB_FAIL(get_partition_service(task_exec_ctx, das))) {
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_FAIL(das->table_scan(scan_ctx->scan_param_, scan_ctx->result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(scan_ctx->scan_param_), K(ret));
    }
  } else {
    scan_ctx->read_action_ = READ_ITERATOR;
  }
  return ret;
}

int ObTableScanWithIndexBack::do_table_rescan_with_index(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScanWithIndexBackCtx* scan_ctx = NULL;
  storage::ObTableScanIterator* table_iter = NULL;
  if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(get_id()));
  } else if (OB_FAIL(extract_range_from_index(ctx))) {
    LOG_WARN("extract range from index failed", K(ret));
  } else if (scan_ctx->scan_param_.key_ranges_.count() <= 0) {
    // do nothing
    scan_ctx->read_action_ = READ_ITER_END;
  } else if (OB_ISNULL(table_iter = static_cast<storage::ObTableScanIterator*>(scan_ctx->result_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table iterator is null");
  } else if (OB_FAIL(table_iter->rescan(scan_ctx->scan_param_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to rescan", K(ret), "scan_param", scan_ctx->scan_param_);
    }
  } else {
    scan_ctx->read_action_ = READ_ITERATOR;
  }
  return ret;
}

int ObTableScanWithIndexBack::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  bool need_continue = true;
  ObTableScanWithIndexBackCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanWithIndexBackCtx, ctx, get_id());
  if (OB_ISNULL(scan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan is NULL", K(ret));
  } else if (OB_FAIL(try_check_status(ctx))) {
    LOG_WARN("check physical plan status failed", K(ret));
  }
  while (OB_SUCC(ret) && need_continue) {
    switch (scan_ctx->read_action_) {
      case READ_TABLE_PARTITION: {
        if (OB_FAIL(do_table_rescan_with_index(ctx))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
          }
        }
        break;
      }
      case READ_ITERATOR: {
        if (OB_FAIL(scan_ctx->result_->get_next_row(cur_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row from ObNewRowIterator", K(ret));
          } else {
            // reach iterator end, read from storage
            scan_ctx->read_action_ = READ_TABLE_PARTITION;
            ret = OB_SUCCESS;
          }
        } else {
          scan_ctx->cur_row_.cells_ = cur_row->cells_;
          row = &scan_ctx->get_cur_row();
          scan_ctx->output_row_count_++;
          need_continue = false;
          LOG_DEBUG("get next row from domain index look up", K(*row));
        }
        break;
      }
      case READ_ITER_END: {
        // read data end, return OB_ITER_END
        ret = OB_ITER_END;
        need_continue = false;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read action is unexpected", K_(scan_ctx->read_action));
        break;
    }
  }
  return ret;
}

int ObTableScanWithIndexBack::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (index_scan_tree_ != NULL) {
    if (OB_FAIL(index_scan_tree_->close(ctx))) {
      LOG_WARN("close index scan tree failed", K(ret));
    }
  }
  tmp_ret = ret;
  if (OB_FAIL(ObTableScan::inner_close(ctx))) {
    LOG_WARN("inner close ooerator failed", K(ret));
  }
  ret = (OB_SUCCESS == ret) ? ret : tmp_ret;
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
