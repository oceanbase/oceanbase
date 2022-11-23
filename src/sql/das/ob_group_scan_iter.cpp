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
#include "sql/das/ob_das_group_scan_op.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{
int ObGroupResultRows::init(const common::ObIArray<ObExpr *> &exprs,
                            ObEvalCtx &eval_ctx,
                            ObIAllocator &das_op_allocator,
                            int64_t max_size,
                            ObExpr *group_id_expr,
                            bool need_check_output_datum)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    need_check_output_datum_ = need_check_output_datum;
    rows_ = static_cast<LastDASStoreRow *>(das_op_allocator.alloc(max_size * sizeof(LastDASStoreRow)));
    if (NULL == rows_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(max_size), K(ret));
    } else {
      for (int64_t i = 0; i < max_size; i++) {
        new(rows_+i) LastDASStoreRow(das_op_allocator);
        rows_[i].reuse_ = true;
      }
      inited_ = true;
      exprs_ = &exprs;
      eval_ctx_ = &eval_ctx;
      max_size_ = max_size;
      group_id_expr_pos_ = OB_INVALID_INDEX;
      for (int64_t i = 0; i < exprs.count(); i++) {
        if (exprs.at(i) == group_id_expr) {
          group_id_expr_pos_ = i;
          break;
        }
      }
      if (OB_INVALID_INDEX == group_id_expr_pos_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), KP(group_id_expr), K(exprs));
      }
    }
  }

  return ret;
}

int ObGroupResultRows::save(bool is_vectorized, int64_t start_pos, int64_t size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (start_pos + size > max_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_pos), K(size), K(max_size_));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(start_pos + size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(start_pos + i);
        OZ(rows_[i].save_store_row(*exprs_, *eval_ctx_));
      }
    } else {
      OZ(rows_[0].save_store_row(*exprs_, *eval_ctx_));
    }
    start_pos_ = 0;
    saved_size_ = size;
  }

  return ret;
}

int ObGroupResultRows::to_expr(bool is_vectorized, int64_t start_pos, int64_t size)
{
  int ret = OB_SUCCESS;
  if (is_vectorized) {
    if (start_pos + size > saved_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(start_pos), K(size), K(saved_size_), K(inited_), K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        OZ(rows_[i + start_pos].store_row_->to_expr<true>(*exprs_, *eval_ctx_));
      }
    }
  } else {
    OZ(rows_[0].store_row_->to_expr<false>(*exprs_, *eval_ctx_));
  }

  return ret;
}

int64_t ObGroupResultRows::cur_group_idx()
{
  return start_pos_ + 1 > saved_size_
         ? OB_INVALID_INDEX
         : (rows_[start_pos_].store_row_->cells()[group_id_expr_pos_]).get_int();
}

ObGroupScanIter::ObGroupScanIter()
  : ObNewRowIterator(),
    cur_group_idx_(0),
    last_group_idx_(MIN_GROUP_INDEX),
    group_size_(0),
    group_id_expr_(),
    row_store_(),
    result_tmp_iter_(NULL),
    iter_(&result_tmp_iter_)
{
}

// 1. 如果last_group_idx > cur_group_idx 则返回iter_end
// 2. 如果last_group_idx = cur_group_idx
//     则将缓存的行store到对应output行表达式的中, 设置last_group_idx = -1, 返回行
// 3. 如果last_group_idx < cur_group_idx
//     3.1 从存储层获取一行数据
//     3.2 判断该行数据中group_idx是否与当前cur_group_idx相同
//         如果相同:
//           则返回;
//         如果不相同:
//           记录当前读到的行的group_idx到last_group_idx
//           对当前行进行深拷贝暂存
//           返回iter end
int ObGroupScanIter::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t group_idx = MIN_GROUP_INDEX;
  if (last_group_idx_ < cur_group_idx_) {
    ObDatum *datum_group_idx = NULL;
    if (OB_FAIL(get_iter()->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else if (OB_FAIL(group_id_expr_->eval(*row_store_.eval_ctx_,
                                            datum_group_idx))) {
      LOG_WARN("fail to eval group id", K(ret));
    } else if (FALSE_IT(group_idx = datum_group_idx->get_int())) {
    } else if (group_idx == cur_group_idx_) {
      // return result
    } else {
      if (OB_FAIL(row_store_.save(false, 0, 1))) {
        LOG_WARN("fail to save last row", K(ret));
      } else {
        last_group_idx_ = group_idx;
        ret = OB_ITER_END;
      }
    }
  } else if (last_group_idx_ == cur_group_idx_) {
    OZ(row_store_.to_expr(false, 0, 1));
    last_group_idx_ = MIN_GROUP_INDEX;
  } else {
    ret = OB_ITER_END;
  }
  LOG_DEBUG("das group next row", K(ret), K(this), K(*this), K(*row_store_.eval_ctx_));

  return ret;
}

// 1. 如果last_group_idx > cur_group_idx 则返回iter_end
// 2. 如果last_group_idx = cur_group_idx
//     2.1 遍历缓存的行, 计算对应group_idx, 当前行是否属于cur_group_idx:
//       a. 如果group_idx = cur_group_idx, 则重复2.1
//       b. 如果group_idx != cur_group_idx,  则将group_idx记录到last_group_idx,
//          记录本次缓存数据下次访问的开始点
//     2.2 如果遍历结束, 则iter_end, last_group_idx = -1
// 3. 如果last_group_idx < cur_group_idx
//     3.1 从存储层获取一批数据
//     3.2 依次遍历每行数据中group_idx, 看下当前行是否属于cur_group_idx:
//        a. 如果group_idx = cur_group_idx, 则重复第3.2步
//        b. 如果group_idx != cur_group_idx, 则记录将group_idx记录到last_group_idx,
//           并将后面的batch数据深拷贝缓存
//        c. iter_end
//     3.3 如果遍历结束, 则iter_end, last_group_idx = -1

int ObGroupScanIter::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("das group before next row", K(last_group_idx_), K(cur_group_idx_));
  int64_t storage_count = 0;
  int64_t ret_count = 0;
  int64_t group_idx = MIN_GROUP_INDEX;
  if (last_group_idx_ < cur_group_idx_) {
    reset_expr_datum_ptr();
    if (OB_FAIL(get_iter()->get_next_rows(storage_count, capacity))) {
      if (OB_ITER_END == ret && storage_count > 0) {
        ret = OB_SUCCESS;
      } else if (OB_ITER_END == ret) {
        last_group_idx_ = INT64_MAX;
      } else {
        LOG_WARN("fail to get next rows", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      PRINT_VECTORIZED_ROWS(SQL, DEBUG, *row_store_.eval_ctx_, *row_store_.exprs_, storage_count);
      ObDatum *group_idx_batch = group_id_expr_->locate_batch_datums(*row_store_.eval_ctx_);
      int64_t i = 0;
      for (i = 0; i < storage_count; i++) {
        group_idx = group_idx_batch[i].get_int();
        if (cur_group_idx_ == group_idx) {
          ret_count++;
          continue;
        } else {
          last_group_idx_ = group_idx;
          ret = OB_ITER_END;
          break;
        }
      } // for end;
      if (i < storage_count) {
        int tmp_ret = row_store_.save(true, i, storage_count - i);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("fail to save batch result", K(tmp_ret));
          ret = tmp_ret;
        }
      } else {
        last_group_idx_ = MIN_GROUP_INDEX;
      }
    }
  } else if (last_group_idx_ == cur_group_idx_) {
    int64_t group_idx = MIN_GROUP_INDEX;
    int64_t start_pos = row_store_.get_start_pos();
    while(cur_group_idx_ == last_group_idx_) {
      group_idx = row_store_.cur_group_idx();
      if (cur_group_idx_ == group_idx) {
        row_store_.next_start_pos();
        ret_count++;
      } else {
        // if row store iter end, group_idx = MIN_GROUP_INDEX;
        last_group_idx_ = group_idx;
      }
    }
    if (ret_count > 0) {
      OZ(row_store_.to_expr(true, start_pos, ret_count));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ret count must be greater than 0", K(ret_count), K(row_store_), K(ret));
    }
    // when batch data in row store not end, and cur_group_idx != last_group_idx,
    // means this group data is end
    if (OB_SUCC(ret) && MIN_GROUP_INDEX != last_group_idx_ && cur_group_idx_ != last_group_idx_) {
      ret = OB_ITER_END;
    }
  } else {
    ret = OB_ITER_END;
  }
  count = ret_count;
  LOG_DEBUG("das group after next row", K(last_group_idx_), K(group_idx), K(cur_group_idx_),
                                  K(ret_count), K(storage_count), K(row_store_), K(this), K(ret));
  if (OB_UNLIKELY(row_store_.need_check_output_datum_)) {
    ObSQLUtils::access_expr_sanity_check(*row_store_.exprs_,
                                         *row_store_.eval_ctx_,
                                         row_store_.max_size_);
  }

  return ret;
}

void ObGroupScanIter::reset_expr_datum_ptr()
{
  FOREACH_CNT(e, *row_store_.exprs_) {
    (*e)->locate_datums_for_update(*row_store_.eval_ctx_, row_store_.max_size_);
    ObEvalInfo &info = (*e)->get_eval_info(*row_store_.eval_ctx_);
    info.point_to_frame_ = true;
  }
}

void ObGroupScanIter::reset()
{
  cur_group_idx_ = 0;
  last_group_idx_ = MIN_GROUP_INDEX;
  group_size_ = 0;
  group_id_expr_ = NULL;
  row_store_.reset();
  result_tmp_iter_ = NULL;
  iter_ = &result_tmp_iter_;
  LOG_DEBUG("reset group scan iter", K(this), K(*this));
}

int ObGroupScanIter::switch_scan_group()
{
  int ret = OB_SUCCESS;
  //TODO shengle Unified interface with das rescan
  if (row_store_.need_check_output_datum_) {
    reset_expr_datum_ptr();
  }
  ++cur_group_idx_;
  if (cur_group_idx_ >= group_size_) {
    ret = OB_ITER_END;
  }
  LOG_DEBUG("switch scan group", K(ret), K(*this), KP(this));

  return ret;
}

OB_SERIALIZE_MEMBER(ObGroupScanIter, cur_group_idx_, group_size_);

}  // namespace sql
}  // namespace oceanbase
