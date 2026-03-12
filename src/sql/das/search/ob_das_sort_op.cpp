/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_das_sort_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASSortOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(child_))) {
    LOG_WARN("failed to push child op", KR(ret), KPC(child_));
  }
  return ret;
}

int ObDASSortOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASSortOpParam &sort_op_param = static_cast<const ObDASSortOpParam &>(op_param);
  if (OB_UNLIKELY(get_rowid_exprs().count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowid exprs count", K(ret));
  } else if (OB_FAIL(search_ctx_.create_rowid_store(max_batch_size(), rowid_store_))) {
    LOG_WARN("failed to create rowid store", K(ret), K(max_batch_size()));
  } else if (OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else if (FALSE_IT(rowid_store_iter_.init(rowid_store_))) {
  } else if (OB_ISNULL(sort_op_param.get_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret));
  } else if (OB_UNLIKELY(sort_op_param.get_child()->get_op_type() != ObDASSearchOpType::DAS_SEARCH_OP_SCALAR_SCAN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op type", K(ret), K(sort_op_param.get_child()->get_op_type()));
  } else if (OB_ISNULL(scan_op_ = static_cast<ObDASScalarScanOp *>(sort_op_param.get_child()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(sort_collations_.init(get_rowid_exprs().count()))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(sort_cmp_funcs_.init(get_rowid_exprs().count()))) {
    LOG_WARN("failed to init sort cmp funcs", K(ret));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < get_rowid_exprs().count(); ++i) {
      ObExpr *expr = get_rowid_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret), K(expr));
      } else {
        ObSortFieldCollation field_collation(i, expr->datum_meta_.cs_type_, true, NULL_FIRST);
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                 expr->datum_meta_.type_,
                                                                 field_collation.null_pos_,
                                                                 field_collation.cs_type_,
                                                                 expr->datum_meta_.scale_,
                                                                 lib::is_oracle_mode(),
                                                                 expr->obj_meta_.has_lob_header(),
                                                                 expr->datum_meta_.precision_,
                                                                 expr->datum_meta_.precision_);
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cmp func", K(ret));
        } else if (OB_FAIL(sort_collations_.push_back(field_collation))) {
          LOG_WARN("failed to push back sort collation", K(ret));
        } else if (OB_FAIL(sort_cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort cmp func", K(ret));
        }
      }
    }
  }
	return ret;
}

int ObDASSortOp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(scan_op_->open())) {
    LOG_WARN("failed to open scan op", K(ret));
  } else {
    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASSortOp", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(sort_memctx_, context_param))) {
      LOG_WARN("create sort memctx failed", K(ret));
    } else if (OB_ISNULL(sort_memctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sort memctx", K(ret));
    } else {
      ObIAllocator &allocator = sort_memctx_->get_arena_allocator();
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSortOpImpl)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate ObSortOpImpl memory", K(ret), KP(buf));
      } else if (OB_ISNULL(sort_impl_ = new (buf) ObSortOpImpl())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate ObSortOpImpl memory", K(ret), KP(buf));
      } else if (OB_FAIL(sort_impl_->init(MTL_ID(),
                                          &sort_collations_,
                                          &sort_cmp_funcs_,
                                          &eval_ctx(),
                                          &eval_ctx().exec_ctx_,
                                          false,     // enable encode sort key
                                          false,     // is local order
                                          false))) { // need rewind
        LOG_WARN("failed to init sort impl", K(ret));
      }
    }
  }
  return ret;
}

int ObDASSortOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(scan_op_->rescan())) {
    LOG_WARN("failed to rescan scan op", K(ret));
  } else {
    sort_impl_->reuse();
    if (OB_NOT_NULL(rowid_store_)) {
      rowid_store_->reuse();
      rowid_store_iter_.reset();
      rowid_store_iter_.init(rowid_store_);
    }
  }
  return ret;
}

int ObDASSortOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(rowid_store_)) {
    rowid_store_->reset();
    rowid_store_iter_.reset();
    rowid_store_ = nullptr;
  }
  if (OB_NOT_NULL(sort_impl_))  {
    sort_impl_->destroy();
    sort_impl_->~ObSortOpImpl();
    sort_impl_ = nullptr;
  }
  if (OB_NOT_NULL(sort_memctx_))  {
    sort_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(sort_memctx_);
    sort_memctx_ = nullptr;
  }
  return ret;
}

int ObDASSortOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  score = 0.0;
  if(OB_ISNULL(rowid_store_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or sort impl", K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort())) {
    LOG_WARN("failed to do sort", K(ret));
  } else {
    rowid_store_iter_.next_idx();
    ret = rowid_store_iter_.lower_bound(target);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
        LOG_WARN("failed to get rowid", K(ret));
      }
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("falied to find lower bound in store", K(ret));
    } else {
      bool reached = false;
      int64_t storage_count = 0;
      while (OB_SUCC(ret) && !reached) {
        if (OB_FAIL(sort_impl_->get_next_batch(get_rowid_exprs(), max_batch_size(), storage_count))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next batch from sort impl", K(ret));
          } else if (storage_count > 0) {
            ret = OB_SUCCESS;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(search_ctx_.lower_bound_in_frame(target, &get_rowid_exprs(), storage_count, idx))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("falied to find lower bound", K(ret));
          }
        } else {
          rowid_store_->reuse();

          if (OB_SUCC(ret)) {
            if (OB_FAIL(rowid_store_->fill(idx, storage_count, get_rowid_exprs()))) {
              LOG_WARN("failed to fill rowid store", K(ret));
            } else if (rowid_store_->count() == 0) {
              // no row satisfy the condition, continue to get next batch
            } else if (FALSE_IT(rowid_store_iter_.reuse())) {
            } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
              LOG_WARN("failed to get rowid", K(ret));
            } else {
              reached = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASSortOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if(OB_ISNULL(rowid_store_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or sort impl", K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort())) {
    LOG_WARN("failed to do sort", K(ret));
  } else if (FALSE_IT(rowid_store_iter_.next_idx())) {
  } else if (!rowid_store_iter_.is_empty()) {
    if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
      LOG_WARN("failed to get next rowid", K(ret));
    }
  } else {
    int64_t storage_count = 0;
    rowid_store_->reuse();
    if (OB_FAIL(sort_impl_->get_next_batch(get_rowid_exprs(), max_batch_size(), storage_count))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows", K(ret));
      } else if (storage_count > 0) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_FAIL(rowid_store_->fill(0, storage_count, get_rowid_exprs()))) {
        LOG_WARN("failed to fill rowid store", K(ret));
      } else if (rowid_store_->count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty rowid store", K(ret));
      } else if (FALSE_IT(rowid_store_iter_.reuse())) {
      } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
        LOG_WARN("failed to get next rowid", K(ret));
      }
    }
  }
  return ret;
}

int ObDASSortOp::do_sort()
{
  int ret = OB_SUCCESS;
  ObBitVector *fake_skip = to_bit_vector(sort_memctx_->get_arena_allocator().alloc(ObBitVector::memory_size(max_batch_size())));
  fake_skip->init(max_batch_size());
  if (OB_ISNULL(scan_op_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      count = 0;
      if (OB_FAIL(scan_op_->get_next_rows(count, max_batch_size()))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed ro get next rows from child iter", K(ret));
        }
      }

      if (OB_SUCCESS == ret || (OB_ITER_END == ret && count > 0)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(sort_impl_->add_batch(get_rowid_exprs(), *fake_skip, count, 0, nullptr))) {
          ret = tmp_ret;
          LOG_WARN("failed to add batch to sort impl", K(tmp_ret));
        }
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(sort_impl_->sort())) {
        LOG_WARN("failed to do sort", K(ret));
      } else {
        sort_finished_ = true;
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
