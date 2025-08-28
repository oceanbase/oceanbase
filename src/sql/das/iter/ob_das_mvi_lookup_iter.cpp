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
#include "sql/das/iter/ob_das_mvi_lookup_iter.h"
#include "sql/das/iter/ob_das_lookup_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_scan_op.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASMVILookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  bool got_next_row = false;
  do {
    switch (state_) {
      case INDEX_SCAN: {
        index_table_iter_->clear_evaluated_flag();
        if (OB_SUCC(index_table_iter_->get_next_row())) {
          bool has_rowkey = check_has_rowkey();
          if (has_rowkey) {
            lookup_rowkey_cnt_++;
            state_ = LookupState::OUTPUT_ROWS;
          } else {
            state_ = DO_LOOKUP;
          }
        }

        if (OB_ITER_END == ret) {
          state_ = FINISHED;
        }
        break;
      }
      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = LookupState::OUTPUT_ROWS;
        }
        break;
      }
      case LookupState::OUTPUT_ROWS: {
        if (lookup_rowkey_cnt_ != 0) {
          lookup_rowkey_cnt_ = 0;
        } else {
          data_table_iter_->clear_evaluated_flag();
          if (OB_FAIL(data_table_iter_->get_next_row())) {
            LOG_WARN("failed to get next row from data table", K(ret));  
          }
        }

        got_next_row = true;
        state_ = LookupState::INDEX_SCAN;
        break;
      }
      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected loopup state", K(state_), K(ret));
        break;
      }
    }
  } while (OB_SUCC(ret) && !got_next_row);

  return ret;
}

int ObDASMVILookupIter::save_rowkey()
{
  int ret = OB_SUCCESS;

  const ObDASScanCtDef *index_table_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
  const ObDASScanCtDef *data_table_ctdef = static_cast<const ObDASScanCtDef*>(lookup_ctdef_);
  
  ObDASScanIter *scan_iter = static_cast<ObDASScanIter *>(data_table_iter_);
  ObTableScanParam &scan_param = scan_iter->get_scan_param(); 
  
  int64_t rowkey_cnt = index_table_ctdef->rowkey_exprs_.count();
  ObExpr *doc_id_expr = index_table_ctdef->result_output_.at(rowkey_cnt);
  ObDatum &doc_id_datum = doc_id_expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
  if (OB_UNLIKELY(doc_id_datum.is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("docid and rowkey can't both be null", K(ret));
  } else {
    ObObj *obj_ptr = nullptr;
    ObArenaAllocator &allocator = get_arena_allocator();
    if (OB_ISNULL(obj_ptr = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate buffer failed", K(ret));
    } else {
      obj_ptr = new(obj_ptr) ObObj;

      if (OB_FAIL(doc_id_datum.to_obj(*obj_ptr, doc_id_expr->obj_meta_, doc_id_expr->obj_datum_map_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else {
        ObRowkey aux_table_rowkey(obj_ptr, 1);
        ObNewRange lookup_range;
        if (OB_FAIL(lookup_range.build_range(lookup_ctdef_->ref_table_id_, aux_table_rowkey))) {
          LOG_WARN("failed to build lookup range", K(ret), K(lookup_ctdef_->ref_table_id_), K(aux_table_rowkey));
        } else if (OB_FAIL(scan_param.key_ranges_.push_back(lookup_range))) {
          LOG_WARN("failed to push back lookup range", K(ret));
        } else {
          scan_param.is_get_ = true;
        }
      }
    }
  }

  return ret;
}

int ObDASMVILookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASMVILookupIter::inner_get_next_row())) {
    LOG_WARN("ObDASMVILookupIter failed to get next row", K(ret));
  } else {
    count = 1;
  }
  return ret;
}

int ObDASMVILookupIter::do_index_lookup()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(data_table_iter_) && OB_FAIL(data_table_iter_->reuse())) {
    LOG_WARN("failed to reuse data table iter");
  } else if (OB_FAIL(save_rowkey())) {
    LOG_WARN("failed to save rowkey", K(ret));
  } else if (OB_FAIL(ObDASLocalLookupIter::do_index_lookup())) {
    LOG_WARN("failed to do index lookup", K(ret));
  }

  return ret;
}

bool ObDASMVILookupIter::check_has_rowkey()
{
  const ObDASScanCtDef *index_table_ctdef = static_cast<const ObDASScanCtDef*>(index_ctdef_);
  const ObDASScanCtDef *data_table_ctdef = static_cast<const ObDASScanCtDef*>(lookup_ctdef_);
  int64_t rowkey_col_cnt = index_table_ctdef->rowkey_exprs_.count();
  int64_t rowkey_null_col_cnt = 0;
  
  for (int64_t i = 0; i < rowkey_col_cnt; ++i) {
    ObExpr *expr = index_table_ctdef->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      // do nothing
    } else {
      ObDatum &datum = expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
      if (datum.is_null()) {
        rowkey_null_col_cnt++;
      }
    }
  }

  return rowkey_null_col_cnt != rowkey_col_cnt;
}


}  // namespace sql
}  // namespace oceanbase
