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
#include "storage/ob_dml_param.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_param.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace storage {
using namespace common;
using namespace share::schema;

int ObRow2ExprsProjector::init(
    const sql::ObExprPtrIArray& exprs, sql::ObEvalCtx& eval_ctx, const common::ObIArray<int32_t>& projector)
{
  int ret = OB_SUCCESS;
  if (outputs_.empty()) {  // not inited
    // first traverse, treat MapConvert::start_ as count
    for (int64_t i = 0; i < exprs.count(); i++) {
      sql::ObExpr* e = exprs.at(i);
      // output should always be T_COLUMN_REF, only virtual column has argument.
      if (e->arg_cnt_ > 0) {
        has_virtual_ = true;
      } else {
        const int32_t obj_idx = projector.at(i);
        if (obj_idx >= 0) {
          common::ObObjDatumMapType map_type = e->obj_datum_map_;
          if (num_.map_type_ == map_type) {
            num_.start_++;
          } else if (str_.map_type_ == map_type) {
            str_.start_++;
          } else if (int_.map_type_ == map_type) {
            int_.start_++;
          }
        }
      }
    }

    // setup MapConvert::start_ and assign %start_ to %end_
    other_idx_ = num_.start_ + str_.start_ + int_.start_;
    int_.start_ = int_.end_ = num_.start_ + str_.start_;
    str_.start_ = str_.end_ = num_.start_;
    num_.start_ = num_.end_ = 0;

    if (OB_FAIL(outputs_.prepare_allocate(exprs.count()))) {
      LOG_WARN("array prepare allocate failed", K(ret));
    } else {
      int64_t other_end = other_idx_;
      // second traverse, setup MapConvert::end_ && outputs_
      for (int64_t i = 0; i < exprs.count(); i++) {
        sql::ObExpr* e = exprs.at(i);
        const int32_t obj_idx = projector.at(i);
        int64_t item_idx = 0;
        if (e->arg_cnt_ == 0 && obj_idx >= 0) {
          common::ObObjDatumMapType map_type = e->obj_datum_map_;
          if (num_.map_type_ == map_type) {
            item_idx = num_.end_++;
          } else if (str_.map_type_ == map_type) {
            item_idx = str_.end_++;
          } else if (int_.map_type_ == map_type) {
            item_idx = int_.end_++;
          } else {
            item_idx = other_end++;
          }
        } else {
          item_idx = other_end++;
        }
        Item& item = outputs_[item_idx];
        item.obj_idx_ = obj_idx;
        item.expr_idx_ = i;
        item.datum_ = &e->locate_datum_for_write(eval_ctx);
        item.eval_info_ = &e->get_eval_info(eval_ctx);
        item.data_ = item.datum_->ptr_;
      }
    }
  }
  return ret;
};

int ObRow2ExprsProjector::project(
    const sql::ObExprPtrIArray& exprs, const common::ObObj* cells, int16_t* nop_pos, int64_t& nop_cnt)
{
  // performance critical, no parameter validity check.
  int ret = OB_SUCCESS;
  num_.project(outputs_.get_data(), cells, nop_pos, nop_cnt);
  str_.project(outputs_.get_data(), cells, nop_pos, nop_cnt);
  int_.project(outputs_.get_data(), cells, nop_pos, nop_cnt);

  for (int64_t i = other_idx_; OB_SUCC(ret) && i < outputs_.count(); i++) {
    const Item& item = outputs_.at(i);
    const ObObj* cell = NULL;
    if (OB_UNLIKELY(item.obj_idx_ < 0 || (cell = &cells[item.obj_idx_])->is_nop_value()) || (cell->is_urowid())) {
      // need to calc urowid col every time. otherwise may get old value.
      nop_pos[nop_cnt++] = item.expr_idx_;
    } else if (OB_UNLIKELY(cell->is_null())) {
      item.datum_->set_null();
      item.eval_info_->evaluated_ = true;
    } else {
      if (OB_UNLIKELY(item.datum_->ptr_ != item.data_)) {
        item.datum_->ptr_ = item.data_;
      }
      if (OB_FAIL(item.datum_->from_obj(*cell, exprs.at(item.expr_idx_)->obj_datum_map_))) {
        LOG_WARN("convert obj to datum failed");
      } else {
        // the other items may contain virtual columns, set evaluated flag.
        item.eval_info_->evaluated_ = true;
      }
    }
  }

  return ret;
}

// temporarily no one will use this func, hope someone will start this feature
int ObTableScanParam::init_rowkey_column_orders()
{
  int ret = OB_SUCCESS;
  int64_t rowkey_count = 0;
  void* buf = NULL;

  if (!is_valid() || OB_NOT_NULL(column_orders_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "can only call get_roweky_column_order on a fully prepared ObTableScanParam",
        KP_(trans_desc),
        KP_(column_orders),
        K(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObArray<ObOrderType>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memrory", K(ret));
  } else {
    const common::ObIArray<ObColumnParam*>& rowkey_columns =
        scan_flag_.is_index_back() ? table_param_->get_index_columns() : table_param_->get_columns();
    rowkey_count =
        scan_flag_.is_index_back() ? table_param_->get_index_rowkey_cnt() : table_param_->get_main_rowkey_cnt();
    column_orders_ = new (buf) ObArray<ObOrderType>();

    if (OB_FAIL(column_orders_->reserve(rowkey_count))) {
      STORAGE_LOG(WARN, "fail to reserve array", K(ret), K(rowkey_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count; i++) {
      if (OB_FAIL(column_orders_->push_back(rowkey_columns.at(i)->get_column_order()))) {
        STORAGE_LOG(WARN, "fail to push back column order", K(ret), K(i));
      } else if (OB_UNLIKELY(column_orders_->at(i) != common::ObOrderType::ASC)) {
        /*
        // for now SQL layer cannot handle reverse index
        // so we shouldn't get DESC value; otherwise something is wrong
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "got an unexpected DESC value!", K(column_orders_), K(rowkey_columns), K(ret));
        */

        // FIXME-: to preserve the previous behavior, we treat DESC as ASC
        // need to change this later
        column_orders_->at(i) = common::ObOrderType::ASC;
        STORAGE_LOG(ERROR, "should not have gotten DESC colum order! Treat is as ASC", K(rowkey_columns));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(column_orders_)) {
      column_orders_->~ObArray<ObOrderType>();
      allocator_->free(column_orders_);
      column_orders_ = NULL;
    }
  }

  return ret;
}

DEF_TO_STRING(ObRow2ExprsProjector::Item)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(obj_idx_), K(expr_idx_), K(datum_), K(eval_info_), K(data_));
  J_OBJ_END();
  return pos;
}

}  // namespace storage
}  // namespace oceanbase
