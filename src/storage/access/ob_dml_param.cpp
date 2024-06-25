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
#include "ob_dml_param.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_dml_param.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share::schema;

int ObRow2ExprsProjector::init(const sql::ObExprPtrIArray &exprs,
                               sql::ObPushdownOperator &op,
                               const common::ObIArray<int32_t> &projector)
{
  int ret = OB_SUCCESS;
  op_ = &op;
  if (outputs_.empty()) { // not inited
    // first traverse, treat MapConvert::start_ as count
    if (op.is_vectorized()) {
      if (has_virtual_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table scan with virtual column is not supported right now", K(ret));
      } else {
        // reset datum pointers to reserved buffer.
        FOREACH_CNT_X(e, exprs, OB_SUCC(ret)) {
          if (!(*e)->is_batch_result()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table scan output is not batch result", K(ret));
          } else {
            (*e)->locate_datums_for_update(op.get_eval_ctx(), op.get_batch_size());
          }
        }
      }
    }


    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(outputs_.prepare_allocate(exprs.count()))) {
      LOG_WARN("array prepare allocate failed", K(ret));
    } else {
      for (int64_t i = 0; i < exprs.count(); i++) {
        sql::ObExpr *e = exprs.at(i);
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

      int64_t other_end = other_idx_;
      // second traverse, setup MapConvert::end_ && outputs_
      for (int64_t i = 0; i < exprs.count(); i++) {
        sql::ObExpr *e = exprs.at(i);
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
        Item &item = outputs_[item_idx];
        item.obj_idx_ = obj_idx;
        item.expr_idx_ = i;
        if (!op.is_vectorized()) {
          item.datum_ = &e->locate_datum_for_write(op_->get_eval_ctx());
        } else {
          item.datum_ = e->locate_batch_datums(op_->get_eval_ctx());
          item.eval_flags_ = &e->get_evaluated_flags(op_->get_eval_ctx());
        }
        item.eval_info_ = &e->get_eval_info(op_->get_eval_ctx());
        item.data_ = item.datum_->ptr_;
      }
    }
  }
  return ret;
};

template <common::ObObjDatumMapType OBJ_DATUM_MAP_TYPE, bool NEED_RESET_PTR>
OB_INLINE void ObRow2ExprsProjector::MapConvert<OBJ_DATUM_MAP_TYPE, NEED_RESET_PTR>::project(
    const ObRow2ExprsProjector::Item *items, const blocksstable::ObStorageDatum *datums,
    int16_t *nop_pos, int64_t &nop_cnt) const
{
  // performance critical, no parameter validity check.
  for (int32_t i = start_; i < end_; i++) {
    const Item &item = items[i];
    const blocksstable::ObStorageDatum &datum = datums[item.obj_idx_];
    if (OB_UNLIKELY(datum.is_nop())) {
      nop_pos[nop_cnt++] = item.expr_idx_;
    } else if (OB_UNLIKELY(datum.is_null())) {
      item.datum_->set_null();
    } else {
      if (NEED_RESET_PTR) {
        if (OB_UNLIKELY(item.datum_->ptr_ != item.data_)) {
          item.datum_->ptr_ = item.data_;
        }
      }
      item.datum_->datum2datum<OBJ_DATUM_MAP_TYPE>(datum);
    }
  }
}

template <common::ObObjDatumMapType OBJ_DATUM_MAP_TYPE, bool NEED_RESET_PTR>
OB_INLINE void ObRow2ExprsProjector::MapConvert<OBJ_DATUM_MAP_TYPE,
          NEED_RESET_PTR>::project_batch_datum(
              const ObRow2ExprsProjector::Item *items, const blocksstable::ObStorageDatum *datums,
              int16_t *nop_pos, int64_t &nop_cnt, const int64_t idx) const
{
  // performance critical, no parameter validity check.
  for (int32_t i = start_; i < end_; i++) {
    const Item &item = items[i];
    const blocksstable::ObStorageDatum &datum = datums[item.obj_idx_];
    if (OB_UNLIKELY(datum.is_nop())) {
      nop_pos[nop_cnt++] = item.expr_idx_;
    } else if (OB_UNLIKELY(datum.is_null())) {
      item.datum_[idx].set_null();
    } else {
      item.datum_[idx].datum2datum<OBJ_DATUM_MAP_TYPE>(datum);
    }
  }
}

int ObRow2ExprsProjector::project(const sql::ObExprPtrIArray &exprs,
                                  const blocksstable::ObStorageDatum *datums,
                                  int16_t *nop_pos,
                                  int64_t &nop_cnt)
{
  // performance critical, no parameter validity check.
  int ret = OB_SUCCESS;
  if (!op_->is_vectorized()) {
    num_.project(outputs_.get_data(), datums, nop_pos, nop_cnt);
    str_.project(outputs_.get_data(), datums, nop_pos, nop_cnt);
    int_.project(outputs_.get_data(), datums, nop_pos, nop_cnt);

    for (int64_t i = other_idx_; OB_SUCC(ret) && i < outputs_.count(); i++) {
      const Item &item = outputs_.at(i);
      const blocksstable::ObStorageDatum *datum = NULL;
      if (OB_UNLIKELY(item.obj_idx_ < 0
                      || (datum = &datums[item.obj_idx_])->is_nop())) {
        nop_pos[nop_cnt++] = item.expr_idx_;
        item.eval_info_->evaluated_ = false;
      } else if (OB_UNLIKELY(datum->is_null())) {
        item.datum_->set_null();
        item.eval_info_->evaluated_ = true;
        item.eval_info_->projected_ = true;
      } else {
        if (OB_UNLIKELY(item.datum_->ptr_ != item.data_)) {
          item.datum_->ptr_ = item.data_;
        }
        if (OB_FAIL(item.datum_->from_storage_datum(*datum, exprs.at(item.expr_idx_)->obj_datum_map_))) {
          LOG_WARN("convert obj to datum failed", K(ret), K(i), K(item), KPC(datum));
        } else {
          // the other items may contain virtual columns, set evaluated flag.
          item.eval_info_->evaluated_ = true;
          item.eval_info_->projected_ = true;
        }
      }
    }
  } else {
    const int64_t idx = op_->get_eval_ctx().get_batch_idx();
    num_.project_batch_datum(outputs_.get_data(), datums, nop_pos, nop_cnt, idx);
    str_.project_batch_datum(outputs_.get_data(), datums, nop_pos, nop_cnt, idx);
    int_.project_batch_datum(outputs_.get_data(), datums, nop_pos, nop_cnt, idx);

    for (int64_t i = other_idx_; OB_SUCC(ret) && i < outputs_.count(); i++) {
      const Item &item = outputs_.at(i);
      const blocksstable::ObStorageDatum *datum = NULL;
      item.eval_info_->evaluated_ = true;
      if (OB_UNLIKELY(item.obj_idx_ < 0
                      || (datum = &datums[item.obj_idx_])->is_nop())) {
        nop_pos[nop_cnt++] = item.expr_idx_;
        item.eval_flags_->unset(idx);
      } else if (OB_UNLIKELY(datum->is_null())) {
        item.datum_[idx].set_null();
        item.eval_flags_->set(idx);
      } else {
        if (OB_FAIL(item.datum_[idx].from_storage_datum(
                    *datum, exprs.at(item.expr_idx_)->obj_datum_map_))) {
          LOG_WARN("convert obj to datum failed", K(ret), K(i), K(item), KPC(datum));
        } else {
          // the other items may contain virtual columns, set evaluated flag.
          item.eval_flags_->set(idx);
        }
      }
    }
  }

  return ret;
}

DEF_TO_STRING(ObDMLBaseParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_TIMEOUT, timeout_,
       N_SCHEMA_VERSION, schema_version_,
       N_SQL_MODE, sql_mode_,
       N_IS_TOTAL_QUANTITY_LOG, is_total_quantity_log_,
       KPC_(table_param),
       K_(tenant_schema_version),
       K_(is_ignore),
       K_(prelock),
       KPC_(encrypt_meta),
       K_(is_batch_stmt),
       K_(write_flag),
       K_(spec_seq_no),
       K_(snapshot),
       K_(branch_id),
       K_(direct_insert_task_id),
       K_(check_schema_version),
       K_(ddl_task_id));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObRow2ExprsProjector::Item)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(obj_idx_), K(expr_idx_), K(datum_), K(eval_info_), K(data_));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObTableScanParam)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tablet_id),
       K_(ls_id),
       N_COLUMN_IDS, column_ids_,
       N_INDEX_ID, index_id_,
       N_KEY_RANGES, key_ranges_,
       K_(ss_key_ranges),
       K_(range_array_pos),
       N_TIMEOUT, timeout_,
       N_SCAN_FLAG, scan_flag_,
       N_SQL_MODE, sql_mode_,
       N_RESERVED_CELL_COUNT, reserved_cell_count_,
       N_SCHEMA_VERSION, schema_version_,
       N_QUERY_BEGIN_SCHEMA_VERSION, tenant_schema_version_,
       N_LIMIT_OFFSET, limit_param_,
       N_FOR_UPDATE, for_update_,
       N_WAIT, for_update_wait_timeout_,
       N_FROZEN_VERSION, frozen_version_,
       K_(is_get),
       KPC_(output_exprs),
       KPC_(op_filters),
       KPC_(trans_desc),
       K_(snapshot),
       KPC_(table_param),
       K_(sample_info),
       K_(need_scn),
       K_(need_switch_param),
       K_(is_mds_query),
       K_(fb_read_tx_uncommitted),
       K_(external_file_format),
       K_(external_file_location));
  J_OBJ_END();
  return pos;
}
}
}
