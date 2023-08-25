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

#define USING_LOG_PREFIX COMMON
#include "share/ob_virtual_table_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/config/ob_server_config.h"
#include "share/object/ob_obj_cast.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/das/ob_das_location_router.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "deps/oblib/src/lib/alloc/memory_sanity.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace common
{

void ObVirtualTableIterator::reset()
{
  output_column_ids_.reset();
  reserved_column_cnt_ = 0;
  schema_guard_ = NULL;
  table_schema_ = NULL;
  index_schema_ = NULL;
  //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
  if (OB_LIKELY(NULL != allocator_ && NULL != cur_row_.cells_)) {
    allocator_->free(cur_row_.cells_);
  }
  cur_row_.cells_ = NULL;
  cur_row_.count_ = 0;
  key_ranges_.reset();
  row_calc_buf_.reset();
  reset_convert_ctx();
  allocator_ = NULL;
  session_ = NULL;
}

void ObVirtualTableIterator::reset_convert_ctx()
{
  if (OB_LIKELY(NULL != allocator_ && NULL != convert_row_.cells_)) {
    allocator_->free(convert_row_.cells_);
  }
  saved_key_ranges_.reset();
  cols_schema_.reset();
  convert_row_.cells_ = NULL;
  convert_row_.count_ = 0;
  need_convert_ = false;
  convert_alloc_.reset();
}

int ObVirtualTableIterator::free_convert_ctx()
{
  int ret = OB_SUCCESS;
  if (!need_convert_) {
  } else if (OB_UNLIKELY(convert_row_.count_ < 0 || NULL == convert_row_.cells_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("convert row is not init", K(ret), K(convert_row_));
  } else {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else {
      key_ranges_.reset();
      if (OB_FAIL(key_ranges_.assign(saved_key_ranges_))) {
        LOG_WARN("failed to assign key ranges", K(ret));
      }
      saved_key_ranges_.reset();
      //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
      allocator_->free(convert_row_.cells_);
      convert_row_.cells_ = NULL;
      convert_row_.count_ = 0;
      convert_alloc_.reset();
      cols_schema_.reset();
    }
  }
  return ret;
}

int ObVirtualTableIterator::convert_key(const ObRowkey &src, ObRowkey &dst, common::ObIArray<const ObColumnSchemaV2*> &key_cols)
{
  int ret = OB_SUCCESS;
  if (src.get_obj_cnt() > 0) {
    const ObObj *src_key_objs = src.get_obj_ptr();
    void *tmp_ptr = NULL;
    ObObj *new_key_obj = NULL;
    tmp_ptr = allocator_->alloc(src.get_obj_cnt() * sizeof(ObObj));
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (OB_ISNULL(new_key_obj = new (tmp_ptr) ObObj[src.get_obj_cnt()])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (src.get_obj_cnt() > key_cols.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keys are not match with columns", K(ret));
    }
    lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
    ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    for (uint64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < src.get_obj_cnt(); ++nth_obj) {
      const ObObj &src_obj = src_key_objs[nth_obj];
      if (src_obj.is_min_value()) {
        new_key_obj[nth_obj].set_min_value();
      } else if (src_obj.is_max_value()) {
        new_key_obj[nth_obj].set_max_value();
      } else if (src_obj.is_null()) {
        new_key_obj[nth_obj].set_null();
      } else {
        if (OB_FAIL(ObObjCaster::to_type(key_cols.at(nth_obj)->get_data_type(),
                                        cast_ctx,
                                        src_key_objs[nth_obj],
                                        new_key_obj[nth_obj]))) {
          LOG_WARN("fail to cast obj", K(ret), K(allocator_), K(key_cols.at(nth_obj)->get_data_type()), K(src_key_objs[nth_obj]));
        }
      }
    }//end for
    if (OB_SUCC(ret)) {
      dst.assign(new_key_obj, src.get_obj_cnt());
    }
  }
  return ret;
}

// get origin type of keys in mysql mode
// first find the column name that is same as origin virtual table in mysql mode
// then find column type by column name
int ObVirtualTableIterator::get_key_cols(common::ObIArray<const ObColumnSchemaV2*> &key_cols)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> column_ids;
  key_cols.reset();
  if (need_convert_ && !key_ranges_.empty()) {
    if (index_schema_->get_rowkey_info().is_valid()
        && index_schema_->get_rowkey_info().get_column_ids(column_ids)) {
      LOG_WARN("get key column ids failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      common::ObArray<const ObString*> column_names;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const ObColumnSchemaV2 * col_schema = table_schema_->get_column_schema(column_ids.at(i));
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret));
        } else if (OB_FAIL(column_names.push_back(&col_schema->get_column_name_str()))) {
          LOG_WARN("fail to push back column name", K(ret));
        }
      }
      if (OB_SUCC(ret) && column_ids.count() != column_names.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column infos are not match ", K(ret));
      }
      // get origin key type by column name
      if (OB_SUCC(ret)) {
        const ObTableSchema *org_table_schema = NULL;
        uint64_t org_table_id = get_origin_tid_by_oracle_mapping_tid(table_schema_->get_table_id());
        if (OB_INVALID_ID == org_table_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get origin table id", K(ret), K(table_schema_->get_table_id()));
        } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID, org_table_id, org_table_schema))) {
          LOG_WARN("get table schema failed", K(org_table_id), K(ret));
        } else if (NULL == org_table_schema) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("get table schema failed", K(ret));
        } else {
          // switch mysql mode, find column schema by column name
          lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
          for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
            const ObString *column_name = column_names.at(i);
            const ObColumnSchemaV2 *col_schema = org_table_schema->get_column_schema(*column_name);
            if (OB_ISNULL(col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(*column_name));
            } else if (OB_FAIL(key_cols.push_back(col_schema))) {
              LOG_WARN("fail to push back column name", K(ret));
            }
          }
          if (OB_SUCC(ret) && key_cols.count() != column_names.count()) {
            LOG_WARN("column infos are not match ", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// If key objects are in oracle mode, then need to convert to obj in mysql mode
// and it's find the origin type in mysql mode
// every virtual table in oracle mode must be match with one virtual table in mysql mode
int ObVirtualTableIterator::convert_key_ranges()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (!key_ranges_.empty()) {
    common::ObSEArray<common::ObNewRange, 16> tmp_range;
    common::ObArray<const ObColumnSchemaV2*> key_cols;
    if (OB_FAIL(get_key_cols(key_cols))) {
      LOG_WARN("failed to get key types", K(ret));
    } else if (key_cols.empty() && 1 == key_ranges_.count() && key_ranges_.at(0).is_whole_range()) {
      ObNewRange new_range;
      new_range.table_id_ = key_ranges_.at(0).table_id_;
      new_range.set_whole_range();
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
        ObNewRange new_range;
        new_range.table_id_ = key_ranges_.at(i).table_id_;
        new_range.border_flag_ = key_ranges_.at(i).border_flag_;
        if (OB_FAIL(convert_key(key_ranges_.at(i).start_key_, new_range.start_key_, key_cols))) {
          LOG_WARN("fail to convert start key", K(ret), K(allocator_));
        } else if (OB_FAIL(convert_key(key_ranges_.at(i).end_key_, new_range.end_key_, key_cols))) {
          LOG_WARN("fail to convert end key", K(ret), K(allocator_));
        } else if (OB_FAIL(tmp_range.push_back(new_range))) {
          LOG_WARN("fail to push back new range", K(ret), K(allocator_));
        }
      }//end for
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(saved_key_ranges_.assign(key_ranges_))) {
        LOG_WARN("fail to assign new range", K(ret), K(allocator_));
      } else {
        key_ranges_.reset();
        if (OB_FAIL(key_ranges_.assign(tmp_range))) {
          LOG_WARN("fail to assign new range", K(ret), K(allocator_));
        }
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::init_convert_ctx()
{
  int ret = OB_SUCCESS;
  row_calc_buf_.set_tenant_id(table_schema_->get_tenant_id());
  convert_alloc_.set_tenant_id(table_schema_->get_tenant_id());
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
  ObCastCtx cast_ctx(&convert_alloc_, &dtc_params, CM_NONE, table_schema_->get_collation_type());
  cast_ctx_ = cast_ctx;

  if (need_convert_) {
    ObObj *cells = NULL;
    void *tmp_ptr = NULL;
    if (OB_UNLIKELY(NULL == allocator_ || NULL == table_schema_ || NULL == session_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("data member is not init", K(ret), K(allocator_));
    } else if (OB_ISNULL(tmp_ptr = allocator_->alloc(reserved_column_cnt_ <= 0 ? 1 * sizeof(ObObj): reserved_column_cnt_ * sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "fail to alloc cells", K(ret), K(reserved_column_cnt_));
    } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[reserved_column_cnt_ <= 0 ? 1 : reserved_column_cnt_])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new cell array", K(ret), K(reserved_column_cnt_));
    } else {
      convert_row_.cells_ = cells;
      convert_row_.count_ = reserved_column_cnt_;
      if (OB_FAIL(convert_key_ranges())) {
        LOG_WARN("fail to convert key ranges", K(ret), K(key_ranges_), K(reserved_column_cnt_));
      }
    }
  }
  LOG_DEBUG("key ranges", K(ret), K(key_ranges_));
  return ret;
}

int ObVirtualTableIterator::open()
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = NULL;
  ObObj *cells = NULL;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(allocator_));
  } else if (OB_ISNULL(scan_param_)
             || (NULL != scan_param_->output_exprs_ && NULL == scan_param_->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(tmp_ptr = allocator_->alloc( (reserved_column_cnt_ > 0 ? reserved_column_cnt_ : 1) * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "fail to alloc cells", K(ret), K(reserved_column_cnt_));
  } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[(reserved_column_cnt_ > 0 ? reserved_column_cnt_ : 1)])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new cell array", K(ret), K(reserved_column_cnt_));
  } else {
    cur_row_.cells_ = cells;
    cur_row_.count_ = reserved_column_cnt_;
    if (OB_FAIL(init_convert_ctx())) {
      LOG_WARN("fail to init convert context", K(ret));
    } else if (OB_FAIL(inner_open())) {
      LOG_WARN("fail to inner open", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::get_all_columns_schema()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    const uint64_t column_id = output_column_ids_.at(i);
    const ObColumnSchemaV2 *col_schema = table_schema_->get_column_schema(column_id);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("col_schema is NULL", K(ret), K(column_id));
    } else if (OB_FAIL(cols_schema_.push_back(col_schema))) {
      LOG_WARN("failed to push back column schema", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::convert_output_row(ObNewRow *&cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is NULL", K(ret));
  } else if (!need_convert_) {
    // don't convert
  } else {
    convert_alloc_.reuse();
    if (cols_schema_.empty() && OB_FAIL(get_all_columns_schema())) {
      LOG_WARN("failed to get columns schema", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      const ObColumnSchemaV2 *col_schema = cols_schema_.at(i);
      if (cur_row->get_cell(i).is_null()
          || (cur_row->get_cell(i).is_string_type() && 0 == cur_row->get_cell(i).get_data_length())
          || ob_is_empty_lob(cur_row->get_cell(i))) {
        convert_row_.cells_[i].set_null();
      } else if (OB_FAIL(ObObjCaster::to_type(col_schema->get_data_type(),
                                              col_schema->get_collation_type(),
                                              cast_ctx_,
                                              cur_row->get_cell(i),
                                              convert_row_.cells_[i]))) {
        LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
      }
    }
    cur_row = &convert_row_;
  }
  return ret;
}

int ObVirtualTableIterator::get_next_row(ObNewRow *&row)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObNewRow *cur_row = NULL;
  row_calc_buf_.reuse();
  if (OB_FAIL(inner_get_next_row(cur_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to inner get next row", K(ret), KPC(scan_param_));
    }
  } else if (OB_ISNULL(cur_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get next row, but row is NULL", K(ret));
  } else if (OB_UNLIKELY(cur_row->count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row count is less than output column count", K(ret),
              K(cur_row->count_), K(output_column_ids_.count()));
  } else if (cur_row->count_ > 0 &&
             OB_ISNULL(cur_row->cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur_row->cells_ is NULL", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table schema is NULL", K(ret));
  } else if (OB_FAIL(convert_output_row(cur_row))) {
    LOG_WARN("failed to convert row", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    const uint64_t column_id = output_column_ids_.at(i);
    const ObColumnSchemaV2 *col_schema = table_schema_->get_column_schema(column_id);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("col_schema is NULL", K(ret), K(column_id));
    } else if (OB_UNLIKELY(col_schema->get_data_type() != cur_row->cells_[i].get_type()
                           && ObNullType != cur_row->cells_[i].get_type())) {
      ret = OB_ERR_UNEXPECTED;
      if (GCONF.in_upgrade_mode()) {
        LOG_WARN("column type in this row is not expected type", K(ret), K(i),
                 "table_name", table_schema_->get_table_name_str(),
                 "column_name", col_schema->get_column_name_str(),
                 K(column_id), K(cur_row->cells_[i]),
                 K(col_schema->get_data_type()), K(output_column_ids_));
      } else {
        LOG_ERROR("column type in this row is not expected type", K(ret), K(i),
                  "table_name", table_schema_->get_table_name_str(),
                  "column_name", col_schema->get_column_name_str(),
                  K(column_id), K(cur_row->cells_[i]), K(cur_row->cells_[i].get_type()),
                  K(col_schema->get_data_type()), K(output_column_ids_));
      }
    }
    if (OB_SUCC(ret)
        && is_lob_storage(col_schema->get_data_type())
        && !cur_row->cells_[i].has_lob_header()) { // cannot be json type;
        ObObj &obj_convert = cur_row->cells_[i];
      if (OB_FAIL(ObTextStringResult::ob_convert_obj_temporay_lob(obj_convert, row_calc_buf_))) {
        LOG_WARN("fail to add lob header", KR(ret), "object", cur_row->cells_[i]);
      }
    }
    if (OB_SUCC(ret) && ob_is_string_tc(col_schema->get_data_type())
        && (col_schema->get_data_length() < cur_row->cells_[i].get_string_len()
            // do charset convert when obj meta is different from expr meta
            || (CS_TYPE_INVALID != cur_row->cells_[i].get_collation_type()
                && col_schema->get_collation_type() != cur_row->cells_[i].get_collation_type()))) {
      //Check the column schema to ensure that it meets the schema definition;
      //But currently, only strings that exceed the length limit are processed to prevent occupying too many performance resources and causing too much interface delay
      ObObj output_obj;
      ObArray<ObString> *type_infos = NULL;
      const bool is_strict = false;
      ObExprResType res_type;
      res_type.set_accuracy(col_schema->get_accuracy());
      res_type.set_collation_type(col_schema->get_collation_type());
      res_type.set_type(col_schema->get_data_type());
      ObCastCtx cast_ctx = cast_ctx_;
      cast_ctx.cast_mode_ = cast_ctx_.cast_mode_ | CM_WARN_ON_FAIL;
      if (OB_FAIL(ObExprColumnConv::convert_skip_null_check(output_obj, cur_row->cells_[i],
                                                            res_type, is_strict, cast_ctx,
                                                            type_infos))) {
        LOG_WARN("fail to convert skip null check", KR(ret), "object", cur_row->cells_[i]);
      } else {
        cur_row->cells_[i] = output_obj;
        if (OB_SUCCESS != cast_ctx.warning_) {
          LOG_WARN("invalid row result, check schema", "warning_num", cast_ctx.warning_,
                   "object", cur_row->cells_[i], K(res_type));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = cur_row;
  }
  LOG_DEBUG("check result row", K(ret), KPC(row));
  return ret;
}

int ObVirtualTableIterator::get_next_row()
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObNewRow *row = NULL;
  if (OB_ISNULL(scan_param_)
      || OB_ISNULL(scan_param_->output_exprs_)
      || OB_ISNULL(scan_param_->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID == scan_param_->tablet_id_.id()) {
    row = NULL;
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(row)) {
    LOG_WARN("NULL row returned", K(ret));
  } else if (scan_param_->output_exprs_->count() > row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count less than output exprs", K(ret), K(*row),
             "output_exprs_cnt", scan_param_->output_exprs_->count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->output_exprs_->count(); i++) {
      ObExpr *expr = scan_param_->output_exprs_->at(i);
      ObDatum &datum = expr->locate_datum_for_write(scan_param_->op_->get_eval_ctx());
      if (OB_FAIL(datum.from_obj(row->cells_[i], expr->obj_datum_map_))) {
        LOG_WARN("convert ObObj to ObDatum failed", K(ret));
      } else if (is_lob_storage(row->cells_[i].get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(row->cells_[i], expr->obj_meta_,
                                             expr->obj_datum_map_, *allocator_, datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(i), K(row->cells_[i].get_meta()), K(expr->obj_meta_));
      } else {
        SANITY_CHECK_RANGE(datum.ptr_, datum.len_);
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_row_.count_ > 0 && NULL == cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_row is not init", K(ret), K(cur_row_));
  } else if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to execute inner close", K(ret));
  } else if (OB_FAIL(free_convert_ctx())) {
    LOG_WARN("fail to free convert context", K(ret));
  } else {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else {
      //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
      if (cur_row_.cells_ != NULL) {
        allocator_->free(cur_row_.cells_);
      }
      cur_row_.cells_ = NULL;
      cur_row_.count_ = 0;
    }
  }
  row_calc_buf_.reset();
  return ret;
}

// level_str support: db_acc, table_acc
// reference: ob_expr_sys_privilege_check.cpp:calc_resultN
int ObVirtualTableIterator::check_priv(const ObString &level_str,
                                       const ObString &db_name,
                                       const ObString &table_name,
                                       int64_t tenant_id,
                                       bool &passed)
{
  int ret = OB_SUCCESS;
  share::schema::ObSessionPrivInfo session_priv;
  CK (OB_NOT_NULL(session_) && OB_NOT_NULL(schema_guard_));
  OX (session_->get_session_priv_info(session_priv));
  // bool allow_show = true;
  if (OB_SUCC(ret)) {
    //tenant_id in table is static casted to int64_t,
    //and use statis_cast<uint64_t> for retrieving(same with schema_service)
    // schema拆分后，普通租户schema表的tenant_id为0，此时鉴权取session_priv.tenant_id_
    if (session_priv.tenant_id_ != static_cast<uint64_t>(tenant_id)
        && OB_INVALID_TENANT_ID != tenant_id) {
      //not current tenant's row
    } else if (0 == level_str.case_compare("db_acc")) {
      if (OB_FAIL(schema_guard_->check_db_show(session_priv, db_name, passed))) {
          LOG_WARN("Check db show failed", K(ret));
      }
    } else if (0 == level_str.case_compare("table_acc")) {
      //if (OB_FAIL(priv_mgr.check_table_show(session_priv,
      if (OB_FAIL(schema_guard_->check_table_show(session_priv, db_name, table_name, passed))) {
        LOG_WARN("Check table show failed", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Check priv level error", K(ret));
    }
  }
  return ret;
}

void ObVirtualTableIterator::set_effective_tenant_id(const uint64_t tenant_id)
{
  effective_tenant_id_ = tenant_id;
}

}// common
}// oceanbase
