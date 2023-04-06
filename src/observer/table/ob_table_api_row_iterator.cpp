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

#define USING_LOG_PREFIX SERVER
#include "ob_table_api_row_iterator.h"
#include "ob_table_rpc_processor.h"
#include "observer/ob_service.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;


namespace oceanbase {
namespace observer {

/**
 * -------------------------------------------------------ObTableApiRowIterator--------------------------------------------------------
 */
ObTableApiRowIterator::ObTableApiRowIterator()
  : part_service_(NULL),
    schema_service_(NULL),
    ctx_(NULL),
    schema_guard_(share::schema::ObSchemaMgrItem::MOD_TABLE_API_ROW_ITER),
    table_schema_(NULL),
    table_id_(0),
    tenant_id_(0),
    schema_version_(0),
    rowkey_column_cnt_(0),
    properties_(),
    column_ids_(),
    columns_type_(),
    column_descs_(),
    row_objs_(),
    missing_default_objs_(),
    generate_column_exprs_(),
    generate_column_idxs_(),
    expr_ctx_(),
    row_(),
    stmt_allocator_(ObModIds::TABLE_BATCH_OPERATION),
    row_allocator_(ObModIds::TABLE_BATCH_OPERATION),
    entity_(NULL),
    has_generate_column_(false),
    is_inited_(false)
{
}

ObTableApiRowIterator::~ObTableApiRowIterator()
{
  if (has_generate_column_) {
    sql::ObSQLUtils::destruct_default_expr_context(expr_ctx_);
    has_generate_column_ = false;
  }
}

int ObTableApiRowIterator::init(
    storage::ObPartitionService &partition_service,
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObTableServiceCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The table api row iterator has been inited, ", K(ret));
  } else if (OB_FAIL(schema_service.get_schema_guard(schema_guard_))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schema(ctx.param_.table_id_, table_schema_))) {
    LOG_WARN("get table schema failed", K(ctx.param_.table_id_), K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL ptr", K(ret), K(table_schema_));
  } else if (OB_FAIL(check_table_supported(table_schema_))) {
    LOG_WARN("check table support failed", K(ret));
  } else {
    table_id_ = ctx.param_.table_id_;
    tenant_id_ = extract_tenant_id(table_id_);
    schema_version_ = table_schema_->get_schema_version();
    rowkey_column_cnt_ = table_schema_->get_rowkey_column_num();
    part_service_ = &partition_service;
    ctx_ = &ctx;
    stmt_allocator_.set_tenant_id(tenant_id_);
    row_allocator_.set_tenant_id(tenant_id_);
    is_inited_ = true;
  }
  return ret;
}

void ObTableApiRowIterator::reset()
{
  if (has_generate_column_) {
    sql::ObSQLUtils::destruct_default_expr_context(expr_ctx_);
  }
  part_service_ = NULL;
  schema_service_ = NULL;
  ctx_ = NULL;
  table_schema_ = NULL;
  table_id_ = 0;
  tenant_id_ = 0;
  schema_version_ = 0;
  rowkey_column_cnt_ = 0;
  properties_.reset();
  column_ids_.reset();
  columns_type_.reset();
  column_descs_.reset();
  row_objs_.reset();
  missing_default_objs_.reset();
  generate_column_exprs_.reset();
  generate_column_idxs_.reset();
  stmt_allocator_.reset();
  row_allocator_.reset();
  entity_ = NULL;
  has_generate_column_ = false;
  is_inited_ = false;
}

int ObTableApiRowIterator::check_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t N = columns_type_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(check_column_type(columns_type_.at(i), row.get_cell(i)))) {
      LOG_WARN("Fail to check column type, ", K(ret), K(i), K(columns_type_.at(i)), K(row.get_cell(i)));
    }
  } // end for
  return ret;
}

int ObTableApiRowIterator::entity_to_row(const ObITableEntity &entity, ObIArray<ObObj> &row)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  const int64_t N = entity.get_rowkey_size();
  row.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(entity.get_rowkey_value(i, obj))) {
      LOG_WARN("failed to get rowkey value", K(ret), K(i));
    } else if (OB_FAIL(row.push_back(obj))) {
      LOG_WARN("failed to push", K(ret), K(i), K(obj));
    }
  } // end for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(entity.get_properties_values(row))) {
      LOG_WARN("failed to get properties values", K(ret));
    }
  }
  return ret;
}

int ObTableApiRowIterator::fill_get_param(
    ObTableServiceCtx &ctx,
    const ObTableOperationType::Type op_type,
    ObRowkey &rowkey,
    storage::ObTableScanParam &scan_param,
    share::schema::ObTableParam &table_param)
{
  int ret = OB_SUCCESS;
  scan_param.key_ranges_.reset();
  if (OB_FAIL(fill_range(rowkey, scan_param.key_ranges_))) {
    LOG_WARN("Fail to fill range, ", K(ret));
  } else if (OB_FAIL(fill_flag(ctx, scan_param))) {
    LOG_WARN("Fail to fill param flag, ", K(ret));
  } else if (OB_FAIL(table_param.convert(*table_schema_, *table_schema_, column_ids_, false /*index back*/))) {
    LOG_WARN("failed to convert table param", K(ret));
  } else {
    if (ObTableOperationType::DEL == op_type
        || ObTableOperationType::UPDATE == op_type
        || ObTableOperationType::INSERT_OR_UPDATE == op_type
        || ObTableOperationType::REPLACE == op_type
        || ObTableOperationType::INCREMENT == op_type
        || ObTableOperationType::APPEND == op_type) {
      scan_param.for_update_ = true;
    }
    scan_param.table_param_ = &table_param;
    LOG_DEBUG("Success to fill get param.");
  }
  return ret;
}

int ObTableApiRowIterator::fill_multi_get_param(
    ObTableServiceCtx &ctx,
    const ObTableBatchOperation &batch_operation,
    storage::ObTableScanParam &scan_param,
    share::schema::ObTableParam &table_param)
{
  int ret = OB_SUCCESS;
  scan_param.key_ranges_.reset();
  const int64_t N = batch_operation.count();
  if (N <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(N));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObRowkey rowkey = const_cast<ObITableEntity &>(batch_operation.at(i).entity()).get_rowkey();
    if (OB_FAIL(fill_range(rowkey, scan_param.key_ranges_))) {
      LOG_WARN("Fail to fill range, ", K(ret), K(i));
    }
  } // end for

  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_flag(ctx, scan_param))) {
      LOG_WARN("Fail to fill param flag, ", K(ret));
    } else if (OB_FAIL(table_param.convert(*table_schema_, *table_schema_, column_ids_, false /*index back*/))) {
      LOG_WARN("failed to convert table param", K(ret));
    } else {
      const ObTableOperation &table_operation = batch_operation.at(0);
      if (ObTableOperationType::DEL == table_operation.type()
          || ObTableOperationType::UPDATE == table_operation.type()
          || ObTableOperationType::INSERT_OR_UPDATE == table_operation.type()
          || ObTableOperationType::REPLACE == table_operation.type()
          || ObTableOperationType::INCREMENT == table_operation.type()
          || ObTableOperationType::APPEND == table_operation.type()) {
        scan_param.for_update_ = true;
      }
      scan_param.table_param_ = &table_param;
    }
  }
  return ret;
}

int ObTableApiRowIterator::fill_generate_columns(common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObISqlExpression *expr = NULL;
  const int64_t N = generate_column_exprs_.count();
  int64_t col_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_FAIL(generate_column_exprs_.at(i, expr))) {
      LOG_WARN("Fail to get generate column expr, ", K(ret), K(i));
    } else if (OB_FAIL(generate_column_idxs_.at(i, col_idx))) {
      LOG_WARN("Fail to get generate column idx, ", K(ret), K(i));
    } else if (NULL != expr) {
      if (OB_FAIL(sql::ObSQLUtils::calc_sql_expression(
          expr,
          *table_schema_,
          column_descs_,
          row,
          row_allocator_,
          expr_ctx_,
          row.cells_[col_idx]))) {
        LOG_WARN("failed to calc expr from str", K(column_descs_), K(ret));
      } else {
        LOG_DEBUG("Success to calc expr, ", K(col_idx), K(row.cells_[col_idx]));
      }
    }
  }
  return ret;
}


int ObTableApiRowIterator::cons_all_columns(const ObITableEntity &entity,
                                            const bool ignore_missing_column,
                                            const bool allow_rowkey_in_properties)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema_->get_rowkey_info();
  if (OB_FAIL(entity.get_properties_names(properties_))) {
    LOG_WARN("failed to get properties, ", K(ret));
  } else if (OB_FAIL(rowkey_info.get_column_ids(column_ids_))) {
    LOG_WARN("failed to get rowkey column ids", K(ret), K(rowkey_info));
  } else {
    const schema::ObColumnSchemaV2 *column_schema = NULL;
    uint64_t column_id = OB_INVALID_ID;

    //add rowkey column
    const int64_t rowkey_cnt = rowkey_info.get_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i));
      } else if (NULL == (column_schema = table_schema_->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey column not exists", K(ret), K(column_id));
      } else if (OB_FAIL(add_column_type(*column_schema))) {
        LOG_WARN("Fail to add column type, ", K(ret));
      }
    } // end for

    //add property column
    const int64_t property_cnt = properties_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < property_cnt; ++i) {
      const ObString &cname = properties_.at(i);
      if (NULL == (column_schema = table_schema_->get_column_schema(cname))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("column not exists", K(ret), K(cname));
      } else if (!ignore_missing_column && column_schema->is_generated_column()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Should not have generate columns, ", K(ret), K(cname));
      } else if (!allow_rowkey_in_properties && column_schema->is_rowkey_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("rowkey column should not be in properties, ", K(ret), K(cname));
      } else if (OB_FAIL(column_ids_.push_back(column_schema->get_column_id()))) {
        LOG_WARN("failed to add column id", K(ret));
      } else if (OB_FAIL(add_column_type(*column_schema))) {
        LOG_WARN("Fail to add column type, ", K(ret));
      }
    } // end for

    //add missing column
    if (OB_SUCC(ret) && !ignore_missing_column) {
      if (OB_FAIL(cons_missing_columns(entity))) {
        LOG_WARN("Fail to cons missing column, ", K(ret));
      }
    }
  }
  return ret;
}

int ObTableApiRowIterator::cons_missing_columns(const ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  has_generate_column_ = false;
  if (table_schema_->get_column_count() - table_schema_->get_rowkey_column_num() <= entity.get_properties_count()) {
    // no missing columns, do nothing
  } else {
    // some columns are missing
    ObTableSchema::const_column_iterator iter = NULL;
    ObTableSchema::const_column_iterator begin = table_schema_->column_begin();
    ObTableSchema::const_column_iterator end = table_schema_->column_end();
    ObObj obj;
    ObExprResType column_type;
    const schema::ObColumnSchemaV2 *column = NULL;
    for (iter = begin; OB_SUCC(ret) && iter != end; ++iter) {
      column = *iter;
      // skip all rowkeys
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column schema", K(column));
      } else if (!column->is_rowkey_column()) {
        if (OB_FAIL(entity.get_property(column->get_column_name_str(), obj))) {
          ret = OB_SUCCESS;
          // missing column
          obj = column->get_cur_default_value();
          if (OB_FAIL(missing_default_objs_.push_back(obj))) {
            LOG_WARN("Failed to push default obj, ", K(ret));
          } else if (OB_FAIL(column_ids_.push_back(column->get_column_id()))) {
            LOG_WARN("failed to add column id", K(ret));
          } else if (OB_FAIL(add_column_type(*column))) {
            LOG_WARN("Fail to add column type, ", K(ret));
          } else {
            if (column->is_generated_column()) {
              if (OB_FAIL(generate_column_idxs_.push_back(column_ids_.count() - 1))) {
                LOG_WARN("Failed to push generate column idx, ", K(ret));
              }
              has_generate_column_ = true;
            }
            LOG_DEBUG("add missing column", K(obj), K(column_type));
          }
        }
      } else {}
    } //end for

    //fill generate expression
    if (has_generate_column_) {
      generate_column_exprs_.reuse();
      uint64_t tenant_id = extract_tenant_id(table_schema_->get_table_id());
      if (OB_FAIL(sql::ObSQLUtils::make_default_expr_context(tenant_id, stmt_allocator_, expr_ctx_))) {
        LOG_WARN("failed to make default expr context ", K(ret));
      }

      for (iter = begin; OB_SUCC(ret) && iter != end; ++iter) {
        column = *iter;
        if (column->is_generated_column()) {
          ObISqlExpression *expr = NULL;
          obj = column->get_orig_default_value();
          if (OB_FAIL(sql::ObSQLUtils::make_generated_expression_from_str(
              obj.get_string(),
              *table_schema_,
              *column,
              column_descs_,
              stmt_allocator_,
              expr))) {
            STORAGE_LOG(WARN,
                        "failed to make sql expression",
                        K(obj.get_string()),
                        K(*table_schema_),
                        K(*column),
                        K(column_descs_),
                        K(ret));
          } else if (OB_FAIL(generate_column_exprs_.push_back(expr))) {
            STORAGE_LOG(WARN, "push back error", K(ret));
          } else {
            /*do nothing*/
          }

        }
      }
    }
  }
  return ret;
}


int ObTableApiRowIterator::add_column_type(const share::schema::ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObExprResType column_type;
  ObColDesc column_desc;
  if (OB_FAIL(cons_column_type(column_schema, column_type))) {
    LOG_WARN("failed to cons column type", K(ret));
  } else if (OB_FAIL(columns_type_.push_back(column_type))) {
    LOG_WARN("failed to push back column type, ", K(ret));
  } else {
    column_desc.col_id_ = column_schema.get_column_id();
    column_desc.col_type_ = column_schema.get_meta_type();
    if (OB_FAIL(column_descs_.push_back(column_desc))) {
      LOG_WARN("failed to push back column desc, ", K(ret));
    }
  }
  return ret;
}

int ObTableApiRowIterator::cons_column_type(const schema::ObColumnSchemaV2 &column_schema, ObExprResType &column_type)
{
  int ret = OB_SUCCESS;
  column_type.set_type(column_schema.get_data_type());
  column_type.set_result_flag(ObRawExprUtils::calc_column_result_flag(column_schema));
  if (ob_is_string_type(column_schema.get_data_type()) || ob_is_json(column_schema.get_data_type())) {
    column_type.set_collation_type(column_schema.get_collation_type());
    column_type.set_collation_level(CS_LEVEL_IMPLICIT);
  } else {
    column_type.set_collation_type(CS_TYPE_BINARY);
    column_type.set_collation_level(CS_LEVEL_NUMERIC);
  }
  const ObAccuracy &accuracy = column_schema.get_accuracy();
  column_type.set_accuracy(accuracy);
  const bool is_zerofill = column_type.has_result_flag(ZEROFILL_FLAG);
  if (is_zerofill) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("modifing column with ZEROFILL flag is not supported", K(ret), K(column_schema));
  }
  return ret;
}


int ObTableApiRowIterator::check_table_supported(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (!table_schema->is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table type not supported", K(ret),
             "table_name", table_schema->get_table_name_str(),
             "type", table_schema->get_table_type());
  }
  return ret;
}

int ObTableApiRowIterator::check_column_type(const ObExprResType &column_type, ObObj &obj)
{
  int ret = OB_SUCCESS;
  const bool is_not_nullable = column_type.has_result_flag(NOT_NULL_FLAG);
  const ObCollationType cs_type = column_type.get_collation_type();
  // 1. check nullable
  if (is_not_nullable && obj.is_null()) {
    ret = OB_BAD_NULL_ERROR;
  } else if (obj.is_null()) {
    // continue
  } else if (column_type.get_type() != obj.get_type()
             && !(ob_is_string_type(column_type.get_type()) && ob_is_string_type(obj.get_type()))) {
    // 2. data type mismatch
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("object type mismatch with column type", K(ret), K(column_type), K(obj));
  } else {
    // 3. check collation
    if (!ob_is_string_type(obj.get_type())) {
      // not string type, continue
    } else {
      if (cs_type == obj.get_collation_type()) {
        // same collation type
      } else if (cs_type == CS_TYPE_BINARY) {
        // any collation type can be compatible with cs_type_binary
        obj.set_collation_type(cs_type);
      } else if (ObCharset::charset_type_by_coll(cs_type) == ObCharset::charset_type_by_coll(obj.get_collation_type())) {
        // same charset, convert it
        obj.set_collation_type(cs_type);
      } else {
        ret = OB_ERR_COLLATION_MISMATCH;
        LOG_WARN("collation type mismatch with column", K(ret), K(column_type), K(obj));
      }
      if (OB_SUCC(ret)) {
        // convert obj type to the column type (char, varchar or text)
        obj.set_type(column_type.get_type());
      }
    }
    // 4. check accuracy
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_obj_accuracy_check_only(column_type.get_accuracy(), cs_type, obj))) {
        LOG_WARN("accuracy check failed", K(ret), K(obj), K(column_type));
      }
    }
  }
  return ret;
}


int ObTableApiRowIterator::fill_range(const ObRowkey &key, ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  ObNewRange range;

  if (key.get_obj_cnt() > rowkey_column_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong rowkey size", K(ret), K(key), K(columns_type_));
  } else {
    const int64_t N = key.get_obj_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(check_column_type(columns_type_.at(i), const_cast<ObObj&> (key.get_obj_ptr()[i])))) {
        LOG_WARN("Invalid column type, ", K(ret), K(i), K(columns_type_.at(i)), K(key.get_obj_ptr()[i]));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(range.build_range(table_id_, key))) {
        LOG_WARN("fail to build key range", K(ret), K_(table_id), K(key));
      } else if (OB_FAIL(ranges.push_back(range))) {
        LOG_WARN("fail to push back key range", K(ret), K(range));
      } else {
      }
    }
  }

  return ret;
}

int ObTableApiRowIterator::fill_flag(ObTableServiceCtx &ctx, storage::ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
  scan_param.timeout_ = ctx.param_.timeout_ts_;
  ObQueryFlag query_flag(ObQueryFlag::KeepOrder, // scan_order KeepOrder!
                         false, // daily_merge
                         false, // optimize
                         false, // whole_macro_scan
                         false, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                         );
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  scan_param.reserved_cell_count_ = column_ids_.count() + 10;
  scan_param.for_update_ = false;
  scan_param.column_ids_.reset();
  scan_param.pkey_ = part_key;
  scan_param.schema_version_ = schema_version_;
  if (OB_FAIL(scan_param.column_ids_.assign(column_ids_))) {
    LOG_WARN("fail to assign column id", K(ret));
  } else {
    scan_param.expr_ctx_.calc_buf_ = NULL;
    scan_param.expr_ctx_.my_session_ = NULL;
    scan_param.expr_ctx_.phy_plan_ctx_ = NULL;
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    scan_param.trans_desc_ = ctx.param_.trans_desc_;
    scan_param.index_id_ = table_id_;
    scan_param.sql_mode_ = SMO_DEFAULT;
    scan_param.allocator_->set_tenant_id(scan_param.pkey_.get_tenant_id());
  }
  return ret;
}



/**
 * ------------------------------------------------------------------ObTableApiInsertRowIterator---------------------------------------------------------
 */
ObTableApiInsertRowIterator::ObTableApiInsertRowIterator()
{
}

ObTableApiInsertRowIterator::~ObTableApiInsertRowIterator()
{
}

int ObTableApiInsertRowIterator::open(const ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api insert row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(table_operation.entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else {
    entity_ = &(table_operation.entity());
  }
  return ret;
}

int ObTableApiInsertRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_ISNULL(entity_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The entity is null, ", K(ret));
  } else if (OB_FAIL(cons_row(*entity_, row))) {
    LOG_WARN("Fail to construct insert row, ", K(ret));
  } else {
    //success
    LOG_DEBUG("Api insert row iter, ", K(*row));
  }
  return ret;
}

int ObTableApiInsertRowIterator::cons_row(const table::ObITableEntity &entity, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(entity_to_row(entity, row_objs_))) {
    LOG_WARN("Fail to generate row from entity, ", K(ret));
  } else {
    const int64_t N = missing_default_objs_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(row_objs_.push_back(missing_default_objs_.at(i)))) {
        LOG_WARN("Fail to push default value to row, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      row_.assign(&row_objs_.at(0), row_objs_.count());
      if (has_generate_column_ && OB_FAIL(fill_generate_columns(row_))) {
        LOG_WARN("Fail to fill generate columns, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_row(row_))) {
        LOG_WARN("Fail to check row, ", K(ret), K_(row));
      } else {
        row = &row_;
      }
    }
  }
  return ret;
}


/**
 * ------------------------------------------------------------------ObTableApiMultiInsertRowIterator---------------------------------------------------------
 */
ObTableApiMultiInsertRowIterator::ObTableApiMultiInsertRowIterator()
  : batch_operation_(NULL),
    row_idx_(0),
    batch_cnt_(0),
    is_iter_pause_(false)
{
}

ObTableApiMultiInsertRowIterator::~ObTableApiMultiInsertRowIterator()
{
}

void ObTableApiMultiInsertRowIterator::reset()
{
  batch_operation_ = NULL;
  row_idx_ = 0;
  batch_cnt_ = 0;
  is_iter_pause_ = false;
  ObTableApiInsertRowIterator::reset();
}

int ObTableApiMultiInsertRowIterator::open(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api multi insert iterator has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(batch_operation.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(batch_operation.count()));
  } else if (OB_FAIL(cons_all_columns(batch_operation.at(0).entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else {
    batch_operation_ = &batch_operation;
    row_idx_ = 0;
    batch_cnt_ = batch_operation.count();
  }
  return ret;
}

int ObTableApiMultiInsertRowIterator::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_ISNULL(batch_operation_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api multi insert row iterator has not been inited, ", K(ret));
  } else if (row_idx_ >= batch_cnt_ || is_iter_pause_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(cons_row(batch_operation_->at(row_idx_).entity(), row))) {
    LOG_WARN("Fail to construct row, ", K(ret), K(row_idx_));
  } else {
    row_idx_++;
    is_iter_pause_ = true;
    LOG_DEBUG("Api insert row iter, ", K(*row), K_(row_idx));
  }
  return ret;
}


/**
 * ------------------------------------------------------------------ObTableApiUpdateRowIterator---------------------------------------------------------
 */
ObTableApiUpdateRowIterator::ObTableApiUpdateRowIterator()
  : scan_param_(),
    table_param_(stmt_allocator_),
    update_column_ids_(),
    scan_iter_(NULL),
    old_row_(NULL),
    new_row_(NULL),
    row_idx_(0),
    need_update_rowkey_(false),
    table_operation_(NULL)
{
}

ObTableApiUpdateRowIterator::~ObTableApiUpdateRowIterator()
{
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
}

void ObTableApiUpdateRowIterator::reset()
{
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
  scan_param_.destroy();
  table_param_.reset();
  update_column_ids_.reset();
  old_row_ = NULL;
  new_row_ = NULL;
  row_idx_ = 0;
  ObTableApiRowIterator::reset();
}

int ObTableApiUpdateRowIterator::open(const ObTableOperation &table_operation,
    const ObRowkey &rowkey, bool need_update_rowkey/* = false */)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(table_operation.entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(cons_update_columns(need_update_rowkey))) {
    LOG_WARN("Fail to construct update columns, ", K(ret));
  } else if (OB_FAIL(fill_get_param(*ctx_,
      table_operation.type(), const_cast<ObRowkey &>(rowkey), scan_param_, table_param_))) {
    LOG_WARN("Fail to fill get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
    table_operation_ = &table_operation;
    old_row_ = NULL;
    new_row_ = NULL;
    row_idx_ = 0;
    need_update_rowkey_ = need_update_rowkey;
  }
  return ret;
}

int ObTableApiUpdateRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(scan_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been opened, ", K(ret));
  } else {
    if (0 == row_idx_ % 2) {
      if (OB_FAIL(scan_iter_->get_next_row(row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to get next row, ", K(ret));
        } else {
          LOG_DEBUG("Update row iter end, ", K_(row_idx), K(scan_param_));
        }
      } else {
        old_row_ = row;
        LOG_DEBUG("Update old row, ", K_(row_idx), K(*old_row_));
      }
    } else if (OB_FAIL(cons_new_row(*table_operation_, row))) {
      LOG_WARN("Fail to construct new row, ", K(ret));
    } else {
      new_row_ = row;
      LOG_DEBUG("Update iter cons new row, ", K_(row_idx), K(*new_row_));
    }

    if (OB_SUCC(ret)) {
      row_idx_++;
    }
  }
  return ret;
}

int ObTableApiUpdateRowIterator::cons_update_columns(bool need_update_rowkey)
{
  int ret = OB_SUCCESS;
  update_column_ids_.reuse();
  if (OB_UNLIKELY(column_ids_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The column id array is empty, ", K(ret), K(column_ids_.count()));
  } else {
    const int64_t N = column_ids_.count() - missing_default_objs_.count();
    int64_t begin_column_idx = need_update_rowkey ? 0 : rowkey_column_cnt_;
    for (int64_t i = begin_column_idx; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(update_column_ids_.push_back(column_ids_.at(i)))) {
        LOG_WARN("Fail to push column id to update column ids array, ", K(ret));
      }
    }

    if (OB_SUCC(ret) && has_generate_column_) {
      const int64_t gen_col_cnt = generate_column_idxs_.count();
      uint64_t column_id = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < gen_col_cnt; ++i) {
        if (OB_FAIL(column_ids_.at(generate_column_idxs_.at(i), column_id))) {
          LOG_WARN("Fail to get ith col id, ", K(ret), K(i));
        } else if (OB_FAIL(update_column_ids_.push_back(column_id))) {
          LOG_WARN("Fail to push generate column id to update column ids array, ", K(ret), K(i), K(column_id));
        }
      }
    }
  }
  return ret;
}

int ObTableApiUpdateRowIterator::cons_new_row(const ObTableOperation &table_operation, common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row_objs_.reuse();
  if (OB_FAIL(entity_to_row(table_operation.entity(), row_objs_))) {
    LOG_WARN("Fail to generate row from entity, ", K(ret));
  } else {
    const int64_t rowkey_col_cnt = table_operation.entity().get_rowkey_size();
    const int64_t base_col_cnt = row_objs_.count();
    ObTableOperationType::Type type = table_operation.type();

    switch(type) {
    case ObTableOperationType::GET:
    case ObTableOperationType::INSERT:
    case ObTableOperationType::DEL:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid operation type", K(ret), K(type));
      break;
    case ObTableOperationType::UPDATE:
    case ObTableOperationType::INSERT_OR_UPDATE:
    case ObTableOperationType::REPLACE:
      //nothing to do
      break;
    case ObTableOperationType::INCREMENT: {
      for (int64_t i = rowkey_col_cnt; OB_SUCC(ret) && i < base_col_cnt; ++i) {
        if (OB_FAIL(obj_increment(row_objs_.at(i), old_row_->cells_[i], columns_type_.at(i), row_objs_.at(i)))) {
          LOG_WARN("failed to increment", K(ret), K(i), K(row_objs_.at(i)), K(old_row_->cells_[i]));
        }
      }
      break;
    }
    case ObTableOperationType::APPEND: {
      for (int64_t i = rowkey_col_cnt; OB_SUCC(ret) && i < base_col_cnt; ++i) {
        if (OB_FAIL(obj_append(row_objs_.at(i), old_row_->cells_[i], columns_type_.at(i), row_objs_.at(i)))) {
          LOG_WARN("failed to append", K(ret), K(i), K(row_objs_.at(i)), K(old_row_->cells_[i]));
        }
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid operation type", K(ret), K(type));
      break;
    }

    if (OB_SUCC(ret) && !need_update_rowkey_) {
      const int64_t N = rowkey_col_cnt;
      for (int64_t i = 0; i < N; ++i) {
        row_objs_[i] = old_row_->cells_[i];
      }
    }

    if (OB_SUCC(ret)) {
      //fill default columns
      const int64_t N = missing_default_objs_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        if (OB_FAIL(row_objs_.push_back(old_row_->cells_[base_col_cnt + i]))) {
          LOG_WARN("Fail to push back old row obj to new row, ", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      //fill generate columns
      row_.assign(&row_objs_.at(0), row_objs_.count());
      if (has_generate_column_ && OB_FAIL(fill_generate_columns(row_))) {
        LOG_WARN("Fail to fill generate columns, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_row(row_))) {
        LOG_WARN("Fail to check new row, ", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &row_;
  }
  return ret;
}


int ObTableApiUpdateRowIterator::obj_increment(
    const common::ObObj &delta,
    const common::ObObj &src,
    const sql::ObExprResType &target_type,
    common::ObObj &target)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(ob_is_int_tc(delta.get_type()))) {
    int64_t delta_int = delta.get_int();
    ObObjType src_type = src.get_type();
    switch (src_type) {
      case ObNullType: {
        // the result is set to delta when src is NULL
        ObObjType result_type = target_type.get_type();
        if (ob_is_int_tc(result_type)) {
          ret = int_add_int_with_check(0, delta_int, result_type, target);
        } else if (ob_is_uint_tc(result_type)) {
          ret = uint_add_int_with_check(0, delta_int, result_type, target);
        } else {
          ret = OB_OBJ_TYPE_ERROR;
        }
        break;
      }
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        // signed := signed + signed
        ret = int_add_int_with_check(src.get_int(), delta_int, src_type, target);
        break;
      }
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        // unsigned := usigned + signed
        ret = uint_add_int_with_check(src.get_uint64(), delta_int, src_type, target);
        break;
      }
      default:
        ret = OB_OBJ_TYPE_ERROR;
        break;
    }  // end switch
  } else {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("delta should only be signed integer type", K(ret), K(delta));
  }
  return ret;
}

int ObTableApiUpdateRowIterator::obj_append(
    const common::ObObj &delta,
    const common::ObObj &src,
    const sql::ObExprResType &target_type,
    common::ObObj &target)
{
  int ret = OB_SUCCESS;
  if (delta.is_null()) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("append NULL is illegal", K(ret), K(delta));
  } else if (OB_UNLIKELY(ObVarcharType != delta.get_type())) {
    ret = OB_OBJ_TYPE_ERROR;
    LOG_WARN("can only append varchar/varbinary type", K(ret), K(delta));
  } else {
    const char *result_ptr = NULL;
    int32_t result_len = 0;

    if (src.is_null()) {
      // the result is set the delta when src is NULL
      if (OB_UNLIKELY(ObVarcharType != target_type.get_type())
          && OB_UNLIKELY(ObTextTC != ob_obj_type_class(target_type.get_type()))) {
        ret = OB_OBJ_TYPE_ERROR;
        LOG_WARN("can only appended to string type", K(ret), K(target_type));
      } else {
        result_ptr = delta.get_string_ptr();
        result_len = delta.get_string_len();
      }
    } else if (OB_UNLIKELY(ObVarcharType != src.get_type())
        && OB_UNLIKELY(ObTextTC != ob_obj_type_class(src.get_type()))) {
      ret = OB_OBJ_TYPE_ERROR;
      LOG_WARN("can only appended to string type", K(ret), K(src));
    } else {
      const int32_t len1 = src.get_string_len();
      const int32_t len2 = delta.get_string_len();
      const int32_t total_len = len1 + len2;
      // the actual length will be checked in ObTableOperationTypeChecker
      if (len1 <= 0) {
        result_ptr = delta.get_string_ptr();
        result_len = delta.get_string_len();
      } else if (len2 <= 0) {
        target = src;
        result_len = -1;  // no need to set the target
      } else {
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(row_allocator_.alloc(total_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(total_len));
        } else {
          MEMCPY(buf, src.get_string_ptr(), len1);
          MEMCPY(buf + len1, delta.get_string_ptr(), len2);
          result_ptr = buf;
          result_len = total_len;
        }
      }
    }

    if (OB_SUCC(ret) && result_len >= 0) {
      if (ob_is_text_tc(target_type.get_type())) {  // LOB
        target.set_lob_value(target_type.get_type(), result_ptr, result_len);
        target.set_meta_type(target_type);
      } else if (ob_is_json_tc(target_type.get_type())) {
        target.set_json_value(target_type.get_type(), result_ptr, result_len);
        target.set_meta_type(target_type);
      } else {  // Varchar/Varbinary
        target.set_varchar_value(result_ptr, result_len);
        target.set_meta_type(target_type);
      }
    }
  }
  return ret;
}

int ObTableApiUpdateRowIterator::int_add_int_with_check(
    int64_t old_int,
    int64_t delta_int,
    common::ObObjType result_type,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t result_int = old_int + delta_int;
  if (sql::ObExprAdd::is_int_int_out_of_range(old_int, delta_int, result_int)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("result of increment out of range", K(ret),
             K(delta_int), K(old_int), K(result_int));
  } else if (result_int < INT_MIN_VAL[result_type]
             || result_int > INT_MAX_VAL[result_type]) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("result of increment out of range", K(ret),
             K(delta_int), K(old_int), K(result_int),
             "min", INT_MIN_VAL[result_type],
             "max", INT_MAX_VAL[result_type]);
  } else {
    result.set_type(result_type);
    result.set_int_value(result_int);
  }
  return ret;
}


int ObTableApiUpdateRowIterator::uint_add_int_with_check(
    uint64_t old_uint,
    int64_t delta_int,
    common::ObObjType result_type,
    common::ObObj &result)
{
  int ret = OB_SUCCESS;
  uint64_t result_uint = old_uint + delta_int;
  if (sql::ObExprAdd::is_int_uint_out_of_range(delta_int, old_uint, result_uint)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("result of increment out of range", K(ret),
             K(delta_int), K(old_uint), K(result_uint));
  } else if (result_uint > UINT_MAX_VAL[result_type]) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("result of increment out of range", K(ret),
             K(delta_int), K(old_uint), K(result_uint),
             "max", UINT_MAX_VAL[result_type]);
  } else {
    result.set_type(result_type);
    result.set_uint64_value(result_uint);
  }
  return ret;
}



/**
 * ------------------------------------------------------------------ObTableApiMultiUpdateRowIterator---------------------------------------------------------
 */
ObTableApiMultiUpdateRowIterator::ObTableApiMultiUpdateRowIterator()
  : batch_operation_(NULL),
    batch_cnt_(0),
    batch_idx_(0),
    cur_update_idx_(-1),
    is_iter_pause_(false)
{
}

ObTableApiMultiUpdateRowIterator::~ObTableApiMultiUpdateRowIterator()
{
}

void ObTableApiMultiUpdateRowIterator::reset()
{
  batch_operation_ = NULL;
  batch_cnt_ = 0;
  batch_idx_ = 0;
  cur_update_idx_ = -1;
  is_iter_pause_ = false;
  ObTableApiUpdateRowIterator::reset();
}

int ObTableApiMultiUpdateRowIterator::open(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(batch_operation.at(0).entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(cons_update_columns(false/*need_update_rowkey*/))) {
    LOG_WARN("Fail to construct update columns, ", K(ret));
  } else if (OB_FAIL(fill_multi_get_param(*ctx_, batch_operation, scan_param_, table_param_))) {
    LOG_WARN("Fail to fill get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
    batch_operation_ = &batch_operation;
    batch_cnt_ = batch_operation.count();
    batch_idx_ = 0;
    cur_update_idx_ = -1;
    is_iter_pause_ = false;
  }
  return ret;
}

int ObTableApiMultiUpdateRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(batch_operation_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been opened, ", K(ret));
  } else if (batch_idx_ >= batch_cnt_ || is_iter_pause_) {
    ret = OB_ITER_END;
  } else {
    row_allocator_.reuse();
    if (0 == row_idx_ % 2) {
      if (OB_FAIL(scan_iter_->get_next_row(row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("Fail to scan next row, ", K(ret), K_(row_idx));
        } else {
          batch_idx_ = batch_cnt_;
        }
        old_row_ = NULL;
      } else {
        old_row_ = row;
      }
    } else {
      ObRowkey old_row_key(old_row_->cells_, rowkey_column_cnt_);
      row = NULL;
      while(batch_idx_ < batch_cnt_) {
        entity_ = &(batch_operation_->at(batch_idx_).entity());
        if (old_row_key.simple_equal((const_cast<ObITableEntity*>(entity_))->get_rowkey())) {
          break;
        } else {
          batch_idx_++;
        }
      }

      if (batch_idx_ < batch_cnt_) {
        if (OB_FAIL(cons_new_row(batch_operation_->at(batch_idx_), row))) {
          LOG_WARN("Fail to construct new row, ", K(ret));
        } else {
          cur_update_idx_ = batch_idx_;
          batch_idx_++;
          is_iter_pause_ = true;
        }
      } else {
        ret = OB_ITER_END;
      }
    }

    if (OB_SUCC(ret)) {
      row_idx_++;
    }
  }
  return ret;
}


/**
 * ------------------------------------------------------------------ObTableApiDeleteRowIterator---------------------------------------------------------
 */
ObTableApiDeleteRowIterator::ObTableApiDeleteRowIterator()
  : scan_param_(),
    table_param_(stmt_allocator_),
    scan_iter_(NULL)
{
}

ObTableApiDeleteRowIterator::~ObTableApiDeleteRowIterator()
{
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
}

void ObTableApiDeleteRowIterator::reset()
{
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
  scan_param_.destroy();
  table_param_.reset();

  ObTableApiRowIterator::reset();
}

int ObTableApiDeleteRowIterator::open(const ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey = const_cast<ObITableEntity &>(table_operation.entity()).get_rowkey();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(table_operation.entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(fill_get_param(*ctx_, table_operation.type(), rowkey,
      scan_param_, table_param_))) {
    LOG_WARN("Fail to fill get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
    entity_ = &(table_operation.entity());
  }
  return ret;
}

int ObTableApiDeleteRowIterator::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_iter_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The delete row iterator has not been inited, ", K(ret));
  } else {
    if (OB_FAIL(scan_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Fail to get next delete row, ", K(ret));
      }
    }
  }
  return ret;
}

/**
 * ------------------------------------------------------------------ObTableApiMultiDeleteRowIterator---------------------------------------------------------
 */
ObTableApiMultiDeleteRowIterator::ObTableApiMultiDeleteRowIterator()
  : batch_operation_(NULL),
    batch_cnt_(0),
    batch_idx_(0),
    cur_delete_idx_(-1),
    is_iter_pause_(false)
{
}

ObTableApiMultiDeleteRowIterator::~ObTableApiMultiDeleteRowIterator()
{
}

void ObTableApiMultiDeleteRowIterator::reset()
{
  batch_operation_ = NULL;
  batch_cnt_ = 0;
  batch_idx_ = 0;
  cur_delete_idx_ = -1;
  is_iter_pause_ = false;
  ObTableApiDeleteRowIterator::reset();
}

int ObTableApiMultiDeleteRowIterator::open(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The multi delte row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(batch_operation.at(0).entity()))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(fill_multi_get_param(*ctx_, batch_operation, scan_param_, table_param_))) {
    LOG_WARN("Fail to fill multi get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
    batch_operation_ = &batch_operation;
    batch_cnt_ = batch_operation.count();
    batch_idx_ = 0;
    cur_delete_idx_ = -1;
    is_iter_pause_ = false;
  }
  return ret;
}

int ObTableApiMultiDeleteRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api multi delete row iterator has not been inited, ", K(ret));
  } else if (OB_ISNULL(batch_operation_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api multi delete row iterator has not been opened, ", K(ret));
  } else if (batch_idx_ >= batch_cnt_ || is_iter_pause_) {
    ret = OB_ITER_END;
  } else {
    if (OB_FAIL(scan_iter_->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Fail to get next delete row, ", K(ret));
      } else {
        batch_idx_ = batch_cnt_;
      }
    } else {
      ObRowkey row_key(row->cells_, rowkey_column_cnt_);
      while(batch_idx_ < batch_cnt_) {
        entity_ = &(batch_operation_->at(batch_idx_).entity());
        if (row_key.simple_equal((const_cast<ObITableEntity*>(entity_))->get_rowkey())) {
          cur_delete_idx_ = batch_idx_;
          break;
        } else {
          batch_idx_++;
        }
      }
      batch_idx_++;
      is_iter_pause_ = true;
    }
  }

  return ret;
}


/**
 * ------------------------------------------------------------------ObTableApiGetRowIterator---------------------------------------------------------
 */
ObTableApiGetRowIterator::ObTableApiGetRowIterator()
  : scan_param_(),
    table_param_(stmt_allocator_),
    scan_iter_(NULL)
{
}

ObTableApiGetRowIterator::~ObTableApiGetRowIterator()
{
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
}

void ObTableApiGetRowIterator::reset()
{
  scan_param_.destroy();
  table_param_.reset();
  if (NULL != scan_iter_) {
    if (NULL != part_service_) {
      part_service_->revert_scan_iter(scan_iter_);
      scan_iter_ = NULL;
    } else {
      LOG_ERROR("The part service is NULL, but the scan iter is NOT NULL!");
    }
  }
  ObTableApiRowIterator::reset();
}

int ObTableApiGetRowIterator::open(const ObTableOperation &table_operation)
{
  int ret = OB_SUCCESS;
  const bool ignore_missing_column = true;
  const bool allow_rowkey_in_properties = true;
  ObRowkey rowkey = const_cast<ObITableEntity &>(table_operation.entity()).get_rowkey();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api update row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(cons_all_columns(table_operation.entity(), ignore_missing_column, allow_rowkey_in_properties))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(fill_get_param(*ctx_, table_operation.type(), rowkey,
      scan_param_, table_param_))) {
    LOG_WARN("Fail to fill get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
  }
  return ret;
}

int ObTableApiGetRowIterator::get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_iter_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api get row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(scan_iter_->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Fail to get next row, ", K(ret));
    }
  }
  return ret;
}

/**
 * ------------------------------------------------------------------ObTableApiMultiGetRowIterator---------------------------------------------------------
 */
ObTableApiMultiGetRowIterator::ObTableApiMultiGetRowIterator()
{
}

ObTableApiMultiGetRowIterator::~ObTableApiMultiGetRowIterator()
{
}

int ObTableApiMultiGetRowIterator::open(const ObTableBatchOperation &batch_operation)
{
  int ret = OB_SUCCESS;
  const bool ignore_missing_column = true;
  const bool allow_rowkey_in_properties = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api multi get row iterator has not been inited, ", K(ret));
  } else if (OB_FAIL(
                 cons_all_columns(batch_operation.at(0).entity(), ignore_missing_column, allow_rowkey_in_properties))) {
    LOG_WARN("Fail to construct all columns, ", K(ret));
  } else if (OB_FAIL(fill_multi_get_param(*ctx_, batch_operation, scan_param_, table_param_))) {
    LOG_WARN("Fail to fill get param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param_, scan_iter_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
  }
  return ret;
}

}
}
