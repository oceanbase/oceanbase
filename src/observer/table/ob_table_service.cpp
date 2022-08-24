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
#include "ob_table_service.h"
#include "ob_table_api_row_iterator.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"
#include "ob_table_rpc_processor.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "ob_htable_filter_operator.h"
#include "sql/engine/expr/ob_expr_add.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

OB_INLINE void replace_ret_code(int &ret)
{
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret
      || OB_BAD_NULL_ERROR == ret
      || OB_OBJ_TYPE_ERROR == ret
      || OB_ERR_COLLATION_MISMATCH == ret
      || OB_ERR_DATA_TOO_LONG == ret
      || OB_DATA_OUT_OF_RANGE == ret) {
    ret = OB_SUCCESS;
  }
}

OB_INLINE int get_rowkey_column_ids(
    const ObIArray<uint64_t> &full_column_ids,
    const int64_t rowkey_column_cnt,
    ObIArray<uint64_t> &rowkey_column_ids)
{
  int ret = OB_SUCCESS;
  rowkey_column_ids.reset();
  if (OB_UNLIKELY(full_column_ids.count() <= 0
      || rowkey_column_cnt <= 0
      || rowkey_column_cnt > full_column_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(full_column_ids), K(rowkey_column_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_cnt; ++i) {
      if (OB_FAIL(rowkey_column_ids.push_back(full_column_ids.at(i)))) {
        LOG_WARN("fail to push back current column id", K(ret), K(i), K(full_column_ids.at(i)));
      }
    }
  }
  return ret;
}

int ObTableService::init(ObGlobalContext &gctx)
{
  int ret = OB_SUCCESS;
  part_service_ = gctx.par_ser_;
  schema_service_ = gctx.schema_service_;
  return ret;
}

int ObTableService::cons_rowkey_infos(const schema::ObTableSchema &table_schema,
                                      ObIArray<uint64_t> *column_ids,
                                      ObIArray<ObExprResType> *columns_type)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  if (NULL != column_ids) {
    if (OB_FAIL(rowkey_info.get_column_ids(*column_ids))) {
      LOG_WARN("failed to get rowkey column ids", K(ret), K(rowkey_info));
    }
  }
  if (OB_SUCC(ret) && NULL != columns_type) {
    const schema::ObColumnSchemaV2 *column_schema = NULL;
    uint64_t column_id = OB_INVALID_ID;
    ObExprResType column_type;
    const int64_t N = rowkey_info.get_size();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i));
      } else if (NULL == (column_schema = table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey column not exists", K(ret), K(column_id));
      } else if (OB_FAIL(cons_column_type(*column_schema, column_type))) {
        LOG_WARN("failed to cons column type", K(ret));
      } else if (OB_FAIL(columns_type->push_back(column_type))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableService::cons_properties_infos(const schema::ObTableSchema &table_schema,
                                          const ObIArray<ObString> &properties,
                                          ObIArray<uint64_t> &column_ids,
                                          ObIArray<ObExprResType> *columns_type)
{
  int ret = OB_SUCCESS;
  const schema::ObColumnSchemaV2 *column_schema = NULL;
  ObExprResType column_type;
  const int64_t N = properties.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    const ObString &cname = properties.at(i);
    if (NULL == (column_schema = table_schema.get_column_schema(cname))) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not exists", K(ret), K(cname));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("failed to add column id", K(ret));
    } else if (NULL != columns_type) {
      if (OB_FAIL(cons_column_type(*column_schema, column_type))) {
        LOG_WARN("failed to cons column type", K(ret));
      } else if (OB_FAIL(columns_type->push_back(column_type))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  } // end for
  return ret;
}

int ObTableService::cons_column_type(const schema::ObColumnSchemaV2 &column_schema, ObExprResType &column_type)
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

// @pre column is not zerofill
int ObTableService::check_column_type(const ObExprResType &column_type, ObObj &obj)
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

int ObTableService::insert_or_update_can_use_put(ObTableEntityType entity_type, uint64_t table_id, const ObITableEntity &entity, bool &use_put)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = NULL;
  if (ObTableEntityType::ET_HKV == entity_type) {
    // hbase model table does not have secondary index and always specify all the properties (column V)
    use_put = true;
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL ptr", K(ret), K(table_schema));
  } else {
    // 1. there is not any index
    // 2. if there are some columns missing, the missing columns don't have a default value
    use_put = (table_schema->get_index_tid_count() <= 0);
    if (use_put) {
      if (table_schema->get_column_count() - table_schema->get_rowkey_column_num() <= entity.get_properties_count()) {
        // all columns are fine
      } else {
        // some columns are missing
        schema::ObTableSchema::const_column_iterator iter = table_schema->column_begin();
        schema::ObTableSchema::const_column_iterator end = table_schema->column_end();
        ObObj obj;
        ObExprResType column_type;
        for (; OB_SUCC(ret) && iter != end; ++iter) {
          const schema::ObColumnSchemaV2 *column = *iter;
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column schema", K(column));
          } else if (!column->is_rowkey_column()) {
            const ObString &column_name = column->get_column_name_str();
            if (OB_FAIL(entity.get_property(column_name, obj))) {
              ret = OB_SUCCESS;
              // missing column
              if (!column->get_cur_default_value().is_null()) {
                use_put = false;
                break;
              }
            }
          }
        }  // end for
      }
    }
  }
  return ret;
}

int ObTableService::execute_insert_or_update(ObTableServiceGetCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &entity = table_operation.entity();
  bool can_use_put = true;
  if (OB_FAIL(insert_or_update_can_use_put(ctx.param_.entity_type_, ctx.param_.table_id_, entity, can_use_put))) {
    LOG_WARN("failed to check", K(ret));
  } else if (can_use_put
             && ctx.param_.binlog_row_image_type_ != ObBinlogRowImageType::FULL) {
    ret = do_put(ctx, table_operation, result);
  } else {
    ret = do_insert_or_update(ctx, table_operation, result);
  }
  return ret;
}

// If there are some columns missing in the property, can not fill default value.
// Because if row exists, it will do update, fill default value will cover the old value.
// If row does not exist, it will do insert, fill null value to missing columns is wrong.
// So, if and only if all columns are given or missing columns have not default value and there is not any index, we can use do_put
int ObTableService::do_put(ObTableServiceCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = ctx.param_.table_id_;
  ObTableApiInsertRowIterator put_row_iter;

  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(put_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init put row iterator, ", K(ret), K(table_id));
  } else if (OB_FAIL(put_row_iter.open(table_operation))) {
    LOG_WARN("Fail to open put row iterator, ", K(ret), K(table_id));
  } else {
    int64_t affected_rows = 0;
    // init dml param
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = false; // always false for put
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = put_row_iter.get_schema_version();

    if (OB_FAIL(part_service_->put_rows(
        *(ctx.param_.trans_desc_),
        dml_param,
        part_key,
        put_row_iter.get_column_ids(),
        &put_row_iter,
        affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to put row", K(ret), K(table_id));
      }
    } else {
      result.set_affected_rows(1);
    }
  }
  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
  return ret;
}

int ObTableService::do_insert_or_update(ObTableServiceGetCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObNewRowIterator *duplicate_row_iter = nullptr;
  if (OB_FAIL(execute_insert(ctx, table_operation, result, duplicate_row_iter))) {
    LOG_WARN("failed to execute_insert", K(ret));
  } else {
    ret = result.get_errno();
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_SUCCESS;
      ctx.reset_get_ctx();
      ObNewRow *duplicate_row = nullptr;
      // update on duplicate key
      if (OB_ISNULL(duplicate_row_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicate row iterator is null", K(ret));
      } else if (OB_FAIL(duplicate_row_iter->get_next_row(duplicate_row))) {
        LOG_WARN("fail to get duplicate row", K(ret));
      } else if (OB_FAIL(execute_update(ctx, table_operation, duplicate_row, result))) {
        LOG_WARN("failed to update", K(ret));
      } else {
        // update succ
      }
    } else {
      // insert succ
    }
  }
  if (nullptr != duplicate_row_iter) {
    ObPartitionKey pkey(ctx.param_.table_id_, ctx.param_.partition_id_, 0);
    int tmp_ret = part_service_->revert_insert_iter(pkey, duplicate_row_iter);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to revert duplicate_row_iter", K(tmp_ret), KP(duplicate_row_iter));
    } else {
      duplicate_row_iter = nullptr;
    }
  }
  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
  return ret;
}

OB_INLINE int ObTableService::add_one_result(ObTableBatchOperationResult &result,
                                   ObTableOperationType::Type op_type,
                                   int32_t error_code,
                                   int64_t affected_rows)
{
  int ret = OB_SUCCESS;
  ObTableOperationResult one_result;
  ObITableEntity *result_entity = result.get_entity_factory()->alloc();
  if (NULL == result_entity) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memroy for result_entity", K(ret));
  } else {
    one_result.set_entity(*result_entity);
    one_result.set_type(op_type);
    one_result.set_errno(error_code);
    one_result.set_affected_rows(affected_rows);
    if (OB_FAIL(result.push_back(one_result))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      LOG_DEBUG("[yzfdebug] add result", K(ret), K(one_result));
    }
  }
  return ret;
}

int ObTableService::multi_insert_or_update(ObTableServiceGetCtx &ctx,
                                           const ObTableBatchOperation &batch_operation,
                                           ObTableBatchOperationResult &result)
{
  NG_TRACE(insertup_start_do);
  int ret = OB_SUCCESS;
  const ObTableOperation &one_op = batch_operation.at(0);
  bool can_use_put = true;
  if (OB_FAIL(insert_or_update_can_use_put(ctx.param_.entity_type_, 
        ctx.param_.table_id_, one_op.entity(), can_use_put))) {
    LOG_WARN("failed to check", K(ret));
  } else if (can_use_put
             && ctx.param_.binlog_row_image_type_ != ObBinlogRowImageType::FULL) {
    ret = multi_put(ctx, batch_operation, result);
  } else {
    ret = do_multi_insert_or_update(ctx, batch_operation, result);
  }
  NG_TRACE(insertup_end);
  return ret;
}

int ObTableService::multi_put(ObTableServiceCtx &ctx,
                              const ObTableBatchOperation &batch_operation,
                              ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiMultiInsertRowIterator multi_put_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(multi_put_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init multi put iterator, ", K(ret), K(table_id));
  } else if (OB_FAIL(multi_put_iter.open(batch_operation))) {
    LOG_WARN("Fail to open multi put iterator, ", K(ret), K(table_id));
  } else {
    int64_t affected_rows = 0;
    // init dml param
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = false;  // always false for put
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = multi_put_iter.get_schema_version();

    const int64_t N = batch_operation.count();
    NG_TRACE_EXT(insertup_calc_new_row, OB_ID(input_count), N);
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      multi_put_iter.continue_iter();
      if (OB_FAIL(part_service_->put_rows(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          multi_put_iter.get_column_ids(),
          &multi_put_iter,
          affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to put rows", K(ret), K(table_id));
        } else {
          NG_TRACE(locked);
        }
      } else if (OB_FAIL(add_one_result(result, ObTableOperationType::INSERT_OR_UPDATE, OB_SUCCESS, affected_rows))) {
        LOG_WARN("failed to add result", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableService::do_multi_insert_or_update(ObTableServiceGetCtx &ctx,
                                              const ObTableBatchOperation &batch_operation,
                                              ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = ctx.param_.table_id_;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else {
    int64_t N = batch_operation.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      const ObTableOperation &table_operation = batch_operation.at(i);
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = result.get_entity_factory()->alloc();
      if (NULL == result_entity) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memroy for result_entity", K(ret));
        break;
      }
      op_result.set_entity(*result_entity);
      if (OB_FAIL(do_insert_or_update(ctx, table_operation, op_result))) {
        LOG_WARN("failed to do insert_or_update", K(ret));
      } else if (OB_FAIL(result.push_back(op_result))) {
        LOG_WARN("failed to push back result", K(ret));
      } else {
        ctx.reset_get_ctx();
      }
    } // end for
  }
  return ret;
}

////////////////////////////////////////////////////////////////

int ObTableService::fill_scan_param(ObTableServiceCtx &ctx,
                                    const ObIArray<uint64_t> &output_column_ids,
                                    int64_t schema_version,
                                    storage::ObTableScanParam &scan_param)
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
  scan_param.reserved_cell_count_ = output_column_ids.count() + 10;
  scan_param.for_update_ = false;
  scan_param.column_ids_.reset();
  scan_param.pkey_ = part_key;
  scan_param.schema_version_ = schema_version;
  if (OB_FAIL(scan_param.column_ids_.assign(output_column_ids))) {
    LOG_WARN("fail to assign column id", K(ret));
  } else {
    SQL_ENG_LOG(DEBUG, "set scan param", K(output_column_ids));
    scan_param.expr_ctx_.calc_buf_ = NULL;
    scan_param.expr_ctx_.my_session_ = NULL;
    scan_param.expr_ctx_.phy_plan_ctx_ = NULL;
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    scan_param.trans_desc_ = ctx.param_.trans_desc_;
    scan_param.index_id_ = table_id;
    scan_param.sql_mode_ = SMO_DEFAULT;
    scan_param.allocator_->set_tenant_id(scan_param.pkey_.get_tenant_id());
  }
  return ret;
}

int ObTableService::fill_get_result(
    ObTableServiceCtx &ctx,
    const ObIArray<ObString> &properties,
    ObTableApiRowIterator *scan_result,
    ObTableOperationResult &operation_result)
{
  int ret = OB_SUCCESS;
  ObNewRow *row = NULL;
  table::ObITableEntity *entity = NULL;
  if (OB_FAIL(operation_result.get_entity(entity))) {
    LOG_WARN("failed to get entity", K(ret));
  } else if (OB_FAIL(scan_result->get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else {
    const int64_t rowkey_column_cnt = scan_result->get_rowkey_column_cnt();
    const int64_t N = row->get_count();
    ObObj cell_clone;
    for (int64_t i = rowkey_column_cnt; OB_SUCCESS == ret && i < N; ++i) {
      const ObString &name = properties.at(i - rowkey_column_cnt);
      const ObObj &cell = row->get_cell(i);
      if (OB_FAIL(ob_write_obj(*ctx.param_.allocator_, cell, cell_clone))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (OB_FAIL(entity->set_property(name, cell_clone))) {
        LOG_WARN("failed to set property", K(ret), K(name), K(cell));
      } else {
        LOG_DEBUG("yzfdebug get cell", K(name), K(i), K(cell), "row", (*row));
      }
    } // end for
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObTableServiceGetCtx::ObTableServiceGetCtx(common::ObArenaAllocator &alloc)
    :table_param_on_stack_(alloc),
     table_param_(&table_param_on_stack_),
     scan_result_(NULL)
{
  table_param_ = &table_param_on_stack_;
  scan_param_.table_param_ = table_param_;
}

int ObTableService::execute_get(ObTableServiceGetCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_table_scan_begin);
  ObTableApiGetRowIterator get_row_iter;
  ctx.scan_result_ = NULL;
  if (OB_FAIL(get_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init get row iterator, ", K(ret));
  } else if (OB_FAIL(get_row_iter.open(table_operation))) {
    LOG_WARN("Fail to open get row iterator, ", K(ret));
  } else if (OB_FAIL(fill_get_result(ctx, get_row_iter.get_properties(), &get_row_iter, result))) {
    LOG_WARN("failed to send result");
  } else {
    LOG_DEBUG("[yzfdebug] execute_get", "prop", get_row_iter.get_properties(), "table_param", *(ctx.table_param_), "scan_param", ctx.scan_param_, "trans_desc", *(ctx.param_.trans_desc_));
  }

  result.set_type(ObTableOperationType::GET);
  result.set_errno(ret);
  replace_ret_code(ret);
  NG_TRACE(S_table_scan_end);
  return ret;
}

int ObTableService::fill_multi_get_result(
    ObTableServiceGetCtx &ctx,
    const ObTableBatchOperation &batch_operation,
    ObTableApiRowIterator *scan_result,
    ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_size = batch_operation.at(0).entity().get_rowkey_size();
  ObNewRow *row = NULL;
  const int64_t N = batch_operation.count();
  bool did_get_next_row = true;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    // left join
    const ObTableEntity &entity = static_cast<const ObTableEntity&>(batch_operation.at(i).entity());
    ObRowkey expected_key = const_cast<ObTableEntity&>(entity).get_rowkey();
    ObTableOperationResult op_result;
    ObITableEntity *result_entity = result.get_entity_factory()->alloc();
    if (NULL == result_entity) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memroy for result_entity", K(ret));
      break;
    }
    op_result.set_entity(*result_entity);
    op_result.set_type(ObTableOperationType::GET);
    if (did_get_next_row) {
      if (OB_FAIL(scan_result->get_next_row(row))) {
        if (OB_ITER_END == ret) {
          // push empty entity
          ret = OB_SUCCESS;
          op_result.set_errno(OB_SUCCESS);
          if (OB_FAIL(result.push_back(op_result))) {
            LOG_WARN("failed to push back result", K(ret), K(i));
          }
          continue;
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        LOG_DEBUG("[yzfdebug] multi get row", K(ret), K(i), K(*row));
      }
    }
    if (OB_SUCC(ret)) {
      ObRowkey the_key(row->cells_, rowkey_size);
      ObObj cell_clone;
      if (expected_key.simple_equal(the_key)) {
        const int64_t N = row->get_count();
        for (int64_t i = rowkey_size; OB_SUCCESS == ret && i < N; ++i) {
          const ObString &name = scan_result->get_properties().at(i-rowkey_size);
          ObObj &cell = row->get_cell(i);
          if (OB_FAIL(ob_write_obj(*ctx.param_.allocator_, cell, cell_clone))) {
            LOG_WARN("failed to copy obj", K(ret));
          } else if (OB_FAIL(result_entity->set_property(name, cell_clone))) {
            LOG_WARN("failed to set property", K(ret));
          } else {
            LOG_DEBUG("get cell", K(name), K(i), K(cell));
          }
        } // end for
        did_get_next_row = true;
      } else {
        did_get_next_row = false;
        LOG_DEBUG("[yzfdebug] the row not exist", K(ret), K(expected_key));
      }
      op_result.set_errno(OB_SUCCESS);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.push_back(op_result))) {
        LOG_WARN("failed to push back result", K(ret), K(i));
      }
    }
  }  // end for
  return ret;
}

int ObTableService::multi_get(ObTableServiceGetCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiMultiGetRowIterator multi_get_iter;
  if (OB_FAIL(multi_get_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init multi get iter, ", K(ret));
  } else if (OB_FAIL(multi_get_iter.open(batch_operation))) {
    LOG_WARN("Fail to open multi get iter, ", K(ret));
  } else if (OB_FAIL(fill_multi_get_result(ctx, batch_operation, &multi_get_iter, result))) {
    LOG_WARN("failed to send result");
  }
  return ret;
}

// for update, if schema is index type, all columns in index schema
// should be added, including index defined columns, rowkey columns
// in data schema, and storing columns.
// for delete, the storing columns might be useless, but still added.
//
int ObTableService::add_index_columns_if_missing(schema::ObSchemaGetterGuard &schema_guard,
                                                 uint64_t data_table_id,
                                                 const schema::ObTableSchema *index_schema,
                                                 ObIArray<uint64_t> &column_ids,
                                                 ObIArray<ObExprResType> *columns_type)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  const schema::ObColumnSchemaV2 *column_schema = NULL;
  ObExprResType column_type;
  schema::ObTableSchema::const_column_iterator b = index_schema->column_begin();
  schema::ObTableSchema::const_column_iterator e = index_schema->column_end();
  for (; OB_SUCC(ret) && b != e; ++b) {  // for all columns of the index, include the storing columns
    if (NULL == (*b)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to to get column schema", K(*b));
    } else {
      if ((*b)->is_shadow_column()) {
        continue;
      }
      column_id = (*b)->get_column_id();
      bool found = false;
      const int64_t N = column_ids.count();
      for (int64_t i = 0; !found && i < N; ++i) {
        if (column_id == column_ids.at(i)) {
          found = true;
        }
      } // end for
      if (!found) {
        if (OB_FAIL(schema_guard.get_column_schema(data_table_id, column_id, column_schema))) {
          LOG_WARN("get column schema failed", K(data_table_id), K(column_id));
        } else if (NULL == column_schema) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", K(data_table_id), K(column_id));
        } else {
          if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
            LOG_WARN("failed to add column id", K(ret));
          } else if (NULL != columns_type) {
            if (OB_FAIL(cons_column_type(*column_schema, column_type))) {
              LOG_WARN("failed to cons column type", K(ret));
            } else if (OB_FAIL(columns_type->push_back(column_type))) {
              LOG_WARN("failed to push back", K(ret));
            } else {
              LOG_DEBUG("[yzfdebug] add missing index column for update/delete",
                        K(column_id), K(column_type));
            }
          }
        }
      }  // end if !found
    }
  }  // end for
  return ret;
}

int ObTableService::delete_can_use_put(table::ObTableEntityType entity_type, uint64_t table_id, bool &use_put)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = NULL;
  if (entity_type == ObTableEntityType::ET_HKV) {
    // hbase model table does not have secondary index
    use_put = true;
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL ptr", K(ret), K(table_schema));
  } else {
    // 1. there is not any index
    use_put = (table_schema->get_index_tid_count() <= 0);
  }
  return ret;
}
////////////////////////////////////////////////////////////////
int ObTableService::execute_delete(ObTableServiceGetCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTableApiDeleteRowIterator delete_row_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);

  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(delete_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init delete row iterator, ", K(ret));
  } else if (OB_FAIL(delete_row_iter.open(table_operation))) {
    LOG_WARN("Fail to open delete row iterator, ", K(ret));
  } else {
    // init dml param
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = delete_row_iter.get_schema_version();
    if (OB_FAIL(part_service_->delete_rows(
        *(ctx.param_.trans_desc_),
        dml_param,
        part_key,
        delete_row_iter.get_delete_column_ids(),
        &delete_row_iter,
        affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to delete", K(ret), K(table_id));
      }
    } else {
      LOG_DEBUG("[yzfdebug] delete rows", K(ret), K(affected_rows));
    }
  }


  if (ctx.param_.returning_affected_rows_) {
    result.set_affected_rows(affected_rows);
  } else {
    result.set_affected_rows(-1);  // always return -1
  }
  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(ObTableOperationType::DEL);
  return ret;
}

int ObTableService::cons_all_index_properties(schema::ObSchemaGetterGuard &schema_guard,
                                              const schema::ObTableSchema &table_schema,
                                              ObIArray<uint64_t> &column_ids,
                                              ObIArray<ObExprResType> *columns_type)
{
  int ret = OB_SUCCESS;
  if (table_schema.get_index_tid_count() <= 0) {
    // no index, do nothing
  } else {
    const schema::ObTableSchema *index_schema = NULL;
    const uint64_t table_id = table_schema.get_table_id();
    uint64_t index_tids[OB_MAX_INDEX_PER_TABLE + 1];
    int64_t index_count = ARRAYSIZEOF(index_tids);
    // get all the indexes
    if (OB_FAIL(schema_guard.get_can_write_index_array(table_id, index_tids, index_count))) {
      LOG_WARN("fail to get index", K(ret), K(table_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) { // for each writable index
        const uint64_t index_id = index_tids[i];
        // get index schema
        if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
          LOG_WARN("get index schema failed", K(index_id));
        } else {
          if (OB_FAIL(add_index_columns_if_missing(schema_guard, table_id, index_schema,
                                                   column_ids, columns_type))) {
            LOG_WARN("failed to add missing index column", K(ret));
          }
        }
      }  // end for
    }
  }
  return ret;
}

int ObTableService::multi_delete(ObTableServiceGetCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTableApiMultiDeleteRowIterator delete_row_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);

  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(delete_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init delete row iterator, ", K(ret));
  } else if (OB_FAIL(delete_row_iter.open(batch_operation))) {
    LOG_WARN("Fail to open delete row iterator, ", K(ret));
  } else {
    // init dml param
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = delete_row_iter.get_schema_version();

    int64_t result_idx = 0;
    while(OB_SUCC(ret) && !delete_row_iter.has_finished()) {
      affected_rows = 0;
      delete_row_iter.continue_iter();
      // get one row
      if (OB_FAIL(part_service_->delete_rows(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          delete_row_iter.get_delete_column_ids(),
          &delete_row_iter,
          affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to delete", K(ret), K(table_id));
        }
      } else {
        // add result for not exist rows
        const int64_t cur_delete_idx = delete_row_iter.get_cur_delete_idx();
        for (; OB_SUCC(ret) && result_idx < cur_delete_idx; ++result_idx) {
          if (OB_FAIL(add_one_result(result, ObTableOperationType::DEL, OB_SUCCESS, 0))) {
            LOG_WARN("failed to add result", K(ret), K(result_idx));
          } else {
            LOG_DEBUG("Success to add delete result, ", K(result_idx), K(cur_delete_idx), K(part_key));
          }
        } // end for
        // add result for this row
        if (OB_SUCC(ret) && result_idx <= cur_delete_idx) {
          if (OB_FAIL(add_one_result(
              result,
              ObTableOperationType::DEL,
              OB_SUCCESS,
              ctx.param_.returning_affected_rows_ ? affected_rows : -1))) {
            LOG_WARN("failed to add result", K(ret));
          } else {
            result_idx++;
            LOG_DEBUG("Success to add delete result, ", K(result_idx), K(cur_delete_idx), K(affected_rows), K(part_key));
          }
        }
      }
    } //end while

    if (OB_SUCC(ret)) {
      const int64_t N = batch_operation.count();
      for (; OB_SUCC(ret) && result_idx < N; result_idx++) {
        if (OB_FAIL(add_one_result(result, ObTableOperationType::DEL, OB_SUCCESS, 0))) {
          LOG_WARN("failed to add result", K(ret), K(result_idx));
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObTableService::execute_insert(
    ObTableServiceCtx &ctx,
    const ObTableOperation &table_operation,
    ObTableOperationResult &result,
    ObNewRowIterator *&duplicate_row_iter)
{
  int ret = OB_SUCCESS;
  duplicate_row_iter = NULL;
  ObTableApiInsertRowIterator insert_iter;
  ObNewRow *row = NULL;
  const uint64_t table_id = ctx.param_.table_id_;

  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(insert_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init insert row iterator, ", K(ret));
  } else if (OB_FAIL(insert_iter.open(table_operation))) {
    LOG_WARN("Fail to open insert row iterator, ", K(ret));
  } else if (OB_FAIL(insert_iter.get_next_row(row))) {
    LOG_WARN("Fail to get next row, ", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    // init dml param
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = insert_iter.get_schema_version();

    if (ObTableOperationType::INSERT_OR_UPDATE == table_operation.type()) {
      // use insert row with duplicate rows
      ObSEArray<uint64_t, COMMON_COLUMN_NUM> rowkey_column_ids;
      ObIArray<uint64_t> &full_column_ids = insert_iter.get_column_ids();
      const int64_t rowkey_column_cnt = table_operation.entity().get_rowkey_size();
      if (OB_FAIL(get_rowkey_column_ids(full_column_ids, rowkey_column_cnt, rowkey_column_ids))) {
        LOG_WARN("failed to fill rowkey column ids, ", K(ret));
      } else if (OB_FAIL(part_service_->insert_row(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          full_column_ids,
          rowkey_column_ids,
          *row,
          storage::ObInsertFlag::INSERT_RETURN_ONE_DUP,
          affected_rows,
          duplicate_row_iter))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to insert", K(ret), K(table_id));
        }
      }
    } else {
      // directly insert
      ObSingleRowIteratorWrapper single_row_iter(row);
      if (OB_FAIL(part_service_->insert_rows(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          insert_iter.get_column_ids(),
          &single_row_iter,
          affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to insert", K(ret), K(table_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      result.set_affected_rows(1);
      // for execute_increment()
      if (ObTableOperationType::INSERT != table_operation.type() /*increment/append*/
          && ctx.param_.returning_affected_entity_) {
        // need to return the new values to the client
        ObITableEntity *new_entity = NULL;
        if (OB_FAIL(result.get_entity(new_entity)) || OB_ISNULL(new_entity)) {
          LOG_WARN("failed to get entity", K(ret), K(new_entity));
        } else if (OB_FAIL(fill_new_entity(
            ctx.param_.returning_rowkey_,
            *row,
            table_operation.entity().get_rowkey_size(),
            insert_iter.get_properties(),
            *ctx.param_.allocator_,
            new_entity))) {
          LOG_WARN("failed to return new entity", K(ret));
        } else {
          LOG_DEBUG("[yzfdebug] increment return new entity", K(*new_entity));
        }
      }
    }
  }
  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(table_operation.type());
  return ret;
}

int ObTableService::multi_insert(ObTableServiceCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiMultiInsertRowIterator insert_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(insert_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init multi insert iterator, ", K(ret));
  } else if (OB_FAIL(insert_iter.open(batch_operation))) {
    LOG_WARN("Fail to open multi insert iterator, ", K(ret));
  } else {
    int64_t affected_rows = 0;
    // init dml param
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = insert_iter.get_schema_version();

    const int64_t N = batch_operation.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      insert_iter.continue_iter();
      if (OB_FAIL(part_service_->insert_rows(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          insert_iter.get_column_ids(),
          &insert_iter,
          affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to insert", K(ret), K(table_id));
        }
      }
      if (OB_SUCCESS == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        // ignore OB_ERR_PRIMARY_KEY_DUPLICATE
        if (OB_FAIL(add_one_result(result, ObTableOperationType::INSERT, ret, affected_rows))) {
          LOG_WARN("failed to add result", K(ret));
        }
      }
    }  // end for
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObTableService::do_replace(
    ObTableServiceCtx &ctx,
    ObPartitionKey &part_key,
    storage::ObDMLBaseParam &dml_param,
    ObIArray<uint64_t> &column_ids,
    ObIArray<uint64_t> &rowkey_column_ids,
    ObNewRow &row,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSingleRowIteratorWrapper single_row_iter(&row);
  ObNewRowIterator *duplicated_rows = NULL;
  int64_t del_rows = 0;

  // 1. try to insert
  if (OB_FAIL(part_service_->insert_row(
      *(ctx.param_.trans_desc_),
      dml_param,
      part_key,
      column_ids,
      rowkey_column_ids,
      row,
      storage::ObInsertFlag::INSERT_RETURN_ALL_DUP,
      affected_rows,
      duplicated_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      // 2. delete the row
      if (OB_FAIL(do_replace_delete(
          ctx,
          part_key,
          dml_param,
          column_ids,
          duplicated_rows,
          del_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("Fail to delete replace duplicate row, ", K(ret), K(part_key));
        }
      } else {
        // 3. insert the row again
        single_row_iter.reset();
        if (OB_FAIL(part_service_->insert_rows(*(ctx.param_.trans_desc_), dml_param, part_key,
                                               column_ids, &single_row_iter, affected_rows))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("we have already delete the rowkey and lock it, maybe have unique index", K(ret), K(row));
          } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("failed to insert", K(ret), K(part_key), K(row));
          }
        } else {
          // replace succ.
          affected_rows += del_rows;
        }
      }
    } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to insert", K(ret), K(part_key));
    }
  } else {
    // insert succ
  }

  if (NULL != duplicated_rows) {
    (void) part_service_->revert_insert_iter(part_key, duplicated_rows);
    duplicated_rows = NULL;
  }
  return ret;
}

int ObTableService::do_replace_delete(
    ObTableServiceCtx &ctx,
    common::ObPartitionKey &part_key,
    storage::ObDMLBaseParam &dml_param,
    common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *duplicated_rows,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  storage::ObTableScanParam scan_param;
  ObNewRow *dup_row = NULL;
  common::ObNewRowIterator *scan_iter = NULL;

  if (NULL == duplicated_rows) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid duplicate row iterator, ", K(ret));
  }

  while (OB_SUCC(ret) && OB_SUCC(duplicated_rows->get_next_row(dup_row))) {
    ObRowkey tmp_key(dup_row->cells_,  dup_row->count_);
    ObRowkey key;
    if (OB_FAIL(tmp_key.deep_copy(key, *ctx.param_.allocator_))) {
      LOG_WARN("fail to deep copy rowkey", K(ret));
    } else {
      common::ObNewRange range;
      if (OB_FAIL(range.build_range(part_key.table_id_, key))) {
        LOG_WARN("fail to build key range", K(ret), K(part_key.table_id_), K(key));
      } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
        LOG_WARN("fail to push back key range", K(ret), K(range));
      }
    }
  }
  if (ret != OB_ITER_END) {
    LOG_WARN("get next row not return ob_iter_end", K(ret));
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_FAIL(fill_query_scan_param(
      ctx,
      column_ids,
      dml_param.schema_version_,
      ObQueryFlag::ScanOrder::Forward,
      part_key.table_id_,
      -1, //limit
      0, //offset
      scan_param))) {
    LOG_WARN("fail to fill query scan param, ", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(scan_param, scan_iter))) {
    LOG_WARN("fail to table scan, ", K(ret));
  } else if (OB_FAIL(part_service_->delete_rows(*(ctx.param_.trans_desc_), dml_param, part_key,
      column_ids, scan_iter, affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to delete", K(ret), K(part_key));
    }
  } else {
    //delete success
  }

  if (NULL != scan_iter) {
    (void) part_service_->revert_scan_iter(scan_iter);
    scan_iter = NULL;
  }
  return ret;
}

int ObTableService::execute_replace(ObTableServiceCtx &ctx, const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiInsertRowIterator replace_iter;
  ObNewRow *row = NULL;
  const uint64_t table_id = ctx.param_.table_id_;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(replace_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init replace iter, ", K(ret), K(table_id));
  } else if (OB_FAIL(replace_iter.open(table_operation))) {
    LOG_WARN("Fail to open replace iter, ", K(ret), K(table_id));
  } else {
    int64_t affected_rows = 0;
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    // init dml param
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = replace_iter.get_schema_version();

    ObIArray<uint64_t> &full_column_ids = replace_iter.get_column_ids();
    const int64_t rowkey_column_cnt = table_operation.entity().get_rowkey_size();
    ObSEArray<uint64_t, COMMON_COLUMN_NUM> rowkey_column_ids;
    if (OB_FAIL(get_rowkey_column_ids(full_column_ids, rowkey_column_cnt, rowkey_column_ids))) {
      LOG_WARN("failed to fill rowkey column ids, ", K(ret));
    } else if (OB_FAIL(replace_iter.get_next_row(row))) {
      LOG_WARN("Fail to get replace row, ", K(ret), K(table_id));
    } else if (OB_FAIL(do_replace(ctx, part_key, dml_param,
        full_column_ids, rowkey_column_ids, *row, affected_rows))) {
      LOG_WARN("failed to do replace", K(ret));
    } else {
      result.set_affected_rows(affected_rows);
    }
  }
  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(ObTableOperationType::REPLACE);
  return ret;
}

int ObTableService::multi_replace(ObTableServiceCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = ctx.param_.table_id_;
  ObTableApiMultiInsertRowIterator replace_iter;
  ObNewRow *row = NULL;

  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table id is invalid", K(ret), K(table_id));
  } else if (OB_FAIL(replace_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init insert iter, ", K(ret), K(table_id));
  } else if (OB_FAIL(replace_iter.open(batch_operation))) {
    LOG_WARN("Fail to open batch operation, ", K(ret), K(table_id));
  } else {
    // init dml param
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = replace_iter.get_schema_version();

    // fill column ids
    ObIArray<uint64_t> &column_ids = replace_iter.get_column_ids();
    ObSEArray<uint64_t, COMMON_COLUMN_NUM> rowkey_column_ids;
    const ObTableOperation &one_operation = batch_operation.at(0);
    const ObITableEntity &one_entity = one_operation.entity();

    for (int64_t i = 0; OB_SUCC(ret) && i < one_entity.get_rowkey_size(); ++i) {
      if (OB_FAIL(rowkey_column_ids.push_back(column_ids.at(i)))) {
        LOG_WARN("failed to fill rowkey column ids, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      const int64_t N = batch_operation.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        replace_iter.continue_iter();
        if (OB_FAIL(replace_iter.get_next_row(row))) {
          LOG_WARN("Fail to get next replace row, ", K(ret), K(i));
        } else if (OB_FAIL(do_replace(ctx, part_key, dml_param, column_ids, rowkey_column_ids, *row, affected_rows))) {
          LOG_WARN("failed to do replace", K(ret));
        } else if (OB_FAIL(add_one_result(result, ObTableOperationType::REPLACE, OB_SUCCESS, affected_rows))) {
          LOG_WARN("failed to add result", K(ret));
        }
      } // end for
    }
  }
  return ret;
}
////////////////////////////////////////////////////////////////
// update & multi_update
////////////////////////////////////////////////////////////////

int ObTableService::execute_update(ObTableServiceGetCtx &ctx,
    const ObTableOperation &table_operation, ObNewRow *target_row, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
  ObTableApiUpdateRowIterator update_iter;
  ObRowkey rowkey;
  if (NULL == target_row) {
    rowkey = const_cast<ObITableEntity &>(table_operation.entity()).get_rowkey();
  } else {
    const int64_t rowkey_cnt = table_operation.entity().get_rowkey_size();
    if (OB_UNLIKELY(rowkey_cnt > target_row->count_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(rowkey_cnt), K(target_row));
    } else {
      rowkey.assign(target_row->cells_, rowkey_cnt);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init update row iterator, ", K(ret));
  } else if (OB_FAIL(update_iter.open(table_operation, rowkey,
      NULL != target_row/*need_update_rowkey*/))) {
    LOG_WARN("Fail to open update row iterator, ", K(ret));
  } else {
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = update_iter.get_schema_version();

    if (OB_FAIL(part_service_->update_rows(
        *(ctx.param_.trans_desc_),
        dml_param,
        part_key,
        update_iter.get_column_ids(),
        update_iter.get_update_column_ids(),
        &update_iter,
        affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to update_rows", K(ret), K(part_key));
      }
    } else {
      result.set_affected_rows(affected_rows);
    }
  }

  result.set_errno(ret);
  replace_ret_code(ret);
  result.set_type(ObTableOperationType::UPDATE);
  return ret;
}


int ObTableService::fill_new_entity(
    bool returning_rowkey,
    const common::ObNewRow &row,
    const int64_t primary_key_size,
    const common::ObIArray<common::ObString> &properties,
    common::ObIAllocator &alloc,
    ObITableEntity *new_entity)
{
  int ret = OB_SUCCESS;
  new_entity->reset();
  ObObj cell_clone;
  ObString name_clone;
  if (primary_key_size > row.count_ || OB_ISNULL(new_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey size", K(ret), K(primary_key_size), KP(new_entity));
  } else if (returning_rowkey) {
    for (int64_t i = 0; i < primary_key_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(ob_write_obj(alloc, row.cells_[i], cell_clone))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (OB_FAIL(new_entity->add_rowkey_value(cell_clone))) {
        LOG_WARN("failed to add rowkey value", K(ret), K(cell_clone));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t N = primary_key_size + properties.count();
    for (int64_t i = primary_key_size, j = 0; OB_SUCCESS == ret && i < N; ++i, ++j) {
      // deep copy property
      const ObString &name = properties.at(j);
      const ObObj &cell = row.cells_[i];
      if (OB_FAIL(ob_write_string(alloc, name, name_clone))) {
        LOG_WARN("failed to copy string", K(ret), K(name));
      } else if (OB_FAIL(ob_write_obj(alloc, cell, cell_clone))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (OB_FAIL(new_entity->set_property(name_clone, cell_clone))) {
        LOG_WARN("failed to set property", K(ret));
      } else {
        LOG_DEBUG("[yzfdebug] affected new cell", K(name), K(i), K(cell_clone));
      }
    } // end for
  }
  return ret;
}

int ObTableService::execute_increment_by_update(
    ObTableServiceGetCtx &ctx,
    const ObTableOperation &table_operation,
    ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTableApiUpdateRowIterator increment_row_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
  ObRowkey rowkey = const_cast<ObITableEntity &>(table_operation.entity()).get_rowkey();
  if (OB_FAIL(increment_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init increment row iter, ", K(ret));
  } else if (OB_FAIL(increment_row_iter.open(table_operation, rowkey))) {
    LOG_WARN("Fail to open increment row iter, ", K(ret));
  } else {
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = increment_row_iter.get_schema_version();

    if (OB_FAIL(part_service_->update_rows(
        *(ctx.param_.trans_desc_),
        dml_param,
        part_key,
        increment_row_iter.get_column_ids(),
        increment_row_iter.get_update_column_ids(),
        &increment_row_iter,
        affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("failed to update_rows", K(ret), K(part_key));
      }
    } else {
      if (affected_rows > 0) {
        affected_rows = 1;
        if (ctx.param_.returning_affected_entity_) {
          // need to return the new values to the client
          ObITableEntity *new_entity = NULL;
          ObNewRow *new_row = NULL;
          if (NULL == (new_row = increment_row_iter.get_cur_new_row())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected error, the new row is null, ", K(ret), K(part_key));
          } else if (OB_FAIL(result.get_entity(new_entity)) || OB_ISNULL(new_entity)) {
            LOG_WARN("failed to get entity", K(ret), K(new_entity));
          } else if (OB_FAIL(fill_new_entity(
              ctx.param_.returning_rowkey_,
              *new_row,
              table_operation.entity().get_rowkey_size(),
              increment_row_iter.get_properties(),
              *ctx.param_.allocator_,
              new_entity))) {
            LOG_WARN("failed to return new entity", K(ret));
          } else {
            LOG_DEBUG("[yzfdebug] increment return new entity", K(*new_entity));
          }
        }
      }

      if (NULL == (increment_row_iter.get_cur_new_row())) {
        ret = OB_EMPTY_RESULT;
      }
    }
  }

  if (OB_EMPTY_RESULT != ret) {
    result.set_affected_rows(affected_rows);
    result.set_errno(ret);
    replace_ret_code(ret);
    result.set_type(table_operation.type());
  }
  return ret;
}

int ObTableService::execute_increment(ObTableServiceGetCtx &ctx,
                                      const ObTableOperation &table_operation,
                                      ObTableOperationResult &result)
{
  int ret = execute_increment_by_update(ctx, table_operation, result);
  if (OB_EMPTY_RESULT == ret) {
    // the row not exist, insert it
    ctx.reset_get_ctx();
    ObNewRowIterator *duplicate_row_iter = nullptr;
    ret = execute_insert(ctx, table_operation, result, duplicate_row_iter);
    if (OB_SUCC(ret) && OB_ERR_PRIMARY_KEY_DUPLICATE == result.get_errno()) {
      // concurrent inserted by another thread, try again in process()
      ret = OB_TRY_LOCK_ROW_CONFLICT;
      LOG_WARN("insert failed again after update failed", K(ret), K(result));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("Fail to execute increment, ", K(ret));
  } else {
    LOG_DEBUG("Success to execute increment, ", K(result));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
// multi update
int ObTableService::multi_update(ObTableServiceGetCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiMultiUpdateRowIterator update_row_iter;
  const uint64_t table_id = ctx.param_.table_id_;
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
  int64_t affected_rows = 0;
  int64_t result_idx = 0;

  if (OB_FAIL(update_row_iter.init(*part_service_, *schema_service_, ctx))) {
    LOG_WARN("Fail to init multi update row iterator, ", K(ret));
  } else if (OB_FAIL(update_row_iter.open(batch_operation))) {
    LOG_WARN("Fail to open multi update row iterator, ", K(ret));
  } else {
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_;
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = update_row_iter.get_schema_version();

    while(OB_SUCC(ret) && !update_row_iter.has_finished()) {
      affected_rows = 0;
      update_row_iter.continue_iter();
      if (OB_FAIL(part_service_->update_rows(
          *(ctx.param_.trans_desc_),
          dml_param,
          part_key,
          update_row_iter.get_column_ids(),
          update_row_iter.get_update_column_ids(),
          &update_row_iter,
          affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to update_rows", K(ret), K(part_key));
        }
      } else {
        // add result for not exist rows
        const int64_t cur_update_idx = update_row_iter.get_cur_update_idx();
        for (; OB_SUCC(ret) && result_idx < cur_update_idx; ++result_idx) {
          if (OB_FAIL(add_one_result(result, ObTableOperationType::UPDATE, OB_SUCCESS, 0))) {
            LOG_WARN("failed to add result", K(ret), K(result_idx));
          } else {
            LOG_DEBUG("Success to add result, ", K(result_idx), K(cur_update_idx), K(part_key));
          }
        } // end for
        // add result for this row
        if (OB_SUCC(ret) && result_idx <= cur_update_idx) {
          if (OB_FAIL(add_one_result(result, ObTableOperationType::UPDATE, OB_SUCCESS, affected_rows))) {
            LOG_WARN("failed to add result", K(ret));
          } else {
            result_idx++;
            LOG_DEBUG("Success to add result, ", K(result_idx), K(cur_update_idx), K(part_key));
          }
        }
      }
    } // end for

    if (OB_SUCC(ret)) {
      const int64_t N = batch_operation.count();
      for (; OB_SUCC(ret) && result_idx < N; result_idx++) {
        if (OB_FAIL(add_one_result(result, ObTableOperationType::UPDATE, OB_SUCCESS, 0))) {
          LOG_WARN("failed to add result", K(ret), K(result_idx));
        }
      }
    }
  }
  return ret;
}

int ObTableService::batch_execute(ObTableServiceGetCtx &ctx, const ObTableBatchOperation &batch_operation, ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  int64_t N = batch_operation.count();
  ObNewRowIterator *duplicate_row_iter = nullptr;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    const ObTableOperation &table_operation = batch_operation.at(i);
    ObTableOperationResult op_result;
    ObITableEntity *result_entity = result.get_entity_factory()->alloc();
    if (NULL == result_entity) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memroy for result_entity", K(ret));
      break;
    }
    op_result.set_entity(*result_entity);
    switch(table_operation.type()) {
      case ObTableOperationType::GET:
        ret = execute_get(ctx, table_operation, op_result);
        break;
      case ObTableOperationType::INSERT:
        ret = execute_insert(ctx, table_operation, op_result, duplicate_row_iter);
        break;
      case ObTableOperationType::DEL:
        ret = execute_delete(ctx, table_operation, op_result);
        break;
      case ObTableOperationType::UPDATE:
        ret = execute_update(ctx, table_operation, nullptr, op_result);
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        ret = execute_insert_or_update(ctx, table_operation, op_result);
        break;
      case ObTableOperationType::REPLACE:
        ret = execute_replace(ctx, table_operation, op_result);
        break;
      case ObTableOperationType::APPEND:
      case ObTableOperationType::INCREMENT:
        ret = execute_increment(ctx, table_operation, op_result);
        // no need to deep copy cells here which have already been deep copied in execute_increment()
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", batch_operation.at(0).type());
        break;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.push_back(op_result))) {
        LOG_WARN("failed to push back result", K(ret));
      } else {
        ctx.reset_get_ctx();
      }
    } else {
      LOG_WARN("failed to execute, ", K(ret), K(table_operation.type()));
    }
  } // end for
  return ret;
}
////////////////////////////////////////////////////////////////
// execute query
////////////////////////////////////////////////////////////////
int ObTableService::cons_index_key_type(schema::ObSchemaGetterGuard &schema_guard,
                                        const share::schema::ObTableSchema *index_schema,
                                        uint64_t data_table_id,
                                        common::ObIArray<sql::ObExprResType> &columns_type)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  const schema::ObColumnSchemaV2 *column_schema = NULL;
  ObExprResType column_type;
  const ObIndexInfo &index_key_info = index_schema->get_index_info();
  const int64_t N = index_key_info.get_size();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    if (OB_FAIL(index_key_info.get_column_id(i, column_id))) {
      LOG_WARN("failed to get index column", K(ret), K(i));
    } else if (OB_FAIL(schema_guard.get_column_schema(data_table_id, column_id, column_schema))) {
      LOG_WARN("get column schema failed", K(data_table_id), K(column_id));
    } else if (NULL == column_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(data_table_id), K(column_id));
    } else if (OB_FAIL(cons_column_type(*column_schema, column_type))) {
      LOG_WARN("failed to cons column type", K(ret));
    } else if (OB_FAIL(columns_type.push_back(column_type))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      LOG_DEBUG("[yzfdebug] add index column for scan",
                K(data_table_id), K(column_id), K(column_type));
    }
  }  // end for
  return ret;
}

int ObTableService::get_index_id_by_name(schema::ObSchemaGetterGuard &schema_guard, uint64_t base_table_id,
                                         const ObString &index_name, uint64_t &index_id,
                                         ObIArray<sql::ObExprResType> &columns_type,
                                         const share::schema::ObTableSchema *&index_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE];
  int64_t table_index_count = OB_MAX_INDEX_PER_TABLE;
  index_schema = nullptr;
  if (index_name.empty()
      || 0 == index_name.case_compare(ObStmtHint::PRIMARY_KEY)) {
    index_id = base_table_id;
  } else if (OB_FAIL(schema_guard.get_can_read_index_array(base_table_id, tids, table_index_count, false))) {
    LOG_WARN("failed to get can read index", K(ret));
  } else if (table_index_count > OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table index count is bigger than OB_MAX_INDEX_PER_TABLE", K(ret), K(table_index_count));
  } else {
    LOG_DEBUG("get readable index", K(table_index_count));
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < table_index_count; ++i) {
      uint64_t this_index_id = tids[i];
      ObString this_index;
      if (OB_FAIL(schema_guard.get_table_schema(this_index_id, index_schema))
          || OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get table schema", K(this_index_id), K(ret));
      } else if (OB_FAIL(index_schema->get_index_name(this_index))) {
        LOG_WARN("fail to get index name", K(this_index), K(ret), K(this_index_id));
      } else if (0 != index_name.case_compare(this_index)) {
        //do nothing, just continue
        continue;
      } else {
        found = true;
        index_id = this_index_id;
        //record the column type in index to check the object type in scan range
        ret = cons_index_key_type(schema_guard, index_schema, base_table_id, columns_type);
      }
    }  // end for
    if (!found) {
      ret = OB_ERR_INDEX_UNKNOWN;
    }
  }
  return ret;
}

int ObTableService::fill_query_table_param(uint64_t table_id,
                                           const ObIArray<ObString> &properties,
                                           const ObString &index_name,
                                           share::schema::ObTableParam &table_param,
                                           ObIArray<uint64_t> &output_column_ids,
                                           common::ObIArray<sql::ObExprResType> &rowkey_columns_type,
                                           int64_t &schema_version,
                                           uint64_t &index_id,
                                           int64_t &padding_num,
                                           table::ObHColumnDescriptor *hcolumn_desc)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  const schema::ObTableSchema *table_schema = NULL;
  const schema::ObTableSchema *index_schema = NULL;
  if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_ERROR("NULL ptr", K(ret), K(table_schema));
  } else if (OB_FAIL(get_index_id_by_name(schema_guard, table_id, index_name, index_id,
                                          rowkey_columns_type, index_schema))) {
    LOG_WARN("failed to get index id", K(ret), K(index_name), K(table_id));
  } else {
    NG_TRACE_EXT(idx, OB_ID(idx), index_id, OB_ID(tag1), index_name);
    schema_version = table_schema->get_schema_version();
    const int64_t key_column_cnt = rowkey_columns_type.count();
    padding_num = (NULL == index_schema) ? -1 : (index_schema->get_rowkey_column_num() - key_column_cnt);
    LOG_DEBUG("[xilin debug]padding", K(padding_num), K(key_column_cnt), K(index_name));

    const bool index_back = (index_id != table_id);
    bool is_index_supported = true;
    if (index_back && OB_FAIL(check_index_supported(schema_guard, table_schema, index_id, is_index_supported))) {
      LOG_WARN("fail to check index supported", K(ret), K(index_id));
    } else if (OB_UNLIKELY(!is_index_supported)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("index type is not supported by table api", K(ret), K(table_id), K(index_id));
    } else if (OB_FAIL(cons_rowkey_infos(*table_schema, NULL, index_back ? NULL : &rowkey_columns_type))) {
    } else if (OB_FAIL(cons_properties_infos(*table_schema, properties, output_column_ids, NULL))) {
    } else if (OB_FAIL(table_param.convert(*table_schema, ((NULL == index_schema) ? *table_schema: *index_schema),
                                           output_column_ids, index_back))) {
      LOG_WARN("failed to convert table param", K(ret));
    } else if (!table_schema->get_comment_str().empty()
               && NULL != hcolumn_desc) {
      if (OB_FAIL(hcolumn_desc->from_string(table_schema->get_comment_str()))) {
        LOG_WARN("failed to parse hcolumn_desc from comment string", K(ret),
                 "comment", table_schema->get_comment_str());
      } else {
        LOG_DEBUG("[yzfdebug] get ttl", K(table_id),
                  "comment", table_schema->get_comment_str(),
                  "ttl", hcolumn_desc->get_time_to_live());
      }
    }
  }
  return ret;
}

int ObTableService::fill_query_scan_ranges(ObTableServiceCtx &ctx,
                                           const ObTableQuery &query,
                                           int64_t padding_num,
                                           storage::ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  scan_param.key_ranges_.reset();
  const ObIArray<common::ObNewRange> &scan_ranges = query.get_scan_ranges();
  int64_t N = scan_ranges.count();
  // check obj type in ranges
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) { // foreach range
    const ObNewRange &range = scan_ranges.at(i);
    // check column type
    for (int64_t j = 0; OB_SUCCESS == ret && j < 2; ++j) {
      const ObRowkey *p_key = nullptr;
      if (0 == j) {
        p_key = &range.get_start_key();
      } else {
        p_key = &range.get_end_key();
      }
      if (p_key->is_min_row() || p_key->is_max_row()) {
        continue;
      } else {
        if (p_key->get_obj_cnt() != ctx.columns_type_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wrong rowkey size", K(ret), K(i), K(j), K(*p_key), K_(ctx.columns_type));
        } else {
          const int64_t M = p_key->get_obj_cnt();
          for (int64_t k = 0; OB_SUCCESS == ret && k < M; ++k) {
            ObObj &obj = const_cast<ObObj&>(p_key->get_obj_ptr()[k]);
            if (obj.is_min_value() || obj.is_max_value()) {
              continue;
            } else if (OB_FAIL(ObTableService::check_column_type(ctx.columns_type_.at(k), obj))) {
            }
          }
        }
      }  // end else
    } // end for
    if (OB_UNLIKELY(padding_num > 0)) {
      // index scan need fill primary key object
      ObNewRange index_key_range = range;
      for (int64_t j = 0; OB_SUCCESS == ret && j < 2; ++j) {
        const ObRowkey *p_key = nullptr;
        if (0 == j) {
          p_key = &range.get_start_key();
        } else {
          p_key = &range.get_end_key();
        }
        if (p_key->is_min_row() || p_key->is_max_row()) {
          continue;
        } else {
          const int64_t old_objs_num = p_key->get_obj_cnt();
          const int64_t new_objs_num = old_objs_num + padding_num;
          ObObj *new_objs = static_cast<ObObj*>(ctx.param_.allocator_->alloc(sizeof(ObObj)*new_objs_num));
          if (nullptr == new_objs) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no memory", K(ret));
          } else {
            const ObObj *old_objs = p_key->get_obj_ptr();
            for (int64_t k = 0; k < old_objs_num; ++k) {
              new_objs[k] = old_objs[k];  // shallow copy
            } // end for
            if (0 == j) {  // padding for startkey
              for (int64_t k = 0; k < padding_num; ++k) {
                new_objs[k+old_objs_num] = ObObj::make_min_obj();
              }
              index_key_range.start_key_.assign(new_objs, new_objs_num);
            } else {  // padding for endkey
              for (int64_t k = 0; k < padding_num; ++k) {
                new_objs[k+old_objs_num] = ObObj::make_max_obj();
              }
              index_key_range.end_key_.assign(new_objs, new_objs_num);
            }
          }
        }
      }  // end for
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan_param.key_ranges_.push_back(index_key_range))) {
          LOG_WARN("fail to push back key range", K(ret), K(index_key_range));
        } else {
          LOG_DEBUG("[yzfdebug] add key range for index scan", K(ret), K(index_key_range));
        }
      }
    } else {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
          LOG_WARN("fail to push back key range", K(ret), K(range));
        }
      }
    }
  } // end for
  return ret;
}

int ObTableService::fill_query_scan_param(ObTableServiceCtx &ctx, const ObIArray<uint64_t> &output_column_ids,
    int64_t schema_version, ObQueryFlag::ScanOrder scan_order, uint64_t index_id, int32_t limit, int32_t offset,
    storage::ObTableScanParam &scan_param, bool for_update /* false */)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = ctx.param_.table_id_;
  const bool index_back = (index_id != table_id);
  ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
  scan_param.timeout_ = ctx.param_.timeout_ts_;
  ObQueryFlag query_flag(scan_order,
                         false, // daily_merge
                         false, // optimize
                         false, // whole_macro_scan
                         false, // full_row
                         index_back, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         true // read_latest
                         );
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  scan_param.reserved_cell_count_ = output_column_ids.count() + 10;
  scan_param.for_update_ = for_update;
  scan_param.column_ids_.reset();
  scan_param.pkey_ = part_key;
  scan_param.schema_version_ = schema_version;
  if (OB_FAIL(scan_param.column_ids_.assign(output_column_ids))) {
    LOG_WARN("fail to assign column id", K(ret));
  } else {
    SQL_ENG_LOG(DEBUG, "set scan param", K(output_column_ids));
    scan_param.expr_ctx_.calc_buf_ = NULL;
    scan_param.expr_ctx_.my_session_ = NULL;
    scan_param.expr_ctx_.phy_plan_ctx_ = NULL;
    scan_param.limit_param_.limit_ = limit;
    scan_param.limit_param_.offset_ = offset;
    scan_param.trans_desc_ = &*(ctx.param_.trans_desc_);
    scan_param.index_id_ = index_id;
    scan_param.sql_mode_ = SMO_DEFAULT;
    scan_param.allocator_->set_tenant_id(scan_param.pkey_.get_tenant_id());
    LOG_DEBUG("[yzfdebug] scan param", K(scan_param));
  }
  return ret;
}

int ObNormalTableQueryResultIterator::get_next_result(table::ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else if (is_first_result_ || is_query_sync_) {
    if (0 != one_result_->get_property_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("property should be empty", K(ret));
    }
    const ObIArray<ObString> &select_columns = query_->get_select_columns();
    const int64_t N = select_columns.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      if (OB_FAIL(one_result_->add_property_name(select_columns.at(i)))) {
        LOG_WARN("failed to copy name", K(ret));
      }
    } // end for

    if (is_first_result_) {
      last_row_ = NULL;
      is_first_result_ = false;
    }
  } else {
    one_result_->reset_except_property();
  }

  if (OB_SUCC(ret)) {
    if (NULL != last_row_) {
      if (OB_FAIL(one_result_->add_row(*last_row_))) {
        LOG_WARN("failed to add row, ", K(ret));
      }
      last_row_ = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    next_result = one_result_;
    ObNewRow *row = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(scan_result_->get_next_row(row))) {
      LOG_DEBUG("[yzfdebug] scan result", "row", *row);
      if (OB_FAIL(one_result_->add_row(*row))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          last_row_ = row;
          break;
        } else {
          LOG_WARN("failed to add row", K(ret));
        }
      } else if (one_result_->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
        NG_TRACE(tag9);
        break;
      } else {
        LOG_DEBUG("[yzfdebug] scan return one row", "row", *row);
      }
    }  // end while
    if (OB_ITER_END == ret) {
      has_more_rows_ = false;
      if (one_result_->get_row_count() > 0) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

bool ObNormalTableQueryResultIterator::has_more_result() const
{
  return has_more_rows_;
}

ObNormalTableQueryResultIterator *ObTableServiceQueryCtx::get_normal_result_iterator(
    const ObTableQuery &query, table::ObTableQueryResult &one_result)
{
  if (NULL == normal_result_iterator_) {
    normal_result_iterator_ = OB_NEWx(ObNormalTableQueryResultIterator, param_.allocator_, query, one_result);
    if (NULL == normal_result_iterator_) {
      LOG_WARN("failed to allocate result iterator");
    }
  }
  return normal_result_iterator_;
}

ObHTableFilterOperator *ObTableServiceQueryCtx::get_htable_result_iterator(
    const ObTableQuery &query, table::ObTableQueryResult &one_result)
{
  if (NULL == htable_result_iterator_) {
    htable_result_iterator_ = OB_NEWx(ObHTableFilterOperator, param_.allocator_, query, one_result);
    if (NULL == htable_result_iterator_) {
      LOG_WARN("failed to allocate htable filter");
    }
  }
  return htable_result_iterator_;
}

void ObTableServiceQueryCtx::destroy_result_iterator(storage::ObPartitionService *part_service)
{
  if (NULL != normal_result_iterator_) {
    normal_result_iterator_->~ObNormalTableQueryResultIterator();
    normal_result_iterator_ = NULL;
  }
  if (NULL != htable_result_iterator_) {
    htable_result_iterator_->~ObHTableFilterOperator();
    htable_result_iterator_ = NULL;
  }
  if (NULL != scan_result_) {
    if (NULL == part_service) {
      LOG_ERROR("part_service is NULL, memory leak");
    } else {
      part_service->revert_scan_iter(scan_result_);
      scan_result_ = NULL;
    }
  }
}

int ObTableService::check_htable_query_args(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  int64_t N = select_columns.count();
  if (N != 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("TableQuery with htable_filter should select 4 columns", K(ret), K(N));
  }
  if (OB_SUCC(ret)) {
    if (ObHTableConstants::ROWKEY_CNAME_STR != select_columns.at(0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select K as the first column", K(ret), K(select_columns));
    } else if (ObHTableConstants::CQ_CNAME_STR != select_columns.at(1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select Q as the second column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VERSION_CNAME_STR != select_columns.at(2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select T as the third column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VALUE_CNAME_STR != select_columns.at(3)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select V as the fourth column", K(ret), K(select_columns));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != query.get_offset()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable scan should not set Offset and Limit", K(ret), K(query));
    } else if (ObQueryFlag::Forward != query.get_scan_order() && ObQueryFlag::Reverse != query.get_scan_order()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("TableQuery with htable_filter only support forward and reverse scan yet", K(ret));
    }
  }
  return ret;
}

int ObTableService::execute_query(ObTableServiceQueryCtx &ctx, const ObTableQuery &query,
    table::ObTableQueryResult &one_result, table::ObTableQueryResultIterator *&query_result,
    bool for_update /* false */)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, COMMON_COLUMN_NUM> output_column_ids;
  int64_t schema_version = 0;
  ctx.scan_result_ = NULL;
  const uint64_t table_id = ctx.param_.table_id_;
  uint64_t index_id = OB_INVALID_ID;
  int64_t padding_num = 0;
  
  ObHColumnDescriptor hcolumn_desc;
  ObHColumnDescriptor *p_hcolumn_desc = NULL;
  if (query.get_htable_filter().is_valid()) {
    if (OB_FAIL(check_htable_query_args(query))) {
      LOG_WARN("invalid query request", K(ret));
    } else if (NULL == (query_result = ctx.get_htable_result_iterator(query, one_result))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate htable result iterator", K(ret));
    } else if (OB_FAIL(ctx.htable_result_iterator_->parse_filter_string(ctx.param_.allocator_))) {
      LOG_WARN("failed to parse htable filter string", K(ret));
    } else {
      p_hcolumn_desc = &hcolumn_desc;
    }
  } else {
    if (NULL == (query_result = ctx.get_normal_result_iterator(query, one_result))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate result iterator", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_query_table_param(table_id, query.get_select_columns(),
                                            query.get_index_name(),
                                            *(ctx.table_param_), output_column_ids,
                                            ctx.columns_type_, schema_version,
                                            index_id, padding_num,
                                            p_hcolumn_desc))) { // @todo optimize, table_param_ can be cached
    LOG_WARN("failed to fill param", K(ret));
  } else if (OB_FAIL(fill_query_scan_ranges(ctx, query,
                                            (table_id != index_id) ? padding_num : -1,
                                            ctx.scan_param_))) {
    LOG_WARN("failed to fill range", K(ret));
  } else if (OB_FAIL(fill_query_scan_param(ctx,
                 output_column_ids,
                 schema_version,
                 query.get_scan_order(),
                 index_id,
                 query.get_limit(),
                 query.get_offset(),
                 ctx.scan_param_,
                 for_update))) {
    LOG_WARN("failed to fill param", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(ctx.scan_param_, ctx.scan_result_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("fail to scan table", K(ret));
    }
  } else {
    if (query.get_htable_filter().is_valid()) {
      ctx.htable_result_iterator_->set_scan_result(ctx.scan_result_);
      if (OB_ISNULL(p_hcolumn_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null p_hcolumn_desc", K(ret));
      } else {
        if (p_hcolumn_desc->get_time_to_live() > 0) {
          ctx.htable_result_iterator_->set_ttl(p_hcolumn_desc->get_time_to_live());
        }
        if (p_hcolumn_desc->get_max_version() > 0) {
          ctx.htable_result_iterator_->set_max_version(p_hcolumn_desc->get_max_version());
        }
      }
    } else {
      ctx.normal_result_iterator_->set_scan_result(ctx.scan_result_);
    }
  }
  return ret;
}

/* given the
   1. ttl and max_version
   2. start k && cq 
   3. records limit
   return the end k & cq
*/

int ObTableService::execute_ttl_delete(ObTableServiceTTLCtx &ctx, const ObTableTTLOperation &ttl_operation, ObTableTTLOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, COMMON_COLUMN_NUM> output_column_ids;
  int64_t schema_version = 0;
  ctx.scan_result_ = NULL;
  const uint64_t table_id = ctx.param_.table_id_;
  uint64_t index_id = OB_INVALID_ID;
  int64_t padding_num = 0;
  table::ObTableQuery query;
  ObTableTTLDeleteRowIterator ttl_row_iter;
  
  if (OB_FAIL(ttl_row_iter.init(ttl_operation))) {
    LOG_WARN("fail to init ttl delete row iterator", K(ret));
  } else if (OB_FAIL(generate_ttl_query(ttl_operation, ctx, query))) {
    LOG_WARN("fail to generate ttl query", K(ret));
  } else if (OB_FAIL(fill_query_table_param(table_id, query.get_select_columns(),
                                            query.get_index_name(),
                                            *(ctx.table_param_), output_column_ids,
                                            ctx.columns_type_, schema_version,
                                            index_id, padding_num,
                                            NULL))) {
    LOG_WARN("failed to fill param", K(ret));
  } else if (OB_FAIL(fill_query_scan_ranges(ctx, query,
                                            (table_id != index_id) ? padding_num : -1,
                                            ctx.scan_param_))) {
    LOG_WARN("failed to fill range", K(ret));
  } else if (OB_FAIL(fill_query_scan_param(ctx, output_column_ids, schema_version,
                                           query.get_scan_order(), index_id, query.get_limit(),
                                           query.get_offset(), ctx.scan_param_))) {
    LOG_WARN("failed to fill param", K(ret));
  } else if (OB_FAIL(part_service_->table_scan(ctx.scan_param_, ctx.scan_result_))) {
    LOG_WARN("fail to scan table", K(ret));
  } else {
    ttl_row_iter.set_scan_result(ctx.scan_result_);
    int64_t affected_rows = 0;
    ObPartitionKey part_key(table_id, ctx.param_.partition_id_, 0);
    // init dml param
    storage::ObDMLBaseParam dml_param;
    dml_param.timeout_ =  ctx.param_.timeout_ts_; // todo@dazhi: should not timeout
    dml_param.is_total_quantity_log_ = (ObBinlogRowImageType::FULL == ctx.param_.binlog_row_image_type_);
    dml_param.tz_info_ = NULL;
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.schema_version_ = ctx.scan_param_.schema_version_;
    if (OB_FAIL(part_service_->delete_rows(
        *(ctx.param_.trans_desc_),
        dml_param,
        part_key,
        ctx.scan_param_.column_ids_,
        &ttl_row_iter,
        affected_rows))) {
      LOG_WARN("failed to delete", K(ret), K(table_id));
    } else {
      LOG_DEBUG("delete rows", K(ret), K(affected_rows));
    }

    if (OB_SUCC(ret)) {
      uint64_t iter_ttl_cnt = ttl_row_iter.ttl_cnt_;
      uint64_t iter_max_version_cnt = ttl_row_iter.max_version_cnt_;
      uint64_t iter_return_cnt = iter_ttl_cnt + iter_max_version_cnt;
      if (OB_UNLIKELY(affected_rows != iter_return_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected affect rows", K(ret), K(affected_rows), K(iter_return_cnt));
      } else {
        result.ttl_del_rows_ = iter_ttl_cnt;
        result.max_version_del_rows_ = iter_max_version_cnt;
      }
    }
    result.end_rowkey_ = ttl_row_iter.cur_rowkey_;
    result.end_qualifier_ = ttl_row_iter.cur_qualifier_;
    result.scan_rows_ = ttl_row_iter.scan_cnt_;
  }
  LOG_DEBUG("execute ttl delete", K(ret), K(result));
  return ret;
}

int ObTableService::generate_ttl_query(const ObTableTTLOperation &ttl_operation, ObTableServiceCtx &ctx, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query.add_select_column(ObHTableConstants::ROWKEY_CNAME_STR))) {
    LOG_WARN("failed to add K", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObHTableConstants::CQ_CNAME_STR))) {
    LOG_WARN("failed to add Q", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObHTableConstants::VERSION_CNAME_STR))) {
    LOG_WARN("failed to add T", K(ret));
  } else {
    ObObj *start_row_key = NULL;
    ObObj *end_row_key = NULL;
    if (OB_ISNULL(start_row_key = static_cast<ObObj*>(ctx.param_.allocator_->alloc(sizeof(ObObj) * 3)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (OB_ISNULL(end_row_key = static_cast<ObObj*>(ctx.param_.allocator_->alloc(sizeof(ObObj) * 3)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      ObNewRange range;
      // [k, q, min] -> [max, max, max]
      start_row_key[0].set_varbinary(ttl_operation.start_rowkey_);
      start_row_key[1].set_varbinary(ttl_operation.start_qualifier_);
      start_row_key[2].set_min_value();
      end_row_key[0].set_max_value();
      end_row_key[1].set_max_value();
      end_row_key[2].set_max_value();
      range.start_key_.assign(start_row_key, 3);
      range.end_key_.assign(end_row_key, 3);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
      if (OB_FAIL(query.set_scan_order(ObQueryFlag::ScanOrder::Forward))) { // scan from new to old
        LOG_WARN("fail to set scan order", K(ret));
      } else if (OB_FAIL(query.add_scan_range(range))) {
        LOG_WARN("fail to add scan range", K(ret), K(range));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObTableTTLDeleteRowIterator::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The table api ttl delete row iterator has not been inited, ", K(ret));
  } else {
    if (cur_del_rows_ >= limit_del_rows_) {
      ret = OB_ITER_END; 
      LOG_DEBUG("finish get next row", K(ret), K(cur_del_rows_), K(limit_del_rows_));
    } else {
      while(OB_SUCC(scan_result_->get_next_row(row))) {
        scan_cnt_++;
        ObHTableCellEntity cell(row);
        ObString cell_rowkey = cell.get_rowkey();
        ObString cell_qualifier = cell.get_qualifier();
        int64_t cell_ts = -cell.get_timestamp(); // obhtable timestamp is nagative in ms
        if ((cell_rowkey != cur_rowkey_) || (cell_qualifier != cur_qualifier_)) {
          cur_version_ = 1;
          cur_rowkey_ = cell_rowkey;
          cur_qualifier_ = cell_qualifier;
        } else {
          cur_version_++;
        }
        if (max_version_ > 0 && cur_version_ > max_version_) {
          max_version_cnt_++;
          cur_del_rows_++;
          is_last_row_ttl_ = false;
          break;
        } else if (time_to_live_ms_ > 0 && (cell_ts + time_to_live_ms_ < ObHTableUtils::current_time_millis())) {
          ttl_cnt_++;
          cur_del_rows_++;
          is_last_row_ttl_ = true;
          break;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }
  return ret;
}

void ObTableServiceTTLCtx::destroy_scan_iterator(storage::ObPartitionService *part_service)
{
  if (NULL != scan_result_) {
    if (NULL == part_service) {
      LOG_ERROR("part_service is NULL, memory leak");
    } else {
      part_service->revert_scan_iter(scan_result_);
      scan_result_ = NULL;
    }
  }
}

void ObTableTTLDeleteRowIterator::reset()
{
  is_inited_ = false;
  scan_result_ = NULL;
  time_to_live_ms_ = 0;
  max_version_ = 0;
  limit_del_rows_ = 0;
  cur_del_rows_ = 0;
  cur_version_ = 0;
  cur_rowkey_.reset(); 
  cur_qualifier_.reset();
  max_version_cnt_ = 0;
  ttl_cnt_ = 0;
  is_last_row_ttl_ = false;
}

int ObTableTTLDeleteRowIterator::init(const ObTableTTLOperation &ttl_operation)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ttl_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl operation", K(ret), K(ttl_operation));
  } else {
    time_to_live_ms_ = ttl_operation.time_to_live_ * 1000l;
    max_version_ = ttl_operation.max_version_;
    limit_del_rows_ = ttl_operation.del_row_limit_;
    cur_rowkey_ = ttl_operation.start_rowkey_;
    cur_qualifier_ = ttl_operation.start_qualifier_;
    is_inited_ = true;
  }
  return ret;
}

// check whether index is supported in given table schema by table api
// global index is not supported by table api. specially, global index in non-partitioned
// table was optimized to local index, which we can support.
int ObTableService::check_index_supported(schema::ObSchemaGetterGuard &schema_guard,
    const schema::ObSimpleTableSchemaV2 *table_schema, uint64_t index_id, bool &is_supported)
{
  int ret = OB_SUCCESS;
  is_supported = true;
  const schema::ObSimpleTableSchemaV2 *index_schema = NULL;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null table schema", K(ret));
  } else if (table_schema->is_partitioned_table()) {
    if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema))) {
      LOG_WARN("fail to get table schmea", K(ret), K(index_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("get null index schema", K(ret), K(index_id));
    } else if (index_schema->is_global_index_table()) {
      is_supported = false;
    }
  }
  return ret;
}