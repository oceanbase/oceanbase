/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_op_wrapper.h"
#include "ob_table_query_common.h"

using namespace oceanbase::sql::stmt;

namespace oceanbase
{
namespace table
{
int ObTableOpWrapper::process_op_with_spec(ObTableCtx &tb_ctx,
                                           ObTableApiSpec *spec,
                                           ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;
  if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableApiSpec is NULL", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create executor", K(ret));
  } else if (OB_FAIL(executor->open())) {
    LOG_WARN("fail to open", K(ret));
  } else if (OB_FAIL(executor->get_next_row())) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to execute", K(ret), K(tb_ctx));
    }
  }

  if (OB_SUCC(ret)) {
    ObTableApiModifyExecutor *modify_executor = static_cast<ObTableApiModifyExecutor *>(executor);
    op_result.set_affected_rows(modify_executor->get_affected_rows());
    if (tb_ctx.return_affected_entity() && op_result.get_affected_rows() > 0 &&
        OB_FAIL(process_affected_entity(tb_ctx, *spec, op_result))) {
      LOG_WARN("fail to process affected entity", K(ret), K(tb_ctx));
    } else if (!tb_ctx.is_htable() && ObTableOperationType::Type::INSERT_OR_UPDATE == tb_ctx.get_opertion_type()) {
      // not duplicated then do insert, duplicated do update
      if (tb_ctx.is_ttl_table()) {
        op_result.set_insertup_do_insert(!static_cast<ObTableApiTTLExecutor *>(executor)->is_insert_duplicated());
      } else {
        op_result.set_insertup_do_insert(!static_cast<ObTableApiInsertUpExecutor *>(executor)->is_insert_duplicated());
        op_result.set_insertup_do_put(static_cast<ObTableApiInsertUpExecutor *>(executor)->is_use_put());
        if (tb_ctx.is_redis_ttl_table() && op_result.get_is_insertup_do_update()) {
          const ObNewRow *old_row = nullptr;
          if (OB_FAIL(static_cast<ObTableApiInsertUpExecutor *>(executor)->get_old_row(old_row))) {
            LOG_WARN("fail to get old row", K(ret));
          } else {
            op_result.set_insertup_old_row(old_row);
          }
        }
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = executor->close())) {
    LOG_WARN("fail to close executor", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  op_result.set_err(ret);
  op_result.set_type(tb_ctx.get_opertion_type());
  spec->destroy_executor(executor);
  return ret;
}

int ObTableOpWrapper::process_affected_entity(ObTableCtx &tb_ctx,
                                              const ObTableApiSpec &spec,
                                              ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;
  ObITableEntity *result_entity = nullptr;
  if (TABLE_API_EXEC_UPDATE != spec.get_type() &&
      TABLE_API_EXEC_INSERT != spec.get_type() &&
      TABLE_API_EXEC_TTL != spec.get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid spec type", K(ret), K(spec.get_type()));
  } else if (OB_FAIL(op_result.get_entity(result_entity))) {
    LOG_WARN("fail to get result entity", K(ret), K(result_entity));
  } else {
    const ObIArray<ObExpr *> *new_row_exprs = nullptr;
    bool is_update = false;
    if (TABLE_API_EXEC_UPDATE == spec.get_type()) { // do update
      is_update = true;
      const ObTableApiUpdateSpec &update_spec = static_cast<const ObTableApiUpdateSpec&>(spec);
      if (update_spec.get_ctdefs().count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update ctdef size is less than 1", K(ret));
      } else if (OB_ISNULL(update_spec.get_ctdefs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update ctdef is NULL", K(ret));
      } else {
        new_row_exprs = &update_spec.get_ctdefs().at(0)->new_row_;
      }
    } else if (TABLE_API_EXEC_TTL == spec.get_type()) {
      const ObTableApiTTLSpec &ttl_spec = static_cast<const ObTableApiTTLSpec&>(spec);
      if (ttl_spec.get_ctdefs().count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ttl ctdef size is less than 1", K(ret));
      } else if (OB_ISNULL(ttl_spec.get_ctdefs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ttl ctdef is NULL", K(ret));
      } else {
        new_row_exprs = &ttl_spec.get_ctdefs().at(0)->ins_ctdef_.new_row_;
      }
    } else {
      const ObTableApiInsertSpec &ins_spec = static_cast<const ObTableApiInsertSpec&>(spec);
      if (ins_spec.get_ctdefs().count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert ctdef size is less than 1", K(ret));
      } else if (OB_ISNULL(ins_spec.get_ctdefs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert ctdef is NULL", K(ret));
      } else {
        new_row_exprs = &ins_spec.get_ctdefs().at(0)->new_row_;
      }
    }

    ObIArray<ObTableAssignment> &assigns = tb_ctx.get_assignments();
    ObIAllocator &allocator = tb_ctx.get_allocator();
    ObObj *obj_array = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * assigns.count()));
    ObEvalCtx eval_ctx(tb_ctx.get_exec_ctx());
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(obj_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("faild to alloc memory for objs", K(ret), K(assigns.count()));
    } else if (OB_ISNULL(new_row_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert exprs is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObTableAssignment &assign = assigns.at(i);
      uint64_t project_idx = OB_INVALID_ID;
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is nullptr", K(ret), K(assign));
      } else if (FALSE_IT(project_idx = assign.column_info_->col_idx_)) {
      } else if (new_row_exprs->count() <= project_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected insert index", K(ret), KP(new_row_exprs), K(assign));
      } else {
        ObObj &obj = obj_array[i];
        ObExpr *rt_expr = new_row_exprs->at(project_idx);
        ObDatum *datum = nullptr;
        ObObj pro_obj;
        if (OB_FAIL(rt_expr->eval(eval_ctx, datum))) {
          LOG_WARN("fail to eval datum", K(ret), K(*rt_expr));
        } else if (OB_FAIL(datum->to_obj(obj, rt_expr->obj_meta_))) {
          LOG_WARN("fail to datum to obj", K(ret), K(*datum), K(rt_expr->obj_meta_));
        } else if (is_lob_storage(obj.get_type())
            && OB_FAIL(ObTableCtx::read_real_lob(allocator, obj))) {
          LOG_WARN("fail to read lob", K(ret), K(obj));
        } else if (OB_FAIL(ob_write_obj(allocator, obj, pro_obj))) {
          LOG_WARN("fail to write obj", K(ret), K(obj));
        } else if (OB_FAIL(result_entity->set_property(assign.column_info_->column_name_, pro_obj))) {
          LOG_WARN("fail to set property", K(ret), K(assign), K(pro_obj));
        }
      }
    }

    if (OB_SUCC(ret) && tb_ctx.return_rowkey()) {
      if (OB_FAIL(result_entity->deep_copy_rowkey(tb_ctx.get_allocator(), *tb_ctx.get_entity()))) {
        LOG_WARN("fail to copy entity rowkey", K(ret));
      }
    }
  }

  return ret;
}

int ObTableOpWrapper::process_get(ObTableCtx &tb_ctx, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // if refer to cache must use ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  if (OB_FAIL(get_or_create_spec<TABLE_API_EXEC_SCAN>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create scan spec", K(ret));
  } else if (OB_FAIL(process_get_with_spec(tb_ctx, spec, row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to process get with spec", K(ret));
    }
  }
  return ret;
}

int ObTableOpWrapper::process_get_with_spec(ObTableCtx &tb_ctx,
                                            ObTableApiSpec *spec,
                                            ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;
  // fill key range
  const ObITableEntity *entity = tb_ctx.get_entity();
  ObTableApiScanRowIterator *row_iter = nullptr;
  if (OB_FAIL(ObTableQueryUtils::get_scan_row_interator(tb_ctx, row_iter))) {
    LOG_WARN("fail to get scan row iterator", K(ret));
  } else if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to entity is null", K(ret), K(tb_ctx));
  } else if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to spec is NULL", K(ret));
  } else {
    ObRowkey rowkey = entity->get_rowkey();
    ObNewRange range;
    tb_ctx.get_key_ranges().reset();
    if (OB_FAIL(range.build_range(tb_ctx.get_ref_table_id(), rowkey))) {
      LOG_WARN("fail to build key range", K(ret), K(rowkey));
    } else if (OB_FAIL(tb_ctx.get_key_ranges().push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to create scan executor", K(ret));
    } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor *>(executor)))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else if (OB_FAIL(row_iter->get_next_row(row, tb_ctx.get_allocator()))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next row", K(ret));
      }
    }
  }

  if (OB_NOT_NULL(row_iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter->close())) {
      LOG_WARN("fail to close row iterator", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  spec->destroy_executor(executor);
  ObTableApiUtil::replace_ret_code(ret);
  return ret;
}

int ObTableOpWrapper::get_insert_spec(ObTableCtx &tb_ctx,
                                      ObTableApiCacheGuard &cache_guard,
                                      ObTableApiSpec *&spec)
{
  int ret = OB_SUCCESS;

  if (!tb_ctx.is_ttl_table()) {
    if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_INSERT>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create insert spec", K(ret));
    }
  } else {
    if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_TTL>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create ttl spec", K(ret));
    }
  }

  return ret;
}

int ObTableOpWrapper::get_insert_up_spec(ObTableCtx &tb_ctx,
                                         ObTableApiCacheGuard &cache_guard,
                                         ObTableApiSpec *&spec)
{
  int ret = OB_SUCCESS;

  if (!tb_ctx.is_ttl_table()) {
    if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_INSERT_UP>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create insert up spec", K(ret));
    }
  } else {
    if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_TTL>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create ttl spec", K(ret));
    }
  }

  return ret;
}

int ObTableOpWrapper::process_insert_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;

  if (!tb_ctx.is_ttl_table()) {
    if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT>(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert operation", K(ret));
    }
  } else {
    if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_TTL>(tb_ctx, op_result))) {
      LOG_WARN("fail to process ttl insert operation", K(ret));
    }
  }

  return ret;
}

int ObTableOpWrapper::process_insert_up_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;

  if (!tb_ctx.is_ttl_table()) {
    if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT_UP>(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert up operation", K(ret));
    }
  } else {
    if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_TTL>(tb_ctx, op_result))) {
      LOG_WARN("fail to process ttl insert up operation", K(ret));
    }
  }

  return ret;
}

int ObTableOpWrapper::process_incr_or_append_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;
  // 1.do update first;
  tb_ctx.set_inc_append_stage(ObTableIncAppendStage::TABLE_INCR_APPEND_UPDATE);
  if (OB_FAIL(process_op<TABLE_API_EXEC_UPDATE>(tb_ctx, op_result))) {
    LOG_WARN("fail to process update operation", K(ret));
  } else if (op_result.get_affected_rows() == 0) {
    // 2.if return empty result, do insert;
    op_result.reset();
    if (OB_FAIL(tb_ctx.init_insert_when_inc_append())) {
      LOG_WARN("fail to int insert when do increment/append", K(ret), K(tb_ctx.get_opertion_type()));
    } else if (OB_FAIL(process_insert_op(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert operation", K(ret));
    }
  }
  return ret;
}

int ObTableApiUtil::construct_entity_from_row(ObIAllocator &allocator,
                                              ObNewRow *row,
                                              ObKvSchemaCacheGuard &schema_cache_guard,
                                              const ObIArray<ObString> &cnames,
                                              ObITableEntity *entity)
{
  int ret = OB_SUCCESS;
  int64_t N = cnames.count();
  const ObColumnSchemaV2 *column_schema = NULL;
  ObSEArray<ObString, 32> all_columns;
  const ObIArray<ObString>* arr_col = &cnames;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_NULL_INPUT;
    LOG_WARN("invalid null ObNewRow ptr", K(ret), K(row));
  } else if (N == 0) {
    const ObIArray<ObTableColumnInfo *>&column_info_array = schema_cache_guard.get_column_info_array();
    if (OB_FAIL(expand_all_columns(column_info_array, all_columns))) {
      LOG_WARN("fail to expand all column to cnames", K(ret));
    } else {
      N = all_columns.count();
      arr_col = &all_columns;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObString &name = arr_col->at(i);
    int64_t column_idx = -1;
    if (OB_FAIL(schema_cache_guard.get_column_info_idx(name, column_idx))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      const ObString &table = schema_cache_guard.get_table_name_str();
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, name.length(), name.ptr(), table.length(), table.ptr());
      LOG_WARN("column not exist", K(ret), K(name));
    } else {
      ObObj &cell = row->get_cell(column_idx);
      if (is_lob_storage(cell.get_type()) && OB_FAIL(ObTableCtx::read_real_lob(allocator, cell))) {
        LOG_WARN("fail to read lob", K(ret));
      } else if (OB_FAIL(entity->set_property(name, cell))) {
        LOG_WARN("fail to set property", K(ret), K(name), K(cell));
      }
    }
  } // end for
  return ret;
}

int ObTableApiUtil::expand_all_columns(const ObIArray<ObTableColumnInfo *>& col_info_array,
                                       ObIArray<ObString> &cnames)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
    ObTableColumnInfo *col_info = col_info_array.at(i);
    if (OB_ISNULL(col_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(ret), K(i));
    } else if (col_info->is_fulltext_column()) {
      // skip and do nothing
    } else if (OB_FAIL(cnames.push_back(col_info->column_name_))) {
      LOG_WARN("fail to push back column name", K(ret));
    }
  }
  return ret;
}

int ObHTableDeleteExecutor::open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is NULL", K(ret));
  } else if (OB_FAIL(executor_->open())) {
    LOG_WARN("fail to open delete executor", K(ret));
  }
  return ret;
}

int ObHTableDeleteExecutor::get_next_row()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableOperation> *ops = tb_ctx_.get_batch_operation();
  ObTableQuery query;
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch operation is null", K(ret));
  } else {
    ObHTableFilter &filter = query.htable_filter();
    for (int64_t i = 0; OB_SUCC(ret) && i < ops->count(); i++) {
      const ObTableOperation &op = ops->at(i);
      if (OB_FAIL(build_range(op.entity(), query))) {
        LOG_WARN("fail to build range", K(ret), K(op.entity()));
      } else if (OB_FAIL(generate_filter(op.entity(), filter))) {
        LOG_WARN("fail to generate htable filter", K(ret), K(op.entity()));
      } else if (OB_FAIL(query_and_delete(query))) {
        LOG_WARN("fail to query and delete", K(ret), K(query));
      }
    }
  }
  return ret;
}

int ObHTableDeleteExecutor::get_next_row_by_entity()
{
  int ret = OB_SUCCESS;
  const ObITableEntity *entity = tb_ctx_.get_entity();
  ObTableQuery query;
  if (OB_ISNULL(entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entity of operation is null", K(ret));
  } else {
    ObHTableFilter &filter = query.htable_filter();
    if (OB_FAIL(build_range(*entity, query))) {
      LOG_WARN("fail to build range", K(ret), K(entity));
    } else if (OB_FAIL(generate_filter(*entity, filter))) {
      LOG_WARN("fail to generate htable filter", K(ret), K(entity));
    } else if (OB_FAIL(query_and_delete(query))) {
      LOG_WARN("fail to query and delete", K(ret), K(query));
    }

  }
  return ret;
}

int ObHTableDeleteExecutor::close()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is NULL", K(ret));
  } else if (OB_FAIL(executor_->close())) {
    LOG_WARN("fail to close delete executor", K(ret));
  }
  return ret;
}

int ObHTableDeleteExecutor::build_range(const ObITableEntity &entity, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  common::ObIArray<common::ObNewRange> &key_ranges = tb_ctx_.get_key_ranges();
  key_ranges.reset();
  // generate scan range
  if (OB_FAIL(query.add_select_column(ObHTableConstants::ROWKEY_CNAME_STR))) {
    LOG_WARN("fail to add K", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObHTableConstants::CQ_CNAME_STR))) {
    LOG_WARN("fail to add Q", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObHTableConstants::VERSION_CNAME_STR))) {
    LOG_WARN("fail to add T", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObHTableConstants::VALUE_CNAME_STR))) {
    LOG_WARN("fail to add V", K(ret));
  } else {
    ObHTableCellEntity3 htable_cell(&entity);
    ObString row =  htable_cell.get_rowkey();
    if (htable_cell.last_get_is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("K is null", K(ret), K(entity));
    } else {
      ObString qualifier = htable_cell.get_qualifier();
      ObNewRange range;
      pk_objs_start_[0].set_varbinary(row);
      pk_objs_start_[2].set_min_value();
      pk_objs_end_[0].set_varbinary(row);
      pk_objs_end_[2].set_max_value();
      // delete column family when qualifier is null
      if (htable_cell.last_get_is_null()) {
        pk_objs_start_[1].set_min_value();
        pk_objs_end_[1].set_max_value();
      } else {
        pk_objs_start_[1].set_varbinary(qualifier);
        pk_objs_end_[1].set_varbinary(qualifier);
      }
      range.start_key_.assign(pk_objs_start_, 3);
      range.end_key_.assign(pk_objs_end_, 3);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();

      if (OB_FAIL(key_ranges.push_back(range))) {
        LOG_WARN("fail to push back hdelete scan range", K(ret), K(key_ranges));
      }
    }
  }

  return ret;
}

int ObHTableDeleteExecutor::query_and_delete(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObTableQueryResultIterator *result_iter = nullptr;
  ObTableQueryResult tmp_result;
  ObTableQueryResult *one_result = nullptr;
  ObTableApiExecutor *child = nullptr;
  OB_TABLE_START_AUDIT(*tb_ctx_.get_credential(),
                       *tb_ctx_.get_sess_guard(),
                       tb_ctx_.get_table_name(),
                       &audit_ctx_, query);

  ObTableApiScanRowIterator *row_iter = nullptr;
  if (OB_FAIL(ObTableQueryUtils::get_scan_row_interator(tb_ctx_, row_iter))) {
    LOG_WARN("fail to get scan row iterator", K(ret));
  } else if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(tmp_allocator,
                                                                query,
                                                                true, /* is_hkv */
                                                                tmp_result,
                                                                tb_ctx_,
                                                                result_iter))) {
    LOG_WARN("fail to generate query result iterator", K(ret));
  } else if (OB_ISNULL(child = const_cast<ObTableApiExecutor *>(executor_->get_child()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor *>(child)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else {
    result_iter->set_scan_result(row_iter);
    if (OB_FAIL(result_iter->get_next_result(one_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next result", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(one_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("one_result is NULL", K(ret));
    } else if (FALSE_IT(one_result->rewind())) {
      // do nothing
    }
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx_.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, tmp_result.get_row_count(),
                     has_table_scan, true,
                     filter, (OB_ISNULL(result_iter) ? nullptr : result_iter->get_filter()));

  if (OB_SUCC(ret) && OB_NOT_NULL(one_result) && OB_FAIL(delete_rows(*one_result))) {
    LOG_WARN("fail to delete rows", K(ret));
  }

  if (OB_NOT_NULL(row_iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter->close())) {
      LOG_WARN("fail to close row iterator", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  ObTableQueryUtils::destroy_result_iterator(result_iter);

  return ret;
}

int ObHTableDeleteExecutor::delete_rows(ObTableQueryResult &result)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *entity = nullptr;
  executor_->set_skip_scan(true);
  while (OB_SUCC(ret) && OB_SUCC(result.get_next_entity(entity))) {
    ObTableOperation op;
    op.set_type(ObTableOperationType::Type::DEL);
    op.set_entity(entity);
    OB_TABLE_START_AUDIT(*tb_ctx_.get_credential(),
                         *tb_ctx_.get_sess_guard(),
                         tb_ctx_.get_table_name(),
                         &audit_ctx_,
                         op);

    executor_->set_entity(entity);
    if (OB_FAIL(executor_->get_next_row())) {
      LOG_WARN("fail to call delete executor get next row", K(ret), K(entity));
    } else {
      affected_rows_++;
    }
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, tb_ctx_.get_exec_ctx().get_das_ctx().get_snapshot(),
                       stmt_type, StmtType::T_KV_DELETE);
  }
  if (OB_FAIL(ret)) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next entity to delete", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObHTableDeleteExecutor::generate_filter(const ObITableEntity &entity,
                                            ObHTableFilter &filter)
{
  int ret = OB_SUCCESS;
  ObHTableCellEntity3 htable_cell(&entity);
  ObString row =  htable_cell.get_rowkey();
  ObString qualifier;
  filter.set_valid(true);
  filter.clear_columns();
  if (htable_cell.last_get_is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("K is null", K(ret), K(entity));
  } else if (FALSE_IT(qualifier = htable_cell.get_qualifier())) {
    // do nothing
  } else if (htable_cell.last_get_is_null()) {
    // delete column family, so we need to scan all qualifier
    // wildcard scan
  } else if (OB_FAIL(filter.add_column(qualifier))) {
    LOG_WARN("failed to add column", K(ret));
  }
  int64_t timestamp = -htable_cell.get_timestamp();        // negative to get the original value
  if (-ObHTableConstants::LATEST_TIMESTAMP == timestamp) {  // INT64_MAX
    // delete the most recently added cell
    filter.set_max_versions(1);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
  } else if (timestamp > 0) {
    // delete the specific version
    filter.set_max_versions(1);
    filter.set_timestamp(timestamp);
  } else if (ObHTableConstants::LATEST_TIMESTAMP == timestamp) { // -INT64_MAX
    // delete all version
    filter.set_max_versions(INT32_MAX);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, ObHTableConstants::INITIAL_MAX_STAMP);
  } else {
    // delete all versions less than or equal to the timestamp
    filter.set_max_versions(INT32_MAX);
    filter.set_time_range(ObHTableConstants::INITIAL_MIN_STAMP, (-timestamp) + 1);
  }

  return ret;
}
} // end namespace table
} // end namespace oceanbase
