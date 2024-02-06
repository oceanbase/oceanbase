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
#include "ob_table_modify_executor.h"
#include "share/table/ob_table.h"
#include "ob_table_scan_executor.h"
#include "ob_htable_utils.h"
#include "ob_htable_filter_operator.h"
#include "ob_table_insert_up_executor.h"
#include "ttl/ob_table_ttl_executor.h"
#include "ob_table_query_common.h"

namespace oceanbase
{
namespace table
{
// 根据执行计划驱动executor执行
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
    if (tb_ctx.return_affected_entity()
        && OB_FAIL(process_affected_entity(tb_ctx, *spec, *executor, op_result))) {
      LOG_WARN("fail to process affected entity", K(ret), K(tb_ctx));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = executor->close())) {
    LOG_WARN("fail to close executor", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  op_result.set_errno(ret);
  op_result.set_type(tb_ctx.get_opertion_type());
  spec->destroy_executor(executor);
  return ret;
}

int ObTableOpWrapper::process_affected_entity(ObTableCtx &tb_ctx,
                                              const ObTableApiSpec &spec,
                                              ObTableApiExecutor &executor,
                                              ObTableOperationResult &op_result)
{
  int ret = OB_SUCCESS;
  ObITableEntity *result_entity = nullptr;
  if (TABLE_API_EXEC_INSERT_UP != spec.get_type() && TABLE_API_EXEC_TTL != spec.get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid spec type", K(ret), K(spec.get_type()));
  } else if (OB_FAIL(op_result.get_entity(result_entity))) {
    LOG_WARN("fail to get result entity", K(ret), K(result_entity));
  } else {
    const ObIArray<ObExpr *> *ins_exprs = nullptr;
    const ObIArray<ObExpr *> *upd_exprs = nullptr;
    bool use_insert_expr = false;
    if (TABLE_API_EXEC_INSERT_UP == spec.get_type()) {
      const ObTableApiInsertUpSpec &ins_up_spec = static_cast<const ObTableApiInsertUpSpec&>(spec);
      ins_exprs = &ins_up_spec.get_ctdef().ins_ctdef_.new_row_;
      upd_exprs = &ins_up_spec.get_ctdef().upd_ctdef_.new_row_;
      use_insert_expr = !static_cast<ObTableApiInsertUpExecutor&>(executor).is_insert_duplicated();
    } else {
      ObTableApiTTLExecutor &ttl_executor = static_cast<ObTableApiTTLExecutor&>(executor);
      const ObTableApiTTLSpec &ttl_spec = static_cast<const ObTableApiTTLSpec&>(spec);
      ins_exprs = &ttl_spec.get_ctdef().ins_ctdef_.new_row_;
      upd_exprs = &ttl_spec.get_ctdef().upd_ctdef_.new_row_;
      use_insert_expr = !ttl_executor.is_insert_duplicated() || ttl_executor.is_expired();
    }

    ObIArray<ObTableAssignment> &assigns = tb_ctx.get_assignments();
    ObIAllocator &allocator = tb_ctx.get_allocator();
    ObObj *obj_array = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * assigns.count()));
    if (OB_ISNULL(obj_array)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("faild to alloc memory for objs", K(ret), K(assigns.count()));
    } else if (OB_ISNULL(ins_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert exprs is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObTableAssignment &assign = assigns.at(i);
      uint64_t project_idx = OB_INVALID_ID;
      if (OB_ISNULL(assign.column_item_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is nullptr", K(ret), K(assign));
      } else if (FALSE_IT(project_idx = assign.column_item_->col_idx_)) {
      } else if (use_insert_expr && ins_exprs->count() <= project_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected insert index", K(ret), K(ins_exprs), K(assign));
      } else if (!use_insert_expr && upd_exprs->count() <= project_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected assign index", K(ret), K(upd_exprs), K(assign));
      } else {
        ObObj &obj = obj_array[i];
        ObExpr *rt_expr = use_insert_expr ? ins_exprs->at(project_idx) : upd_exprs->at(project_idx);
        ObDatum *datum = nullptr;
        if (OB_FAIL(rt_expr->eval(executor.get_eval_ctx(), datum))) {
          LOG_WARN("fail to eval datum", K(ret), K(*rt_expr));
        } else if (OB_FAIL(datum->to_obj(obj, rt_expr->obj_meta_))) {
          LOG_WARN("fail to datum to obj", K(ret), K(*datum), K(rt_expr->obj_meta_));
        } else if (is_lob_storage(obj.get_type())
            && OB_FAIL(ObTableCtx::read_real_lob(allocator, obj))) {
          LOG_WARN("fail to read lob", K(ret), K(obj));
        } else if (OB_FAIL(result_entity->set_property(assign.column_item_->column_name_, obj))) {
          LOG_WARN("fail to set property", K(ret), K(assign), K(obj));
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

// get操作单独处理
int ObTableOpWrapper::process_get(ObTableCtx &tb_ctx, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  if (OB_FAIL(get_or_create_spec<TABLE_API_EXEC_SCAN>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create scan spec", K(ret));
  } else if (OB_FAIL(process_get_with_spec(tb_ctx, spec, row))) {
    LOG_WARN("fail to process get with spec", K(ret));
  }
  return ret;
}

int ObTableOpWrapper::process_get_with_spec(ObTableCtx &tb_ctx,
                                            ObTableApiSpec *spec,
                                            ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;
  ObTableApiScanRowIterator row_iter;
  // fill key range
  const ObITableEntity *entity = tb_ctx.get_entity();
  ObRowkey rowkey = entity->get_rowkey();
  ObNewRange range;
  tb_ctx.get_key_ranges().reset();
  if (OB_FAIL(range.build_range(tb_ctx.get_ref_table_id(), rowkey))) {
    LOG_WARN("fail to build key range", K(ret), K(rowkey));
  } else if (OB_FAIL(tb_ctx.get_key_ranges().push_back(range))) {
    LOG_WARN("fail to push back key range", K(ret), K(range));
  } else if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to spec is NULL", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create scan executor", K(ret));
  } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else if (OB_FAIL(row_iter.get_next_row(row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", K(ret));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
    LOG_WARN("fail to close row iterator", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  spec->destroy_executor(executor);
  ObTableApiUtil::replace_ret_code(ret);
  return ret;
}

int ObTableApiUtil::construct_entity_from_row(ObIAllocator &allocator,
                                              ObNewRow *row,
                                              const ObTableSchema *table_schema,
                                              const ObIArray<ObString> &cnames,
                                              ObITableEntity *entity)
{
  int ret = OB_SUCCESS;
  const int64_t N = cnames.count();
  const ObColumnSchemaV2 *column_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObString &name = cnames.at(i);
    if (OB_ISNULL(column_schema = table_schema->get_column_schema(name))) {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not exist", K(ret), K(name));
    } else {
      int64_t column_idx = table_schema->get_column_idx(column_schema->get_column_id());
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
  const ObTableBatchOperation *batch_op = tb_ctx_.get_batch_operation();
  ObTableQuery query;
  if (OB_ISNULL(batch_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch operation is null", K(ret));
  } else if (OB_FAIL(build_range(query))) {
    LOG_WARN("fail to build query range", K(ret));
  } else {
    ObHTableFilter &filter = query.htable_filter();
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_op->count(); i++) {
      const ObTableOperation &op = batch_op->at(i);
      if (OB_FAIL(generate_filter(op.entity(), filter))) {
        LOG_WARN("fail to generate htable filter", K(ret), K(op.entity()));
      } else if (OB_FAIL(query_and_delete(query))) {
        LOG_WARN("fail to query and delete", K(ret), K(query));
      }
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

int ObHTableDeleteExecutor::build_range(ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  common::ObIArray<common::ObNewRange> &key_ranges = tb_ctx_.get_key_ranges();
  const ObTableBatchOperation *batch_op = tb_ctx_.get_batch_operation();
  const ObTableOperation &del_op = batch_op->at(0);
  const ObITableEntity &entity = del_op.entity();
  ObHTableCellEntity3 htable_cell(&entity);
  ObString row = htable_cell.get_rowkey();
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
    query.set_batch(1);  // mutate for each row
    query.set_max_result_size(-1);
  }
  ObNewRange range;
  pk_objs_start_[0].set_varbinary(row);
  pk_objs_start_[1].set_min_value();
  pk_objs_start_[2].set_min_value();
  pk_objs_end_[0].set_varbinary(row);
  pk_objs_end_[1].set_max_value();
  pk_objs_end_[2].set_max_value();
  range.start_key_.assign(pk_objs_start_, 3);
  range.end_key_.assign(pk_objs_end_, 3);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  if (OB_FAIL(key_ranges.push_back(range))) {
    LOG_WARN("fail to push back hdelete scan range", K(ret), K(key_ranges));
  }

  return ret;
}

int ObHTableDeleteExecutor::query_and_delete(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObTableApiScanRowIterator row_iter;
  ObTableQueryResultIterator *result_iter = nullptr;
  ObTableQueryResult tmp_result;
  ObTableQueryResult *one_result = nullptr;
  ObTableApiExecutor *child = nullptr;

  if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(tmp_allocator,
                                                                query,
                                                                true, /* is_hkv */
                                                                tmp_result,
                                                                tb_ctx_,
                                                                result_iter))) {
    LOG_WARN("fail to generate query result iterator", K(ret));
  } else if (OB_ISNULL(child = const_cast<ObTableApiExecutor *>(executor_->get_child()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor *>(child)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else {
    result_iter->set_scan_result(&row_iter);
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
    } else if (OB_FAIL(delete_rows(*one_result))) {
      LOG_WARN("fail to delete rows", K(ret));
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
    LOG_WARN("fail to close row iterator", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  ObTableQueryUtils::destroy_result_iterator(result_iter);

  return ret;
}

int ObHTableDeleteExecutor::delete_rows(ObTableQueryResult &result)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *entity = nullptr;
  executor_->set_skip_scan(true);
  while (OB_SUCC(result.get_next_entity(entity))) {
    executor_->set_entity(entity);
    if (OB_FAIL(executor_->get_next_row())) {
      LOG_WARN("fail to call delete executor get next row", K(ret), K(entity));
    } else {
      affected_rows_++;
    }
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