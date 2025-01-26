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

#define USING_LOG_PREFIX SERVER
#include "ob_table_batch_service.h"
#include "ob_table_op_wrapper.h"
#include "observer/ob_req_time_service.h"
#include "ob_htable_utils.h"
#include "observer/table/ob_table_query_common.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::stmt;

int ObTableBatchService::check_legality(ObTableBatchCtx &ctx,
                                        const ObIArray<ObTableOperation> &ops,
                                        ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ctx.check_legality())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch ctx is invalid", K(ret));
  } else if (ops.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("requese is empty", K(ret));
  } else if (results.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is empty", K(ret), K(results));
  } else if (ops.count() != results.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the quantities do not match", K(ret), K(ops.count()), K(results.count()));
  }

  return ret;
}

int ObTableBatchService::prepare_results(const ObIArray<ObTableOperation> &ops,
                                         ObITableEntityFactory &entity_factory,
                                         ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  const int64_t count = ops.count();

  if (!results.empty()) {
    // do nothing
  } else if (ops.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ops is empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      const ObTableOperation &op = ops.at(i);
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = entity_factory.alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else {
        op_result.set_err(OB_SUCCESS);
        op_result.set_type(op.type());
        op_result.set_entity(result_entity);
        if (OB_FAIL(results.push_back(op_result))) {
          LOG_WARN("fail to push back result", K(ret), K(results));
        }
      }
    }
  }

  return ret;
}

int ObTableBatchService::aggregate_one_result(ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;

  if (results.empty()) {
    // do nothing
  } else {
    int64_t affected_rows = 0;
    ObTableOperationResult final_result = results.at(0); // shaddow copy

    for (int64_t i = 0; i < results.count(); ++i) {
      affected_rows += results.at(i).get_affected_rows();
    }

    final_result.set_affected_rows(affected_rows);
    results.reuse();
    if (OB_FAIL(results.push_back(final_result))) {
      LOG_WARN("fail to push back result", K(ret), K(results));
    }
  }

  return ret;
}

int ObTableBatchService::execute(ObTableBatchCtx &ctx,
                                 const ObIArray<ObTableOperation> &ops,
                                 ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_legality(ctx, ops, results))) {
    LOG_WARN("fail to check legality", K(ret), K(ret), K(results));
  } else if (ctx.is_readonly_) {
    if (ctx.is_same_properties_names_) {
      ret = multi_get(ctx, ops, results);
    } else {
      ret = batch_execute(ctx, ops, results);
    }
  } else if (ctx.is_same_type_) {
    ObTableOperationType::Type op_type = ops.at(0).type();
    switch(op_type) {
      case ObTableOperationType::INSERT:
        ret = multi_insert(ctx, ops, results);
        break;
      case ObTableOperationType::DEL:
        if (ObTableEntityType::ET_HKV == ctx.tb_ctx_.get_entity_type()) {
          ret = htable_delete(ctx, ops, results);
        } else {
          ret = multi_delete(ctx, ops, results);
        }
        break;
      case ObTableOperationType::UPDATE:
        ret = batch_execute(ctx, ops, results);
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        if (ObTableEntityType::ET_HKV == ctx.tb_ctx_.get_entity_type()) {
          ret = htable_put(ctx, ops, results);
        } else {
          ret = batch_execute(ctx, ops, results);
        }
        break;
      case ObTableOperationType::REPLACE:
        ret = multi_replace(ctx, ops, results);
        break;
      case ObTableOperationType::PUT:
          ret = multi_put(ctx, ops, results);
          break;
      case ObTableOperationType::APPEND:
        ret = batch_execute(ctx, ops, results);
        break;
      case ObTableOperationType::INCREMENT:
        ret = batch_execute(ctx, ops, results);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
    }
  } else {
    if (ObTableEntityType::ET_HKV == ctx.tb_ctx_.get_entity_type()) {
      // HTable mutate_row(RowMutations)
      ret = htable_mutate_row(ctx, ops, results);
    } else {
      // complex batch hybrid operation
      ret = batch_execute(ctx, ops, results);
    }
  }

  return ret;
}

int ObTableBatchService::multi_op_in_executor(ObTableBatchCtx &ctx,
                                              ObTableApiSpec &spec,
                                              const ObIArray<ObTableOperation> &ops,
                                              ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableApiExecutor *executor = nullptr;
  tb_ctx.set_batch_operation(&ops);
  tb_ctx.set_batch_tablet_ids(&ctx.tablet_ids_);
  tb_ctx.set_need_dist_das(ctx.tb_ctx_.need_dist_das());
  if (OB_FAIL(adjust_entities(ctx, ops))) {
    LOG_WARN("fail to adjust entities", K(ret), K(ctx));
  } else if (OB_FAIL(spec.create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create scan executor", K(ret));
  } else if (OB_FAIL(executor->open())) {
    LOG_WARN("fail to open executor", K(ret));
  } else if (OB_FAIL(executor->get_next_row())) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get next row", K(ret), K(tb_ctx));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t affected_rows = (static_cast<ObTableApiModifyExecutor *>(executor))->get_affected_rows();
    for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); ++i) {
      ObTableOperationResult &op_result = results.at(i);
      op_result.set_affected_rows(affected_rows);
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(executor) && OB_SUCCESS != (tmp_ret = executor->close())) {
    LOG_WARN("fail to close executor", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  spec.destroy_executor(executor);
  return ret;
}

int ObTableBatchService::adjust_entities(ObTableBatchCtx &ctx, const ObIArray<ObTableOperation> &ops)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;

  // first entity has beed adjusted when create tb_ctx
  // but in hbase, the tb_ctx maybe reused and first entity
  // also need to be adjusted
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    if (i == 0 && tb_ctx.get_entity_type() != ObTableEntityType::ET_HKV) {
      // do noting
    } else {
      const ObTableOperation &op = ops.at(i);
      tb_ctx.set_entity(&op.entity());
      if (OB_FAIL(tb_ctx.adjust_entity())) {
        LOG_WARN("fail to adjust entity", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::get_result_index(
    const ObNewRow &row,
    const ObIArray<ObTableOperation> &ops,
    const ObIArray<uint64_t> &rowkey_ids,
    ObObj *rowkey_cells,
    ObIArray<int64_t> &indexs)
{
  int ret = OB_SUCCESS;

  for (int64_t pos = 0; pos < rowkey_ids.count(); ++pos) {
    rowkey_cells[pos] = row.get_cell(rowkey_ids.at(pos));
  }
  ObRowkey row_rowkey(rowkey_cells, rowkey_ids.count());

  for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
    const ObITableEntity &entity = ops.at(i).entity();
    ObRowkey entity_rowkey = entity.get_rowkey();
    bool equal = false;
    if (OB_FAIL(row_rowkey.equal(entity_rowkey, equal))) {
      LOG_WARN("fail to compare rowkey", K(row_rowkey), K(entity_rowkey));
    } else if (equal && OB_FAIL(indexs.push_back(i))) {
      LOG_WARN("fail to push_back index", K(row_rowkey), K(indexs), K(i));
    }
  }

  return ret;
}

int ObTableBatchService::multi_get_fuse_key_range(ObTableBatchCtx &ctx,
                                                  ObTableApiSpec &spec,
                                                  const ObIArray<ObTableOperation> &ops,
                                                  ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableApiExecutor *executor = nullptr;
  const int64_t op_size = ops.count();
  int64_t row_cnt = 0;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::GET, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  ObTableApiScanRowIterator *row_iter = nullptr;
  if (OB_FAIL(ObTableQueryUtils::get_scan_row_interator(tb_ctx, row_iter))) {
    LOG_WARN("fail to get scan row iterator", K(ret));
  } else if (OB_FAIL(spec.create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create scan executor", K(ret));
  } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor *>(executor)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else {
    ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx.get_schema_cache_guard();
    if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
    } else {
      // generate scan range with all rowkeys
      for (int64_t i = 0; OB_SUCC(ret) && i < op_size; ++i) {
        ObRowkey rowkey = ops.at(i).entity().get_rowkey();
        ObNewRange range;
        if (OB_FAIL(range.build_range(tb_ctx.get_ref_table_id(), rowkey))) {
          LOG_WARN("fail to build key range", K(ret), K(rowkey));
        } else if (OB_FAIL(tb_ctx.get_key_ranges().push_back(range))) {
          LOG_WARN("fail to push back key range", K(ret), K(range));
        }
      }

      ObIAllocator &allocator = tb_ctx.get_allocator();
      ObObj *rowkey_cells = nullptr;
      ObSEArray<uint64_t, 4> rowkey_column_ids;
      ObSEArray<uint64_t, 4> rowkey_idxs;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(schema_cache_guard->get_rowkey_column_ids(rowkey_column_ids))) {
        LOG_WARN("fail to get rowkey column ids", K(ret));
      } else if (OB_ISNULL(
                     rowkey_cells =
                         static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * rowkey_column_ids.count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc cells buffer", K(ret), K(rowkey_column_ids.count()));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); ++i) {
        int64_t idx = 0;
        if (OB_FAIL(schema_cache_guard->get_column_info_idx(rowkey_column_ids[i], idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column info idx", K(ret), K(rowkey_column_ids[i]));
        } else if (OB_FAIL(rowkey_idxs.push_back(idx))) {
          LOG_WARN("fail to push back rowkey idx", K(ret), K(rowkey_column_ids[i]));
        }
      }
      // There may be duplicate primary keys in the range, duplicate primary keys can only scan 1 record,
      // we will return an empty result for duplicate primary keys op.
      // For example, if there are 50 operations out of 100 with the same primary key,
      // only 50 records can be scanned, but we return 100 results
      for (int64_t i = 0; OB_SUCC(ret) && i < op_size; ++i) {
        ObNewRow *row = nullptr;
        if (OB_FAIL(row_iter->get_next_row(row, allocator))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next row", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          row_cnt++;
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(row)) {
          ObSEArray<int64_t, 2> indexs;
          if (OB_FAIL(get_result_index(*row, ops, rowkey_idxs, rowkey_cells, indexs))) {
            LOG_WARN("fail to get reuslt indexs", K(ret), KPC(row), K(ops));
          } else {
            const ObTableEntity *requset_entity = nullptr;
            ObITableEntity *result_entity = nullptr;
            for (int64_t idx = 0; OB_SUCC(ret) && idx < indexs.count(); ++idx) {
              int64_t index = indexs[idx];
              if (index >= results.count() || index >= op_size) {
                ret = OB_INDEX_OUT_OF_RANGE;
                LOG_WARN("result index is out of range",
                    K(ret), K(index), K(results), K(op_size), KPC(row), K(ops));
              } else if (OB_FAIL(results.at(index).get_entity(result_entity))) {
                LOG_WARN("fail to get result entity", K(ret));
              } else if (OB_ISNULL(requset_entity = &static_cast<const ObTableEntity &>(ops.at(index).entity()))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("entity is null", K(ret), K(index));
              } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator, row,
                  *schema_cache_guard, requset_entity->get_properties_names(), result_entity))) {
                LOG_WARN("fail to construct result entity from row", K(ret), KPC(row), KPC(requset_entity));
              }
            }  // end for
          }
        }
      }  // end for
    }
  }

  if (OB_NOT_NULL(row_iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter->close())) {
      LOG_WARN("fail to close row iterator", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  spec.destroy_executor(executor);

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_GET,
                     return_rows, row_cnt,
                     has_table_scan, true);
  return ret;
}

int ObTableBatchService::multi_execute_internal(ObTableBatchCtx &ctx,
                                                ObTableApiSpec &spec,
                                                const ObIArray<ObTableOperation> &ops,
                                                ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  if (ctx.tablet_ids_.count() == 1) {
    tb_ctx.set_tablet_id(ctx.tablet_ids_.at(0));
  }
  bool use_multi_tablets = ctx.tablet_ids_.count() > 1;
  if (use_multi_tablets && OB_UNLIKELY(ctx.tablet_ids_.count() != ops.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablets id count is not equal to ops count", K(ctx.tablet_ids_.count()), K(ops.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    const ObTableOperation &op = ops.at(i);
    ObTableOperationResult &op_result = results.at(i);
    tb_ctx.set_entity(&op.entity());
    if (i > 0 && OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
      LOG_WARN("fail to adjust entity", K(ret));
    } else if (use_multi_tablets && FALSE_IT(tb_ctx.set_tablet_id(ctx.tablet_ids_.at(i)))) {
    } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, &spec, op_result))) {
      LOG_WARN("fail to process insert with spec", K(ret), K(i));
      ObTableApiUtil::replace_ret_code(ret);
    }

    if (OB_FAIL(ret)) {
    } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
      LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
    }
  }

  return ret;
}

int ObTableBatchService::multi_get(ObTableBatchCtx &ctx,
                                   const ObIArray<ObTableOperation> &ops,
                                   ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  tb_ctx.set_read_latest(false);
  tb_ctx.set_batch_operation(&ops);
  tb_ctx.set_batch_tablet_ids(&ctx.tablet_ids_);
  tb_ctx.set_need_dist_das(ctx.tb_ctx_.need_dist_das());

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(adjust_entities(ctx, ops))) {
    LOG_WARN("fail to adjust entities", K(ret), K(ctx), K(ops));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_SCAN>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(multi_get_fuse_key_range(ctx, *spec, ops, results))) {
    LOG_WARN("fail to multi get fuse key range", K(ret), K(ctx), K(ops));
  }

  return ret;
}

int ObTableBatchService::multi_insert(ObTableBatchCtx &ctx,
                                      const ObIArray<ObTableOperation> &ops,
                                      ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::INSERT, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_insert_spec(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create insert spec", K(ret));
  } else if (ctx.is_atomic_ && !tb_ctx.is_ttl_table()) {
    if (OB_FAIL(multi_op_in_executor(ctx, *spec, ops, results))) {
      LOG_WARN("fail to do multi operarion in executor", K(ret), K(ops));
    }
  } else if (OB_FAIL(multi_execute_internal(ctx, *spec, ops, results))) {
    LOG_WARN("fail to multi exeucte internal", K(ret));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_INSERT);

  return ret;
}

int ObTableBatchService::multi_delete(ObTableBatchCtx &ctx,
                                      const ObIArray<ObTableOperation> &ops,
                                      ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::DEL, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(multi_execute_internal(ctx, *spec, ops, results))) {
    LOG_WARN("fail to multi exeucte internal", K(ret));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_DELETE);

  return ret;
}

int ObTableBatchService::multi_replace(ObTableBatchCtx &ctx,
                                       const ObIArray<ObTableOperation> &ops,
                                       ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::REPLACE, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_REPLACE>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(multi_execute_internal(ctx, *spec, ops, results))) {
    LOG_WARN("fail to multi exeucte internal", K(ret));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_REPLACE);

  return ret;
}

int ObTableBatchService::multi_put(ObTableBatchCtx &ctx,
                                   const ObIArray<ObTableOperation> &ops,
                                   ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::PUT, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_INSERT>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create insert spec", K(ret));
  } else if (ctx.is_atomic_) {
    if (OB_FAIL(multi_op_in_executor(ctx, *spec, ops, results))) {
      LOG_WARN("fail to do multi operarion in executor", K(ret));
    }
  } else if (OB_FAIL(multi_execute_internal(ctx, *spec, ops, results))) {
    LOG_WARN("fail to multi exeucte internal", K(ret));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_PUT);

  return ret;
}

int ObTableBatchService::htable_delete(ObTableBatchCtx &ctx,
                                       const ObIArray<ObTableOperation> &ops,
                                       ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;
  tb_ctx.set_batch_operation(&ops);
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_ISNULL(trans_lock_handle) && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(
      ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(tb_ctx.get_table_id(),
                                                     ops,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ops));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx,
                                                                                 cache_guard,
                                                                                 spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create executor", K(ret));
  } else {
    ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor), ctx.audit_ctx_);
    if (OB_FAIL(delete_executor.open())) {
      LOG_WARN("fail to open htable delete executor", K(ret));
    } else if (OB_FAIL(delete_executor.get_next_row())) {
      LOG_WARN("fail to call htable delete get_next_row", K(ret));
    } else if (FALSE_IT(affected_rows = delete_executor.get_affected_rows())) {
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
      LOG_WARN("fail to close htable delete executor", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  if (OB_NOT_NULL(spec)) {
    spec->destroy_executor(executor);
    tb_ctx.set_expr_info(nullptr);
  }

  if (OB_SUCC(ret)) {
    ObTableOperationResult op_result = results.at(0); // shaddow copy
    results.reuse();
    op_result.set_affected_rows(affected_rows);
    if (OB_FAIL(results.push_back(op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }

  return ret;
}

int ObTableBatchService::htable_put(ObTableBatchCtx &ctx,
                                    const ObIArray<ObTableOperation> &ops,
                                    ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  int64_t affected_rows = 0;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  bool can_use_put = true;
  ObTableAuditMultiOp multi_op(ObTableOperationType::Type::PUT, ops);
  OB_TABLE_START_AUDIT(*ctx.credential_,
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       &ctx.audit_ctx_, multi_op);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_ISNULL(trans_lock_handle) && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(
      ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(tb_ctx.get_table_id(),
                                                     ops,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ops));
  } else if (OB_FAIL(tb_ctx.check_insert_up_can_use_put(can_use_put))) {
    LOG_WARN("fail to check htable put can use table api put", K(ret));
  } else if (can_use_put) {
    ObTableOperationType::Type origin_type = tb_ctx.get_opertion_type();
    tb_ctx.set_operation_type(ObTableOperationType::INSERT);
    tb_ctx.set_client_use_put(true);
    if (OB_FAIL(ObTableOpWrapper::get_insert_spec(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get insert spec", K(ret));
    } else {
      if (OB_FAIL(multi_op_in_executor(ctx, *spec, ops, results))) {
        LOG_WARN("fail to do multi operarion in executor", K(ret));
      } else {
        for (int64_t i = 0; i < results.count(); ++i) {
          const ObTableOperationResult &op_result = results.at(i);
          affected_rows += op_result.get_affected_rows();
        }
      }
    }
    tb_ctx.set_operation_type(origin_type); // reset to origin type
  } else {
    if (OB_FAIL(ObTableOpWrapper::get_insert_up_spec(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get insert up spec", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
        const ObTableOperation &op = ops.at(i);
        ObTableOperationResult single_op_result;
        tb_ctx.set_entity(&op.entity());
        if (OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
          LOG_WARN("fail to adjust entity", K(ret));
        } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, spec, single_op_result))) {
          LOG_WARN("fail to process op with spec", K(ret));
        } else {
          affected_rows += single_op_result.get_affected_rows();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTableOperationResult op_result = results.at(0); // shaddow copy
    results.reuse();
    op_result.set_affected_rows(affected_rows);
    if (OB_FAIL(results.push_back(op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.trans_param_->tx_snapshot_,
                     stmt_type, StmtType::T_KV_MULTI_PUT);
  return ret;
}

int ObTableBatchService::process_htable_delete(const ObTableOperation &op,
                                               ObTableBatchCtx &ctx,
                                               ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, ctx.allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    ObReqTimeGuard req_time_guard;
    ObTableApiCacheGuard cache_guard;
    int64_t affected_rows = 0;
    ObSEArray<ObTableOperation, 1> ops;
    tb_ctx.set_batch_operation(&ops);

    if (OB_FAIL(ops.push_back(op))) {
      LOG_WARN("fail to push back operation", K(ret), K(ops));
    } else if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx, ctx.tablet_ids_.at(0)))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                         ctx.trans_param_->tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create spec", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else {
      ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor), ctx.audit_ctx_);
      if (OB_FAIL(delete_executor.open())) {
        LOG_WARN("fail to open htable delete executor", K(ret));
      } else if (OB_FAIL(delete_executor.get_next_row())) {
        LOG_WARN("fail to call htable delete get_next_row", K(ret));
      } else if (FALSE_IT(affected_rows = delete_executor.get_affected_rows())) {
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
        LOG_WARN("fail to close htable delete executor", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
    if (OB_NOT_NULL(spec)) {
      spec->destroy_executor(executor);
      tb_ctx.set_expr_info(nullptr);
    }

    if (OB_SUCC(ret)) {
      ObTableOperationResult op_result = results.at(0); // shaddow copy
      results.reuse();
      op_result.set_affected_rows(affected_rows);
      if (OB_FAIL(results.push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::process_htable_put(const ObTableOperation &op,
                                            ObTableBatchCtx &ctx,
                                            ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, ctx.allocator_) {
    ObSEArray<ObTableOperation, 1> ops;
    tb_ctx.set_batch_operation(&ops);
    ObTableOperationResult tmp_op_result;
    if (OB_FAIL(ops.push_back(op))) {
      LOG_WARN("fail to push back operation", K(ret), K(ops));
    } else if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx, ctx.tablet_ids_.at(0)))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                         ctx.trans_param_->tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(process_insert_up(tb_ctx, tmp_op_result))) {
      LOG_WARN("fail to process insertup op", K(ret));
    } else {
      ObTableOperationResult op_result = results.at(0); // shaddow copy
      results.reuse();
      op_result.set_affected_rows(tmp_op_result.get_affected_rows());
      if (OB_FAIL(results.push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::htable_mutate_row(ObTableBatchCtx &ctx,
                                           const ObIArray<ObTableOperation> &ops,
                                           ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_ISNULL(trans_lock_handle) && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(
      ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(tb_ctx.get_table_id(),
                                                     ops,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ops));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < ops.count(); ++i) {
      // execute each mutation one by one
      const ObTableOperation &op = ops.at(i);
      switch (op.type()) {
        case ObTableOperationType::INSERT_OR_UPDATE: {
          if (OB_FAIL(process_htable_put(op, ctx, results))) {
            LOG_WARN("fail to process htable put", K(ret), K(i), K(op));
          }
          break;
        }
        case ObTableOperationType::DEL: {
          if (OB_FAIL(process_htable_delete(op, ctx, results))) {
            LOG_WARN("fail to process htable delete", K(ret), K(i), K(op));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutation type");
          LOG_WARN("not supported mutation type", K(ret), K(op));
          break;
        }
      }
    }
  }

  return ret;
}

int ObTableBatchService::init_table_ctx(ObTableCtx &tb_ctx,
                                        const ObTableOperation &op,
                                        const ObTableBatchCtx &batch_ctx,
                                        ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  tb_ctx.set_entity(&op.entity());
  tb_ctx.set_entity_type(batch_ctx.tb_ctx_.get_entity_type());
  tb_ctx.set_operation_type(op.type());
  tb_ctx.set_schema_cache_guard(batch_ctx.tb_ctx_.get_schema_cache_guard());
  tb_ctx.set_schema_guard(batch_ctx.tb_ctx_.get_schema_guard());
  tb_ctx.set_simple_table_schema(batch_ctx.tb_ctx_.get_simple_table_schema());
  tb_ctx.set_sess_guard(batch_ctx.tb_ctx_.get_sess_guard());
  tb_ctx.set_audit_ctx(&batch_ctx.audit_ctx_);
  tb_ctx.set_need_dist_das(batch_ctx.tb_ctx_.need_dist_das());

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(batch_ctx.credential_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("credential is null", K(ret));
  } else if (OB_FAIL(tb_ctx.init_common(*batch_ctx.credential_,
                                        tablet_id,
                                        batch_ctx.tb_ctx_.get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(batch_ctx));
  } else {
    ObTableOperationType::Type op_type = op.type();
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx.init_update())) {
          LOG_WARN("fail to init update ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(batch_ctx.use_put_))) {
          LOG_WARN("fail to init insert up ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx.init_append(batch_ctx.returning_affected_entity_,
                                       batch_ctx.returning_rowkey_))) {
          LOG_WARN("fail to init append ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx.init_increment(batch_ctx.returning_affected_entity_,
                                          batch_ctx.returning_rowkey_))) {
          LOG_WARN("fail to init increment ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type);
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  }

  return ret;
}

int ObTableBatchService::process_get(ObIAllocator &allocator,
                                     ObTableCtx &tb_ctx,
                                     ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::GET);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  ObNewRow *row = nullptr;
  ObITableEntity *result_entity = nullptr;
  const ObTableEntity *request_entity = static_cast<const ObTableEntity *>(tb_ctx.get_entity());
  const ObIArray<ObString> &cnames = request_entity->get_properties_names();
  ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx.get_schema_cache_guard();
  if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
  } else if (OB_FAIL(ObTableOpWrapper::process_get(tb_ctx, row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to process get", K(ret));
    }
  } else if (OB_FAIL(result.get_entity(result_entity))) {
    LOG_WARN("fail to get result entity", K(ret));
  } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator,
                                                               row,
                                                               *schema_cache_guard,
                                                               cnames,
                                                               result_entity))) {
    LOG_WARN("fail to cosntruct result entity", K(ret));
  }
  result.set_err(ret);
  result.set_type(tb_ctx.get_opertion_type());
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_GET,
                     has_table_scan, true);
  return ret;
}

int ObTableBatchService::process_insert(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::INSERT);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_insert_op(tb_ctx, result))) {
    LOG_WARN("fail to process insert", K(ret));
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_INSERT);
  return ret;
}

int ObTableBatchService::process_delete(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::DEL);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_DELETE>(tb_ctx, result))) {
    LOG_WARN("fail to process delete", K(ret));
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_DELETE);
  return ret;
}

int ObTableBatchService::process_update(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::UPDATE);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_UPDATE>(tb_ctx, result))) {
    LOG_WARN("fail to process update", K(ret));
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_UPDATE);
  return ret;
}

int ObTableBatchService::process_replace(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::REPLACE);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_REPLACE>(tb_ctx, result))) {
    LOG_WARN("fail to process replace", K(ret));
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_REPLACE);
  return ret;
}

int ObTableBatchService::process_insert_up(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(tb_ctx.get_opertion_type());
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_insert_up_op(tb_ctx, result))) {
    LOG_WARN("fail to process insert or update", K(ret));
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_INSERT_OR_UPDATE);
  return ret;
}

int ObTableBatchService::process_put(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::PUT);
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_put_op(tb_ctx, result))) {
    LOG_WARN("fail to process put", K(ret));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_PUT);
  return ret;
}

int ObTableBatchService::process_increment_or_append(ObTableCtx &tb_ctx, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(tb_ctx.get_opertion_type());
  op.set_entity(tb_ctx.get_entity());
  OB_TABLE_START_AUDIT(*tb_ctx.get_credential(),
                       *tb_ctx.get_sess_guard(),
                       tb_ctx.get_table_name(),
                       tb_ctx.get_audit_ctx(), op);
  if (OB_FAIL(ObTableOpWrapper::process_incr_or_append_op(tb_ctx, result))) {
    LOG_WARN("fail to process increment or append", K(ret));
  }

  StmtType stmt_type = tb_ctx.is_inc() ? StmtType::T_KV_INCREMENT : StmtType::T_KV_APPEND;
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, tb_ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, stmt_type);
  return ret;
}

int ObTableBatchService::batch_execute(ObTableBatchCtx &ctx,
                                       const ObIArray<ObTableOperation> &ops,
                                       ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id = ctx.tablet_ids_.empty() ? ctx.tb_ctx_.get_tablet_id() : ctx.tablet_ids_.at(0);
  bool is_multi_tablets_batch = ctx.tablet_ids_.count() > 1;
  if (is_multi_tablets_batch) {
    if (OB_UNLIKELY(ctx.tablet_ids_.count() != ops.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet ids count != ops count", K(ctx.tablet_ids_.count()), K(ops.count()));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    const ObTableOperation &op = ops.at(i);
    ObTableOperationResult &op_result = results.at(i);
    if (is_multi_tablets_batch) {
      tablet_id = ctx.tablet_ids_.at(i);
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObTableCtx, tb_ctx, ctx.allocator_) {
        if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx, tablet_id))) {
          LOG_WARN("fail to init table ctx for single operation", K(ret), K(tablet_id));
        } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                            ctx.trans_param_->tx_snapshot_))) {
          LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
        }
        if (OB_SUCC(ret)) {
          switch(op.type()) {
            case ObTableOperationType::GET:
              ret = process_get(ctx.allocator_, tb_ctx, op_result);
              if (ret == OB_ITER_END) {
                ret = OB_SUCCESS;
                op_result.set_err(ret);
              }
              break;
            case ObTableOperationType::INSERT:
              ret = process_insert(tb_ctx, op_result);
              break;
            case ObTableOperationType::DEL:
              ret = process_delete(tb_ctx, op_result);
              break;
            case ObTableOperationType::UPDATE:
              ret = process_update(tb_ctx, op_result);
              break;
            case ObTableOperationType::REPLACE:
              ret = process_replace(tb_ctx, op_result);
              break;
            case ObTableOperationType::INSERT_OR_UPDATE:
              ret = process_insert_up(tb_ctx, op_result);
              break;
            case ObTableOperationType::APPEND:
            case ObTableOperationType::INCREMENT:
              ret = process_increment_or_append(tb_ctx, op_result);
              break;
            case ObTableOperationType::PUT:
              ret = process_put(tb_ctx, op_result);
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("unexpected operation type", "type", op.type());
              break;
          }
          ObTableApiUtil::replace_ret_code(ret);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
      }
    } else {
      LOG_WARN("fail to execute batch operation, ", K(ret), K(op.type()), K(i));
    }
  }

  return ret;
}

int ObTableBatchService::check_arg2(bool returning_rowkey,
                                    bool returning_affected_entity)
{
  int ret = OB_SUCCESS;
  if (returning_rowkey || returning_affected_entity) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "returning rowkey or returning affected entity");
    LOG_WARN("some options not supported yet", K(ret), K(returning_rowkey), K(returning_affected_entity));
  }
  return ret;
}
