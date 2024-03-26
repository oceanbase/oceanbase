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
#include "ob_table_rpc_processor_util.h"
#include "ob_table_op_wrapper.h"
#include "observer/ob_req_time_service.h"
#include "ob_htable_utils.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

int ObTableBatchCtx::check_legality()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stat_event_type_) || OB_ISNULL(trans_param_)
      || OB_ISNULL(ops_) || OB_ISNULL(results_)
      || OB_ISNULL(entity_factory_) || OB_ISNULL(result_entity_)
      || OB_ISNULL(credential_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null input", K(ret), KP_(stat_event_type), KP_(trans_param), KP_(ops),
        KP_(results), KP_(entity_factory), KP_(result_entity), KP_(credential));
  } else if (ops_->count() == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "empty operarions in batch operation");
    LOG_WARN("empty operarions in batch operation is not supported");
  }

  return ret;
}

int ObTableBatchService::execute(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ctx.check_legality())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch ctx is invalid", K(ret));
  } else if (ctx.is_readonly_) {
    if (ctx.is_same_properties_names_) {
      *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_GET;
      ret = multi_get(ctx);
    } else {
      *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_BATCH_RETRIVE;
      ret = batch_execute(ctx);
    }
  } else if (ctx.is_same_type_) {
    ObTableOperationType::Type op_type = ctx.ops_->at(0).type();
    switch(op_type) {
      case ObTableOperationType::INSERT:
        *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INSERT;
        ret = multi_insert(ctx);
        break;
      case ObTableOperationType::DEL:
        if (ObTableEntityType::ET_HKV == ctx.entity_type_) {
          *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_DELETE;
          ret = htable_delete(ctx);
        } else {
          *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_DELETE;
          ret = multi_delete(ctx);
        }
        break;
      case ObTableOperationType::UPDATE:
        *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_UPDATE;
        ret = batch_execute(ctx);
        break;
      case ObTableOperationType::INSERT_OR_UPDATE:
        if (ObTableEntityType::ET_HKV == ctx.entity_type_) {
          *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_PUT;
          ret = htable_put(ctx);
        } else {
          *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INSERT_OR_UPDATE;
          ret = batch_execute(ctx);
        }
        break;
      case ObTableOperationType::REPLACE:
        *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_REPLACE;
        ret = multi_replace(ctx);
        break;
      case ObTableOperationType::PUT:
          *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_PUT;
          ret = multi_put(ctx);
          break;
      case ObTableOperationType::APPEND:
        *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_APPEND;
        ret = batch_execute(ctx);
        break;
      case ObTableOperationType::INCREMENT:
        *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_MULTI_INCREMENT;
        ret = batch_execute(ctx);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected operation type", "type", op_type, K(ctx.stat_event_type_));
        break;
    }
  } else {
    if (ObTableEntityType::ET_HKV == ctx.entity_type_) {
      // HTable mutate_row(RowMutations)
      *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_HYBRID;
      ret = htable_mutate_row(ctx);
    } else {
      // complex batch hybrid operation
      *ctx.stat_event_type_ = ObTableProccessType::TABLE_API_BATCH_HYBRID;
      ret = batch_execute(ctx);
    }
  }

  return ret;
}

int ObTableBatchService::multi_op_in_executor(ObTableBatchCtx &ctx, ObTableApiSpec &spec)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableApiExecutor *executor = nullptr;
  const ObIArray<ObTableOperation> &ops = *ctx.ops_;
  tb_ctx.set_batch_operation(&ops);

  if (OB_FAIL(adjust_entities(ctx))) {
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
    ObTableOperationResult op_result;
    ObITableEntity *result_entity = ctx.entity_factory_->alloc();
    if (OB_ISNULL(result_entity)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc entity", K(ret));
    } else if (!ctx.return_one_result_) {
      for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); ++i) {
        op_result.set_err(ret);
        op_result.set_type(tb_ctx.get_opertion_type());
        op_result.set_affected_rows(affected_rows);
        op_result.set_entity(*result_entity);
        if (OB_FAIL(ctx.results_->push_back(op_result))) {
          LOG_WARN("fail to push back result", K(ret));
        }
      }
    } else {
      op_result.set_err(ret);
      op_result.set_type(tb_ctx.get_opertion_type());
      op_result.set_affected_rows(affected_rows * ops.count());
      op_result.set_entity(*result_entity);
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
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

int ObTableBatchService::adjust_entities(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;

  // first entity has beed adjusted when create tb_ctx
  for (int64_t i = 1; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
    const ObTableOperation &op = ctx.ops_->at(i);
    tb_ctx.set_entity(&op.entity());
    if (OB_FAIL(tb_ctx.adjust_entity())) {
      LOG_WARN("fail to adjust entity", K(ret));
    }
  }

  return ret;
}

int ObTableBatchService::get_result_index(const ObNewRow &row,
                                          const ObIArray<ObTableOperation> &ops,
                                          ObIArray<int64_t> &indexs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
    const ObITableEntity &entity = ops.at(i).entity();
    ObRowkey entity_rowkey = entity.get_rowkey();
    ObRowkey row_rowkey(row.cells_, entity_rowkey.get_obj_cnt());
    bool equal = false;
    if (row_rowkey.equal(entity_rowkey, equal)) {
      LOG_WARN("fail to compare rowkey", K(row_rowkey), K(entity_rowkey));
    } else if (equal && OB_FAIL(indexs.push_back(i))) {
      LOG_WARN("fail to push_back index", K(row_rowkey), K(indexs), K(i));
    }
  }

  return ret;
}

int ObTableBatchService::multi_get_fuse_key_range(ObTableBatchCtx &ctx,
                                                  ObTableApiSpec &spec)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  ObTableApiExecutor *executor = nullptr;
  ObTableApiScanRowIterator row_iter;
  const ObIArray<ObTableOperation> &ops = *ctx.ops_;
  const int64_t op_size = ops.count();

  if (OB_FAIL(spec.create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create scan executor", K(ret));
  } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor *>(executor)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else if (OB_ISNULL(ctx.results_) || OB_ISNULL(ctx.entity_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ctx.results_ or ctx.entity_factory_ is nullptr", K(ret), KPC(ctx.results_));
  } else if (OB_FAIL(ctx.results_->prepare_allocate(op_size))) {
    LOG_WARN("fail to prepare allocate results", K(ret), K(op_size));
  } else {
    // 1. generate scan range with all rowkeys
    // 2. init result with empty result entity
    ObITableEntity *result_entity = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < op_size; ++i) {
      const ObTableOperation &op = ops.at(i);
      const ObITableEntity &entity = op.entity();
      ObRowkey rowkey = entity.get_rowkey();
      ObNewRange range;
      if (OB_FAIL(range.build_range(tb_ctx.get_ref_table_id(), rowkey))) {
        LOG_WARN("fail to build key range", K(ret), K(rowkey));
      } else if (OB_FAIL(tb_ctx.get_key_ranges().push_back(range))) {
        LOG_WARN("fail to push back key range", K(ret), K(range));
      } else if (OB_ISNULL(result_entity = ctx.entity_factory_->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc result entity", K(ret));
      } else {
        ObTableOperationResult op_result;
        op_result.set_err(OB_SUCCESS);
        op_result.set_type(tb_ctx.get_opertion_type());
        op_result.set_entity(result_entity);
        ctx.results_->at(i) = op_result;
      }
    }

    if (OB_SUCC(ret)) {
      ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx.get_schema_cache_guard();
      if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
      } else {
        const ObTableEntity *entity = nullptr;
        ObIAllocator &allocator = tb_ctx.get_allocator();
        // There may be duplicate primary keys in the range, duplicate primary keys can only scan 1 record,
        // we will return an empty result for duplicate primary keys op.
        // For example, if there are 50 operations out of 100 with the same primary key,
        // only 50 records can be scanned, but we return 100 results
        for (int64_t i = 0; OB_SUCC(ret) && i < op_size; ++i) {
          ObNewRow *row = nullptr;
          if (OB_FAIL(row_iter.get_next_row(row, allocator))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next row", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(row)) {
            ObSEArray<int64_t, 2> indexs;
            if (OB_FAIL(get_result_index(*row, ops, indexs))) {
              LOG_WARN("fail to get reuslt indexs", K(ret), KPC(row), K(ops));
            } else {
              for (int64_t idx = 0; OB_SUCC(ret) && idx < indexs.count(); ++idx) {
                int64_t index = indexs[idx];
                if (index >= ctx.results_->count() || index >= op_size) {
                  ret = OB_INDEX_OUT_OF_RANGE;
                  LOG_WARN("result index is out of range",
                      K(ret), K(index), K(ctx.results_->count()), K(op_size), KPC(row), K(ops));
                } else if (OB_FAIL(ctx.results_->at(index).get_entity(result_entity))) {
                  LOG_WARN("fail to get result entity", K(ret));
                } else if (OB_ISNULL(entity = &static_cast<const ObTableEntity &>(ops.at(index).entity()))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("entity is null", K(ret), K(index));
                } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(
                               allocator, row, *schema_cache_guard, entity->get_properties_names(), result_entity))) {
                  LOG_WARN("fail to construct result entity from row", K(ret), KPC(row), KPC(entity));
                }
              }  // end for
            }
          }
        }  // end for
      }
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
    LOG_WARN("fail to close row iterator", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }
  spec.destroy_executor(executor);

  return ret;
}

int ObTableBatchService::multi_get(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  const ObIArray<ObTableOperation> &ops = *ctx.ops_;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  tb_ctx.set_read_latest(false);
  tb_ctx.set_batch_operation(&ops);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(adjust_entities(ctx))) {
    LOG_WARN("fail to adjust entities", K(ret), K(ctx));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_SCAN>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(multi_get_fuse_key_range(ctx, *spec))) {
    LOG_WARN("fail to multi get fuse key range", K(ret), K(ctx));
  }

  return ret;
}

int ObTableBatchService::multi_insert(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_insert_spec(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create insert spec", K(ret));
  } else if (ctx.is_atomic_) {
    if (OB_FAIL(multi_op_in_executor(ctx, *spec))) {
      LOG_WARN("fail to do multi operarion in executor", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
      const ObTableOperation &op = ctx.ops_->at(i);
      tb_ctx.set_entity(&op.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = ctx.entity_factory_->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else if (i > 0 && OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
        LOG_WARN("fail to adjust entity", K(ret));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }

      affected_rows += op_result.get_affected_rows();
      if (OB_FAIL(ret)) {
      } else if (!ctx.return_one_result_ && OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
      }
    }
    if (OB_SUCC(ret) && ctx.return_one_result_) {
      ObTableOperationResult op_result;
      op_result.set_type(tb_ctx.get_opertion_type());
      op_result.set_entity(ctx.result_entity_);
      op_result.set_err(ret);
      op_result.set_affected_rows(affected_rows);
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::multi_delete(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
      const ObTableOperation &op = ctx.ops_->at(i);
      tb_ctx.set_entity(&op.entity());
      ObTableOperationResult op_result;
      ObTableApiExecutor *executor = nullptr;
      ObITableEntity *result_entity = ctx.entity_factory_->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else if (i > 0 && OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
        LOG_WARN("fail to adjust entity", K(ret));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }

      affected_rows += op_result.get_affected_rows();
      if (OB_FAIL(ret)) {
      } else if (!ctx.return_one_result_ && OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
      }
    }
    if (OB_SUCC(ret) && ctx.return_one_result_) {
      ObTableOperationResult op_result;
      op_result.set_type(tb_ctx.get_opertion_type());
      op_result.set_entity(ctx.result_entity_);
      op_result.set_err(ret);
      op_result.set_affected_rows(affected_rows);
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::multi_replace(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_REPLACE>(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
      const ObTableOperation &op = ctx.ops_->at(i);
      tb_ctx.set_entity(&op.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = ctx.entity_factory_->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else if (i > 0 && OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
        LOG_WARN("fail to adjust entity", K(ret));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }

      affected_rows += op_result.get_affected_rows();
      if (OB_FAIL(ret)) {
      } else if (!ctx.return_one_result_ && OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
      }
    }
    if (OB_SUCC(ret) && ctx.return_one_result_) {
      ObTableOperationResult op_result;
      op_result.set_type(tb_ctx.get_opertion_type());
      op_result.set_entity(ctx.result_entity_);
      op_result.set_err(ret);
      op_result.set_affected_rows(affected_rows);
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::multi_put(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_FAIL(ObTableOpWrapper::get_insert_spec(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get or create insert spec", K(ret));
  } else if (ctx.is_atomic_) {
    if (OB_FAIL(multi_op_in_executor(ctx, *spec))) {
      LOG_WARN("fail to do multi operarion in executor", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
      const ObTableOperation &op = ctx.ops_->at(i);
      tb_ctx.set_entity(&op.entity());
      ObTableOperationResult op_result;
      ObITableEntity *result_entity = ctx.entity_factory_->alloc();
      if (OB_ISNULL(result_entity)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else if (i > 0 && OB_FAIL(tb_ctx.adjust_entity())) { // first entity adjust in init_single_op_tb_ctx
        LOG_WARN("fail to adjust entity", K(ret));
      } else if (FALSE_IT(op_result.set_entity(*result_entity))) {
      } else if (OB_FAIL(ObTableOpWrapper::process_op_with_spec(tb_ctx, spec, op_result))) {
        LOG_WARN("fail to process insert with spec", K(ret), K(i));
        table::ObTableApiUtil::replace_ret_code(ret);
      }

      affected_rows += op_result.get_affected_rows();
      if (OB_FAIL(ret)) {
      } else if (!ctx.return_one_result_ && OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
        LOG_WARN("fail to execute one operation when batch execute as atomic", K(ret), K(op));
      }
    }
    if (OB_SUCC(ret) && ctx.return_one_result_) {
      ObTableOperationResult op_result;
      op_result.set_type(tb_ctx.get_opertion_type());
      op_result.set_entity(ctx.result_entity_);
      op_result.set_err(ret);
      op_result.set_affected_rows(affected_rows);
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::htable_delete(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  int64_t affected_rows = 0;
  tb_ctx.set_batch_operation(ctx.ops_);
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_ISNULL(trans_lock_handle) && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(
      ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(tb_ctx.get_table_id(),
                                                     *ctx.ops_,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ctx.ops_));
  } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx,
                                                                                 cache_guard,
                                                                                 spec))) {
    LOG_WARN("fail to get or create spec", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to create executor", K(ret));
  } else {
    ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor));
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
    ObTableOperationResult single_op_result;
    single_op_result.set_entity(ctx.result_entity_);
    single_op_result.set_type(ObTableOperationType::DEL);
    single_op_result.set_err(ret);
    single_op_result.set_affected_rows(affected_rows);
    ctx.results_->reset();
    if (OB_FAIL(ctx.results_->push_back(single_op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }

  return ret;
}

int ObTableBatchService::htable_put(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  int64_t affected_rows = 0;
  observer::ObReqTimeGuard req_time_guard;
  ObTableApiCacheGuard cache_guard;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;
  ObTableCtx &tb_ctx = ctx.tb_ctx_;
  // hbase put use 'put' in TABLE_API_EXEC_INSERT
  tb_ctx.set_client_use_put(true);

  if (OB_FAIL(check_arg2(ctx.returning_rowkey_, ctx.returning_affected_entity_))) {
    LOG_WARN("fail to check arg", K(ret), K(ctx.returning_rowkey_), K(ctx.returning_affected_entity_));
  } else if (OB_ISNULL(trans_lock_handle) && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(
      ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
  } else if (OB_FAIL(ObHTableUtils::lock_htable_rows(tb_ctx.get_table_id(),
                                                     *ctx.ops_,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ctx.ops_));
  } else if (OB_FAIL(ObTableOpWrapper::get_insert_spec(tb_ctx, cache_guard, spec))) {
    LOG_WARN("fail to get insert spec", K(ret));
  } else {
    if (OB_FAIL(multi_op_in_executor(ctx, *spec))) {
      LOG_WARN("fail to do multi operarion in executor", K(ret));
    } else {
      for (int64_t i = 0; i < ctx.results_->count(); ++i) {
        const ObTableOperationResult &single_op_result = ctx.results_->at(i);
        affected_rows += single_op_result.get_affected_rows();
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTableOperationResult op_result;
    op_result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
    op_result.set_entity(ctx.result_entity_);
    op_result.set_err(ret);
    op_result.set_affected_rows(affected_rows);
    ctx.results_->reset();
    if (OB_FAIL(ctx.results_->push_back(op_result))) {
      LOG_WARN("failed to add result", K(ret));
    }
  }

  return ret;
}

int ObTableBatchService::process_htable_delete(const ObTableOperation &op,
                                               ObTableBatchCtx &ctx)
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
    } else if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                         ctx.trans_param_->tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get or create spec", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else {
      ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor));
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
      ObTableOperationResult single_op_result;
      single_op_result.set_entity(ctx.result_entity_);
      single_op_result.set_type(ObTableOperationType::DEL);
      single_op_result.set_err(ret);
      single_op_result.set_affected_rows(affected_rows);
      ctx.results_->reset();
      if (OB_FAIL(ctx.results_->push_back(single_op_result))) {
        LOG_WARN("failed to add result", K(ret));
      }
    }
  }

  return ret;
}

int ObTableBatchService::process_htable_put(const ObTableOperation &op,
                                            ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, ctx.allocator_) {
    ObSEArray<ObTableOperation, 1> ops;
    tb_ctx.set_batch_operation(&ops);
    ObTableOperationResult single_op_result;
    single_op_result.set_entity(ctx.result_entity_);
    if (OB_FAIL(ops.push_back(op))) {
      LOG_WARN("fail to push back operation", K(ret), K(ops));
    } else if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                         ctx.trans_param_->tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_insert_up_op(tb_ctx, single_op_result))) {
      LOG_WARN("fail to process insertup op", K(ret));
    } else if (FALSE_IT(ctx.results_->reset())) {
    } else if (OB_FAIL(ctx.results_->push_back(single_op_result))) {
      LOG_WARN("fail to push add result", K(ret));
    }
  }

  return ret;
}

int ObTableBatchService::htable_mutate_row(ObTableBatchCtx &ctx)
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
                                                     *ctx.ops_,
                                                     *trans_lock_handle,
                                                     ObHTableLockMode::SHARED))) {
    LOG_WARN("fail to lock htable rows", K(ret), K(tb_ctx.get_table_id()), K(ctx.ops_));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < ctx.ops_->count(); ++i) {
      // execute each mutation one by one
      const ObTableOperation &op = ctx.ops_->at(i);
      switch (op.type()) {
        case ObTableOperationType::INSERT_OR_UPDATE: {
          if (OB_FAIL(process_htable_put(op, ctx))) {
            LOG_WARN("fail to process htable put", K(ret), K(i), K(op));
          }
          break;
        }
        case ObTableOperationType::DEL: {
          if (OB_FAIL(process_htable_delete(op, ctx))) {
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

int ObTableBatchService::init_table_ctx(table::ObTableCtx &tb_ctx,
                                        const ObTableOperation &op,
                                        const ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  tb_ctx.set_entity(&op.entity());
  tb_ctx.set_entity_type(batch_ctx.entity_type_);
  tb_ctx.set_operation_type(op.type());
  tb_ctx.set_schema_cache_guard(batch_ctx.tb_ctx_.get_schema_cache_guard());
  tb_ctx.set_schema_guard(batch_ctx.tb_ctx_.get_schema_guard());
  tb_ctx.set_simple_table_schema(batch_ctx.tb_ctx_.get_simple_table_schema());
  tb_ctx.set_sess_guard(batch_ctx.tb_ctx_.get_sess_guard());

  if (OB_ISNULL(batch_ctx.credential_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("credential is null", K(ret));
  } else if (OB_FAIL(tb_ctx.init_common(*batch_ctx.credential_,
                                        batch_ctx.tablet_id_,
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
  ObNewRow *row;
  ObITableEntity *result_entity = nullptr;
  const ObTableEntity *request_entity = static_cast<const ObTableEntity *>(tb_ctx.get_entity());
  const ObIArray<ObString> &cnames = request_entity->get_properties_names();
  ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx.get_schema_cache_guard();
  if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
  } else if (OB_FAIL(ObTableOpWrapper::process_get(tb_ctx, row))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
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
  return ret;
}

int ObTableBatchService::batch_execute(ObTableBatchCtx &ctx)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.ops_->count(); ++i) {
    const ObTableOperation &op = ctx.ops_->at(i);
    ObTableOperationResult op_result;
    ObITableEntity *result_entity = ctx.entity_factory_->alloc();
    if (OB_ISNULL(result_entity)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc entity", K(ret));
    }

    SMART_VAR(table::ObTableCtx, tb_ctx, ctx.allocator_) {
      if (OB_FAIL(init_table_ctx(tb_ctx, op, ctx))) {
        LOG_WARN("fail to init table ctx for single operation", K(ret));
      }  else if (OB_FAIL(tb_ctx.init_trans(ctx.trans_param_->trans_desc_,
                                            ctx.trans_param_->tx_snapshot_))) {
        LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
      } else {
        op_result.set_entity(*result_entity);
      }
      if (OB_SUCC(ret)) {
        switch(op.type()) {
          case ObTableOperationType::GET:
            ret = process_get(ctx.allocator_, tb_ctx, op_result);
            break;
          case ObTableOperationType::INSERT:
            ret = ObTableOpWrapper::process_insert_op(tb_ctx, op_result);
            break;
          case ObTableOperationType::DEL:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_DELETE>(tb_ctx, op_result);
            break;
          case ObTableOperationType::UPDATE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_UPDATE>(tb_ctx, op_result);
            break;
          case ObTableOperationType::REPLACE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_REPLACE>(tb_ctx, op_result);
            break;
          case ObTableOperationType::INSERT_OR_UPDATE:
          case ObTableOperationType::APPEND:
          case ObTableOperationType::INCREMENT:
            ret = ObTableOpWrapper::process_insert_up_op(tb_ctx, op_result);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected operation type", "type", op.type());
            break;
        }
        ObTableApiUtil::replace_ret_code(ret);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ctx.results_->push_back(op_result))) {
        LOG_WARN("fail to push back result", K(ret));
      } else if (ctx.is_atomic_ && OB_FAIL(op_result.get_errno())) {
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