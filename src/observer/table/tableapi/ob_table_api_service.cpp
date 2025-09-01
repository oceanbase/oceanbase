/**
 * Copyright (c) 2025 OceanBase
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
#include "ob_table_api_service.h"
#include "observer/table/ob_table_op_wrapper.h"
#include "lib/container/ob_fixed_array.h"
#include "observer/table/part_calc/ob_table_part_calc.h"

using namespace oceanbase::sql::stmt;
namespace oceanbase
{
namespace table
{
// ======================== single opertion ==========================
int ObTableApiService::insert(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tb_ctx is not inited", K(ret));
  } else if (!ctx.is_ttl_table()) {
    ret = process_single_dml_op<TABLE_API_EXEC_INSERT, ObTableOperationType::INSERT, StmtType::T_KV_INSERT>(ctx, entity, result);
  } else {
    ret = process_single_dml_op<TABLE_API_EXEC_TTL, ObTableOperationType::INSERT, StmtType::T_KV_INSERT>(ctx, entity, result);
  }
  return ret;
}

int ObTableApiService::update(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  return process_single_dml_op<TABLE_API_EXEC_UPDATE, ObTableOperationType::UPDATE, StmtType::T_KV_UPDATE>(ctx, entity, result);
}

int ObTableApiService::insert_or_update(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else if (!ctx.is_ttl_table()) {
    ret = process_single_dml_op<TABLE_API_EXEC_INSERT_UP, ObTableOperationType::INSERT_OR_UPDATE, StmtType::T_KV_INSERT_OR_UPDATE>(ctx, entity, result);
  } else {
    ret = process_single_dml_op<TABLE_API_EXEC_TTL, ObTableOperationType::INSERT_OR_UPDATE, StmtType::T_KV_INSERT_OR_UPDATE>(ctx, entity, result);
  }
  return ret;
}

int ObTableApiService::put(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  return process_single_dml_op<TABLE_API_EXEC_INSERT, ObTableOperationType::PUT, StmtType::T_KV_PUT>(ctx, entity, result);
}

int ObTableApiService::replace(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  return process_single_dml_op<TABLE_API_EXEC_REPLACE, ObTableOperationType::REPLACE, StmtType::T_KV_REPLACE>(ctx, entity, result);
}

int ObTableApiService::del(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  return process_single_dml_op<TABLE_API_EXEC_DELETE, ObTableOperationType::DEL, StmtType::T_KV_DELETE>(ctx, entity, result);
}

int ObTableApiService::get(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else {
    ctx.set_operation_type(ObTableOperationType::GET);
    ctx.set_entity(&entity);
    ObIAllocator &allocator = ctx.get_allocator();
    ObTableApiRowIterator row_iter; // table row iterator
    ObRowkey rowkey = entity.get_rowkey();
    ObNewRange range;
    ObNewRow *row = nullptr;
    ctx.get_key_ranges().reset();
    if (OB_FAIL(range.build_range(ctx.get_ref_table_id(), rowkey))) {
      LOG_WARN("fail to build key range", K(ret), K(rowkey), K(ctx.get_ref_table_id()));
    } else if (OB_FAIL(ctx.get_key_ranges().push_back(range))) {
      LOG_WARN("fail to push back key range", K(ret), K(range));
    } else if (OB_FAIL(row_iter.init(ctx))) {
      LOG_WARN("fail to init row iter", K(ret));
    } else if (OB_FAIL(row_iter.open())) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else if (OB_FAIL(row_iter.get_next_row(row, allocator))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get row", K(ret));
      }
    } else if (OB_NOT_NULL(row)) {
      // fill result entity
      ObITableEntity *result_entity = nullptr;
      ObKvSchemaCacheGuard *schema_cache_guard = ctx.get_schema_cache_guard();
      if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
      } else if (OB_FAIL(result.get_entity(result_entity))) {
        LOG_WARN("fail to get result entity", K(ret));
      } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator,
                                                                  row,
                                                                  *schema_cache_guard,
                                                                  ctx.get_query_col_names(),
                                                                  result_entity))) {
        LOG_WARN("fail to fill result entity", K(ret));
      }
    }
    result.set_err(ret);
    result.set_type(ctx.get_opertion_type());
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(row_iter.close())) {
      LOG_WARN("fail to close row iterator", K(tmp_ret));
    }
  }
  return ret;
}

// ======================== batch opertion ==========================
int ObTableApiService::multi_insert(ObTableCtx &ctx,
                                    const ObIArray<const ObITableEntity*> &entities,
                                    ObIArray<ObTableOperationResult> *results)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tb_ctx is not inited", K(ret));
  } else if (!ctx.is_ttl_table()) {
    ret = process_batch_in_executor<TABLE_API_EXEC_INSERT, ObTableOperationType::INSERT, StmtType::T_KV_MULTI_INSERT>(ctx, entities, results);
  } else {
    ret = process_batch_with_same_spec<TABLE_API_EXEC_TTL, ObTableOperationType::INSERT, StmtType::T_KV_MULTI_INSERT>(ctx, entities, results);
  }
  return ret;
}

int ObTableApiService::multi_update(ObTableCtx &ctx,
                                    const ObIArray<const ObITableEntity*> &entities,
                                    ObIArray<ObTableOperationResult> *results)
{
  return process_batch_with_same_spec<TABLE_API_EXEC_UPDATE, ObTableOperationType::UPDATE, StmtType::T_KV_MULTI_UPDATE>(ctx, entities, results);
}

int ObTableApiService::multi_insert_or_update(ObTableCtx &ctx,
                                              const ObIArray<const ObITableEntity*> &entities,
                                              ObIArray<ObTableOperationResult> *results)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret), K(ctx));
  } else if (ctx.is_ttl_table()) {
    ret = process_batch_with_same_spec<TABLE_API_EXEC_TTL, ObTableOperationType::INSERT_OR_UPDATE, StmtType::T_KV_MULTI_INSERT_OR_UPDATE>(ctx, entities, results);
  } else {
    ret = process_batch_with_same_spec<TABLE_API_EXEC_INSERT_UP, ObTableOperationType::INSERT_OR_UPDATE, StmtType::T_KV_MULTI_INSERT_OR_UPDATE>(ctx, entities, results);
  }
  return ret;
}

int ObTableApiService::multi_put(ObTableCtx &ctx,
                                 const ObIArray<const ObITableEntity*> &entities,
                                 ObIArray<ObTableOperationResult> *results)
{
  return process_batch_in_executor<TABLE_API_EXEC_INSERT, ObTableOperationType::INSERT, StmtType::T_KV_MULTI_PUT>(ctx, entities, results);
}

int ObTableApiService::multi_get(ObTableCtx &ctx,
                                 const ObIArray<const ObITableEntity*> &entities,
                                 ObIArray<ObTableOperationResult> *results)
{
  int ret = OB_SUCCESS;
  ObTableApiCacheGuard cache_guard;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ctx.set_batch_entities(&entities);
  ctx.set_operation_type(ObTableOperationType::GET);
  if (OB_FAIL(check_batch_args(ctx, entities, results))) {
    LOG_WARN("fail to check batch args", K(ret));
  } else if (OB_FAIL(generate_scan_ranges_by_entities(ctx.get_ref_table_id(), entities, ctx.get_key_ranges()))) {
    LOG_WARN("fail to gen scan range from entities", K(ret), K(ctx.get_ref_table_id()), K(entities));
  } else {
    ObKvSchemaCacheGuard *schema_cache_guard = ctx.get_schema_cache_guard();
    ObIAllocator &allocator = ctx.get_allocator();
    ObTableApiRowIterator row_iter; // table row iterator
    if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
    } else if (OB_FAIL(row_iter.init(ctx))) {
      LOG_WARN("fail to init row iter", K(ret));
    } else if (OB_FAIL(row_iter.open())) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else {
      ObNewRow *row = nullptr;
      for (int64_t i = 0; i < entities.count() && OB_SUCC(ret); i++) {
        if (OB_FAIL(row_iter.get_next_row(row, allocator))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get row", K(ret));
          }
        } else if (OB_NOT_NULL(row) && OB_NOT_NULL(results) &&
                OB_FAIL(construct_entities_from_row(allocator, *schema_cache_guard, *row, entities, *results))) {
          LOG_WARN("fail to construct result entities from row", K(ret), KPC(row));
        }
      } // end for
      ret = ret == OB_ITER_END ? OB_SUCCESS : ret;
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
      LOG_WARN("fail to close row iterator", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObTableApiService::multi_delete(ObTableCtx &ctx,
                                    const ObIArray<const ObITableEntity*> &entities,
                                    ObIArray<ObTableOperationResult> *results)
{
  return process_batch_with_same_spec<TABLE_API_EXEC_DELETE, ObTableOperationType::DEL, StmtType::T_KV_MULTI_DELETE>(ctx, entities, results);
}

int ObTableApiService::multi_replace(ObTableCtx &ctx,
                                     const ObIArray<const ObITableEntity*> &entities,
                                     ObIArray<ObTableOperationResult> *results)
{
  return process_batch_with_same_spec<TABLE_API_EXEC_REPLACE, ObTableOperationType::REPLACE, StmtType::T_KV_MULTI_REPLACE>(ctx, entities, results);
}

int ObTableApiService::get_appropriate_spec(ObTableCtx &ctx,
                                           ObTableApiCacheGuard &cache_guard,
                                           ObTableApiSpec *&spec)
{
  int ret = OB_SUCCESS;
  bool use_put = ctx.get_opertion_type() == ObTableOperationType::PUT;
  if (use_put) { // for ttl table: we can use put directly
    ret = get_spec<TABLE_API_EXEC_INSERT>(ctx, cache_guard, spec);
  } else if (ctx.is_ttl_table()) {
    ret = get_spec<TABLE_API_EXEC_TTL>(ctx, cache_guard, spec);
  } else {
    ret = get_spec<TABLE_API_EXEC_INSERT_UP>(ctx, cache_guard, spec);
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to get spec", K(ret));
  } else if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spec is NULL", K(ret));
  }

  return ret;
}

int ObTableApiService::execute_batch_put(ObTableCtx &ctx, ObTableApiSpec *spec)
{
  int ret = OB_SUCCESS;
  ObTableAuditMultiOp multi_op(ctx.get_opertion_type(), ctx.get_batch_entities());
  OB_TABLE_START_AUDIT(*ctx.get_credential(),
                       *ctx.get_sess_guard(),
                       ctx.get_table_name(),
                       ctx.get_audit_ctx(), multi_op);
  ctx.set_need_dist_das(true);
  if (OB_ISNULL(spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spec is NULL", K(ret));
  } else {
    bool use_put = ctx.get_opertion_type() == ObTableOperationType::PUT;
    if (use_put) {
      ret = execute_multi_put_within_executor(ctx, spec);
    } else {
      ret = execute_multi_put_with_same_spec(ctx, spec);
    }
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                     stmt_type, StmtType::T_KV_MULTI_PUT);
  return ret;
}

int ObTableApiService::execute_multi_put_within_executor(ObTableCtx &ctx, ObTableApiSpec *spec)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;

  if (OB_FAIL(spec->create_executor(ctx, executor))) {
    LOG_WARN("fail to create executor", K(ret));
  } else if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is NULL", K(ret));
  } else if (OB_FAIL(execute_and_cleanup_executor(ctx, spec, executor))) {
    LOG_WARN("fail to execute and cleanup executor", K(ret));
  }

  return ret;
}

int ObTableApiService::execute_multi_put_with_same_spec(ObTableCtx &ctx,
                                                        ObTableApiSpec *spec)
{
  int ret = OB_SUCCESS;
  const ObIArray<const ObITableEntity*> *entities = ctx.get_batch_entities();
  if (OB_ISNULL(entities)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entities is NULL", K(ret));
  }
  for (int64_t i = 0; i < entities->count() && OB_SUCC(ret); i++) {
    ObTableApiExecutor *executor = nullptr;
    const ObITableEntity *entity = entities->at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is NULL", K(ret));
    } else if (OB_FAIL(spec->create_executor(ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else {
      ctx.set_entity(const_cast<ObITableEntity*>(entity));
      ctx.set_tablet_id(entity->get_tablet_id());
      if (OB_FAIL(execute_and_cleanup_executor(ctx, spec, executor))) {
        LOG_WARN("fail to execute and cleanup executor", K(ret));
      }
    }
  }

  return ret;
}

int ObTableApiService::execute_and_cleanup_executor(ObTableCtx &ctx,
                                                   ObTableApiSpec *spec,
                                                   ObTableApiExecutor *executor)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is NULL", K(ret));
  } else if (OB_FAIL(executor->open())) {
    LOG_WARN("fail to open executor", K(ret));
  } else if (OB_FAIL(executor->get_next_row())) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get next row", K(ret));
    }
  }

  // Cleanup executor
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(executor) && OB_TMP_FAIL(executor->close())) {
    LOG_WARN("fail to close executor", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  if (OB_NOT_NULL(executor)) {
    spec->destroy_executor(executor);
    executor = nullptr;
  }

  return ret;
}

// ======================== query interface ==========================
int ObTableApiService::query(ObTableCtx &ctx, ObTableApiRowIterator &iter)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTabletID> *tablet_ids = ctx.get_batch_tablet_ids();
  ObIArray<ObTabletID> *new_tablet_ids = nullptr;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else if (need_calc_tablet_id(tablet_ids) && OB_FAIL(calc_tablet_ids(ctx, ctx.get_key_ranges(), new_tablet_ids))) {
    LOG_WARN("fail to calc tablet ids", K(ret));
  } else if (OB_FAIL(iter.init(ctx))) {
    LOG_WARN("fail to init table row iter", K(ret));
  }
  return ret;
}

// ======================== others ==========================
int ObTableApiService::init_tablet_ids_array(ObTableCtx &ctx,
                                             const int64_t array_size,
                                             ObIArray<ObTabletID> *&tablet_ids)
{
  int ret = OB_SUCCESS;
  typedef ObFixedArray<ObTabletID, ObIAllocator> ObTabletIDArray;
  ObIAllocator &allocator = ctx.get_allocator();
  ObTabletIDArray *tmp_tablet_ids = nullptr;
  if (OB_ISNULL(tmp_tablet_ids = OB_NEWx(ObTabletIDArray, (&allocator), (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc tablet_ids array", K(ret));
  } else if (OB_FAIL(tmp_tablet_ids->init(array_size))) {
    LOG_WARN("fail to init tmp tablet ids array", K(ret));
  } else {
    ObTableApiSessGuard *sess_guard = nullptr;
    ObKvSchemaCacheGuard *kv_schema_guard = nullptr;
    share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
    if (OB_ISNULL(sess_guard = ctx.get_sess_guard()) ||
        OB_ISNULL(kv_schema_guard = ctx.get_schema_cache_guard()) ||
        OB_ISNULL(schema_guard = ctx.get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sess and schema members of ctx is NULL", K(ret), KP(sess_guard),
               KP(kv_schema_guard), KP(schema_guard));
    } else {
      tablet_ids = tmp_tablet_ids;
    }
  }
  return ret;
}

int ObTableApiService::calc_tablet_ids(ObTableCtx &ctx,
                                      const ObIArray<ObNewRange> &ranges,
                                      ObIArray<ObTabletID> *&tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *simple_table_schema = !ctx.is_index_scan() ?
                    ctx.get_simple_table_schema() : ctx.get_index_schema();
  if (OB_ISNULL(simple_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("simple schema is NULL", K(ret));
  } else {
    int64_t size = simple_table_schema->get_all_part_num();
    if (OB_FAIL(init_tablet_ids_array(ctx, size, tablet_ids))) {
      LOG_WARN("fail to init tablet ids array", K(ret), K(size));
    } else {
      ObTablePartClipType clip_type = ObTablePartClipType::NONE;
      if (OB_NOT_NULL(ctx.get_query()) && ctx.get_query()->is_hot_only()) {
        clip_type = ObTablePartClipType::HOT_ONLY;
      }
      ObTablePartCalculator part_calc(ctx.get_allocator(),
                                      *ctx.get_sess_guard(),
                                      *ctx.get_schema_cache_guard(),
                                      *ctx.get_schema_guard(),
                                      clip_type);
      if (OB_FAIL(part_calc.calc(ctx.get_index_table_id(), ranges, *tablet_ids))) {
        LOG_WARN("fail to calc tablet_ids", K(ret), K(ctx.get_table_id()));
      } else {
        ctx.set_batch_tablet_ids(tablet_ids);
      }
    }
  }
  return ret;
}

int ObTableApiService::check_batch_args(ObTableCtx &ctx,
                                       const ObIArray<const ObITableEntity*> &entities,
                                       ObIArray<ObTableOperationResult> *results)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTabletID> *tablet_ids = ctx.get_batch_tablet_ids();

  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else if (ctx.return_rowkey() || ctx.return_affected_entity()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "returning rowkey or returning affected entity");
    LOG_WARN("some options not supported yet", K(ret), K(ctx.return_rowkey()), K(ctx.return_affected_entity()));
  } else if (OB_NOT_NULL(results) && entities.count() != results->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entities count is not match with result count", K(entities.count()), K(results->count()));
  } else if (OB_FAIL(adjust_entities(ctx, entities))) {
    LOG_WARN("fail to adjust entities", K(ret));
  } else if (need_calc_tablet_id(tablet_ids, entities)) {
    ObIArray<ObTabletID> *tmp_tablet_ids = nullptr;
    if (OB_FAIL(init_tablet_ids_array(ctx, entities.count(), tmp_tablet_ids))) {
      LOG_WARN("fail to init tablet ids array", K(ret));
    } else {
      ObTablePartCalculator part_calc(ctx.get_allocator(),
                                      *ctx.get_sess_guard(),
                                      *ctx.get_schema_cache_guard(),
                                      *ctx.get_schema_guard());
      if (OB_FAIL(part_calc.calc(ctx.get_table_id(), entities, *tmp_tablet_ids))) {
        LOG_WARN("fail to calc tablet_ids", K(ret), K(ctx.get_table_id()));
      } else {
        ctx.set_batch_tablet_ids(tmp_tablet_ids);
        ctx.set_tablet_id(tmp_tablet_ids->at(0));
      }
    }
  }
  return ret;
}

bool ObTableApiService::need_calc_tablet_id(const common::ObIArray<ObTabletID> *tablet_ids,
                                            const common::ObIArray<const ObITableEntity *> &entities)
{
  bool bret = false;
  if (OB_ISNULL(tablet_ids) || tablet_ids->empty()) {
    bret = true;
  } else if (tablet_ids->count() == 1 || tablet_ids->count() == entities.count()) {
    for (int64_t i = 0; i < tablet_ids->count() && !bret; i++) {
      if (!tablet_ids->at(i).is_valid()) {
        bret = true;
      }
    }
  } else {
    bret = true;
  }
  return bret;
}

bool ObTableApiService::need_calc_tablet_id(const common::ObIArray<ObTabletID> *tablet_ids)
{
  bool bret = false;
  if (OB_ISNULL(tablet_ids) || tablet_ids->empty()) {
    bret = true;
  } else {
    for (int64_t i = 0; i < tablet_ids->count() && !bret; i++) {
      if (!tablet_ids->at(i).is_valid()) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObTableApiService::adjust_entities(ObTableCtx &ctx, const common::ObIArray<const ObITableEntity *> &entities)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < entities.count() && OB_SUCC(ret); i++) {
    const ObITableEntity *entity = entities.at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is NULL", K(ret), K(i));
    } else if (i > 0 && !is_same_plan(ctx.get_opertion_type(), *entities.at(0), *entities.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op in batch can not use the same plan", K(ret));
    } else {
      ctx.set_entity(entity);
      if (OB_FAIL(ctx.adjust_entity())) {
        LOG_WARN("fail to adjust entity", K(ret));
      }
    }
  }
  return ret;
}

bool ObTableApiService::is_same_plan(ObTableOperationType::Type op_type,
                                    const ObITableEntity &src_i_entity,
                                    const ObITableEntity &dest_i_entity)
{
  bool bret = true;
  if (op_type == ObTableOperationType::Type::UPDATE ||
      op_type == ObTableOperationType::Type::INSERT_OR_UPDATE) {
    const ObTableEntity &src_entity = static_cast<const ObTableEntity &>(src_i_entity);
    const ObTableEntity &dst_entity = static_cast<const ObTableEntity &>(dest_i_entity);
    const ObIArray<ObString> &src_prop_names = src_entity.get_properties_names();
    const ObIArray<ObString> &dest_prop_names = dst_entity.get_properties_names();
    if (src_prop_names.count() != dest_prop_names.count()) {
      bret = false;
    } else {
      for (int64_t i = 0; i < src_prop_names.count() && !bret; i++) {
        if (0 != src_prop_names.at(i).compare(dest_prop_names.at(i))) {
          bret = false;
        }
      }
    }
  }
  return bret;
}


int ObTableApiService::process_executor(ObTableCtx &ctx, ObTableApiExecutor *executor, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else if (OB_ISNULL(executor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("executor is NULL", K(ret));
  } else if (OB_FAIL(executor->open())) {
    LOG_WARN("fail to open executor", K(ret));
  } else if (OB_FAIL(executor->get_next_row())) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get_next_row", K(ret), K(ctx));
    }
  } else if (OB_FAIL(process_dml_result(ctx, *executor, result))) {
    LOG_WARN("fail to process result", K(ret));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = executor->close())) {
    LOG_WARN("fail to close executor", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  result.set_err(ret);
  result.set_type(ctx.get_opertion_type());
  return ret;
}

int ObTableApiService::process_dml_result(ObTableCtx &ctx, ObTableApiExecutor &executor, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  ObTableApiModifyExecutor &modify_executor = static_cast<ObTableApiModifyExecutor &>(executor);
  result.set_affected_rows(modify_executor.get_affected_rows());
  // TODO: currently hbase has no need and implement later
  // - insert_or_update && ttl && replace：judge if return the executed op type and old row value
  // - increment && append: judge flag return_affected_entity and return_rowkey
  return ret;
}

int ObTableApiService::construct_entities_from_row(ObIAllocator &allocator,
                                                   ObKvSchemaCacheGuard &schema_cache_guard,
                                                   ObNewRow &row,
                                                   const ObIArray<const ObITableEntity*> &entities,
                                                   ObIArray<ObTableOperationResult> &results)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> rowkey_column_ids;
  ObSEArray<uint64_t, 4> rowkey_idxs;
  ObObj *rowkey_cells = nullptr;
  rowkey_column_ids.set_attr(ObMemAttr(MTL_ID(), "rkColIds"));
  rowkey_idxs.set_attr(ObMemAttr(MTL_ID(), "rkIdxs"));
  if (OB_FAIL(schema_cache_guard.get_rowkey_column_ids(rowkey_column_ids))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else if (OB_ISNULL(rowkey_cells =
              static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * rowkey_column_ids.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cells buffer", K(ret), K(rowkey_column_ids.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); ++i) {
    int64_t idx = 0;
    if (OB_FAIL(schema_cache_guard.get_column_info_idx(rowkey_column_ids[i], idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column info idx", K(ret), K(rowkey_column_ids[i]));
    } else if (OB_FAIL(rowkey_idxs.push_back(idx))) {
      LOG_WARN("fail to push back rowkey idx", K(ret), K(rowkey_column_ids[i]));
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<int64_t, 4> indexs;
    if (OB_FAIL(get_result_index(row, entities, rowkey_idxs, rowkey_cells, indexs))) {
      LOG_WARN("fail to get reuslt indexs", K(ret), K(row), K(ops));
    } else {
      const ObTableEntity *requset_entity = nullptr;
      ObITableEntity *result_entity = nullptr;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < indexs.count(); ++idx) {
        int64_t index = indexs[idx];
        if (index >= results.count() || index >= entities.count()) {
          ret = OB_INDEX_OUT_OF_RANGE;
          LOG_WARN("result index is out of range",
              K(ret), K(index), K(results), K(entities.count()), K(row), K(ops));
        } else if (OB_FAIL(results.at(index).get_entity(result_entity))) {
          LOG_WARN("fail to get result entity", K(ret));
        } else if (OB_ISNULL(requset_entity = static_cast<const ObTableEntity *>(entities.at(index)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("entity is null", K(ret), K(index));
        } else if (OB_FAIL(ObTableApiUtil::construct_entity_from_row(allocator, &row,
                      schema_cache_guard, requset_entity->get_properties_names(), result_entity))) {
          LOG_WARN("fail to construct result entity from row", K(ret), K(row), KPC(requset_entity));
        }
        results.at(index).set_err(ret);
        results.at(index).set_type(ObTableOperationType::GET);
      }  // end for
    }
  }

  return ret;
}

int ObTableApiService::generate_scan_ranges_by_entities(const uint64_t table_id,
                                                        const ObIArray<const ObITableEntity*> &entities,
                                                        ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < entities.count(); ++i) {
    const ObITableEntity *entity = entities.at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is NULL", K(ret), K(i));
    } else {
      ObRowkey rowkey = entity->get_rowkey();
      ObNewRange range;
      if (OB_FAIL(range.build_range(table_id, rowkey))) {
        LOG_WARN("fail to build key range", K(ret), K(rowkey), K(i));
      } else if (OB_FAIL(ranges.push_back(range))) {
        LOG_WARN("fail to push back key range", K(ret), K(range), K(i));
      }
    }
  }

  return ret;
}


int ObTableApiService::get_result_index(const ObNewRow &row,
                                        const ObIArray<const ObITableEntity*> &entities,
                                        const ObIArray<uint64_t> &rowkey_ids,
                                        ObObj *rowkey_cells,
                                        ObIArray<int64_t> &indexs)
{
  int ret = OB_SUCCESS;

  for (int64_t pos = 0; pos < rowkey_ids.count(); ++pos) {
    rowkey_cells[pos] = row.get_cell(rowkey_ids.at(pos));
  }

  ObRowkey row_rowkey(rowkey_cells, rowkey_ids.count());
  for (int64_t i = 0; i < entities.count() && OB_SUCC(ret); i++) {
    const ObITableEntity *entity = entities.at(i);
    if (OB_ISNULL(entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity is NULL", K(ret), K(i));
    } else {
      ObRowkey entity_rowkey = entity->get_rowkey();
      bool equal = false;
      if (OB_FAIL(compare_rowkey(row_rowkey, entity_rowkey, equal))) {
        LOG_WARN("fail to compare rowkey", K(ret), K(row_rowkey), K(entity_rowkey));
      } else if (equal && OB_FAIL(indexs.push_back(i))) {
        LOG_WARN("fail to push_back index", K(row_rowkey), K(indexs), K(i));
      }
    }
  }

  return ret;
}

int ObTableApiService::compare_rowkey(ObRowkey &storage_rowkey, ObRowkey &request_rowkey, bool &is_equal) {
  int ret = OB_SUCCESS;
  if (storage_rowkey.get_obj_ptr() == request_rowkey.get_obj_ptr()) {
    is_equal = (storage_rowkey.get_obj_cnt() == request_rowkey.get_obj_cnt());
  } else if (storage_rowkey.get_obj_cnt() != request_rowkey.get_obj_cnt()){
    is_equal = false;
  } else {
    for (int64_t i = 0; i < request_rowkey.get_obj_cnt() && OB_SUCC(ret); i++) {
      ObObj &req_obj = request_rowkey.get_obj_ptr()[i];
      ObObj &storage_obj = storage_rowkey.get_obj_ptr()[i]; // its rowkey cells is allocated for temperally use so that we can modify
      if (ob_is_mysql_date_tc(req_obj.get_type()) && ob_is_date_tc(storage_obj.get_type())) {
        ObMySQLDate mdate = 0;
        if (OB_FAIL(ObTimeConverter::date_to_mdate(storage_obj.get_date(), mdate))) {
          LOG_WARN("fail to convert date to mysql date", K(ret), K(storage_obj));
        } else {
          storage_obj.set_mysql_date(mdate);
        }
      } else if (ob_is_mysql_datetime(req_obj.get_type()) && ob_is_datetime(storage_obj.get_type())) {
        ObMySQLDateTime mdatetime = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(storage_obj.get_datetime(), mdatetime))) {
          LOG_WARN("fail to convert datetime to mysql datetime", K(ret), K(storage_obj));
        } else {
          storage_obj.set_mysql_datetime(mdatetime);
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(storage_rowkey.equal(request_rowkey, is_equal))) {
        LOG_WARN("fail to compare rowkey", K(storage_rowkey), K(request_rowkey));
      }
    }
  }
  return ret;
}

int ObTableApiRowIterator::init(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_init()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ctx is not inited", K(ret));
  } else if (OB_FAIL(init_audit_helper_if_need(ctx))) {
    LOG_WARN("fail to init audit helper", K(ret));
  } else {
    ctx.set_need_dist_das(true);
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    if (OB_FAIL(ObTableApiService::get_spec<TABLE_API_EXEC_SCAN>(ctx, cache_guard_, spec))) {
      LOG_WARN("fail to get spec", K(ret));
    } else if (OB_FAIL(spec->create_executor(ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else if (OB_ISNULL(executor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("executor is NULL", K(ret));
    } else {
      scan_executor_ = static_cast<ObTableApiScanExecutor *>(executor);
    }
  }
  ret_code_ = ret;
  return ret;
}

int ObTableApiRowIterator::init_audit_helper_if_need(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableAuditCtx* audit_ctx = ctx.get_audit_ctx();
  if (OB_ISNULL(audit_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("audit_ctx is NULL", K(ret));
  } else if (audit_ctx->need_audit_) {
    void *buf = nullptr;
    if (ctx.get_opertion_type() == ObTableOperationType::GET) {
      if (ctx.get_batch_entities() == nullptr) { // single get
        stmt_type_ = StmtType::T_KV_GET;
        ObTableOperation op;
        op.set_type(ObTableOperationType::GET);
        op.set_entity(ctx.get_entity());
        buf = ctx.get_allocator().alloc(sizeof(ObTableAuditHelper<ObTableOperation>));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObTableAuditHelper<ObTableOperation>)));
        } else {
          get_audit_helper_ = new(buf)ObTableAuditHelper<ObTableOperation>(*ctx.get_credential(),
                                                                           op,
                                                                           ctx.get_table_name(),
                                                                           *ctx.get_sess_guard(),
                                                                           ctx.get_audit_ctx());
          get_audit_helper_->start_audit();
        }
      } else { // multi get
        stmt_type_ = StmtType::T_KV_MULTI_GET;
        ObTableAuditMultiOp multi_op(ObTableOperationType::GET, ctx.get_batch_entities());
        buf = ctx.get_allocator().alloc(sizeof(ObTableAuditHelper<ObTableAuditMultiOp>));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObTableAuditHelper<ObTableAuditMultiOp>)));
        } else {
          multi_get_audit_helper_ = new(buf)ObTableAuditHelper<ObTableAuditMultiOp>(*ctx.get_credential(),
                                                                                    multi_op,
                                                                                    ctx.get_table_name(),
                                                                                    *ctx.get_sess_guard(),
                                                                                    ctx.get_audit_ctx());
          multi_get_audit_helper_->start_audit();
        }
      }
    } else if (ctx.is_scan()) { // query
      stmt_type_ = StmtType::T_KV_QUERY;
      buf = ctx.get_allocator().alloc(sizeof(ObTableAuditHelper<ObTableQuery>));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObTableAuditHelper<ObTableQuery>)));
      } else {
        query_audit_helper_ = new(buf)ObTableAuditHelper<ObTableQuery>(*ctx.get_credential(),
                                                                       *ctx.get_query(),
                                                                       ctx.get_table_name(),
                                                                       *ctx.get_sess_guard(),
                                                                       ctx.get_audit_ctx());
        query_audit_helper_->start_audit();
      }
    }
  }
  return ret;
}

int ObTableApiRowIterator::open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan executor is null", K(ret));
  } else if (OB_FAIL(scan_executor_->open())) {
    LOG_WARN("fail to open scan executor", K(ret));
  } else {
    is_opened_ = true;
  }
  ret_code_ = ret;
  return ret;
}

int ObTableApiRowIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row))){
    if (ret != OB_ITER_END) {
      ret_code_ = ret;
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    row_count_++;
  }
  LOG_DEBUG("ObTableApiRowIterator::get_next_row", K(ret), KPC(row));
  return ret;
}

int ObTableApiRowIterator::get_next_row(ObNewRow *&row, common::ObIAllocator &allocator)
{
    int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row, allocator))){
    if (ret != OB_ITER_END) {
      ret_code_ = ret;
      LOG_WARN("fail to get next row", K(ret));
    }
  } else {
    row_count_++;
  }
  return ret;
}

int ObTableApiRowIterator::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_executor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_executor is NULL", K(ret));
  } else if (OB_FAIL(scan_executor_->rescan())) {
    LOG_WARN("fail to rescan", K(ret));
  }
  ret_code_ = ret;
  return ret;
}

int ObTableApiRowIterator::close()
{
  int ret = OB_SUCCESS;
  ObTableCtx *tb_ctx = get_tb_ctx();
  if (OB_FAIL(ObTableApiScanRowIterator::close())) {
    LOG_WARN("fail to clise row iter", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(tb_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tb_ctx is NULL", K(ret));
  } else if (OB_NOT_NULL(audit_helper_ptr_)) {
    ret_code_ = ret_code_ == OB_SUCCESS ? ret : ret_code_;
    transaction::ObTxReadSnapshot &snapshot = tb_ctx->get_exec_ctx().get_das_ctx().get_snapshot();
    switch(stmt_type_) {
      case StmtType::T_KV_GET:
        get_audit_helper_->end_audit(ret_code_, snapshot, stmt_type_, row_count_, true);
        get_audit_helper_->~ObTableAuditHelper<ObTableOperation>();
        get_audit_helper_ = nullptr;
        break;
      case StmtType::T_KV_MULTI_GET:
        multi_get_audit_helper_->end_audit(ret_code_, snapshot, stmt_type_, row_count_, true);
        multi_get_audit_helper_->~ObTableAuditHelper<ObTableAuditMultiOp>();
        multi_get_audit_helper_ = nullptr;
        break;
      case StmtType::T_KV_QUERY:
        query_audit_helper_->end_audit(ret_code_, snapshot, stmt_type_, row_count_, true);
        query_audit_helper_->~ObTableAuditHelper<ObTableQuery>();
        query_audit_helper_ = nullptr;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stmt type in row iter", K(ret), K(stmt_type_));
        break;
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
