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

#include "lib/container/ob_iarray.h"
#include "observer/table/ob_table_context.h"
#include "observer/table/ob_table_executor.h"
#include "observer/table/ob_table_cache.h"
#include "observer/table/ob_table_cg_service.h"
#include "observer/ob_req_time_service.h"
#include "sql/resolver/ob_stmt_type.h"

#ifndef OCEANBASE_OBSERVER_OB_TABLE_API_SERVICE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_API_SERVICE_H_

namespace oceanbase
{
namespace table
{
class ObTableApiRowIterator;

class ObTableApiService
{
friend class ObTableApiRowIterator;
// single operation interface
public:
  static int insert(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int update(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int insert_or_update(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int put(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int get(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int del(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  static int replace(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  // may be deprecated and implemented by model layer
  // static int increment(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);
  // static int append(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result);

// batch operation interface
public:
  static int multi_insert(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_update(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_insert_or_update(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_put(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_get(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_delete(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int multi_replace(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results = nullptr);
  static int construct_entities_from_row(ObIAllocator &allocator,
                                         ObKvSchemaCacheGuard &schema_cache_guard,
                                         ObNewRow &row,
                                         const ObIArray<ObITableEntity*> &entities,
                                         ObIArray<ObTableOperationResult> &results);
  static int get_result_index(const ObNewRow &row,
                              const ObIArray<ObITableEntity*> &entities,
                              const ObIArray<uint64_t> &rowkey_ids,
                              ObObj *rowkey_cells,
                              ObIArray<int64_t> &indexs);
  static int generate_scan_ranges_by_entities(const uint64_t table_id,
                                              const ObIArray<ObITableEntity*> &entities,
                                              ObIArray<common::ObNewRange> &ranges);
  static int compare_rowkey(ObRowkey &storage_rowkey, ObRowkey &request_rowkey, bool &is_equal);
// query interface
public:
  static int query(ObTableCtx &ctx, ObTableApiRowIterator &iter);

// for service inner use
private:
  static int check_batch_args(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results);
  static int adjust_entities(ObTableCtx &ctx, const common::ObIArray<ObITableEntity *> &entities);
  static bool need_calc_tablet_id(const common::ObIArray<ObTabletID> *tablet_ids, const common::ObIArray<ObITableEntity *> &entities);
  static bool need_calc_tablet_id(const common::ObIArray<ObTabletID> *tablet_ids);
  static int calc_tablet_ids(ObTableCtx &ctx, const ObIArray<ObNewRange> &ranges, ObIArray<ObTabletID> *&tablet_ids);
  static int process_executor(ObTableCtx &ctx, ObTableApiExecutor *executor, ObTableOperationResult &result);
  static int process_dml_result(ObTableCtx &ctx, ObTableApiExecutor &executor, ObTableOperationResult &result);
  static bool is_same_plan(ObTableOperationType::Type op_type, ObITableEntity &src_entity, ObITableEntity &dest_entity);
  static int init_tablet_ids_array(ObTableCtx &ctx, const int64_t array_size, ObIArray<ObTabletID> *&tablet_ids);
private:
  template<int SPEC_TYPE>
  static int get_spec(ObTableCtx &ctx, ObTableApiCacheGuard &cache_guard, ObTableApiSpec *&spec)
  {
    int ret = OB_SUCCESS;
    ObExprFrameInfo *expr_frame_info = nullptr;
    if (!ctx.is_init()) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "ctx is not inited", K(ret));
    } else if (OB_FAIL(cache_guard.init(&ctx))) {
      SERVER_LOG(WARN, "fail to init cache guard", K(ret));
    } else if (OB_FAIL(cache_guard.get_expr_info(&ctx, expr_frame_info))) {
      SERVER_LOG(WARN, "fail to get expr frame info", K(ret));
    } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(ctx, *expr_frame_info))) {
      SERVER_LOG(WARN, "fail to alloc exprs memory", K(ret));
    } else if (FALSE_IT(ctx.set_expr_info(expr_frame_info))) {
    } else if (OB_FAIL(cache_guard.get_spec<SPEC_TYPE>(&ctx, spec))) {
      SERVER_LOG(WARN, "fail to get spec from cache", K(ret), K(SPEC_TYPE));
    } else if (OB_ISNULL(spec)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ObTableApiSpec is NULL", K(ret));
    }
    return ret;
  }

  template<int SPEC_TYPE, ObTableOperationType::Type OP_TYPE, sql::stmt::StmtType STMT_TYPE>
  static int process_single_dml_op(ObTableCtx &ctx, const ObITableEntity &entity, ObTableOperationResult &result)
  {
    int ret = OB_SUCCESS;
    ObTableOperation op;
    op.set_type(OP_TYPE);
    op.set_entity(entity);
    OB_TABLE_START_AUDIT(*ctx.get_credential(),
                         *ctx.get_sess_guard(),
                         ctx.get_table_name(),
                         ctx.get_audit_ctx(), op);
    if (!ctx.is_init()) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "ctx is not inited", K(ret));
    } else {
      ObTableApiCacheGuard cache_guard;
      ObTableApiSpec *spec = nullptr;
      ObTableApiExecutor *executor = nullptr;
      ctx.set_operation_type(OP_TYPE);
      ctx.set_entity(&entity);
      ctx.set_need_dist_das(true);
      // set all op to ET_DYNAMIC temperally, because if entity_type == ET_HKV, if will modify timestamp
      ctx.set_entity_type(ObTableEntityType::ET_DYNAMIC);
      if (OB_FAIL(ctx.adjust_entity())) {
        SERVER_LOG(WARN, "fail to adjust entity", K(ret), K(entity));
      } else if (OB_FAIL(get_spec<SPEC_TYPE>(ctx, cache_guard, spec))) {
        SERVER_LOG(WARN, "fail to get spec", K(ret), K(SPEC_TYPE), K(OP_TYPE));
      } else if (OB_FAIL(spec->create_executor(ctx, executor))) {
        SERVER_LOG(WARN, "fail to create executor", K(ret), K(SPEC_TYPE), K(OP_TYPE));
      } else if (OB_FAIL(process_executor(ctx, executor, result))) {
        SERVER_LOG(WARN, "fail to process executor", K(ret), K(SPEC_TYPE), K(OP_TYPE));
      }
      if (OB_NOT_NULL(executor)) {
        spec->destroy_executor(executor);
        executor = nullptr;
      }
      // cache is release, avoid others to visit it
      ctx.set_expr_info(nullptr);
    }
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                       stmt_type, STMT_TYPE);
    return ret;
  }

  template<int SPEC_TYPE, ObTableOperationType::Type OP_TYPE, sql::stmt::StmtType STMT_TYPE>
  static int process_batch_in_executor(ObTableCtx &ctx, const ObIArray<ObITableEntity*> &entities, ObIArray<ObTableOperationResult> *results)
  {
    int ret = OB_SUCCESS;
    ObTableAuditMultiOp multi_op(OP_TYPE, &entities);
    OB_TABLE_START_AUDIT(*ctx.get_credential(),
                         *ctx.get_sess_guard(),
                         ctx.get_table_name(),
                         ctx.get_audit_ctx(), multi_op);
    ObTableApiCacheGuard cache_guard;
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    const ObIArray<ObTabletID> *tablet_ids = nullptr;
    ctx.set_batch_entities(&entities);
    ctx.set_operation_type(OP_TYPE);
    ctx.set_need_dist_das(true);
    // set all op to ET_DYNAMIC temperally, because if entity_type == ET_HKV, if will modify timestamp
    ctx.set_entity_type(ObTableEntityType::ET_DYNAMIC);
    if (OB_FAIL(check_batch_args(ctx, entities, results))) {
      SERVER_LOG(WARN, "fail to check batch args", K(ret));
    } else if (OB_FAIL(get_spec<SPEC_TYPE>(ctx, cache_guard, spec))) {
      SERVER_LOG(WARN, "fail to get spec", K(ret), K(SPEC_TYPE));
    } else if (OB_FAIL(spec->create_executor(ctx, executor))) {
      SERVER_LOG(WARN, "fail to create executor", K(ret), K(SPEC_TYPE));
    } else if (OB_ISNULL(executor)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,"executor is NULL", K(ret));
    } else if (OB_FAIL(executor->open())) {
      SERVER_LOG(WARN,"fail to open executor", K(ret));
    } else if (OB_FAIL(executor->get_next_row())) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN,"fail to get next row", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(results)) {
      for (int64_t i = 0; i < results->count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(process_dml_result(ctx, *executor, results->at(i)))) {
          SERVER_LOG(WARN, "fail to process result", K(ret), K(i));
        }
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(executor) && OB_TMP_FAIL(executor->close())) {
      SERVER_LOG(WARN,"fail to close executor", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }

    if (OB_NOT_NULL(executor)) {
      spec->destroy_executor(executor);
      executor = nullptr;
    }
    // cache is release, avoid others to visit it
    ctx.set_expr_info(nullptr);
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                       stmt_type, STMT_TYPE);
    return ret;
  }

  template<int SPEC_TYPE, ObTableOperationType::Type OP_TYPE, sql::stmt::StmtType STMT_TYPE>
  static int process_batch_with_same_spec(ObTableCtx &ctx,
                                          const common::ObIArray<ObITableEntity*> &entities,
                                          common::ObIArray<ObTableOperationResult> *results)
  {
    int ret = OB_SUCCESS;
    ObTableAuditMultiOp multi_op(OP_TYPE, &entities);
    OB_TABLE_START_AUDIT(*ctx.get_credential(),
                         *ctx.get_sess_guard(),
                         ctx.get_table_name(),
                         ctx.get_audit_ctx(), multi_op);
    ObTableApiCacheGuard cache_guard;
    ObTableApiSpec *spec = nullptr;
    ctx.set_batch_entities(&entities);
    ctx.set_operation_type(OP_TYPE);
    ctx.set_need_dist_das(true);
    // set all op to ET_DYNAMIC temperally, because if entity_type == ET_HKV, if will modify timestamp
    ctx.set_entity_type(ObTableEntityType::ET_DYNAMIC);
    if (OB_FAIL(check_batch_args(ctx, entities, results))) {
      SERVER_LOG(WARN,"fail to check batch args", K(ret));
    } else if (OB_FAIL(get_spec<SPEC_TYPE>(ctx, cache_guard, spec))) {
      SERVER_LOG(WARN,"fail to get executor", K(ret));
    } else {
      const ObIArray<ObTabletID> *tablet_ids = ctx.get_batch_tablet_ids();
      for (int64_t i = 0; i < entities.count() && OB_SUCC(ret); i++) {
        ObTableApiExecutor *executor = nullptr;
        // tablet_ids has been checked valid in check_batch_args
        ObTabletID tablet_id = tablet_ids->count() == 1 ? tablet_ids->at(0) : tablet_ids->at(i);
        ctx.set_tablet_id(tablet_id);
        ctx.set_entity(entities.at(i));
        if (OB_FAIL(spec->create_executor(ctx, executor))) {
          SERVER_LOG(WARN,"fail to create executor", K(ret));
        } else if (OB_FAIL(executor->open())) {
          SERVER_LOG(WARN,"fail to open", K(ret));
        } else if (OB_FAIL(executor->get_next_row())) {
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
          } else {
            SERVER_LOG(WARN,"fail to execute", K(ret), K(ctx));
          }
        }

        if (OB_SUCC(ret) && OB_NOT_NULL(results)) {
          if (OB_FAIL(process_dml_result(ctx, *executor, results->at(i)))) {
            SERVER_LOG(WARN,"fail to process result", K(ret), K(i));
          }
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_NOT_NULL(executor) && OB_TMP_FAIL(executor->close())) {
          SERVER_LOG(WARN,"fail to close executor", K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }

        if (OB_NOT_NULL(executor)) {
          spec->destroy_executor(executor);
          executor = nullptr;
        }
      } // end for
    }
    // cache is release, avoid others to visit it
    ctx.set_expr_info(nullptr);
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, ctx.get_exec_ctx().get_das_ctx().get_snapshot(),
                       stmt_type, STMT_TYPE);
    return ret;
  }
};

class ObTableApiRowIterator: protected ObTableApiScanRowIterator
{
public:
  ObTableApiRowIterator()
    : ObTableApiScanRowIterator(),
      audit_helper_ptr_(nullptr),
      ret_code_(OB_SUCCESS),
      stmt_type_(sql::stmt::StmtType::T_KV_QUERY),
      row_count_(0),
      filter_(nullptr)
  {}
  virtual ~ObTableApiRowIterator() {
    if (OB_NOT_NULL(scan_executor_)) {
      scan_executor_->~ObTableApiScanExecutor();
    }
  };
public:
  OB_INLINE ObTableCtx* get_tb_ctx()
  {
    return OB_NOT_NULL(scan_executor_) ? &scan_executor_->get_table_ctx() : nullptr;
  }
  virtual int init(ObTableCtx &ctx);
  virtual int open();
  virtual int get_next_row(ObNewRow *&row);
  virtual int get_next_row(ObNewRow *&row, common::ObIAllocator &allocator);
  virtual int rescan();
  virtual int close();
private:
  int init_audit_helper_if_need(ObTableCtx &ctx);
private:
  ObTableApiCacheGuard cache_guard_;
  union {
    void *audit_helper_ptr_;
    ObTableAuditHelper<ObTableQuery> *query_audit_helper_;
    ObTableAuditHelper<ObTableOperation> *get_audit_helper_;
    ObTableAuditHelper<ObTableAuditMultiOp> *multi_get_audit_helper_;
  };
  int ret_code_;
  sql::stmt::StmtType stmt_type_;
  int64_t row_count_;
  hfilter::Filter *filter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiRowIterator);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_API_SERVICE_H_ */
