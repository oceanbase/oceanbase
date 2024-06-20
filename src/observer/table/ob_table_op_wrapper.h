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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_OP_WARPPER_H_
#define OCEANBASE_OBSERVER_OB_TABLE_OP_WARPPER_H_
#include "ob_table_context.h"
#include "ob_table_executor.h"
#include "ob_table_delete_executor.h"
#include "ob_table_cache.h"
#include "ob_table_cg_service.h"

namespace oceanbase
{
namespace table
{

class ObTableOpWrapper
{
public:
  // dml操作模板函数
  template<int TYPE>
  static int process_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result)
  {
    int ret = OB_SUCCESS;
    ObTableApiSpec *spec = nullptr;
    observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
    ObTableApiCacheGuard cache_guard;
    if (OB_FAIL(get_or_create_spec<TYPE>(tb_ctx, cache_guard, spec))) {
      SERVER_LOG(WARN, "fail to get or create spec", K(ret), K(TYPE));
    } else if (OB_FAIL(process_op_with_spec(tb_ctx, spec, op_result))) {
      SERVER_LOG(WARN, "fail to process op with spec", K(ret), K(TYPE));
    } else {
      tb_ctx.set_expr_info(nullptr);
    }

    return ret;
  }
  // 生成/匹配计划
  template<int TYPE>
  static int get_or_create_spec(ObTableCtx &tb_ctx, ObTableApiCacheGuard &cache_guard, ObTableApiSpec *&spec)
  {
    int ret = OB_SUCCESS;
    ObExprFrameInfo *expr_frame_info;
    if (OB_FAIL(cache_guard.init(&tb_ctx))) {
      SERVER_LOG(WARN, "fail to init cache guard", K(ret));
    } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx, expr_frame_info))) {
      SERVER_LOG(WARN, "fail to get expr frame info", K(ret));
    } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(tb_ctx, *expr_frame_info))) {
      SERVER_LOG(WARN, "fail to alloc exprs memory", K(ret));
    } else if (FALSE_IT(tb_ctx.set_expr_info(expr_frame_info))) {
    } else if (FALSE_IT(tb_ctx.set_init_flag(true))) {
    } else if (OB_FAIL(cache_guard.get_spec<TYPE>(&tb_ctx, spec))) {
      SERVER_LOG(WARN, "fail to get spec from cache", K(ret), K(TYPE));
    }
    return ret;
  }
  // 根据执行计划驱动executor执行
  static int process_op_with_spec(ObTableCtx &tb_ctx, ObTableApiSpec *spec, ObTableOperationResult &op_result);
  // get特有的逻辑，单独处理
  static int process_get(ObTableCtx &tb_ctx, ObNewRow *&row);
  static int process_get_with_spec(ObTableCtx &tb_ctx, ObTableApiSpec *spec, ObNewRow *&row);
  static int get_insert_spec(ObTableCtx &tb_ctx, ObTableApiCacheGuard &cache_guard, ObTableApiSpec *&spec);
  static int get_insert_up_spec(ObTableCtx &tb_ctx, ObTableApiCacheGuard &cache_guard, ObTableApiSpec *&spec);
  static int process_insert_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result);
  static int process_insert_up_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result);
private:
  static int process_affected_entity(ObTableCtx &tb_ctx,
                                     const ObTableApiSpec &spec,
                                     ObTableApiExecutor &executor,
                                     ObTableOperationResult &op_result);
};

class ObTableApiUtil
{
public:
  // schema序的ObNewRow组装成ObTableEntity
  static int construct_entity_from_row(ObIAllocator &allocator,
                                       ObNewRow *row,
                                       const ObTableSchema *table_schema,
                                       const ObIArray<ObString> &cnames,
                                       ObITableEntity *entity);
  static int expand_all_columns(const ObTableSchema *table_schema,
                                ObIArray<ObString> &cnames);
  static void replace_ret_code(int &ret)
  {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret
        || OB_BAD_NULL_ERROR == ret
        || OB_OBJ_TYPE_ERROR == ret
        || OB_KV_COLUMN_TYPE_NOT_MATCH == ret
        || OB_ERR_COLLATION_MISMATCH == ret
        || OB_KV_COLLATION_MISMATCH == ret
        || OB_ERR_DATA_TOO_LONG == ret
        || OB_DATA_OUT_OF_RANGE == ret) {
      ret = OB_SUCCESS;
    }
  }
};

class ObHTableDeleteExecutor
{
public:
  ObHTableDeleteExecutor(ObTableCtx& tb_ctx, ObTableApiDeleteExecutor *executor)
      : tb_ctx_(tb_ctx),
        executor_(executor),
        affected_rows_(0) {}
  virtual ~ObHTableDeleteExecutor() {}

public:
  int open();
  int get_next_row();
  int close();
  int64_t get_affected_rows() { return affected_rows_; }

private:
  int build_range(ObTableQuery &query);
  int query_and_delete(const ObTableQuery &query);
  int delete_rows(ObTableQueryResult &result);
  int generate_filter(const ObITableEntity &entity,
                      ObHTableFilter &filter);

private:
  ObTableCtx &tb_ctx_;
  ObTableApiDeleteExecutor *executor_;
  int64_t affected_rows_;
  ObObj pk_objs_start_[3];
  ObObj pk_objs_end_[3];
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_OP_WRAPPER_H_ */