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

namespace oceanbase
{
namespace table
{

class ObTableOpWrapper
{
public:
  // dml操作模板函数
  template<int TYPE>
  static int process_op(ObTableCtx &tb_ctx, ObTableOperationResult &op_result);
  // 生成/匹配计划
  template<int TYPE>
  static int get_or_create_spec(ObTableCtx &tb_ctx, ObTableApiCacheGuard &cache_guard, ObTableApiSpec *&spec);
  // 根据执行计划驱动executor执行
  static int process_op_with_spec(ObTableCtx &tb_ctx, ObTableApiSpec *spec, ObTableOperationResult &op_result);
  // get特有的逻辑，单独处理
  static int process_get(ObTableCtx &tb_ctx, ObNewRow *&row);
  static int process_get_with_spec(ObTableCtx &tb_ctx, ObTableApiSpec *spec, ObNewRow *&row);
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
  static int construct_entity_from_row(ObNewRow *row,
                                      const ObTableSchema *table_schema,
                                      const ObIArray<ObString> &cnames,
                                      ObITableEntity *entity);
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