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
#include "ob_table_executor.h"
#include "ob_table_executor_factory.h"
#include "ob_table_context.h"

using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

// NOTE: In fact, the table api executor/spec tree is just a doubly linked list
int ObTableApiSpec::create_executor(ObTableCtx &ctx, ObTableApiExecutor *&executor)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *cur_spec = this;
  ObTableApiExecutor *pre_executor = nullptr;
  ObTableApiExecutor *cur_executor = nullptr;
  ObTableApiExecutor *root_executor = nullptr;
  while(cur_spec != nullptr && OB_SUCC(ret)) {
    if (OB_FAIL(ObTableExecutorFactory::alloc_executor(ctx.get_allocator(),
                                                       ctx,
                                                       *cur_spec,
                                                       cur_executor))) {
      LOG_WARN("fail to alloc executor", K(ret));
    } else {
      if (root_executor == nullptr) {
        root_executor = cur_executor;
      }
      if (pre_executor != nullptr) {
        pre_executor->set_child(cur_executor);
        cur_executor->set_parent(pre_executor);
      }
      pre_executor = cur_executor;
      cur_spec = cur_spec->child_;
    }
  }

  if (OB_SUCC(ret)) {
    executor = root_executor;
  }

  return ret;
}

void ObTableApiExecutor::set_parent(ObTableApiExecutor *parent)
{
  parent_ = parent;
}

void ObTableApiExecutor::set_child(ObTableApiExecutor *child)
{
  child_ = child;
}

void ObTableApiExecutor::clear_evaluated_flag()
{
  if (tb_ctx_.get_table_schema()->has_generated_column() || tb_ctx_.is_inc_or_append()) {
    ObExprFrameInfo *expr_info = const_cast<ObExprFrameInfo *>(tb_ctx_.get_expr_frame_info());
    if (OB_NOT_NULL(expr_info)) {
      for (int64_t i = 0; i < expr_info->rt_exprs_.count(); i++) {
        const ObExpr &expr = expr_info->rt_exprs_.at(i);
        if (expr.type_ != T_FUN_SYS_AUTOINC_NEXTVAL) {
          expr_info->rt_exprs_.at(i).clear_evaluated_flag(eval_ctx_);
        }
      }
    }
  }
}

}  // namespace table
}  // namespace oceanbase