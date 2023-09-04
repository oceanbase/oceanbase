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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_EXECUTOR_H
#include "sql/engine/ob_exec_context.h"
#include "ob_table_context.h" // for ObTableCtx
namespace oceanbase
{
namespace table
{

class ObTableApiExecutor
{
public:
  ObTableApiExecutor(ObTableCtx &ctx)
      : tb_ctx_(ctx),
        exec_ctx_(ctx.get_exec_ctx()),
        eval_ctx_(exec_ctx_),
        parent_(nullptr),
        child_(nullptr),
        is_opened_(false)
  {
  }
  virtual ~ObTableApiExecutor()
  {
    if (OB_NOT_NULL(child_)) {
      child_->~ObTableApiExecutor();
      child_ = nullptr;
    }
  }
public:
  virtual int open() = 0;
  virtual int get_next_row() = 0;
  virtual int close() = 0;
  virtual void destroy() = 0;
public:
  void set_parent(ObTableApiExecutor *parent);
  void set_child(ObTableApiExecutor *child);
  const ObTableApiExecutor* get_parent() const { return parent_; }
  const ObTableApiExecutor* get_child() const { return child_; }
  OB_INLINE const ObTableCtx& get_table_ctx() const { return tb_ctx_; }
  OB_INLINE sql::ObEvalCtx &get_eval_ctx() { return eval_ctx_; }
  virtual void clear_evaluated_flag();

protected:
  ObTableCtx &tb_ctx_;
  sql::ObExecContext &exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  ObTableApiExecutor *parent_;
  ObTableApiExecutor *child_;
  bool is_opened_;
};

class ObTableApiSpec
{
public:
  ObTableApiSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : alloc_(alloc),
        type_(type),
        parent_(nullptr),
        child_(nullptr),
        expr_frame_info_(nullptr)
  {
  }
  virtual ~ObTableApiSpec() {};
public:
  // getter
  OB_INLINE const ObTableApiSpec* get_parent() const { return parent_; }
  OB_INLINE const ObTableApiSpec* get_child() const { return child_; }
  OB_INLINE ObTableExecutorType get_type() const { return type_; }
  OB_INLINE sql::ObExprFrameInfo* get_expr_frame_info() const { return expr_frame_info_; }
  // setter
  OB_INLINE void set_parent(ObTableApiSpec *parent) { parent_ = parent; }
  OB_INLINE void set_child(ObTableApiSpec *child) { child_ = child; }
  OB_INLINE void set_expr_frame_info(sql::ObExprFrameInfo *info) { expr_frame_info_ = info; }
public:
  int create_executor(ObTableCtx &ctx, ObTableApiExecutor *&executor);
  template <typename T>
  void destroy_executor(T *executor)
  {
    if (OB_NOT_NULL(executor)) {
      executor->~T();
    }
  }
protected:
  common::ObIAllocator &alloc_;
  ObTableExecutorType type_;
  ObTableApiSpec *parent_;
  ObTableApiSpec *child_;
  sql::ObExprFrameInfo *expr_frame_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSpec);
};

struct ObTableApiDmlBaseCtDef
{
public:
  sql::ExprFixedArray old_row_;
  sql::ExprFixedArray new_row_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_EXECUTOR_H */