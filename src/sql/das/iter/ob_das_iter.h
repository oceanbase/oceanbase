/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_H_
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/das/ob_das_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASIter;

enum ObDASIterType : uint32_t
{
  DAS_ITER_INVALID = 0,
  DAS_ITER_SCAN,
  DAS_ITER_MERGE,
  DAS_ITER_GROUP_FOLD,
  DAS_ITER_LOOKUP,
  // append DASIterType before me
  DAS_ITER_MAX
};

enum MergeType : uint32_t {
  SEQUENTIAL_MERGE = 0,
  SORT_MERGE
};

struct ObDASIterParam
{
public:
  ObDASIterParam()
    : type_(ObDASIterType::DAS_ITER_INVALID),
      max_size_(0),
      eval_ctx_(nullptr),
      exec_ctx_(nullptr),
      output_(nullptr),
      group_id_expr_(nullptr),
      child_(nullptr),
      right_(nullptr)
  {}

  virtual ~ObDASIterParam() {}

  void assgin(const ObDASIterParam &param)
  {
    type_ = param.type_;
    max_size_ = param.max_size_;
    eval_ctx_ = param.eval_ctx_;
    exec_ctx_ = param.exec_ctx_;
    output_ = param.output_;
    group_id_expr_ = param.group_id_expr_;
    child_ = param.child_;
    right_ = param.right_;
  }

  virtual bool is_valid() const
  {
    return eval_ctx_ != nullptr && exec_ctx_ != nullptr && output_ != nullptr;
  }

  ObDASIterType type_;
  int64_t max_size_;
  ObEvalCtx *eval_ctx_;
  ObExecContext *exec_ctx_;
  const ObIArray<ObExpr*> *output_;
  const ObExpr *group_id_expr_;
  ObDASIter *child_;
  ObDASIter *right_;
  TO_STRING_KV(K_(type), K_(max_size), K_(eval_ctx), K_(exec_ctx), KPC_(output), K_(group_id_expr),
      K_(child), K_(right));
};

class ObDASIter
{
public:
  ObDASIter()
    : type_(ObDASIterType::DAS_ITER_INVALID),
      max_size_(0),
      eval_ctx_(nullptr),
      exec_ctx_(nullptr),
      output_(nullptr),
      group_id_expr_(nullptr),
      child_(nullptr),
      right_(nullptr),
      inited_(false)
  {}
  virtual ~ObDASIter() { release(); }

  VIRTUAL_TO_STRING_KV(K_(type), K_(max_size), K_(eval_ctx), K_(exec_ctx), K_(output),
      K_(group_id_expr), K_(child), K_(right), K_(inited));

  void set_type(ObDASIterType type) { type_ = type; }
  ObDASIterType get_type() const { return type_; }

  // The state of ObDASMergeIter may change many times during execution, e.g., the merge_type
  // changing from SEQUENTIAL_MERGE to SORT_MERGE, or the creation of a new batch of DAS tasks.
  // Therefore, the status needs to be explicitly set before calling get next rows.
  virtual int set_merge_status(MergeType merge_type);

  int init(ObDASIterParam &param);
  OB_INLINE bool is_inited() const { return inited_; }
  // Make the iter go back to the state after calling init().
  int reuse();
  // Make the iter go back to the state before calling init().
  int release();

  // get_next_row(s) should be called after init().
  int get_next_row();
  int get_next_rows(int64_t &count, int64_t capacity);


protected:
  virtual int inner_init(ObDASIterParam &param) = 0;
  virtual int inner_reuse() = 0;
  virtual int inner_release() = 0;
  virtual int inner_get_next_row() = 0;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) = 0;

  ObDASIterType type_;
  int64_t max_size_;
  ObEvalCtx *eval_ctx_;
  ObExecContext *exec_ctx_;
  const ObIArray<ObExpr*> *output_;
  const ObExpr *group_id_expr_;
  ObDASIter *child_;
  ObDASIter *right_;

private:
  bool inited_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_ITER_H_ */
