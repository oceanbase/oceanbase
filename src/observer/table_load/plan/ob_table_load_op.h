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

#pragma once

#include "lib/container/ob_array.h"
#include "observer/table_load/plan/ob_table_load_plan.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadOp
{
protected:
  // 在plan下创建独立op
  ObTableLoadOp(ObTableLoadPlan *plan);
  // 在op下创建嵌套op
  ObTableLoadOp(ObTableLoadOp *parent);

public:
  virtual ~ObTableLoadOp() = default;
  ObTableLoadPlan *get_plan() const { return plan_; }
  ObTableLoadOpType::Type get_op_type() const { return op_type_; }
  ObTableLoadOp *get_parent() const { return parent_; }
  const ObIArray<ObTableLoadOp *> &get_childs() const { return childs_; }

  int64_t simple_to_string(char *buf, int64_t buf_len, const bool show_childs = true) const;
  VIRTUAL_TO_STRING_KV(KP_(plan), "op_type", ObTableLoadOpType::get_type_string(op_type_),
                       KP_(parent), K_(childs));

protected:
  ObTableLoadPlan *plan_;
  ObTableLoadOpType::Type op_type_;
  // 嵌套op才会有parent和childs
  ObTableLoadOp *parent_;
  ObArray<ObTableLoadOp *> childs_;

public:
  int64_t start_time_;
  int64_t end_time_;
};

template <typename Derived>
class ObTableLoadOpFactory : public ObTableLoadOp
{
protected:
  template <typename... Args>
  ObTableLoadOpFactory(Args &&... args) : ObTableLoadOp(std::forward<Args>(args)...)
  {
  }

public:
  template <typename T, typename... Args>
  int alloc_op(T *&op, Args &&... args)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(plan_->alloc_op(op, static_cast<Derived *>(this), args...))) {
      SERVER_LOG(WARN, "fail to alloc op", KR(ret));
    } else if (OB_FAIL(childs_.push_back(op))) {
      SERVER_LOG(WARN, "fail to push back", KR(ret));
    }
    return ret;
  }
};

class ObTableLoadFinishOp final : public ObTableLoadOp
{
public:
  ObTableLoadFinishOp(ObTableLoadPlan *plan) : ObTableLoadOp(plan)
  {
    op_type_ = ObTableLoadOpType::FINISH_OP;
  }
};

} // namespace observer
} // namespace oceanbase
