/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/optimizer_plan_rewriter/ob_set_parent_visitor.h"

namespace oceanbase
{
namespace sql
{

int ObSetParentVisitor::visit_node(ObLogicalOperator *plannode, Void* context, Void*& result)
{
  int ret = OB_SUCCESS;
  UNUSED(context);
  UNUSED(result);

  if (OB_ISNULL(plannode)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // 先为每个子节点设置parent指针
    for (int64_t i = 0; OB_SUCC(ret) && i < plannode->get_num_of_child(); ++i) {
      ObLogicalOperator* child = plannode->get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(child));
      } else {
        child->set_parent(plannode);
      }
    }
    // 然后访问所有子节点（递归设置子树的parent）
    if (OB_SUCC(ret)) {
      ret = visit_children_with(plannode, context, result);
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
