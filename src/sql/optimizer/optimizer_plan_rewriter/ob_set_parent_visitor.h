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

#ifndef OCEANBASE_SQL_OB_SET_PARENT_VISITOR_H
#define OCEANBASE_SQL_OB_SET_PARENT_VISITOR_H

#include "sql/optimizer/optimizer_plan_rewriter/ob_plan_visitor.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief Visitor用于设置计划树中每个节点的parent指针
 *
 * 该visitor遍历整个计划树，为每个子节点设置其parent指针指向其父节点。
 * 这通常在计划树构建完成后使用，用于建立完整的父子关系。
 */
class ObSetParentVisitor : public SimplePlanVisitor<Void, Void>
{
public:
  ObSetParentVisitor() {}
  virtual ~ObSetParentVisitor() {}

  /**
   * @brief 访问计划节点，设置其子节点的parent指针
   *
   * @param plannode 当前访问的计划节点
   * @param context 上下文（未使用）
   * @param result 结果（未使用）
   * @return int 返回码
   */
  virtual int visit_node(ObLogicalOperator *plannode, Void* context, Void*& result) override;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_SET_PARENT_VISITOR_H
