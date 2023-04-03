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

#ifndef _OB_TRANSFORM_MIX_MAX_H
#define _OB_TRANSFORM_MIX_MAX_H

#include "sql/rewrite/ob_transform_rule.h"
#include "objit/common/ob_item_type.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T>
class ObIArray;
}//common
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
}//oceanbase

namespace oceanbase
{
namespace sql
{
class ObSelectStmt;
class ObDMLStmt;
class ObStmt;
class ObRawExpr;
class ObOpRawExpr;
class ObColumnRefRawExpr;
class ObAggFunRawExpr;

// 1. 针对列的 min/max 聚合
//select中含有max和min时，尝试消除max和min，添加order by 和limit
//例如：
//  原始语句: select min(c2) from t1;
//  转换后：select min(c2) from (select c2 from t2 where c2 is not null order by c2 limit 1) as t;
//
//转换原则：
//  a. max或min中是表的某一列；且该列某个索引前缀中的第一个非常量的列
//  b. select语句中不含有limit和group by语句
//  c. 只处理单表情况
//
class ObTransformMinMax : public ObTransformRule
{
public:
  explicit ObTransformMinMax(ObTransformerCtx *ctx);
  virtual ~ObTransformMinMax();
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:

  int check_transform_validity(ObSelectStmt *stmt,
                               ObAggFunRawExpr *&aggr_expr,
                               bool &is_valid);

  int do_transform(ObSelectStmt *select_stmt, ObAggFunRawExpr *aggr_expr);

  int is_valid_index_column(const ObSelectStmt *stmt,
                            const ObRawExpr *expr,
                            bool &is_expected_index);

  int is_valid_having(const ObSelectStmt *stmt,
                      const ObAggFunRawExpr *column_aggr_expr,
                      bool &is_expected);

  int is_valid_aggr_expr(const ObSelectStmt *stmt,
                         const ObRawExpr *expr,
                         ObAggFunRawExpr *&column_aggr_expr,
                         bool &is_valid);

  int find_unexpected_having_expr(const ObAggFunRawExpr *aggr_expr,
                                  const ObRawExpr *cur_expr,
                                  bool &is_unexpected);

  int set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_expr);

  int set_child_order_item(ObSelectStmt *stmt, ObRawExpr *aggr_expr);

  /**
   * @brief: 检查select item中是否至多包含一个非常数列，并存到is_valid中，
   * 改写要求select item只有一项聚集函数，其它的列只能是常量。
   *
   * @param[in]  stmt          待检查的表达式
   * @param[out] is_valid      是否至多包含一个非常数列
   *
   * 副作用：
   * 将遇到的第一个非常数select item 记录在 idx_aggr_column_中，如果is_valid
   * 为true，最终会记录唯一的聚集函数的select item中的索引，如果全都为常量，则置为0
   *
   */
  int is_valid_select_list(const ObSelectStmt &stmt, const ObRawExpr *&aggr_expr, bool &is_valid);
  DISALLOW_COPY_AND_ASSIGN(ObTransformMinMax);
};

} //namespace sql
} //namespace oceanbase
#endif
