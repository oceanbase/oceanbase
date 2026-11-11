/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_relation_group.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

int ObGroupRelation::assign(const ObGroupRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_exprs_.assign(other.group_exprs_))) {
    LOG_WARN("failed to assign group_exprs", K(ret));
  } else if (OB_FAIL(rollup_exprs_.assign(other.rollup_exprs_))) {
    LOG_WARN("failed to assign rollup_exprs", K(ret));
  } else if (OB_FAIL(having_exprs_.assign(other.having_exprs_))) {
    LOG_WARN("failed to assign having_exprs", K(ret));
  } else if (OB_FAIL(agg_items_.assign(other.agg_items_))) {
    LOG_WARN("failed to assign agg_items", K(ret));
  } else if (OB_FAIL(rollup_directions_.assign(other.rollup_directions_))) {
    LOG_WARN("failed to assign rollup_directions", K(ret));
  } else if (OB_FAIL(grouping_sets_items_.assign(other.grouping_sets_items_))) {
    LOG_WARN("failed to assign grouping_sets_items", K(ret));
  } else if (OB_FAIL(rollup_items_.assign(other.rollup_items_))) {
    LOG_WARN("failed to assign rollup_items", K(ret));
  } else if (OB_FAIL(cube_items_.assign(other.cube_items_))) {
    LOG_WARN("failed to assign cube_items", K(ret));
  }
  return ret;
}

int ObGroupRelation::deep_copy(ObRawExprCopier &expr_copier,
                               const ObGroupRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_copier.copy(other.group_exprs_, group_exprs_))) {
    LOG_WARN("failed to copy group exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.rollup_exprs_, rollup_exprs_))) {
    LOG_WARN("failed to copy rollup exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.having_exprs_, having_exprs_))) {
    LOG_WARN("failed to copy having exprs", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.agg_items_, agg_items_))) {
    LOG_WARN("failed to copy aggregate items", K(ret));
  } else if (OB_FAIL(rollup_directions_.assign(other.rollup_directions_))) {
    LOG_WARN("failed to assign rollup directions", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObGroupingSetsItem>(expr_copier,
                                                                other.grouping_sets_items_,
                                                                grouping_sets_items_))) {
    LOG_WARN("failed to copy grouping sets items", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObRollupItem>(expr_copier,
                                                          other.rollup_items_,
                                                          rollup_items_))) {
    LOG_WARN("failed to copy rollup items", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObCubeItem>(expr_copier,
                                                        other.cube_items_,
                                                        cube_items_))) {
    LOG_WARN("failed to copy cube items", K(ret));
  }
  return ret;
}

bool ObGroupRelation::is_empty() const
{
  return group_exprs_.empty()
      && rollup_exprs_.empty()
      && having_exprs_.empty()
      && agg_items_.empty()
      && rollup_directions_.empty()
      && grouping_sets_items_.empty()
      && rollup_items_.empty()
      && cube_items_.empty();
}

} // namespace sql
} // namespace oceanbase
