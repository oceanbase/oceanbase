/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_relation_set.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

const char *ObSetRelation::set_operator_str(ObSetRelation::SetOperator op)
{
  static const char *set_operator_name[SET_OP_NUM + 1] =
  {
    "none",
    "union",
    "intersect",
    "except",
    "recursive",
    "unknown",
  };
  static const char *set_operator_name_oracle[SET_OP_NUM + 1] =
  {
    "none",
    "union",
    "intersect",
    "minus",
    "unknown",
    "unknown",
  };
  return lib::is_oracle_mode() ? set_operator_name_oracle[op] : set_operator_name[op];
}

int ObSetRelation::assign(const ObSetRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_query_.assign(other.set_query_))) {
    LOG_WARN("failed to assign set queries", K(ret));
  } else {
    set_op_ = other.set_op_;
    is_set_distinct_ = other.is_set_distinct_;
  }
  return ret;
}

int ObSetRelation::deep_copy(const ObSetRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("failed to deep copy set relation", K(ret));
  }
  return ret;
}

int ObSetRelation::get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs)
{
  int ret = OB_SUCCESS;
  UNUSED(stmt);
  for (int64_t i = 0; OB_SUCC(ret) && i < set_query_.count(); ++i) {
    if (OB_ISNULL(set_query_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set query is unexpected null", K(ret), K(i));
    } else if (OB_FAIL(inputs.push_back(set_query_.at(i)))) {
      LOG_WARN("failed to push back set query", K(ret), K(i));
    }
  }
  return ret;
}

int ObSetRelation::set_set_query(const int64_t index, ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= set_query_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid set query index", K(ret), K(index), "set_query_count", set_query_.count());
  } else {
    set_query_.at(index) = stmt;
  }
  return ret;
}

ObSelectStmt *ObSetRelation::get_set_query(const int64_t index) const
{
  return OB_LIKELY(index >= 0 && index < set_query_.count()) ? set_query_.at(index) : NULL;
}

} // namespace sql
} // namespace oceanbase
