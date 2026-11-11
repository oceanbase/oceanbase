/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_SET_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_SET_H_

#include "sql/resolver/dml/ob_relation.h"

namespace oceanbase
{
namespace sql
{

class ObSetRelation : public ObRelation
{
public:
  enum SetOperator
  {
    NONE = 0,
    UNION,
    INTERSECT,
    EXCEPT,
    RECURSIVE,
    SET_OP_NUM,
  };

  static const char *set_operator_str(SetOperator op);

  explicit ObSetRelation(common::ObIAllocator &allocator)
    : ObRelation(allocator),
      set_query_(allocator),
      set_op_(NONE),
      is_set_distinct_(false)
  {}
  virtual ~ObSetRelation() {}

  int assign(const ObSetRelation &other);
  int deep_copy(const ObSetRelation &other);

  RelationType get_relation_type() const override { return RelationType::SET; }
  bool is_empty() const override { return NONE == set_op_; }
  bool is_multi_input() const override { return true; }
  int get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs) override;

  void assign_set_op(SetOperator op) { set_op_ = op; }
  void assign_set_distinct() { is_set_distinct_ = true; }
  void assign_set_all() { is_set_distinct_ = false; }
  SetOperator get_set_op() const { return set_op_; }
  bool is_set_distinct() const { return is_set_distinct_; }
  common::ObIArray<ObSelectStmt *> &get_set_query() { return set_query_; }
  const common::ObIArray<ObSelectStmt *> &get_set_query() const { return set_query_; }
  int add_set_query(ObSelectStmt *stmt) { return set_query_.push_back(stmt); }
  int set_set_query(const int64_t index, ObSelectStmt *stmt);
  ObSelectStmt *get_set_query(const int64_t index) const;

  INHERIT_TO_STRING_KV("ObRelation", ObRelation,
                       K_(set_op),
                       K_(is_set_distinct),
                       "set_query_count", set_query_.count());

private:
  ObSqlArray<ObSelectStmt *> set_query_;
  SetOperator set_op_;
  bool is_set_distinct_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_SET_H_
