/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_H_

#include "lib/string/ob_string.h"
#include "sql/resolver/ob_sql_array.h"

namespace oceanbase
{
namespace sql
{

class ObSelectStmt;

enum class RelationType
{
  TABLE,
  JOIN,
  FROM,
  GROUP,
  SELECT,
  SET,
  MAX
};

class ObRelation
{
public:
  ObRelation(common::ObIAllocator &) {}
  virtual ~ObRelation() {}

  virtual bool is_empty() const { return false; }

  virtual RelationType get_relation_type() const = 0;
  const char* get_relation_type_string() const;
  static const char* get_relation_type_string(const RelationType type);

  virtual bool is_multi_input() const { return false; }
  virtual int get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs);

  VIRTUAL_TO_STRING_KV("relation_type", get_relation_type_string());
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_H_
