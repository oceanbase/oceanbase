/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_relation.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

const char* ObRelation::get_relation_type_string() const
{
  return get_relation_type_string(get_relation_type());
}

const char* ObRelation::get_relation_type_string(const RelationType type)
{
  const char *str = NULL;
  #define RELATION_TYPE_TO_STR(relation_type) case RelationType::relation_type: str = #relation_type; break;
  switch(type) {
    RELATION_TYPE_TO_STR(TABLE)
    RELATION_TYPE_TO_STR(JOIN)
    RELATION_TYPE_TO_STR(FROM)
    RELATION_TYPE_TO_STR(GROUP)
    RELATION_TYPE_TO_STR(SELECT)
    RELATION_TYPE_TO_STR(SET)
    default:  str = "UNKNOWN";
  }
  return str;
}


int ObRelation::get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs)
{
  int ret = OB_SUCCESS;
  UNUSEDx(stmt, inputs);
  return ret;
}

} // namespace sql
} // namespace oceanbase
