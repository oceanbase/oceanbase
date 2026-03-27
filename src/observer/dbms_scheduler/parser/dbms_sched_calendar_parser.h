/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_DBMS_SCHED_PARSER_H_
#define SRC_OBSERVER_DBMS_SCHED_PARSER_H_

#include "pl/parser/parse_stmt_node.h"
#include "share/ob_define.h"
#include "sql/parser/ob_parser_utils.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
}  // namespace common
namespace sql
{
class ObSQLSessionInfo;
}

namespace dbms_scheduler
{
class ObDBMSSchedCalendarParser
{
public:
  ObDBMSSchedCalendarParser(common::ObIAllocator &allocator, sql::ObCharsets4Parser charsets4parser, ObSQLMode sql_mode = 0)
    : allocator_(allocator),
      charsets4parser_(charsets4parser),
      sql_mode_(sql_mode)
  {}

  int parse(const ObString &calendar_body, ObStmtNodeTree *& parser_tree);

private:
  common::ObIAllocator &allocator_;
  sql::ObCharsets4Parser charsets4parser_;
  ObSQLMode sql_mode_;
};
}  // namespace pl
}  // namespace oceanbase
#endif /* SRC_OBSERVER_DBMS_SCHED_PARSER_H_ */
