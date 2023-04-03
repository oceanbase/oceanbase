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

#ifndef OCEANBASE_SRC_SQL_PARSER_OB_SQL_PARSER_H_
#define OCEANBASE_SRC_SQL_PARSER_OB_SQL_PARSER_H_

#ifdef SQL_PARSER_COMPILATION
#include "parse_node.h"
#else
#include "sql/parser/parse_node.h"
#endif
namespace oceanbase
{
namespace common
{
class ObIAllocator;
}  // namespace common

namespace sql
{
class ObSQLParser
{
public:
  ObSQLParser(common::ObIAllocator &allocator, ObSQLMode mode)
    : allocator_(allocator),
      sql_mode_(mode)
  {}

  int parse(const char *str_ptr, const int64_t str_len, ParseResult &result);

  // only for obproxy fast parser
  // do not use the this function in observer kernel
  int parse_and_gen_sqlid(void *malloc_pool,
                          const char *str_ptr, const int64_t str_len,
                          const int64_t len,
                          char *sql_id);
private:
  int gen_sqlid(const char* paramed_sql, const int64_t sql_len,
                const int64_t len, char *sql_id);

private:
  common::ObIAllocator &allocator_ __attribute__((unused));
  ObSQLMode sql_mode_ __attribute__((unused));
};
}  // namespace pl
}  // namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_PARSER_OB_SQL_PARSER_H_ */
