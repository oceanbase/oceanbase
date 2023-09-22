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

#ifndef OCEANBASE_SRC_PL_PARSER_OB_PL_PARSER_H_
#define OCEANBASE_SRC_PL_PARSER_OB_PL_PARSER_H_

#include "pl/parser/parse_stmt_node.h"
#include "share/ob_define.h"
#include "share/schema/ob_trigger_info.h"
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

namespace pl
{
class ObPLParser
{
public:
  ObPLParser(common::ObIAllocator &allocator, sql::ObCharsets4Parser charsets4parser, ObSQLMode sql_mode = 0)
    : allocator_(allocator),
      charsets4parser_(charsets4parser),
      sql_mode_(sql_mode)
  {}
  int fast_parse(const ObString &stmt_block,
                 ParseResult &parse_result);
  int parse(const common::ObString &stmt_block,
            const common::ObString &orig_stmt_block,
            ParseResult &parse_result,
            bool is_inner_parse = false);
  int parse_routine_body(const common::ObString &routine_body,
                         ObStmtNodeTree *&routine_stmt,
                         bool is_for_trigger);
  int parse_package(const common::ObString &source,
                    ObStmtNodeTree *&package_stmt,
                    const ObDataTypeCastParams &dtc_params,
                    share::schema::ObSchemaGetterGuard *schema_guard,
                    bool is_for_trigger,
                    const share::schema::ObTriggerInfo *trg_info = NULL);
private:
  int parse_procedure(const common::ObString &stmt_block,
                      const common::ObString &orig_stmt_block,
                      ObStmtNodeTree *&multi_stmt,
                      ObQuestionMarkCtx &question_mark_ctx,
                      bool is_for_trigger,
                      bool is_dynamic,
                      bool is_inner_parse,
                      bool &is_include_old_new_in_trigger);
  int parse_stmt_block(ObParseCtx &parse_ctx,
                       ObStmtNodeTree *&multi_stmt);
  int reconstruct_trigger_package(ObStmtNodeTree *&package_stmt,
                                  const share::schema::ObTriggerInfo *trg_info,
                                  const ObDataTypeCastParams &dtc_params,
                                  share::schema::ObSchemaGetterGuard *schema_guard);
private:
  common::ObIAllocator &allocator_;
  sql::ObCharsets4Parser charsets4parser_;
  ObSQLMode sql_mode_;
};
}  // namespace pl
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_PL_PARSER_OB_PL_PARSER_H_ */
