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

#ifndef _OB_CONSTRAINT_PROCESS_H
#define _OB_CONSTRAINT_PROCESS_H 1

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "sql/parser/parse_node.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace sql {
class ObDMLStmt;
class ObSelectStmt;
class ObRawExpr;
class ObResolverParams;
class ObRawExprFactory;
class ObSQLSessionInfo;

class ObConstraintProcess {
public:
  ObConstraintProcess(common::ObIAllocator& allocator, ObRawExprFactory* expr_factory, ObSQLSessionInfo* session_info)
      : allocator_(allocator), expr_factory_(expr_factory), session_info_(session_info)
  {}

  int after_transform(ObDMLStmt*& stmt, share::schema::ObSchemaGetterGuard& schema_guard);

private:
  int resolve_related_part_exprs(const ObRawExpr* expr, ObDMLStmt*& stmt,
      const share::schema::ObTableSchema& table_schema, common::ObIArray<ObRawExpr*>& related_part_exprs);
  int resolve_constraint_column_expr(ObResolverParams& params, uint64_t table_id, ObDMLStmt*& stmt,
      const ParseNode* node, ObRawExpr*& expr, bool& is_success);
  int resolve_constraint_column_expr(ObResolverParams& params, uint64_t table_id, ObDMLStmt*& stmt,
      const common::ObString& expr_str, ObRawExpr*& expr, bool& is_success);

private:
  common::ObIAllocator& allocator_;
  ObRawExprFactory* expr_factory_;
  ObSQLSessionInfo* session_info_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_CONSTRAINT_PROCESS_H */
