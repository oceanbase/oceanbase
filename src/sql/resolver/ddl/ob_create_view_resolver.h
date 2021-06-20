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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_CREATE_VIEW_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_CREATE_VIEW_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_view_table_resolver.h"  // resolve select clause
#include "share/schema/ob_table_schema.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnSchemaV2;
}
}  // namespace share
namespace sql {
class ObCreateViewResolver : public ObDDLResolver {
  static const int64_t MATERIALIZED_NODE = 0;
  static const int64_t VIEW_NODE = 1;
  static const int64_t VIEW_COLUMNS_NODE = 2;
  static const int64_t TABLE_ID_NODE = 3;
  static const int64_t SELECT_STMT_NODE = 4;
  static const int64_t IF_NOT_EXISTS_NODE = 5;
  static const int64_t WITH_OPT_NODE = 6;
  static const int64_t ROOT_NUM_CHILD = 7;

public:
  explicit ObCreateViewResolver(ObResolverParams& params);
  virtual ~ObCreateViewResolver();

  virtual int resolve(const ParseNode& parse_tree);

private:
  int resolve_column_list(ParseNode* view_columns_node, common::ObIArray<common::ObString>& column_list);
  int check_select_stmt_col_name(SelectItem& select_item, ObArray<int64_t>& index_array, int64_t pos,
      common::hash::ObHashSet<ObString>& view_col_names);
  int create_alias_names_auto(
      ObArray<int64_t>& index_array, ObSelectStmt* select_stmt, common::hash::ObHashSet<ObString>& view_col_names);
  /**
   * use stmt_print instead of ObSelectStmtPrinter. When do_print return OB_SIZE_OVERFLOW
   * and the buf_len is less than OB_MAX_PACKET_LENGTH, stmt_print will expand buf and try again.
   */
  int stmt_print(
      const ObSelectStmt* stmt, common::ObIArray<common::ObString>* column_list, common::ObString& expanded_view);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateViewResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_VIEW_RESOLVER_H_
