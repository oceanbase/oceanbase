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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_INDEX_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_CREATE_INDEX_RESOLVER_H_ 1
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
namespace oceanbase
{
namespace obrpc
{
struct ObCreateIndexArg;
}
namespace sql
{
class ObCreateIndexStmt;
class ObCreateIndexResolver : public ObDDLResolver
{
public:
  static const int64_t CREATE_INDEX_CHILD_NUM = 9;
  explicit ObCreateIndexResolver(ObResolverParams &params);
  virtual ~ObCreateIndexResolver();

  virtual int resolve(const ParseNode &parse_tree);
protected:
  int resolve_index_name_node(
      ParseNode *index_name_node,
      ObCreateIndexStmt *crt_idx_stmt);
  int resolve_index_table_name_node(
      ParseNode *index_table_name_node,
      ObCreateIndexStmt *crt_idx_stmt);
  int resolve_index_column_node(
      ParseNode *index_column_node,
      const int64_t index_keyname_value,
      ParseNode *table_option_node,
      ObCreateIndexStmt *crt_idx_stmt,
      const share::schema::ObTableSchema *tbl_schema);
  int resolve_index_option_node(
      ParseNode *index_option_node,
      ObCreateIndexStmt *crt_idx_stmt,
      const share::schema::ObTableSchema *tbl_schema,
      bool is_partitioned);
  int resolve_index_method_node(
      ParseNode *index_method_node,
      ObCreateIndexStmt *crt_idx_stmt);
  int check_generated_partition_column(
      share::schema::ObTableSchema &index_schema);
  int add_sort_column(const obrpc::ObColumnSortItem &sort_column);
  int set_table_option_to_stmt(bool is_partitioned);
  int add_new_indexkey_for_oracle_temp_table();
  int fill_session_info_into_arg(const sql::ObSQLSessionInfo *session,
                                 ObCreateIndexStmt *crt_idx_stmt);
private:
  bool is_oracle_temp_table_; //是否创建oracle的临时表上索引
  bool is_spec_block_size; //是否指定block size
  DISALLOW_COPY_AND_ASSIGN(ObCreateIndexResolver);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_INDEX_RESOLVER_H_ */
