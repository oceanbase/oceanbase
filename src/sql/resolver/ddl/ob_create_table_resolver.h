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

#ifndef _OB_CREATE_TABLE_RESOLVER_H
#define _OB_CREATE_TABLE_RESOLVER_H 1
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableResolver: public ObDDLResolver
{
public:
  //store generated column and its' dependent expr.
  struct GenColExpr
  {
    GenColExpr(uint64_t col_id,
               ObRawExpr *gen_col_expr)
        : col_id_(col_id), gen_col_expr_(gen_col_expr)
    { }
    GenColExpr()
        : col_id_(common::OB_NOT_EXIST_COLUMN_ID), gen_col_expr_(NULL)
    { }
    TO_STRING_KV(K_(col_id), K_(gen_col_expr));

    ~GenColExpr() { }
    uint64_t col_id_;
    ObRawExpr *gen_col_expr_;
  };
  explicit ObCreateTableResolver(ObResolverParams &params);
  virtual ~ObCreateTableResolver();

  virtual int resolve(const ParseNode &parse_tree);
  static int set_nullable_for_cta_column(ObSelectStmt *select_stmt,
                                  share::schema::ObColumnSchemaV2& column,
                                  const ObRawExpr *expr,
                                  const ObString &table_name,
                                  common::ObIAllocator &allocator,
                                  ObStmt *stmt);
private:
  enum ResolveRule
    {
      RESOLVE_ALL = 0,
      RESOLVE_COL_ONLY,
      RESOLVE_NON_COL
    };
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableResolver);
  // function members
  uint64_t gen_column_id();
  uint64_t gen_udt_set_id();
  int64_t get_primary_key_size() const;
  int add_primary_key_part(const common::ObString &column_name, common::ObArray<ObColumnResolveStat> &stats, int64_t &pk_data_length);
  int add_hidden_tablet_seq_col();
  int add_hidden_external_table_pk_col();

  int add_generated_hidden_column_for_udt(ObTableSchema &table_schema,
                                          ObSEArray<ObColumnSchemaV2, SEARRAY_INIT_NUM> &resolved_cols,
                                          ObColumnSchemaV2 &udt_column);
  int add_generated_hidden_column_for_udt(ObTableSchema &table_schema,
                                          ObColumnSchemaV2 &udt_column);
  int check_column_name_duplicate(const ParseNode *node);
  int resolve_primary_key_node(const ParseNode &pk_node, common::ObArray<ObColumnResolveStat> &stats);
  int resolve_table_elements(const ParseNode *node,
                             common::ObArray<int> &idx_index_node,
                             common::ObArray<int> &foreign_key_node_position_list,
                             common::ObArray<int> &table_level_constraint_list,
                             const int resolve_rule);
  int resolve_table_elements_from_select(const ParseNode &parse_tree);
  int set_temp_table_info(share::schema::ObTableSchema &table_schema, ParseNode *commit_option_node);

  int set_table_option_to_schema(share::schema::ObTableSchema &table_schema);
  int resolve_table_charset_info(const ParseNode *node);
  //index
  int add_sort_column(const obrpc::ObColumnSortItem &sort_column);
  int generate_index_arg();
  int set_index_name();
  int set_index_option_to_arg();
  int set_storing_column();
  int resolve_index(const ParseNode *node, common::ObArray<int> &index_node_position_list);
  int resolve_index_node(const ParseNode *node);
  int resolve_index_name(
      const ParseNode *node,
      const common::ObString &first_column_name,
      bool is_unique,
      ObString &uk_name);
  int resolve_table_level_constraint_for_mysql(
      const ParseNode* node, ObArray<int> &constraint_position_list);
  int build_partition_key_info(share::schema::ObTableSchema &table_schema,
                               const bool is_subpart);
  //resolve partition option only used in ObCreateTableResolver now.
  int resolve_partition_option(ParseNode *node,
                               share::schema::ObTableSchema &table_schema,
                               const bool is_partition_option_node_with_opt);
  //check generated column whether valid
  int check_generated_partition_column(share::schema::ObTableSchema &table_schema);
  ObRawExpr* find_gen_col_expr(const uint64_t column_id);
  int get_resolve_stats_from_table_schema(const share::schema::ObTableSchema &table_schema,
                                          ObArray<ObColumnResolveStat> &stats);
  static int check_same_substr_expr(ObRawExpr &left, ObRawExpr &right, bool &same);
  virtual int get_table_schema_for_check(share::schema::ObTableSchema &table_schema) override;
  int add_new_column_for_oracle_temp_table(share::schema::ObTableSchema &table_schema, ObArray<ObColumnResolveStat> &stats);
  int add_new_indexkey_for_oracle_temp_table(const int32_t org_key_len);
  int add_pk_key_for_oracle_temp_table(ObArray<ObColumnResolveStat> &stats, int64_t &pk_data_length);
  int set_partition_info_for_oracle_temp_table(share::schema::ObTableSchema &table_schema);
  // following four functions should be used only in oracle mode
  int generate_primary_key_name_array(const share::schema::ObTableSchema &table_schema, ObIArray<ObString> &pk_columns_name);
  int generate_uk_idx_array(const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list, ObIArray<int64_t> &uk_idx_in_index_arg_list);
  bool is_pk_uk_duplicate(const ObIArray<ObString> &pk_columns_name, const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list, const ObIArray<int64_t> &uk_idx);
  bool is_uk_uk_duplicate(const ObIArray<int64_t> &uk_idx, const ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  int resolve_vertical_partition(const ParseNode *column_partition_node);
  int resolve_auto_partition(const ParseNode *partition_node);

  typedef common::hash::ObPlacementHashSet<uint64_t, common::OB_MAX_USER_DEFINED_COLUMNS_COUNT> VPColumnIdHashSet;
  int resolve_vertical_column(
      const ParseNode *aux_vertical_column_node,
      VPColumnIdHashSet &vp_column_id_set,
      const bool is_auxiliary_part);

private:
  // data members
  uint64_t cur_column_id_;
  common::ObSEArray<uint64_t, 16> primary_keys_;
  common::hash::ObPlacementHashSet<share::schema::ObColumnNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> column_name_set_;
  bool if_not_exist_;
  bool is_oracle_temp_table_; //是否创建oracle的临时表
  bool is_temp_table_pk_added_;
  obrpc::ObCreateIndexArg index_arg_;
  IndexNameSet current_index_name_set_;

  common::ObSEArray<GenColExpr, 5> gen_col_exprs_;//store generated column and dependent exprs
  common::ObSEArray<ObRawExpr *, 5> constraint_exprs_;//store constraint exprs

  uint64_t cur_udt_set_id_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CREATE_TABLE_RESOLVER_H */
