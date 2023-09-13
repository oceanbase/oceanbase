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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_DDL_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_DDL_RESOLVER_H_ 1
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/hash/ob_placement_hashset.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_duplicate_scope_define.h"
#include "share/schema/ob_schema_struct.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "sql/resolver/ddl/ob_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
namespace oceanbase
{
namespace common
{
struct ObObjCastParams;
}
namespace sql
{
struct ObExternalFileFormat;
struct PartitionInfo
{
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionOption part_option_;
  share::schema::ObPartitionOption subpart_option_;
  common::ObSEArray<share::schema::ObPartition, 4> parts_;
  common::ObSEArray<share::schema::ObSubPartition, 2> subparts_;
  common::ObSEArray<common::ObString, 8> part_keys_;
  common::ObSEArray<ObRawExpr*, 8> part_func_exprs_;
  common::ObSEArray<ObRawExpr*, 8> range_value_exprs_;
  ObDDLStmt::array_t list_value_exprs_;
};

enum NUMCHILD {
  CREATE_TABLE_NUM_CHILD = 7,
  CREATE_TABLE_AS_SEL_NUM_CHILD = 8,
  COLUMN_DEFINITION_NUM_CHILD = 4,
  COLUMN_DEF_NUM_CHILD = 3,
  INDEX_NUM_CHILD = 5,
  CREATE_SYNONYM_NUM_CHILD = 7,
  GEN_COLUMN_DEFINITION_NUM_CHILD = 7, // generated column、identity column参数个数相同
  IDEN_OPTION_DEFINITION_NUM_CHILD = 1
};

struct ObColumnResolveStat
{
  ObColumnResolveStat() {reset();}
  ~ObColumnResolveStat() {}
  void reset()
  {
    column_id_ = common::OB_INVALID_ID;
    is_primary_key_ = false;
    is_autoincrement_ = false;
    is_unique_key_ = false;
    is_set_null_ = false;
    is_set_not_null_ = false;
    is_set_default_value_ = false;
    is_set_orig_default_value_ = false;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K(column_id_),
         K(is_primary_key_),
         K(is_autoincrement_),
         K(is_set_null_),
         K(is_set_not_null_),
         K(is_set_default_value_),
         K(is_unique_key_),
         K(is_set_orig_default_value_));
    J_OBJ_END();
    return pos;
  }
  uint64_t column_id_;
  bool is_primary_key_;
  bool is_autoincrement_;
  bool is_set_null_;
  bool is_set_not_null_;
  bool is_set_default_value_;
  bool is_unique_key_;
  bool is_set_orig_default_value_;
};

class ObDDLResolver : public ObStmtResolver
{
public:
  enum INDEX_TYPE {
    NOT_SPECIFIED = 0,
    LOCAL_INDEX = 1,
    GLOBAL_INDEX = 2
  };
  enum INDEX_KEYNAME {
    NORMAL_KEY = 0,
    UNIQUE_KEY = 1,
    SPATIAL_KEY = 2
  };
  enum COLUMN_NODE {
    COLUMN_REF_NODE = 0,
    COLUMN_TYPE_NODE,
    COLUMN_NAME_NODE,
  };
  enum RangeNode {
    RANGE_FUN_EXPR_NODE = 0,
    RANGE_ELEMENTS_NODE = 1,
    RANGE_SUBPARTITIOPPN_NODE = 2,
    RANGE_PARTITION_NUM_NODE = 3,
    RANGE_TEMPLATE_MARK = 4,
    RANGE_INTERVAL_NODE = 5,
  };
  enum ElementsNode {
    PARTITION_NAME_NODE = 0,
    PARTITION_ELEMENT_NODE = 1,
    PART_ID_NODE = 2,
    ELEMENT_ATTRIBUTE_NODE = 3,
    ELEMENT_SUBPARTITION_NODE = 4
  };
  enum ListNode {
    LIST_FUN_EXPR_NODE = 0,
    LIST_ELEMENTS_NODE = 1,
    LIST_SUBPARTITIOPPN_NODE = 2,
    LIST_PARTITION_NUM_NODE = 3,
    LIST_TEMPLATE_MARK = 4
  };
  enum HashOrKeyNode {
    HASH_FUN_EXPR_NODE = 0,
    HASH_PARTITION_NUM_NODE = 1,
    HASH_PARTITION_LIST_NODE = 2,
    HASH_SUBPARTITIOPPN_NODE = 3,
    HASH_TEMPLATE_MARK = 4,
    HASH_TABLESPACE_NODE = 5
  };
  enum SplitActionNode {
    PARTITION_DEFINE_NODE = 0,
    AT_VALUES_NODE = 1,
    SPLIT_PARTITION_TYPE_NODE = 2
  };
  static const int NAMENODE = 1;

  static const int64_t MAX_PROGRESSIVE_MERGE_NUM = 64;
  static const int64_t MAX_REPLICA_NUM = 6;
  static const int64_t MIN_BLOCK_SIZE = 1024;
  static const int64_t MAX_BLOCK_SIZE = 1048576;
  static const int64_t DEFAULT_TABLE_DOP = 1;
  explicit ObDDLResolver(ObResolverParams &params);
  virtual ~ObDDLResolver();
  static int check_text_length(ObCharsetType cs_type, ObCollationType co_type,
                               const char *name, ObObjType &type,
                               int32_t &length,
                               bool need_rewrite_length,
                               const bool is_byte_length = false);
  static int rewrite_text_length_mysql(ObObjType &type, int32_t &length);
  // check whether the column is allowed to be primary key.
  int check_add_column_as_pk_allowed(const ObColumnSchemaV2 &column_schema);
  static int get_primary_key_default_value(
      common::ObObjType type,
      common::ObObj &default_value);
  static bool is_valid_prefix_key_type(
      const common::ObObjTypeClass column_type_class);
  static int check_prefix_key(
      const int32_t prefix_len,
      const share::schema::ObColumnSchemaV2 &column_schema);
  int resolve_default_value(
      ParseNode *def_node,
      common::ObObjParam &default_value);
  static int check_and_fill_column_charset_info(
      share::schema::ObColumnSchemaV2 &column,
      const common::ObCharsetType table_charset_type,
      const common::ObCollationType table_collation_type);
  static int check_string_column_length(
      const share::schema::ObColumnSchemaV2 &column,
      const bool is_oracle_mode);
  static int check_raw_column_length(
      const share::schema::ObColumnSchemaV2 &column);
  static int check_urowid_column_length(
      const share::schema::ObColumnSchemaV2 &column);
  static int check_default_value_length(
      const share::schema::ObColumnSchemaV2 &column,
      common::ObObj &default_value);
  static int cast_default_value(
      common::ObObj &default_value,
      const common::ObTimeZoneInfo *tz_info,
      const common::ObString *nls_formats,
      common::ObIAllocator &allocator,
      share::schema::ObColumnSchemaV2 &column_schema,
      const ObSQLMode sql_mode);
  static int print_expr_to_default_value(ObRawExpr &expr,
                                         share::schema::ObColumnSchemaV2 &column,
                                         ObSchemaChecker *schema_checker,
                                         const common::ObTimeZoneInfo *tz_info);
  static int check_dup_gen_col(const ObString &expr,
                               ObIArray<ObString> &gen_col_expr_arr);
  static int init_empty_session(const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                const ObString *nls_formats,
                                common::ObIAllocator &allocator,
                                share::schema::ObTableSchema &table_schema,
                                const ObSQLMode sql_mode,
                                ObSchemaChecker *schema_checker,
                                ObSQLSessionInfo &session_info);
  static int reformat_generated_column_expr(ObObj &default_value,
                                            const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                            const common::ObString *nls_formats,
                                            common::ObIAllocator &allocator,
                                            share::schema::ObTableSchema &table_schema,
                                            share::schema::ObColumnSchemaV2 &column,
                                            const ObSQLMode sql_mode,
                                            ObSchemaChecker *schema_checker);
  static int resolve_generated_column_expr(
      ObString &expr_str,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &table_schema,
      ObIArray<share::schema::ObColumnSchemaV2 *> &resolved_cols,
      share::schema::ObColumnSchemaV2 &column,
      ObSQLSessionInfo *session_info,
      ObSchemaChecker *schema_checker,
      ObRawExpr *&expr,
      ObRawExprFactory &expr_factory);
  static int check_default_value(
      common::ObObj &default_value,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const common::ObString *nls_formats,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &table_schema,
      share::schema::ObColumnSchemaV2 &column_schema,
      ObIArray<ObString> &gen_col_expr_arr,
      const ObSQLMode sql_mode,
      bool allow_sequence,
      ObSchemaChecker *schema_checker,
      share::schema::ObColumnSchemaV2 *hidden_col = NULL);
  static int check_default_value(
      common::ObObj &default_value,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const common::ObString *nls_formats,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &table_schema,
      ObIArray<share::schema::ObColumnSchemaV2> &resolved_cols,
      share::schema::ObColumnSchemaV2 &column_schema,
      ObIArray<ObString> &gen_col_expr_arr,
      const ObSQLMode sql_mode,
      ObSQLSessionInfo *session_info,
      bool allow_sequence,
      ObSchemaChecker *schema_checker = NULL);
  static int check_default_value(
      common::ObObj &default_value,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const common::ObString *nls_formats,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema &table_schema,
      ObIArray<share::schema::ObColumnSchemaV2 *> &resolved_cols,
      share::schema::ObColumnSchemaV2 &column_schema,
      ObIArray<ObString> &gen_col_expr_arr,
      const ObSQLMode sql_mode,
      ObSQLSessionInfo *session_info,
      bool allow_sequence,
      ObSchemaChecker *schema_checker);
  static int calc_default_value(
      share::schema::ObColumnSchemaV2 &column_schema,
      common::ObObj &default_value,
      const common::ObTimeZoneInfoWrap &tz_info_wrap,
      const common::ObString *nls_formats,
      common::ObIAllocator &allocator);
  static int get_udt_column_default_values(const ObObj &default_value,
                                           const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                           const common::ObString *nls_formats,
                                           ObIAllocator &allocator,
                                           ObColumnSchemaV2 &column,
                                           const ObSQLMode sql_mode,
                                           ObSQLSessionInfo *session_info,
                                           ObSchemaChecker *schema_checker,
                                           ObObj &extend_result,
                                           obrpc::ObDDLArg &ddl_arg);
  static int ob_udt_check_and_add_ddl_dependency(const uint64_t schema_id,
                                                 const ObSchemaType schema_type,
                                                 const int64_t schema_version,
                                                 const uint64_t schema_tenant_id,
                                                 obrpc::ObDDLArg &ddl_arg);
  static int add_udt_default_dependency(ObRawExpr *expr,
                                        ObSchemaChecker *schema_checker,
                                        obrpc::ObDDLArg &ddl_arg);
  static int adjust_string_column_length_within_max(
      share::schema::ObColumnSchemaV2 &column,
      const bool is_oracle_mode);
  // { used for enum and set
  int fill_extended_type_info(
      const ParseNode &str_list_node,
      share::schema::ObColumnSchemaV2 &column);
  int check_extended_type_info(
      share::schema::ObColumnSchemaV2 &column,
      ObSQLMode sql_mode);
  static int calc_enum_or_set_data_length(
      const ObIArray<common::ObString> &type_info,
      const ObCollationType &collation_type,
      const ObObjType &type,
      int32_t &length);
  static int calc_enum_or_set_data_length(
      share::schema::ObColumnSchemaV2 &column);
  static int check_duplicates_in_type_infos(
      const share::schema::ObColumnSchemaV2 &col,
      ObSQLMode sql_mode,
      int32_t &dup_cnt);
  static int check_type_info_incremental_change(
      const share::schema::ObColumnSchemaV2 &ori_schema,
      const share::schema::ObColumnSchemaV2 &new_schema,
      bool &is_incremental);
  static int cast_enum_or_set_default_value(
      const share::schema::ObColumnSchemaV2 &column,
      common::ObObjCastParams &params, common::ObObj &def_val);
  int check_partition_name_duplicate(ParseNode *node, bool is_oracle_modle = false);
  static int check_text_column_length_and_promote(share::schema::ObColumnSchemaV2 &column,
                                                  int64_t table_id,
                                                  const bool is_byte_length = false);
  static int get_enable_split_partition(const int64_t tenant_id, bool &enable_split_partition);
  typedef common::hash::ObPlacementHashSet<share::schema::ObIndexNameHashWrapper, common::OB_MAX_COLUMN_NUMBER> IndexNameSet;
  int generate_index_name(ObString &index_name, IndexNameSet &current_index_name_set, const common::ObString &first_col_name);
  int resolve_range_partition_elements(ParseNode *node,
                                       const bool is_subpartition,
                                       const share::schema::ObPartitionFuncType part_type,
                                       const int64_t expr_num,
                                       common::ObIArray<ObRawExpr *> &range_value_exprs,
                                       common::ObIArray<share::schema::ObPartition> &partitions,
                                       common::ObIArray<share::schema::ObSubPartition> &subpartitions,
                                       const bool &in_tablegroup = false);
   int resolve_partition_hash_or_key(
       ObPartitionedStmt *stmt,
       ParseNode *node,
       const bool is_subpartition,
       share::schema::ObTableSchema &table_schema);
  int resolve_list_partition_elements(ParseNode *node,
                                      const bool is_subpartition,
                                      const share::schema::ObPartitionFuncType part_type,
                                      int64_t &expr_num,
                                      ObDDLStmt::array_t &list_value_exprs,
                                      common::ObIArray<share::schema::ObPartition> &partitions,
                                      common::ObIArray<share::schema::ObSubPartition> &subpartitions,
                                      const bool &in_tablegroup = false);
  int resolve_split_partition_range_element(const ParseNode *node,
                                            const share::schema::ObPartitionFuncType part_type,
                                            const ObIArray<ObRawExpr *> &part_func_exprs,
                                            common::ObIArray<ObRawExpr *> &range_value_exprs,
                                            const bool &in_tablegroup = false);
  int resolve_split_partition_list_value(const ParseNode *node,
                                         const share::schema::ObPartitionFuncType part_type,
                                         const ObIArray<ObRawExpr *> &part_func_exprs,
                                         ObDDLStmt::array_t &list_value_exprs,
                                         int64_t &expr_num,
                                         const bool &in_tablegroup);
  template <typename STMT>
  int resolve_split_at_partition(STMT *stmt, const ParseNode *node,
                                 const share::schema::ObPartitionFuncType part_type,
                                 const ObIArray<ObRawExpr *> &part_func_exprs,
                                 share::schema::ObPartitionSchema &t_schema,
                                 int64_t &expr_num,
                                 const bool &in_tablegroup = false);
  template <typename STMT>
  int resolve_split_into_partition(STMT *stmt, const ParseNode *node,
                                   const share::schema::ObPartitionFuncType part_type,
                                   const ObIArray<ObRawExpr *> &part_func_exprs,
                                   int64_t &part_num,
                                   int64_t &expr_num,
                                   share::schema::ObPartitionSchema &t_schema,
                                   const bool &in_tablegroup = false);
  //}
  int check_column_in_foreign_key(const share::schema::ObTableSchema &table_schema,
                                  const common::ObString &column_name,
                                  const bool is_drop_column);
  int check_column_in_foreign_key_for_oracle(
      const share::schema::ObTableSchema &table_schema,
      const ObString &column_name,
      ObAlterTableStmt *alter_table_stmt);
  int check_is_json_contraint(ObTableSchema &tmp_table_schema, ObIArray<ObConstraint> &csts, ParseNode *cst_check_expr_node);

  int check_column_in_check_constraint(
      const share::schema::ObTableSchema &table_schema,
      const ObString &column_name,
      ObAlterTableStmt *alter_table_stmt);

  int check_index_columns_equal_foreign_key(const share::schema::ObTableSchema &table_schema,
                                            const share::schema::ObTableSchema &index_table_schema);
  static bool is_ids_match(const common::ObIArray<uint64_t> &src_list, const common::ObIArray<uint64_t> &dest_list);
  static int check_indexes_on_same_cols(const share::schema::ObTableSchema &table_schema,
                                        const share::schema::ObTableSchema &index_table_schema,
                                        ObSchemaChecker &schema_checker,
                                        bool &has_other_indexes_on_same_cols);
  static int check_indexes_on_same_cols(const share::schema::ObTableSchema &table_schema,
                                        const obrpc::ObCreateIndexArg &create_index_arg,
                                        ObSchemaChecker &schema_checker,
                                        bool &has_other_indexes_on_same_cols);
  static int check_index_name_duplicate(const share::schema::ObTableSchema &table_schema,
                                        const obrpc::ObCreateIndexArg &create_index_arg,
                                        ObSchemaChecker &schema_checker,
                                        bool &has_same_index_name);
  static int resolve_check_constraint_expr(
        ObResolverParams &params,
        const ParseNode *node,
        share::schema::ObTableSchema &table_schema,
        share::schema::ObConstraint &constraint,
        ObRawExpr *&check_expr,
        const share::schema::ObColumnSchemaV2 *column_schema = NULL);
  static int set_index_tablespace(const share::schema::ObTableSchema &table_schema,
                                  obrpc::ObCreateIndexArg &index_arg);

  int resolve_partition_node(ObPartitionedStmt *stmt,
                             ParseNode *part_node,
                             share::schema::ObTableSchema &table_schema);
  int resolve_subpartition_option(ObPartitionedStmt *stmt,
                                  ParseNode *subpart_node,
                                  share::schema::ObTableSchema &table_schema);
  // @param [in] resolved_cols the columns which have been resolved in alter table, default null
  int resolve_spatial_index_constraint(
      const share::schema::ObTableSchema &table_schema,
      const common::ObString &column_name,
      int64_t column_num,
      const int64_t index_keyname_value,
      bool is_explicit_order,
      bool is_func_index,
      ObIArray<share::schema::ObColumnSchemaV2*> *resolved_cols = NULL);
  int resolve_spatial_index_constraint(
      const share::schema::ObColumnSchemaV2 &column_schema,
      int64_t column_num,
      const int64_t index_keyname_value,
      bool is_oracle_mode,
      bool is_explicit_order);
protected:
  static int get_part_str_with_type(
      const bool is_oracle_mode,
      share::schema::ObPartitionFuncType part_func_type,
      common::ObString &func_str,
      common::ObSqlString &part_str);
  int resolve_hints(const ParseNode *parse_node, ObDDLStmt &stmt, const ObTableSchema &table_schema);
  int calc_ddl_parallelism(const uint64_t hint_parallelism, const uint64_t table_dop, uint64_t &parallelism);
  int deep_copy_str(const common::ObString &src, common::ObString &dest);
  int set_table_name(
      const common::ObString &table_name);
  int set_database_name(
      const common::ObString &database_name);
  int set_encryption_name(
      const common::ObString &encryption);
  int resolve_table_options(
      ParseNode *node,
      bool is_index_option);
  int resolve_table_id_pre(ParseNode *node);
  int resolve_table_option(
      const ParseNode *node,
      const bool is_index_option);
  int resolve_column_definition_ref(
      share::schema::ObColumnSchemaV2 &column,
      ParseNode *node,
      bool is_resolve_for_alter_table);
  int resolve_column_name(common::ObString &col_name, ParseNode *node);
  int resolve_column_name(share::schema::ObColumnSchemaV2 &column, ParseNode *node);
  int get_identity_column_count(
      const share::schema::ObTableSchema &table_schema,
      int64_t &identity_column_count);
  int resolve_identity_column_definition(
      share::schema::ObColumnSchemaV2 &column,
      ParseNode *node);
  int resolve_column_definition(
      share::schema::ObColumnSchemaV2 &column,
      ParseNode *node,
      ObColumnResolveStat &reslove_stat,
      bool &is_modify_column_visibility,
      common::ObString &pk_name,
      const bool is_oracle_temp_table = false,
      const bool is_create_table_as = false,
      const bool is_external_table = false,
      const bool allow_has_default = true);
  int resolve_uk_name_from_column_attribute(
      ParseNode *attrs_node,
      common::ObString &uk_name);
  int drop_not_null_constraint(const share::schema::ObColumnSchemaV2 &column);
  int resolve_normal_column_attribute_constr_not_null(ObColumnSchemaV2 &column,
                                                      ParseNode *attrs_node,
                                                      ObColumnResolveStat &resolve_stat);
  int resolve_normal_column_attribute_constr_default(ObColumnSchemaV2 &column,
                                                     ParseNode *attr_node,
                                                     ObColumnResolveStat &resolve_stat,
                                                     ObObjParam& default_value,
                                                     bool& is_set_cur_default,
                                                     bool& is_set_orig_default);
  int resolve_normal_column_attribute_constr_null(ObColumnSchemaV2 &column,
                                                  ObColumnResolveStat &resolve_stat);
  int resolve_normal_column_attribute(share::schema::ObColumnSchemaV2 &column,
                                      ParseNode *attrs_node,
                                      ObColumnResolveStat &reslove_stat,
                                      common::ObString &pk_name,
                                      const bool allow_has_default = true);
  int resolve_normal_column_attribute_check_cons(ObColumnSchemaV2 &column,
                                                 ParseNode *attrs_node,
                                                 ObCreateTableStmt *create_table_stmt);
  int resolve_normal_column_attribute_foreign_key(ObColumnSchemaV2 &column,
                                                  ParseNode *attrs_node,
                                                  ObCreateTableStmt *create_table_stmt);
  int resolve_generated_column_attribute(share::schema::ObColumnSchemaV2 &column,
                                         ParseNode *attrs_node,
                                         ObColumnResolveStat &reslove_stat,
                                         const bool is_external_table);
  int resolve_identity_column_attribute(share::schema::ObColumnSchemaV2 &column,
                                        ParseNode *attrs_node,
                                        ObColumnResolveStat &reslove_stat,
                                        common::ObString &pk_name);
  int resolve_srid_node(share::schema::ObColumnSchemaV2 &column,
                        const ParseNode &srid_node);
  /*
  int resolve_generated_column_definition(
      share::schema::ObColumnSchemaV2 &column,
      ParseNode *node,
      ObColumnResolveStat &reslove_stat);
  */

  virtual int add_storing_column(
      const common::ObString &column_name,
      bool check_column_exist = true,
      bool is_hidden = false);
  virtual int get_table_schema_for_check(share::schema::ObTableSchema &table_schema)
  {
    UNUSED(table_schema);
    return common::OB_SUCCESS;
  };
  int fill_column_collation_info(
      const int64_t tenant_id,
      const int64_t database_id,
      const common::ObString &table_name);
  int check_column_name_duplicate(
      const ParseNode *node);
  int resolve_partition_range(
      ObPartitionedStmt *stmt,
      ParseNode *node,
      const bool is_subpartition,
      share::schema::ObTableSchema &table_schema);
  int resolve_interval_clause(
      ObPartitionedStmt *stmt,
      ParseNode *node,
      share::schema::ObTableSchema &table_schema,
      common::ObSEArray<ObRawExpr*, 8> &range_exprs);
  static int resolve_interval_node(
      ObResolverParams &params,
      ParseNode *interval_node,
      common::ColumnType &col_dt,
      int64_t precision,
      int64_t scale,
      ObRawExpr *&interval_value_expr_out);
  static int resolve_interval_expr_low(
      ObResolverParams &params,
      ParseNode *interval_node,
      const share::schema::ObTableSchema &table_schema,
      ObRawExpr *transition_expr,
      ObRawExpr *&interval_value);
  int resolve_partition_list(
      ObPartitionedStmt *stmt,
      ParseNode *node,
      const bool is_subpartition,
      share::schema::ObTableSchema &table_schema);
  int resolve_range_partition_elements(
      const ObDDLStmt *stmt,
      ParseNode *node,
      const bool is_subpartition,
      const share::schema::ObPartitionFuncType part_type,
      const common::ObIArray<ObRawExpr *> &part_func_exprs,
      common::ObIArray<ObRawExpr *> &range_value_exprs,
      common::ObIArray<share::schema::ObPartition> &partitions,
      common::ObIArray<share::schema::ObSubPartition> &subpartitions,
      const bool &in_tablegroup = false);
  int resolve_list_partition_elements(
      const ObDDLStmt *stmt,
      ParseNode *node,
      const bool is_subpartition,
      const share::schema::ObPartitionFuncType part_type,
      const common::ObIArray<ObRawExpr *> &part_func_exprs,
      ObDDLStmt::array_t &list_value_exprs,
      common::ObIArray<share::schema::ObPartition> &partitions,
      common::ObIArray<share::schema::ObSubPartition> &subpartitions,
      const bool &in_tablegroup = false);
    static int resolve_part_func(
      ObResolverParams &params,
      const ParseNode *node,
      const share::schema::ObPartitionFuncType partition_func_type,
      const share::schema::ObTableSchema &table_schema,
      common::ObIArray<ObRawExpr *> &part_fun_expr,
      common::ObIArray<common::ObString> &partition_keys);
    int set_partition_option_to_schema(
      share::schema::ObTableSchema &table_schema);
  int build_partition_key_info(
      share::schema::ObTableSchema &table_schema,
      common::ObSEArray<ObString, 4> &partition_keys,
      const share::schema::ObPartitionFuncType &part_func_type);
  int set_partition_keys(
      share::schema::ObTableSchema &table_schema,
      common::ObIArray<common::ObString> &partition_keys,
      bool is_subpart);
    int resolve_enum_or_set_column(
      const ParseNode *type_node,
      share::schema::ObColumnSchemaV2 &column);

  static int is_gen_col_with_udf(const ObTableSchema &table_schema,
                                 const ObRawExpr *col_expr,
                                 bool &res);

  int resolve_range_value_exprs(ParseNode *expr_list_node,
                                const share::schema::ObPartitionFuncType part_type,
                                const common::ObString &partition_name,
                                common::ObIArray<ObRawExpr *> &range_value_exprs,
                                const bool &in_tablegroup = false);

  int resolve_index_partition_node(
      ParseNode *index_partition_node,
      ObCreateIndexStmt *crt_idx_stmt);
  int check_key_cover_partition_keys(
      const bool is_range_part,
      const common::ObPartitionKeyInfo &part_key_info,
      share::schema::ObTableSchema &index_schema);
  int check_key_cover_partition_column(
      ObCreateIndexStmt *crt_idx_stmt,
      share::schema::ObTableSchema &index_schema);
  int generate_global_index_schema(
      ObCreateIndexStmt *crt_idx_stmt);
  int do_generate_global_index_schema(
      obrpc::ObCreateIndexArg &create_index_arg,
      share::schema::ObTableSchema &table_schema);
  int resolve_check_constraint_node(
      const ParseNode &cst_node,
      common::ObIArray<share::schema::ObConstraint> &csts,
      const share::schema::ObColumnSchemaV2 *column_schema = NULL);
  int resolve_check_cst_state_node_oracle(
      const ParseNode *cst_check_state_node,
      share::schema::ObConstraint &cst);
  int resolve_check_cst_state_node_mysql(const ParseNode* cst_check_state_node,
      share::schema::ObConstraint& cst);
  int resolve_pk_constraint_node(const ParseNode &cst_node,
                                 common::ObString pk_name,
                                 common::ObSEArray<share::schema::ObConstraint, 4> &csts);
  int check_split_type_valid(const ParseNode *split_node,
                             const share::schema::ObPartitionFuncType part_type);
  int resolve_foreign_key(const ParseNode *node, common::ObArray<int> &node_position_list);
  int resolve_foreign_key_node(
      const ParseNode *node,
      obrpc::ObCreateForeignKeyArg &arg,
      bool is_alter_table = false,
      const share::schema::ObColumnSchemaV2 *column = NULL);
  int resolve_fk_referenced_columns_oracle(
      const ParseNode *node,
      const obrpc::ObCreateForeignKeyArg &arg,
      bool is_alter_table,
      common::ObIArray<common::ObString> &columns);
  int resolve_foreign_key_columns(const ParseNode *node, common::ObIArray<common::ObString> &columns);
  int resolve_foreign_key_options(const ParseNode *node,
                                  share::schema::ObReferenceAction &update_action,
                                  share::schema::ObReferenceAction &delete_action);
  int resolve_foreign_key_option(const ParseNode *node,
                                 share::schema::ObReferenceAction &update_action,
                                 share::schema::ObReferenceAction &delete_action);
  int resolve_foreign_key_name(const ParseNode *constraint_node,
                               common::ObString &foreign_key_name,
                               ObNameGeneratedType &name_generated_type);
  int resolve_foreign_key_state(
      const ParseNode *fk_state_node,
      obrpc::ObCreateForeignKeyArg &arg);
  int check_foreign_key_reference(
      obrpc::ObCreateForeignKeyArg &arg,
      bool is_alter_table = false,
      const share::schema::ObColumnSchemaV2 *column = NULL);
  int resolve_match_options(const ParseNode *match_options_node);
  int create_fk_cons_name_automatically(ObString &foreign_key_name);
  int resolve_tablespace_node(const ParseNode *node, int64_t &tablespace_id);
  int resolve_not_null_constraint_node(share::schema::ObColumnSchemaV2 &column,
                                        const ParseNode *cst_node,
                                        const bool is_identity_column);
  static int add_default_not_null_constraint(share::schema::ObColumnSchemaV2 &column,
                                             const common::ObString &table_name,
                                             common::ObIAllocator &allocator,
                                             ObStmt *stmt);
  static int add_not_null_constraint(share::schema::ObColumnSchemaV2 &column,
                              const common::ObString &cst_name,
                              bool is_sys_generate_name,
                              share::schema::ObConstraint &cst,
                              common::ObIAllocator &allocator,
                              ObStmt *stmt);
  int create_name_for_empty_partition(const bool is_subpartition,
                                      ObIArray<share::schema::ObPartition> &partitions,
                                      ObIArray<share::schema::ObSubPartition> &subpartitions);
  template <typename PARTITION>
  int create_name_for_empty_partition(ObIArray<PARTITION> &partitions);
  template <typename PARTITION>
  int check_partition_name_valid(const ObIArray<PARTITION> &partitions,
                                 const common::ObString &part_name_str,
                                 bool &is_valid);
  int store_part_key(const share::schema::ObTableSchema &table_schema,
                     obrpc::ObCreateIndexArg &index_arg);
  bool is_column_exists(ObIArray<share::schema::ObColumnNameWrapper> &sort_column_array,
                        share::schema::ObColumnNameWrapper &column_key,
                        bool check_prefix_len);

  inline bool is_hash_type_partition(ObItemType type) const
  {
    return T_HASH_PARTITION == type || T_KEY_PARTITION == type;
  }
  inline bool is_range_type_partition(ObItemType type) const
  {
    return T_RANGE_PARTITION == type || T_RANGE_COLUMNS_PARTITION == type;
  }
  inline bool is_list_type_partition(ObItemType type) const
  {
    return T_LIST_PARTITION == type || T_LIST_COLUMNS_PARTITION == type;
  }

  int resolve_individual_subpartition(ObPartitionedStmt *stmt,
                                      ParseNode *part_node,
                                      ParseNode *partition_list_node,
                                      ParseNode *subpart_node,
                                      share::schema::ObTableSchema &table_schema,
                                      bool &force_template);

  int resolve_subpartition_elements(ObPartitionedStmt *stmt,
                                    ParseNode *node,
                                    share::schema::ObTableSchema &table_schema,
                                    share::schema::ObPartition *partition,
                                    bool in_tablegroup);

  int resolve_tablespace_id(ParseNode *node, int64_t &tablespace_id);

  int resolve_partition_name(ParseNode *partition_name_node,
                             ObString &partition_name,
                             share::schema::ObBasePartition &partition);

  int resolve_hash_or_key_partition_basic_infos(ParseNode *node,
                                                bool is_subpartition,
                                                share::schema::ObTableSchema &table_schema,
                                                share::schema::ObPartitionFuncType &part_func_type,
                                                ObString &func_expr_name);

  int resolve_range_partition_basic_infos(ParseNode *node,
                                          bool is_subpartition,
                                          share::schema::ObTableSchema &table_schema,
                                          share::schema::ObPartitionFuncType &part_func_type,
                                          ObString &func_expr_name,
                                          ObIArray<ObRawExpr*> &part_func_exprs);

  int resolve_list_partition_basic_infos(ParseNode *node,
                                         bool is_subpartition,
                                         share::schema::ObTableSchema &table_schema,
                                         share::schema::ObPartitionFuncType &part_func_type,
                                         ObString &func_expr_name,
                                         ObIArray<ObRawExpr*> &part_func_exprs);

  int resolve_hash_partition_elements(ObPartitionedStmt *stmt,
                                      ParseNode *node,
                                      share::schema::ObTableSchema &table_schema);

  int resolve_hash_subpartition_elements(ObPartitionedStmt *stmt,
                                         ParseNode *node,
                                         share::schema::ObTableSchema &table_schema,
                                         share::schema::ObPartition *partition);

  int resolve_range_partition_elements(ObPartitionedStmt *stmt,
                                       ParseNode *node,
                                       share::schema::ObTableSchema &table_schema,
                                       const share::schema::ObPartitionFuncType part_type,
                                       const ObIArray<ObRawExpr *> &part_func_exprs,
                                       ObIArray<ObRawExpr *> &range_value_exprs,
                                       const bool &in_tablegroup = false);

  int resolve_range_subpartition_elements(ObPartitionedStmt *stmt,
                                          ParseNode *node,
                                          share::schema::ObTableSchema &table_schema,
                                          share::schema::ObPartition *partition,
                                          const share::schema::ObPartitionFuncType part_type,
                                          const ObIArray<ObRawExpr *> &part_func_exprs,
                                          ObIArray<ObRawExpr *> &range_value_exprs,
                                          const bool &in_tablegroup = false);

  int resolve_list_partition_elements(ObPartitionedStmt*stmt,
                                      ParseNode *node,
                                      share::schema::ObTableSchema &table_schema,
                                      const share::schema::ObPartitionFuncType part_type,
                                      const ObIArray<ObRawExpr *> &part_func_exprs,
                                      ObDDLStmt::array_t &list_value_exprs,
                                      const bool &in_tablegroup = false);

  int resolve_list_subpartition_elements(ObPartitionedStmt *stmt,
                                         ParseNode *node,
                                         share::schema::ObTableSchema &table_schema,
                                         share::schema::ObPartition *partition,
                                         const share::schema::ObPartitionFuncType part_type,
                                         const ObIArray<ObRawExpr *> &part_func_exprs,
                                         ObDDLStmt::array_t &list_value_exprs,
                                         const bool &in_tablegroup = false);

  int resolve_range_partition_value_node(ParseNode &expr_list_node,
                                         const ObString &partition_name,
                                         const share::schema::ObPartitionFuncType part_type,
                                         const ObIArray<ObRawExpr *> &part_func_exprs,
                                         ObIArray<ObRawExpr *> &range_value_exprs,
                                         const bool &in_tablegroup);

  int resolve_list_partition_value_node(ParseNode &expr_list_node,
                                        const ObString &partition_name,
                                        const share::schema::ObPartitionFuncType part_type,
                                        const ObIArray<ObRawExpr *> &part_func_exprs,
                                        ObDDLStmt::array_t &list_value_exprs,
                                        const bool &in_tablegroup);

  int generate_default_hash_part(const int64_t partition_num,
                                 const int64_t tablespace_id,
                                 share::schema::ObTableSchema &table_schema);

  int generate_default_hash_subpart(ObPartitionedStmt *stmt,
                                    const int64_t partition_num,
                                    const int64_t tablespace_id,
                                    share::schema::ObTableSchema &table_schema,
                                    share::schema::ObPartition *partition);

  int generate_default_range_subpart(ObPartitionedStmt *stmt,
                                     const int64_t tablespace_id,
                                     ObTableSchema &table_schema,
                                     ObPartition *partition,
                                     ObIArray<ObRawExpr *> &range_value_exprs);

  int generate_default_list_subpart(ObPartitionedStmt *stmt,
                                    const int64_t tablespace_id,
                                    ObTableSchema &table_schema,
                                    ObPartition *partition,
                                    ObIArray<ObRawExpr *> &list_value_exprs);

  int check_and_set_partition_names(ObPartitionedStmt *stmt,
                                    share::schema::ObTableSchema &table_schema);
  int check_and_set_partition_names(ObPartitionedStmt *stmt,
                                    share::schema::ObTableSchema &table_schema,
                                    bool is_subpart);
  int check_and_set_individual_subpartition_names(ObPartitionedStmt *stmt,
                                                  share::schema::ObTableSchema &table_schema);

  int resolve_file_format(const ParseNode *node, ObExternalFileFormat &format);

  int check_format_valid(const ObExternalFileFormat &format, bool &is_valid);

  int deep_copy_string_in_part_expr(ObPartitionedStmt* stmt);
  int deep_copy_column_expr_name(common::ObIAllocator &allocator, ObIArray<ObRawExpr*> &exprs);
  int check_ttl_definition(const ParseNode *node);

  int get_ttl_columns(const ObString &ttl_definition, ObIArray<ObString> &ttl_columns);

  void reset();
  int64_t block_size_;
  int64_t consistency_level_;
  INDEX_TYPE index_scope_;
  int64_t replica_num_;
  int64_t tablet_size_;
  int64_t pctfree_;
  uint64_t tablegroup_id_;
  uint64_t index_attributes_set_;
  common::ObCharsetType charset_type_;
  common::ObCollationType collation_type_;
  bool use_bloom_filter_;
  common::ObString expire_info_;
  common::ObString compress_method_;
  common::ObString parser_name_;
  common::ObString comment_;
  common::ObString tablegroup_name_;
  common::ObString primary_zone_;
  common::ObRowStoreType row_store_type_;
  common::ObStoreFormatType store_format_;
  int64_t progressive_merge_num_;
  int64_t storage_format_version_;
  uint64_t table_id_;
  uint64_t data_table_id_; // for restore index
  uint64_t index_table_id_; // for restore index
  uint64_t virtual_column_id_; // for restore fulltext index
  bool read_only_;
  bool with_rowid_;
  common::ObString table_name_;
  common::ObString database_name_;
  share::schema::ObPartitionFuncType partition_func_type_;
  common::ObString partition_expr_;
  uint64_t auto_increment_;
  common::ObString index_name_;
  INDEX_KEYNAME index_keyname_;
  bool global_;
  common::ObArray<common::ObString> store_column_names_;
  common::ObArray<common::ObString> hidden_store_column_names_;
  common::ObArray<common::ObString> zone_list_;
  common::ObSEArray<share::schema::ObColumnNameWrapper, 16, common::ModulePageAllocator, true> sort_column_array_;
  common::hash::ObPlacementHashSet<share::schema::ObColumnNameHashWrapper,
                                   common::OB_MAX_COLUMN_NUMBER> storing_column_set_;
  common::hash::ObPlacementHashSet<share::schema::ObForeignKeyNameHashWrapper,
                                   common::OB_MAX_INDEX_PER_TABLE> current_foreign_key_name_set_;
  common::ObBitSet<> alter_table_bitset_;
  bool has_index_using_type_;
  share::schema::ObIndexUsingType index_using_type_;
  common::ObString locality_;
  bool is_random_primary_zone_;
  share::ObDuplicateScope duplicate_scope_;
  bool enable_row_movement_;
  share::schema::ObTableMode table_mode_;
  common::ObString encryption_;
  int64_t tablespace_id_;
  int64_t table_dop_; // default value is 1
  int64_t hash_subpart_num_;
  bool is_external_table_;
  common::ObString ttl_definition_;
  common::ObString kv_attributes_;
  ObNameGeneratedType name_generated_type_;
private:
  template <typename STMT>
  DISALLOW_COPY_AND_ASSIGN(ObDDLResolver);
};

//FIXME:支持非模版化二级分区
template <typename STMT>
int ObDDLResolver::resolve_split_at_partition(STMT *stmt, const ParseNode *node,
                                              const share::schema::ObPartitionFuncType part_type,
                                              const ObIArray<ObRawExpr *> &part_func_exprs,
                                              share::schema::ObPartitionSchema &t_schema,
                                              int64_t &expr_num,
                                              const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)
      || T_SPLIT_ACTION != node->type_
      || 3 != node->num_child_
      || OB_ISNULL(node->children_[AT_VALUES_NODE])
      || OB_ISNULL(node->children_[SPLIT_PARTITION_TYPE_NODE])
      || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node), "node num", node->num_child_,
                 "children[1]", node->children_[AT_VALUES_NODE], KP(stmt));
  } else if (share::schema::is_range_part(part_type)) {
    if (OB_FAIL(resolve_split_partition_range_element(node->children_[AT_VALUES_NODE],
                                                      part_type,
                                                      part_func_exprs,
                                                      stmt->get_part_values_exprs(),
                                                      in_tablegroup))) {
      SQL_RESV_LOG(WARN, "failed to resolve expr_list", K(ret));
    } else {
      expr_num = node->children_[1]->num_child_;
    }
  } else if (share::schema::is_list_part(part_type)) {
    if (OB_FAIL(resolve_split_partition_list_value(node->children_[AT_VALUES_NODE],
                                                   part_type,
                                                   part_func_exprs,
                                                   stmt->get_part_values_exprs(),
                                                   expr_num,
                                                   in_tablegroup))) {
      SQL_RESV_LOG(WARN, "failed to resolve expr_list", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    share::schema::ObPartition first_part;
    share::schema::ObPartition second_part;
    ParseNode *part_name_node = NULL;
    bool check_part_name = false;
    if (OB_NOT_NULL(node->children_[PARTITION_DEFINE_NODE])
        && OB_NOT_NULL(node->children_[PARTITION_DEFINE_NODE]->children_[0])) {
      //如果into （partition）不为空，必须有两个
      const ParseNode *part_node = node->children_[PARTITION_DEFINE_NODE]->children_[0];
      if (part_node->num_child_ != 2
          || OB_ISNULL(part_node->children_[0])
          || OB_ISNULL(part_node->children_[1])) {
        ret = OB_ERR_INVALID_SPLIT_COUNT;
        SQL_RESV_LOG(WARN,"split at two exactly partition", K(ret), "partition_num", part_node->num_child_);
        LOG_USER_ERROR(OB_ERR_INVALID_SPLIT_COUNT);
      } else if (OB_NOT_NULL(part_node->children_[0]->children_[ObResolverUtils::PARTITION_ELEMENT_NODE])
                 || OB_NOT_NULL(part_node->children_[1]->children_[ObResolverUtils::PARTITION_ELEMENT_NODE])) {
        //at的语法中，不允许显示指定最大值
        ret = OB_ERR_INVALID_SPLIT_GRAMMAR;
        SQL_RESV_LOG(WARN,"split at no need specify less than values", K(ret));
        LOG_USER_ERROR(OB_ERR_INVALID_SPLIT_GRAMMAR);
      } else if (OB_NOT_NULL(part_node->children_[0]->children_[ObResolverUtils::PARTITION_NAME_NODE])) {
        //分区名不为空的情况下，需要判断是否检查分区名冲突
        check_part_name = true;
        part_name_node = part_node->children_[0]->children_[ObResolverUtils::PARTITION_NAME_NODE];
        ObString part_name(static_cast<int32_t>(part_name_node->str_len_),
                           part_name_node->str_value_);
        if (OB_FAIL(first_part.set_part_name(part_name))) {
          SQL_RESV_LOG(WARN,"failed to set partition_name", K(ret), K(part_name));
        }
      } else {
        first_part.set_is_empty_partition_name(true);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(part_node->children_[1]->children_[ObResolverUtils::PARTITION_NAME_NODE])) {
        check_part_name = true;
        part_name_node = part_node->children_[1]->children_[ObResolverUtils::PARTITION_NAME_NODE];
        ObString part_name(static_cast<int32_t>(part_name_node->str_len_),
                           part_name_node->str_value_);
        if (OB_FAIL(second_part.set_part_name(part_name))) {
          SQL_RESV_LOG(WARN,"failed to set partition_name", K(ret), K(part_name));
        }
      } else {
        second_part.set_is_empty_partition_name(true);
      }
    } else {
      first_part.set_is_empty_partition_name(true);
      second_part.set_is_empty_partition_name(true);
    }
    if (OB_SUCC(ret) && check_part_name) {
      if (OB_FAIL(t_schema.check_part_name(first_part))) {
        SQL_RESV_LOG(WARN,"failed to check part name", K(ret), K(first_part), K(t_schema));
      } else if (OB_FAIL(t_schema.check_part_name(second_part))) {
        SQL_RESV_LOG(WARN,"failed to check part name", K(ret), K(second_part), K(t_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(t_schema.add_partition(first_part))) {
      SQL_RESV_LOG(WARN,"failed to add partition", K(ret), K(first_part), K(t_schema));
    } else if (OB_FAIL(t_schema.add_partition(second_part))) {
      SQL_RESV_LOG(WARN,"failed to add partition", K(ret), K(second_part), K(t_schema));
    }
  }
  return ret;
}

template <typename STMT>
int ObDDLResolver::resolve_split_into_partition(STMT *stmt, const ParseNode *node,
                                                const share::schema::ObPartitionFuncType part_type,
                                                const ObIArray<ObRawExpr *> &part_func_exprs,
                                                int64_t &part_num,
                                                int64_t &expr_num,
                                                share::schema::ObPartitionSchema &t_schema,
                                                const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)
      || 0 == node->num_child_
      || OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN,"invalid argument", K(ret), K(node), "node num", node->num_child_, K(stmt));
  } else {
    ParseNode *part_node = NULL;
    bool check_part_name = false;
    part_num = node->num_child_;
    expr_num = OB_INVALID_COUNT;
    if (1 == node->num_child_) {
      ret = OB_ERR_SPLIT_INTO_ONE_PARTITION;
      LOG_USER_ERROR(OB_ERR_SPLIT_INTO_ONE_PARTITION);
      SQL_RESV_LOG(WARN, "cannot split partition into one partition", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      check_part_name = false;
      part_node = node->children_[i];
      share::schema::ObPartition part;
      if (OB_ISNULL(part_node) ||
          (T_PARTITION_ELEMENT != part_node->type_ &&
           T_PARTITION_HASH_ELEMENT != part_node->type_ &&
           T_PARTITION_LIST_ELEMENT != part_node->type_ &&
           T_PARTITION_RANGE_ELEMENT != part_node->type_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN,"invalid argument", K(ret), K(part_node), "node type", part_node->type_);
      } else if (OB_NOT_NULL(part_node->children_[ObDDLResolver::PART_ID_NODE])) {
        ret = OB_ERR_PARSE_SQL;
        SQL_RESV_LOG(WARN,"only support create table with part_id", K(ret));
      } else if (OB_ISNULL(part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE])) {
        if (i == (node->num_child_ - 1)) {
          part_num--;
        } else {
          ret = OB_ERR_MISS_VALUES;
          SQL_RESV_LOG(WARN, "miss vales less than", K(ret), K(i), "node num", node->num_child_);
          LOG_USER_ERROR(OB_ERR_MISS_VALUES);
        }
      } else if (share::schema::is_range_part(part_type)) {
        if (OB_INVALID_COUNT != expr_num &&
            expr_num != part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE]->num_child_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "expr values num must equal", K(ret), K(expr_num),
                       "expr values num", part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE]->num_child_,
                       "index", i);
        } else if (OB_FAIL(resolve_split_partition_range_element(part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE],
                                                                 part_type,
                                                                 part_func_exprs,
                                                                 stmt->get_part_values_exprs(),
                                                                 in_tablegroup))) {
          SQL_RESV_LOG(WARN, "failed to resolve expr_list", K(ret));
        } else {
          expr_num = part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE]->num_child_;
        }
      } else if (share::schema::is_list_part(part_type)) {
        if (OB_FAIL(resolve_split_partition_list_value(part_node->children_[ObDDLResolver::PARTITION_ELEMENT_NODE],
                                                       part_type,
                                                       part_func_exprs,
                                                       stmt->get_part_values_exprs(),
                                                       expr_num,
                                                       in_tablegroup))) {
          SQL_RESV_LOG(WARN, "failed to resolve expr_list", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(part_node->children_[ObDDLResolver::PARTITION_NAME_NODE])) {
        check_part_name = true;
        ObString part_name(static_cast<int32_t>(part_node->children_[ObDDLResolver::PARTITION_NAME_NODE]->str_len_),
                           part_node->children_[ObDDLResolver::PARTITION_NAME_NODE]->str_value_);
        if (OB_FAIL(part.set_part_name(part_name))) {
          SQL_RESV_LOG(WARN,"failed to set partition name", K(ret), K(part_name));
        }
      } else {
        part.set_is_empty_partition_name(true);
      }
      if (OB_SUCC(ret) && check_part_name) {
        if (OB_FAIL(t_schema.check_part_name(part))) {
          SQL_RESV_LOG(WARN,"failed to check part name", K(ret), K(part), K(t_schema));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(t_schema.add_partition(part))) {
        SQL_RESV_LOG(WARN,"failed to add partition", K(ret), K(part), K(t_schema));
      }
    }//end for
  }
  return  ret;
}

/**
 * @brief create_name_for_empty_partition
 * 为用户未显示命名的分区自动命名，分区名为Pnumber
 * number从8192开始自增
 */
template <typename PARTITION>
int ObDDLResolver::create_name_for_empty_partition(ObIArray<PARTITION> &partitions)
{
  int ret = OB_SUCCESS;
  int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
  common::ObString part_name_str;
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
    PARTITION &part = partitions.at(i);
    bool is_valid = !part.is_empty_partition_name();
    while (!is_valid) {
      char part_name[OB_MAX_PARTITION_NAME_LENGTH];
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH,
          pos, "P%ld", max_part_id))) {
        SQL_RESV_LOG(WARN, "failed to print databuff", K(ret), K(max_part_id));
      } else if (FALSE_IT(part_name_str.assign(part_name, static_cast<int32_t>(pos)))) {
        // never reach
      } else if (OB_FAIL(check_partition_name_valid(partitions, part_name_str, is_valid))) {
        SQL_RESV_LOG(WARN, "failed to check partition name valid", K(ret), K(part_name_str));
      } else if (is_valid) {
        if (OB_FAIL(part.set_part_name(part_name_str))) {
          SQL_RESV_LOG(WARN, "failed to set partition name", K(ret), K(part_name_str));
        } else {
          part.set_is_empty_partition_name(false);
          ++max_part_id;
        }
      } else {
        ++max_part_id;
      }
    }
  }
  return ret;
}

template <typename PARTITION>
int ObDDLResolver::check_partition_name_valid(const ObIArray<PARTITION> &partitions,
                                              const common::ObString &part_name_str,
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); ++i) {
    const PARTITION &part = partitions.at(i);
    if (part.is_empty_partition_name()) {
      // do nothing
    } else if (common::ObCharset::case_insensitive_equal(part.get_part_name(),
                                                         part_name_str)) {
      is_valid = false;
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DDL_OB_DDL_RESOLVER_H_ */
