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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_SHOW_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_SHOW_RESOLVER_
#include "sql/resolver/dml/ob_select_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObShowResolver : public ObSelectResolver
{
public:
  class ObShowResolverContext;
  class ShowColumnInfo;
  explicit ObShowResolver(ObResolverParams &params);
  virtual ~ObShowResolver();
  virtual int resolve(const ParseNode &parse_tree);
protected:
private:
  class ObSqlStrGenerator;
  struct ObShowSqlSet;
  int get_database_info(const ParseNode *databse_node,
                        const common::ObString &database_name,
                        uint64_t real_tenant_id,
                        ObShowResolverContext &show_resv_ctx,
                        uint64_t &show_db_id);

  int check_desc_priv_if_ness(uint64_t real_tenant_id,
                              const share::schema::ObTableSchema *table_schema,
                              const ObString &database_name,
                              bool is_sys_view);

  // in oracle mode, check_desc_priv_if_ness is called inside
  int resolve_show_from_table(const ParseNode *from_table_node,
                              const ParseNode *from_database_clause_node,
                              bool is_database_unselected,
                              ObItemType node_type,
                              uint64_t real_tenant_id,
                              common::ObString &show_database_name,
                              uint64_t &show_database_id,
                              common::ObString &show_table_name,
                              uint64_t &show_table_id,
                              bool &is_view,
                              ObSynonymChecker &synonym_checker);
  int resolve_show_from_database(const ParseNode &from_db_node,
                                 uint64_t real_tenant_id,
                                 uint64_t &show_database_id,
                                 common::ObString &show_database_name);
  int resolve_show_from_routine(const ParseNode *from_routine_node,
                                const ParseNode *from_database_clause_node,
                                bool is_database_unselected,
                                ObItemType node_type,
                                uint64_t real_tenant_id,
                                ObString &show_database_name,
                                uint64_t &show_database_id,
                                ObString &show_routine_name,
                                uint64_t &show_routine_id,
                                int64_t &proc_type);
  int resolve_show_from_trigger(const ParseNode *from_tg_node,
                                const ParseNode *from_database_clause_node,
                                bool is_database_unselected,
                                uint64_t real_tenant_id,
                                ObString &show_database_name,
                                uint64_t &show_database_id,
                                ObString &show_tg_name,
                                uint64_t &show_tg_id);
  int parse_and_resolve_select_sql(const common::ObString &select_sql);
  int resolve_like_or_where_clause(ObShowResolverContext &ctx);
  int replace_where_clause(ParseNode* expr_node, const ObShowResolverContext &show_resv_ctx);
  int process_select_type(
      ObSelectStmt *select_stmt, stmt::StmtType stmt_type, const ParseNode &parse_tree);
  virtual int resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowResolver);
};// ObShowresolver

struct ObShowResolver::ObShowSqlSet
{
#define DECLARE_SHOW_CLAUSE_SET(SHOW_STMT_TYPE)     \
  static const char *SHOW_STMT_TYPE##_SELECT;       \
  static const char *SHOW_STMT_TYPE##_SUBQUERY;     \
  static const char *SHOW_STMT_TYPE##_ORA_SUBQUERY; \
  static const char *SHOW_STMT_TYPE##_LIKE

  static const char *SUBQERY_ALIAS;
  DECLARE_SHOW_CLAUSE_SET(SHOW_TABLES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TABLES_LIKE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_FULL_TABLES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_FULL_TABLES_LIKE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CHARSET);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TABLEGROUPS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TABLEGROUPS_V2);
  DECLARE_SHOW_CLAUSE_SET(SHOW_VARIABLES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_GLOBAL_VARIABLES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_COLUMNS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_FULL_COLUMNS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_DATABASE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_DATABASE_EXISTS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_TABLEGROUP);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_TABLEGROUP_EXISTS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_INDEXES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_COLLATION);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TRACE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TRACE_JSON);
  DECLARE_SHOW_CLAUSE_SET(SHOW_ENGINES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PRIVILEGES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_QUERY_RESPONSE_TIME);
  DECLARE_SHOW_CLAUSE_SET(SHOW_GRANTS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PROCESSLIST);
  DECLARE_SHOW_CLAUSE_SET(SHOW_FULL_PROCESSLIST);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SYS_PROCESSLIST);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SYS_FULL_PROCESSLIST);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TABLE_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PROCEDURE_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_WARNINGS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_COUNT_WARNINGS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_ERRORS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_COUNT_ERRORS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PARAMETERS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_UNSYS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_COMPAT);
  DECLARE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_SEED);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SESSION_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_GLOBAL_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TENANT);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TENANT_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_TENANT);
  DECLARE_SHOW_CLAUSE_SET(SHOW_DATABASES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_DATABASES_LIKE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_DATABASES_STATUS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_DATABASES_STATUS_LIKE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_TABLE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_VIEW);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_PROCEDURE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_FUNCTION);
  DECLARE_SHOW_CLAUSE_SET(SHOW_RECYCLEBIN);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SYS_RECYCLEBIN);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TRIGGERS);
  DECLARE_SHOW_CLAUSE_SET(SHOW_RESTORE_PREVIEW);
  DECLARE_SHOW_CLAUSE_SET(SHOW_CREATE_TRIGGER);
  DECLARE_SHOW_CLAUSE_SET(SHOW_TRIGGERS_LIKE);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SEQUENCES);
  DECLARE_SHOW_CLAUSE_SET(SHOW_SEQUENCES_LIKE);
};// ObShowSqlSet

class ObShowResolver::ObSqlStrGenerator
{
public:
  ObSqlStrGenerator()
      : sql_buf_(NULL),
        sql_buf_pos_(0)
        {}
  virtual ~ObSqlStrGenerator() {}
  int init(common::ObIAllocator *alloc);
  virtual int gen_select_str(const char *select_str, ...);
  virtual int gen_from_str(const char *subquery_str, ...);
  virtual int gen_limit_str(int64_t offset, int64_t row_cnt);
  void assign_sql_str(common::ObString &sql_str);
private:
  char *sql_buf_;
  int64_t sql_buf_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObSqlStrGenerator);
};// ObSqlstrgenerator

class ObShowResolver::ObShowResolverContext
{
public:
  ObShowResolverContext()
    : cur_tenant_id_(common::OB_INVALID_ID),
      actual_tenant_id_(common::OB_INVALID_ID),
      database_name_(),
      ref_table_id_(common::OB_INVALID_ID),
      show_database_name_(),
      show_database_id_(common::OB_INVALID_ID),
      show_table_id_(common::OB_INVALID_ID),
      grants_user_name_(),
      grants_user_id_(common::OB_INVALID_ID),
      stmt_type_(stmt::T_NONE),
      global_scope_(false),
      like_pattern_(),
      like_escape_(),
      parse_tree_(NULL),
      condition_node_(NULL),
      column_name_(),
      like_column_()
  {
  }
  ~ObShowResolverContext() {}
  uint64_t cur_tenant_id_;
  uint64_t actual_tenant_id_;
  common::ObString database_name_;
  uint64_t ref_table_id_;
  common::ObString show_database_name_;
  uint64_t show_database_id_;
  uint64_t show_table_id_;
  common::ObString grants_user_name_;
  uint64_t grants_user_id_;
  stmt::StmtType stmt_type_;
  bool global_scope_;
  common::ObString like_pattern_;
  common::ObString like_escape_;
  const ParseNode *parse_tree_;
  ParseNode *condition_node_;
  common::ObString column_name_; // used for show tables
  common::ObString like_column_; // used for the show stmt who has like clause
  TO_STRING_KV(K_(cur_tenant_id),
               K_(actual_tenant_id),
               K_(database_name),
               K_(ref_table_id),
               K_(show_database_name),
               K_(show_database_id),
               K_(show_table_id),
               K_(stmt_type),
               K_(global_scope),
               K_(like_pattern),
               K_(like_escape),
               K_(column_name));
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowResolverContext);
};//ObShowResolvercontext

class ObShowResolver::ShowColumnInfo
{
public:
  ShowColumnInfo() : display_name_(), qualified_name_()
  {
  }
  ~ShowColumnInfo() {}
  common::ObString display_name_;
  ObQualifiedName qualified_name_;
  TO_STRING_KV(K_(display_name), K_(qualified_name));
private:
  DISALLOW_COPY_AND_ASSIGN(ShowColumnInfo);
};//ShowColumninfo

} // sql
} // oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_SHOW_RESOLVER_ */
