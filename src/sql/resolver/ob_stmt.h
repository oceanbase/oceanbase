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

#ifndef OCEANBASE_SQL_OB_STMT_H_
#define OCEANBASE_SQL_OB_STMT_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_print_utils.h"
#include "common/row/ob_row_desc.h"
#include "share/ob_autoincrement_param.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_stmt_type.h"
#include "share/stat/ob_opt_stat_manager.h"
namespace oceanbase {
namespace sql {
class ObStmt;
struct ObStmtHint;
struct ObQueryCtx;
class ObSelectStmt;

struct ObStmtLevelRefSet : public common::ObBitSet<common::OB_DEFAULT_STATEMEMT_LEVEL_COUNT> {
  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    int32_t num = 0;
    for (int32_t i = 0; i < bit_count(); ++i) {
      if (has_member(i)) {
        if (num != 0) {
          J_COMMA();
        }
        BUF_PRINTO(i);
        ++num;
      }
    }
    J_ARRAY_END();
    return pos;
  }
};

struct expr_hash_func {
  uint64_t operator()(ObRawExpr* expr) const
  {
    return reinterpret_cast<uint64_t>(expr);
  }
};

struct expr_equal_to {
  bool operator()(ObRawExpr* a, ObRawExpr* b) const
  {
    return a == b;
  }
};

/// the base class of all statements
class ObStmt {
public:
  typedef common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ObSynonymIds;

public:
  ObStmt()
      : stmt_type_(stmt::T_NONE),
        literal_stmt_type_(stmt::T_NONE),
        sql_stmt_(),
        sql_stmt_coll_type_(CS_TYPE_INVALID),
        query_ctx_(NULL),
        stmt_id_(0),
        prepare_param_count_(0),
        is_prepare_stmt_(false),
        has_nested_sql_(false),
        synonym_id_store_(),
        tz_info_(NULL)
  {}
  explicit ObStmt(const stmt::StmtType stmt_type)
      : stmt_type_(stmt_type),
        literal_stmt_type_(stmt::T_NONE),
        sql_stmt_(),
        sql_stmt_coll_type_(CS_TYPE_INVALID),
        query_ctx_(NULL),
        stmt_id_(0),
        prepare_param_count_(0),
        is_prepare_stmt_(false),
        has_nested_sql_(false),
        synonym_id_store_(),
        tz_info_(NULL)
  {}
  ObStmt(common::ObIAllocator* name_pool, stmt::StmtType type)
      : stmt_type_(type),
        literal_stmt_type_(stmt::T_NONE),
        sql_stmt_(),
        sql_stmt_coll_type_(CS_TYPE_INVALID),
        query_ctx_(NULL),
        stmt_id_(0),
        prepare_param_count_(0),
        is_prepare_stmt_(false),
        has_nested_sql_(false),
        synonym_id_store_(),
        tz_info_(NULL)
  {
    UNUSED(name_pool);
  }
  virtual ~ObStmt();
  void set_stmt_id();
  int64_t get_stmt_id() const
  {
    return stmt_id_;
  }
  int get_stmt_name_by_id(int64_t stmt_id, common::ObString& stmt_name) const;
  int get_stmt_org_name_by_id(int64_t stmt_id, common::ObString& org_name) const;
  int get_stmt_name(common::ObString& stmt_name) const;
  int get_stmt_org_name(common::ObString& org_name) const;
  virtual int get_first_stmt(common::ObString& first_stmt);
  void set_stmt_type(const stmt::StmtType stmt_type);
  stmt::StmtType get_stmt_type() const;
  void set_literal_stmt_type(const stmt::StmtType type)
  {
    literal_stmt_type_ = type;
  }
  stmt::StmtType get_literal_stmt_type() const
  {
    return literal_stmt_type_;
  }
  static bool is_diagnostic_stmt(const stmt::StmtType type)
  {
    return stmt::T_SHOW_WARNINGS == type || stmt::T_SHOW_ERRORS == type;
  }
  static bool is_show_trace_stmt(const stmt::StmtType type)
  {
    return stmt::T_SHOW_TRACE == type;
  }

  virtual bool has_global_variable() const
  {
    return false;
  }
  virtual bool is_show_stmt() const;
  inline bool is_select_stmt() const
  {
    return is_select_stmt(stmt_type_);
  }
  inline bool is_insert_stmt() const
  {
    return stmt::T_INSERT == stmt_type_ || stmt::T_REPLACE == stmt_type_;
  }
  inline bool is_merge_stmt() const
  {
    return stmt::T_MERGE == stmt_type_;
  }
  inline bool is_update_stmt() const
  {
    return stmt::T_UPDATE == stmt_type_;
  }
  inline bool is_delete_stmt() const
  {
    return stmt::T_DELETE == stmt_type_;
  }
  inline bool is_explain_stmt() const
  {
    return stmt::T_EXPLAIN == stmt_type_;
  }
  bool is_dml_stmt() const;
  bool is_pdml_supported_stmt() const;
  bool is_px_dml_supported_stmt() const;
  bool is_dml_write_stmt() const
  {
    return is_dml_write_stmt(stmt_type_);
  }
  bool is_valid_transform_stmt() const
  {
    return stmt_type_ == stmt::T_SELECT || stmt_type_ == stmt::T_DELETE || stmt_type_ == stmt::T_UPDATE ||
           stmt_type_ == stmt::T_INSERT || stmt_type_ == stmt::T_REPLACE || stmt_type_ == stmt::T_MERGE;
  }

  bool is_sel_del_upd() const
  {
    return stmt_type_ == stmt::T_SELECT || stmt_type_ == stmt::T_DELETE || stmt_type_ == stmt::T_UPDATE;
  }

  bool is_allowed_reroute() const
  {
    return (stmt_type_ == stmt::T_SELECT || stmt_type_ == stmt::T_DELETE || stmt_type_ == stmt::T_UPDATE ||
            stmt_type_ == stmt::T_INSERT || stmt_type_ == stmt::T_REPLACE || stmt_type_ == stmt::T_MERGE);
  }

  static inline bool is_show_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type >= stmt::T_SHOW_TABLES && stmt_type <= stmt::T_SHOW_GRANTS);
  }

  static inline bool is_dml_write_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT || stmt_type == stmt::T_REPLACE || stmt_type == stmt::T_DELETE ||
            stmt_type == stmt::T_UPDATE || stmt_type == stmt::T_MERGE);
  }
  static inline bool is_write_stmt(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return is_ddl_stmt(stmt_type, has_global_variable) ||
           (stmt_type == stmt::T_INSERT || stmt_type == stmt::T_REPLACE || stmt_type == stmt::T_DELETE ||
               stmt_type == stmt::T_UPDATE);
  }

  static inline bool is_select_stmt(stmt::StmtType stmt_type)
  {
    return stmt_type == stmt::T_SELECT;
  }

  static inline bool is_dml_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_SELECT || stmt_type == stmt::T_INSERT || stmt_type == stmt::T_REPLACE ||
            stmt_type == stmt::T_MERGE || stmt_type == stmt::T_DELETE || stmt_type == stmt::T_UPDATE ||
            stmt_type == stmt::T_EXPLAIN || is_show_stmt(stmt_type));
  }

  static inline bool is_execute_stmt(stmt::StmtType stmt_type)
  {
    return stmt_type == stmt::T_EXECUTE;
  }

  static inline bool is_pdml_supported_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT || stmt_type == stmt::T_DELETE || stmt_type == stmt::T_UPDATE);
  }

  static inline bool is_px_dml_supported_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT || stmt_type == stmt::T_DELETE || stmt_type == stmt::T_UPDATE ||
            stmt_type == stmt::T_REPLACE || stmt_type == stmt::T_MERGE);
  }

  static inline bool is_savepoint_stmt(stmt::StmtType stmt_type)
  {
    return (stmt::T_CREATE_SAVEPOINT == stmt_type || stmt::T_ROLLBACK_SAVEPOINT == stmt_type ||
            stmt::T_RELEASE_SAVEPOINT == stmt_type);
  }

  static inline bool is_tcl_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_START_TRANS || stmt_type == stmt::T_END_TRANS);
  }

  static inline bool is_ddl_stmt(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (
        // tenant resource
        stmt_type == stmt::T_CREATE_RESOURCE_POOL || stmt_type == stmt::T_DROP_RESOURCE_POOL ||
        stmt_type == stmt::T_ALTER_RESOURCE_POOL || stmt_type == stmt::T_SPLIT_RESOURCE_POOL ||
        stmt_type == stmt::T_MERGE_RESOURCE_POOL || stmt_type == stmt::T_CREATE_RESOURCE_UNIT ||
        stmt_type == stmt::T_ALTER_RESOURCE_UNIT || stmt_type == stmt::T_DROP_RESOURCE_UNIT ||
        stmt_type == stmt::T_CREATE_TENANT || stmt_type == stmt::T_DROP_TENANT || stmt_type == stmt::T_MODIFY_TENANT ||
        stmt_type == stmt::T_LOCK_TENANT
        // database
        || stmt_type == stmt::T_CREATE_DATABASE || stmt_type == stmt::T_ALTER_DATABASE ||
        stmt_type == stmt::T_DROP_DATABASE
        // tablegroup
        || stmt_type == stmt::T_CREATE_TABLEGROUP || stmt_type == stmt::T_ALTER_TABLEGROUP ||
        stmt_type == stmt::T_DROP_TABLEGROUP
        // table
        || stmt_type == stmt::T_CREATE_TABLE || stmt_type == stmt::T_DROP_TABLE || stmt_type == stmt::T_RENAME_TABLE ||
        stmt_type == stmt::T_TRUNCATE_TABLE || stmt_type == stmt::T_CREATE_TABLE_LIKE ||
        stmt_type == stmt::T_ALTER_TABLE ||
        stmt_type == stmt::T_SET_TABLE_COMMENT
        // column
        || stmt_type == stmt::T_SET_COLUMN_COMMENT
        // audit and noaudit
        || stmt_type == stmt::T_AUDIT
        // optimize
        || stmt_type == stmt::T_OPTIMIZE_TABLE || stmt_type == stmt::T_OPTIMIZE_TENANT ||
        stmt_type == stmt::T_OPTIMIZE_ALL
        // view
        || stmt_type == stmt::T_CREATE_VIEW || stmt_type == stmt::T_ALTER_VIEW ||
        stmt_type == stmt::T_DROP_VIEW
        // index
        || stmt_type == stmt::T_CREATE_INDEX ||
        stmt_type == stmt::T_DROP_INDEX
        // flashback
        || stmt_type == stmt::T_FLASHBACK_TENANT
        || stmt_type == stmt::T_FLASHBACK_DATABASE
        || stmt_type == stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN
        || stmt_type == stmt::T_FLASHBACK_INDEX
        // purge
        || stmt_type == stmt::T_PURGE_RECYCLEBIN || stmt_type == stmt::T_PURGE_TENANT ||
        stmt_type == stmt::T_PURGE_DATABASE || stmt_type == stmt::T_PURGE_TABLE ||
        stmt_type == stmt::T_PURGE_INDEX
        // outline
        || stmt_type == stmt::T_CREATE_OUTLINE || stmt_type == stmt::T_ALTER_OUTLINE ||
        stmt_type == stmt::T_DROP_OUTLINE
        // sequence
        || stmt_type == stmt::T_CREATE_SEQUENCE || stmt_type == stmt::T_ALTER_SEQUENCE ||
        stmt_type == stmt::T_DROP_SEQUENCE

        // grant and revoke
        || stmt_type == stmt::T_GRANT ||
        stmt_type == stmt::T_REVOKE

        // synonym
        || stmt_type == stmt::T_CREATE_SYNONYM ||
        stmt_type == stmt::T_DROP_SYNONYM

        // variable
        || (stmt_type == stmt::T_VARIABLE_SET && has_global_variable)

        // plan baseline
        || stmt_type == stmt::T_ALTER_BASELINE

        // stored procedure
        || stmt_type == stmt::T_CREATE_ROUTINE || stmt_type == stmt::T_DROP_ROUTINE ||
        stmt_type == stmt::T_ALTER_ROUTINE

        // package
        || stmt_type == stmt::T_CREATE_PACKAGE || stmt_type == stmt::T_CREATE_PACKAGE_BODY ||
        stmt_type == stmt::T_ALTER_PACKAGE ||
        stmt_type == stmt::T_DROP_PACKAGE

        // trigger
        || stmt_type == stmt::T_CREATE_TRIGGER || stmt_type == stmt::T_DROP_TRIGGER ||
        stmt_type == stmt::T_ALTER_TRIGGER

        // user define type
        || stmt_type == stmt::T_CREATE_TYPE ||
        stmt_type == stmt::T_DROP_TYPE

        // trigger
        || stmt_type == stmt::T_CREATE_DBLINK ||
        stmt_type == stmt::T_DROP_DBLINK

        // user function
        || stmt_type == stmt::T_CREATE_FUNC || stmt_type == stmt::T_DROP_FUNC ||
        (stmt_type == stmt::T_CREATE_USER && lib::is_oracle_mode()));
  }

  static inline bool is_ddl_stmt_allowed_in_dropping_tenant(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (  // tenant resource
        stmt_type == stmt::T_CREATE_RESOURCE_POOL || stmt_type == stmt::T_DROP_RESOURCE_POOL ||
        stmt_type == stmt::T_ALTER_RESOURCE_POOL || stmt_type == stmt::T_SPLIT_RESOURCE_POOL ||
        stmt_type == stmt::T_MERGE_RESOURCE_POOL || stmt_type == stmt::T_CREATE_RESOURCE_UNIT ||
        stmt_type == stmt::T_ALTER_RESOURCE_UNIT || stmt_type == stmt::T_DROP_RESOURCE_UNIT ||
        stmt_type == stmt::T_CREATE_TENANT || stmt_type == stmt::T_DROP_TENANT || stmt_type == stmt::T_MODIFY_TENANT ||
        stmt_type == stmt::T_LOCK_TENANT
        // database
        || stmt_type == stmt::T_ALTER_DATABASE ||
        stmt_type == stmt::T_DROP_DATABASE
        // tablegroup
        || stmt_type == stmt::T_ALTER_TABLEGROUP ||
        stmt_type == stmt::T_DROP_TABLEGROUP
        // table
        || stmt_type == stmt::T_DROP_TABLE ||
        stmt_type == stmt::T_ALTER_TABLE
        // view
        || stmt_type == stmt::T_DROP_VIEW
        // index
        || stmt_type == stmt::T_DROP_INDEX
        // flashback
        || stmt_type == stmt::T_FLASHBACK_TENANT		
        || stmt_type == stmt::T_FLASHBACK_DATABASE		
        || stmt_type == stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN		
        || stmt_type == stmt::T_FLASHBACK_INDEX
        // purge
        || stmt_type == stmt::T_PURGE_RECYCLEBIN || stmt_type == stmt::T_PURGE_TENANT ||
        stmt_type == stmt::T_PURGE_DATABASE || stmt_type == stmt::T_PURGE_TABLE ||
        stmt_type == stmt::T_PURGE_INDEX
        // outline
        || stmt_type == stmt::T_DROP_OUTLINE
        // sequence
        || stmt_type == stmt::T_DROP_SEQUENCE
        // synonym
        || stmt_type == stmt::T_DROP_SYNONYM
        // variable
        || (stmt_type == stmt::T_VARIABLE_SET && has_global_variable)
        // stored procedure
        || stmt_type == stmt::T_DROP_ROUTINE
        // package
        || stmt_type == stmt::T_DROP_PACKAGE
        // user define type
        || stmt_type == stmt::T_DROP_TYPE
        // udf
        || stmt_type == stmt::T_DROP_FUNC);
  }

  static inline bool is_ddl_stmt_allowed_in_creating_tenant(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (
        // tenant resource
        stmt_type == stmt::T_CREATE_RESOURCE_POOL || stmt_type == stmt::T_DROP_RESOURCE_POOL ||
        stmt_type == stmt::T_ALTER_RESOURCE_POOL || stmt_type == stmt::T_SPLIT_RESOURCE_POOL ||
        stmt_type == stmt::T_MERGE_RESOURCE_POOL || stmt_type == stmt::T_CREATE_RESOURCE_UNIT ||
        stmt_type == stmt::T_ALTER_RESOURCE_UNIT || stmt_type == stmt::T_DROP_RESOURCE_UNIT ||
        stmt_type == stmt::T_CREATE_TENANT || stmt_type == stmt::T_DROP_TENANT || stmt_type == stmt::T_MODIFY_TENANT ||
        stmt_type == stmt::T_LOCK_TENANT
        // variable
        || (stmt_type == stmt::T_VARIABLE_SET && !has_global_variable));
  }

  static bool is_dcl_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type >= stmt::T_CREATE_USER && stmt_type <= stmt::T_REVOKE)
           // user profile
           || stmt_type == stmt::T_USER_PROFILE || stmt_type == stmt::T_ALTER_USER_PROFILE ||
           stmt_type == stmt::T_ALTER_USER_PRIMARY_ZONE ||
           stmt_type == stmt::T_ALTER_USER
           //
           || stmt_type == stmt::T_CREATE_ROLE || stmt_type == stmt::T_DROP_ROLE || stmt_type == stmt::T_SET_ROLE ||
           stmt_type == stmt::T_ALTER_ROLE || stmt_type == stmt::T_GRANT_ROLE ||
           stmt_type == stmt::T_REVOKE_ROLE
           //
           || stmt_type == stmt::T_SYSTEM_GRANT || stmt_type == stmt::T_SYSTEM_REVOKE;
  }

  static bool check_change_tenant_stmt(stmt::StmtType stmt_type)
  {
    return is_dml_stmt(stmt_type) || is_tcl_stmt(stmt_type) || stmt_type == stmt::T_VARIABLE_SET ||
           stmt_type == stmt::T_USE_DATABASE || stmt_type == stmt::T_EMPTY_QUERY || stmt_type == stmt::T_CHANGE_TENANT;
  }

  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
    return pos;
  }
  int assign(const ObStmt& other);
  int deep_copy(const ObStmt& other);
  bool get_fetch_cur_time() const;
  int64_t get_pre_param_size() const;
  void increase_question_marks_count();
  int64_t get_question_marks_count() const;
  void set_query_ctx(ObQueryCtx* query_ctx)
  {
    query_ctx_ = query_ctx;
  }
  ObQueryCtx* get_query_ctx()
  {
    return query_ctx_;
  }
  const ObQueryCtx* get_query_ctx() const
  {
    return query_ctx_;
  }
  int distribute_hint_in_query_ctx(common::ObIAllocator* allocator);
  int add_calculable_item(const ObHiddenColumnItem& calcuable_item);
  const common::ObIArray<ObHiddenColumnItem>& get_calculable_exprs() const;
  common::ObIArray<ObHiddenColumnItem>& get_calculable_exprs();
  int check_table_id_exists(uint64_t table_id, bool& is_exist);
  int check_synonym_id_exist(uint64_t synonym_id, bool& is_exist);
  common::ObIArray<ObRawExpr*>& get_exec_param_ref_exprs() const;
  common::ObIArray<ObRawExpr*>& get_exec_param_ref_exprs();
  inline common::ObString& get_sql_stmt()
  {
    return sql_stmt_;
  }
  inline const common::ObString& get_sql_stmt() const
  {
    return sql_stmt_;
  }
  inline void set_sql_stmt(const char* sql, int32_t sql_len)
  {
    sql_stmt_.assign_ptr(sql, sql_len);
  }
  inline void set_sql_stmt(const common::ObString sql_stmt)
  {
    sql_stmt_ = sql_stmt;
  }
  void set_prepare_param_count(const int64_t prepare_param_count)
  {
    prepare_param_count_ = prepare_param_count;
  }
  int64_t get_prepare_param_count() const
  {
    return prepare_param_count_;
  }
  bool is_prepare_stmt() const
  {
    return is_prepare_stmt_;
  }
  void set_is_prepare_stmt(bool is_prepare)
  {
    is_prepare_stmt_ = is_prepare;
  }
  bool has_nested_sql() const
  {
    return has_nested_sql_;
  }
  void set_has_nested_sql(bool has_nested_sql)
  {
    has_nested_sql_ = has_nested_sql;
  }
  inline const ObSynonymIds& get_synonym_id_store() const
  {
    return synonym_id_store_;
  }
  int add_synonym_ids(const common::ObIArray<uint64_t>& synonym_ids, bool error_with_repeate);
  int add_global_dependency_table(const share::schema::ObSchemaObjVersion& dependency_table);
  const common::ObIArray<share::schema::ObSchemaObjVersion>* get_global_dependency_table() const;
  common::ObIArray<share::schema::ObSchemaObjVersion>* get_global_dependency_table();
  int add_table_stat_version(const common::ObOptTableStatVersion& table_stat_version);
  const common::ObIArray<common::ObOptTableStatVersion>* get_table_stat_versions() const;
  void set_sql_stmt_coll_type(common::ObCollationType coll_type)
  {
    sql_stmt_coll_type_ = coll_type;
  }
  common::ObCollationType get_sql_stmt_coll_type()
  {
    return sql_stmt_coll_type_;
  }

protected:
  void print_indentation(FILE* fp, int32_t level) const;

public:
  static const int64_t MAX_PRINTABLE_SIZE = 2 * 1024 * 1024;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStmt);
  // protected:
public:
  stmt::StmtType stmt_type_;
  stmt::StmtType literal_stmt_type_;
  common::ObString sql_stmt_;
  common::ObCollationType sql_stmt_coll_type_;
  ObQueryCtx* query_ctx_;
  int64_t stmt_id_;
  int64_t prepare_param_count_;
  bool is_prepare_stmt_;
  bool has_nested_sql_;

  // used for plan cache check synonym version
  ObSynonymIds synonym_id_store_;
  const common::ObTimeZoneInfo* tz_info_;
};

inline void ObStmt::set_stmt_type(stmt::StmtType stmt_type)
{
  stmt_type_ = stmt_type;
}

inline stmt::StmtType ObStmt::get_stmt_type() const
{
  return stmt_type_;
}

inline void ObStmt::print_indentation(FILE* fp, int32_t level) const
{
  for (int i = 0; i < level; ++i) {
    fprintf(fp, "    ");
  }
}

inline bool ObStmt::is_show_stmt() const
{
  return is_show_stmt(stmt_type_);
}

inline bool ObStmt::is_dml_stmt() const
{
  return is_dml_stmt(stmt_type_);
}

inline bool ObStmt::is_pdml_supported_stmt() const
{
  return is_pdml_supported_stmt(stmt_type_);
}

inline bool ObStmt::is_px_dml_supported_stmt() const
{
  return is_px_dml_supported_stmt(stmt_type_);
}

class ObStmtFactory {
public:
  explicit ObStmtFactory(common::ObIAllocator& alloc)
      : allocator_(alloc), stmt_store_(alloc), free_list_(alloc), query_ctx_(NULL)
  {}
  ~ObStmtFactory()
  {
    destory();
  }

  template <typename StmtType>
  inline int create_stmt(StmtType*& stmt)
  {
    int ret = common::OB_SUCCESS;
    void* ptr = allocator_.alloc(sizeof(StmtType));

    stmt = NULL;
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "no more memory to stmt");
    } else {
      stmt = new (ptr) StmtType();
      if (OB_FAIL(stmt_store_.store_obj(stmt))) {
        SQL_RESV_LOG(WARN, "store stmt failed", K(ret));
        stmt->~StmtType();
        stmt = NULL;
      }
    }
    return ret;
  }

  int free_stmt(ObSelectStmt *stmt);

  void destory();
  /**
   * @brief query_ctx is the global struct of stmts in the single query
   *        so, query_ctx is globally unique in the single query
   * @return query_ctx_
   */
  ObQueryCtx* get_query_ctx();
  inline common::ObIAllocator& get_allocator()
  {
    return allocator_;
  }

private:
  common::ObIAllocator& allocator_;
  common::ObObjStore<ObStmt*, common::ObIAllocator&, true> stmt_store_;
  common::ObObjStore<ObSelectStmt*, common::ObIAllocator&, true> free_list_;
  ObQueryCtx* query_ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStmtFactory);
};

template <>
int ObStmtFactory::create_stmt<ObSelectStmt>(ObSelectStmt*& stmt);
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_OB_STMT_H_
