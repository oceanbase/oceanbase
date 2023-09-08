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
#include "share/schema/ob_dependency_info.h"      // ObReferenceObjTable
#include "lib/allocator/ob_pooled_allocator.h"
namespace oceanbase
{
namespace sql
{
class ObStmt;
struct ObStmtHint;
struct ObQueryCtx;
class ObSelectStmt;

struct ObStmtLevelRefSet: public common::ObBitSet<common::OB_DEFAULT_STATEMEMT_LEVEL_COUNT>
{
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

struct expr_hash_func
{
  uint64_t operator()(ObRawExpr *expr) const
  {
    return reinterpret_cast<uint64_t>(expr);
  }
};

struct expr_equal_to
{
  bool operator()(ObRawExpr *a, ObRawExpr *b) const
  {
    return a == b;
  }
};

typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint64_t, int64_t>::AllocType,
                                    common::ObWrapperAllocator> TableHashAllocator;

/// the base class of all statements
class ObStmt
{
public:
    typedef common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ObSynonymIds;

public:
  ObStmt()
      : stmt_type_(stmt::T_NONE),
        query_ctx_(NULL),
        stmt_id_(OB_INVALID_STMT_ID)
  {
  }
  explicit ObStmt(const stmt::StmtType stmt_type)
      : stmt_type_(stmt_type),
        query_ctx_(NULL),
        stmt_id_(OB_INVALID_STMT_ID)
  {
  }
  ObStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
      : stmt_type_(type),
        query_ctx_(NULL),
        stmt_id_(OB_INVALID_STMT_ID)
  {
    UNUSED(name_pool);
  }
  virtual ~ObStmt();
  int set_stmt_id();
  int64_t get_stmt_id() const { return stmt_id_; }
  virtual int get_first_stmt(common::ObString &first_stmt);
  void set_stmt_type(const stmt::StmtType stmt_type);
  stmt::StmtType get_stmt_type() const;
  // 因为对于show，字面type是SHOW，实际stmt_type_是SELECT
  // 所以这里实现成通用方法
  static bool is_diagnostic_stmt(const stmt::StmtType type)
  {
    return stmt::T_SHOW_WARNINGS == type || stmt::T_SHOW_ERRORS == type || stmt::T_DIAGNOSTICS == type;
  }
  static bool is_show_trace_stmt(const stmt::StmtType type)
  {
    return stmt::T_SHOW_TRACE == type;
  }

  virtual bool has_global_variable() const { return false; }
  virtual bool is_show_stmt() const;
  inline bool is_select_stmt() const { return is_select_stmt(stmt_type_); }
  inline bool is_insert_stmt() const { return stmt::T_INSERT == stmt_type_ || stmt::T_REPLACE == stmt_type_; }
  inline bool is_insert_all_stmt() const { return stmt::T_INSERT_ALL == stmt_type_; }
  inline bool is_merge_stmt() const { return stmt::T_MERGE == stmt_type_; }
  inline bool is_update_stmt() const { return stmt::T_UPDATE == stmt_type_; }
  inline bool is_delete_stmt() const { return stmt::T_DELETE == stmt_type_; }
  inline bool is_explain_stmt() const { return stmt::T_EXPLAIN == stmt_type_; }
  inline bool is_help_stmt() const { return stmt::T_HELP == stmt_type_; }
  bool is_dml_stmt() const;
  bool is_pdml_supported_stmt() const;
  bool is_px_dml_supported_stmt() const;
  bool is_dml_write_stmt() const
  { return is_dml_write_stmt(stmt_type_); }
  bool is_support_batch_exec_stmt() const
  {
    return stmt_type_ == stmt::T_INSERT
            || stmt_type_ == stmt::T_REPLACE
            || stmt_type_ == stmt::T_UPDATE
            || stmt_type_ == stmt::T_DELETE;
  }
  bool is_valid_transform_stmt() const
  {
    return stmt_type_ == stmt::T_SELECT
            || stmt_type_ == stmt::T_DELETE
            || stmt_type_ == stmt::T_UPDATE
            || stmt_type_ == stmt::T_INSERT_ALL
            || stmt_type_ == stmt::T_INSERT
            || stmt_type_ == stmt::T_REPLACE
            || stmt_type_ == stmt::T_MERGE;
  }

  bool is_sel_del_upd() const
  {
    return stmt_type_ == stmt::T_SELECT
            || stmt_type_ == stmt::T_DELETE
            || stmt_type_ == stmt::T_UPDATE;
  }

  bool is_allowed_reroute() const
  {
    return (stmt_type_ == stmt::T_SELECT
            || stmt_type_ == stmt::T_DELETE
            || stmt_type_ == stmt::T_UPDATE
            || stmt_type_ == stmt::T_INSERT
            || stmt_type_ == stmt::T_INSERT_ALL
            || stmt_type_ == stmt::T_REPLACE
            || stmt_type_ == stmt::T_MERGE);
  }

  inline bool is_support_instead_of_trigger_stmt() const {
    return stmt::T_DELETE == stmt_type_
           || stmt::T_UPDATE == stmt_type_
           || stmt::T_INSERT == stmt_type_;
  }

  static inline bool is_show_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type >= stmt::T_SHOW_TABLES && stmt_type <= stmt::T_SHOW_GRANTS)
           || stmt_type == stmt::T_SHOW_TRIGGERS;
  }

  static inline bool is_dml_write_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT
            || stmt_type == stmt::T_INSERT_ALL
            || stmt_type == stmt::T_REPLACE
            || stmt_type == stmt::T_DELETE
            || stmt_type == stmt::T_UPDATE
            || stmt_type == stmt::T_MERGE);
  }
  static inline bool is_write_stmt(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return is_ddl_stmt(stmt_type, has_global_variable) ||
         (stmt_type == stmt::T_INSERT ||
          stmt_type == stmt::T_INSERT_ALL ||
          stmt_type == stmt::T_REPLACE ||
          stmt_type == stmt::T_DELETE ||
          stmt_type == stmt::T_UPDATE);
  }

  static inline bool is_select_stmt(stmt::StmtType stmt_type)
  {
    return stmt_type == stmt::T_SELECT;
  }

  static inline bool is_dml_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_SELECT
            || stmt_type == stmt::T_INSERT
            || stmt_type == stmt::T_INSERT_ALL
            || stmt_type == stmt::T_REPLACE
            || stmt_type == stmt::T_MERGE
            || stmt_type == stmt::T_DELETE
            || stmt_type == stmt::T_UPDATE
            || stmt_type == stmt::T_EXPLAIN
            || is_show_stmt(stmt_type));
  }

  static inline bool is_execute_stmt(stmt::StmtType stmt_type)
  {
    return stmt_type == stmt::T_EXECUTE;
  }

  static inline bool is_pdml_supported_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT
            || stmt_type == stmt::T_DELETE
            || stmt_type == stmt::T_UPDATE
            || stmt_type == stmt::T_MERGE);
  }

  static inline bool is_px_dml_supported_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_INSERT
            || stmt_type == stmt::T_INSERT_ALL
            || stmt_type == stmt::T_DELETE
            || stmt_type == stmt::T_UPDATE
            || stmt_type == stmt::T_REPLACE
            || stmt_type == stmt::T_MERGE);
  }

  static bool is_dynamic_supported_stmt(stmt::StmtType stmt_type)
  {
    return !(stmt::T_KILL == stmt_type);
  }

  static inline bool is_savepoint_stmt(stmt::StmtType stmt_type)
  {
    return (stmt::T_CREATE_SAVEPOINT == stmt_type
            || stmt::T_ROLLBACK_SAVEPOINT == stmt_type
            || stmt::T_RELEASE_SAVEPOINT == stmt_type);
  }

  static inline bool is_tcl_stmt(stmt::StmtType stmt_type)
  {
    return (stmt_type == stmt::T_START_TRANS || stmt_type == stmt::T_END_TRANS);
  }

  static inline bool is_ddl_stmt(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (
        // tenant resource
         stmt_type == stmt::T_CREATE_RESOURCE_POOL
            || stmt_type == stmt::T_DROP_RESOURCE_POOL
            || stmt_type == stmt::T_ALTER_RESOURCE_POOL
            || stmt_type == stmt::T_SPLIT_RESOURCE_POOL
            || stmt_type == stmt::T_MERGE_RESOURCE_POOL
            || stmt_type == stmt::T_CREATE_RESOURCE_UNIT
            || stmt_type == stmt::T_ALTER_RESOURCE_UNIT
            || stmt_type == stmt::T_DROP_RESOURCE_UNIT
            || stmt_type == stmt::T_CREATE_TENANT
            || stmt_type == stmt::T_CREATE_STANDBY_TENANT
            || stmt_type == stmt::T_DROP_TENANT
            || stmt_type == stmt::T_MODIFY_TENANT
            || stmt_type == stmt::T_LOCK_TENANT
            // database
            || stmt_type == stmt::T_CREATE_DATABASE
            || stmt_type == stmt::T_ALTER_DATABASE
            || stmt_type == stmt::T_DROP_DATABASE
            // tablegroup
            || stmt_type == stmt::T_CREATE_TABLEGROUP
            || stmt_type == stmt::T_ALTER_TABLEGROUP
            || stmt_type == stmt::T_DROP_TABLEGROUP
            // table
            || stmt_type == stmt::T_CREATE_TABLE
            || stmt_type == stmt::T_DROP_TABLE
            || stmt_type == stmt::T_RENAME_TABLE
            || stmt_type == stmt::T_TRUNCATE_TABLE
            || stmt_type == stmt::T_CREATE_TABLE_LIKE
            || stmt_type == stmt::T_ALTER_TABLE
            || stmt_type == stmt::T_SET_TABLE_COMMENT
            // column
            || stmt_type == stmt::T_SET_COLUMN_COMMENT
            // audit and noaudit
            || stmt_type == stmt::T_AUDIT
            // analyze, 这个在 oracle 里属于 ddl，但是 ob 判定其为 ddl 时会有一些问题
            // TODO:待溪峰处理完 analyze 的问题后放开
            //|| stmt_type == stmt::T_ANALYZE
            // optimize
            || stmt_type == stmt::T_OPTIMIZE_TABLE
            || stmt_type == stmt::T_OPTIMIZE_TENANT
            || stmt_type == stmt::T_OPTIMIZE_ALL
            // view
            || stmt_type == stmt::T_CREATE_VIEW
            || stmt_type == stmt::T_ALTER_VIEW
            || stmt_type == stmt::T_DROP_VIEW
            // index
            || stmt_type == stmt::T_CREATE_INDEX
            || stmt_type == stmt::T_DROP_INDEX
            // flashback
            || stmt_type == stmt::T_FLASHBACK_TENANT
            || stmt_type == stmt::T_FLASHBACK_DATABASE
            || stmt_type == stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN
            || stmt_type == stmt::T_FLASHBACK_TABLE_TO_SCN
            || stmt_type == stmt::T_FLASHBACK_INDEX
            // purge
            || stmt_type == stmt::T_PURGE_RECYCLEBIN
            || stmt_type == stmt::T_PURGE_TENANT
            || stmt_type == stmt::T_PURGE_DATABASE
            || stmt_type == stmt::T_PURGE_TABLE
            || stmt_type == stmt::T_PURGE_INDEX
            // outline
            || stmt_type == stmt::T_CREATE_OUTLINE
            || stmt_type == stmt::T_ALTER_OUTLINE
            || stmt_type == stmt::T_DROP_OUTLINE
            // sequence
            || stmt_type == stmt::T_CREATE_SEQUENCE
            || stmt_type == stmt::T_ALTER_SEQUENCE
            || stmt_type == stmt::T_DROP_SEQUENCE

            // grant and revoke
            || stmt_type == stmt::T_GRANT
            || stmt_type == stmt::T_REVOKE

            //synonym
            || stmt_type == stmt::T_CREATE_SYNONYM
            || stmt_type == stmt::T_DROP_SYNONYM

            // variable
            //目前只有set global variable才是DDL操作，session级别的variable变更不是DDL
            || (stmt_type == stmt::T_VARIABLE_SET && has_global_variable)

            // stored procedure
            || stmt_type == stmt::T_CREATE_ROUTINE
            || stmt_type == stmt::T_DROP_ROUTINE
            || stmt_type == stmt::T_ALTER_ROUTINE

            // package
            || stmt_type == stmt::T_CREATE_PACKAGE
            || stmt_type == stmt::T_CREATE_PACKAGE_BODY
            || stmt_type == stmt::T_ALTER_PACKAGE
            || stmt_type == stmt::T_DROP_PACKAGE

            // trigger
            || stmt_type == stmt::T_CREATE_TRIGGER
            || stmt_type == stmt::T_DROP_TRIGGER
            || stmt_type == stmt::T_ALTER_TRIGGER

            // user define type
            || stmt_type == stmt::T_CREATE_TYPE
            || stmt_type == stmt::T_DROP_TYPE

            // trigger
            || stmt_type == stmt::T_CREATE_TRIGGER
            || stmt_type == stmt::T_DROP_TRIGGER
            || stmt_type == stmt::T_ALTER_TRIGGER
            || stmt_type == stmt::T_CREATE_DBLINK
            || stmt_type == stmt::T_DROP_DBLINK

            // keystore
            || stmt_type == stmt::T_CREATE_KEYSTORE
            || stmt_type == stmt::T_ALTER_KEYSTORE
            // tablespace
            || stmt_type == stmt::T_CREATE_TABLESPACE
            || stmt_type == stmt::T_ALTER_TABLESPACE
            || stmt_type == stmt::T_DROP_TABLESPACE
            // user function
            || stmt_type == stmt::T_CREATE_FUNC
            || stmt_type == stmt::T_DROP_FUNC
            || (stmt_type == stmt::T_CREATE_USER && lib::is_oracle_mode())
            // directory
            || stmt_type == stmt::T_CREATE_DIRECTORY
            || stmt_type == stmt::T_DROP_DIRECTORY
            // application context
            || stmt_type == stmt::T_CREATE_CONTEXT
            || stmt_type == stmt::T_DROP_CONTEXT
            );
  }

  static inline bool is_ddl_stmt_allowed_in_dropping_tenant(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (// tenant resource
            stmt_type == stmt::T_CREATE_RESOURCE_POOL
            || stmt_type == stmt::T_DROP_RESOURCE_POOL
            || stmt_type == stmt::T_ALTER_RESOURCE_POOL
            || stmt_type == stmt::T_SPLIT_RESOURCE_POOL
            || stmt_type == stmt::T_MERGE_RESOURCE_POOL
            || stmt_type == stmt::T_CREATE_RESOURCE_UNIT
            || stmt_type == stmt::T_ALTER_RESOURCE_UNIT
            || stmt_type == stmt::T_DROP_RESOURCE_UNIT
            || stmt_type == stmt::T_CREATE_TENANT
            || stmt_type == stmt::T_CREATE_STANDBY_TENANT
            || stmt_type == stmt::T_DROP_TENANT
            || stmt_type == stmt::T_MODIFY_TENANT
            || stmt_type == stmt::T_LOCK_TENANT
            // database
            || stmt_type == stmt::T_ALTER_DATABASE
            || stmt_type == stmt::T_DROP_DATABASE
            // tablegroup
            || stmt_type == stmt::T_ALTER_TABLEGROUP
            || stmt_type == stmt::T_DROP_TABLEGROUP
            // table
            || stmt_type == stmt::T_DROP_TABLE
            || stmt_type == stmt::T_ALTER_TABLE
            // view
            || stmt_type == stmt::T_DROP_VIEW
            // index
            || stmt_type == stmt::T_DROP_INDEX
            // purge
            || stmt_type == stmt::T_PURGE_RECYCLEBIN
            || stmt_type == stmt::T_PURGE_TENANT
            || stmt_type == stmt::T_PURGE_DATABASE
            || stmt_type == stmt::T_PURGE_TABLE
            || stmt_type == stmt::T_PURGE_INDEX
            // outline
            || stmt_type == stmt::T_DROP_OUTLINE
            // sequence
            || stmt_type == stmt::T_DROP_SEQUENCE
            //synonym
            || stmt_type == stmt::T_DROP_SYNONYM
            // variable
            //目前只有set global variable才是DDL操作，session级别的variable变更不是DDL
            || (stmt_type == stmt::T_VARIABLE_SET && has_global_variable)
            // stored procedure
            || stmt_type == stmt::T_DROP_ROUTINE
            // package
            || stmt_type == stmt::T_DROP_PACKAGE
            // user define type
            || stmt_type == stmt::T_DROP_TYPE
            //tablespace
            || stmt_type == stmt::T_DROP_TABLESPACE
            //udf
            || stmt_type == stmt::T_DROP_FUNC
            //trigger
            || stmt_type == stmt::T_DROP_TRIGGER
            );
  }

  static inline bool is_ddl_stmt_allowed_in_creating_tenant(stmt::StmtType stmt_type, bool has_global_variable)
  {
    return (
        // tenant resource
         stmt_type == stmt::T_CREATE_RESOURCE_POOL
            || stmt_type == stmt::T_DROP_RESOURCE_POOL
            || stmt_type == stmt::T_ALTER_RESOURCE_POOL
            || stmt_type == stmt::T_SPLIT_RESOURCE_POOL
            || stmt_type == stmt::T_MERGE_RESOURCE_POOL
            || stmt_type == stmt::T_CREATE_RESOURCE_UNIT
            || stmt_type == stmt::T_ALTER_RESOURCE_UNIT
            || stmt_type == stmt::T_DROP_RESOURCE_UNIT
            || stmt_type == stmt::T_CREATE_TENANT
            || stmt_type == stmt::T_CREATE_STANDBY_TENANT
            || stmt_type == stmt::T_DROP_TENANT
            || stmt_type == stmt::T_MODIFY_TENANT
            || stmt_type == stmt::T_LOCK_TENANT
            // variable
            //目前只有set global variable才是DDL操作，session级别的variable变更不是DDL
            || (stmt_type == stmt::T_VARIABLE_SET && !has_global_variable));
  }

  static bool is_dcl_stmt(stmt::StmtType stmt_type)
  { return (stmt_type >= stmt::T_CREATE_USER && stmt_type <= stmt::T_REVOKE)
            // user profile
            || stmt_type == stmt::T_CREATE_PROFILE
            || stmt_type == stmt::T_ALTER_PROFILE
            || stmt_type == stmt::T_DROP_PROFILE
            || stmt_type == stmt::T_ALTER_USER_PROFILE
            || stmt_type == stmt::T_ALTER_USER_PRIMARY_ZONE
            || stmt_type == stmt::T_ALTER_USER
            //
            || stmt_type == stmt::T_CREATE_ROLE
            || stmt_type == stmt::T_DROP_ROLE
            || stmt_type == stmt::T_SET_ROLE
            || stmt_type == stmt::T_ALTER_ROLE
            || stmt_type == stmt::T_GRANT_ROLE
            || stmt_type == stmt::T_REVOKE_ROLE
            //
            || stmt_type == stmt::T_SYSTEM_GRANT
            || stmt_type == stmt::T_SYSTEM_REVOKE;
  }

  static bool check_change_tenant_stmt(stmt::StmtType stmt_type)
  {
    return is_dml_stmt(stmt_type)
           || is_tcl_stmt(stmt_type)
           || stmt_type == stmt::T_HELP
           || stmt_type == stmt::T_VARIABLE_SET
           || stmt_type == stmt::T_USE_DATABASE
           || stmt_type == stmt::T_EMPTY_QUERY
           // TODO: When T_LOCK_TABLE is actually implemented, needs to be checked for legitimacy
           || stmt_type == stmt::T_LOCK_TABLE
           || stmt_type == stmt::T_CHANGE_TENANT;
  }

  // following stmt don't do retry
  static bool force_skip_retry_stmt(stmt::StmtType stmt_type)
  {
      return stmt_type == stmt::T_CHANGE_TENANT;
  }

  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(N_STMT_TYPE, ((int)stmt_type_));
    J_OBJ_END();
    return pos;
  }
  int assign(const ObStmt &other);
  int deep_copy(const ObStmt &other);
  bool get_fetch_cur_time() const;
  int64_t get_pre_param_size() const;
  void increase_question_marks_count();
  int64_t get_question_marks_count() const;
  void set_query_ctx(ObQueryCtx *query_ctx) { query_ctx_ = query_ctx; }
  ObQueryCtx *get_query_ctx() const { return query_ctx_; }
  int add_calculable_item(const ObHiddenColumnItem &calcuable_item);
  const common::ObIArray<ObHiddenColumnItem> &get_calculable_exprs() const;
  common::ObIArray<ObHiddenColumnItem> &get_calculable_exprs();
  int check_synonym_id_exist(uint64_t synonym_id, bool &is_exist);
  int add_global_dependency_table(const share::schema::ObSchemaObjVersion &dependency_table);
  const common::ObIArray<share::schema::ObSchemaObjVersion> *get_global_dependency_table() const;
  common::ObIArray<share::schema::ObSchemaObjVersion> *get_global_dependency_table();
  int add_ref_obj_version(const uint64_t dep_obj_id,
                          const uint64_t dep_db_id,
                          const share::schema::ObObjectType dep_obj_type,
                          const share::schema::ObSchemaObjVersion &ref_obj_version,
                          common::ObIAllocator &allocator);
  const share::schema::ObReferenceObjTable *get_ref_obj_table() const;
  share::schema::ObReferenceObjTable *get_ref_obj_table();
  virtual int init_stmt(TableHashAllocator &table_hash_alloc, ObWrapperAllocator &wrapper_alloc) { return common::OB_SUCCESS; }
protected:
  void print_indentation(FILE *fp, int32_t level) const;

public:
  static const int64_t MAX_PRINTABLE_SIZE = 2*1024*1024;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStmt);
//protected:
public:
  // 实际stmt类型，即：resolver改写后的类型
  stmt::StmtType  stmt_type_;
  // 字面stmt类型，例如show语句的字面类型为show，而stmt_type_为SELECT
  ObQueryCtx *query_ctx_;
  int64_t stmt_id_;
};

inline void ObStmt::set_stmt_type(stmt::StmtType stmt_type)
{
  stmt_type_ = stmt_type;
}

inline stmt::StmtType ObStmt::get_stmt_type() const
{
  return stmt_type_;
}

inline void ObStmt::print_indentation(FILE *fp, int32_t level) const
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

class ObStmtFactory
{
public:
  explicit ObStmtFactory(common::ObIAllocator &alloc)
    : allocator_(alloc),
      wrapper_allocator_(&alloc),
      table_hash_allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, wrapper_allocator_),
      stmt_store_(alloc),
      free_list_(alloc),
      query_ctx_(NULL)
  {
  }
  ~ObStmtFactory() { destory(); }

  template <typename StmtType>
  inline int create_stmt(StmtType *&stmt)
  {
    int ret = common::OB_SUCCESS;
    void *ptr = allocator_.alloc(sizeof(StmtType));

    stmt = NULL;
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "no more memory to stmt");
    } else {
      stmt = new(ptr) StmtType();
      if (OB_FAIL(stmt_store_.store_obj(stmt))) {
        SQL_RESV_LOG(WARN, "store stmt failed", K(ret));
        stmt->~StmtType();
        stmt = NULL;
      } else if (OB_FAIL(stmt->init_stmt(table_hash_allocator_, wrapper_allocator_))) {
        SQL_RESV_LOG(WARN, "failed to init tables hash", K(ret));
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
  ObQueryCtx *get_query_ctx();
  inline common::ObIAllocator &get_allocator() { return allocator_; }
private:
  common::ObIAllocator &allocator_;
  common::ObWrapperAllocator wrapper_allocator_;
  TableHashAllocator table_hash_allocator_;
  common::ObObjStore<ObStmt*, common::ObIAllocator&, true> stmt_store_;
  common::ObObjStore<ObSelectStmt*, common::ObIAllocator&, true> free_list_;
  ObQueryCtx *query_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStmtFactory);
};
}
}

#endif //OCEANBASE_SQL_OB_STMT_H_
