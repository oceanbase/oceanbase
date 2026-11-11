/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_CHECKER_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{
class ObSchemaChecker;
class ObStmtFactory;
struct ObTransformerCtx;
struct TableItem;

enum ObMVRefreshableType // FARM COMPAT WHITELIST
{
  OB_MV_REFRESH_INVALID  = 0,           // refresh type not set
  OB_MV_COMPLETE_REFRESH,               // can not fast refresh
  OB_MV_FAST_REFRESH_SIMPLE_MAV,        // fast refresh for single table MAV
  OB_MV_FAST_REFRESH_SIMPLE_MJV,        // fast refresh for inner join MJV
  OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV,   // fast refresh for inner join MAV
  OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV, // fast refresh for major compaction MV
  OB_MV_FAST_REFRESH_OUTER_JOIN_MJV,    // fast refresh for outer join MJV
  OB_MV_FAST_REFRESH_UNION_ALL,         // fast refresh for union all query
  OB_MV_FAST_REFRESH_OUTER_JOIN_MAV,    // fast refresh for outer join MAV
  OB_MV_FAST_REFRESH_GENERAL,           // fast refresh using relation-based generator
};

inline bool IS_VALID_FAST_REFRESH_TYPE(ObMVRefreshableType type)
{
  return type >= OB_MV_FAST_REFRESH_SIMPLE_MAV;
}

struct FastRefreshableNotes
{
  FastRefreshableNotes() : error_("ObMVChecker") {}
  ~FastRefreshableNotes() {}

  TO_STRING_KV(K_(error));

  ObSqlString error_;
};

typedef common::ObIArray<std::pair<const TableItem*, const share::schema::ObTableSchema*>> MlogSchemaPairIArray;

class ObMLogRequiredColumnsInfo
{
public:
  ObMLogRequiredColumnsInfo()
      : is_inited_(false),
        inner_alloc_("MLogReqCol", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID),
        required_columns_map_()
  {}

  ~ObMLogRequiredColumnsInfo() { reset(); }

  void reset();
  int record_required_columns(const uint64_t base_object_id, const ObIArray<uint64_t> &col_ids);
  int merge_from(const ObMLogRequiredColumnsInfo &other);
  int convert_to_required_columns_infos(ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos) const;

private:
  typedef common::hash::ObHashMap<uint64_t, common::hash::ObHashSet<uint64_t> *> MLogRequiredColumnsMap;

  int init();
  int get_or_create_required_columns(const uint64_t base_object_id,
                                     common::hash::ObHashSet<uint64_t> *&required_columns);

  bool is_inited_;
  common::ObArenaAllocator inner_alloc_;
  MLogRequiredColumnsMap required_columns_map_;
};

// Owns all outputs produced by MV refresh-ability checking.
// Core outputs are mutable only between start_check() and finish_check();
// error notes can still be updated after checking.
class ObMVCheckerResult final
{
public:
  ObMVCheckerResult()
      : state_(State::INITED),
        refresh_type_(OB_MV_REFRESH_INVALID),
        note_(),
        dependent_columns_(),
        mlog_required_columns_info_(),
        mlog_tables_(),
        union_all_marker_idx_(OB_INVALID_INDEX),
        child_refresh_types_()
  {}

  ~ObMVCheckerResult() {}

  // Check lifecycle.
  int start_check();
  int finish_check();

  // getters
  ObMVRefreshableType get_refresh_type() const { return refresh_type_; }

  FastRefreshableNotes &get_note() { return note_; }

  const FastRefreshableNotes &get_note() const { return note_; }

  const common::ObIArray<std::pair<ObRawExpr *, int64_t>> &get_dependent_columns() const { return dependent_columns_; }

  const ObMLogRequiredColumnsInfo &get_mlog_required_columns_info() const { return mlog_required_columns_info_; }

  const MlogSchemaPairIArray &get_mlog_tables() const { return mlog_tables_; }

  int64_t get_union_all_marker_idx() const { return union_all_marker_idx_; }

  const common::ObIArray<ObMVRefreshableType> &get_child_refresh_types() const { return child_refresh_types_; }

  // setters
  int assign(const ObMVCheckerResult &other);
  int set_refresh_type(const ObMVRefreshableType refresh_type);
  int add_dependent_column(ObRawExpr *dep_col, const int64_t stmt_idx);
  int record_mlog_required_columns(const uint64_t base_object_id, const ObIArray<uint64_t> &col_ids);
  int add_mlog_table(const TableItem *table_item, const share::schema::ObTableSchema *mlog_schema);
  int set_union_all_marker_idx(const int64_t marker_idx);
  int add_child_refresh_type(const ObMVRefreshableType refresh_type);

  // Diagnostic mutation is allowed after checking.
  OB_INLINE void log_error_note(const char *error)
  {
    if (OB_ISNULL(error)) {
      // do not write empty error
    } else if (!note_.error_.empty()) {
      // do not overwrite
    } else {
      note_.error_.assign(error);  // explicitly ignore ret
    }
  }

private:
  enum class State
  {
    INITED,
    IN_PROGRESS,
    FINISHED
  };

  void reset_output();

  State state_;
  ObMVRefreshableType refresh_type_;
  FastRefreshableNotes note_;  // error info
  // Dependent columns discovered during checking
  // which will be added as select exprs named `__MV_DEP_COL__xxx` during mview creation.
  // The first element is the dependent column.
  // The second element identifies the source stmt.
  // OB_INVALID_INDEX means the non-set root or the UNION ALL marker of a set stmt;
  // [0, N) identifies a set child.
  common::ObSEArray<std::pair<ObRawExpr *, int64_t>, 8> dependent_columns_;
  ObMLogRequiredColumnsInfo mlog_required_columns_info_;
  // Maps each table in the MV to its MLOG using the physical table ID.
  common::ObSEArray<std::pair<const TableItem *, const share::schema::ObTableSchema *>, 4> mlog_tables_;
  // marker column index in select list
  int64_t union_all_marker_idx_;
  // (for printer-based refresh only) refresh type of each branch
  common::ObSEArray<ObMVRefreshableType, 4> child_refresh_types_;

  DISALLOW_COPY_AND_ASSIGN(ObMVCheckerResult);
};

// Static, read-only parameters and dependencies shared by MV checkers.
struct ObMVCheckerParams
{
  ObMVCheckerParams(common::ObIAllocator &allocator,
                    ObSchemaChecker &schema_checker,
                    ObStmtFactory &stmt_factory,
                    ObRawExprFactory &expr_factory,
                    ObSQLSessionInfo &session_info,
                    const ObTableSchema &mv_schema,
                    const ObTableSchema &mv_container_table_schema,
                    uint64_t compat_version,
                    bool need_on_query_computation,
                    bool for_create_mview)
      : allocator_(allocator),
        schema_checker_(schema_checker),
        stmt_factory_(stmt_factory),
        expr_factory_(expr_factory),
        session_info_(session_info),
        mv_schema_(mv_schema),
        mv_container_table_schema_(mv_container_table_schema),
        compat_version_(compat_version),
        need_on_query_computation_(need_on_query_computation),
        for_create_mview_(for_create_mview)
  {}

  uint64_t get_tenant_id() const;

  common::ObIAllocator &allocator_;
  ObSchemaChecker &schema_checker_;
  ObStmtFactory &stmt_factory_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo &session_info_;
  const ObTableSchema &mv_schema_;
  const ObTableSchema &mv_container_table_schema_;
  uint64_t compat_version_;
  bool need_on_query_computation_;
  bool for_create_mview_;
};

class ObMVChecker
{
public:
  virtual ~ObMVChecker() {}

  // Prepares mv stmt in place and run fast refresh checker
  static int prepare_mv_stmt_and_check_refresh_type(const ObMVCheckerParams &params,
                                                    ObDMLStmt *&stmt,
                                                    ObMVCheckerResult &result);
  static int collect_dep_mview_ids_from_stmt(const ObSelectStmt &stmt,
                                             ObIArray<uint64_t> &dep_mview_ids);

protected:
  explicit ObMVChecker(const ObSelectStmt &stmt,
                       const ObMVCheckerParams &params,
                       ObMVCheckerResult &result)
      : stmt_(stmt), params_(params), result_(result)
  {}

  int set_final_refresh_type(const ObMVRefreshableType refresh_type);
  int fallback_to_complete_refresh(const char *error_note);
  int check_mv_dependency_mlog_tables(const ObSelectStmt &stmt, bool &is_valid);

  const ObSelectStmt &stmt_;
  const ObMVCheckerParams &params_;
  ObMVCheckerResult &result_;

private:
  int check_nested_mview_refresh_type(bool &can_all_mviews_fast_refresh);
  int check_fast_refresh_dep_consistency(bool &is_dep_consistent);
  static int prepare_mv_stmt_for_check(ObDMLStmt *&stmt, const ObMVCheckerParams &params);
  static int transform_preprocess_mv_stmt(ObDMLStmt *stmt,
                                          ObTransformerCtx &trans_ctx,
                                          const ObMVCheckerParams &params);
  static int transform_mv_stmt_to_full_group_by(ObDMLStmt *stmt,
                                                ObTransformerCtx &trans_ctx,
                                                const ObMVCheckerParams &params);
  static int check_enable_transform_to_full_group_by(ObDMLStmt *stmt,
                                                     const ObMVCheckerParams &params,
                                                     bool &enable_transform);
  static int get_trans_rule_set(const ObDMLStmt *stmt, const ObMVCheckerParams &params, uint64_t &rule_set);
  static int normalize_mv_stmt_for_check(ObSelectStmt &stmt, const ObMVCheckerParams &params);
  static int adjust_set_stmt_sel_item(ObSelectStmt &stmt);
  static int reset_stmt_id_recursively(ObSelectStmt *stmt);

  int check_mlog_table_valid(const share::schema::ObTableSchema *table_schema,
                             const TableItem *table_item,
                             const ObIArray<ColumnItem> &columns,
                             const share::schema::ObTableSchema &mlog_schema,
                             ObSchemaGetterGuard *schema_guard,
                             bool &is_valid);

  DISALLOW_COPY_AND_ASSIGN(ObMVChecker);
};

}  // namespace sql
}  // namespace oceanbase

#endif
