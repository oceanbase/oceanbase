/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_PROVIDER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_PROVIDER_H_
#include "share/scn.h"
#include "lib/string/ob_string.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/mv/ob_mv_printer.h"

namespace oceanbase
{
namespace sql
{

enum RtMvCheckType {
  NO_NEED_RT_EXPAND = 0,
  DEPENDS_ON_SCHEMA = 1,
  NEED_RT_EXPAND = 2,
};

class ObMVProvider
{
public:
  explicit ObMVProvider(const uint64_t mview_id)
    : inited_(false),
      mem_ctx_(NULL),
      stmt_factory_(NULL),
      expr_factory_(NULL),
      mview_id_(mview_id),
      mv_def_stmt_(NULL),
      mv_schema_(NULL),
      mv_container_schema_(NULL),
      refreshable_type_(OB_MV_REFRESH_INVALID),
      checker_info_(),
      fast_refreshable_note_()
    {}
    virtual ~ObMVProvider();
  static int check_mv_refreshable(const uint64_t tenant_id,
                                  const uint64_t mview_id,
                                  ObSQLSessionInfo *session_info,
                                  ObSchemaGetterGuard *schema_guard,
                                  bool &can_fast_refresh,
                                  FastRefreshableNotes &note);
  static int get_real_time_mv_expand_view(const uint64_t tenant_id,
                                          const uint64_t mview_id,
                                          ObSQLSessionInfo *session_info,
                                          ObSchemaGetterGuard *schema_guard,
                                          ObIAllocator &alloc,
                                          ObString &expand_view,
                                          bool &is_major_refresh_mview);
  static int get_complete_refresh_mview_str(const ObTableSchema &mv_schema,
                                            ObSchemaGetterGuard &schema_guard,
                                            const share::SCN *mv_refresh_scn,
                                            const share::SCN *table_refresh_scn,
                                            ObIAllocator &str_alloc,
                                            ObString &mview_str);
  static int expand_mv_stmt_with_dependent_columns(ObSelectStmt *view_stmt,
                                                   const ObIArray<std::pair<ObRawExpr*, int64_t>> &dependent_columns,
                                                   ObIAllocator &alloc,
                                                   ObSQLSessionInfo *session_info,
                                                   ObRawExprFactory &expr_factory);
  static int gen_dep_column_alias_name(const ObSelectStmt &stmt,
                                       int64_t &idx,
                                       ObIAllocator &alloc,
                                       ObString &alias_name);
  int get_mlog_mv_refresh_infos(ObSQLSessionInfo *session_info,
                                ObSchemaGetterGuard *schema_guard,
                                const ObMVRefreshMethod refresh_method,
                                ObIArray<ObDependencyInfo> &dep_infos,
                                ObIArray<uint64_t> &tables_need_mlog,
                                bool &can_fast_refresh);
  int get_major_refresh_operators(ObSQLSessionInfo *session_info,
                                  ObSchemaGetterGuard *schema_guard,
                                  const ObMVPrinterRefreshInfo &refresh_info,
                                  ObIArray<ObString> &operators);
  OB_INLINE const ObSqlString &get_error_str() const  { return fast_refreshable_note_.error_; }
  static int check_mview_dep_session_vars(const ObTableSchema &mv_schema,
                                          const ObSQLSessionInfo &session,
                                          const bool gen_error,
                                          bool &is_vars_matched);
  OB_INLINE bool is_major_refresh_mview()
  { return ObMVRefreshableType::OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == refreshable_type_; }
  static int transform_mv_def_stmt(ObDMLStmt *&mv_def_stmt,
                                   ObIAllocator *allocator,
                                   ObSchemaChecker *schema_checker,
                                   ObSQLSessionInfo *session_info,
                                   ObRawExprFactory *expr_factory,
                                   ObStmtFactory *stmt_factory);
  static int generate_mv_stmt(ObIAllocator &alloc,
                              ObStmtFactory &stmt_factory,
                              ObRawExprFactory &expr_factory,
                              ObSchemaChecker &schema_checker,
                              ObSQLSessionInfo &session_info,
                              const ObTableSchema &mv_schema,
                              ObSelectStmt *&view_stmt);
  int print_mv_operators(ObSQLSessionInfo *session_info,
                         const ObMVPrinterRefreshInfo *refresh_info,
                         ObIAllocator &str_alloc,
                         ObIArray<ObString> &operators);
private:
  int init_mv_provider(ObSQLSessionInfo *session_info,
                       ObSchemaGetterGuard *schema_guard,
                       const RtMvCheckType rt_expand_type,
                       ObTableReferencedColumnsInfo *table_referenced_columns_info = NULL);
  int check_mv_column_type(const ObTableSchema *mv_schema, const ObSelectStmt *view_stmt,
                           ObSQLSessionInfo &session);
  int check_mv_column_type(const ObColumnSchemaV2 &org_column, const ObColumnSchemaV2 &cur_column);
  int check_column_type_and_accuracy(const ObColumnSchemaV2 &org_column,
                                     const ObColumnSchemaV2 &cur_column,
                                     bool &is_match);
  static int collect_tables_need_mlog(const ObSelectStmt* stmt,
                                      ObIArray<uint64_t> &tables_need_mlog);
  static int check_fast_refresh_dep_consistency(const uint64_t tenant_id,
                                                const uint64_t mview_id,
                                                const ObReferenceObjTable &ref_objs,
                                                bool &can_fast_refresh);
  static int transform_preprocess_mv_def_stmt(ObDMLStmt *mv_def_stmt,
                                              ObSQLSessionInfo *session_info,
                                              ObRawExprFactory *expr_factory);
  static int get_trans_rule_set(const ObDMLStmt *mv_def_stmt,
                                uint64_t &rule_set);
  static int create_select_item_for_mv_stmt(ObSelectStmt *stmt,
                                            ObRawExpr *select_expr,
                                            const int64_t sub_stmt_idx,
                                            ObString &alias_name,
                                            ObRawExprFactory &expr_factory);
  TO_STRING_KV(K_(mview_id), K_(inited), KPC_(mv_def_stmt), K_(refreshable_type), K_(fast_refreshable_note));

private:
  bool inited_;
  lib::MemoryContext mem_ctx_;
  ObStmtFactory *stmt_factory_;
  ObRawExprFactory *expr_factory_;
  const uint64_t mview_id_;
  ObSelectStmt *mv_def_stmt_; // definition of mv after transform and pre process
  const ObTableSchema *mv_schema_;
  const ObTableSchema *mv_container_schema_;
  ObMVRefreshableType refreshable_type_;
  MvCheckerExtraInfo checker_info_;
  FastRefreshableNotes fast_refreshable_note_;
};

}
}

#endif
