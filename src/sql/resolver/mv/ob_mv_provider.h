/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_MV_OB_MV_PROVIDER_H_
#define OCEANBASE_SQL_RESOLVER_MV_OB_MV_PROVIDER_H_
#include "share/scn.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/resolver/mv/ob_mv_printer.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMViewInfo;
}
}
namespace sql
{

struct ObRtMViewInfo;

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
      compat_version_(0),
      rt_mv_last_refresh_ts_(0),
      checker_result_()
    {}

    virtual ~ObMVProvider();
  static int check_mv_refreshable(const uint64_t tenant_id,
                                  const uint64_t mview_id,
                                  ObSQLSessionInfo *session_info,
                                  ObSchemaGetterGuard *schema_guard,
                                  uint64_t compat_version,
                                  bool &can_fast_refresh,
                                  FastRefreshableNotes &note);
  static int get_real_time_mv_expand_view(const uint64_t tenant_id,
                                          const uint64_t mview_id,
                                          ObSQLSessionInfo *session_info,
                                          ObSchemaGetterGuard *schema_guard,
                                          ObIAllocator &alloc,
                                          ObString &expand_view,
                                          bool &is_major_refresh_mview,
                                          ObIArray<ObRtMViewInfo> &rt_mview_infos);
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
  /**
   * Align SELECT aliases with the MV container columns used by refresh operators.
   * @param stmt MV definition statement to update.
   * @param container_table_schema MV container schema that owns the physical column names.
   * @return OB_SUCCESS on success; an error code if the ordinal mapping is invalid.
   */
  static int align_mv_stmt_output_column_names(ObSelectStmt *stmt,
                                               const ObTableSchema &container_table_schema);
  static int gen_dep_column_alias_name(const ObSelectStmt &stmt,
                                       int64_t &idx,
                                       ObIAllocator &alloc,
                                       ObString &alias_name);
  int get_mlog_mv_refresh_infos(ObSQLSessionInfo *session_info,
                                ObSchemaGetterGuard *schema_guard,
                                const ObMVRefreshMethod refresh_method,
                                const bool initial_refresh_finished,
                                const uint64_t compat_version,
                                ObIArray<ObDependencyInfo> &dep_infos,
                                ObIArray<uint64_t> &tables_need_mlog,
                                bool &can_fast_refresh);
  int get_major_refresh_operators(ObSQLSessionInfo *session_info,
                                  ObSchemaGetterGuard *schema_guard,
                                  const ObMVPrinterRefreshInfo &refresh_info,
                                  ObIArray<ObString> &operators);

  OB_INLINE const ObSqlString &get_error_str() const { return checker_result_.get_note().error_; }

  static int check_mview_dep_session_vars(const ObTableSchema &mv_schema,
                                          const ObSQLSessionInfo &session,
                                          const bool gen_error,
                                          bool &is_vars_matched);

  OB_INLINE bool is_major_refresh_mview()
  { return ObMVRefreshableType::OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == checker_result_.get_refresh_type(); }

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
                       const uint64_t compat_version);
  int get_rt_mview_info(const share::schema::ObMViewInfo &mview_info,
                        ObIArray<ObRtMViewInfo> &rt_mview_infos,
                        ObRtMViewInfo &rt_mview_info) const;
  static int collect_tables_need_mlog(const ObSelectStmt* stmt,
                                      ObIArray<uint64_t> &tables_need_mlog);
  static int create_select_item_for_mv_stmt(ObSelectStmt *stmt,
                                            ObRawExpr *select_expr,
                                            const int64_t sub_stmt_idx,
                                            ObString &alias_name,
                                            ObRawExprFactory &expr_factory);
  TO_STRING_KV(K_(mview_id),
               K_(inited),
               KPC_(mv_def_stmt),
               "refreshable_type",
               checker_result_.get_refresh_type(),
               "fast_refreshable_note",
               checker_result_.get_note());

private:
  bool inited_;
  lib::MemoryContext mem_ctx_;
  ObStmtFactory *stmt_factory_;
  ObRawExprFactory *expr_factory_;
  const uint64_t mview_id_;
  ObSelectStmt *mv_def_stmt_; // definition of mv after transform and pre process
  const ObTableSchema *mv_schema_;
  const ObTableSchema *mv_container_schema_;
  uint64_t compat_version_;
  int64_t rt_mv_last_refresh_ts_;
  ObMVCheckerResult checker_result_;
};

}  // namespace sql
}  // namespace oceanbase

#endif
