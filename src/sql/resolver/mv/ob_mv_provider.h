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

class ObMVProvider
{
public:
  explicit ObMVProvider(int64_t tenant_id, uint64_t mview_id)
    : inner_alloc_("MVProvider", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::DEFAULT_CTX_ID),
      mview_id_(mview_id),
      inited_(false),
      refreshable_type_(OB_MV_REFRESH_INVALID),
      operators_(&inner_alloc_),
      dependency_infos_(&inner_alloc_),
      tables_need_mlog_(&inner_alloc_),
      fast_refreshable_note_()
    {}
  ~ObMVProvider() {}
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
                                            ObSQLSessionInfo &session_info,
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
                                const share::SCN &last_refresh_scn,
                                const share::SCN &refresh_scn,
                                const share::SCN *mv_last_refresh_scn,
                                const share::SCN *mv_refresh_scn,
                                ObIArray<ObDependencyInfo> &dep_infos,
                                ObIArray<uint64_t> &tables_need_mlog,
                                bool &can_fast_refresh,
                                const ObIArray<ObString> *&operators);
  int get_major_refresh_operators(ObSQLSessionInfo *session_info,
                                  ObSchemaGetterGuard *schema_guard,
                                  const share::SCN &last_refresh_scn,
                                  const share::SCN &refresh_scn,
                                  const int64_t part_idx,
                                  const int64_t sub_part_idx,
                                  const ObNewRange &range,
                                  const ObIArray<ObString> *&operators);
  OB_INLINE const common::ObIArray<ObString> &get_operators() const
  { return operators_; }
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
  int get_columns_referenced_by_mv(const uint64_t tenant_id,
                                   const uint64_t mview_id,
                                   const uint64_t table_id,
                                   ObSQLSessionInfo *session_info,
                                   ObSchemaGetterGuard *schema_guard,
                                   common::hash::ObHashSet<uint64_t> &table_referenced_columns);
private:
  int init_mv_provider(ObSQLSessionInfo *session_info,
                       ObSchemaGetterGuard *schema_guard,
                       const bool check_refreshable_only,
                       ObMVPrinterRefreshInfo *refresh_info,
                       ObTableReferencedColumnsInfo *table_referenced_columns_info = nullptr);
  int check_mv_column_type(const ObTableSchema *mv_schema, const ObSelectStmt *view_stmt,
                           ObSQLSessionInfo &session);
  int check_mv_column_type(const ObColumnSchemaV2 &org_column, const ObColumnSchemaV2 &cur_column);
  int check_column_type_and_accuracy(const ObColumnSchemaV2 &org_column,
                                     const ObColumnSchemaV2 &cur_column,
                                     bool &is_match);
  static int generate_mv_stmt(ObIAllocator &alloc,
                              ObStmtFactory &stmt_factory,
                              ObRawExprFactory &expr_factory,
                              ObSchemaChecker &schema_checker,
                              ObSQLSessionInfo &session_info,
                              const ObTableSchema &mv_schema,
                              ObSelectStmt *&view_stmt);
  static int collect_tables_need_mlog(const ObSelectStmt* stmt,
                                      ObIArray<uint64_t> &tables_need_mlog);
  int print_mv_operators(ObMVPrinterCtx &mv_printer_ctx,
                         ObMVChecker &checker,
                         const ObTableSchema &mv_schema,
                         const ObTableSchema &mv_container_schema,
                         const ObSelectStmt &mv_def_stmt,
                         ObIArray<ObString> &operators);
  static int get_trans_rule_set(const ObDMLStmt *mv_def_stmt,
                                uint64_t &rule_set);
  int check_is_rt_expand(const bool check_refreshable_only, const ObTableSchema &mv_schema, const ObMVPrinterCtx &mv_printer_ctx, bool &is_rt_expand);
  static int create_select_item_for_mv_stmt(ObSelectStmt *stmt,
                                            ObRawExpr *select_expr,
                                            const int64_t sub_stmt_idx,
                                            ObString &alias_name,
                                            ObRawExprFactory &expr_factory);
  TO_STRING_KV(K_(mview_id), K_(inited), K_(refreshable_type),
                K_(operators), K_(dependency_infos), K_(tables_need_mlog));

private:
  common::ObArenaAllocator inner_alloc_;
  const uint64_t mview_id_;
  bool inited_;
  ObMVRefreshableType refreshable_type_;
  common::ObFixedArray<ObString, common::ObIAllocator> operators_;  // refresh or real time access operator for mv
  common::ObFixedArray<ObDependencyInfo, common::ObIAllocator> dependency_infos_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> tables_need_mlog_;
  FastRefreshableNotes fast_refreshable_note_;
};

}
}

#endif
