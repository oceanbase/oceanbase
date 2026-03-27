/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TRANSFORM_MV_REWRITE_PREPARE_H
#define _OB_TRANSFORM_MV_REWRITE_PREPARE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_resolver.h"

namespace oceanbase
{
namespace sql
{

enum QueryRewriteEnabledType {
  REWRITE_ENABLED_FALSE = 0,
  REWRITE_ENABLED_TRUE     ,
  REWRITE_ENABLED_FORCE
};

enum QueryRewriteIntegrityType {
  REWRITE_INTEGRITY_ENFORCED        = 0,
  REWRITE_INTEGRITY_STALE_TOLERATED
};

class ObTransformMVRewritePrepare
{
  public:
  explicit ObTransformMVRewritePrepare(ObTransformerCtx *ctx)
     : ctx_(ctx) {}
  ~ObTransformMVRewritePrepare() {}

  int prepare_mv_rewrite_info(const ObDMLStmt *stmt);
  static int generate_mv_stmt(MvInfo &mv_info,
                              ObTransformerCtx *ctx,
                              ObQueryCtx *temp_query_ctx);
  static int resolve_temp_stmt(const ObString &sql_string,
                               ObTransformerCtx *ctx,
                               ObQueryCtx *query_ctx,
                               ObSelectStmt *&output_stmt);

  private:
  int need_do_prepare(const ObDMLStmt *stmt,
                      bool &need_prepare);
  int check_table_has_mv(const ObDMLStmt *stmt,
                         bool &has_mv);
  int check_sys_var_and_hint(const ObDMLStmt *stmt,
                             bool &need_prepare);
  int recursive_check_hint(const ObDMLStmt *stmt,
                           bool &need_prepare);
  int prepare_mv_info(const ObDMLStmt *root_stmt);
  int get_mv_list(const ObDMLStmt *root_stmt,
                  ObIArray<uint64_t> &mv_list,
                  ObIArray<uint64_t> &intersect_tbl_num);
  int get_base_table_id_string(const ObDMLStmt *stmt,
                               ObSqlString &table_ids);
  int get_all_base_table_id(const ObDMLStmt *stmt,
                            ObIArray<uint64_t> &table_ids);
  int generate_mv_info(ObIArray<uint64_t> &mv_list,
                       ObIArray<uint64_t> &intersect_tbl_num);
  int sort_mv_infos();
  int quick_rewrite_check(const ObSQLSessionInfo &session_info,
                          const ObTableSchema &mv_schema,
                          bool allow_stale,
                          bool &is_valid);

  private:
    ObTransformerCtx *ctx_;
};


} //namespace sql
} //namespace oceanbase
#endif