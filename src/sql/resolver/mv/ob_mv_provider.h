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

namespace oceanbase
{
namespace sql
{

class ObMVProvider
{
public:
  explicit ObMVProvider(int64_t tenant_id, uint64_t mview_id, bool for_rt_expand = false)
    : inner_alloc_("MVProvider", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::DEFAULT_CTX_ID),
      mview_id_(mview_id),
      for_rt_expand_(for_rt_expand),
      inited_(false),
      refreshable_type_(OB_MV_REFRESH_INVALID),
      operators_(&inner_alloc_),
      dependency_infos_(&inner_alloc_)
    {}
  ~ObMVProvider() {}

  int init_mv_provider(ObSchemaGetterGuard *schema_guard, ObSQLSessionInfo *session_info)
  { return init_mv_provider(share::SCN(), share::SCN(), schema_guard, session_info);  }
  int init_mv_provider(const share::SCN &last_refresh_scn,
                       const share::SCN &refresh_scn,
                       ObSchemaGetterGuard *schema_guard,
                       ObSQLSessionInfo *session_info);
  int check_mv_refreshable(bool &can_fast_refresh) const;
  int get_fast_refresh_operators(const ObIArray<ObString> *&operators) const;
  int get_real_time_mv_expand_view(ObIAllocator &alloc, ObString &expand_view) const;
  int get_mv_dependency_infos(ObIArray<ObDependencyInfo> &dep_infos) const;
private:
  int check_mv_column_type(const ObTableSchema *mv_schema, const ObSelectStmt *view_stmt);
  int check_mv_column_type(const ObColumnSchemaV2 &org_column, const ObColumnSchemaV2 &cur_column);
  int check_column_type_and_accuracy(const ObColumnSchemaV2 &org_column,
                                     const ObColumnSchemaV2 &cur_column,
                                     bool &is_match);
  int generate_mv_stmt(ObIAllocator &alloc,
                       ObStmtFactory &stmt_factory,
                       ObRawExprFactory &expr_factory,
                       ObSchemaChecker &schema_checker,
                       ObSQLSessionInfo &session_info,
                       const uint64_t mv_id,
                       const ObTableSchema *&mv_schema,
                       const ObSelectStmt *&view_stmt);

  TO_STRING_KV(K_(mview_id), K_(for_rt_expand), K_(inited), K_(refreshable_type),
                K_(operators), K_(dependency_infos));

private:
  common::ObArenaAllocator inner_alloc_;
  const uint64_t mview_id_;
  const bool for_rt_expand_;
  bool inited_;
  ObMVRefreshableType refreshable_type_;
  common::ObFixedArray<ObString, common::ObIAllocator> operators_;  // refresh or real time access operator for mv
  common::ObFixedArray<ObDependencyInfo, common::ObIAllocator> dependency_infos_;
};

}
}

#endif
