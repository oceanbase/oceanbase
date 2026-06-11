/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_array.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{

struct ObMViewExplainRefreshArg
{
public:
  ObMViewExplainRefreshArg() : nested_(false), tenant_id_(0) {}
  bool is_valid() const { return !list_.empty(); }
  TO_STRING_KV(K_(list), K_(method), K_(nested), K_(tenant_id));
public:
  ObString list_;
  ObString method_;
  bool nested_;
  // target tenant to explain; 0 means use the current session's tenant
  uint64_t tenant_id_;
};

class ObMViewExplainRefreshExecutor
{
public:
  ObMViewExplainRefreshExecutor();
  ~ObMViewExplainRefreshExecutor();
  DISABLE_COPY_ASSIGN(ObMViewExplainRefreshExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewExplainRefreshArg &arg,
              ObIAllocator &alloc, ObString &result);
private:
  int resolve_tenant(const ObMViewExplainRefreshArg &arg);
  int resolve_arg(const ObMViewExplainRefreshArg &arg);
  int do_explain_refresh(ObSqlString &output);
  int do_explain_nested_refresh(ObSqlString &output);
  int explain_single_mview(const uint64_t mview_id,
                           const share::schema::ObMVRefreshMethod refresh_method,
                           int64_t &step_no,
                           ObSqlString &output);
  int get_mview_full_name(const uint64_t mview_id, ObSqlString &full_name);
private:
  sql::ObExecContext *ctx_;
  const ObMViewExplainRefreshArg *arg_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;
  // holds the target tenant's schema guard when explaining across tenants
  share::schema::ObSchemaGetterGuard tenant_schema_guard_;
  uint64_t tenant_id_;
  ObArray<uint64_t> mview_ids_;
  ObArray<share::schema::ObMVRefreshMethod> refresh_methods_;
};

} // namespace storage
} // namespace oceanbase
