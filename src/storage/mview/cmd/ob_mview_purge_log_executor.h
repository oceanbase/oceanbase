/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct ObMViewPurgeLogArg
{
public:
  ObMViewPurgeLogArg() : num_(-1), purge_log_parallel_(0) {}
  bool is_valid() const { return !master_.empty(); }
  TO_STRING_KV(K_(master), K_(num), K_(flag), K_(purge_log_parallel));

public:
  ObString master_;
  int64_t num_;
  ObString flag_;
  int64_t purge_log_parallel_;
};

class ObMViewPurgeLogExecutor
{
public:
  ObMViewPurgeLogExecutor();
  ~ObMViewPurgeLogExecutor();
  DISABLE_COPY_ASSIGN(ObMViewPurgeLogExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewPurgeLogArg &arg);

private:
  int resolve_arg(const ObMViewPurgeLogArg &arg);

private:
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  uint64_t master_table_id_;
};

} // namespace storage
} // namespace oceanbase
