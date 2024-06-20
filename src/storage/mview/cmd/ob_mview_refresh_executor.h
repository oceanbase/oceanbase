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

#include "lib/container/ob_array.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"
#include "storage/mview/ob_mview_refresh_stats_collect.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
struct ObMViewRefreshArg
{
public:
  ObMViewRefreshArg()
    : push_deferred_rpc_(false),
      refresh_after_errors_(false),
      purge_option_(0),
      parallelism_(0),
      heap_size_(0),
      atomic_refresh_(false),
      nested_(false),
      out_of_place_(false),
      skip_ext_data_(false),
      refresh_parallel_(0)
  {
  }
  bool is_valid() const { return !list_.empty(); }
  TO_STRING_KV(K_(list), K_(method));

public:
  ObString list_;
  ObString method_;
  ObString rollback_seg_;
  bool push_deferred_rpc_;
  bool refresh_after_errors_;
  int64_t purge_option_;
  int64_t parallelism_;
  int64_t heap_size_;
  bool atomic_refresh_;
  bool nested_;
  bool out_of_place_;
  bool skip_ext_data_;
  int64_t refresh_parallel_;
};

class ObMViewRefreshExecutor
{
public:
  ObMViewRefreshExecutor();
  ~ObMViewRefreshExecutor();
  DISABLE_COPY_ASSIGN(ObMViewRefreshExecutor);

  int execute(sql::ObExecContext &ctx, const ObMViewRefreshArg &arg);

private:
  int resolve_arg(const ObMViewRefreshArg &arg);
  int do_refresh();

private:
  sql::ObExecContext *ctx_;
  const ObMViewRefreshArg *arg_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObSchemaChecker schema_checker_;

  uint64_t tenant_id_;
  ObArray<uint64_t> mview_ids_;
  ObArray<share::schema::ObMVRefreshMethod> refresh_methods_;
};

} // namespace storage
} // namespace oceanbase
