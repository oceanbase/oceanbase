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

#include "storage/mview/ob_mview_refresh_ctx.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql
namespace storage
{
class ObMViewRefreshStatsCollection;

struct ObMViewRefreshParam
{
public:
  ObMViewRefreshParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      mview_id_(OB_INVALID_ID),
      refresh_method_(share::schema::ObMVRefreshMethod::MAX),
      parallelism_(0)
  {
  }
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && mview_id_ != OB_INVALID_ID &&
           refresh_method_ != share::schema::ObMVRefreshMethod::NEVER && parallelism_ >= 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(mview_id), K_(refresh_method), K_(parallelism));

public:
  uint64_t tenant_id_;
  uint64_t mview_id_;
  share::schema::ObMVRefreshMethod refresh_method_; // MAX means use default refresh method
  uint64_t parallelism_;
};

class ObMViewRefresher
{
public:
  ObMViewRefresher();
  ~ObMViewRefresher();
  DISABLE_COPY_ASSIGN(ObMViewRefresher);

  int init(sql::ObExecContext &ctx, ObMViewRefreshCtx &refresh_ctx,
           const ObMViewRefreshParam &refresh_param,
           ObMViewRefreshStatsCollection *refresh_stats_collection);
  int refresh();

  TO_STRING_KV(KP_(ctx), KP_(refresh_ctx), K_(refresh_param), KP_(refresh_stats_collection));

private:
  int lock_mview_for_refresh();
  int prepare_for_refresh();
  int fetch_based_infos(share::schema::ObSchemaGetterGuard &schema_guard);
  int check_fast_refreshable();
  int complete_refresh();
  int fast_refresh();

private:
  sql::ObExecContext *ctx_;
  ObMViewRefreshCtx *refresh_ctx_;
  ObMViewRefreshParam refresh_param_;
  ObMViewRefreshStatsCollection *refresh_stats_collection_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
