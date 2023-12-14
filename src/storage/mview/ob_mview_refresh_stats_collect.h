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

#include "share/schema/ob_mview_refresh_stats.h"

namespace oceanbase
{
namespace storage
{
class ObMViewRefreshArg;
class ObMViewRefreshCtx;

struct ObMViewRefreshStatsCollection
{
public:
  ObMViewRefreshStatsCollection();
  ~ObMViewRefreshStatsCollection();
  DISABLE_COPY_ASSIGN(ObMViewRefreshStatsCollection);

  int init(sql::ObExecContext &ctx, const uint64_t tenant_id, const int64_t refresh_id,
           const uint64_t mview_id);
  int clear_for_retry();

  int collect_before_refresh(ObMViewRefreshCtx &refresh_ctx);
  int collect_after_refresh(ObMViewRefreshCtx &refresh_ctx);
  int collect_stmt_stats(ObMViewRefreshCtx &refresh_ctx, const ObString &stmt,
                         const int64_t execution_time);

  int commit(ObISQLClient &sql_client);

#define DEFINE_BASIC_SETTER(type, name) \
  OB_INLINE void set_##name(type name) { refresh_stats_.set_##name(name); }

  DEFINE_BASIC_SETTER(share::schema::ObMVRefreshType, refresh_type);
  DEFINE_BASIC_SETTER(int64_t, start_time);
  DEFINE_BASIC_SETTER(int64_t, end_time);
  DEFINE_BASIC_SETTER(int64_t, elapsed_time);
  DEFINE_BASIC_SETTER(int64_t, log_purge_time);
  DEFINE_BASIC_SETTER(int64_t, num_steps);
  DEFINE_BASIC_SETTER(int, result);

#undef DEFINE_BASIC_SETTER

  TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(mview_id), K_(retry_id), K_(collection_level),
               K_(refresh_stats), K_(change_stats_array), K_(stmt_stats_array));

public:
  sql::ObExecContext *ctx_;
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t retry_id_;
  share::schema::ObMVRefreshStatsCollectionLevel collection_level_;
  share::schema::ObMViewRefreshStats refresh_stats_;
  ObArray<share::schema::ObMViewRefreshChangeStats> change_stats_array_;
  ObArray<share::schema::ObMViewRefreshStmtStats> stmt_stats_array_;
  bool is_inited_;
};

class ObMViewRefreshStatsCollector
{
public:
  ObMViewRefreshStatsCollector();
  ~ObMViewRefreshStatsCollector();
  DISABLE_COPY_ASSIGN(ObMViewRefreshStatsCollector);

  int init(sql::ObExecContext &ctx, const ObMViewRefreshArg &refresh_arg, const uint64_t tenant_id,
           const int64_t refresh_id, const int64_t mv_cnt);
  int alloc_collection(const uint64_t mview_id, ObMViewRefreshStatsCollection *&stats_collection);
  int commit();
  TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(run_stats));

private:
  typedef common::hash::ObHashMap<uint64_t, ObMViewRefreshStatsCollection *,
                                  common::hash::NoPthreadDefendMode>
    MVRefStatsMap;

  ObArenaAllocator allocator_;
  sql::ObExecContext *ctx_;
  uint64_t tenant_id_;
  int64_t refresh_id_;
  share::schema::ObMViewRefreshRunStats run_stats_;
  MVRefStatsMap mv_ref_stats_map_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
