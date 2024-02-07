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

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
struct ObMViewRefreshStatsParams : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  static const ObMVRefreshStatsCollectionLevel DEFAULT_COLLECTION_LEVEL =
    ObMVRefreshStatsCollectionLevel::TYPICAL;
  static const int64_t DEFAULT_RETENTION_PERIOD = 31;
  ObMViewRefreshStatsParams();
  ObMViewRefreshStatsParams(ObMVRefreshStatsCollectionLevel collection_level,
                            int64_t retention_period);
  explicit ObMViewRefreshStatsParams(common::ObIAllocator *allocator);
  ObMViewRefreshStatsParams(const ObMViewRefreshStatsParams &other);
  virtual ~ObMViewRefreshStatsParams();

  ObMViewRefreshStatsParams &operator=(const ObMViewRefreshStatsParams &other);
  int assign(const ObMViewRefreshStatsParams &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

  static bool is_retention_period_valid(int64_t retention_period)
  {
    return retention_period == -1 || (retention_period >= 1 && retention_period <= 365000);
  }
  static ObMViewRefreshStatsParams get_default()
  {
    return ObMViewRefreshStatsParams(DEFAULT_COLLECTION_LEVEL, DEFAULT_RETENTION_PERIOD);
  }

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

  DEFINE_GETTER_AND_SETTER(ObMVRefreshStatsCollectionLevel, collection_level);
  DEFINE_GETTER_AND_SETTER(int64_t, retention_period);

#undef DEFINE_GETTER_AND_SETTER

  // sys_defaults
  int gen_sys_defaults_dml(uint64_t tenant_id, share::ObDMLSqlSplicer &dml) const;
  static int set_sys_defaults(ObISQLClient &sql_client, uint64_t tenant_id,
                              const ObMViewRefreshStatsParams &params);
  static int fetch_sys_defaults(ObISQLClient &sql_client, uint64_t tenant_id,
                                ObMViewRefreshStatsParams &params, bool for_update = false);

  // mview_refresh_params
  int gen_mview_refresh_stats_params_dml(uint64_t tenant_id, uint64_t mview_id,
                                         share::ObDMLSqlSplicer &dml) const;
  static int set_mview_refresh_stats_params(ObISQLClient &sql_client, uint64_t tenant_id,
                                            uint64_t mview_id,
                                            const ObMViewRefreshStatsParams &params);
  static int drop_mview_refresh_stats_params(ObISQLClient &sql_client, uint64_t tenant_id,
                                             uint64_t mview_id, bool if_exists = false);
  static int drop_all_mview_refresh_stats_params(ObISQLClient &sql_client, uint64_t tenant_id,
                                                 int64_t &affected_rows, int64_t limit = -1);
  static int fetch_mview_refresh_stats_params(ObISQLClient &sql_client, uint64_t tenant_id,
                                              uint64_t mview_id, ObMViewRefreshStatsParams &params,
                                              bool with_sys_defaults);

  TO_STRING_KV(K_(collection_level), K_(retention_period));

private:
  static int read_stats_params(ObISQLClient &sql_client, uint64_t exec_tenant_id, ObSqlString &sql,
                               ObMViewRefreshStatsParams &params);

public:
  ObMVRefreshStatsCollectionLevel collection_level_;
  int64_t retention_period_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
