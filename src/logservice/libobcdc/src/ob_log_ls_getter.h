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

#ifndef OCEANBASE_LOG_LS_GETTER_H_
#define OCEANBASE_LOG_LS_GETTER_H_

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap
#include "ob_log_systable_helper.h"         // ObLogSysTableHelper
#include "ob_log_tenant.h"
#include "ob_cdc_tenant_query.h"

namespace oceanbase
{
namespace libobcdc
{

typedef ObArray<share::ObLSID> LSIDArray;

class TenantLSQueryer : public ObCDCTenantQuery<LSIDArray>
{
public:
  TenantLSQueryer(const int64_t snapshot_ts_ns, common::ObMySQLProxy &sql_proxy)
    : ObCDCTenantQuery(sql_proxy), snapshot_ts_ns_(snapshot_ts_ns) {}
  ~TenantLSQueryer() { snapshot_ts_ns_ = OB_INVALID_TIMESTAMP; }
private:
  int build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql) override;
  int parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<LSIDArray> &result) override;
private:
  static const char* QUERY_LS_INFO_SQL_FORMAT;
private:
  int64_t snapshot_ts_ns_;
};

class ObLogLsGetter
{
public:
  ObLogLsGetter();
  ~ObLogLsGetter();
  int init(const common::ObIArray<uint64_t> &tenant_ids, const int64_t start_tstamp_ns);
  void destroy();

  int get_ls_ids(
      const uint64_t tenant_id,
      const int64_t snapshot_ts,
      common::ObIArray<share::ObLSID> &ls_id_array);

private:
  int query_and_set_tenant_ls_info_(
      const uint64_t tenant_id,
      const int64_t snapshot_ts);

  int query_tenant_ls_info_(
      const uint64_t tenant_id,
      const int64_t snapshot_ts,
      LSIDArray &ls_array);

private:
  typedef common::ObLinearHashMap<TenantID, LSIDArray> TenantLSIDsCache;

  bool is_inited_;
  TenantLSIDsCache tenant_ls_ids_cache_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
