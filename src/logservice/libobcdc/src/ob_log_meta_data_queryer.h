/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OB_LOG_META_SQL_QUERYER_H_
#define OCEANBASE_OB_LOG_META_SQL_QUERYER_H_

#include "ob_cdc_tenant_query.h"
#include "logservice/logfetcher/ob_log_data_dictionary_in_log_table.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace libobcdc
{

// query OB_SYS_TENANT_ID in tenant_sync_mode and query specific tenant_id in cluster_sync_mode
class ObLogMetaDataSQLQueryer : public ObCDCTenantQuery<logfetcher::DataDictionaryInLogInfo>
{
public:
  ObLogMetaDataSQLQueryer(const int64_t start_timstamp_ns, common::ObMySQLProxy &sql_proxy)
    : ObCDCTenantQuery(sql_proxy), start_timstamp_ns_(start_timstamp_ns) {}
  ~ObLogMetaDataSQLQueryer() { start_timstamp_ns_ = OB_INVALID_TIMESTAMP; }
public:
  int get_data_dict_in_log_info(
      const uint64_t tenant_id,
      const int64_t start_timestamp_ns,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);
private:
  int build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql) override;
  int parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<logfetcher::DataDictionaryInLogInfo> &result) override;
private:
  static const char* QUERY_SQL_FORMAT;
private:
  int64_t start_timstamp_ns_;
  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataSQLQueryer);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
