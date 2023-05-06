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

#include "lib/mysqlclient/ob_isql_client.h"    // ObISQLClient
#include "logservice/logfetcher/ob_log_data_dictionary_in_log_table.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace libobcdc
{
class ObLogMetaDataSQLQueryer
{
public:
  ObLogMetaDataSQLQueryer();
  virtual ~ObLogMetaDataSQLQueryer();
  int init(
      const int64_t cluster_id,
      const bool is_across_cluster,
      common::ObISQLClient &sql_proxy);
  bool is_inited() const { return is_inited_; }
  void destroy();

public:
  int get_data_dict_in_log_info(
      const uint64_t tenant_id,
      const int64_t start_timstamp_ns,
      int64_t &record_count,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);

private:
  int do_query_(const uint64_t tenant_id,
      ObSqlString &sql,
      ObISQLClient::ReadResult &result);

  template <typename RecordsType>
  int get_records_template_(
      common::sqlclient::ObMySQLResult &res,
      RecordsType &records,
      const char *event,
      int64_t &record_count);

  // logfetcher::DataDictionaryInLogInfo
  // @param [in] res, result read from __all_virtual_data_dictionary_in_log
  // @param [out] data_dict_in_log_info
  int parse_record_from_row_(
      common::sqlclient::ObMySQLResult &res,
      logfetcher::DataDictionaryInLogInfo &data_dict_in_log_info);

private:
  bool is_inited_;                     // whether this class is inited
  bool is_across_cluster_;             // whether the SQL query across cluster
  int64_t cluster_id_;                 // ClusterID
  common::ObISQLClient *sql_proxy_;    // sql_proxy to use

  DISALLOW_COPY_AND_ASSIGN(ObLogMetaDataSQLQueryer);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
