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

#ifndef OCEANBASE_OB_LOG_SYSTABLE_QUERYER_H_
#define OCEANBASE_OB_LOG_SYSTABLE_QUERYER_H_

#include "ob_ls_log_stat_info.h"   // ObLSLogInfo
#include "ob_all_server_info.h"    // ObAllServerInfo
#include "ob_all_zone_info.h"      // ObAllZoneInfo, ObAllZoneTypeInfo
#include "src/logservice/logfetcher/ob_log_fetcher_err_handler.h"
#include "ob_all_units_info.h"     // ObUnitsRecordInfo

namespace oceanbase
{
namespace common
{
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace logservice
{
class ObLogSysTableQueryer
{
public:
  ObLogSysTableQueryer();
  virtual ~ObLogSysTableQueryer();
  int init(const int64_t cluster_id,
      const bool is_across_cluster,
      common::ObISQLClient &sql_proxy,
      logfetcher::IObLogErrHandler *err_handler);
  bool is_inited() const { return is_inited_; }
  void destroy();

public:
  // SELECT SVR_IP, SVR_PORT, ROLE, BEGIN_LSN, END_LSN FROM GV$OB_LOG_STAT
  int get_ls_log_info(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      ObLSLogInfo &ls_log_info);

  //  SELECT SVR_IP, SVR_PORT, ZONE, ZONE_TYPE, REGION from GV$OB_UNITS;
  int get_all_units_info(
      const uint64_t tenant_id,
      ObUnitsRecordInfo &units_record_info);

  int get_all_server_info(
      const uint64_t tenant_id,
      ObAllServerInfo &all_server_info);

  int get_all_zone_info(
      const uint64_t tenant_id,
      ObAllZoneInfo &all_zone_info);

  int get_all_zone_type_info(
      const uint64_t tenant_id,
      ObAllZoneTypeInfo &all_zone_type_info);

private:
  int do_query_(const uint64_t tenant_id,
      ObSqlString &sql,
      ObISQLClient::ReadResult &result);

  template <typename RecordsType>
  int get_records_template_(common::sqlclient::ObMySQLResult &res,
      RecordsType &records,
      const char *event,
      int64_t &record_count);

  // ObUnitsRecordInfo
  // @param [in] res, result read from the GV$OB_UNITS table
  // @param [out] units_record_info, items in the GV$OB_UNITS table
  int parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
      ObUnitsRecordInfo &units_record_info);

  // ObLSLogInfo
  // @param [in] res, result read from __all_virtual_log_stat table
  // @param [out] ls_log_info, meta/user tenant's LS Palf Info
  int parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
      ObLSLogInfo &ls_log_info);

  // ObAllServerInfo
  // @param [in] res, result read from __all_server table
  // @param [out] all_server_info, for __all_server record
  int parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
      ObAllServerInfo &all_server_info);

  // ObAllZoneInfo
  // @param [in] res, result read from __all_zone
  // @param [out] all_zone_info, for __all_zone record
  int parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
      ObAllZoneInfo &all_zone_info);

  // ObAllZoneTypeInfo
  // @param [in] res, result read from __all_zone
  // @param [out] all_zone_type_info, for __all_zone record
  int parse_record_from_row_(common::sqlclient::ObMySQLResult &res,
      ObAllZoneTypeInfo &all_zone_type_info);

private:
  bool is_inited_;                     // whether this class is inited
  bool is_across_cluster_;             // whether the SQL query across cluster
  int64_t cluster_id_;                 // ClusterID
  common::ObISQLClient *sql_proxy_;    // sql_proxy to use
  logfetcher::IObLogErrHandler *err_handler_;

  DISALLOW_COPY_AND_ASSIGN(ObLogSysTableQueryer);
};

} // namespace logservice
} // namespace oceanbase

#endif

