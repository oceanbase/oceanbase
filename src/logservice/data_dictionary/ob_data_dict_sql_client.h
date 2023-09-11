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
*
*/

#ifndef OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_SQL_CLIENT_
#define OCEANBASE_DICT_SERVICE_DATA_DICTIONARY_SQL_CLIENT_

#include "lib/mysqlclient/ob_mysql_result.h"    // ObMySQLResult
#include "logservice/palf/lsn.h"                // LSN
#include "share/scn.h"                // SCN
#include "share/ob_ls_id.h"                     // ObLSArray

namespace oceanbase
{
namespace common
{
 class ObMySQLProxy;
}
namespace datadict
{

class ObDataDictSqlClient
{
public:
  ObDataDictSqlClient();
  ~ObDataDictSqlClient() { destroy(); }
public:
  int init(common::ObMySQLProxy *sql_client);
  void destroy();
public:
  int get_ls_info(
      const uint64_t tenant_id,
      const share::SCN &snapshot_scn,
      share::ObLSArray &ls_array);
  int get_schema_version(
      const uint64_t tenant_id,
      const share::SCN &snapshot_scn,
      int64_t &schema_version);
public:
  int report_data_dict_persist_info(
      const uint64_t tenant_id,
      const share::SCN &snapshot_scn,
      const palf::LSN &start_lsn,
      const palf::LSN &end_lsn);
private:
  static const char *query_ls_info_sql_format;
  static const char *query_tenant_schema_version_sql_format;
  static const char *report_data_dict_persist_info_sql_format;
private:
  int parse_record_from_row_(
      common::sqlclient::ObMySQLResult &result,
      int64_t &record_count,
      int64_t &schema_version);
private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
};

} // namespace datadict
} // namespace oceanbase
#endif
