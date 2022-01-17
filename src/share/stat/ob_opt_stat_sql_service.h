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

#ifndef _OB_OPT_STAT_SQL_SERVICE_H_
#define _OB_OPT_STAT_SQL_SERVICE_H_

#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObMySQLResult;
}
struct ObPartitionKey;
class ObServerConfig;
class ObMySQLProxy;
/**
 * SQL Service for fetching/updating table level statistics and column level statistics
 */
class ObOptStatSqlService {
public:
  ObOptStatSqlService();
  ~ObOptStatSqlService();
  bool is_inited() const
  {
    return inited_;
  }
  int init(ObMySQLProxy* proxy, ObServerConfig* config);
  int fetch_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& stat);

  int fill_table_stat(sqlclient::ObMySQLResult& result, ObOptTableStat& stat);

  // TODO: temporarily used
  int fetch_table_stat(uint64_t table_id, ObOptTableStat& stat);
  int fill_column_stat(const uint64_t table_id, sqlclient::ObMySQLResult& result, ObOptColumnStat& stat,
      ObHistogram& basic_histogram_info);
  int fetch_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStat& stat);
  int update_column_stat(const common::ObIArray<ObOptColumnStat*>& column_stats);
  int delete_column_stat(const ObIArray<ObOptColumnStat*>& column_stats);

private:
  int get_table_stat_sql(const ObOptColumnStat& stat, const int64_t current_time, ObSqlString& sql_string);
  int get_column_stat_sql(const ObOptColumnStat& stat, const int64_t current_time, ObSqlString& sql_string);
  int get_histogram_stat_sql(const ObOptColumnStat& stat, common::ObIAllocator& allocator,
      ObOptColumnStat::Bucket& bucket, ObSqlString& sql_string);
  int get_obj_str(const common::ObObj& obj, common::ObIAllocator& allocator, common::ObString& out_str);
  int get_obj_binary_hex_str(const common::ObObj& obj, common::ObIAllocator& allocator, common::ObString& out_str);
  int hex_str_to_obj(const char* buf, int64_t buf_len, common::ObIAllocator& allocator, common::ObObj& obj);

private:
  int fetch_histogram_stat(
      const ObOptColumnStat::Key& key, const ObHistogram& basic_histogram_info, ObOptColumnStat& stat);
  int fill_bucket_stat(sqlclient::ObMySQLResult& result, ObOptColumnStat& stat, int64_t& last_endpoint_num);
  bool inited_;
  ObMySQLProxy* mysql_proxy_;
  lib::ObMutex mutex_;
  ObServerConfig* config_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* _OB_OPT_STAT_SQL_SERVICE_H_ */
