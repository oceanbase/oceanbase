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

#ifndef _OB_USER_TAB_COL_STAT_SERVICE_H_
#define _OB_USER_TAB_COL_STAT_SERVICE_H_

#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_table_stat_cache.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_stat_sql_service.h"

namespace oceanbase {
namespace common {
class ObOptStatService {
public:
  ObOptStatService() : inited_(false)
  {}
  virtual int init(common::ObMySQLProxy* proxy, ObServerConfig* config);
  virtual int get_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& tstat);
  virtual int get_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle);
  virtual int load_column_stat_and_put_cache(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle);
  virtual int load_table_stat_and_put_cache(const ObOptTableStat::Key& key, ObOptTableStatHandle& handle);

private:
  int update_column_stat(const ObOptColumnStat& stat);
  int load_column_stat_and_put_cache(
      const ObOptColumnStat::Key& key, ObOptColumnStat& new_entry, ObOptColumnStatHandle& handle);

protected:
  bool inited_;
  static const int64_t DEFAULT_TAB_STAT_CACHE_PRIORITY = 1;
  static const int64_t DEFAULT_COL_STAT_CACHE_PRIORITY = 1;
  ObOptStatSqlService sql_service_;

  ObOptTableStatCache table_stat_cache_;
  ObOptColumnStatCache column_stat_cache_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* _OB_OPT_STAT_SERVICE_H_ */
