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

#ifndef _OB_USER_TAB_COL_STATISTICS_H_
#define _OB_USER_TAB_COL_STATISTICS_H_

#include <stdint.h>
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_col_stat_sql_service.h"
#include "share/stat/ob_column_stat_cache.h"
#include "share/stat/ob_table_stat_cache.h"

namespace oceanbase {
namespace common {
struct ObPartitionKey;
class ObServerConfig;

class ObColStatService : public ObColumnStatDataService {
public:
  ObColStatService();
  ~ObColStatService();

  int init(common::ObMySQLProxy* proxy, common::ObServerConfig* config);

  /**
   * Get a ColumnStat which is stored in ObColumnStatCache, this interface for
   * read column statistics object used by SQL plan optimize.
   * Caller should not hold ObColumnStat object for a long time that ObColumnStatCache
   * can not release it.
   */
  int get_column_stat(const ObColumnStat::Key& key, const bool force_new, ObColumnStatValueHandle& handle) override;

  /**
   * same as above interface, except do not hold object in cache instead of deep copy
   * so %stat can be written in incremental calculation.
   */
  int get_column_stat(
      const ObColumnStat::Key& key, const bool force_new, ObColumnStat& stat, ObIAllocator& alloc) override;

  int get_batch_stat(const uint64_t table_id, const ObIArray<uint64_t>& partition_id,
      const ObIArray<uint64_t>& column_id, ObIArray<ObColumnStatValueHandle>& handles) override;

  int get_batch_stat(const share::schema::ObTableSchema& table_schema, const uint64_t partition_id,
      ObIArray<ObColumnStat*>& stats, ObIAllocator& allocator) override;
  /**
   * item in stats must not NULL, or will return OB_ERR_UNEXPECTED
   */
  int update_column_stats(const ObIArray<ObColumnStat*>& stats) override;
  int update_column_stat(const ObColumnStat& stat) override;
  int erase_column_stat(const ObPartitionKey& pkey, const int64_t column_id) override;

private:
  int load_and_put_cache(const ObColumnStat::Key& key, ObColumnStatValueHandle& handle);
  int load_and_put_cache(const ObColumnStat::Key& key, ObColumnStat& new_entry, ObColumnStatValueHandle& handle);
  int batch_load_and_put_cache(const share::schema::ObTableSchema& table_schema, const uint64_t partition_id,
      ObIArray<ObColumnStatValueHandle>& handles);
  int batch_load_and_put_cache(const uint64_t table_id, const ObIArray<uint64_t>& partition_ids,
      const ObIArray<uint64_t>& column_ids, ObIArray<ObColumnStatValueHandle>& handles);
  int batch_put_and_fetch_row(ObIArray<ObColumnStat*>& col_stats, ObIArray<ObColumnStatValueHandle>& handles);
  static int deep_copy(ObIAllocator& alloc, const ObColumnStat& src, ObColumnStat& dst);

private:
  ObTableColStatSqlService sql_service_;
  ObColumnStatCache column_stat_cache_;
  bool inited_;
};  // end of class ObColStatService

class ObTableStatService : public ObTableStatDataService {
public:
  ObTableStatService();
  ~ObTableStatService();

  int init(common::ObMySQLProxy* proxy, ObTableStatDataService* cache, ObServerConfig* config);
  virtual int get_table_stat(const common::ObPartitionKey& key, ObTableStat& tstat);
  int erase_table_stat(const common::ObPartitionKey& pkey);

private:
  int load_and_put_cache(ObTableStat::Key& key, ObTableStatValueHandle& handle);

private:
  static const int64_t DEFAULT_USER_TAB_STAT_CACHE_PRIORITY = 1;

private:
  ObTableColStatSqlService sql_service_;
  ObTableStatDataService* service_cache_;
  ObTableStatCache table_stat_cache_;
  bool inited_;
};

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_USER_TAB_COL_STATISTICS_H_ */
