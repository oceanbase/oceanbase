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
#include "share/stat/ob_opt_ds_stat.h"
#include "share/stat/ob_opt_ds_stat_cache.h"

namespace oceanbase {
namespace common {
class ObOptStatService
{
public:
  ObOptStatService() : inited_(false) {}
  virtual int init(common::ObMySQLProxy *proxy, ObServerConfig *config);
  virtual int get_table_stat(const uint64_t tenant_id,
                             const ObOptTableStat::Key &key,
                             ObOptTableStat &tstat);
  virtual int get_column_stat(const uint64_t tenant_id,
                              const ObOptColumnStat::Key &key,
                              ObOptColumnStatHandle &handle);
  virtual int load_table_stat_and_put_cache(const uint64_t tenant_id,
                                            const ObOptTableStat::Key &key,
                                            ObOptTableStatHandle &handle);
  int get_column_stat(const uint64_t tenant_id,
                      ObIArray<const ObOptColumnStat::Key*> &keys,
                      ObIArray<ObOptColumnStatHandle> &handles);
  int get_table_stat(const uint64_t tenant_id,
                     const ObOptTableStat::Key &key,
                     ObOptTableStatHandle &handle);
  int get_ds_stat(const ObOptDSStat::Key &key,
                  ObOptDSStatHandle &handle);

  int erase_table_stat(const ObOptTableStat::Key &key);
  int erase_column_stat(const ObOptColumnStat::Key &key);
  int erase_ds_stat(const ObOptDSStat::Key &key);
  int add_ds_stat_cache(const ObOptDSStat::Key &key,
                        const ObOptDSStat &value,
                        ObOptDSStatHandle &ds_stat_handle);

  ObOptStatSqlService &get_sql_service() { return sql_service_; }

  int get_table_rowcnt(const uint64_t tenant_id,
                       const uint64_t table_id,
                       const ObIArray<ObTabletID> &all_tablet_ids,
                       const ObIArray<share::ObLSID> &all_ls_ids,
                       int64_t &table_rowcnt);

private:
  /**
    * 接口load_and_put_cache(key, handle)的实现，外部不应该直接调用这个函数
    * new_entry是在栈上分配的空间，用于临时存放统计信息
    */
  int load_column_stat_and_put_cache(const uint64_t tenant_id,
                                     ObIArray<const ObOptColumnStat::Key*> &keys,
                                     ObIArray<ObOptColumnStatHandle> &handles);

  int init_key_column_stats(ObIAllocator &allocator,
                            ObIArray<const ObOptColumnStat::Key*> &keys,
                            ObIArray<ObOptKeyColumnStat> &key_column_stats);

  int load_table_rowcnt_and_put_cache(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const ObIArray<ObTabletID> &all_tablet_ids,
                                      const ObIArray<share::ObLSID> &all_ls_ids,
                                      int64_t &table_rowcnt);
protected:
  bool inited_;
  static const int64_t DEFAULT_TAB_STAT_CACHE_PRIORITY = 1;
  static const int64_t DEFAULT_COL_STAT_CACHE_PRIORITY = 1;
  static const int64_t DEFAULT_DS_STAT_CACHE_PRIORITY = 1;
  ObOptStatSqlService sql_service_;

  ObOptTableStatCache table_stat_cache_;
  ObOptColumnStatCache column_stat_cache_;
  ObOptDSStatCache ds_stat_cache_;
};

}
}

#endif /* _OB_OPT_STAT_SERVICE_H_ */
