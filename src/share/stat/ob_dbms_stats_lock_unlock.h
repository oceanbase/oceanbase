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

#ifndef OB_DBMS_STATS_LOCK_UNLOCK_H
#define OB_DBMS_STATS_LOCK_UNLOCK_H

#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"

namespace oceanbase {
using namespace sql;
namespace common {
struct ObOptTableStatHandle;
class ObOptColumnStatHandle;

class ObDbmsStatsLockUnlock
{
public:
  static int set_table_stats_lock(ObExecContext &ctx,
                                  const ObTableStatParam &param,
                                  bool set_locked);

  static int check_stat_locked(ObExecContext &ctx,
                               ObTableStatParam &param);

  static int fill_stat_locked(ObExecContext &ctx,
                              ObTableStatParam &param);

  static int adjust_table_stat_param(const ObIArray<int64_t> &locked_partition_ids,
                                     ObTableStatParam &param);

  static int get_insert_locked_type_sql(const ObTableStatParam &param,
                                        const ObIArray<int64_t> &no_stats_partition_ids,
                                        const ObIArray<uint64_t> &part_stattypes,
                                        ObSqlString &insert_sql);

private:

  static int get_stat_locked_partition_ids(ObExecContext &ctx,
                                           uint64_t tenant_id,
                                           const ObSqlString &raw_sql,
                                           ObIArray<int64_t> &partition_ids,
                                           ObIArray<int64_t> &stattype_locked_array);

  static int gen_partition_list(const ObTableStatParam &param,
                                ObSqlString &sql_string,
                                ObIArray<int64_t> &all_partition_ids);

  static int adjust_table_stat_locked(const ObIArray<int64_t> &locked_partition_ids,
                                      const ObIArray<int64_t> &stattype_locked_array,
                                      ObTableStatParam &param);

  static bool is_partition_id_locked(int64_t partition_id,
                                     const ObIArray<int64_t> &locked_partition_ids,
                                     int64_t &idx);

  static int get_stats_history_sql(ObExecContext &ctx,
                                   ObMySQLTransaction &trans,
                                   const ObTableStatParam &param,
                                   bool set_locked,
                                   bool &need_update_lock,
                                   ObIArray<int64_t> &no_stats_partition_ids,
                                   ObIArray<uint64_t> &part_stattypes);

  static int get_no_stats_partition_ids(const StatTypeLocked stattype,
                                        const ObIArray<int64_t> &all_partition_ids,
                                        const ObIArray<int64_t> &stat_partition_ids,
                                        const ObIArray<int64_t> &stattype_locked,
                                        ObIArray<int64_t> &no_stats_partition_ids,
                                        ObIArray<uint64_t> &part_stattypes,
                                        bool &need_update_lock);

};


} // end of sql
} // end of namespace

#endif //OB_DBMS_STATS_LOCK_UNLOCK_H