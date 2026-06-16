/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_LOCK_UNLOCK_H
#define OB_DBMS_CATALOG_STATS_LOCK_UNLOCK_H

#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_item.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;

class ObDbmsCatalogStatsLockUnlock
{
public:
  static int set_catalog_table_stats_lock(const ObCatalogTableStatParam &param,
                                          sql::ObExecContext &ctx,
                                          bool set_locked);

  static int check_catalog_stat_locked(sql::ObExecContext &ctx, ObCatalogTableStatParam &param);

  static int fill_catalog_stat_locked(sql::ObExecContext &ctx, ObCatalogTableStatParam &param);

private:
  static int get_catalog_stats_history_sql(const ObCatalogTableStatParam &param,
                                           sql::ObExecContext &ctx,
                                           ObMySQLTransaction &trans,
                                           bool set_locked,
                                           bool &need_update_lock,
                                           ObIArray<ObString> &no_stats_partition_values,
                                           ObIArray<uint64_t> &part_stattypes);

  static int get_catalog_stat_locked_partition_values(const ObSqlString &raw_sql,
                                                      sql::ObExecContext &ctx,
                                                      uint64_t tenant_id,
                                                      ObIAllocator &allocator,
                                                      ObIArray<ObString> &partition_values,
                                                      ObIArray<uint64_t> &stattype_locked_array);

  static int gen_partition_value_list(const ObCatalogTableStatParam &param,
                                      ObSqlString &sql_string,
                                      ObIArray<ObString> &all_partition_values);

  static int get_catalog_no_stats_partition_values(const StatTypeLocked stattype,
                                                   const ObIArray<ObString> &all_partition_values,
                                                   const ObIArray<ObString> &stat_partition_values,
                                                   const ObIArray<uint64_t> &stattype_locked,
                                                   ObIArray<ObString> &no_stats_partition_values,
                                                   ObIArray<uint64_t> &part_stattypes,
                                                   bool &need_update_lock);

  static int get_catalog_insert_locked_type_sql(const ObCatalogTableStatParam &param,
                                                const ObIArray<ObString> &no_stats_partition_values,
                                                const ObIArray<uint64_t> &part_stattypes,
                                                ObSqlString &insert_sql);

  static bool is_partition_value_locked(const ObString &partition_value,
                                        const ObIArray<ObString> &locked_partition_values,
                                        int64_t &idx);
};

} // namespace common
} // namespace oceanbase

#endif //OB_DBMS_CATALOG_STATS_LOCK_UNLOCK_H