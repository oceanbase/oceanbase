/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_PREFERENCES_H
#define OB_DBMS_CATALOG_STATS_PREFERENCES_H

#include "sql/engine/ob_exec_context.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"

namespace oceanbase
{
namespace common
{

class ObDbmsCatalogStatsPreferences
{
public:
  static int get_prefs(ObMySQLProxy *mysql_proxy,
                       ObIAllocator &allocator,
                       const ObCatalogTableIdentity &table_identity,
                       const ObString &opt_name,
                       ObObj &result);

  static int set_prefs(ObExecContext &ctx,
                       const ObCatalogTableIdentity &table_identity,
                       const ObString &opt_name,
                       const ObString &opt_value);

  static int delete_prefs(ObExecContext &ctx,
                          const ObCatalogTableIdentity &table_identity,
                          const ObString &opt_name);

  static int get_global_prefs(ObMySQLProxy *mysql_proxy,
                              ObIAllocator &allocator,
                              const uint64_t tenant_id,
                              const ObString &opt_name,
                              ObObj &result);

  static int set_global_prefs(ObExecContext &ctx,
                              const ObString &opt_name,
                              const ObString &opt_value);

  static int delete_global_prefs(ObExecContext &ctx,
                                 const ObString &opt_name);

private:
  static int do_get_prefs(ObMySQLProxy *mysql_proxy,
                          ObIAllocator &allocator,
                          const uint64_t tenant_id,
                          const ObSqlString &raw_sql,
                          bool &get_result,
                          ObObj &result);
};

} // namespace common
} // namespace oceanbase

#endif // OB_DBMS_CATALOG_STATS_PREFERENCES_H
