/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DBMS_CATALOG_STATS_H
#define OB_DBMS_CATALOG_STATS_H

#include "sql/engine/ob_exec_context.h"
#include "lib/oblog/ob_log_module.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/ob_opt_stat_gather_stat.h"

namespace oceanbase
{
namespace sql
{
class ObSqlSchemaGuard;
}
namespace share
{
class ObILakeTableMetadata;
}
namespace pl
{

class ObCatalogStatPrefs;

class ObDbmsCatalogStats
{
public:
  static int gather_table_stat(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result);

  static int lock_catalog_table_stat(sql::ObExecContext &ctx,
                                     sql::ParamStore &params,
                                     common::ObObj &result);

  static int unlock_catalog_table_stat(sql::ObExecContext &ctx,
                                       sql::ParamStore &params,
                                       common::ObObj &result);

  static int lock_catalog_partition_stat(sql::ObExecContext &ctx,
                                         sql::ParamStore &params,
                                         common::ObObj &result);

  static int unlock_catalog_partition_stat(sql::ObExecContext &ctx,
                                           sql::ParamStore &params,
                                           common::ObObj &result);

  static int set_catalog_table_prefs(sql::ObExecContext &ctx,
                                     sql::ParamStore &params,
                                     common::ObObj &result);

  static int get_catalog_prefs(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result);

  static int delete_catalog_table_prefs(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);

  static int set_catalog_global_prefs(sql::ObExecContext &ctx,
                                      sql::ParamStore &params,
                                      common::ObObj &result);

  static int get_catalog_global_prefs(sql::ObExecContext &ctx,
                                      sql::ParamStore &params,
                                      common::ObObj &result);

  static int delete_catalog_global_prefs(sql::ObExecContext &ctx,
                                         sql::ParamStore &params,
                                         common::ObObj &result);

  static int parse_catalog_table_stats_params(sql::ParamStore &params,
                                              sql::ObExecContext &ctx,
                                              ObCatalogTableStatParam &stat_param);

private:
  static int parse_table_part_info(const ObObjParam &catalog,
                                   const ObObjParam &db,
                                   const ObObjParam &table,
                                   const ObObjParam &part,
                                   sql::ObExecContext &ctx,
                                   ObCatalogTableStatParam &stat_param,
                                   const share::schema::ObTableSchema *&table_schema,
                                   const share::ObILakeTableMetadata *&lake_table_metadata);

  static int parse_base_stat_param(const ObObjParam &catalog,
                                   const ObObjParam &db,
                                   const ObObjParam &table,
                                   const ObObjParam &part,
                                   sql::ObExecContext &ctx,
                                   ObCatalogTableStatParam &stat_param);

  static int get_table_schema(ObCatalogTableStatParam &stat_param,
                              sql::ObExecContext &ctx,
                              sql::ObSqlSchemaGuard &sql_schema_guard,
                              const share::schema::ObTableSchema *&table_schema,
                              const share::ObILakeTableMetadata *&lake_table_metadata);

  static int parse_partition_name(const share::schema::ObTableSchema *table_schema,
                                  sql::ObExecContext &ctx,
                                  ObCatalogTableStatParam &stat_param);

  static int find_selected_part_info(const ObString &part_name,
                                     const ObIArray<common::ObCatalogExtPartitionInfo> &part_infos,
                                     bool is_sensitive_compare,
                                     common::ObCatalogExtPartitionInfo &found_part);

  static int init_column_stat_params(const share::schema::ObTableSchema *table_schema,
                                     ObIAllocator &allocator,
                                     ObIArray<ObCatalogColumnStatParam> &column_params);

  static int parse_catalog_gather_stat_options(const ObObjParam &method_opt,
                                               const ObObjParam &degree,
                                               const ObObjParam &granularity,
                                               const ObObjParam &force,
                                               sql::ObExecContext &ctx,
                                               ObCatalogTableStatParam &param);
  static int parse_catalog_sample_mode(const ObString &sample_type,
                                       common::ObCatalogAnalyzeSampleInfo::SampleMode &sample_mode);
  static int resolve_catalog_granularity(ObCatalogTableStatParam &param);
  static int parse_method_opt_and_filter_columns(sql::ObExecContext &ctx,
                                                 ObCatalogTableStatParam &param);

  static int parser_for_all_clause(const ParseNode *for_all_node,
                                   ObIArray<ObCatalogColumnStatParam> &column_params);

  static int parser_for_columns_clause(const ParseNode *for_col_node,
                                       ObIArray<ObCatalogColumnStatParam> &column_params);

  static bool is_match_column_option(const ObCatalogColumnStatParam &param,
                                     const ObIArray<ObString> &for_col_list);

  static bool check_column_validity(const share::schema::ObColumnSchemaV2 &col_schema);

  static int check_statistic_table_writeable(sql::ObExecContext &ctx);

  static int init_gather_task_info(sql::ObExecContext &ctx,
                                   ObOptStatGatherType type,
                                   int64_t start_time,
                                   int64_t task_table_count,
                                   ObOptStatTaskInfo &task_info);
  static int update_catalog_stat_cache(const uint64_t rpc_tenant_id,
                                       const ObCatalogTableStatParam &stat_param,
                                       ObOptStatRunningMonitor *running_monitor /*default null*/);
  static int convert_vaild_ident_name(const common::ObDataTypeCastParams &dtc_params,
                                      common::ObIAllocator &allocator,
                                      bool need_extra_conv,
                                      ObString &ident_name);
  static int get_stats_consumer_group_id(ObCatalogTableStatParam &param);
  static int check_catalog_prefs_validity(sql::ObExecContext &ctx,
                                          common::ObIAllocator &allocator,
                                          const common::ObString &opt_name,
                                          const common::ObString &opt_value,
                                          bool is_set_op);
  static int get_new_catalog_stat_pref(common::ObIAllocator &allocator,
                                       const common::ObString &opt_name,
                                       const common::ObString &opt_value,
                                       ObCatalogStatPrefs *&stat_pref);
  static void update_optimizer_gather_stat_info(const ObOptStatTaskInfo *task_info,
                                                const ObOptStatGatherStat *gather_stat,
                                                const ObCatalogTableStatParam &stat_param);
  static int split_part_func_exprs_(ObIAllocator &allocator,
                                    const ObString &part_func_expr,
                                    ObIArray<ObString> &part_exprs);
};

} // namespace pl
} // namespace oceanbase

#endif // OB_DBMS_CATALOG_STATS_H
