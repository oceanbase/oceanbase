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

#ifndef OB_DBMS_STAT_H
#define OB_DBMS_STAT_H

#include "share/stat/ob_stat_define.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_type.h"
#include "share/stat/ob_dbms_stats_preferences.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "share/stat/ob_dbms_stats_copy_table_stats.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace pl
{

enum MethodOptColConf
{
  FOR_ALL = 0,
  FOR_INDEXED,
  FOR_HIDDEN
};

struct MethodOptSizeConf
{
  //oracle default value is: val = 75 and mode = 1, compatible oracle
  MethodOptSizeConf(int32_t mode = 1, int32_t val = 75)
    : mode_(mode), val_(val)
  {}

  MethodOptSizeConf(MethodOptSizeConf &other)
    : mode_(other.mode_), val_(other.val_)
  {}

  inline bool is_auto() const { return mode_ == 0 && val_ == 0; }
  inline bool is_repeat() const { return mode_ == 0 && val_ == 1; }
  inline bool is_skewonly() const { return mode_ == 0 && val_ == 2; }
  inline bool is_manual() const {return mode_ == 1; }

  int32_t mode_;
  int32_t val_;
  TO_STRING_KV(K_(mode), K_(val));
};

class ObDbmsStats
{
public:

  static int gather_table_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int gather_schema_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int gather_index_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int gather_table_index_stats(ObExecContext &ctx,
                                      const ObTableStatParam &data_param,
                                      ObIArray<int64_t> &no_gather_index_ids);

  static int fast_gather_index_stats(ObExecContext &ctx,
                                     const ObTableStatParam &data_param,
                                     bool &is_fast_gather,
                                     ObIArray<int64_t> &no_gather_index_ids);

  static int set_table_stats(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &result);

  static int set_column_stats(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int set_index_stats(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &result);

  static int delete_table_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int delete_column_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int delete_schema_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int delete_index_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int delete_table_index_stats(sql::ObExecContext &ctx,
                                      const ObTableStatParam data_param);

  static int create_stat_table(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result);

  static int drop_stat_table(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &result);

  static int export_table_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int export_column_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int export_schema_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int export_index_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int export_table_index_stats(sql::ObExecContext &ctx,
                                      const ObTableStatParam data_param);

  static int import_table_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int import_column_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int import_schema_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int import_index_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int import_table_index_stats(sql::ObExecContext &ctx,
                                      const ObTableStatParam data_param);

  static int lock_table_stats(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int lock_or_unlock_index_stats(sql::ObExecContext &ctx,
                                        const ObTableStatParam data_param,
                                        bool is_lock_stats);

  static int lock_partition_stats(sql::ObExecContext &ctx,
                                  sql::ParamStore &params,
                                  common::ObObj &result);

  static int lock_schema_stats(sql::ObExecContext &ctx,
                               sql::ParamStore &params,
                               common::ObObj &result);

  static int unlock_table_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int unlock_partition_stats(sql::ObExecContext &ctx,
                                    sql::ParamStore &params,
                                    common::ObObj &result);

  static int unlock_schema_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int restore_table_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int restore_schema_stats(sql::ObExecContext &ctx,
                                  sql::ParamStore &params,
                                  common::ObObj &result);

  static int purge_stats(sql::ObExecContext &ctx,
                         sql::ParamStore &params,
                         common::ObObj &result);

  static int alter_stats_history_retention(sql::ObExecContext &ctx,
                                           sql::ParamStore &params,
                                           common::ObObj &result);

  static int get_stats_history_availability(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result);

  static int get_stats_history_retention(sql::ObExecContext &ctx,
                                         sql::ParamStore &params,
                                         common::ObObj &result);

  static int reset_global_pref_defaults(sql::ObExecContext &ctx,
                                        sql::ParamStore &params,
                                        common::ObObj &result);

  static int set_global_prefs(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int set_schema_prefs(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int set_table_prefs(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &result);

  static int get_prefs(sql::ObExecContext &ctx,
                       sql::ParamStore &params,
                       common::ObObj &result);

  static int delete_schema_prefs(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int delete_table_prefs(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int copy_table_stats(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int cancel_gather_stats(sql::ObExecContext &ctx,
                                 sql::ParamStore &params,
                                 common::ObObj &result);

  static int parse_method_opt(sql::ObExecContext &ctx,
                              ObIAllocator *allocator,
                              ObIArray<ObColumnStatParam> &column_params,
                              const ObString &method_opt,
                              bool &use_size_auto);

  static int parser_for_all_clause(const ParseNode *for_all_node,
                                   ObIArray<ObColumnStatParam> &column_params,
                                   bool &use_size_auto);

  static int parser_for_columns_clause(const ParseNode *for_col_node,
                                       ObIArray<ObColumnStatParam> &column_params,
                                       common::ObIArray<ObString> &record_cols);

  static int parse_partition_name(ObExecContext &ctx,
                                  const share::schema::ObTableSchema *&table_schema,
                                  const ObObjParam &part_name,
                                  ObTableStatParam &param);

  static int parse_table_info(ObExecContext &ctx,
                              const ObObjParam &owner,
                              const ObObjParam &tab_name,
                              const bool is_index,
                              const share::schema::ObTableSchema *&table_schema,
                              ObTableStatParam &param);

  static int parse_table_info(ObExecContext &ctx,
                              const StatTable &stat_table,
                              const share::schema::ObTableSchema *&table_schema,
                              ObTableStatParam &param);

  static int parse_index_table_info(ObExecContext &ctx,
                                    const ObObjParam &owner,
                                    const ObObjParam &tab_name,
                                    const ObObjParam &idx_name,
                                    const share::schema::ObTableSchema *&table_schema,
                                    ObTableStatParam &param);

  static int parse_table_part_info(ObExecContext &ctx,
                                   const ObObjParam &owner,
                                   const ObObjParam &tab_name,
                                   const ObObjParam &part_name,
                                   ObTableStatParam &param,
                                   bool need_parse_col_group = false);

  static int parse_table_part_info(ObExecContext &ctx,
                                   const ObObjParam &owner,
                                   const ObObjParam &tab_name,
                                   const ObObjParam &part_name,
                                   ObTableStatParam &param,
                                   const share::schema::ObTableSchema *&table_schema,
                                   bool need_parse_col_group = false);

  static int parse_table_part_info(ObExecContext &ctx,
                                   const StatTable stat_table,
                                   ObTableStatParam &param,
                                   bool need_parse_col_group = false);

  static int parse_set_table_info(ObExecContext &ctx,
                                  const ObObjParam &owner,
                                  const ObObjParam &tab_name,
                                  const ObObjParam &part_name,
                                  ObTableStatParam &param);

  static int parse_set_column_stats(ObExecContext &ctx,
                                    const ObObjParam &owner,
                                    const ObObjParam &tab_name,
                                    const ObObjParam &colname,
                                    const ObObjParam &part_name,
                                    ObObjMeta &col_meta,
                                    ObTableStatParam &param);

  static int parse_set_column_stats_options(ObExecContext &ctx,
                                            const ObObjParam &stattab,
                                            const ObObjParam &statid,
                                            const ObObjParam &distcnt,
                                            const ObObjParam &density,
                                            const ObObjParam &nullcnt,
                                            const ObObjParam &avgclen,
                                            const ObObjParam &flags,
                                            const ObObjParam &statown,
                                            const ObObjParam &no_invalidate,
                                            const ObObjParam &force,
                                            ObSetColumnStatParam &param);

  static int parse_gather_stat_options(ObExecContext &ctx,
                                       const ObObjParam &est_percent,
                                       const ObObjParam &block_sample,
                                       const ObObjParam &method_opt,
                                       const ObObjParam &degree,
                                       const ObObjParam &granularity,
                                       const ObObjParam &cascade,
                                       const ObObjParam &no_invalidate,
                                       const ObObjParam &force,
                                       ObTableStatParam &param);

  static int use_default_gather_stat_options(ObExecContext &ctx,
                                             const StatTable &stat_table,
                                             ObTableStatParam &param);

  static int get_default_stat_options(ObExecContext &ctx,
                                      const int64_t stat_options,
                                      ObTableStatParam &param);

  static int parse_granularity_and_method_opt(ObExecContext &ctx,
                                              ObTableStatParam &param);

  static int init_column_stat_params(ObIAllocator &allocator,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     const share::schema::ObTableSchema &schema,
                                     ObIArray<ObColumnStatParam> &column_params);

  static bool check_column_validity(const share::schema::ObTableSchema &tab_schema,
                                   const share::schema::ObColumnSchemaV2 &col_schema);

  static int set_default_column_params(ObIArray<ObColumnStatParam> &column_params);

  static int parse_size_clause(const ParseNode *node, MethodOptSizeConf &size_opt);

  static int parse_for_columns(const ParseNode *node,
                               const ObIArray<ObColumnStatParam> &column_params,
                               common::ObIArray<ObString> &cols,
                               common::ObIArray<ObString> &record_cols);

  static int check_is_valid_col(const ObString &src_str,
                                const ObIArray<ObColumnStatParam> &column_params,
                                const common::ObIArray<ObString> &record_cols);

  static bool is_match_column_option(ObColumnStatParam &param,
                                     const MethodOptColConf &for_all_opt);

  static bool is_match_column_option(ObColumnStatParam &param,
                                     const ObIArray<ObString> &for_col_list);

  static int compute_bucket_num(ObColumnStatParam &param,
                                const MethodOptSizeConf &size_conf);

  static int get_table_part_infos(const share::schema::ObTableSchema *table_schema,
                                  ObIAllocator &allocator,
                                  common::ObIArray<PartInfo> &part_infos,
                                  common::ObIArray<PartInfo> &subpart_infos,
                                  OSGPartMap *part_map = NULL);

  static int get_part_ids_from_schema(const share::schema::ObTableSchema *table_schema,
                                      common::ObIArray<ObObjectID> &target_part_ids);

  static int update_stat_cache(const uint64_t rpc_tenant_id,
                               const ObTableStatParam &param,
                               ObOptStatRunningMonitor *running_monitor = NULL);

  static int parse_set_table_stat_options(ObExecContext &ctx,
                                          const ObObjParam &stattab,
                                          const ObObjParam &statid,
                                          const ObObjParam &numrows,
                                          const ObObjParam &numblks,
                                          const ObObjParam &avgrlen,
                                          const ObObjParam &flags,
                                          const ObObjParam &statown,
                                          const ObObjParam &no_invalidate,
                                          const ObObjParam &cachedblk,
                                          const ObObjParam &cachehit,
                                          const ObObjParam &force,
                                          const ObObjParam &nummacroblks,
                                          const ObObjParam &nummicroblks,
                                          ObSetTableStatParam &param);

  static int parse_set_hist_stats_options(ObExecContext &ctx,
                                          const ObObjParam &epc,
                                          const ObObjParam &minval,
                                          const ObObjParam &maxval,
                                          const ObObjParam &bkvals,
                                          const ObObjParam &novals,
                                          const ObObjParam &chvals,
                                          const ObObjParam &eavals,
                                          const ObObjParam &rpcnts,
                                          const ObObjParam &eavs,
                                          ObHistogramParam &hist_param);

  static int parser_pl_numarray(const ObObjParam &numarray_param,
                                ObIArray<int64_t> &num_array);

  static int parser_pl_chararray(const ObObjParam &chararray_param,
                                 ObIArray<ObString> &char_array);

  static int parser_pl_rawarray(const ObObjParam &rawarray_param,
                                ObIArray<ObString> &raw_array);

  static int find_selected_part_infos(const ObString &part_name,
                                      const ObIArray<PartInfo> &part_infos,
                                      const ObIArray<PartInfo> &subpart_infos,
                                      const bool is_sensitive_compare,
                                      ObIArray<PartInfo> &new_part_infos,
                                      ObIArray<PartInfo> &new_subpart_infos,
                                      bool &is_subpart_name);

  static int flush_database_monitoring_info(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result);

  static int process_not_size_manual_column(sql::ObExecContext &ctx, ObTableStatParam &table_param);

  static int parse_set_partition_name(ObExecContext &ctx,
                                      const share::schema::ObTableSchema *&table_schema,
                                      const ObObjParam &part_name,
                                      ObTableStatParam &param);

  static int gather_database_stats_job_proc(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result);

  static int gather_database_table_stats(sql::ObExecContext &ctx,
                                         const int64_t duration_time,
                                         int64_t &succeed_cnt,
                                         ObOptStatTaskInfo &task_info);

  static int do_gather_table_stats(sql::ObExecContext &ctx,
                                   const int64_t table_id,
                                   const uint64_t tenant_id,
                                   const int64_t duration_time,
                                   int64_t &succeed_cnt,
                                   ObOptStatTaskInfo &task_info);

  static int do_gather_tables_stats(sql::ObExecContext &ctx,
                                    const uint64_t tenant_id,
                                    const ObIArray<int64_t> &table_ids,
                                    const int64_t duration_time,
                                    int64_t &succeed_cnt,
                                    ObOptStatTaskInfo &task_info);

  static int get_table_stale_percent(sql::ObExecContext &ctx,
                                     const uint64_t tenant_id,
                                     const share::schema::ObTableSchema &table_schema,
                                     const double stale_percent_threshold,
                                     StatTable &stat_table);

  static int gather_table_stats_with_default_param(ObExecContext &ctx,
                                                   const int64_t duration_time,
                                                   const StatTable &stat_table,
                                                   ObOptStatTaskInfo &task_info);

  static int set_param_global_part_id(ObExecContext &ctx,
                                      ObTableStatParam &param,
                                      bool is_data_table = false,
                                      const int64_t data_table_id = -1,
                                      share::schema::ObPartitionLevel data_table_level
                                          = share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO);

  static int init_gather_task_info(ObExecContext &ctx,
                                   ObOptStatGatherType type,
                                   int64_t start_time,
                                   int64_t task_table_count,
                                   ObOptStatTaskInfo &task_info);

  static int get_table_stale_percent_threshold(sql::ObExecContext &ctx,
                                               const uint64_t tenant_id,
                                               const uint64_t table_id,
                                               double &stale_percent_threshold);
  static int extract_copy_stat_helper(sql::ParamStore &params,
                                      sql::ObExecContext &ctx,
                                      const share::schema::ObTableSchema *table_schema,
                                      CopyTableStatHelper &copy_stat_helper);

  static int init_column_group_stat_param(const share::schema::ObTableSchema &table_schema,
                                          ObIArray<ObColumnGroupStatParam> &column_group_params);

  static int gather_system_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int delete_system_stats(sql::ObExecContext &ctx,
                                sql::ParamStore &params,
                                common::ObObj &result);

  static int set_system_stats(sql::ObExecContext &ctx,
                              sql::ParamStore &params,
                              common::ObObj &result);

  static int update_system_stats_cache(const uint64_t rpc_tenant_id,
                                      const uint64_t tenant_id);

  static void update_optimizer_gather_stat_info(const ObOptStatTaskInfo *task_info,
                                                const ObOptStatGatherStat *gather_stat);

private:
  static int check_statistic_table_writeable(sql::ObExecContext &ctx);

  static int parse_column_info(sql::ObExecContext &ctx,
                               const ObObjParam &column_name,
                               ObTableStatParam &param);

  static int parse_stat_category(const ObString &stat_category);

  static int parse_stat_type(const ObString &stat_type_str, StatTypeLocked &stat_type);

  static int get_all_table_ids_in_database(ObExecContext &ctx,
                                           const ObObjParam &owner,
                                           ObTableStatParam &stat_param,
                                           ObIArray<uint64_t> &table_ids);

  static int get_new_stat_pref(ObExecContext &ctx,
                               common::ObIAllocator &allocator,
                               ObString &opt_name,
                               ObString &opt_value,
                               bool is_global_prefs,
                               ObStatPrefs *&stat_pref);

  static int convert_vaild_ident_name(common::ObIAllocator &allocator,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      ObString &ident_name,
                                      bool need_extra_conv = false);


  static int get_common_table_stale_percent(sql::ObExecContext &ctx,
                                            const uint64_t tenant_id,
                                            const share::schema::ObTableSchema &table_schema,
                                            StatTable &stat_table);

  static int get_user_partition_table_stale_percent(sql::ObExecContext &ctx,
                                                    const uint64_t tenant_id,
                                                    const share::schema::ObTableSchema &table_schema,
                                                    const double stale_percent_threshold,
                                                    StatTable &stat_table);

  static bool is_table_gather_global_stats(const int64_t global_id,
                                           const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
                                           int64_t &cur_row_cnt);

  static int parse_index_part_info(ObExecContext &ctx,
                                   const ObObjParam &owner,
                                   const ObObjParam &index_name,
                                   const ObObjParam &part_name,
                                   const ObObjParam &table_name,
                                   ObTableStatParam &param);

  static int get_table_index_infos(share::schema::ObSchemaGetterGuard *schema_guard,
                                   const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   uint64_t *index_tid_arr,
                                   int64_t &index_count);

  static int get_table_partition_infos(const ObTableSchema &table_schema,
                                       ObIAllocator &allocator,
                                       ObIArray<PartInfo> &partition_infos);

  static int get_index_schema(sql::ObExecContext &ctx,
                              common::ObIAllocator &allocator,
                              const int64_t data_table_id,
                              const bool is_sensitive_compare,
                              ObString &index_name,
                              const share::schema::ObTableSchema *&index_schema);

  static bool is_func_index(const ObTableStatParam &index_param);


  static bool need_gather_index_stats(const ObTableStatParam &table_param);

  static int resovle_granularity(ObGranularityType granu_type,
                                 const bool use_size_auto,
                                 ObTableStatParam &param);

  static void decide_modified_part(ObTableStatParam &param, const bool cascade_parts);

  static int parse_degree_option(ObExecContext &ctx, const ObObjParam &degree,
                                 ObTableStatParam &stat_param);

  static int refresh_tenant_schema_guard(ObExecContext &ctx, const uint64_t tenant_id);

  static int adjust_auto_gather_stat_option(const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
                                            ObTableStatParam &param);

  static bool is_partition_no_regather(int64_t part_id,
                                       const ObIArray<ObPartitionStatInfo> &partition_stat_infos,
                                       bool &is_locked);

  static int check_system_stats_name_valid(const ObString& name, bool &is_valid);

  static int check_modify_system_stats_pri(const ObSQLSessionInfo& session);

  static int check_system_stat_table_ready(int64_t tenant_id);

};

}
}

#endif // OB_DBMS_STAT_H
